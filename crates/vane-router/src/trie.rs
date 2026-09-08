//! Immutable segment-wise radix trie with param + catch-all support.
//!
//! The trie is a persistent data structure: [`PathTrie::insert`] clones only
//! the nodes along the inserted path (plus their child lists), leaving all
//! other subtrees structurally shared with the previous generation. Lookups
//! walk `/`-separated segments; `:name` segments in *inserted patterns*
//! match any single segment; `*name` matches the remaining path.
//!
//! Nodes are `Arc`; readers operate on `&Trie` snapshots for free.

use std::collections::BTreeMap;
use std::sync::Arc;

/// What a segment in an inserted pattern means.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// Literal segment text.
    Literal(String),
    /// `:name` — matches exactly one segment, captured.
    Param(String),
    /// `*name` — matches the rest of the path (must be last).
    CatchAll(String),
}

impl Segment {
    fn parse(s: &str) -> Self {
        if let Some(name) = s.strip_prefix(':') {
            Self::Param(name.to_owned())
        } else if let Some(name) = s.strip_prefix('*') {
            Self::CatchAll(name.to_owned())
        } else {
            Self::Literal(s.to_owned())
        }
    }
}

/// A terminal route registration.
#[derive(Debug)]
pub struct Terminal<T> {
    /// Segment names for params, by capture index.
    pub param_names: Vec<String>,
    /// The value registered.
    pub value: T,
}

/// Node of the trie.
pub struct Node<T> {
    /// Literal children keyed by segment.
    literal: BTreeMap<String, Arc<Node<T>>>,
    /// Param children (usually 0 or 1; a list keeps patterns unambiguous
    /// with distinct names, first match wins).
    params: Vec<(String, Arc<Node<T>>)>,
    /// Catch-all subtree (at most one).
    catch_all: Option<(String, Arc<Node<T>>)>,
    /// Value(s) registered exactly at this node.
    terminal: Option<Arc<Terminal<T>>>,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self {
            literal: BTreeMap::new(),
            params: Vec::new(),
            catch_all: None,
            terminal: None,
        }
    }
}

impl<T> Node<T> {
    fn new() -> Self {
        Self::default()
    }

    /// Clones this node shallowly for path-copying.
    fn shallow(&self) -> Self {
        Self {
            literal: self.literal.clone(),
            params: self.params.clone(),
            catch_all: self.catch_all.clone(),
            terminal: self.terminal.clone(),
        }
    }
}

/// Match result: the terminal plus captured ranges into the request path.
#[derive(Debug)]
pub struct Matched<'r, T> {
    /// The matched terminal.
    pub terminal: &'r Terminal<T>,
    /// Captured `:param` values as `(capture_index, start, end)` into the path.
    pub params: Vec<(usize, u32, u32)>,
    /// Captured `*wildcard` as a range into the path (if any).
    pub wildcard: Option<(u32, u32)>,
}

/// Root of an immutable trie.
pub struct PathTrie<T> {
    root: Arc<Node<T>>,
    route_count: usize,
}

/// Internal parts for structural cloning across crates.
pub struct Parts<T> {
    /// Shared root node.
    pub root: Arc<Node<T>>,
    /// Route count.
    pub count: usize,
}

impl<T> Clone for PathTrie<T> {
    fn clone(&self) -> Self {
        Self {
            root: Arc::clone(&self.root),
            route_count: self.route_count,
        }
    }
}

impl<T> PathTrie<T> {
    /// Rebuilds a trie from [`Parts`] (structural clone).
    #[must_use]
    pub fn from_parts(parts: Parts<T>) -> Self {
        Self {
            root: parts.root,
            route_count: parts.count,
        }
    }

    /// Exposes internal parts for structural cloning.
    #[must_use]
    pub fn clone_parts(&self) -> Parts<T> {
        Parts {
            root: Arc::clone(&self.root),
            count: self.route_count,
        }
    }
}

impl<T> PathTrie<T> {
    /// Empty trie.
    #[allow(clippy::new_without_default)] // T has no Default bound
    pub fn new() -> Self {
        Self {
            root: Arc::new(Node::new()),
            route_count: 0,
        }
    }

    /// Number of registered routes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.route_count
    }

    /// `true` when no routes are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.route_count == 0
    }

    /// Returns a new trie with `pattern -> value` inserted.
    ///
    /// Path-copying: only nodes along `pattern` are cloned; everything else
    /// is shared with `self`. `pattern` is split on `/`; leading slash
    /// optional. `:name` and `*name` segments are recognized.
    #[must_use]
    pub fn insert(&self, pattern: &str, value: T) -> Self {
        let segments: Vec<Segment> = pattern
            .split('/')
            .filter(|s| !s.is_empty())
            .map(Segment::parse)
            .collect();
        let root = self.insert_at(Arc::clone(&self.root), &segments, 0, &[], value);
        Self {
            root,
            route_count: self.route_count + 1,
        }
    }

    fn insert_at(
        &self,
        node: Arc<Node<T>>,
        segments: &[Segment],
        idx: usize,
        param_names: &[String],
        value: T,
    ) -> Arc<Node<T>> {
        if idx >= segments.len() {
            // Terminal: clone the node, replace its terminal.
            let mut copy = node.shallow();
            copy.terminal = Some(Arc::new(Terminal {
                param_names: param_names.to_vec(),
                value,
            }));
            return Arc::new(copy);
        }
        match &segments[idx] {
            Segment::Literal(seg) => {
                let child = node
                    .literal
                    .get(seg)
                    .map(Arc::clone)
                    .unwrap_or_else(|| Arc::new(Node::new()));
                let new_child = self.insert_at(child, segments, idx + 1, param_names, value);
                let mut copy = node.shallow();
                copy.literal.insert(seg.clone(), new_child);
                Arc::new(copy)
            }
            Segment::Param(name) => {
                let mut names = param_names.to_vec();
                names.push(name.clone());
                let existing = node
                    .params
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, c)| Arc::clone(c));
                let child = existing.unwrap_or_else(|| Arc::new(Node::new()));
                let new_child = self.insert_at(child, segments, idx + 1, &names, value);
                let mut copy = node.shallow();
                if let Some(slot) = copy.params.iter_mut().find(|(n, _)| n == name) {
                    slot.1 = new_child;
                } else {
                    copy.params.push((name.clone(), new_child));
                }
                Arc::new(copy)
            }
            Segment::CatchAll(name) => {
                let mut names = param_names.to_vec();
                names.push(name.clone());
                let child = node
                    .catch_all
                    .as_ref()
                    .map(|(_, c)| Arc::clone(c))
                    .unwrap_or_else(|| Arc::new(Node::new()));
                // Catch-all is always terminal.
                let new_child = Arc::new(Node {
                    terminal: Some(Arc::new(Terminal {
                        param_names: names,
                        value,
                    })),
                    ..Node::new()
                });
                let _ = child;
                let mut copy = node.shallow();
                copy.catch_all = Some((name.clone(), new_child));
                Arc::new(copy)
            }
        }
    }

    /// Looks up `path` (e.g. `/users/42/posts`) with backtracking.
    ///
    /// Precedence at each segment: literal > param > catch-all; literals
    /// that dead-end deeper fall back to param siblings automatically.
    #[must_use]
    pub fn lookup(&self, path: &str) -> Option<Matched<'_, T>> {
        let bytes = path.as_bytes();
        let start = if bytes.first() == Some(&b'/') { 1 } else { 0 };
        match_node(&self.root, path, start, Vec::new())
    }
}

/// Recursive segment matcher with backtracking.
fn match_node<'r, T>(
    node: &'r Node<T>,
    path: &str,
    pos: usize,
    params: Vec<(usize, u32, u32)>,
) -> Option<Matched<'r, T>> {
    let bytes = path.as_bytes();
    if pos > bytes.len() {
        return None;
    }
    // Terminal check at end of path.
    if pos == bytes.len() {
        return node.terminal.as_ref().map(|t| Matched {
            terminal: t,
            params,
            wildcard: None,
        });
    }
    let end = bytes[pos..]
        .iter()
        .position(|&b| b == b'/')
        .map_or(bytes.len(), |i| i + pos);
    let seg = &path[pos..end];
    let next_pos = if end >= bytes.len() {
        bytes.len()
    } else {
        end + 1
    };

    if seg.is_empty() {
        return None; // '//' in request path: no match below
    }

    // 1. Literal child.
    if let Some(c) = node.literal.get(seg) {
        if let Some(m) = match_node(c, path, next_pos, params.clone()) {
            return Some(m);
        }
    }
    // 2. Param children (each consumes this segment).
    for (_, c) in &node.params {
        let mut p2 = params.clone();
        p2.push((p2.len(), pos as u32, end as u32));
        if let Some(m) = match_node(c, path, next_pos, p2) {
            return Some(m);
        }
    }
    // 3. Catch-all consumes the rest.
    if let Some((_, c)) = &node.catch_all {
        let t = c.terminal.as_ref()?;
        return Some(Matched {
            terminal: t,
            params,
            wildcard: Some((pos as u32, bytes.len() as u32)),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        let t = PathTrie::new()
            .insert("/a/b", 1)
            .insert("/a/c", 2)
            .insert("/", 3);
        assert_eq!(t.lookup("/a/b").expect("hit").terminal.value, 1);
        assert_eq!(t.lookup("/a/c").expect("hit").terminal.value, 2);
        assert_eq!(t.lookup("/").expect("hit").terminal.value, 3);
        assert!(t.lookup("/a").is_none());
        assert!(t.lookup("/a/b/c").is_none());
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn params_and_catchall() {
        let t = PathTrie::new()
            .insert("/users/:id", "user")
            .insert("/users/:id/posts/:pid", "post")
            .insert("/files/*rest", "file");
        let m = t.lookup("/users/42").expect("hit");
        assert_eq!(m.terminal.value, "user");
        assert_eq!(m.params.len(), 1);
        assert_eq!(
            &"/users/42"[m.params[0].1 as usize..m.params[0].2 as usize],
            "42"
        );

        let m = t.lookup("/users/42/posts/7").expect("hit");
        assert_eq!(m.terminal.value, "post");
        let p0 = &"/users/42/posts/7"[m.params[0].1 as usize..m.params[0].2 as usize];
        let p1 = &"/users/42/posts/7"[m.params[1].1 as usize..m.params[1].2 as usize];
        assert_eq!((p0, p1), ("42", "7"));

        let m = t.lookup("/files/a/b/c.txt").expect("hit");
        assert_eq!(m.terminal.value, "file");
        let w = m.wildcard.expect("wildcard");
        assert_eq!(&"/files/a/b/c.txt"[w.0 as usize..w.1 as usize], "a/b/c.txt");
    }

    #[test]
    fn structural_sharing() {
        let t1 = PathTrie::new().insert("/a", 1);
        let t2 = t1.insert("/b", 2);
        // t1 unaffected.
        assert!(t1.lookup("/a").is_some());
        assert!(t1.lookup("/b").is_none());
        assert!(t2.lookup("/b").is_some());
        assert_eq!(t1.len(), 1);
        assert_eq!(t2.len(), 2);
    }

    #[test]
    fn overwrite_terminal() {
        let t = PathTrie::new().insert("/a", 1).insert("/a", 2);
        assert_eq!(t.lookup("/a").expect("hit").terminal.value, 2);
    }
}
