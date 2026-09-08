//! The monomorphized pipeline: compile-time composition, zero dispatch.

/// What a filter decided about the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Keep walking the chain.
    Continue,
    /// Reject: `(status_code, reason)` — pipeline stops, response is sent.
    Reject(u16, &'static str),
}

/// One stage of the pipeline.
///
/// Implemented by filter types; `run` is called through *concrete* types in
/// a [`Pipeline`], so each invocation is a direct (inlinable) call.
pub trait Filter {
    /// Filter name (metrics/logging tag).
    fn name(&self) -> &'static str;

    /// Applies the filter.
    fn run(&self, ctx: &mut crate::RequestCtx<'_>) -> Outcome;
}

/// Terminal empty pipeline.
#[derive(Debug, Clone, Copy, Default)]
pub struct Nil;

impl Pipeline<Nil> {
    /// Starts a pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self { head: Nil }
    }
}

impl Default for Pipeline<Nil> {
    fn default() -> Self {
        Self::new()
    }
}

/// A compile-time filter chain: `Pipeline<A>` → `.then(B)` → `Pipeline<Chain<B, A>>`.
#[derive(Debug, Clone)]
pub struct Pipeline<L> {
    head: L,
}

impl<L: FilterChain> Pipeline<L> {
    /// Appends `f` to the front of the chain (runs first).
    #[must_use]
    pub fn then<F: Filter>(self, f: F) -> Pipeline<Chain<F, L>> {
        Pipeline {
            head: Chain {
                filter: f,
                next: self.head,
            },
        }
    }

    /// Runs the chain head → tail, short-circuiting on [`Outcome::Reject`].
    #[inline]
    pub fn run(&self, ctx: &mut crate::RequestCtx<'_>) -> Outcome {
        self.head.run_chain(ctx)
    }

    /// Head filter name (introspection).
    #[must_use]
    pub fn head_name(&self) -> &'static str {
        self.head.name()
    }
}

/// Linked chain cell.
#[derive(Debug, Clone)]
pub struct Chain<F, N> {
    filter: F,
    next: N,
}

impl<F: Filter, N: FilterChain> FilterChain for Chain<F, N> {
    #[inline]
    fn run_chain(&self, ctx: &mut crate::RequestCtx<'_>) -> Outcome {
        match self.filter.run(ctx) {
            Outcome::Continue => self.next.run_chain(ctx),
            reject => reject,
        }
    }

    fn name(&self) -> &'static str {
        self.filter.name()
    }
}

impl Filter for Nil {
    fn name(&self) -> &'static str {
        "nil"
    }

    #[inline]
    fn run(&self, _ctx: &mut crate::RequestCtx<'_>) -> Outcome {
        Outcome::Continue
    }
}

/// Trait for chain tails (implemented by `Nil` and every `Chain`).
pub trait FilterChain {
    /// Runs the remaining chain.
    fn run_chain(&self, ctx: &mut crate::RequestCtx<'_>) -> Outcome;

    /// Name of the first filter in this chain.
    fn name(&self) -> &'static str;
}

impl FilterChain for Nil {
    #[inline]
    fn run_chain(&self, _ctx: &mut crate::RequestCtx<'_>) -> Outcome {
        Outcome::Continue
    }

    fn name(&self) -> &'static str {
        "nil"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Outcome;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    struct Tag(u16);

    impl Filter for Tag {
        fn name(&self) -> &'static str {
            "tag"
        }

        fn run(&self, ctx: &mut crate::RequestCtx<'_>) -> Outcome {
            if self.0 == 0 {
                ctx.short_circuit = Some((403, "tagged"));
                return Outcome::Reject(403, "tagged");
            }
            Outcome::Continue
        }
    }

    struct Held {
        path: String,
        ctx: Option<crate::RequestCtx<'static>>,
    }

    impl Held {
        fn new(path: &str) -> Self {
            let mut h = Held {
                path: path.to_owned(),
                ctx: None,
            };
            // SAFETY-free pattern: the ctx borrows `h.path`, which lives as
            // long as `h`; we transmute the lifetime locally for storage and
            // only use `h.ctx()` accessors afterwards.
            let ctx = crate::RequestCtx::new(
                "GET",
                &mut h.path,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000),
                None,
            );
            // SAFETY: test-only — ctx borrows `h.path`, which outlives all
            // `h.ctx()` uses; the transmute restates the lifetime for the
            // self-referential storage and nothing else.
            h.ctx = Some(unsafe {
                std::mem::transmute::<crate::RequestCtx<'_>, crate::RequestCtx<'static>>(ctx)
            });
            h
        }

        fn ctx(&mut self) -> &mut crate::RequestCtx<'static> {
            self.ctx.as_mut().expect("ctx")
        }
    }

    #[test]
    fn chain_runs_all_and_short_circuits() {
        let pipe = Pipeline::new().then(Tag(1)).then(Tag(2)).then(Tag(3));
        let mut h = Held::new("/x");
        assert_eq!(pipe.run(h.ctx()), Outcome::Continue);
        assert!(h.ctx().short_circuit.is_none());

        let pipe = Pipeline::new().then(Tag(1)).then(Tag(0)).then(Tag(3));
        let mut h = Held::new("/x");
        assert_eq!(pipe.run(h.ctx()), Outcome::Reject(403, "tagged"));
        assert_eq!(h.ctx().short_circuit, Some((403, "tagged")));
    }

    #[test]
    fn empty_pipeline_continues() {
        let mut h = Held::new("/x");
        assert_eq!(Pipeline::<Nil>::new().run(h.ctx()), Outcome::Continue);
    }
}
