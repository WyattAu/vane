//! Budget gate (`percentile-kit`): compares criterion results under
//! `target/criterion` against the committed `percentile-budgets.toml`.
//!
//! Ignored by default — CI runs it explicitly after `cargo bench`:
//!
//! ```text
//! cargo bench --workspace
//! cargo test -p vane-router --test budget_gate -- --ignored --nocapture
//! ```
//!
//! Budgets are set against a loaded development machine (see
//! `percentile-budgets.toml` for the baselines and rationale).

use std::path::{Path, PathBuf};

#[test]
#[ignore = "runs in CI after cargo bench (needs target/criterion)"]
fn criterion_results_within_budget() {
    // Paths resolve from the workspace root (the test's CWD is the
    // package dir); CRITERION_DIR overrides for non-default target
    // directories.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let criterion = std::env::var_os("CRITERION_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target/criterion"));
    let report = percentile_kit::check_budgets(
        &root.join("percentile-budgets.toml"),
        &criterion,
    )
    .expect("budgets parse + criterion results readable");
    println!("{}", report.to_markdown());
    report
        .ensure_pass()
        .expect("benchmark budgets exceeded — see the table above");
}
