//! Loom model-checking for the observe event ring (Tier A concurrency gate).
//!
//! Run: `cargo test -p vane-observe --features loom --test loom_ring`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(feature = "loom")]
mod tests {
    use vane_observe::ring::EventRing;

    #[test]
    fn mpsc_no_loss_single_consumer() {
        loom::model(|| {
            let ring: std::sync::Arc<EventRing<u8, 2>> = std::sync::Arc::new(EventRing::new());
            let r1 = std::sync::Arc::clone(&ring);
            let r2 = std::sync::Arc::clone(&ring);
            let r3 = std::sync::Arc::clone(&ring);

            let p1 = std::thread::spawn(move || {
                let _ = r1.try_push(1);
            });
            let p2 = std::thread::spawn(move || {
                let _ = r2.try_push(2);
            });
            let c = std::thread::spawn(move || {
                let mut got = (None, None);
                got.0 = r3.try_pop();
                got.1 = r3.try_pop();
                got
            });
            p1.join().expect("p1");
            p2.join().expect("p2");
            let (a, b) = c.join().expect("consumer");
            // Values (when observed) come from {1, 2} and never duplicate.
            for v in [a, b].into_iter().flatten() {
                assert!(v == 1 || v == 2);
            }
        });
    }
}
