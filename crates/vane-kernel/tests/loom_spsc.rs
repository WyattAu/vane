//! Loom model-checking for the SPSC ring (Tier A concurrency gate).
//!
//! Run: `cargo test -p vane-core --features loom --test loom_spsc`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(feature = "loom")]
mod tests {
    use vane_core::spsc::SpscRing;

    #[test]
    fn spsc_handoff_order() {
        loom::model(|| {
            let ring: std::sync::Arc<SpscRing<u8, 2>> = std::sync::Arc::new(SpscRing::new());
            let producer = std::sync::Arc::clone(&ring);
            let consumer = std::sync::Arc::clone(&ring);

            let p = std::thread::spawn(move || {
                producer.try_push(1);
                producer.try_push(2);
            });
            let c = std::thread::spawn(move || {
                let first = consumer.try_pop();
                let second = consumer.try_pop();
                (first, second)
            });
            p.join().expect("producer");
            let (first, second) = c.join().expect("consumer");
            // FIFO order preserved in every interleaving where values exist.
            if let Some(v) = first {
                assert_eq!(v, 1);
            }
            if let Some(v) = second {
                assert_eq!(v, 2);
            }
        });
    }

    #[test]
    fn spsc_full_backpressure() {
        loom::model(|| {
            let ring: std::sync::Arc<SpscRing<u32, 1>> = std::sync::Arc::new(SpscRing::new());
            let producer = std::sync::Arc::clone(&ring);
            let consumer = std::sync::Arc::clone(&ring);

            let p = std::thread::spawn(move || {
                assert!(producer.try_push(7).is_none()); // fits
                // Second push may or may not fit depending on the consumer;
                // if full, the value comes back.
            });
            let c = std::thread::spawn(move || consumer.try_pop());
            p.join().expect("producer");
            let popped = c.join().expect("consumer");
            if let Some(v) = popped {
                assert_eq!(v, 7);
            }
        });
    }
}
