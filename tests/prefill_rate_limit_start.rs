//! A paced prefill starts at its rate. The prefill limiter is built before
//! connections open and holds 10 ms of the rate or one batch plus one batch;
//! starting full, a prefill at 1,000 per second with batches of 64 opened with
//! 128 SETs at once. It starts empty and is emptied again when prefill begins,
//! so the first 100 ms carry about 100.

use std::time::Duration;

mod common;

#[test]
fn prefill_does_not_open_with_the_hold() {
    let shape = common::Shape {
        endpoints: 1,
        threads: 2,
        connections: 4,
        pipeline: 64,
        connection: "batch_size = 64",
        ..Default::default()
    };
    let times = common::prefill_times_shaped("resp", shape, 2000, 1000);
    let first = times[0];
    let early = times
        .iter()
        .filter(|t| t.duration_since(first) < Duration::from_millis(100))
        .count();
    assert!(early <= 160, "{early} SETs in the first 100 ms of prefill");
}
