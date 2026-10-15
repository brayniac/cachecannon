//! `workload.prefill_rate_limit` across two endpoints whose prefill queues
//! differ in size, on one worker: when one queue drains first, prefill still
//! finishes at the limit, 4000 keys at 2000 per second in about two seconds.
//! The order of pop and token take is covered by the disjoint-workers tests.

mod common;

#[test]
fn a_drained_endpoint_does_not_slow_the_other() {
    let shape = common::Shape {
        endpoints: 2,
        threads: 1,
        connections: 8,
        pipeline: 1,
        ..Default::default()
    };
    let span = common::prefill_span_shaped("resp", shape, 4000, 2000);
    assert!(span.as_secs_f64() > 1.6, "prefill took {span:?}");
    assert!(span.as_secs_f64() < 2.6, "prefill took {span:?}");
}
