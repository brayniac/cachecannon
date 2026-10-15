//! `workload.prefill_rate_limit` when workers serve different endpoints and
//! run out of prefill keys at different times. With zipf keys (theta 1.5) and
//! `key_share` allocation, the endpoint owning the hottest keys gets most of
//! the connections but only about a quarter of the prefill keys, so the
//! workers serving it drain first. The others must be able to use the whole
//! rate: 8000 keys at 4000 per second take about two seconds. With the rate
//! split per worker this took 3.8 s.

mod common;

#[test]
fn workers_whose_endpoints_drained_leave_the_rate_to_the_others() {
    let shape = common::Shape {
        endpoints: 4,
        threads: 16,
        connections: 16,
        pipeline: 1,
        keyspace: "distribution = \"zipf\"\nzipf_theta = 1.5",
        connection: "endpoint_allocation = \"key_share\"",
    };
    let span = common::prefill_span_shaped("resp", shape, 8000, 4000);
    assert!(span.as_secs_f64() > 1.6, "prefill took {span:?}");
    assert!(span.as_secs_f64() < 2.4, "prefill took {span:?}");
}
