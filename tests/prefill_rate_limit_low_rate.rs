//! `workload.prefill_rate_limit` at a low rate on one connection with
//! pipeline depth 1, where 10 ms of the rate is under one request: 300 keys
//! at 150 per second. A limiter holding exactly one token lost what accrued
//! while the connection woke and ran about 10-15% slow.
//!
//! The rate is checked on the second-fastest tenth of the keys, so a stall on
//! a busy test host does not fail it. Each tenth is 29 intervals of 6.67 ms,
//! 193 ms, and paced runs took up to 193.5 ms under heavy load. The one-token
//! limiter took 205 ms or more at load averages of 250-360 and 210 ms or more
//! on a quiet host, so the bound is 200 ms. The total span is also bounded
//! loosely (4 s, against 2.05 s seen under heavy load) so a limiter slow for
//! most of the run fails.

mod common;

#[test]
fn a_low_rate_is_reached() {
    let shape = common::Shape {
        endpoints: 1,
        threads: 1,
        connections: 1,
        pipeline: 1,
        ..Default::default()
    };
    let times = common::prefill_times_shaped("resp", shape, 300, 150);
    let span = *times.last().unwrap() - *times.first().unwrap();
    assert!(span.as_secs_f64() > 1.8, "prefill took {span:?}");
    assert!(span.as_secs_f64() < 4.0, "prefill took {span:?}");
    let tenths = common::decile_spans(&times);
    assert!(tenths[1].as_secs_f64() < 0.200, "tenths took {tenths:?}");
}
