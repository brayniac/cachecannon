//! `workload.prefill_rate_limit` on one connection with pipeline depth 1:
//! 5,000 keys at 5,000 per second. A limiter that held one token discarded
//! what accrued while the refused connection slept and ran about 17-25% slow.
//!
//! The rate is checked on the second-fastest tenth of the keys rather than
//! the whole span: a stall on a busy test host lengthens a few tenths, and
//! one connection on a slow CI runner does not sustain 10,000 SET/s. Each
//! tenth is 499 intervals of 0.2 ms, 99.8 ms. Paced runs took 91-105 ms
//! (under 99.8 ms when a stall leaves the bucket holding tokens); the
//! one-token limiter took 117 ms or more. The total span is also bounded
//! loosely (3 s, against 1.85 s seen under heavy load) so a limiter slow for
//! most of the run fails.

mod common;

#[test]
fn one_connection_reaches_the_limit() {
    let shape = common::Shape {
        endpoints: 1,
        threads: 1,
        connections: 1,
        pipeline: 1,
        ..Default::default()
    };
    let times = common::prefill_times_shaped("resp", shape, 5_000, 5_000);
    let span = *times.last().unwrap() - *times.first().unwrap();
    assert!(span.as_secs_f64() > 0.8, "prefill took {span:?}");
    assert!(span.as_secs_f64() < 3.0, "prefill took {span:?}");
    let tenths = common::decile_spans(&times);
    assert!(tenths[1].as_secs_f64() < 0.11, "tenths took {tenths:?}");
}
