//! `workload.prefill_rate_limit` over memcache: the SETs of a 300-key prefill
//! limited to 300 requests per second span about a second. Unpaced, the same
//! prefill takes a few milliseconds.

mod common;

#[test]
fn prefill_is_paced_to_the_limit_over_memcache() {
    let span = common::prefill_span("memcache", 300, 300);
    // About one second: the limiter starts empty when prefill begins.
    assert!(span.as_secs_f64() > 0.8, "prefill took {span:?}");
    assert!(span.as_secs_f64() < 1.5, "prefill took {span:?}");
}
