//! `workload.routing = "routed"` against a server slower than the offered
//! rate: requests wait in the node's queue, that wait is part of the reported
//! response latency, and requests that find the queue full are dropped and
//! counted.

mod common;

use cachecannon::metrics::{RESPONSE_LATENCY, ROUTED_QUEUE_DROPPED, ROUTED_QUEUE_WAIT};
use std::time::Duration;

fn p50_ns(h: &metriken::AtomicHistogram) -> u64 {
    let h = h.load().expect("histogram has samples");
    h.quantiles(&[0.5])
        .unwrap()
        .unwrap()
        .entries()
        .values()
        .next()
        .unwrap()
        .end()
}

#[test]
fn queue_wait_is_counted_and_overflow_is_dropped() {
    // One connection with a pipeline of one against a server that takes 2 ms
    // per GET serves about 500 requests a second; 2000 are offered.
    let (addr, _) = common::spawn_get_counter("resp", Duration::from_millis(2));
    common::run_routed(&common::Routed {
        protocol: "resp",
        endpoints: &[addr],
        threads: 1,
        connections: 1,
        pipeline: 1,
        rate: 2000,
        duration: "2s",
        routing: "routed",
        keyspace: "count = 1000",
    });

    assert!(
        ROUTED_QUEUE_DROPPED.value() > 0,
        "the queue holds 16 requests and fills, so some are dropped"
    );
    let wait = p50_ns(&ROUTED_QUEUE_WAIT);
    let latency = p50_ns(&RESPONSE_LATENCY);
    assert!(
        wait > 4_000_000,
        "a full queue of 16 at 2 ms each waits well over 4 ms, got p50 {wait} ns"
    );
    assert!(
        latency >= wait,
        "response latency (p50 {latency} ns) includes the queue wait (p50 {wait} ns)"
    );
}
