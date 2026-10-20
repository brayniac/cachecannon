//! `workload.routing = "routed"` over RESP against a server slower than the
//! offered rate: requests wait in the node's queue, that wait is part of the
//! reported response latency, and requests that find the queue full are
//! dropped and counted.

mod common;

#[test]
fn queue_wait_is_counted_and_overflow_is_dropped() {
    common::assert_routed_queue_wait("resp");
}
