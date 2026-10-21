//! `workload.routing = "routed"` over memcache with `batch_size` unset, which
//! in routed mode is 1: a request queued while a reply is pending is sent at
//! once, so it spends almost no time in the node's queue.

mod common;

#[test]
fn a_request_queued_behind_a_pending_reply_is_sent_at_once() {
    let wait = common::routed_queue_wait_with_a_reply_pending("memcache", None);
    assert!(
        wait < 500_000,
        "with room for a batch the request is sent at once, got p50 {wait} ns"
    );
}
