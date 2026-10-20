//! `workload.routing = "routed"` over RESP with `batch_size` left at the
//! pipeline depth: a connection with a reply pending has no room for a batch,
//! so a request queued meanwhile waits for the reply.

mod common;

#[test]
fn a_request_queued_behind_a_pending_reply_waits_for_it() {
    let wait = common::routed_queue_wait_with_a_reply_pending("resp", None);
    assert!(
        wait > 1_000_000,
        "the request waits for part of a 12 ms reply, got p50 {wait} ns"
    );
    // Hundreds of ms means the stub's replies are held by Nagle, not by the
    // connection waiting for its replies.
    assert!(
        wait < 50_000_000,
        "the request waits for at most a few 12 ms replies, got p50 {wait} ns"
    );
}
