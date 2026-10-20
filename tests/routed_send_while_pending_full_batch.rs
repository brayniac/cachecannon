//! `workload.routing = "routed"` over RESP with `batch_size` set to the
//! pipeline depth: a connection with a reply pending has no room for a batch,
//! so a request queued meanwhile waits for the reply.

mod common;

#[test]
fn a_request_queued_behind_a_pending_reply_waits_for_it() {
    let wait =
        common::routed_queue_wait_with_a_reply_pending("resp", Some(common::PENDING_PIPELINE));
    assert!(
        wait > 1_000_000,
        "the request waits for part of a 12 ms reply, got p50 {wait} ns"
    );
    // Nagle on the stub produced 650 ms on Linux. A slow host stretches each
    // 12 ms reply: about 110 ms under macOS background QoS, where this read
    // 54 ms.
    assert!(
        wait < 250_000_000,
        "the request waits for at most a few replies, got p50 {wait} ns"
    );
}
