//! KV replay in open loop: each trace request starts at its timestamp
//! divided by `speedup`, whether or not the previous one has finished, and
//! its schedule lag is recorded.

mod kv_common;

use kv_common::{CHUNK_TOKENS, Hooks, Server, key, replay_on, request};
use std::time::Duration;

#[test]
fn kv_replay_open_loop_follows_the_trace_clock() {
    let ct = CHUNK_TOKENS;
    // The first request's SET is answered after 300 ms, so it is still in
    // flight when the second request is due.
    let server = Server::with_hooks(Hooks {
        delay_set: Some((key(1, 0), Duration::from_millis(300))),
        ..Default::default()
    });
    // Trace times 0, 600 and 1200 ms at speedup 3: due at 0, 200 and 400 ms.
    let rows = replay_on(
        &server,
        &[
            request(1, 0, &[1], ct),
            request(2, 600, &[2], ct),
            request(3, 1200, &[3], ct),
        ],
        "speedup = 3",
        2,
    );
    assert_eq!(rows.len(), 3, "{rows:?}");
    for r in &rows {
        let lag = r.lag_ns.expect("open loop records a schedule lag");
        assert!(
            lag < 50_000_000,
            "request {} started {lag} ns late",
            r.req_id
        );
    }

    // The server saw each request's lookup about 200 ms after the previous
    // one: neither back to back nor after the slow SET.
    let log = server.log();
    let lookup_at = |chunk: u64| {
        log.iter()
            .find(|o| o.cmd == "EXISTS" && o.key == key(chunk, 0))
            .unwrap()
            .at
    };
    for (a, b) in [(1, 2), (2, 3)] {
        let gap = lookup_at(b).duration_since(lookup_at(a));
        assert!(
            gap >= Duration::from_millis(180) && gap < Duration::from_millis(260),
            "lookups of requests {a} and {b} were {gap:?} apart"
        );
    }
    assert_eq!(cachecannon::metrics::KV_REQUESTS.value(), 3);
}
