//! KV replay when an object is evicted between its EXISTS and its GET: the
//! nil GET is counted, and the cached prefix ends at its chunk. The store
//! still starts where the lookup's prefix ended, as LMCache's does, so the
//! evicted object is not written again.

mod kv_common;

use cachecannon::metrics;
use kv_common::{CHUNK_TOKENS, Hooks, Server, key, replay, request};

#[test]
fn kv_replay_get_nil_ends_the_cached_prefix() {
    let ct = CHUNK_TOKENS;
    // The first request finds nothing, so the hook fires on the repeat's
    // EXISTS of the second chunk.
    let server = Server::with_hooks(Hooks {
        evict_after_exists: Some(key(2, 0)),
        ..Default::default()
    });
    let a = request(1, 0, &[1, 2, 3], 3 * ct + 3);
    let again = request(2, 1, &[1, 2, 3], 3 * ct + 3);
    let rows = replay(&server, &[a, again], "");
    let r = &rows[1];
    assert_eq!((r.lookup_hit_chunks, r.hit_chunks), (3, 1), "{r:?}");
    assert_eq!((r.gets, r.get_nil, r.sets), (3, 1, 0), "{r:?}");
    assert_eq!(r.cached_tokens, u64::from(ct), "{r:?}");
    assert_eq!(metrics::KV_GET_NIL.value(), 1);
    assert_eq!(server.value_len(&key(2, 0)), None);
}
