//! KV replay with an object deleted from the middle of a cached prefix, under
//! the default write rule: the prefix stops at the gap, and every chunk from
//! the gap on is written again although the chunks after it are present.

mod kv_common;

use kv_common::{CHUNK_TOKENS, Hooks, Server, key, replay, request};

#[test]
fn kv_replay_gap_ends_the_prefix_and_set_from_miss_rewrites_after_it() {
    let ct = CHUNK_TOKENS;
    // After the first request's 4 SETs, delete its second chunk.
    let server = Server::with_hooks(Hooks {
        delete_after_sets: Some((4, vec![key(2, 0)])),
        ..Default::default()
    });
    let a = request(1, 0, &[1, 2, 3, 4], 4 * ct + 3);
    let again = request(2, 1, &[1, 2, 3, 4], 4 * ct + 3);
    let rows = replay(&server, &[a, again], "");
    let r = &rows[1];
    assert_eq!((r.lookup_hit_chunks, r.hit_chunks), (1, 1), "{r:?}");
    assert_eq!(r.cached_tokens, u64::from(ct), "{r:?}");
    assert_eq!((r.gets, r.sets), (1, 3), "{r:?}");
    assert_eq!(server.sets()[4..], [key(2, 0), key(3, 0), key(4, 0)]);
}
