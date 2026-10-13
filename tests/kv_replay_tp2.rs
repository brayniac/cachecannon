//! KV replay of a dense model at tensor parallelism 2: each chunk is one
//! object per rank, and a chunk counts as cached only when both ranks'
//! objects are present.

mod kv_common;

use kv_common::{CHUNK_TOKENS, Hooks, Server, key, replay, request};

#[test]
fn kv_replay_tp2_prefix_is_the_minimum_over_ranks() {
    let ct = CHUNK_TOKENS;
    // After the first request's 8 SETs (4 chunks x 2 ranks), delete rank 1's
    // object for the third chunk. Rank 0 still has all four.
    let server = Server::with_hooks(Hooks {
        delete_after_sets: Some((8, vec![key(3, 1)])),
        ..Default::default()
    });
    let a = request(1, 0, &[1, 2, 3, 4], 4 * ct + 3);
    let again = request(2, 1, &[1, 2, 3, 4], 4 * ct + 3);
    let rows = replay(&server, &[a, again], "tp = 2");

    let r = &rows[0];
    assert_eq!((r.gets, r.sets), (0, 8), "{r:?}");
    let r = &rows[1];
    assert_eq!((r.lookup_hit_chunks, r.hit_chunks), (2, 2), "{r:?}");
    assert_eq!(r.cached_tokens, u64::from(2 * ct), "{r:?}");
    // Both ranks' objects of the two cached chunks are read; both ranks'
    // objects from the third chunk on are written.
    assert_eq!((r.gets, r.sets), (4, 4), "{r:?}");
    assert_eq!(
        server.ops("GET"),
        vec![key(1, 0), key(1, 1), key(2, 0), key(2, 1)]
    );
    assert_eq!(
        server.sets()[8..],
        [key(3, 0), key(3, 1), key(4, 0), key(4, 1)]
    );
    // Each rank holds half of 4 bytes per token.
    assert_eq!(server.value_len(&key(1, 1)), Some(2 * ct as usize));
}
