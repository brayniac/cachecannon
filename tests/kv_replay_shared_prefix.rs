//! KV replay of two prompts that share their first two chunks: the second
//! finds those two cached and writes only the chunks after them.

mod kv_common;

use kv_common::{CHUNK_TOKENS, Server, key, replay, request};

#[test]
fn kv_replay_shared_prefix_hits_and_writes_from_the_divergence() {
    let ct = CHUNK_TOKENS;
    let server = Server::default();
    let rows = replay(
        &server,
        &[
            request(1, 0, &[1, 2, 3, 4], 4 * ct + 3),
            request(2, 1, &[1, 2, 7, 8], 4 * ct + 3),
        ],
        "",
    );
    let b = &rows[1];
    assert_eq!(b.req_id, 2);
    assert_eq!((b.lookup_hit_chunks, b.hit_chunks), (2, 2), "{b:?}");
    assert_eq!(b.cached_tokens, u64::from(2 * ct), "{b:?}");
    assert_eq!(b.new_tokens, u64::from(2 * ct + 3), "{b:?}");
    assert_eq!((b.gets, b.sets), (2, 2), "{b:?}");

    // B read chunks 1 and 2 and wrote only 7 and 8.
    assert_eq!(server.ops("GET"), vec![key(1, 0), key(2, 0)]);
    assert_eq!(server.sets()[4..], [key(7, 0), key(8, 0)]);
}
