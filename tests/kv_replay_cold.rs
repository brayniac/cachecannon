//! KV replay against an empty server: nothing is cached until a prompt
//! repeats, and a repeat that hits every chunk of a prompt made of whole
//! chunks reports all tokens but the last as cached.

mod kv_common;

use cachecannon::metrics;
use kv_common::{CHUNK_TOKENS, Server, key, replay, request};

#[test]
fn kv_replay_cold_cache_then_full_prompt_repeat() {
    let ct = CHUNK_TOKENS;
    let server = Server::default();
    let rows = replay(
        &server,
        &[
            request(1, 0, &[10, 11, 12, 13], 4 * ct),
            request(2, 1, &[20, 21, 22], 3 * ct + 5),
            request(3, 2, &[10, 11, 12, 13], 4 * ct),
        ],
        "",
    );
    assert_eq!(rows.len(), 3, "{rows:?}");

    // Closed loop has no schedule to lag.
    assert!(rows.iter().all(|r| r.lag_ns.is_none()), "{rows:?}");
    for r in &rows[..2] {
        assert_eq!(r.cached_tokens, 0, "{r:?}");
        assert_eq!(r.new_tokens, r.input_tokens, "{r:?}");
        assert_eq!(
            (r.lookup_hit_chunks, r.gets, r.sets),
            (0, 0, r.chunks),
            "{r:?}"
        );
    }
    let r = &rows[2];
    assert_eq!(r.req_id, 3);
    assert_eq!((r.hit_chunks, r.gets, r.sets), (4, 4, 0), "{r:?}");
    assert_eq!(r.cached_tokens, u64::from(4 * ct - 1), "{r:?}");
    assert_eq!(r.new_tokens, 1, "{r:?}");

    assert_eq!(metrics::KV_REQUESTS.value(), 3);
    assert_eq!(metrics::KV_CACHED_TOKENS.value(), u64::from(4 * ct - 1));
    assert_eq!(metrics::KV_FULL_HITS.value(), 1);
    assert_eq!(metrics::KV_CHUNK0_MISSES.value(), 2);
    assert_eq!(metrics::KV_EXISTS_COUNT.value(), 4 + 3 + 4);

    // Every chunk of both distinct prompts was written once, at 4 bytes per
    // token.
    let want: Vec<Vec<u8>> = [10, 11, 12, 13, 20, 21, 22].map(|c| key(c, 0)).to_vec();
    assert_eq!(server.sets(), want);
    assert_eq!(server.value_len(&key(10, 0)), Some(4 * ct as usize));
}
