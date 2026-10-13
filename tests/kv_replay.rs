//! KV-cache replay (`[workload.kv_replay]`) against an in-process RESP
//! store, one trace per test (`common::kv`).
//!
//! The runner can be started once per process and its metrics are
//! process-global, so every test runs its body in a child process of this
//! binary (`common::kv::isolated`): one test binary instead of one per test,
//! because each is a separate fat-LTO link in the release test job.

mod common;

use cachecannon::metrics;
use cachecannon::{ColorMode, OutputFormat, create_formatter, run_benchmark_full};
use common::kv::{
    CHUNK_TOKENS, Hooks, Server, key, load_config, replay, replay_on, request, temp_path,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// A Hugging Face config with the given `layer_types`.
fn model_config(layer_types: &str) -> String {
    format!(
        r#"{{
            "num_hidden_layers": 4,
            "layer_types": {layer_types},
            "hidden_size": 512,
            "num_attention_heads": 8,
            "num_key_value_heads": 2,
            "head_dim": 64,
            "linear_num_value_heads": 4,
            "linear_num_key_heads": 2,
            "linear_key_head_dim": 32,
            "linear_value_head_dim": 32,
            "torch_dtype": "bfloat16"
        }}"#
    )
}

/// KV replay against an empty server: nothing is cached until a prompt
/// repeats, and a repeat that hits every chunk of a prompt made of whole
/// chunks reports all tokens but the last as cached.
#[test]
fn kv_replay_cold_cache_then_full_prompt_repeat() {
    common::kv::isolated("kv_replay_cold_cache_then_full_prompt_repeat", || {
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
    });
}

/// KV replay of two prompts that share their first two chunks: the second
/// finds those two cached and writes only the chunks after them.
#[test]
fn kv_replay_shared_prefix_hits_and_writes_from_the_divergence() {
    common::kv::isolated(
        "kv_replay_shared_prefix_hits_and_writes_from_the_divergence",
        || {
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
        },
    );
}

/// KV replay with an object deleted from the middle of a cached prefix, under
/// the default write rule: the prefix stops at the gap, and every chunk from
/// the gap on is written again although the chunks after it are present.
#[test]
fn kv_replay_gap_ends_the_prefix_and_set_from_miss_rewrites_after_it() {
    common::kv::isolated(
        "kv_replay_gap_ends_the_prefix_and_set_from_miss_rewrites_after_it",
        || {
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
        },
    );
}

/// KV replay with an object deleted from the middle of a cached prefix, under
/// insert-if-absent: the prefix stops at the gap, and only the missing object
/// is written.
#[test]
fn kv_replay_gap_ends_the_prefix_and_insert_if_absent_fills_only_the_gap() {
    common::kv::isolated(
        "kv_replay_gap_ends_the_prefix_and_insert_if_absent_fills_only_the_gap",
        || {
            let ct = CHUNK_TOKENS;
            let server = Server::with_hooks(Hooks {
                delete_after_sets: Some((4, vec![key(2, 0)])),
                ..Default::default()
            });
            let a = request(1, 0, &[1, 2, 3, 4], 4 * ct + 3);
            let again = request(2, 1, &[1, 2, 3, 4], 4 * ct + 3);
            let rows = replay(&server, &[a, again], r#"write_rule = "insert-if-absent""#);
            let r = &rows[1];
            assert_eq!((r.lookup_hit_chunks, r.hit_chunks), (1, 1), "{r:?}");
            assert_eq!(r.cached_tokens, u64::from(ct), "{r:?}");
            assert_eq!((r.gets, r.sets), (1, 1), "{r:?}");
            assert_eq!(server.sets()[4..], [key(2, 0)]);
        },
    );
}

/// KV replay of a dense model at tensor parallelism 2: each chunk is one
/// object per rank, and a chunk counts as cached only when both ranks'
/// objects are present.
#[test]
fn kv_replay_tp2_prefix_is_the_minimum_over_ranks() {
    common::kv::isolated("kv_replay_tp2_prefix_is_the_minimum_over_ranks", || {
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
    });
}

/// KV replay of an MLA model at tensor parallelism 4: only rank 0 stores, at
/// full size, so no other rank's object is looked up, read or written.
#[test]
fn kv_replay_mla_reads_and_writes_rank_0_only() {
    common::kv::isolated("kv_replay_mla_reads_and_writes_rank_0_only", || {
        let ct = CHUNK_TOKENS;
        let server = Server::default();
        let a = request(1, 0, &[1, 2, 3], 3 * ct + 3);
        let again = request(2, 1, &[1, 2, 3], 3 * ct + 3);
        let rows = replay(
            &server,
            &[a, again],
            "kv_bytes_per_token = 4\nmla = true\ntp = 4",
        );
        assert_eq!(rows[1].cached_tokens, u64::from(3 * ct), "{rows:?}");

        let log = server.log();
        let kv_ops: Vec<_> = log.iter().filter(|o| o.cmd != "PING").collect();
        assert_eq!(kv_ops.len(), 3 + 3 + 3 + 3, "{kv_ops:?}");
        for op in kv_ops {
            let id = u64::from_str_radix(std::str::from_utf8(&op.key[3..]).unwrap(), 16).unwrap();
            assert_eq!(kvtrace::obj_id_fields(id).2, 0, "{op:?} is not rank 0");
        }
        assert_eq!(server.sets(), vec![key(1, 0), key(2, 0), key(3, 0)]);
        // Rank 0 stores the whole latent.
        assert_eq!(server.value_len(&key(1, 0)), Some(4 * ct as usize));
    });
}

/// KV replay rejects a hybrid model (linear-attention layers with a
/// recurrent state) before the run starts.
#[test]
fn kv_replay_rejects_a_hybrid_model() {
    common::kv::isolated("kv_replay_rejects_a_hybrid_model", || {
        // Control: the same config with every layer full attention is accepted.
        let dense = temp_path("dense.json");
        std::fs::write(
            &dense,
            model_config(
                r#"["full_attention", "full_attention", "full_attention", "full_attention"]"#,
            ),
        )
        .unwrap();
        let cfg = cachecannon::config::KvReplay {
            trace: "unused".into(),
            speedup: 0.0,
            model_config: Some(dense.clone()),
            kv_dtype: None,
            kv_bytes_per_token: None,
            mla: false,
            tp: 1,
            write_rule: Default::default(),
            key_prefix: "kv:".into(),
            key_format: Default::default(),
            requests_out: None,
        };
        cachecannon::kvreplay::geometry(&cfg).unwrap();
        std::fs::remove_file(&dense).unwrap();

        let hybrid = temp_path("hybrid.json");
        std::fs::write(
            &hybrid,
            model_config(
                r#"["linear_attention", "full_attention", "linear_attention", "full_attention"]"#,
            ),
        )
        .unwrap();
        let config = load_config(&format!(
            r#"
            [target]
            endpoints = ["127.0.0.1:1"]

            [workload.kv_replay]
            trace = "{}"
            model_config = "{}"

            [admin]
            format = "quiet"
            "#,
            temp_path("no-trace.parquet").display(),
            hybrid.display(),
        ));
        let err = run_benchmark_full(
            config,
            None,
            create_formatter(OutputFormat::Quiet, ColorMode::Never),
            Arc::new(AtomicBool::new(true)),
        )
        .unwrap_err()
        .to_string();
        std::fs::remove_file(&hybrid).unwrap();
        assert!(err.contains("Hybrid models are not supported"), "{err}");
    });
}

/// KV replay when an object is evicted between its EXISTS and its GET: the
/// nil GET is counted, and the cached prefix ends at its chunk. The store
/// still starts where the lookup's prefix ended, as LMCache's does, so the
/// evicted object is not written again.
#[test]
fn kv_replay_get_nil_ends_the_cached_prefix() {
    common::kv::isolated("kv_replay_get_nil_ends_the_cached_prefix", || {
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
    });
}

/// KV replay in open loop: each trace request starts at its timestamp
/// divided by `speedup`, whether or not the previous one has finished, and
/// its schedule lag is recorded.
#[test]
fn kv_replay_open_loop_follows_the_trace_clock() {
    common::kv::isolated("kv_replay_open_loop_follows_the_trace_clock", || {
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
    });
}

/// KV replay of a trace row whose `input_tokens` is 0 although it has full
/// chunks (kvtrace keeps the source's prompt length, which can disagree with
/// the chunks): the repeat finds both chunks, counts 0 cached and 0 new
/// tokens, and the run finishes.
#[test]
fn kv_replay_zero_input_tokens_with_chunks() {
    common::kv::isolated("kv_replay_zero_input_tokens_with_chunks", || {
        let server = Server::default();
        let rows = replay(
            &server,
            &[request(1, 0, &[1, 2], 0), request(2, 1, &[1, 2], 0)],
            "",
        );
        assert_eq!(rows.len(), 2, "{rows:?}");
        let r = &rows[1];
        assert_eq!((r.hit_chunks, r.gets), (2, 2), "{r:?}");
        assert_eq!((r.cached_tokens, r.new_tokens), (0, 0), "{r:?}");
        assert_eq!(metrics::KV_NEW_TOKENS.value(), 0);
        assert_eq!(metrics::KV_FULL_HITS.value(), 0);
    });
}

/// KV replay when the first chunk's object is evicted between its EXISTS and
/// its GET: the request has a load but no cached tokens, so it adds no sample
/// to the load time per 1,000 cached tokens.
#[test]
fn kv_replay_get_nil_at_the_first_chunk_has_no_per_token_load_time() {
    common::kv::isolated(
        "kv_replay_get_nil_at_the_first_chunk_has_no_per_token_load_time",
        || {
            let ct = CHUNK_TOKENS;
            let server = Server::with_hooks(Hooks {
                evict_after_exists: Some(key(1, 0)),
                ..Default::default()
            });
            let a = request(1, 0, &[1, 2], 2 * ct + 3);
            let again = request(2, 1, &[1, 2], 2 * ct + 3);
            let rows = replay(&server, &[a, again], "");
            let r = &rows[1];
            assert_eq!((r.lookup_hit_chunks, r.hit_chunks), (2, 0), "{r:?}");
            assert_eq!((r.gets, r.get_nil, r.cached_tokens), (2, 1, 0), "{r:?}");
            assert_eq!(metrics::KV_LOADS.value(), 1);
            assert_eq!(metrics::KV_LOADS_WITH_CACHED.value(), 0);
            assert!(
                metrics::KV_LOAD_TIME_PER_1K_TOKENS
                    .load()
                    .is_none_or(|h| h.into_iter().all(|b| b.count() == 0)),
                "a per-token load time was recorded"
            );
        },
    );
}
