//! KV replay of an MLA model at tensor parallelism 4: only rank 0 stores, at
//! full size, so no other rank's object is looked up, read or written.

mod kv_common;

use kv_common::{CHUNK_TOKENS, Server, key, replay, request};

#[test]
fn kv_replay_mla_reads_and_writes_rank_0_only() {
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
}
