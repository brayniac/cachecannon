//! `workload.read_values` over memcache: every GET hit's value is read once and
//! misses are not.

mod common;

#[test]
fn read_values_reads_every_memcache_get_hit() {
    common::assert_every_hit_read("memcache", common::Server { value_len: 100 });
}
