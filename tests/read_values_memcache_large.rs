//! `workload.read_values` over memcache with values that span several receive
//! buffers.

mod common;

#[test]
fn read_values_reads_every_large_memcache_get_hit() {
    common::assert_every_hit_read(
        "memcache",
        common::Server {
            value_len: common::LARGE_VALUE_LEN,
        },
    );
}
