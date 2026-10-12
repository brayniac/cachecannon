//! `workload.read_values` over memcache: every GET hit's value is read once.

mod common;

#[test]
fn read_values_reads_every_memcache_get_hit() {
    let addr = common::spawn_server(common::serve_memcache);
    let (hits, read) = common::run(addr, "memcache", true);
    assert!(hits > 0, "no hits recorded");
    assert_eq!(
        read,
        hits * common::VALUE_LEN as u64,
        "{hits} hits of {} bytes, but {read} bytes read",
        common::VALUE_LEN
    );
}
