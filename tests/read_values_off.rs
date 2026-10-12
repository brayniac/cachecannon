//! Control for `workload.read_values`: with the option off, no value bytes are
//! read.

mod common;

#[test]
fn read_values_off_reads_nothing() {
    let addr = common::spawn_server(common::serve_resp);
    let (hits, read) = common::run(addr, "resp", false);
    assert!(hits > 0, "no hits recorded");
    assert_eq!(read, 0, "read_values = false read {read} value bytes");
}
