//! `workload.read_values` over resp: every GET hit's value is read once and
//! misses are not.

mod common;

#[test]
fn read_values_reads_every_resp_get_hit() {
    common::assert_every_hit_read("resp", common::Server { value_len: 100 });
}
