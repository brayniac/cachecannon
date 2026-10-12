//! Control for `workload.read_values`: with the option off, no value bytes are
//! read.

mod common;

use common::Server;

#[test]
fn read_values_off_reads_nothing() {
    let addr = Server { value_len: 100 }.spawn_resp();
    let c = common::run(addr, "resp", false);
    assert!(c.hits > 0, "no hits recorded: {c:?}");
    assert_eq!(c.read, 0, "read_values = false read {} value bytes", c.read);
}
