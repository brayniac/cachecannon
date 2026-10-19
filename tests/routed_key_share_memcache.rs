//! `workload.routing = "routed"` over memcache: each server receives the share
//! of GETs its keys carry under zipf, not an even split.

mod common;

#[test]
fn each_node_receives_its_key_share() {
    common::assert_routed_key_share("memcache");
}
