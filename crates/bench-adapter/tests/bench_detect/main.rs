//! `ct-bench-detect` (`crosstalk_bench_adapter`): crosstalk's side of the
//! bench's detector contract, on synthetic fixtures only.

mod common;
mod contract;
mod from_export;
mod p5;

#[allow(dead_code)]
#[path = "../swarm/fixture.rs"]
mod swarm_fixture;
