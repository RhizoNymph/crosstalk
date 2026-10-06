//! `ct-bench-detect` (`crosstalk_eval::bench_detect`): crosstalk's side of
//! the bench's detector contract, on synthetic fixtures only.

mod common;
mod contract;
mod from_export;
mod p5;

#[allow(dead_code)]
#[path = "../swarm_truth/fixture.rs"]
mod swarm_fixture;
