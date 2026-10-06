//! The golden export (`crosstalk_eval::golden`): ct-eval's worlds, labels
//! and predictions in the bench format `a2a-bench/1`, checked with the
//! format's own checks.

mod cli;
mod common;
mod converters;
mod live;
mod mapping;
mod swarm;
mod village;

// Fixture builders shared with the other test targets; each uses part.
#[path = "../common/mod.rs"]
mod eval_common;
#[allow(dead_code)]
#[path = "../swarm_truth/fixture.rs"]
mod swarm_fixture;
#[allow(dead_code)]
#[path = "../ai_village/fixture.rs"]
mod village_fixture;
