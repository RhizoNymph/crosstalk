//! Spec primitives under simulation: ULID generators on skewed nodes whose
//! clocks step back, minting concurrently.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::ids::UlidGenerator;
use tokio::sync::mpsc;

use crate::clock::ClockStep;
use crate::driver::CheckFailed;
use crate::rng::SimRng;

const NODES: usize = 3;
const GENERATORS_PER_NODE: usize = 2;
const IDS_PER_GENERATOR: usize = 150;

fn below(rng: &mut SimRng, bound: u64) -> u64 {
    NonZeroU64::new(bound).map_or(0, |bound| rng.below(bound))
}

crate::sim_test! {
    /// `canonical.ids.ulid-unique`: generators on three nodes (each node's
    /// clock skewed back or forward by a few milliseconds, and stepped back
    /// now and then while they mint), two per node sharing its clock, mint
    /// interleaved at sub-millisecond paces. No id is minted twice, and each
    /// generator's ids increase.
    fn dst_concurrent_generators_never_collide(ctx) {
        let (sender, mut receiver) = mpsc::unbounded_channel::<(usize, u128)>();
        let base = ctx.clock();
        let mut tasks = Vec::new();
        for node in 0..NODES {
            let mut skew = ctx.rng();
            let millis = Duration::from_millis(below(&mut skew, 5));
            let step = if below(&mut skew, 2) == 0 {
                ClockStep::Back(millis)
            } else {
                ClockStep::Forward(millis)
            };
            let clock = base.skewed(&format!("node-{node}"), step);
            for local in 0..GENERATORS_PER_NODE {
                let generator = node * GENERATORS_PER_NODE + local;
                let mut ids = UlidGenerator::new(Arc::new(clock.clone()), ctx.rng());
                let mut pace = ctx.rng();
                let clock = clock.clone();
                let sender = sender.clone();
                tasks.push(ctx.spawn(&format!("generator-{generator}"), async move {
                    for _ in 0..IDS_PER_GENERATOR {
                        if below(&mut pace, 25) == 0 {
                            let back = below(&mut pace, 4) + 1;
                            clock.step(ClockStep::Back(Duration::from_millis(back)));
                        }
                        let id = ids.next_ulid().map_err(|error| error.to_string())?;
                        sender.send((generator, id)).map_err(|error| error.to_string())?;
                        let wait = below(&mut pace, 1_500);
                        tokio::time::sleep(Duration::from_micros(wait)).await;
                    }
                    Ok::<(), String>(())
                }));
            }
        }
        drop(sender);
        for task in tasks {
            task.join()
                .await
                .map_err(|error| CheckFailed::new(error.to_string()))?
                .map_err(CheckFailed::new)?;
        }
        let mut seen = BTreeSet::new();
        let mut last: BTreeMap<usize, u128> = BTreeMap::new();
        while let Some((generator, id)) = receiver.recv().await {
            ctx.check(seen.insert(id), || format!("{id:#x} minted twice"))?;
            if let Some(previous) = last.insert(generator, id) {
                ctx.check(id > previous, || {
                    format!("generator {generator} minted {id:#x} after {previous:#x}")
                })?;
            }
        }
        ctx.check(seen.len() == NODES * GENERATORS_PER_NODE * IDS_PER_GENERATOR, || {
            format!("{} ids", seen.len())
        })
    }
}
