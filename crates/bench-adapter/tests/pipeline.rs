//! The gateway pipeline as a detector (`--mode pipeline`): a bench world's
//! exchanges go through `Pipeline::ingest` and come back as
//! `ExchangeCaptured` envelopes, in order and at their world times; with
//! no detection consumers on that path, every world is written
//! `no_consumers { ingested }`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_bench_adapter::convert::{self, ConvertedWorld};
use crosstalk_bench_adapter::gateway::{PipelineDetector, ingest_exchanges, subscribe};
use crosstalk_bench_adapter::input::{InputDir, WorldRead};
use crosstalk_bench_adapter::run::pipeline_world;
use crosstalk_gateway::pipeline::{Deps, Pipeline, Settings};
use crosstalk_sim::CheckFailed;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::support::Clock;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};

fn salt() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bench/salt")
}

/// The SALT input's world `key`, converted.
const MAIN: &str = "main/main__fixture-model/rep001";

fn converted(key: &str) -> Result<ConvertedWorld, String> {
    let mut dir = InputDir::open(&salt()).map_err(|e| e.to_string())?;
    let dataset = dir.manifest().dataset.clone();
    while let Some(read) = dir.next_world().map_err(|e| e.to_string())? {
        let WorldRead::Ready(inputs) = read else {
            return Err("a fixture world does not check".to_owned());
        };
        if inputs.decl().key.as_str() == key {
            return convert::world(&dataset, &inputs).map_err(|e| e.to_string());
        }
    }
    Err(format!("no world {key}"))
}

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

crosstalk_sim::sim_test! {
    /// A SALT world ingested under the simulation's clock: one
    /// `ExchangeCaptured` per exchange, in world order, each naming its
    /// exchange, stamped at the exchange's `at_us`, with strictly
    /// increasing envelope ids; the sim clock is never behind a world time.
    fn salt_world_ingests_in_order_at_world_times(ctx) {
        let world = converted(MAIN).map_err(|e| failed("load", e))?;
        let timed = world.timed();
        let clock = ctx.clock();
        let bus = MpscBus::start(BusConfig::default()).map_err(|e| failed("bus", e))?;
        let pipeline = Pipeline::build(
            Settings::default(),
            Deps::stores(MemoryBlobStore::new(), bus, SeededRandom::new(ctx.seed().get())),
            Arc::new(clock.clone()) as Arc<dyn Clock>,
        )
        .await
        .map_err(|e| failed("build", e))?;
        let mut captured = subscribe(pipeline.bus()).await.map_err(|e| failed("subscribe", e))?;
        ctx.step("ingest");
        let mut disagreed = None;
        let published = ingest_exchanges(&pipeline, &mut captured, &timed, async |at| {
            let now = clock.now();
            if at > now {
                tokio::time::sleep(Duration::from_micros(at.as_micros() - now.as_micros())).await;
            }
            // Tokio's timer has millisecond resolution: the sim clock may
            // run up to a tick past a microsecond world time, never behind.
            let now = clock.now().as_micros();
            let within = now >= at.as_micros() && now - at.as_micros() < 2_000;
            if disagreed.is_none() && !within {
                disagreed = Some((clock.now(), at));
            }
        })
        .await
        .map_err(|e| failed("ingest", e))?;
        ctx.check(published.len() == timed.len(), || {
            format!("{} envelopes for {} exchanges", published.len(), timed.len())
        })?;
        for (envelope, exchange) in published.iter().zip(&timed) {
            let id = exchange.exchange.exchange.meta.id;
            ctx.check(envelope.exchange == id && envelope.at == exchange.at, || {
                format!("{envelope:?} is not exchange {id:?} at {:?}", exchange.at)
            })?;
        }
        ctx.check(published.windows(2).all(|w| w[0].event < w[1].event), || {
            "envelope ids are not strictly increasing".to_owned()
        })?;
        ctx.check(disagreed.is_none(), || {
            format!("the sim clock read {disagreed:?} (clock, world time)")
        })?;
        pipeline.shutdown(tokio::time::Instant::now() + Duration::from_secs(5)).await;
        Ok(())
    }
}

#[test]
fn the_pipeline_detector_ingests_every_world() {
    let mut dir = InputDir::open(&salt()).unwrap_or_else(|e| panic!("{e}"));
    let dataset = dir.manifest().dataset.clone();
    let mut detector = PipelineDetector::new(7).unwrap_or_else(|e| panic!("{e}"));
    let (mut worlds, mut ingested) = (0, 0);
    while let Some(read) = dir.next_world().unwrap_or_else(|e| panic!("{e}")) {
        let WorldRead::Ready(inputs) = read else {
            panic!("a fixture world does not check");
        };
        worlds += 1;
        ingested +=
            pipeline_world(&mut detector, &dataset, &inputs).unwrap_or_else(|e| panic!("{e}"));
    }
    assert_eq!(worlds, 3);
    assert_eq!(ingested, 37, "every fixture exchange");
}

#[test]
fn a_pipeline_world_counts_what_it_ingested() {
    let world = converted(MAIN).unwrap_or_else(|e| panic!("{e}"));
    let mut detector = PipelineDetector::new(1).unwrap_or_else(|e| panic!("{e}"));
    let ingested = detector
        .ingest(&world.timed())
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(ingested, world.exchanges.len() as u64);
    assert_eq!(ingested, 17);
}
