//! The gateway pipeline as a detector: corpus exchanges go through
//! `Pipeline::ingest` and come back as `ExchangeCaptured` envelopes, in
//! order and at their corpus times; with no detection consumers yet, the run
//! reports the worlds unscored.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_eval::corpus::TraceSource;
use crosstalk_eval::datasets::salt::{SaltSource, Selection, load_world};
use crosstalk_eval::gateway::{PipelineDetector, ingest_world, subscribe};
use crosstalk_eval::pipeline::{DetectionStatus, Detector, run};
use crosstalk_eval::report::Report;
use crosstalk_eval::report::table::render;
use crosstalk_gateway::pipeline::{Deps, Pipeline, Settings};
use crosstalk_sim::CheckFailed;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::support::Clock;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/salt")
}

const MAIN: &str = "traces/main/main__fixture-model/rep001.json";

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

crosstalk_sim::sim_test! {
    /// A SALT world ingested under the simulation's clock: one
    /// `ExchangeCaptured` per corpus exchange, in corpus order, each naming
    /// its exchange, stamped at the exchange's corpus time, with strictly
    /// increasing envelope ids; the sim clock is never behind a corpus time.
    fn salt_world_ingests_in_order_at_corpus_times(ctx) {
        let world = load_world(&root(), Path::new(MAIN)).map_err(|e| failed("load", e))?;
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
        let published = ingest_world(&pipeline, &mut captured, &world, async |at| {
            let now = clock.now();
            if at > now {
                tokio::time::sleep(Duration::from_micros(at.as_micros() - now.as_micros())).await;
            }
            // Tokio's timer has millisecond resolution: the sim clock may
            // run up to a tick past a microsecond corpus time, never behind.
            let now = clock.now().as_micros();
            let within = now >= at.as_micros() && now - at.as_micros() < 2_000;
            if disagreed.is_none() && !within {
                disagreed = Some((clock.now(), at));
            }
        })
        .await
        .map_err(|e| failed("ingest", e))?;
        ctx.check(published.len() == world.exchanges().len(), || {
            format!("{} envelopes for {} exchanges", published.len(), world.exchanges().len())
        })?;
        for (envelope, exchange) in published.iter().zip(world.exchanges()) {
            ctx.check(envelope.exchange == exchange.id() && envelope.at == exchange.at(), || {
                format!("{envelope:?} is not exchange {:?} at {:?}", exchange.id(), exchange.at())
            })?;
        }
        ctx.check(published.windows(2).all(|w| w[0].event < w[1].event), || {
            "envelope ids are not strictly increasing".to_owned()
        })?;
        ctx.check(disagreed.is_none(), || {
            format!("the sim clock read {disagreed:?} (clock, corpus time)")
        })?;
        pipeline.shutdown(tokio::time::Instant::now() + Duration::from_secs(5)).await;
        Ok(())
    }
}

#[test]
fn the_pipeline_detector_ingests_and_leaves_worlds_unscored() {
    let mut source =
        SaltSource::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let mut detector = PipelineDetector::new(7).unwrap_or_else(|e| panic!("{e}"));
    let summary = run(&mut source, &mut detector, 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!(summary.unscored.worlds, 3);
    assert_eq!(summary.unscored.ingested, 37, "every fixture exchange");
    assert_eq!(summary.score.totals.worlds, 0, "nothing is scored as zero");
    let report = Report::new(
        source.id(),
        detector.name(),
        summary.score,
        vec![],
        vec![],
        summary.unscored,
    );
    assert!(render(&report).contains("no detector consumers yet"));
}

#[test]
fn a_pipeline_detection_says_why_it_is_unscored() {
    let world = load_world(&root(), Path::new(MAIN)).unwrap_or_else(|e| panic!("{e}"));
    let mut detector = PipelineDetector::new(1).unwrap_or_else(|e| panic!("{e}"));
    let detection = detector.detect(&world).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        detection.status,
        DetectionStatus::NoConsumers {
            ingested: world.exchanges().len() as u64
        }
    );
    assert!(detection.transmissions.is_empty());
}
