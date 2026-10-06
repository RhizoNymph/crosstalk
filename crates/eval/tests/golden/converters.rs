//! Every converter's synthetic fixtures export, pass the format's checks
//! (`WorldInputs::new`, `check_labels`), and their reference predictions
//! pass `check_predictions`; every derived exchange id is the format's
//! `exchange_id` of the same dataset, source and time.

use a2a_bench_format::ids::{SourceRef, exchange_id};
use a2a_bench_format::labels::Label;
use a2a_bench_format::time::Timestamp;
use crosstalk_eval::corpus::{TraceSource, World};
use crosstalk_eval::datasets::agentdojo::{self, AgentDojoSource};
use crosstalk_eval::datasets::cipher::CipherSource;
use crosstalk_eval::datasets::lmcache::LmcacheSource;
use crosstalk_eval::datasets::open_swe::{Mixing, OpenSweSource};
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::datasets::swarm::{SwarmSelection, SwarmSource};
use crosstalk_eval::datasets::swe_splice::SpliceSource;
use crosstalk_eval::datasets::tau2::{self, Tau2Source};
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
use crosstalk_eval::golden::{Gap, GoldenError, ids};

use super::common::{dir, export_reference, fixtures, labels, worlds};

/// Every exchange of `worlds` has the id the format derives.
fn ids_are_derived(worlds: &[World]) {
    let mut checked = 0;
    for world in worlds {
        let dataset = ids::dataset(world.dataset()).unwrap_or_else(|e| panic!("{e}"));
        for exchange in world.exchanges() {
            let source = SourceRef::new(
                exchange.source().file.clone(),
                exchange.source().path.clone(),
            );
            let at = Timestamp::from_micros(exchange.at().as_micros());
            assert_eq!(
                exchange_id(&dataset, &source, at),
                ids::exchange(exchange.id()),
                "{} {}",
                world.key(),
                exchange.source()
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no exchange checked");
}

/// Exports `source` with reference predictions, checks the counts, and
/// checks a fresh copy's ids. Returns the label rows.
fn exported(
    name: &str,
    mut source: impl TraceSource,
    fresh: impl FnOnce() -> Vec<World>,
) -> Vec<(String, Vec<Label>)> {
    let out = dir(name);
    let (finished, verified) = export_reference(&mut source, &out);
    assert!(verified.worlds > 0, "{name}: no world");
    assert_eq!(verified.worlds, finished.manifest.worlds.len() as u64);
    assert!(verified.exchanges > 0, "{name}: no exchange");
    assert!(verified.predictions > 0, "{name}: no prediction row");
    ids_are_derived(&fresh());
    labels(&out)
}

/// How many truth rows (not `exchange_agent`) the label files hold.
fn truth_rows(labels: &[(String, Vec<Label>)]) -> usize {
    labels
        .iter()
        .flat_map(|(_, rows)| rows)
        .filter(|row| !matches!(row, Label::ExchangeAgent(_)))
        .count()
}

fn salt() -> SaltSource {
    SaltSource::open(&fixtures().join("salt"), &Selection::default())
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn salt_exports_and_checks() {
    let labels = exported("salt", salt(), || worlds(&mut salt()));
    assert!(truth_rows(&labels) > 0);
    let controls = labels
        .iter()
        .flat_map(|(_, rows)| rows)
        .filter(|row| matches!(row, Label::NegativeControl(_)))
        .count();
    assert!(controls > 0, "SALT's controls are exported");
}

fn agentdojo() -> AgentDojoSource {
    AgentDojoSource::open(
        &fixtures().join("agentdojo"),
        &agentdojo::Selection::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn agentdojo_exports_and_checks() {
    let labels = exported("agentdojo", agentdojo(), || worlds(&mut agentdojo()));
    assert!(truth_rows(&labels) > 0);
}

fn tau2() -> Tau2Source {
    Tau2Source::open(&fixtures().join("tau2"), &tau2::Selection::default())
        .unwrap_or_else(|e| panic!("{e}"))
}

/// τ²-bench labels a shared-source control on a record after the reader's
/// last call: a message no exchange carries, which a bench location cannot
/// name. The export stops on it; every exchange id is still derived.
#[test]
fn tau2_stops_on_a_control_no_exchange_carries() {
    let mut refused = 0;
    for world in worlds(&mut tau2()) {
        match crosstalk_eval::golden::export(&world) {
            Ok(export) => {
                export.check().unwrap_or_else(|e| panic!("{e}"));
            }
            Err(GoldenError::Unexpressible(Gap::UncarriedLocation { .. })) => refused += 1,
            Err(other) => panic!("{}: {other}", world.key()),
        }
    }
    assert!(refused > 0, "the fixture holds such a control");
    ids_are_derived(&worlds(&mut tau2()));
}

fn wiki() -> WikiSource {
    WikiSource::open(
        &fixtures().join("wiki/collusion-wiki"),
        &WikiSelection::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn wiki_exports_and_checks() {
    let labels = exported("wiki", wiki(), || worlds(&mut wiki()));
    assert!(truth_rows(&labels) > 0);
}

fn swarm_traces() -> SwarmSource {
    SwarmSource::open(
        &fixtures().join("swarm/swarm-traces"),
        &SwarmSelection::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn swarm_traces_export_and_check() {
    let labels = exported("swarm-traces", swarm_traces(), || {
        worlds(&mut swarm_traces())
    });
    assert!(truth_rows(&labels) > 0);
}

fn cipher() -> CipherSource {
    CipherSource::open(&fixtures().join("cipher"), &Selection::default(), 2, 0)
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn cipher_exports_and_checks() {
    let labels = exported("cipher", cipher(), || worlds(&mut cipher()));
    assert!(truth_rows(&labels) > 0);
}

fn splice() -> SpliceSource {
    SpliceSource::open(&fixtures().join("open_swe"), &Selection::default(), 4, 0)
        .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn swe_splice_exports_and_checks() {
    let labels = exported("swe-splice", splice(), || worlds(&mut splice()));
    assert!(truth_rows(&labels) > 0);
}

fn open_swe() -> OpenSweSource {
    OpenSweSource::open(
        &fixtures().join("open_swe"),
        &Selection::default(),
        Mixing {
            agents_per_world: 2,
            per_shard: None,
        },
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn open_swe_exports_and_checks() {
    exported("open-swe", open_swe(), || worlds(&mut open_swe()));
}

fn lmcache() -> LmcacheSource {
    LmcacheSource::open(
        &fixtures().join("lmcache"),
        &Selection::default(),
        Mixing::default(),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn lmcache_exports_and_checks() {
    exported("lmcache", lmcache(), || worlds(&mut lmcache()));
}
