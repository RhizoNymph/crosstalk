//! A demo-swarm bench run in the bench format: one world, its exchanges
//! from the gateway's exchange log (the gateway's minted ids, the harness
//! session and the request's ordinal in it as `client.session` and
//! `client.turn`) with their bodies from its blobs, its labels from the
//! swarm's truth file, and the gateway's saved export and evidence as
//! predictions.
//!
//! The world is ct-eval's ([`crate::datasets::swarm_truth::resolve()`]):
//! only the log's exchanges inside the run window, of sessions the truth
//! names, each its session owner's. Exchanges are in time order
//! (`started_at`, then id). Truth rows are `t<index>` as for every world.
//! A key group of two or more agents is an `agent_cluster` of kind
//! `key_group`, `k<key group>`, which the format cannot write today
//! ([`Gap::ClusterRow`]); a key group of one agent, which a bench cluster
//! cannot hold, is counted in `Lossy::single_agent_key_groups`.
//!
//! Predictions are the transmissions `detected::predictions` predicts from
//! (`detected::choose`). The gateway's export places no exchange under an
//! agent, so attribution is what ct-eval ties agents through: a confirmed
//! transmission's reader exchanges are its reader's, an access's exchange
//! is its canonical agent's (`AccessDetail::agent`), and an access's own
//! agent id is an alias of that canonical one. A sender that is neither a
//! reader nor an accessor holds no exchange and is `unattributed`, whose
//! predictions ct-eval drops as `unknown_detected_agent` too. A
//! transmission whose evidence lies outside the world is dropped and
//! counted, as ct-eval drops its predictions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::exchange::{AgentDecl, Driven, WorldDecl};
use bench::files::DetectorInfo;
use bench::labels::{AgentCluster, ClusterFields, ClusterKind, Tier};
use bench::predictions::WorldStatus;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::Message;

use super::manifest::{self, ManifestSpec};
use super::predictions::{self, Unlocated};
use super::run::{Finished, write_manifest};
use super::world::{Draft, WorldBuilder, WorldExport};
use super::writer::{ExportWriter, PredictionsWriter};
use super::{Gap, GoldenError, ids, kinds, labels};
use crate::corpus::{self, World};
use crate::datasets::swarm_truth::bodies::{BlobBodies, Bodies, Cached};
use crate::datasets::swarm_truth::detected::{
    Exported, SwarmDirectory, choose, read_evidence, read_export,
};
use crate::datasets::swarm_truth::exchange_log::{self, Sessions};
use crate::datasets::swarm_truth::schema::KeyGroup;
use crate::datasets::swarm_truth::truth_file::{self, TruthFile};
use crate::datasets::swarm_truth::window::{self, Margins, RunWindow};
use crate::datasets::swarm_truth::{
    AgentIndex, Diagnostics, Inputs, JoinFailure, SwarmTruthError, resolve,
};
use crate::keys::SourceRef;

/// The detector name a demo-swarm predictions file carries.
pub const DETECTOR: &str = "crosstalk-gateway-export";

/// Where a demo-swarm export goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outputs {
    /// The export directory; `None` derives the manifest without writing it.
    pub export: Option<PathBuf>,
    pub predictions: Option<PathBuf>,
    /// The gateway build that ran (its image digest, say).
    pub detector_version: String,
}

/// The run's world and what predictions need beside it.
struct Prepared {
    truth: TruthFile,
    world: World,
    agents: AgentIndex,
    key_groups: Vec<KeyGroup>,
    sessions: Sessions,
    outside: std::collections::HashSet<ExchangeId>,
}

fn open(path: &Path) -> Result<BufReader<File>, GoldenError> {
    File::open(path)
        .map(BufReader::new)
        .map_err(|source| GoldenError::io(path, source))
}

fn shown(path: &Path) -> String {
    path.display().to_string()
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}

fn prepare<B: Bodies>(
    inputs: &Inputs,
    margins: Margins,
    bodies: &mut Cached<B>,
) -> Result<Prepared, GoldenError> {
    let truth =
        truth_file::read(open(&inputs.truth)?).map_err(|source| SwarmTruthError::Truth {
            path: shown(&inputs.truth),
            source,
        })?;
    let log = exchange_log::read(&inputs.exchanges).map_err(SwarmTruthError::from)?;
    let run_window = RunWindow::of(&truth, margins);
    let split = window::split(log.exchanges, run_window, &window::truth_sessions(&truth));
    let sessions = Sessions::index(split.inside);
    let resolved = resolve(&truth, &file_name(&inputs.truth), &sessions, bodies)
        .map_err(SwarmTruthError::from)?;
    Ok(Prepared {
        truth,
        world: resolved.world,
        agents: resolved.agents,
        key_groups: resolved.key_groups,
        sessions,
        outside: split.outside,
    })
}

/// The world's exchanges: each owned session's, with its ordinal, in time
/// order.
fn owned(prepared: &Prepared) -> Vec<(&Exchange, String, u32)> {
    let mut out = Vec::new();
    for (session, found) in prepared.sessions.iter() {
        let Some(agent) = prepared.agents.sessions.get(session) else {
            continue;
        };
        for (turn, exchange) in found.exchanges.iter().enumerate() {
            out.push((
                exchange,
                agent.name.clone(),
                u32::try_from(turn).unwrap_or(u32::MAX),
            ));
        }
    }
    out.sort_by_key(|(exchange, _, _)| (exchange.meta.started_at, exchange.meta.id));
    out
}

fn export_world<B: Bodies>(
    prepared: &Prepared,
    truth_name: &str,
    log_name: &str,
    bodies: &mut Cached<B>,
) -> Result<WorldExport, GoldenError> {
    let mut builder = WorldBuilder::default();
    for (exchange, agent, turn) in owned(prepared) {
        let mut hashes: Vec<MessageHash> = exchange.request.clone();
        match &exchange.outcome {
            ExchangeOutcome::Completed { response, .. } => hashes.push(*response),
            ExchangeOutcome::Failed {
                partial_response, ..
            } => hashes.extend(partial_response.iter().copied()),
        }
        let mut messages: HashMap<MessageHash, Message> = HashMap::new();
        for hash in hashes {
            if let Ok(message) = bodies.get(hash) {
                messages.insert(hash, message.clone());
            }
        }
        let source = SourceRef::new(log_name, format!("/{}", exchange.meta.id.ulid_text()));
        builder.exchange(
            Draft {
                exchange,
                agent: &agent,
                at: exchange.meta.started_at,
                fidelity: corpus::Fidelity::Exact,
                source: &source,
                turn: Some(turn),
            },
            |hash| messages.get(&hash),
        )?;
    }
    let world = &prepared.world;
    let key = ids::world(world.key())?;
    let decl = WorldDecl {
        key: key.clone(),
        agents: world
            .agents()
            .iter()
            .map(|agent| {
                Ok(AgentDecl {
                    key: ids::agent(&agent.key.name)?,
                    driven: Driven::Model,
                    model: Some(agent.model.clone()),
                })
            })
            .collect::<Result<_, GoldenError>>()?,
    };
    let mut rows = labels::exchange_agents(builder.owners())?;
    rows.extend(labels::truth(world.key(), world.truth(), builder.index())?);
    let mut single = 0;
    for group in &prepared.key_groups {
        let agents = group
            .agents
            .iter()
            .map(|agent| ids::agent(agent))
            .collect::<Result<Vec<_>, _>>()?;
        if agents.len() < 2 {
            single += 1;
            continue;
        }
        let id = ids::label(format!("k{}", group.key_group))?;
        AgentCluster::new(ClusterFields {
            id: id.clone(),
            agents,
            kind: ClusterKind::KeyGroup,
            tier: Tier::Construction,
            source: bench::ids::SourceRef::new(
                truth_name,
                format!("/key_group/{}", group.key_group),
            ),
        })
        .map_err(|source| GoldenError::Label {
            label: id.to_string(),
            source,
        })?;
        return Err(GoldenError::Unexpressible(Gap::ClusterRow {
            label: id.to_string(),
        }));
    }
    let mut export = builder.finish(key, decl, rows, kinds::coverage(world.coverage()));
    export.lossy.single_agent_key_groups += single;
    Ok(export)
}

/// The agents ct-eval ties through the evidence (module docs): each
/// exported exchange's canonical agent, and every alias's canonical id.
type Ties = (BTreeMap<ExchangeId, AgentId>, BTreeMap<AgentId, AgentId>);

fn ties(
    chosen: &[&crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence],
    export: &WorldExport,
) -> Result<Ties, GoldenError> {
    let exported: BTreeSet<bench::ids::ExchangeId> = export
        .exchanges
        .iter()
        .map(|exchange| exchange.id)
        .collect();
    let mut aliases = BTreeMap::new();
    for item in chosen {
        for detail in item.accesses() {
            if detail.access().agent != detail.agent() {
                aliases.insert(detail.access().agent, detail.agent());
            }
        }
    }
    let canonical = |agent: AgentId| aliases.get(&agent).copied().unwrap_or(agent);
    let mut claims: BTreeMap<ExchangeId, BTreeSet<AgentId>> = BTreeMap::new();
    for item in chosen {
        let transmission = item.transmission();
        if let Some(confirmed) = transmission.state.confirmed() {
            for content in confirmed.content().iter() {
                claims
                    .entry(content.reader_exchange())
                    .or_default()
                    .insert(canonical(transmission.to));
            }
        }
        for detail in item.accesses() {
            claims
                .entry(detail.access().exchange)
                .or_default()
                .insert(canonical(detail.agent()));
        }
    }
    let mut attribution = BTreeMap::new();
    for (exchange, agents) in claims {
        if !exported.contains(&ids::exchange(exchange)) {
            continue;
        }
        let mut agents = agents.into_iter();
        match (agents.next(), agents.next()) {
            (Some(agent), None) => {
                attribution.insert(exchange, agent);
            }
            (Some(first), Some(second)) => {
                return Err(GoldenError::Unexpressible(Gap::ExchangeTiedTwice {
                    exchange,
                    first,
                    second,
                }));
            }
            (None, _) => {}
        }
    }
    Ok((attribution, aliases))
}

/// The selection a demo-swarm export pins: the run window's margins.
pub fn selection(margins: Margins) -> BTreeMap<String, bench::manifest::Setting> {
    BTreeMap::from([
        ("run_lead_ms".to_owned(), manifest::int(margins.lead_ms)),
        ("run_slack_ms".to_owned(), manifest::int(margins.slack_ms)),
    ])
}

/// Exports the run `inputs` names (module docs). The export's `--export`
/// and `--evidence` files are read only when predictions are wanted.
pub fn export(
    inputs: &Inputs,
    margins: Margins,
    outputs: &Outputs,
) -> Result<Finished, GoldenError> {
    let mut bodies = Cached::new(BlobBodies::open(&inputs.blobs).map_err(SwarmTruthError::from)?);
    let prepared = prepare(inputs, margins, &mut bodies)?;
    let export = export_world(
        &prepared,
        &file_name(&inputs.truth),
        &file_name(&inputs.exchanges),
        &mut bodies,
    )?;
    let dataset = ids::dataset(prepared.world.dataset())?;
    let spec = ManifestSpec {
        dataset: dataset.clone(),
        source: bench::manifest::Source {
            path: inputs.truth.parent().map(file_name).unwrap_or_default(),
            revision: prepared.truth.header.run.clone(),
            digest: manifest::digest_files(&[
                (file_name(&inputs.truth), inputs.truth.clone()),
                (file_name(&inputs.exchanges), inputs.exchanges.clone()),
            ])?,
        },
        converter: manifest::converter(),
        selection: selection(margins),
        pace: BTreeMap::new(),
    };
    match &outputs.export {
        Some(dir) => {
            let writer = ExportWriter::create(dir, &dataset)?;
            let finished = finish(
                writer,
                &export,
                inputs,
                &prepared,
                outputs,
                &spec,
                &mut bodies,
            )?;
            write_manifest(dir, &finished.manifest)?;
            Ok(finished)
        }
        None => {
            let writer = ExportWriter::sink(&dataset)?;
            finish(
                writer,
                &export,
                inputs,
                &prepared,
                outputs,
                &spec,
                &mut bodies,
            )
        }
    }
}

fn finish<W: Write, B: Bodies>(
    mut writer: ExportWriter<W>,
    export: &WorldExport,
    inputs: &Inputs,
    prepared: &Prepared,
    outputs: &Outputs,
    spec: &ManifestSpec,
    bodies: &mut Cached<B>,
) -> Result<Finished, GoldenError> {
    let world_inputs = writer.world(export)?;
    let mut lossy = export.lossy;
    let mut predictions = None;
    if let Some(path) = &outputs.predictions {
        let mut out = PredictionsWriter::create(path)?;
        let exported: Exported = read_export(
            &std::fs::read(&inputs.export)
                .map_err(|source| GoldenError::io(&inputs.export, source))?,
        )
        .map_err(|source| SwarmTruthError::Detected {
            path: shown(&inputs.export),
            source,
        })?;
        let evidence =
            read_evidence(open(&inputs.evidence)?).map_err(|source| SwarmTruthError::Detected {
                path: shown(&inputs.evidence),
                source,
            })?;
        let mut diagnostics = Diagnostics::default();
        let chosen = choose(&exported, &evidence, &prepared.outside, &mut diagnostics);
        let directory = SwarmDirectory::learn(&chosen, &prepared.agents, bodies, &mut diagnostics);
        lossy.agent_conflicts += diagnostics
            .entries
            .iter()
            .filter(|entry| matches!(entry.failure, JoinFailure::DetectedAgentConflict { .. }))
            .count() as u64;
        let (attribution, aliases) = ties(&chosen, export)?;
        let transmissions: Vec<_> = chosen
            .iter()
            .map(|item| item.transmission().clone())
            .collect();
        let rows = predictions::rows(
            &transmissions,
            &directory,
            &attribution,
            &aliases,
            Unlocated::Drop,
            &export.index,
            &mut lossy,
        )?;
        out.world(&world_inputs, WorldStatus::Scored, &rows)?;
        predictions = Some(out);
    }
    let written = writer.finish()?;
    let manifest = spec.manifest(&written);
    let predictions = match predictions {
        Some(out) => Some(out.finish(&bench::files::PredictionsHeader::new(
            spec.dataset.clone(),
            DetectorInfo {
                name: DETECTOR.to_owned(),
                version: outputs.detector_version.clone(),
                variant: "default".to_owned(),
                config_digest: None,
            },
            super::writer::manifest_digest(&manifest)?,
        ))?),
        None => None,
    };
    Ok(Finished {
        manifest,
        written,
        predictions,
        lossy,
    })
}
