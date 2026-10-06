//! SALT-NLP "Emergent Collusion in Long-Horizon LLM Agent Interaction":
//! two agents (Alice and Bob) per trajectory exchanging messages through a
//! harness-mediated channel.
//!
//! One trace file (`traces/<exp>/<cond>/repNNN.json[.gz]`) is one world.
//! Each episode's calls are reconstructed per agent ([`episode`]), turned
//! into exchanges, and labelled ([`truth`]). The world's coverage is
//! complete at the construction tier: the only way the two agents talk is
//! the logged channel, so a prediction no label explains is a false positive.

pub mod episode;
pub mod files;
pub mod forwarding;
pub mod messages;
pub mod schema;
pub mod truth;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crosstalk_spec::observed::exchange::WireProtocol;
use flate2::read::GzDecoder;

pub use files::Selection;

use crate::corpus::clock::{ClockError, Pace};
use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, SourceError, TraceSource, World, WorldBuilder,
};
use crate::keys::{DatasetId, SourceRef, WorldKey};
use crate::location::LocationError;
use crate::truth::{InvalidLabel, Tier};
use episode::{AgentEpisode, EpisodeClock, episode_steps, reconstruct, stop_reason, token_usage};
use schema::Trace;
use truth::{EpisodeLabels, Labelled};

/// The dataset's id.
pub const DATASET: &str = "salt";

#[derive(Debug, thiserror::Error)]
pub enum SaltError {
    #[error("{root} has no traces/ directory")]
    NoTraces { root: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a SALT trace: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("a message has unknown role {0:?}")]
    UnknownRole(String),
    #[error("virtual clock: {0}")]
    Clock(#[source] ClockError),
    #[error("location: {0}")]
    Location(#[from] LocationError),
    #[error("label: {0}")]
    Label(#[from] InvalidLabel),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
}

/// SALT as a stream of worlds, one per trace file.
pub struct SaltSource {
    root: PathBuf,
    files: Vec<PathBuf>,
    pace: Pace,
}

impl SaltSource {
    /// The trace files under `root` that `selection` picks, in stratified
    /// order ([`files::discover`]).
    pub fn open(root: &Path, selection: &Selection) -> Result<Self, SaltError> {
        let files = files::discover(root, selection)?;
        Ok(Self {
            root: root.to_path_buf(),
            files,
            pace: Pace::DEFAULT,
        })
    }

    /// These worlds with calls `pace` apart.
    pub fn with_pace(mut self, pace: Pace) -> Self {
        self.pace = pace;
        self
    }

    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }
}

impl TraceSource for SaltSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let root = self.root.clone();
        let pace = self.pace;
        self.files
            .iter()
            .map(move |relative| load_world_paced(&root, relative, pace).map_err(SourceError::from))
    }
}

/// Reads and converts one trace file, calls [`Pace::DEFAULT`] apart.
pub fn load_world(root: &Path, relative: &Path) -> Result<World, SaltError> {
    load_world_paced(root, relative, Pace::DEFAULT)
}

/// Reads and converts one trace file, calls `pace` apart.
pub fn load_world_paced(root: &Path, relative: &Path, pace: Pace) -> Result<World, SaltError> {
    let path = root.join(relative);
    let bytes = read(&path)?;
    let trace: Trace = serde_json::from_slice(&bytes).map_err(|source| SaltError::Json {
        path: path.display().to_string(),
        source,
    })?;
    let file = relative.to_string_lossy().replace('\\', "/");
    convert_trace_paced(&trace, &file, pace)
}

fn read(path: &Path) -> Result<Vec<u8>, SaltError> {
    let io = |source| SaltError::Io {
        path: path.display().to_string(),
        source,
    };
    let raw = fs::read(path).map_err(io)?;
    if path.extension().is_some_and(|ext| ext == "gz") {
        let mut out = Vec::with_capacity(raw.len() * 8);
        GzDecoder::new(raw.as_slice())
            .read_to_end(&mut out)
            .map_err(io)?;
        Ok(out)
    } else {
        Ok(raw)
    }
}

/// Converts a parsed trace into a world named after `file`, calls
/// [`Pace::DEFAULT`] apart.
pub fn convert_trace(trace: &Trace, file: &str) -> Result<World, SaltError> {
    convert_trace_paced(trace, file, Pace::DEFAULT)
}

/// Converts a parsed trace into a world named after `file`, calls `pace`
/// apart.
pub fn convert_trace_paced(trace: &Trace, file: &str, pace: Pace) -> Result<World, SaltError> {
    let dataset = DatasetId::new(DATASET);
    let mut builder = WorldBuilder::new(dataset, WorldKey::new(files::world_name(Path::new(file))));
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for episode in &trace.results {
        names.extend(episode.agents.keys().map(String::as_str));
    }
    let mut keys = BTreeMap::new();
    for name in &names {
        let driven = trace
            .results
            .iter()
            .flat_map(|episode| &episode.llm_usage)
            .any(|usage| usage.actor == *name && usage.accepted());
        let model = trace
            .run_config
            .models
            .get(*name)
            .map_or("unknown", String::as_str);
        let driven = if driven {
            Driven::Model
        } else {
            Driven::Scripted
        };
        keys.insert(*name, (builder.agent(name, driven, model)?, driven));
    }
    let mut seen_system = BTreeSet::new();
    let mut episode_start = 0u64;
    for (position, episode) in trace.results.iter().enumerate() {
        let mut reconstructed: Vec<(String, AgentEpisode, Driven)> = Vec::new();
        for (name, (_, driven)) in &keys {
            reconstructed.push((
                name.to_string(),
                reconstruct(
                    name,
                    episode,
                    file,
                    *driven == Driven::Scripted,
                    EpisodeClock {
                        pace,
                        start: episode_start,
                    },
                )?,
                *driven,
            ));
        }
        let mut labelled = Vec::new();
        for (name, agent_episode, driven) in &reconstructed {
            let Some((key, _)) = keys.get(name.as_str()) else {
                continue;
            };
            let mut exchanges = Vec::new();
            if *driven == Driven::Model {
                let model = trace.run_config.models.get(name).cloned();
                for turn in &agent_episode.turns {
                    let model = turn
                        .usage
                        .as_ref()
                        .and_then(|usage| usage.requested_model.clone())
                        .or_else(|| model.clone())
                        .unwrap_or_else(|| "unknown".to_owned());
                    let draft = ExchangeDraft {
                        agent: key.clone(),
                        at: turn.at,
                        protocol: WireProtocol::OpenAiChat,
                        model,
                        request: agent_episode.messages[..turn.index].to_vec(),
                        response: agent_episode.messages[turn.index].clone(),
                        stop: stop_reason(
                            turn.usage.as_ref().and_then(|u| u.finish_reason.as_deref()),
                        ),
                        usage: turn.usage.as_ref().and_then(token_usage),
                        fidelity: agent_episode.fidelity,
                        source: SourceRef::new(
                            file,
                            format!("/results/{position}/agents/{name}/messages/{}", turn.index),
                        ),
                    };
                    exchanges.push(builder.exchange(draft)?);
                }
            }
            labelled.push(Labelled {
                key: key.clone(),
                episode: agent_episode,
                exchanges,
                scripted: *driven == Driven::Scripted,
            });
        }
        episode_start = episode_start.saturating_add(episode_steps(episode));
        let labels = EpisodeLabels {
            file,
            position,
            episode,
            agents: &labelled,
            seen_system: &mut seen_system,
        }
        .label()?;
        for label in labels {
            builder.expect(label);
        }
    }
    tracing::debug!(
        file,
        condition = trace.condition_id.as_deref().unwrap_or(""),
        episodes = trace.results.len(),
        "converted SALT trace"
    );
    Ok(builder.finish(Coverage::Complete {
        tier: Tier::Construction,
    }))
}
