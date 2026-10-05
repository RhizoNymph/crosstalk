//! τ²-bench (customer-service agents and an LLM user simulator): two model
//! agents per simulation, talking turn by turn.
//!
//! One simulation of a results file is one world:
//!
//! - **agent**: the agent model. Its view ([`views`]) is its reconstructed
//!   system prompt ([`prompts`]), the conversation, and its own tools.
//! - **user** (absent when the agent works alone, `dummy_user`): the user
//!   simulator, seeing the conversation with roles flipped and its own
//!   tools.
//!
//! Every assistant or user record with `raw_data` is one model call, an
//! exchange at its recorded time; the agent's hard-coded greeting is not.
//! Labels are in [`truth`]. There are no attacks: this is a benign baseline
//! for precision. Coverage is complete at the structural tier: the turns
//! are the only way the two talk.

pub mod files;
pub mod prompts;
pub mod schema;
pub mod time;
pub mod truth;
pub mod views;

use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crosstalk_spec::observed::exchange::{StopReason, TokenCounts, TokenUsage, WireProtocol};
use crosstalk_spec::support::Timestamp;

pub use files::Selection;

use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, Fidelity, SourceError, TraceSource, World,
    WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::location::LocationError;
use crate::truth::{InvalidLabel, Tier};
use schema::{RawMessage, Results, Usage};
use time::{TimeError, parse_time};
use truth::{Participant, SimulationLabels};
use views::{Side, View, view};

/// The dataset's id.
pub const DATASET: &str = "tau2";
/// The agent's name in every world.
pub const AGENT: &str = "agent";
/// The user simulator's name in every world that has one.
pub const USER: &str = "user";

#[derive(Debug, thiserror::Error)]
pub enum Tau2Error {
    #[error("{root} is not a directory of results files")]
    NoResults { root: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a τ²-bench results file: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("simulation {0} does not exist")]
    NoSimulation(usize),
    #[error("simulation {simulation} names unknown task {task:?}")]
    UnknownTask { simulation: usize, task: String },
    #[error("a message has unknown role {0:?}")]
    UnknownRole(String),
    #[error("message {0} is a model call without a timestamp")]
    MissingTime(usize),
    #[error("time: {0}")]
    Time(#[from] TimeError),
    #[error("location: {0}")]
    Location(#[from] LocationError),
    #[error("label: {0}")]
    Label(#[from] InvalidLabel),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
}

/// τ²-bench as a stream of worlds, one per simulation. One results file is
/// parsed at a time.
pub struct Tau2Source {
    root: PathBuf,
    files: Vec<PathBuf>,
    limit: Option<usize>,
}

impl Tau2Source {
    /// The results files under `root` that `selection` picks.
    pub fn open(root: &Path, selection: &Selection) -> Result<Self, Tau2Error> {
        Ok(Self {
            root: root.to_path_buf(),
            files: files::discover(root, selection)?,
            limit: selection.limit,
        })
    }

    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }
}

type Worlds<'a> = Box<dyn Iterator<Item = Result<World, SourceError>> + 'a>;

impl TraceSource for Tau2Source {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let root = &self.root;
        let quota = files::quota(self.limit, self.files.len());
        self.files
            .iter()
            .flat_map(move |relative| -> Worlds<'_> {
                let results = match load_results(root, relative) {
                    Ok(results) => Rc::new(results),
                    Err(error) => return Box::new(std::iter::once(Err(error.into()))),
                };
                let file = relative.to_string_lossy().into_owned();
                let picks = files::pick(results.simulations.len(), quota);
                Box::new(picks.into_iter().map(move |index| {
                    convert_simulation(&results, &file, index).map_err(SourceError::from)
                }))
            })
            .take(self.limit.unwrap_or(usize::MAX))
    }
}

/// Reads one results file.
pub fn load_results(root: &Path, relative: &Path) -> Result<Results, Tau2Error> {
    let path = root.join(relative);
    let bytes = fs::read(&path).map_err(|source| Tau2Error::Io {
        path: path.display().to_string(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| Tau2Error::Json {
        path: path.display().to_string(),
        source,
    })
}

/// Converts simulation `index` of a parsed results file named `file`.
pub fn convert_simulation(results: &Results, file: &str, index: usize) -> Result<World, Tau2Error> {
    let simulation = results
        .simulations
        .get(index)
        .ok_or(Tau2Error::NoSimulation(index))?;
    let task = results
        .tasks
        .iter()
        .find(|task| task.id == simulation.task_id)
        .ok_or_else(|| Tau2Error::UnknownTask {
            simulation: index,
            task: simulation.task_id.clone(),
        })?;
    let info = &results.info;
    let messages = &simulation.messages;
    let mut builder = WorldBuilder::new(
        DatasetId::new(DATASET),
        WorldKey::new(files::world_name(file, index)),
    );
    let agent_model = info.agent_info.llm.as_deref().unwrap_or("unknown");
    let user_model = info.user_info.llm.as_deref().unwrap_or("unknown");
    let agent_key = builder.agent(AGENT, Driven::Model, agent_model)?;
    let has_user = info.user_info.implementation != "dummy_user"
        || messages.iter().any(|message| message.role == "user");
    let user_key = if has_user {
        Some(builder.agent(USER, Driven::Model, user_model)?)
    } else {
        None
    };
    let (agent_prompt, agent_fidelity) = prompts::agent_system_prompt(info, task);
    let user_prompt = prompts::user_system_prompt(info, task);
    let side = Sim {
        file,
        index,
        messages,
    };
    let agent = side.participant(
        &mut builder,
        Side::Agent,
        agent_key,
        view(Side::Agent, messages, agent_prompt.clone())?,
        agent_model,
        agent_fidelity,
    )?;
    let user = match user_key {
        Some(key) => Some(side.participant(
            &mut builder,
            Side::User,
            key,
            view(Side::User, messages, user_prompt.clone())?,
            user_model,
            Fidelity::Reconstructed,
        )?),
        None => None,
    };
    let labels = SimulationLabels {
        file,
        simulation: index,
        messages,
        agent: &agent,
        user: user.as_ref(),
        agent_prompt: &agent_prompt,
        user_prompt: &user_prompt,
    }
    .label()?;
    for label in labels {
        builder.expect(label);
    }
    Ok(builder.finish(Coverage::Complete {
        tier: Tier::Structural,
    }))
}

/// One simulation's records, for building each side's exchanges.
struct Sim<'a> {
    file: &'a str,
    index: usize,
    messages: &'a [RawMessage],
}

impl Sim<'_> {
    /// Adds `side`'s exchanges (one per model call it made) and returns the
    /// participant.
    fn participant(
        &self,
        builder: &mut WorldBuilder,
        side: Side,
        key: AgentKey,
        view: View,
        model: &str,
        fidelity: Fidelity,
    ) -> Result<Participant, Tau2Error> {
        let mut exchanges = Vec::new();
        let mut last: Option<Timestamp> = None;
        for (position, entry) in view.entries.iter().enumerate() {
            let Some(message) = self.messages.get(entry.raw) else {
                continue;
            };
            if message.role != side.role() || !message.is_model_call() {
                continue;
            }
            let recorded = message
                .timestamp
                .as_deref()
                .ok_or(Tau2Error::MissingTime(entry.raw))?;
            let mut at = parse_time(recorded)?;
            // Times are recorded to the microsecond; keep each agent's
            // strictly increasing.
            if let Some(last) = last
                && at <= last
            {
                at = Timestamp::from_micros(last.as_micros() + 1);
            }
            last = Some(at);
            let draft = ExchangeDraft {
                agent: key.clone(),
                at,
                protocol: WireProtocol::OpenAiChat,
                model: model.to_owned(),
                request: view.request(position),
                response: entry.message.clone(),
                stop: stop_reason(message.finish_reason()),
                usage: message.usage.as_ref().and_then(token_usage),
                fidelity,
                source: SourceRef::new(
                    self.file,
                    format!("/simulations/{}/messages/{}", self.index, entry.raw),
                ),
            };
            exchanges.push((entry.raw, builder.exchange(draft)?));
        }
        Ok(Participant {
            key,
            view,
            exchanges,
        })
    }
}

fn stop_reason(finish: Option<&str>) -> StopReason {
    match finish {
        Some("stop") => StopReason::EndTurn,
        Some("tool_calls") | Some("function_call") => StopReason::ToolUse,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::Refusal,
        _ => StopReason::Other,
    }
}

fn token_usage(usage: &Usage) -> Option<TokenUsage> {
    if usage.prompt_tokens.is_none() && usage.completion_tokens.is_none() {
        return None;
    }
    let clamp = |n: Option<u64>| u32::try_from(n.unwrap_or(0)).unwrap_or(u32::MAX);
    TokenUsage::new(TokenCounts {
        input: clamp(usage.prompt_tokens),
        output: clamp(usage.completion_tokens),
        cache_read: 0,
        cache_write: None,
        reasoning: None,
    })
    .ok()
}
