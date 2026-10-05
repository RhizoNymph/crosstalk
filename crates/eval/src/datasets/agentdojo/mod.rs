//! AgentDojo (prompt-injection attacks on tool-using agents): one agent per
//! run, attacked through injections planted in its tools' data.
//!
//! One run file (`runs/<pipeline>/<suite>/<task>/<attack>/<file>.json`) is
//! one world:
//!
//! - **victim**: the pipeline's model. Its exchange `k` is the prefix of the
//!   conversation up to assistant message `k`. Tool schemas are not recorded
//!   and are left out.
//! - **attacker** (attacked runs only): a synthetic agent standing for the
//!   injections' author, with one `Synthetic` exchange before the victim's
//!   first whose response writes every injection.
//!
//! Labels are in [`truth`]; how each injection arrived is in [`classify`].
//! Coverage is complete at the construction tier: the injections are the
//! only content the attacker contributes, so a prediction no label explains
//! is a false positive. Runs without an attack (`none`), including the
//! `injection_task_*` runs whose user prompt is the attacker's goal, have no
//! attacker and no labels.

pub mod classify;
pub mod files;
pub mod messages;
pub mod route;
pub mod schema;
pub mod tally;
pub mod truth;

use std::fs;
use std::path::{Path, PathBuf};

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody, Text, UserPart};

pub use files::{RunFile, Selection};
pub use tally::Tally;

use crate::corpus::clock::{ClockError, compose};
use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, SourceError,
    TraceSource, World, WorldBuilder,
};
use crate::keys::{DatasetId, SourceRef, WorldKey};
use crate::location::LocationError;
use crate::truth::{InvalidLabel, Tier};
use schema::Run;
use truth::{Attacker, RunLabels};

/// The dataset's id.
pub const DATASET: &str = "agentdojo";
/// The victim agent's name in every world.
pub const VICTIM: &str = "victim";
/// The synthetic attacker's name in attacked worlds.
pub const ATTACKER: &str = "attacker";

/// Pipeline-name suffixes naming a defense, not a model.
const DEFENSES: &[&str] = &[
    "-repeat_user_prompt",
    "-spotlighting_with_delimiting",
    "-tool_filter",
    "-transformers_pi_detector",
];

#[derive(Debug, thiserror::Error)]
pub enum AgentDojoError {
    #[error("{root} has no runs/ directory")]
    NoRuns { root: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not an AgentDojo run: {source}")]
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

/// A converted run and what its conversion counted.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub world: World,
    pub tally: Tally,
}

/// AgentDojo as a stream of worlds, one per run file. The tally of every
/// run converted so far is kept for the report.
pub struct AgentDojoSource {
    root: PathBuf,
    files: Vec<RunFile>,
    tally: Tally,
}

impl AgentDojoSource {
    /// The run files under `root` that `selection` picks
    /// ([`files::discover`]).
    pub fn open(root: &Path, selection: &Selection) -> Result<Self, AgentDojoError> {
        Ok(Self {
            root: root.to_path_buf(),
            files: files::discover(root, selection)?,
            tally: Tally::default(),
        })
    }

    pub fn files(&self) -> &[RunFile] {
        &self.files
    }

    /// What the runs converted so far counted.
    pub fn tally(&self) -> Tally {
        self.tally
    }
}

impl TraceSource for AgentDojoSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let root = &self.root;
        let tally = &mut self.tally;
        self.files.iter().map(move |file| {
            let loaded = load_world(root, &file.relative)?;
            tally.add(&loaded.tally);
            Ok(loaded.world)
        })
    }
}

/// Reads and converts one run file.
pub fn load_world(root: &Path, relative: &Path) -> Result<Loaded, AgentDojoError> {
    let path = root.join(relative);
    let bytes = fs::read(&path).map_err(|source| AgentDojoError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let run: Run = serde_json::from_slice(&bytes).map_err(|source| AgentDojoError::Json {
        path: path.display().to_string(),
        source,
    })?;
    let file = relative.to_string_lossy().replace('\\', "/");
    convert_run(&run, &file)
}

/// The model behind a pipeline name: the name without a defense suffix.
pub fn model_of(pipeline: &str) -> &str {
    DEFENSES
        .iter()
        .find_map(|suffix| pipeline.strip_suffix(suffix))
        .unwrap_or(pipeline)
}

/// Converts a parsed run into a world named after `file`.
pub fn convert_run(run: &Run, file: &str) -> Result<Loaded, AgentDojoError> {
    let dataset = DatasetId::new(DATASET);
    let mut builder = WorldBuilder::new(dataset, WorldKey::new(files::world_name(Path::new(file))));
    let model = model_of(&run.pipeline_name);
    let victim = builder.agent(VICTIM, Driven::Model, model)?;
    let conversation = messages::convert(&run.messages)?;
    let attacker = if run.attacked() {
        let key = builder.agent(ATTACKER, Driven::Model, "agentdojo-attacker")?;
        let exchange = builder.exchange(attacker_draft(run, &key, file)?)?;
        Some((key, exchange))
    } else {
        None
    };
    let mut exchanges = Vec::new();
    for (index, raw) in run.messages.iter().enumerate() {
        if raw.role != "assistant" {
            continue;
        }
        let at = compose(0, index as u64 + 1, 0).map_err(AgentDojoError::Clock)?;
        let draft = ExchangeDraft {
            agent: victim.clone(),
            at,
            protocol: WireProtocol::OpenAiChat,
            model: model.to_owned(),
            request: conversation.messages[..index].to_vec(),
            response: conversation.messages[index].clone(),
            stop: if raw.calls().is_empty() {
                StopReason::EndTurn
            } else {
                StopReason::ToolUse
            },
            usage: None,
            fidelity: Fidelity::Reconstructed,
            source: SourceRef::new(file, format!("/messages/{index}")),
        };
        exchanges.push((index, builder.exchange(draft)?));
    }
    let mut tally = Tally::default();
    let labels = RunLabels {
        file,
        run,
        conversation: &conversation,
        victim: &victim,
        attacker: attacker.as_ref().map(|(key, exchange)| Attacker {
            key,
            exchange: *exchange,
        }),
        exchanges: &exchanges,
    }
    .label(&mut tally)?;
    for label in labels {
        builder.expect(label);
    }
    tracing::debug!(
        file,
        attack = run.attack_type.as_deref().unwrap_or("none"),
        messages = run.messages.len(),
        "converted AgentDojo run"
    );
    Ok(Loaded {
        world: builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        tally,
    })
}

/// The attacker's one exchange: asked for the injections, it writes each
/// (in vector order) as one text part.
fn attacker_draft(
    run: &Run,
    attacker: &crate::keys::AgentKey,
    file: &str,
) -> Result<ExchangeDraft, AgentDojoError> {
    let attack = run.attack_type.as_deref().unwrap_or("unknown");
    let goal = run.injection_task_id.as_deref().unwrap_or("unknown");
    let request = HashedMessage::new(MessageBody::User(vec![UserPart::Text(Text(format!(
        "Write the {attack} injections for {goal} against {} in the {} suite.",
        run.user_task_id, run.suite_name
    )))]));
    let parts = run
        .injections()
        .filter(|(_, injection)| !injection.is_empty())
        .map(|(_, injection)| AssistantPart::Text(Text(injection.clone())))
        .collect();
    Ok(ExchangeDraft {
        agent: attacker.clone(),
        at: compose(0, 0, 0).map_err(AgentDojoError::Clock)?,
        protocol: WireProtocol::OpenAiChat,
        model: format!("agentdojo-attack/{attack}"),
        request: vec![request],
        response: HashedMessage::new(MessageBody::Assistant(parts)),
        stop: StopReason::EndTurn,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: SourceRef::new(file, "/injections"),
    })
}
