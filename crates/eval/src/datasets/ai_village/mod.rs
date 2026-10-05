//! AI Village (AI Digest): agents on frontier models living together in a
//! long-running village, with computers, a shared chat and memories.
//!
//! The dataset (`~/Data/ai/agents/ai-village`, gzipped JSON Lines; the
//! screenshot tars are never read) records model **responses** only:
//! `llm_calls` is withheld. Two sources come out of it:
//!
//! - [`Mode::ClaudeCode`] ([`claude_code`]): the one agent that ran the
//!   Claude Agent SDK, whose SDK entries give exact call boundaries and the
//!   tool results it read. The chat it read through the village MCP
//!   server's `get_events` tool is keyed by event id, so those labels are
//!   construction-tier. One world per context.
//! - [`Mode::Window`] ([`window`]): every standard agent over a window of
//!   village days, with requests rebuilt from the responses, structural
//!   chat labels and heuristic repository labels. One world per day.
//!
//! The streaming passes ([`tables`], [`stream`]), the resource normalizer
//! ([`resource`]) and the bash access tagger ([`access`]) are reusable on
//! their own.

pub mod access;
pub mod claude_code;
pub mod provider;
pub mod report;
pub mod resource;
pub mod rooms;
pub mod schema;
pub mod stream;
pub mod tables;
pub mod text;
pub mod time;
pub mod window;

use std::path::Path;

use crate::corpus::{CorpusError, SourceError, TraceSource, World};
use crate::keys::DatasetId;
use crate::location::LocationError;
use crate::truth::InvalidLabel;
use claude_code::{ClaudeCodeStats, ClaudeCodeStream};
use stream::StreamError;
use time::{Day, TimeError};
use window::{WindowStats, WindowStream};

/// The dataset's id.
pub const DATASET: &str = "ai-village";

/// The default window: Monday 2026-07-13 to Friday 2026-07-17 (26 agents,
/// about 116k turns).
pub const DEFAULT_FROM: &str = "2026-07-13";
pub const DEFAULT_TO: &str = "2026-07-17";

#[derive(Debug, thiserror::Error)]
pub enum AiVillageError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error("time: {0}")]
    Time(#[from] TimeError),
    #[error("the dataset has no {0} agent")]
    NoAgent(String),
    #[error("location: {0}")]
    Location(#[from] LocationError),
    #[error("label: {0}")]
    Label(#[from] InvalidLabel),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
}

/// Which part of the dataset to convert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The Claude Code agent's stream, at most `limit` contexts.
    ClaudeCode { limit: Option<usize> },
    /// Village days `from..=to`.
    Window { from: Day, to: Day },
}

/// What a source saw, for reports.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Stats {
    ClaudeCode(ClaudeCodeStats),
    Window(WindowStats),
}

enum Inner {
    ClaudeCode(Box<ClaudeCodeStream>),
    Window(Box<WindowStream>),
}

/// AI Village as a [`TraceSource`].
pub struct AiVillageSource {
    inner: Inner,
}

impl AiVillageSource {
    /// Opens the dataset at `root` and makes the passes `mode` needs.
    pub fn open(root: &Path, mode: Mode) -> Result<Self, AiVillageError> {
        let inner = match mode {
            Mode::ClaudeCode { limit } => {
                Inner::ClaudeCode(Box::new(ClaudeCodeStream::open(root, limit)?))
            }
            Mode::Window { from, to } => {
                Inner::Window(Box::new(WindowStream::open(root, from, to)?))
            }
        };
        Ok(Self { inner })
    }

    pub fn stats(&self) -> Stats {
        match &self.inner {
            Inner::ClaudeCode(stream) => Stats::ClaudeCode(stream.stats().clone()),
            Inner::Window(stream) => Stats::Window(stream.stats().clone()),
        }
    }

    fn next_world(&mut self) -> Option<Result<World, AiVillageError>> {
        match &mut self.inner {
            Inner::ClaudeCode(stream) => stream.next_world(),
            Inner::Window(stream) => stream.next_world(),
        }
    }
}

impl TraceSource for AiVillageSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        std::iter::from_fn(move || self.next_world().map(|w| w.map_err(SourceError::from)))
    }
}
