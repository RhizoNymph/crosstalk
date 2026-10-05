//! Open-SWE-Traces as a background (negative) corpus.
//!
//! Each row of a shard (`data/<harness>/<model>/<dataset>/*.parquet`) is
//! one trajectory: one agent solving one SWE task alone. Its calls are its
//! assistant messages: call `i` sends `messages[..i]` and receives
//! `messages[i]`. Tool messages carry no `tool_call_id`, so they are paired
//! with calls by position ([`chat`](crate::datasets::chat)); parallel calls
//! (mini-swe-agent on qwen3.8) are answered in order.
//!
//! **Worlds.** Trajectories are mixed into worlds of `agents_per_world`,
//! taking one row from each selected shard in turn, so a world holds
//! several harnesses, models and repositories. No trajectory read another,
//! so every world is a [background world](crate::datasets::background):
//! complete coverage, no positives, a `SharedSource` control between
//! trajectories of one repository and a `Boilerplate` control otherwise.
//!
//! **Clock.** Traces have no times. Trajectory slot `t` of a world makes its
//! call `i` at `compose(i, t, 0)`: every trajectory starts together and
//! they interleave call by call.

pub mod files;
pub mod schema;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crosstalk_spec::support::Timestamp;

pub use files::Shard;
pub use schema::OpenSweRow;

use crate::corpus::clock::{ClockError, compose};
use crate::corpus::{Fidelity, SourceError, TraceSource, World};
use crate::datasets::background::{BackgroundError, BackgroundWorld, Call, Trajectory, stop_for};
use crate::datasets::chat::{ChatError, ChatMessage, convert};
use crate::datasets::parquet_rows::{ParquetError, ParquetRows};
use crate::datasets::salt::Selection;
use crate::keys::{DatasetId, SourceRef, WorldKey};

/// The dataset's id.
pub const DATASET: &str = "open_swe";

/// The trajectories mixed into one world unless configured.
pub const AGENTS_PER_WORLD: usize = 16;

/// The columns read.
pub const COLUMNS: &[&str] = &["instance_id", "repo", "trajectory_id", "messages"];

#[derive(Debug, thiserror::Error)]
pub enum OpenSweError {
    #[error("{root} has no data/ directory")]
    NoData { root: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error("{file} row {row}: {source}")]
    Chat {
        file: String,
        row: usize,
        #[source]
        source: ChatError,
    },
    #[error("virtual clock: {0}")]
    Clock(#[source] ClockError),
    #[error(transparent)]
    Background(#[from] BackgroundError),
}

/// How a source samples and mixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mixing {
    /// Trajectories per world (at least 1).
    pub agents_per_world: usize,
    /// Rows read from each shard; every row when `None`.
    pub per_shard: Option<usize>,
}

impl Default for Mixing {
    fn default() -> Self {
        Self {
            agents_per_world: AGENTS_PER_WORLD,
            per_shard: None,
        }
    }
}

/// The calls of one trajectory's `messages`, call `i` (counting assistant
/// messages) at `clock(i)`.
pub fn calls(
    messages: &[ChatMessage],
    file: &str,
    row: usize,
    clock: impl Fn(u64) -> Result<Timestamp, ClockError>,
) -> Result<Vec<Call>, OpenSweError> {
    let hashed = convert(messages).map_err(|source| OpenSweError::Chat {
        file: file.to_owned(),
        row,
        source,
    })?;
    let mut calls = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if !message.is_assistant() {
            continue;
        }
        let ordinal = calls.len() as u64;
        calls.push(Call {
            at: clock(ordinal).map_err(OpenSweError::Clock)?,
            request: hashed[..index].to_vec(),
            response: hashed[index].clone(),
            stop: stop_for(!message.calls().is_empty()),
            fidelity: Fidelity::Reconstructed,
            source: SourceRef::new(file, format!("/rows/{row}/messages/{index}")),
        });
    }
    Ok(calls)
}

/// The agent name of a shard's row: unique across shards.
pub fn agent_name(shard: &Shard, row: usize) -> String {
    format!("{}/{}/{}/{row}", shard.harness, shard.model, shard.dataset)
}

/// One row as a trajectory in slot `slot` of its world.
pub fn trajectory(
    shard: &Shard,
    row: usize,
    record: &OpenSweRow,
    slot: u64,
) -> Result<Trajectory, OpenSweError> {
    Ok(Trajectory {
        name: agent_name(shard, row),
        model: shard.model.clone(),
        group: record.repo.clone(),
        calls: calls(&record.messages, &shard.relative, row, |call| {
            compose(call, slot, 0)
        })?,
    })
}

/// Builds one world from rows, in slot order.
pub fn mix(key: WorldKey, rows: &[(Shard, usize, OpenSweRow)]) -> Result<World, OpenSweError> {
    let mut world = BackgroundWorld::new(DatasetId::new(DATASET), key);
    for (slot, (shard, row, record)) in rows.iter().enumerate() {
        world.add(trajectory(shard, *row, record, slot as u64)?)?;
    }
    Ok(world.finish()?)
}

/// Rows of each selected shard, taken in turn.
pub struct RoundRobin {
    root: PathBuf,
    shards: Vec<Shard>,
    open: Vec<Option<ParquetRows<OpenSweRow>>>,
    taken: Vec<usize>,
    done: Vec<bool>,
    per_shard: Option<usize>,
    cursor: usize,
}

impl RoundRobin {
    pub fn new(root: &Path, shards: Vec<Shard>, per_shard: Option<usize>) -> Self {
        let count = shards.len();
        Self {
            root: root.to_path_buf(),
            shards,
            open: (0..count).map(|_| None).collect(),
            taken: vec![0; count],
            done: vec![false; count],
            per_shard,
            cursor: 0,
        }
    }

    /// The next row of the next shard with rows left; `None` when every
    /// shard is done. A shard that fails to open or read is reported once
    /// and then skipped.
    pub fn next_row(&mut self) -> Option<Result<(Shard, usize, OpenSweRow), OpenSweError>> {
        let count = self.shards.len();
        for _ in 0..count {
            let at = self.cursor % count;
            self.cursor = self.cursor.wrapping_add(1);
            if self.done[at] {
                continue;
            }
            if self.per_shard.is_some_and(|limit| self.taken[at] >= limit) {
                self.done[at] = true;
                self.open[at] = None;
                continue;
            }
            if self.open[at].is_none() {
                let path = self.root.join(&self.shards[at].relative);
                match ParquetRows::open(&path, COLUMNS) {
                    Ok(rows) => self.open[at] = Some(rows),
                    Err(error) => {
                        self.done[at] = true;
                        return Some(Err(error.into()));
                    }
                }
            }
            let next = self.open[at].as_mut().and_then(Iterator::next);
            match next {
                None => {
                    self.done[at] = true;
                    self.open[at] = None;
                }
                Some(Err(error)) => {
                    self.done[at] = true;
                    self.open[at] = None;
                    return Some(Err(error.into()));
                }
                Some(Ok((row, record))) => {
                    self.taken[at] += 1;
                    return Some(Ok((self.shards[at].clone(), row, record)));
                }
            }
        }
        None
    }
}

/// Open-SWE as a stream of mixed worlds.
pub struct OpenSweSource {
    root: PathBuf,
    shards: Vec<Shard>,
    mixing: Mixing,
}

impl OpenSweSource {
    pub fn open(root: &Path, selection: &Selection, mixing: Mixing) -> Result<Self, OpenSweError> {
        Ok(Self {
            root: root.to_path_buf(),
            shards: files::discover(root, selection)?,
            mixing,
        })
    }

    pub fn shards(&self) -> &[Shard] {
        &self.shards
    }
}

impl TraceSource for OpenSweSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let size = self.mixing.agents_per_world.max(1);
        let mut rows = RoundRobin::new(&self.root, self.shards.clone(), self.mixing.per_shard);
        let mut pending: VecDeque<OpenSweError> = VecDeque::new();
        let mut number = 0usize;
        std::iter::from_fn(move || {
            if let Some(error) = pending.pop_front() {
                return Some(Err(SourceError::from(error)));
            }
            let mut batch = Vec::with_capacity(size);
            while batch.len() < size {
                match rows.next_row() {
                    None => break,
                    Some(Ok(row)) => batch.push(row),
                    Some(Err(error)) => pending.push_back(error),
                }
            }
            if batch.is_empty() {
                return pending.pop_front().map(|error| Err(error.into()));
            }
            let key = WorldKey::new(format!("mix-{number:05}"));
            number += 1;
            tracing::debug!(world = %key, trajectories = batch.len(), "mixing open-swe world");
            Some(mix(key, &batch).map_err(SourceError::from))
        })
    }
}
