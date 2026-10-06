//! The correlator's checkpoint: every shard's state, encoded, with the tick
//! it ran through (`docs/features/postgres_stores.md`, "L5: flow checkpoint
//! and restore").
//!
//! A checkpoint is taken only when the consumer is idle (no step waits in
//! its queue), so it covers exactly the inputs the consumer took before it:
//! the bus deliveries it holds unacked until the checkpoint is stored, and
//! the accesses and tool calls recorded up to the recording number the
//! store reads in the same transaction. A shard's row and its
//! `shard_ticks` row are written together, so `ticked_through` never names
//! a tick the stored state has not run (`flow.checkpoint.ticks-with-state`,
//! INV-1216).
//!
//! **Encoding.** Each shard is the JSON of a [`ShardState`] (the
//! correlator's state and the read routing of the media it holds) under
//! [`SNAPSHOT_FORMAT`]. A binary reads only the formats it knows: any
//! other is [`Incompatible::Format`], a start error that needs `crosstalk
//! migrate --reset-correlator` (decision Q2). A shard count that changed
//! since the checkpoint is incompatible the same way: media hash to other
//! shards.

use std::collections::BTreeMap;

use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use super::durability::DurabilityError;
use crate::correlate::snapshot::pairs;
use crate::correlate::{CorrelatorState, MediumKey, ReadPart};

/// The snapshot encoding this binary writes, and the only one it reads.
pub const SNAPSHOT_FORMAT: u32 = 1;

/// One shard as a checkpoint holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardState {
    pub(crate) correlator: CorrelatorState,
    /// The consumer's routing of every read part to its medium, for the
    /// media whose home is this shard.
    #[serde(with = "pairs")]
    pub(crate) reads: BTreeMap<ReadPart, (MediumKey, Timestamp)>,
}

/// One shard's encoded state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSnapshot {
    /// The shard's index, below the shard count.
    pub shard: u32,
    /// The last tick the shard ran; `None` before its first.
    pub ticked_through: Option<Timestamp>,
    /// The [`ShardState`]'s JSON.
    pub state: Vec<u8>,
}

/// A checkpoint of every shard, to store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub format: u32,
    /// One per shard, by index.
    pub shards: Vec<ShardSnapshot>,
}

/// A checkpoint as stored, with what it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCheckpoint {
    pub format: u32,
    /// The shard count the checkpoint was taken with.
    pub count: u32,
    /// The last recording number (accesses and tool calls) it covers.
    pub recorded_through: u64,
    pub shards: Vec<ShardSnapshot>,
}

/// Why a stored checkpoint cannot be restored by this binary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Incompatible {
    #[error("snapshot format {found}; this binary reads format {reads}")]
    Format { found: u32, reads: u32 },
    #[error("the checkpoint has {found} shards; {configured} are configured")]
    ShardCount { found: u32, configured: usize },
    #[error("the checkpoint has no row for shard {shard}")]
    MissingShard { shard: u32 },
    #[error("shard {shard} does not decode: {reason}")]
    Undecodable { shard: u32, reason: String },
}

/// Why the consumer could not restore.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlowRestoreError {
    /// The stored checkpoint is not one this binary reads. The process
    /// must not consume: the operator runs `crosstalk migrate
    /// --reset-correlator`, which loses the pairings pending at the
    /// checkpoint, knowingly (decision Q2).
    #[error(
        "incompatible correlator checkpoint ({0}); run `crosstalk migrate --reset-correlator`"
    )]
    IncompatibleSnapshot(Incompatible),
    #[error("reading the checkpoint: {0}")]
    Store(#[from] DurabilityError),
}

/// Why a checkpoint was not taken.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    /// Steps wait in the queue (a store failed): a snapshot now would
    /// cover inputs whose effects are not stored. Retried once the queue
    /// drained.
    #[error("{backlog} steps wait in the queue")]
    Busy { backlog: usize },
    #[error("encoding shard {shard}: {reason}")]
    Encode { shard: u32, reason: String },
    #[error("storing the checkpoint: {0}")]
    Store(#[from] DurabilityError),
}

impl ShardState {
    pub(crate) fn encode(&self, shard: u32) -> Result<Vec<u8>, CheckpointError> {
        serde_json::to_vec(self).map_err(|error| CheckpointError::Encode {
            shard,
            reason: error.to_string(),
        })
    }

    pub(crate) fn decode(shard: u32, bytes: &[u8]) -> Result<Self, Incompatible> {
        serde_json::from_slice(bytes).map_err(|error| Incompatible::Undecodable {
            shard,
            reason: error.to_string(),
        })
    }
}
