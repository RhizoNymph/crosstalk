//! What the flow consumer keeps across a restart: the port
//! [`FlowDurability`], the volatile implementation memory mode runs with
//! ([`Volatile`]), and a memory one that models the Postgres tables for
//! simulations ([`MemoryDurability`]). The Postgres one is
//! [`crate::store::PgFlowDurability`].
//!
//! The consumer's state is its correlator shards plus what it took since
//! the last checkpoint. The port keeps:
//!
//! - **held writes**, inserted when a write is held and deleted once its
//!   released access is recorded;
//! - the **recording order** of accesses (the registry numbers them as it
//!   records them, `flow.accesses.recorded_seq`) and of tool calls, so a
//!   restore re-feeds exactly the inputs after the checkpoint, in order;
//! - the **checkpoint**, written with the shards' tick record in one
//!   transaction, covering every input recorded when it was taken.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AccessId, ChannelId};
use crosstalk_spec::support::Timestamp;

use super::checkpoint::{Checkpoint, StoredCheckpoint};
use super::input::{Observed, ToolCalled, WriteCall};

/// Why the port failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DurabilityError {
    /// The store is unreachable or refused for now: worth a retry.
    #[error("the flow store is unavailable: {reason}")]
    Unavailable { reason: String },
    /// A stored value does not decode, or the stored state contradicts
    /// itself: no retry helps.
    #[error("the flow store holds an inconsistent value: {reason}")]
    Corrupt { reason: String },
}

impl DurabilityError {
    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Corrupt { .. })
    }
}

/// The channel an access's resource resolved to when the consumer
/// recorded the access: the medium it was correlated in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// On no channel: the resource's own medium.
    NoChannel,
    Channel(ChannelId),
    /// Not stored (the process stopped between recording the access and
    /// its resolution): resolved again on re-feed.
    Unknown,
}

impl Resolved {
    pub fn of(channel: Option<ChannelId>) -> Self {
        channel.map_or(Self::NoChannel, Self::Channel)
    }
}

/// An input recorded after the stored checkpoint, to re-feed on restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recorded {
    /// A recorded access, with the locator of its resource and the channel
    /// it was resolved to.
    Access {
        access: Access,
        locator: Locator,
        resolved: Resolved,
    },
    ToolCall(ToolCalled),
}

/// What a restore starts from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovered {
    /// The last checkpoint; `None` before the first.
    pub checkpoint: Option<StoredCheckpoint>,
    /// Every held write, with its settle time.
    pub held: Vec<(Observed<WriteCall>, Timestamp)>,
    /// Every input recorded after the checkpoint, in recording order.
    pub inputs: Vec<Recorded>,
}

/// Where the flow consumer keeps what survives a restart.
pub trait FlowDurability: Send + Sync {
    /// Whether anything kept here outlives the process. A volatile
    /// consumer acks each delivery once its steps ran; a durable one only
    /// once a checkpoint covers it.
    fn survives_restart(&self) -> bool;

    /// `write` is held until its result, at the latest until `settles_at`.
    /// Holding one already held changes nothing.
    fn hold(
        &self,
        write: &Observed<WriteCall>,
        settles_at: Timestamp,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;

    /// The held write `access` was released and its access recorded.
    fn release(&self, access: AccessId)
    -> impl Future<Output = Result<(), DurabilityError>> + Send;

    /// `access` (of the resource at `locator`) was recorded in the
    /// registry, resolved to `channel` (`None`: on no channel). A store
    /// that numbers accesses as it records them (Postgres) keeps only the
    /// resolution.
    fn access_recorded(
        &self,
        access: &Access,
        locator: &Locator,
        channel: Option<ChannelId>,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;

    /// Record a tool call, numbered in the recording order.
    fn tool_called(
        &self,
        call: &ToolCalled,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;

    /// Store `checkpoint`, taken at `taken_at`, as covering every input
    /// recorded so far, together with each shard's tick record, in one
    /// transaction.
    fn save(
        &self,
        checkpoint: &Checkpoint,
        taken_at: Timestamp,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;

    /// The last checkpoint, the held writes and the inputs recorded after
    /// the checkpoint, read in one snapshot.
    fn load(&self) -> impl Future<Output = Result<Recovered, DurabilityError>> + Send;
}

/// Nothing survives: memory mode, where the stores die with the process
/// too.
#[derive(Debug, Clone, Copy, Default)]
pub struct Volatile;

impl FlowDurability for Volatile {
    fn survives_restart(&self) -> bool {
        false
    }

    async fn hold(
        &self,
        _write: &Observed<WriteCall>,
        _settles_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn release(&self, _access: AccessId) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn access_recorded(
        &self,
        _access: &Access,
        _locator: &Locator,
        _channel: Option<ChannelId>,
    ) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn tool_called(&self, _call: &ToolCalled) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn save(
        &self,
        _checkpoint: &Checkpoint,
        _taken_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn load(&self) -> Result<Recovered, DurabilityError> {
        Ok(Recovered::default())
    }
}

/// The Postgres tables' model in memory: a handle shared by every
/// consumer of one simulated process lifetime and the next, so a
/// simulation can drop a consumer and restore another from what it kept.
#[derive(Debug, Clone, Default)]
pub struct MemoryDurability {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    held: BTreeMap<AccessId, (Observed<WriteCall>, Timestamp)>,
    /// Every recorded input by its recording number.
    journal: BTreeMap<u64, Recorded>,
    /// Each recorded access's recording number.
    recorded_accesses: BTreeMap<AccessId, u64>,
    next: u64,
    checkpoint: Option<StoredCheckpoint>,
    /// Each shard's tick record (`flow.shard_ticks`).
    ticks: BTreeMap<u32, Timestamp>,
}

impl MemoryState {
    fn record(&mut self, input: Recorded) -> u64 {
        self.next += 1;
        self.journal.insert(self.next, input);
        self.next
    }

    /// The registry records an access once: so does its numbering. Its
    /// first resolution stays.
    fn access(&mut self, access: &Access, locator: &Locator, resolved: Resolved) {
        match self.recorded_accesses.get(&access.id) {
            Some(number) => {
                if let Some(Recorded::Access {
                    resolved: stored @ Resolved::Unknown,
                    ..
                }) = self.journal.get_mut(number)
                {
                    *stored = resolved;
                }
            }
            None => {
                let number = self.record(Recorded::Access {
                    access: access.clone(),
                    locator: locator.clone(),
                    resolved,
                });
                self.recorded_accesses.insert(access.id, number);
            }
        }
    }
}

impl MemoryDurability {
    pub fn new() -> Self {
        Self::default()
    }

    fn with<T>(&self, f: impl FnOnce(&mut MemoryState) -> T) -> T {
        // A poisoned lock means a panicking holder; the maps it left are
        // whole (every update is one insert or remove).
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut state)
    }

    /// Each shard's tick record, as `flow.shard_ticks` holds it.
    pub fn ticks(&self) -> BTreeMap<u32, Timestamp> {
        self.with(|state| state.ticks.clone())
    }

    /// The stored checkpoint.
    pub fn checkpoint(&self) -> Option<StoredCheckpoint> {
        self.with(|state| state.checkpoint.clone())
    }

    /// `access` was recorded and numbered, and the process stopped before
    /// its resolution was stored: what Postgres holds when the consumer's
    /// update after `record_access` failed. For simulations.
    pub fn record_unresolved(&self, access: &Access, locator: &Locator) {
        self.with(|state| state.access(access, locator, Resolved::Unknown));
    }

    /// How many writes are held.
    pub fn held(&self) -> usize {
        self.with(|state| state.held.len())
    }
}

impl FlowDurability for MemoryDurability {
    fn survives_restart(&self) -> bool {
        true
    }

    async fn hold(
        &self,
        write: &Observed<WriteCall>,
        settles_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        self.with(|state| {
            state
                .held
                .entry(write.id)
                .or_insert_with(|| (write.clone(), settles_at));
        });
        Ok(())
    }

    async fn release(&self, access: AccessId) -> Result<(), DurabilityError> {
        self.with(|state| state.held.remove(&access));
        Ok(())
    }

    async fn access_recorded(
        &self,
        access: &Access,
        locator: &Locator,
        channel: Option<ChannelId>,
    ) -> Result<(), DurabilityError> {
        self.with(|state| state.access(access, locator, Resolved::of(channel)));
        Ok(())
    }

    async fn tool_called(&self, call: &ToolCalled) -> Result<(), DurabilityError> {
        self.with(|state| state.record(Recorded::ToolCall(call.clone())));
        Ok(())
    }

    async fn save(
        &self,
        checkpoint: &Checkpoint,
        _taken_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        let count = u32::try_from(checkpoint.shards.len()).map_err(|_| DurabilityError::Corrupt {
            reason: "more shards than a u32 counts".to_owned(),
        })?;
        self.with(|state| {
            for shard in &checkpoint.shards {
                if let Some(at) = shard.ticked_through {
                    let tick = state.ticks.entry(shard.shard).or_insert(at);
                    *tick = (*tick).max(at);
                }
            }
            let through = state.next;
            // Tool calls the checkpoint covers are no longer re-fed.
            state
                .journal
                .retain(|seq, input| *seq > through || matches!(input, Recorded::Access { .. }));
            state.checkpoint = Some(StoredCheckpoint {
                format: checkpoint.format,
                count,
                recorded_through: through,
                shards: checkpoint.shards.clone(),
            });
        });
        Ok(())
    }

    async fn load(&self) -> Result<Recovered, DurabilityError> {
        Ok(self.with(|state| {
            let through = state
                .checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.recorded_through);
            Recovered {
                checkpoint: state.checkpoint.clone(),
                held: state.held.values().cloned().collect(),
                inputs: state
                    .journal
                    .range(through.saturating_add(1)..)
                    .map(|(_, input)| input.clone())
                    .collect(),
            }
        }))
    }
}
