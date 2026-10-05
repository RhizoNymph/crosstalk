//! The registry's `ChannelDirectory`: the supersession table, cached in
//! memory so `canonical` answers synchronously, and the correlator shard
//! key it decides.
//!
//! Supersession only grows: a superseded channel stays superseded by the
//! same channel, and a superseding channel (declared) is never superseded.
//! So the cache is never wrong, only possibly behind, and catching up is
//! adding entries. It is loaded when the registry is opened, extended by
//! every promotion the registry commits (before `promote` returns, so a
//! `ChannelPromoted` consumer on the same node already resolves through
//! it), and caught up with promotions committed by other nodes by
//! [`PgChannelRegistry::refresh_directory`](super::PgChannelRegistry::refresh_directory),
//! which the flow consumer calls when it processes a `ChannelPromoted`.
//!
//! **Shards.** The correlator keys its shards by [`ShardKey`]: the canonical
//! channel of an access's resource, or the resource when it is on no
//! channel (`l5_flow`, INV-253 `flow.correlator.shard-affinity`). Keying by
//! the canonical channel puts evidence recorded on a channel before a
//! promotion superseded it and evidence recorded after on one shard.

use std::collections::HashMap;
use std::num::NonZeroU16;
use std::sync::{Arc, RwLock};

use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use sqlx::PgPool;
use tracing::warn;

use super::codec::parse_id;
use super::error::FlowStoreError;

/// Superseded channel to the channel that superseded it. Clones share one
/// table.
#[derive(Debug, Clone, Default)]
pub(crate) struct Supersessions {
    table: Arc<RwLock<HashMap<ChannelId, ChannelId>>>,
}

impl Supersessions {
    /// `ChannelDirectory::canonical`.
    pub(crate) fn canonical(&self, id: ChannelId) -> ChannelId {
        match self.table.read() {
            Ok(table) => table.get(&id).copied().unwrap_or(id),
            Err(poisoned) => poisoned.into_inner().get(&id).copied().unwrap_or(id),
        }
    }

    /// Record supersessions a committed promotion made.
    pub(crate) fn extend(&self, entries: impl IntoIterator<Item = (ChannelId, ChannelId)>) {
        let mut table = match self.table.write() {
            Ok(table) => table,
            Err(poisoned) => {
                warn!("directory lock was poisoned; continuing with its contents");
                poisoned.into_inner()
            }
        };
        table.extend(entries);
    }

    /// Catch up with every supersession stored.
    pub(crate) async fn load(&self, pool: &PgPool) -> Result<usize, FlowStoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, superseded_by FROM flow.channels WHERE superseded_by IS NOT NULL",
        )
        .fetch_all(pool)
        .await?;
        let mut entries = Vec::with_capacity(rows.len());
        for (id, by) in rows {
            entries.push((
                parse_id::<ChannelId>("channels.id", &id)?,
                parse_id::<ChannelId>("channels.superseded_by", &by)?,
            ));
        }
        let count = entries.len();
        self.extend(entries);
        Ok(count)
    }
}

impl ChannelDirectory for Supersessions {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        Supersessions::canonical(self, id)
    }
}

/// What a correlator shard is keyed by: the canonical channel an access's
/// resource is on, or the resource itself while it is on no channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardKey {
    Channel(ChannelId),
    Resource(ResourceId),
}

impl ShardKey {
    /// The key of evidence on `resource`, which is on `channel` (as stored
    /// or as a lookup named it), resolved through `directory`: always the
    /// canonical channel, never a superseded one.
    pub fn of(
        directory: &impl ChannelDirectory,
        channel: Option<ChannelId>,
        resource: ResourceId,
    ) -> Self {
        match channel {
            Some(channel) => ShardKey::Channel(directory.canonical(channel)),
            None => ShardKey::Resource(resource),
        }
    }

    /// The shard, of `shards`, that owns this key: the key's ULID modulo
    /// the shard count, so it is the same on every node.
    pub fn shard(self, shards: NonZeroU16) -> ShardIndex {
        let raw = match self {
            ShardKey::Channel(id) => id.as_ulid(),
            ShardKey::Resource(id) => id.as_ulid(),
        };
        let index = raw % u128::from(shards.get());
        // `index < shards <= u16::MAX`.
        ShardIndex(u16::try_from(index).unwrap_or(0))
    }
}

/// One correlator shard, `0..shards`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardIndex(pub u16);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_superseded_channel_keys_the_shard_of_its_superseder() {
        let directory = Supersessions::default();
        let (old, promoted) = (ChannelId::from_ulid(11), ChannelId::from_ulid(12));
        let resource = ResourceId::from_ulid(99);
        let before = ShardKey::of(&directory, Some(old), resource);
        assert_eq!(before, ShardKey::Channel(old));
        directory.extend([(old, promoted)]);
        let after_old = ShardKey::of(&directory, Some(old), resource);
        let after_new = ShardKey::of(&directory, Some(promoted), resource);
        assert_eq!(after_old, after_new);
        assert_eq!(after_old, ShardKey::Channel(promoted));
        let shards = NonZeroU16::MIN.saturating_add(7);
        assert_eq!(after_old.shard(shards), after_new.shard(shards));
    }

    #[test]
    fn a_resource_on_no_channel_keys_its_own_shard() {
        let directory = Supersessions::default();
        let resource = ResourceId::from_ulid(5);
        assert_eq!(
            ShardKey::of(&directory, None, resource),
            ShardKey::Resource(resource)
        );
        let shards = NonZeroU16::MIN.saturating_add(3);
        assert_eq!(ShardKey::Resource(resource).shard(shards), ShardIndex(1));
    }

    #[test]
    fn canonical_of_an_unknown_id_is_itself() {
        let directory = Supersessions::default();
        let id = ChannelId::from_ulid(3);
        assert_eq!(directory.canonical(id), id);
    }
}
