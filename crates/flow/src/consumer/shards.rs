//! The correlator shards and the routing between them.
//!
//! Each shard is a [`WindowedCorrelator`]. A medium's evidence lives on
//! the shard its key hashes to (`flow.correlator.shard-affinity`): an
//! access on the shard of its medium, a tool-result match on the shard of
//! the read that carried it. A match whose read is not known yet, and any
//! other match, goes to the shard of its reader exchange; when the read
//! arrives on another shard, the matches it carries move there with it.
//!
//! **Handoff.** When a channel is discovered from a resource, or a
//! promotion supersedes a channel, [`Shards::rekey`] takes the old
//! medium's evidence from its shard and the new medium's shard absorbs it,
//! before the consumer processes another input
//! (`flow.correlator.resource-shard-handoff`).
//!
//! The shards are driven from the consumer's task, one input at a time;
//! each sees only its own inputs, so a shard can move to a task of its own
//! fed over a channel without changing what it decides.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId};
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;

use crate::correlate::{Decided, Kin, MediumKey, ReadPart, WindowedCorrelator};

/// The consumer's correlator shards.
#[derive(Debug, Clone)]
pub struct Shards {
    shards: Vec<WindowedCorrelator>,
    /// Which medium each read part was routed to, with the read's time.
    reads: BTreeMap<ReadPart, (MediumKey, Timestamp)>,
}

/// FNV-1a over a tag and an id: a stable shard choice on every node.
fn spread(tag: u8, id: u128, shards: usize) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in std::iter::once(tag).chain(id.to_le_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let shards = u64::try_from(shards).unwrap_or(u64::MAX).max(1);
    // The remainder is below `shards`, which came from a usize.
    usize::try_from(hash % shards).unwrap_or(0)
}

impl Shards {
    pub fn new(timing: CorrelationTiming, count: NonZeroUsize) -> Self {
        Self {
            shards: (0..count.get())
                .map(|_| WindowedCorrelator::new(timing))
                .collect(),
            reads: BTreeMap::new(),
        }
    }

    pub fn count(&self) -> usize {
        self.shards.len()
    }

    /// The shard of a medium.
    pub fn medium_shard(&self, medium: MediumKey) -> usize {
        match medium {
            MediumKey::Channel(channel) => spread(1, channel.as_ulid(), self.shards.len()),
            MediumKey::Resource(resource) => spread(2, resource.as_ulid(), self.shards.len()),
        }
    }

    /// The shard of a reader exchange.
    pub fn exchange_shard(&self, exchange: ExchangeId) -> usize {
        spread(3, exchange.as_ulid(), self.shards.len())
    }

    pub fn shard(&self, index: usize) -> Option<&WindowedCorrelator> {
        self.shards.get(index)
    }

    /// The last tick every shard processed.
    pub fn last_tick(&self) -> Option<Timestamp> {
        self.shards
            .iter()
            .filter_map(WindowedCorrelator::last_tick)
            .min()
    }

    pub fn access(&mut self, access: &Access, channel: Option<ChannelId>) -> Vec<Decided> {
        let medium = MediumKey::of(access, channel);
        let home = self.medium_shard(medium);
        let exchange = self.exchange_shard(access.exchange);
        let mut out = Vec::new();
        if exchange != home {
            out.extend(self.with(exchange, |shard| shard.exchange(access.exchange, access.at)));
        }
        out.extend(self.with(home, |shard| shard.access(access, channel)));
        if let Some(part) = ReadPart::of_read(access) {
            self.reads.insert(part, (medium, access.at));
            if exchange != home {
                let moved = self.with(exchange, |shard| shard.take_uncarried(access));
                for content in moved {
                    out.extend(self.with(home, |shard| shard.content(&content)));
                }
            }
        }
        out
    }

    pub fn content(&mut self, content: &ContentMatch) -> Vec<Decided> {
        let carried = match content.carrier() {
            Carrier::ToolResult(_) => self.reads.get(&ReadPart::of_match(content)).copied(),
            Carrier::UserTurn | Carrier::SystemPrompt | Carrier::ReaderOutput => None,
        };
        let shard = match carried {
            Some((medium, _)) => self.medium_shard(medium),
            None => self.exchange_shard(content.reader_exchange()),
        };
        self.with(shard, |shard| shard.content(content))
    }

    pub fn exchange(&mut self, exchange: ExchangeId, started_at: Timestamp) -> Vec<Decided> {
        let shard = self.exchange_shard(exchange);
        self.with(shard, |shard| shard.exchange(exchange, started_at))
    }

    pub fn tick(&mut self, now: Timestamp) -> Vec<Decided> {
        let mut out = Vec::new();
        for shard in &mut self.shards {
            out.extend(shard.tick(now));
        }
        if let Some(horizon) = self.shards.first().map(|shard| {
            let timing = shard.timing();
            let keep = timing
                .settle_after()
                .saturating_add(timing.correlation_window());
            let micros = u64::try_from(keep.as_micros()).unwrap_or(u64::MAX);
            Timestamp::from_micros(now.as_micros().saturating_sub(micros))
        }) {
            self.reads.retain(|_, (_, at)| *at >= horizon);
        }
        out
    }

    /// Move `from`'s evidence to `to`, across shards when they differ.
    pub fn rekey(&mut self, from: MediumKey, to: MediumKey) -> Vec<Decided> {
        if from == to {
            return Vec::new();
        }
        let (source, target) = (self.medium_shard(from), self.medium_shard(to));
        // Nothing held: nothing to move (the read index names only media
        // that hold their reads).
        let Some(evidence) = self.with(source, |shard| shard.take_medium(from)) else {
            return Vec::new();
        };
        for (medium, _) in self.reads.values_mut() {
            if *medium == from {
                *medium = to;
            }
        }
        tracing::debug!(?from, ?to, source, target, "medium handed off");
        self.with(target, |shard| shard.absorb(to, evidence))
    }

    pub fn knows_agent(&self, agent: AgentId) -> bool {
        self.shards.iter().all(|shard| shard.knows_agent(agent))
    }

    pub fn learn_kin(&mut self, agent: AgentId, kin: Kin) {
        for shard in &mut self.shards {
            shard.learn_kin(agent, kin);
        }
    }

    pub fn forget_kin(&mut self) {
        for shard in &mut self.shards {
            shard.forget_kin();
        }
    }

    pub fn tool_named(
        &mut self,
        agent: AgentId,
        call: &ToolCallId,
        name: &ToolName,
        at: Timestamp,
    ) {
        for shard in &mut self.shards {
            shard.tool_named(agent, call, name.clone(), at);
        }
    }

    fn with<T: Default>(
        &mut self,
        index: usize,
        f: impl FnOnce(&mut WindowedCorrelator) -> T,
    ) -> T {
        self.shards.get_mut(index).map(f).unwrap_or_default()
    }
}
