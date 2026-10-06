//! What a live detection's rows look up, over a converted bench world
//! instead of a corpus world: the transmissions' channels, spans and
//! accesses as `Resolved::gather` read them, and the whole text of a part
//! an exchange carries.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::PartRef;

use super::convert::ConvertedWorld;
use crate::keys::AgentKey;
use crate::location::whole_part;
use crate::predict::Directory;
use crate::predict::reads::Resolved;

/// A converted world and a detection's resolved reads.
pub struct BenchDirectory<'a> {
    world: &'a ConvertedWorld,
    resolved: &'a Resolved,
}

impl<'a> BenchDirectory<'a> {
    pub fn new(world: &'a ConvertedWorld, resolved: &'a Resolved) -> Self {
        Self { world, resolved }
    }
}

impl Directory for BenchDirectory<'_> {
    /// A bench world has no corpus agents: rows name detector agents only.
    fn agent(&self, _id: AgentId) -> Option<AgentKey> {
        None
    }

    fn channel(&self, id: ChannelId) -> Option<&[Locator]> {
        self.resolved.channel(id)
    }

    fn span(&self, id: SpanId) -> Option<IndexedSpan> {
        self.resolved.span(id).copied()
    }

    fn access(&self, id: AccessId) -> Option<&(Access, Resource)> {
        self.resolved.access(id)
    }

    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation> {
        let message = self
            .world
            .exchange(exchange)?
            .messages
            .iter()
            .find(|message| message.hash == part.message)?;
        whole_part(message, part.index).ok()
    }
}
