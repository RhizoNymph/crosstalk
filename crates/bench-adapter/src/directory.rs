//! What a detection's rows look up by id ([`Directory`]), and the bench
//! world's answer to it ([`BenchDirectory`]): the transmissions' channels,
//! spans and accesses as `Resolved::gather` read them, and the whole text
//! of a part an exchange carries.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AccessId, ChannelId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::PartRef;

use crate::convert::ConvertedWorld;
use crate::location::whole_part;
use crate::reads::Resolved;

/// What prediction rows look up by id.
pub trait Directory {
    /// The canonical resources a channel holds; `None` when unknown.
    fn channel(&self, id: ChannelId) -> Option<&[Locator]>;

    /// The record of an originated span; `None` when unknown.
    fn span(&self, id: SpanId) -> Option<IndexedSpan>;

    /// A recorded access and its resource; `None` when unknown.
    fn access(&self, id: AccessId) -> Option<&(Access, Resource)>;

    /// The whole text of `part` as `exchange` carries it; `None` when the
    /// exchange or the part is unknown or has no text.
    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation>;
}

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
