//! Evidence for correlator and consumer tests: accesses, content matches
//! and a timing, built through the testkit and the spec's checked
//! constructors.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::{AgentId, ExchangeId, ResourceId, SpanId};
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::flow::AccessBuilder;
use crosstalk_testkit::build::provenance::ContentMatchBuilder;
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::{T0, after};

/// Correlation window 10 min, evidence window 1 min, suspected TTL 5 min.
pub(crate) fn timing() -> CorrelationTiming {
    match CorrelationTiming::new(
        Duration::from_secs(600),
        Duration::from_secs(60),
        Duration::from_secs(300),
    ) {
        Ok(timing) => timing,
        Err(error) => panic!("test timing: {error:?}"),
    }
}

/// `T0 + seconds`.
pub(crate) fn secs(seconds: u64) -> Timestamp {
    after(T0, Duration::from_secs(seconds))
}

/// Builds evidence from one id generator.
pub(crate) struct Scene {
    pub(crate) ids: Ids,
}

impl Scene {
    pub(crate) fn new(seed: u32) -> Self {
        Self {
            ids: Ids::seeded(seed),
        }
    }

    pub(crate) fn agent(&mut self) -> AgentId {
        self.ids.agent()
    }

    pub(crate) fn resource(&mut self) -> ResourceId {
        self.ids.resource()
    }

    pub(crate) fn exchange(&mut self) -> ExchangeId {
        self.ids.exchange()
    }

    pub(crate) fn span(&mut self) -> SpanId {
        self.ids.span()
    }

    /// A write by `agent` of `resource` at `at` in a fresh exchange, its
    /// arguments holding `spans`.
    pub(crate) fn write(
        &mut self,
        agent: AgentId,
        resource: ResourceId,
        at: Timestamp,
        spans: Vec<SpanId>,
    ) -> Access {
        let exchange = self.exchange();
        AccessBuilder::new(&mut self.ids)
            .by(agent)
            .on(resource)
            .at(at)
            .in_exchange(exchange)
            .write_spans(spans)
            .build()
    }

    /// A read by `agent` of `resource` at `at` in a fresh exchange.
    pub(crate) fn read(&mut self, agent: AgentId, resource: ResourceId, at: Timestamp) -> Access {
        let exchange = self.exchange();
        self.read_in(agent, resource, at, exchange)
    }

    /// A read by `agent` of `resource` in `exchange`, which started at `at`.
    pub(crate) fn read_in(
        &mut self,
        agent: AgentId,
        resource: ResourceId,
        at: Timestamp,
        exchange: ExchangeId,
    ) -> Access {
        AccessBuilder::new(&mut self.ids)
            .by(agent)
            .on(resource)
            .at(at)
            .in_exchange(exchange)
            .read()
            .build()
    }

    /// A match of `origin` (by `from`) in `read`'s tool result.
    pub(crate) fn carried(&mut self, read: &Access, from: AgentId, origin: SpanId) -> ContentMatch {
        let AccessOp::Read { result } = read.op else {
            panic!("carried: not a read");
        };
        self.matched(
            from,
            read.agent,
            read.exchange,
            origin,
            result,
            tool_result(read.exchange),
        )
    }

    /// A match of `origin` (by `from`) in `to`'s `exchange`, carried by
    /// `carrier`, in a fresh part.
    pub(crate) fn found(
        &mut self,
        from: AgentId,
        to: AgentId,
        exchange: ExchangeId,
        origin: SpanId,
        carrier: Carrier,
    ) -> ContentMatch {
        let part = PartRef {
            message: self.ids.message(),
            index: 0,
        };
        self.matched(from, to, exchange, origin, part, carrier)
    }

    /// The same match, `bytes` long, at another offset: a second match of
    /// the same span in the same part.
    pub(crate) fn again(&mut self, content: &ContentMatch, start: u32) -> ContentMatch {
        let bytes = NonZeroU32::MIN.saturating_add(31);
        match ContentMatchBuilder::new(&mut self.ids)
            .from(content.origin_agent())
            .to(content.reader())
            .origin(content.origin())
            .reader_exchange(content.reader_exchange())
            .part(content.read_at().part)
            .carrier(content.carrier().clone())
            .range(start, bytes)
            .matched(bytes)
            .build()
        {
            Ok(content) => content,
            Err(error) => panic!("match: {error:?}"),
        }
    }

    pub(crate) fn matched(
        &mut self,
        from: AgentId,
        to: AgentId,
        exchange: ExchangeId,
        origin: SpanId,
        part: PartRef,
        carrier: Carrier,
    ) -> ContentMatch {
        match ContentMatchBuilder::new(&mut self.ids)
            .from(from)
            .to(to)
            .origin(origin)
            .reader_exchange(exchange)
            .part(part)
            .carrier(carrier)
            .build()
        {
            Ok(content) => content,
            Err(error) => panic!("match: {error:?}"),
        }
    }
}

/// The tool-result carrier of a call in `exchange`.
pub(crate) fn tool_result(exchange: ExchangeId) -> Carrier {
    Carrier::ToolResult(ToolCallId(format!("toolu_{}", exchange.ulid_text())))
}
