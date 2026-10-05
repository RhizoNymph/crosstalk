//! Transmissions in every state, with coherent evidence.
//!
//! A [`TransmissionBuilder`] lays out one channel transmission end to end: a
//! write by the sender, a read of the same resource by the reader
//! ([`LAG`] later), the co-access between
//! them, and content matches of the sender's spans found in the read's tool
//! result. Each state takes from that evidence what it holds, so a
//! suspected transmission's co-access and a confirmed one's matches always
//! agree with its sender, reader and timing.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ChannelId, SpanId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::observed::message::ToolCallId;
use crosstalk_spec::support::{ByteRange, NonEmpty, Timestamp};

use crate::build::error::BuildError;
use crate::build::provenance::{CrossAccessBuilder, LAG, MATCHED_BYTES};
use crate::ids::Ids;
use crate::time::{T0, after};

/// How long after the read the correlation window closes.
pub const WINDOW_CLOSES_AFTER: Duration = Duration::from_secs(300);

/// How long after being suspected a transmission is discarded.
pub const DISCARDED_AFTER: Duration = Duration::from_secs(3600);

/// A built transmission and the evidence it was built from.
#[derive(Debug, Clone, PartialEq)]
pub struct TransmissionParts {
    pub transmission: Transmission,
    /// The sender's write.
    pub write: Access,
    /// The reader's read, whose tool result holds the matches.
    pub read: Access,
    pub co_access: CoAccess,
    /// Every content match, sender to reader, in order.
    pub content: NonEmpty<ContentMatch>,
}

/// Builds a [`Transmission`] in any [`TransmissionStateKind`].
///
/// The default is a confirmed transmission between two fresh agents on a
/// fresh channel, opened at the read ([`T0`] plus the default lag), backed
/// by one exact 64-byte content match and the co-access. Classified and
/// aggregated states classify it under topic-model version 1 into a fresh
/// topic, unwatched.
#[derive(Debug, Clone, PartialEq)]
pub struct TransmissionBuilder {
    id: TransmissionId,
    from: AgentId,
    to: AgentId,
    route: Route,
    cross: CrossAccessBuilder,
    write_at: Timestamp,
    spans: NonEmpty<SpanId>,
    matched: NonZeroU32,
    state: TransmissionStateKind,
    classification: Classification,
}

impl TransmissionBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        Self {
            id: ids.transmission(),
            from: ids.agent(),
            to: ids.agent(),
            route: Route::Channel(ids.channel()),
            cross: CrossAccessBuilder::new(ids),
            write_at: T0,
            spans: NonEmpty::new(ids.span()),
            matched: MATCHED_BYTES,
            state: TransmissionStateKind::Confirmed,
            classification: Classification {
                version: TopicModelVersion(1),
                topic: Some(ids.topic()),
                watched: false,
            },
        }
    }

    pub fn id(&self) -> TransmissionId {
        self.id
    }

    pub fn with_id(mut self, id: TransmissionId) -> Self {
        self.id = id;
        self
    }

    /// The sender and the reader.
    pub fn between(mut self, from: AgentId, to: AgentId) -> Self {
        self.from = from;
        self.to = to;
        self
    }

    /// Routed through `channel`.
    pub fn channel(mut self, channel: ChannelId) -> Self {
        self.route = Route::Channel(channel);
        self
    }

    /// Any route. The evidence is laid out the same way.
    pub fn route(mut self, route: Route) -> Self {
        self.route = route;
        self
    }

    /// Opened at `at`: the read is at `at`, the write the default lag
    /// before it.
    pub fn opened_at(mut self, at: Timestamp) -> Self {
        let lag = u64::try_from(LAG.as_micros()).unwrap_or(u64::MAX);
        self.write_at = Timestamp::from_micros(at.as_micros().saturating_sub(lag));
        self
    }

    /// Adjust the write and read (resource, lag, window, exchanges).
    pub fn accesses(
        mut self,
        adjust: impl FnOnce(CrossAccessBuilder) -> CrossAccessBuilder,
    ) -> Self {
        self.cross = adjust(self.cross);
        self
    }

    /// One more content match, of a fresh span.
    pub fn with_match(mut self, ids: &mut Ids) -> Self {
        self.spans.push(ids.span());
        self
    }

    /// Bytes each content match covers.
    pub fn matched(mut self, bytes: NonZeroU32) -> Self {
        self.matched = bytes;
        self
    }

    pub fn state(mut self, state: TransmissionStateKind) -> Self {
        self.state = state;
        self
    }

    pub fn detected(self) -> Self {
        self.state(TransmissionStateKind::Detected)
    }

    pub fn awaiting_content(self) -> Self {
        self.state(TransmissionStateKind::AwaitingContent)
    }

    pub fn suspected(self) -> Self {
        self.state(TransmissionStateKind::Suspected)
    }

    pub fn confirmed(self) -> Self {
        self.state(TransmissionStateKind::Confirmed)
    }

    pub fn classified(self) -> Self {
        self.state(TransmissionStateKind::Classified)
    }

    pub fn aggregated(self) -> Self {
        self.state(TransmissionStateKind::Aggregated)
    }

    pub fn discarded(self) -> Self {
        self.state(TransmissionStateKind::Discarded)
    }

    /// The classification of a classified or aggregated transmission.
    pub fn classification(mut self, classification: Classification) -> Self {
        self.classification = classification;
        self
    }

    /// Classified under `version` into `topic` (`None`: an outlier).
    pub fn topic(mut self, version: TopicModelVersion, topic: Option<TopicId>) -> Self {
        self.classification.version = version;
        self.classification.topic = topic;
        self
    }

    pub fn watched(mut self, watched: bool) -> Self {
        self.classification.watched = watched;
        self
    }

    pub fn build(self) -> Result<Transmission, BuildError> {
        self.build_parts().map(|parts| parts.transmission)
    }

    /// The transmission with the write, read, co-access and matches it was
    /// built from, whatever its state holds.
    pub fn build_parts(self) -> Result<TransmissionParts, BuildError> {
        let cross = self
            .cross
            .writer(self.from)
            .reader(self.to)
            .write_at(self.write_at)
            .build()?;
        let part = match cross.read.op {
            AccessOp::Read { result } => result,
            AccessOp::Write { call, .. } => call,
        };
        let carrier = Carrier::ToolResult(ToolCallId(format!(
            "toolu_{}",
            cross.read.exchange.ulid_text()
        )));
        let read_range = ByteRange::new(0, self.matched.get())?;
        let content_match = |span: SpanId| {
            ContentMatch::new(
                span,
                self.from,
                self.to,
                cross.read.exchange,
                SpanLocation {
                    part,
                    range: read_range,
                },
                carrier.clone(),
                MatchKind::Exact,
                self.matched,
            )
        };
        let mut spans = self.spans.iter();
        let mut content = NonEmpty::new(content_match(*self.spans.first())?);
        spans.next();
        for span in spans {
            content.push(content_match(*span)?);
        }
        let read_at = cross.read.at;
        let closes_at = after(read_at, WINDOW_CLOSES_AFTER);
        let confirmed = || Confirmed::new(content.clone(), vec![cross.co_access], read_at);
        let state = match self.state {
            TransmissionStateKind::Detected => TransmissionState::Detected,
            TransmissionStateKind::AwaitingContent => TransmissionState::AwaitingContent {
                co_access: cross.co_access,
                window_closes_at: closes_at,
            },
            TransmissionStateKind::Suspected => TransmissionState::Suspected {
                co_access: NonEmpty::new(cross.co_access),
                since: closes_at,
            },
            TransmissionStateKind::Confirmed => TransmissionState::Confirmed(confirmed()?),
            TransmissionStateKind::Classified => TransmissionState::Classified {
                confirmed: confirmed()?,
                classification: self.classification.clone(),
            },
            TransmissionStateKind::Aggregated => TransmissionState::Aggregated {
                confirmed: confirmed()?,
                classification: self.classification.clone(),
            },
            TransmissionStateKind::Discarded => TransmissionState::Discarded {
                at: after(closes_at, DISCARDED_AFTER),
                co_access: NonEmpty::new(cross.co_access),
            },
        };
        Ok(TransmissionParts {
            transmission: Transmission {
                id: self.id,
                to: self.to,
                route: self.route,
                opened_at: read_at,
                state,
            },
            write: cross.write,
            read: cross.read,
            co_access: cross.co_access,
            content,
        })
    }
}
