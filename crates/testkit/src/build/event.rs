//! Bus events and their envelopes.
//!
//! The free functions wrap a payload in its layer's [`BusEvent`] variant;
//! [`EnvelopeBuilder`] stamps one with a fresh event id and a time.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::alert::{Alert, AlertRevision};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::transmission::{Transmission, TransmissionState};
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::{ConversationDelta, IngestEvent};
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, EventId};
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::support::{Timestamp, Watermark};

use crate::ids::Ids;
use crate::time::T0;

pub fn exchange_captured(exchange: Exchange) -> BusEvent {
    BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange)))
}

pub fn conversation_delta(delta: ConversationDelta) -> BusEvent {
    BusEvent::Ingest(IngestEvent::ConversationDelta(delta))
}

pub fn agent_seen(agent: AgentId, evidence: IdentityEvidence) -> BusEvent {
    BusEvent::Ingest(IngestEvent::AgentSeen { agent, evidence })
}

pub fn content_matched(content: ContentMatch) -> BusEvent {
    BusEvent::Detect(DetectEvent::ContentMatched(content))
}

pub fn access_recorded(access: Access, channel: ChannelId) -> BusEvent {
    BusEvent::Detect(DetectEvent::AccessRecorded { access, channel })
}

pub fn channel_discovered(channel: ChannelId, first_access: AccessId) -> BusEvent {
    BusEvent::Detect(DetectEvent::ChannelDiscovered {
        channel,
        first_access,
    })
}

/// `TransmissionConfirmed` for a transmission holding content evidence
/// (confirmed, classified or aggregated); `None` for any other state.
pub fn transmission_confirmed(transmission: &Transmission) -> Option<BusEvent> {
    let confirmed = transmission.state.confirmed()?;
    Some(BusEvent::Detect(DetectEvent::TransmissionConfirmed {
        transmission: transmission.id,
        from: confirmed.from(),
        to: transmission.to,
        route: transmission.route.clone(),
        at: confirmed.at(),
        matched_bytes: confirmed.matched_bytes(),
    }))
}

/// `TransmissionClassified` for a classified or aggregated transmission;
/// `None` for any other state.
pub fn transmission_classified(
    transmission: &Transmission,
    cause: ClassificationCause,
) -> Option<BusEvent> {
    let (confirmed, classification) = match &transmission.state {
        TransmissionState::Classified {
            confirmed,
            classification,
        }
        | TransmissionState::Aggregated {
            confirmed,
            classification,
        } => (confirmed, classification),
        TransmissionState::Detected
        | TransmissionState::AwaitingContent { .. }
        | TransmissionState::Suspected { .. }
        | TransmissionState::Confirmed(_)
        | TransmissionState::Discarded { .. } => return None,
    };
    Some(BusEvent::Insight(InsightEvent::TransmissionClassified {
        cause,
        transmission: transmission.id,
        from: confirmed.from(),
        to: transmission.to,
        route: transmission.route.clone(),
        at: confirmed.at(),
        matched_bytes: confirmed.matched_bytes(),
        classification: classification.clone(),
    }))
}

pub fn topic_version_ready(version: TopicModelVersion, transmissions: u64) -> BusEvent {
    BusEvent::Insight(InsightEvent::TopicVersionReady {
        version,
        transmissions,
    })
}

pub fn watermark_advanced(watermark: Watermark) -> BusEvent {
    BusEvent::Insight(InsightEvent::WatermarkAdvanced(watermark))
}

pub fn alert_opened(alert: Alert) -> BusEvent {
    BusEvent::Insight(InsightEvent::AlertOpened(alert))
}

pub fn alert_changed(alert: Alert, revision: AlertRevision) -> BusEvent {
    BusEvent::Insight(InsightEvent::AlertChanged { alert, revision })
}

pub fn policy_changed(channel: ChannelId, policy: Policy) -> BusEvent {
    BusEvent::Insight(InsightEvent::PolicyChanged { channel, policy })
}

pub fn changed(changed: Changed) -> BusEvent {
    BusEvent::Changed(changed)
}

/// Matched bytes as the confirmed events carry them.
pub fn matched_bytes(bytes: u64) -> NonZeroU64 {
    NonZeroU64::new(bytes).unwrap_or(NonZeroU64::MIN)
}

/// Builds an [`Envelope`]: a fresh event id, at [`T0`] unless set.
#[derive(Debug, Clone, PartialEq)]
pub struct EnvelopeBuilder {
    id: EventId,
    at: Timestamp,
    event: BusEvent,
}

impl EnvelopeBuilder {
    pub fn new(ids: &mut Ids, event: BusEvent) -> Self {
        Self {
            id: ids.event(),
            at: T0,
            event,
        }
    }

    pub fn id(&self) -> EventId {
        self.id
    }

    /// The same id: a redelivery, for idempotence tests.
    pub fn with_id(mut self, id: EventId) -> Self {
        self.id = id;
        self
    }

    pub fn at(mut self, at: Timestamp) -> Self {
        self.at = at;
        self
    }

    pub fn build(self) -> Envelope {
        Envelope {
            id: self.id,
            at: self.at,
            event: self.event,
        }
    }
}
