//! Feeding the surface's evidence records from the bus.
//!
//! The evidence page reads spans, accesses and resources by id
//! ([`MemoryEvidence`]), which no reference store keeps. This stage fills
//! it from what L4 and L5 announce:
//!
//! - `SpanOriginated` / `SpanRelayed` name a span by id; its record comes
//!   from the [`SpanSource`] the L4 wiring supplies ([`NoSpans`] until
//!   then, so spans stay missing);
//! - `AccessRecorded` carries the access whole; its resource is read back
//!   from the channel registry's `resource_use` at the access's instant.
//!
//! Kept to this one module so it can switch to batch reads by id
//! (`SpanIndex::spans`, `AccessStore::accesses`) once those traits land,
//! instead of copying records here.

use std::future::Future;

use crosstalk_api::in_process::MemoryEvidence;
use crosstalk_memory::flow::MemoryChannels;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{ChannelId, SpanId};
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::stage::{Stage, StageError};

/// Why a span could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("span source unavailable: {reason}")]
pub struct SpanSourceError {
    pub reason: String,
}

/// Where span records are read by id: L4's span store.
pub trait SpanSource: Send + Sync + 'static {
    fn span(
        &self,
        id: SpanId,
    ) -> impl Future<Output = Result<Option<Span>, SpanSourceError>> + Send;
}

/// No span store yet: every span is missing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSpans;

impl SpanSource for NoSpans {
    async fn span(&self, _id: SpanId) -> Result<Option<Span>, SpanSourceError> {
        Ok(None)
    }
}

/// The evidence slot's stage.
pub struct EvidenceFeeder<S> {
    evidence: MemoryEvidence,
    channels: MemoryChannels<MemoryAgents>,
    spans: S,
}

impl<S: SpanSource> EvidenceFeeder<S> {
    pub fn new(evidence: MemoryEvidence, channels: MemoryChannels<MemoryAgents>, spans: S) -> Self {
        Self {
            evidence,
            channels,
            spans,
        }
    }

    async fn span(&self, id: SpanId) -> Result<(), StageError> {
        match self.spans.span(id).await {
            Ok(Some(span)) => {
                self.evidence.insert_span(span);
                Ok(())
            }
            Ok(None) => {
                tracing::debug!(span = %id.ulid_text(), "span not in the span source");
                Ok(())
            }
            Err(error) => Err(StageError::Retry {
                reason: error.to_string(),
            }),
        }
    }

    async fn access(&self, access: &Access, channel: ChannelId) -> Result<(), StageError> {
        self.evidence.insert_access(access.clone());
        let window = instant(access.at)?;
        let size = PageSize::new(PageSize::MAX).map_err(|error| StageError::Reject {
            reason: format!("page size: {error:?}"),
        })?;
        let mut request = PageRequest { size, after: None };
        loop {
            let page = self
                .channels
                .resource_use(channel, window, &request)
                .await
                .map_err(|error| StageError::Retry {
                    reason: format!("reading the resources of the access's channel: {error:?}"),
                })?;
            let (uses, next) = page.page.into_parts();
            if let Some(found) = uses
                .into_iter()
                .find(|used| used.resource().id == access.resource)
            {
                self.evidence.insert_resource(found.resource().clone());
                return Ok(());
            }
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        tracing::warn!(
            access = %access.id.ulid_text(),
            resource = %access.resource.ulid_text(),
            "the access's resource is not in the registry"
        );
        Ok(())
    }
}

/// The one-microsecond window holding `at`.
fn instant(at: Timestamp) -> Result<TimeWindow, StageError> {
    let end = Timestamp::from_micros(at.as_micros().saturating_add(1));
    TimeWindow::new(at, end).map_err(|_| StageError::Reject {
        reason: "the access is at the last representable instant".to_owned(),
    })
}

impl<S: SpanSource> Stage for EvidenceFeeder<S> {
    fn subjects(&self) -> Vec<Subject> {
        vec![
            Subject::SpanOriginated,
            Subject::SpanRelayed,
            Subject::AccessRecorded,
        ]
    }

    async fn handle(&mut self, envelope: &Envelope) -> Result<(), StageError> {
        match &envelope.event {
            BusEvent::Detect(DetectEvent::SpanOriginated { span, .. })
            | BusEvent::Detect(DetectEvent::SpanRelayed { span, .. }) => self.span(*span).await,
            BusEvent::Detect(DetectEvent::AccessRecorded { access, channel }) => {
                self.access(access, *channel).await
            }
            _ => Ok(()),
        }
    }
}
