//! Recording accesses: the locator's resource, on the channel its lookup
//! names, then the access, then `AccessRecorded`, then correlation.
//!
//! A resource's id is derived from its locator and first sighting, so a
//! redelivered access resolves to the same id; a resource another
//! sighting stored first is found through `DuplicateLocator`. A resource
//! stored on no channel that a declared pattern now claims joins that
//! channel. Whenever an access resolves to a channel, any evidence still
//! held for its resource moves to the channel's medium first (a declared
//! pattern took it in, or another consumer discovered its channel).

use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::channels::{ChannelTraffic, TrafficError};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::{ChannelLookup, ChannelRegistry};
use crosstalk_spec::support::Timestamp;

use super::input::{Observed, ReadResult, WriteCall};
use super::{FlowConsumer, Step, StepError, decisions};
use crate::correlate::pairing::{self, WriteOutcome};
use crate::correlate::{Derive, MediumKey};

/// Where a locator's resource is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Resolved {
    resource: ResourceId,
    channel: Option<ChannelId>,
}

/// The id of a resource first seen at `at` with `locator`.
pub(crate) fn resource_id(locator: &Locator, at: Timestamp) -> ResourceId {
    let bytes = serde_json::to_vec(locator).unwrap_or_else(|_| format!("{locator:?}").into_bytes());
    ResourceId::from_ulid(Derive::new("crosstalk.flow.resource").bytes(&bytes).at(at))
}

impl<R, T, A, B> FlowConsumer<R, T, A, B>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
{
    pub(super) async fn record_read(
        &mut self,
        read: Observed<ReadResult>,
    ) -> Result<Vec<Step>, StepError> {
        let op = AccessOp::Read {
            result: read.op.result,
        };
        self.record(read.map_op(()), op, None).await
    }

    pub(super) async fn record_write(
        &mut self,
        write: Observed<WriteCall>,
        outcome: WriteOutcome,
    ) -> Result<Vec<Step>, StepError> {
        let op = pairing::write_op(write.op.call, write.op.spans.clone(), outcome);
        self.record(write.map_op(()), op, Some(outcome)).await
    }

    /// Store the resource, record the access, announce it, and hand it to
    /// the correlator unless it is a write that does not pair.
    async fn record(
        &mut self,
        observed: Observed<()>,
        op: AccessOp,
        outcome: Option<WriteOutcome>,
    ) -> Result<Vec<Step>, StepError> {
        let resolved = self.resolve(&observed.locator, observed.at).await?;
        // Evidence still held for the resource (it joined a declared
        // channel, or another consumer discovered its channel) moves to the
        // channel's medium before this access is correlated.
        let mut steps = Vec::new();
        if let Some(channel) = resolved.channel {
            steps.extend(decisions(self.shards.rekey(
                MediumKey::Resource(resolved.resource),
                MediumKey::Channel(channel),
            )));
        }
        let access = Access {
            id: observed.id,
            agent: observed.agent,
            exchange: observed.exchange,
            resource: resolved.resource,
            at: observed.at,
            via: observed.via,
            op,
        };
        match self.registry.record_access(access.clone()).await {
            Ok(()) => {}
            Err(TrafficError::DuplicateAccess(id)) => {
                tracing::debug!(access = %id.ulid_text(), "access already recorded");
            }
            Err(error) => return Err(error.into()),
        }
        steps.push(Step::Publish(BusEvent::Detect(
            DetectEvent::AccessRecorded {
                access: access.clone(),
                channel: resolved.channel,
            },
        )));
        if outcome.is_none_or(WriteOutcome::pairs) {
            steps.push(Step::Correlate(access, resolved.channel));
        } else {
            tracing::debug!(access = %access.id.ulid_text(), "rejected write recorded, not correlated");
        }
        Ok(steps)
    }

    async fn resolve(&mut self, locator: &Locator, at: Timestamp) -> Result<Resolved, StepError> {
        let id = resource_id(locator, at);
        let fresh = Resource {
            id,
            locator: locator.clone(),
            first_seen: at,
        };
        match self.registry.add_resource(fresh).await {
            Ok(channel) => Ok(Resolved {
                resource: id,
                channel,
            }),
            Err(TrafficError::DuplicateLocator { existing, lookup }) => {
                self.placed(existing, locator, at, lookup).await
            }
            Err(TrafficError::DuplicateResource(existing)) => {
                let lookup = self.registry.lookup(locator).await?;
                self.placed(existing, locator, at, lookup).await
            }
            Err(error) => Err(error.into()),
        }
    }

    /// The stored resource `existing`, whose lookup is `lookup`.
    async fn placed(
        &mut self,
        existing: ResourceId,
        locator: &Locator,
        at: Timestamp,
        lookup: ChannelLookup,
    ) -> Result<Resolved, StepError> {
        match lookup {
            ChannelLookup::Known(channel) => Ok(Resolved {
                resource: existing,
                channel: Some(channel),
            }),
            ChannelLookup::NoChannel => Ok(Resolved {
                resource: existing,
                channel: None,
            }),
            ChannelLookup::Declared(_) => {
                let stored = Resource {
                    id: existing,
                    locator: locator.clone(),
                    first_seen: at,
                };
                let channel = self.registry.add_resource(stored).await?;
                Ok(Resolved {
                    resource: existing,
                    channel,
                })
            }
        }
    }
}

impl<Op> Observed<Op> {
    /// The same access with another operation.
    pub fn map_op<To>(self, op: To) -> Observed<To> {
        Observed {
            id: self.id,
            agent: self.agent,
            exchange: self.exchange,
            at: self.at,
            locator: self.locator,
            via: self.via,
            op,
        }
    }
}
