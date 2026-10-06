//! Applying the correlator's decisions: each becomes the transmission's
//! next stored state (`TransmissionStore::save`), then, for a channel
//! route, the channel's traffic (`ChannelTraffic::record_transmission`),
//! then the event announcing it.
//!
//! An update is applied only when the stored state admits it
//! ([`lifecycle::advance`]), so a stored transmission never moves back. A
//! decision whose state is already stored (its save committed but a later
//! step failed) goes on to the steps after the save.
//!
//! **Discovery.** `OpenChannel` on a resource on no channel first
//! discovers the channel (`ChannelTraffic::discover`, under an id derived
//! from the resource), then hands the resource's correlator evidence to
//! the channel's medium before anything else runs
//! (`flow.correlator.resource-shard-handoff`), and stores the transmission
//! routed through the channel `discover` returned.

use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, NonChannelRoute, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::EventBus;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::{
    ChannelRegistry, Discovery, OpensOn, TransmissionUpdate,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::durability::FlowDurability;
use super::{FlowConsumer, Step, StepError, decisions};
use crate::correlate::lifecycle::{self, Stage, UpdateKind};
use crate::correlate::{Decided, Derive, MediumKey};

/// The id a channel discovered from `resource` by a transmission opened at
/// `at` is created under.
pub(crate) fn discovered_channel_id(resource: ResourceId, at: Timestamp) -> ChannelId {
    ChannelId::from_ulid(
        Derive::new("crosstalk.flow.discovered-channel")
            .ulid(resource.as_ulid())
            .at(at),
    )
}

fn confirmed_event(transmission: &Transmission, confirmed: &Confirmed) -> BusEvent {
    BusEvent::Detect(DetectEvent::TransmissionConfirmed {
        transmission: transmission.id,
        from: confirmed.from(),
        to: transmission.to,
        route: transmission.route.clone(),
        at: confirmed.at(),
        matched_bytes: confirmed.matched_bytes(),
    })
}

/// The record step for a channel transmission; none for another route.
fn record(transmission: &Transmission) -> Option<Step> {
    matches!(transmission.route, Route::Channel(_)).then(|| Step::Record(transmission.clone()))
}

impl<R, T, A, B, D> FlowConsumer<R, T, A, B, D>
where
    R: ChannelRegistry + ChannelTraffic + Send + Sync,
    T: TransmissionStore + Send + Sync,
    A: AgentReads + Send + Sync,
    B: EventBus + Send + Sync,
    D: FlowDurability,
{
    pub(super) async fn decide(&mut self, decided: Decided) -> Result<Vec<Step>, StepError> {
        let opened_at = decided.opened_at;
        match decided.update {
            TransmissionUpdate::OpenChannel {
                transmission,
                to,
                on,
                co_access,
            } => {
                self.open_channel(transmission, to, on, co_access, opened_at)
                    .await
            }
            TransmissionUpdate::OpenConfirmed {
                transmission,
                to,
                route,
                confirmed,
            } => {
                self.open_confirmed(transmission, to, route, confirmed)
                    .await
            }
            TransmissionUpdate::Confirm {
                transmission,
                confirmed,
            } => self.confirm(transmission, confirmed).await,
            TransmissionUpdate::Extend {
                transmission,
                content,
            } => self.extend(transmission, content).await,
            TransmissionUpdate::Suspect {
                transmission,
                co_access,
            } => self.suspect(transmission, co_access).await,
            TransmissionUpdate::Discard { transmission } => self.discard(transmission).await,
        }
    }

    async fn stored(&self, id: TransmissionId) -> Result<Transmission, StepError> {
        self.transmissions
            .transmission(id)
            .await?
            .ok_or(StepError::MissingTransmission(id))
    }

    /// Whether `update` may follow `stored`'s state; logs when it may not.
    fn admits(stored: &Transmission, update: UpdateKind) -> bool {
        match lifecycle::advance(Stage::of(&stored.state), update) {
            Ok(_) => true,
            Err(illegal) => {
                tracing::debug!(transmission = %stored.id.ulid_text(), ?illegal, "update not admitted by the stored state; skipped");
                false
            }
        }
    }

    async fn open_channel(
        &mut self,
        id: TransmissionId,
        to: AgentId,
        on: OpensOn,
        co_access: CoAccess,
        opened_at: Timestamp,
    ) -> Result<Vec<Step>, StepError> {
        let timing = self.settings.timing;
        let state = TransmissionState::AwaitingContent {
            co_access,
            window_closes_at: timing.window_closes_at(opened_at),
        };
        if let Some(stored) = self.transmissions.transmission(id).await? {
            // Opened before (a redelivery, or a restore re-feeding its
            // accesses): its resource's evidence still goes to the channel
            // it was routed through, as when it was first opened.
            let handed = match (on, &stored.route) {
                (OpensOn::Resource(resource), Route::Channel(channel)) => decisions(
                    self.shards
                        .rekey(MediumKey::Resource(resource), MediumKey::Channel(*channel)),
                ),
                _ => Vec::new(),
            };
            let mut steps = if stored.state == state {
                self.announce_open(&stored, co_access)
            } else {
                Vec::new()
            };
            steps.extend(handed);
            return Ok(steps);
        }
        let mut handed = Vec::new();
        let channel = match on {
            OpensOn::Channel(channel) => channel,
            OpensOn::Resource(resource) => {
                let minted = discovered_channel_id(resource, opened_at);
                let channel = match self
                    .registry
                    .discover(minted, resource, id, opened_at)
                    .await?
                {
                    Discovery::Created(channel) => {
                        tracing::info!(channel = %channel.ulid_text(), resource = %resource.ulid_text(), transmission = %id.ulid_text(), "channel discovered");
                        channel
                    }
                    Discovery::Existing(channel) => channel,
                };
                handed = decisions(
                    self.shards
                        .rekey(MediumKey::Resource(resource), MediumKey::Channel(channel)),
                );
                channel
            }
        };
        let transmission = Transmission {
            id,
            to,
            route: Route::Channel(channel),
            opened_at,
            state,
        };
        self.transmissions.save(transmission.clone()).await?;
        let mut steps = self.announce_open(&transmission, co_access);
        steps.extend(handed);
        Ok(steps)
    }

    fn announce_open(&self, transmission: &Transmission, co_access: CoAccess) -> Vec<Step> {
        let Route::Channel(channel) = transmission.route else {
            return Vec::new();
        };
        vec![
            Step::Record(transmission.clone()),
            Step::Publish(BusEvent::Detect(DetectEvent::ChannelCrossAccessed {
                channel,
                co_access,
                reader: transmission.to,
            })),
        ]
    }

    async fn open_confirmed(
        &mut self,
        id: TransmissionId,
        to: AgentId,
        route: NonChannelRoute,
        confirmed: Confirmed,
    ) -> Result<Vec<Step>, StepError> {
        let state = TransmissionState::Confirmed(confirmed.clone());
        if let Some(stored) = self.transmissions.transmission(id).await? {
            return if stored.state == state {
                Ok(vec![Step::Publish(confirmed_event(&stored, &confirmed))])
            } else {
                self.reconfirm(stored, &confirmed).await
            };
        }
        let transmission = Transmission {
            id,
            to,
            route: route.into(),
            opened_at: confirmed.at(),
            state,
        };
        self.transmissions.save(transmission.clone()).await?;
        Ok(vec![Step::Publish(confirmed_event(
            &transmission,
            &confirmed,
        ))])
    }

    async fn confirm(
        &mut self,
        id: TransmissionId,
        confirmed: Confirmed,
    ) -> Result<Vec<Step>, StepError> {
        let stored = self.stored(id).await?;
        let state = TransmissionState::Confirmed(confirmed.clone());
        if stored.state != state {
            if Stage::of(&stored.state) == Some(Stage::Confirmed) {
                return self.reconfirm(stored, &confirmed).await;
            }
            if !Self::admits(&stored, UpdateKind::Confirm) {
                return Ok(Vec::new());
            }
            self.transmissions
                .save(Transmission {
                    state: state.clone(),
                    ..stored.clone()
                })
                .await?;
        }
        let transmission = Transmission { state, ..stored };
        Ok(record(&transmission)
            .into_iter()
            .chain([Step::Publish(confirmed_event(&transmission, &confirmed))])
            .collect())
    }

    /// A confirmation decided again for a transmission the store holds
    /// confirmed with other content: a restored correlator that saw the
    /// matches of the stored confirmation and of its extensions at once
    /// confirms with all of them. The stored confirmation's event is
    /// published again (under its own id: a no-op unless it was lost), and
    /// the matches it lacks extend it, as the extensions would have.
    async fn reconfirm(
        &mut self,
        mut transmission: Transmission,
        confirmed: &Confirmed,
    ) -> Result<Vec<Step>, StepError> {
        let before = transmission.clone();
        let stored = match &mut transmission.state {
            TransmissionState::Confirmed(stored)
            | TransmissionState::Classified {
                confirmed: stored, ..
            }
            | TransmissionState::Aggregated {
                confirmed: stored, ..
            } => stored,
            TransmissionState::Detected
            | TransmissionState::AwaitingContent { .. }
            | TransmissionState::Suspected { .. }
            | TransmissionState::Discarded { .. } => return Ok(Vec::new()),
        };
        let announced = confirmed_event(&before, stored);
        let mut changed = false;
        for content in confirmed.content().iter() {
            if !stored.content().iter().any(|known| known == content) {
                match stored.extend(content.clone()) {
                    Ok(()) => changed = true,
                    Err(mixed) => {
                        tracing::warn!(transmission = %transmission.id.ulid_text(), error = ?mixed, "match not extended");
                    }
                }
            }
        }
        if changed {
            self.transmissions.save(transmission.clone()).await?;
        }
        Ok(record(&transmission)
            .into_iter()
            .chain([Step::Publish(announced)])
            .collect())
    }

    async fn extend(
        &mut self,
        id: TransmissionId,
        content: ContentMatch,
    ) -> Result<Vec<Step>, StepError> {
        let mut transmission = self.stored(id).await?;
        if !Self::admits(&transmission, UpdateKind::Extend) {
            return Ok(Vec::new());
        }
        let confirmed = match &mut transmission.state {
            TransmissionState::Confirmed(confirmed)
            | TransmissionState::Classified { confirmed, .. }
            | TransmissionState::Aggregated { confirmed, .. } => confirmed,
            TransmissionState::Detected
            | TransmissionState::AwaitingContent { .. }
            | TransmissionState::Suspected { .. }
            | TransmissionState::Discarded { .. } => return Ok(Vec::new()),
        };
        if !confirmed.content().iter().any(|known| *known == content) {
            if let Err(mixed) = confirmed.extend(content) {
                tracing::warn!(transmission = %id.ulid_text(), error = ?mixed, "match not extended");
                return Ok(Vec::new());
            }
            self.transmissions.save(transmission.clone()).await?;
        }
        Ok(record(&transmission).into_iter().collect())
    }

    async fn suspect(
        &mut self,
        id: TransmissionId,
        co_access: NonEmpty<CoAccess>,
    ) -> Result<Vec<Step>, StepError> {
        let stored = self.stored(id).await?;
        let since = match &stored.state {
            TransmissionState::AwaitingContent {
                window_closes_at, ..
            } => *window_closes_at,
            TransmissionState::Suspected {
                co_access: held,
                since,
            } if *held == co_access => *since,
            _ => {
                Self::admits(&stored, UpdateKind::Suspect);
                return Ok(Vec::new());
            }
        };
        let state = TransmissionState::Suspected {
            co_access: co_access.clone(),
            since,
        };
        if stored.state != state {
            self.transmissions
                .save(Transmission {
                    state: state.clone(),
                    ..stored.clone()
                })
                .await?;
        }
        let transmission = Transmission { state, ..stored };
        let Route::Channel(channel) = transmission.route else {
            return Ok(Vec::new());
        };
        Ok(vec![
            Step::Record(transmission.clone()),
            Step::Publish(BusEvent::Detect(DetectEvent::TransmissionSuspected {
                transmission: id,
                to: transmission.to,
                channel,
                co_access,
            })),
        ])
    }

    async fn discard(&mut self, id: TransmissionId) -> Result<Vec<Step>, StepError> {
        let mut transmission = self.stored(id).await?;
        let since = match &transmission.state {
            TransmissionState::Suspected { since, .. } => *since,
            TransmissionState::Discarded { .. } => {
                return Ok(record(&transmission).into_iter().collect());
            }
            _ => {
                Self::admits(&transmission, UpdateKind::Discard);
                return Ok(Vec::new());
            }
        };
        let expired_at = self.settings.timing.expires_at(since);
        if transmission.state.expire(expired_at).is_err() {
            return Ok(Vec::new());
        }
        self.transmissions.save(transmission.clone()).await?;
        Ok(record(&transmission).into_iter().collect())
    }
}
