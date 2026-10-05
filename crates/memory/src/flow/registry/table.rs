//! L5's channel registry as plain data, and every operation on it as a
//! function of that data. Each write checks before it changes anything.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{AgentAccesses, ResourceUse};
use crosstalk_spec::derived::flow::access::{Access, AccessKind};
use crosstalk_spec::derived::flow::channel::detection::DeclaredDetection;
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{
    Promotion, PromotionCoverage, PromotionRefusal, Registered, coverage, plan,
};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory,
};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::{ChannelLookup, Promoted, RegistryError};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crosstalk_spec::interfaces::l5_flow::channels::{DetectionUpdate, TrafficError};

/// A resource and the channel it is stored on, `None` for none: a resource
/// only, until a cross-agent transmission through it discovers a channel or
/// a declared pattern claims it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredResource {
    pub(crate) resource: Resource,
    pub(crate) channel: Option<ChannelId>,
}

/// Everything the registry stores.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ChannelTable {
    pub(crate) channels: BTreeMap<ChannelId, Channel>,
    pub(crate) histories: BTreeMap<ChannelId, PolicyHistory>,
    pub(crate) resources: BTreeMap<ResourceId, StoredResource>,
    pub(crate) accesses: BTreeMap<AccessId, Access>,
    /// Every channel transmission as last recorded
    /// (`ChannelTraffic::record_transmission`).
    pub(crate) transmissions: BTreeMap<TransmissionId, Transmission>,
}

pub(crate) fn changed(channel: ChannelId) -> BusEvent {
    BusEvent::Changed(Changed::Channel(channel))
}

impl ChannelTable {
    /// `ChannelDirectory::canonical`.
    pub(crate) fn canonical(&self, id: ChannelId) -> ChannelId {
        self.channels.get(&id).map_or(id, Channel::canonical)
    }

    pub(crate) fn channel(&self, id: ChannelId) -> Result<&Channel, RegistryError> {
        self.channels
            .get(&id)
            .ok_or(RegistryError::UnknownChannel(id))
    }

    fn seed_locator(&self, channel: &Channel) -> Option<&Locator> {
        let seed = channel.origin.seed()?;
        self.resources
            .get(&seed.resource)
            .map(|stored| &stored.resource.locator)
    }

    /// What promotion reads about every channel, in registry order
    /// (ascending id).
    pub(crate) fn registered(&self) -> Vec<Registered<'_>> {
        self.channels
            .values()
            .map(|channel| Registered {
                channel,
                seed: self.seed_locator(channel),
            })
            .collect()
    }

    /// The resources stored on `channel`: its seed and its own resources.
    fn held(&self, channel: ChannelId) -> Vec<Resource> {
        let Some(stored) = self.channels.get(&channel) else {
            return Vec::new();
        };
        let seed = stored.origin.seed().map(|seed| seed.resource);
        seed.into_iter()
            .chain(stored.resources.iter().copied())
            .filter_map(|id| self.resources.get(&id))
            .map(|stored| stored.resource.clone())
            .collect()
    }

    /// The canonical channel `id` resolves to and every channel it
    /// superseded.
    pub(crate) fn members(&self, canonical: ChannelId) -> Vec<ChannelId> {
        self.channels
            .values()
            .filter(|channel| channel.canonical() == canonical)
            .map(|channel| channel.id)
            .collect()
    }

    // ---- the trait's operations ----------------------------------------------

    /// `ChannelRegistry::lookup`: `Known` on the canonical channel of a
    /// stored resource with that locator on a channel, else (unstored, or
    /// stored on no channel) `Declared` for a declared pattern matching it,
    /// else `NoChannel`. Creates nothing.
    pub(crate) fn lookup(&self, locator: &Locator) -> ChannelLookup {
        if let Some(channel) = self
            .resources
            .values()
            .find(|stored| stored.resource.locator == *locator)
            .and_then(|stored| stored.channel)
        {
            return ChannelLookup::Known(self.canonical(channel));
        }
        self.channels
            .values()
            .find(|channel| {
                channel
                    .origin
                    .pattern()
                    .is_some_and(|pattern| pattern.matches(locator))
            })
            .map_or(ChannelLookup::NoChannel, |channel| {
                ChannelLookup::Declared(channel.id)
            })
    }

    /// `ChannelRegistry::declare`, at `at`. The id is drawn from `next_id`
    /// only once the declaration is accepted.
    pub(crate) fn declare(
        &mut self,
        pattern: ResourcePattern,
        policy: Policy,
        by: PolicyAuthor,
        at: Timestamp,
        next_id: impl FnOnce() -> ChannelId,
    ) -> Result<(ChannelId, Vec<BusEvent>), RegistryError> {
        if let Some(existing) = self.channels.values().find(|channel| {
            channel
                .origin
                .pattern()
                .is_some_and(|declared| declared.overlaps(&pattern))
        }) {
            return Err(RegistryError::OverlappingDeclaration {
                existing: existing.id,
            });
        }
        let id = next_id();
        let mut history = PolicyHistory::empty();
        if let Ok(decision) = PolicyDecision::try_from(policy) {
            history.record(decision);
        }
        let channel = Channel {
            id,
            origin: ChannelOrigin::Declared {
                declaration: Declaration { pattern, by, at },
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            },
            resources: Vec::new(),
            policy: history.current(),
        };
        self.channels.insert(id, channel);
        self.histories.insert(id, history);
        Ok((id, vec![changed(id)]))
    }

    /// `ChannelRegistry::set_policy`.
    pub(crate) fn set_policy(
        &mut self,
        id: ChannelId,
        decision: PolicyDecision,
    ) -> Result<(Recorded, Vec<BusEvent>), RegistryError> {
        if let Some(supersession) = self.channel(id)?.origin.supersession() {
            return Err(RegistryError::Superseded {
                channel: id,
                by: supersession.by,
            });
        }
        let history = self.histories.entry(id).or_default();
        let recorded = history.record(decision);
        let current = history.current();
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.policy = current;
        }
        let events = match recorded {
            Recorded::Duplicate => Vec::new(),
            Recorded::Current | Recorded::Superseded => vec![changed(id)],
        };
        Ok((recorded, events))
    }

    /// `ChannelRegistry::policy_history`.
    pub(crate) fn policy_history(&self, id: ChannelId) -> Result<PolicyHistory, RegistryError> {
        self.channel(id)?;
        Ok(self.histories.get(&id).cloned().unwrap_or_default())
    }

    /// `ChannelRegistry::promote`.
    pub(crate) fn promote(
        &mut self,
        id: ChannelId,
        promotion: Promotion,
    ) -> Result<(Promoted, Vec<BusEvent>), PromotionRefusal> {
        let plan = plan(id, promotion.declaration(), &self.registered())?;
        let superseded: Vec<ChannelId> = plan.superseded_ids().collect();
        let history = self.histories.entry(id).or_default();
        let recorded = history.record(promotion.decision().clone());
        let current = history.current();
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.origin = plan.origin;
            channel.policy = current;
        }
        for (other, origin) in plan.superseded {
            if let Some(channel) = self.channels.get_mut(&other) {
                channel.origin = origin;
            }
        }
        let events = std::iter::once(BusEvent::Detect(DetectEvent::ChannelPromoted {
            channel: id,
            declaration: promotion.declaration().clone(),
            policy: promotion.decision().clone(),
            superseded: superseded.clone(),
        }))
        .chain(
            Changed::promotion(id, &superseded)
                .into_iter()
                .map(BusEvent::Changed),
        )
        .collect();
        Ok((
            Promoted {
                superseded,
                policy: recorded,
            },
            events,
        ))
    }

    /// `ChannelRegistry::promotion_coverage`.
    pub(crate) fn coverage(
        &self,
        id: ChannelId,
        declaration: &Declaration,
    ) -> Result<PromotionCoverage, PromotionRefusal> {
        coverage(id, declaration, &self.registered(), |channel| {
            self.held(channel)
        })
    }

    /// `ChannelRegistry::resource_use`, before paging: the canonical
    /// channel and its resources accessed in `window`, newest first.
    pub(crate) fn resource_use(
        &self,
        id: ChannelId,
        window: TimeWindow,
        canonical_agent: impl Fn(AgentId) -> AgentId,
    ) -> Result<(ChannelId, Vec<ResourceUse>), RegistryError> {
        self.channel(id)?;
        let canonical = self.canonical(id);
        let mut resources: Vec<Resource> = self
            .members(canonical)
            .into_iter()
            .flat_map(|member| self.held(member))
            .collect();
        resources.sort_by_key(|resource| std::cmp::Reverse(resource.id));
        resources.dedup_by_key(|resource| resource.id);
        let mut rows = Vec::new();
        for resource in resources {
            let mut writers: BTreeMap<AgentId, u64> = BTreeMap::new();
            let mut readers: BTreeMap<AgentId, u64> = BTreeMap::new();
            for access in self.accesses.values() {
                if access.resource != resource.id || !window.contains(access.at) {
                    continue;
                }
                let counts = match access.op.kind() {
                    AccessKind::Write => &mut writers,
                    AccessKind::Read => &mut readers,
                };
                *counts.entry(canonical_agent(access.agent)).or_default() += 1;
            }
            let list = |counts: BTreeMap<AgentId, u64>| -> Vec<AgentAccesses> {
                counts
                    .into_iter()
                    .filter_map(|(agent, count)| {
                        std::num::NonZeroU64::new(count)
                            .map(|accesses| AgentAccesses { agent, accesses })
                    })
                    .collect()
            };
            if writers.is_empty() && readers.is_empty() {
                continue;
            }
            let row =
                ResourceUse::new(resource, list(writers), list(readers)).map_err(|error| {
                    RegistryError::Store {
                        reason: format!("resource use refused: {error:?}"),
                    }
                })?;
            rows.push(row);
        }
        Ok((canonical, rows))
    }
}

/// `origin` with its detection set by `update`.
pub(crate) fn next_origin(
    origin: &ChannelOrigin,
    id: ChannelId,
    update: DetectionUpdate,
) -> Result<ChannelOrigin, TrafficError> {
    match (origin, update) {
        (ChannelOrigin::Superseded { supersession, .. }, _) => Err(TrafficError::Superseded {
            channel: id,
            by: supersession.by,
        }),
        (ChannelOrigin::Discovered { seed, .. }, DetectionUpdate::Traffic(detection)) => {
            Ok(ChannelOrigin::Discovered {
                seed: *seed,
                detection,
            })
        }
        (
            ChannelOrigin::Declared {
                declaration,
                history,
            },
            DetectionUpdate::Traffic(detection),
        ) => {
            let history = match history {
                DeclaredHistory::Promoted { from, .. } => DeclaredHistory::Promoted {
                    from: *from,
                    detection,
                },
                DeclaredHistory::BeforeTraffic(_) => {
                    DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(detection))
                }
            };
            Ok(ChannelOrigin::Declared {
                declaration: declaration.clone(),
                history,
            })
        }
        (
            ChannelOrigin::Declared {
                declaration,
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            },
            DetectionUpdate::Unused { since },
        ) => Ok(ChannelOrigin::Declared {
            declaration: declaration.clone(),
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::Unused { since }),
        }),
        (
            ChannelOrigin::Declared { .. } | ChannelOrigin::Discovered { .. },
            DetectionUpdate::Unused { .. },
        ) => Err(TrafficError::NotAwaitingTraffic(id)),
    }
}
