//! L5's channel registry as plain data, and every operation on it as a
//! function of that data. Each write checks before it changes anything.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{AgentAccesses, ResourceUse};
use crosstalk_spec::derived::flow::access::{Access, AccessKind};
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{
    Promotion, PromotionCoverage, PromotionRefusal, Registered, coverage, plan,
};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed,
};
use crosstalk_spec::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::{ChannelLookup, Promoted, RegistryError};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};

use crosstalk_spec::interfaces::l5_flow::channels::{DetectionUpdate, TrafficError};

/// A resource and the channel it is stored on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredResource {
    pub(crate) resource: Resource,
    pub(crate) channel: ChannelId,
}

/// Everything the registry stores.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ChannelTable {
    pub(crate) channels: BTreeMap<ChannelId, Channel>,
    pub(crate) histories: BTreeMap<ChannelId, PolicyHistory>,
    pub(crate) resources: BTreeMap<ResourceId, StoredResource>,
    pub(crate) accesses: BTreeMap<AccessId, Access>,
}

fn changed(channel: ChannelId) -> BusEvent {
    BusEvent::Changed(Changed::Channel(channel))
}

impl ChannelTable {
    /// `ChannelDirectory::canonical`.
    pub(crate) fn canonical(&self, id: ChannelId) -> ChannelId {
        self.channels.get(&id).map_or(id, Channel::canonical)
    }

    fn channel(&self, id: ChannelId) -> Result<&Channel, RegistryError> {
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
    fn members(&self, canonical: ChannelId) -> Vec<ChannelId> {
        self.channels
            .values()
            .filter(|channel| channel.canonical() == canonical)
            .map(|channel| channel.id)
            .collect()
    }

    // ---- the trait's operations ----------------------------------------------

    /// `ChannelRegistry::lookup`: `Known` on the canonical channel of a
    /// stored resource with that locator, else `Declared` for a declared
    /// pattern matching it, else `New`.
    pub(crate) fn lookup(&self, locator: &Locator) -> ChannelLookup {
        if let Some(stored) = self
            .resources
            .values()
            .find(|stored| stored.resource.locator == *locator)
        {
            return ChannelLookup::Known(self.canonical(stored.channel));
        }
        self.channels
            .values()
            .find(|channel| {
                channel
                    .origin
                    .pattern()
                    .is_some_and(|pattern| pattern.matches(locator))
            })
            .map_or(ChannelLookup::New, |channel| {
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

    // ---- `ChannelTraffic` ---------------------------------------------------------

    /// The lookup of an unstored resource's locator; a stored resource is
    /// refused.
    fn new_resource_lookup(&self, resource: &Resource) -> Result<ChannelLookup, TrafficError> {
        if self.resources.contains_key(&resource.id) {
            return Err(TrafficError::DuplicateResource(resource.id));
        }
        Ok(self.lookup(&resource.locator))
    }

    pub(crate) fn discover(
        &mut self,
        id: ChannelId,
        resource: Resource,
        first_access: AccessId,
    ) -> Result<Vec<BusEvent>, TrafficError> {
        if self.channels.contains_key(&id) {
            return Err(TrafficError::DuplicateChannel(id));
        }
        let lookup = self.new_resource_lookup(&resource)?;
        if lookup != ChannelLookup::New {
            return Err(TrafficError::NotNew(lookup));
        }
        let seed = Seed {
            resource: resource.id,
            first_access,
        };
        self.resources.insert(
            resource.id,
            StoredResource {
                resource,
                channel: id,
            },
        );
        self.channels.insert(
            id,
            Channel {
                id,
                origin: ChannelOrigin::Discovered {
                    seed,
                    detection: TrafficDetection::Observed { first_access },
                },
                resources: Vec::new(),
                policy: Policy::Unreviewed(None),
            },
        );
        self.histories.insert(id, PolicyHistory::empty());
        Ok(vec![changed(id)])
    }

    pub(crate) fn add_resource(
        &mut self,
        id: ChannelId,
        resource: Resource,
    ) -> Result<Vec<BusEvent>, TrafficError> {
        let channel = self
            .channels
            .get(&id)
            .ok_or(TrafficError::UnknownChannel(id))?;
        if let Some(supersession) = channel.origin.supersession() {
            return Err(TrafficError::Superseded {
                channel: id,
                by: supersession.by,
            });
        }
        // A resource joins the channel its lookup names: a declared channel
        // whose pattern matches it, or any channel for a locator nothing
        // claims yet.
        match self.new_resource_lookup(&resource)? {
            ChannelLookup::New => {}
            ChannelLookup::Declared(declared) if declared == id => {}
            other @ (ChannelLookup::Known(_) | ChannelLookup::Declared(_)) => {
                return Err(TrafficError::NotNew(other));
            }
        }
        let resource_id = resource.id;
        self.resources.insert(
            resource_id,
            StoredResource {
                resource,
                channel: id,
            },
        );
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.resources.push(resource_id);
        }
        Ok(vec![changed(id)])
    }

    pub(crate) fn record_access(&mut self, access: Access) -> Result<(), TrafficError> {
        if !self.resources.contains_key(&access.resource) {
            return Err(TrafficError::UnknownResource(access.resource));
        }
        if self.accesses.contains_key(&access.id) {
            return Err(TrafficError::DuplicateAccess(access.id));
        }
        self.accesses.insert(access.id, access);
        Ok(())
    }

    pub(crate) fn set_detection(
        &mut self,
        id: ChannelId,
        update: DetectionUpdate,
    ) -> Result<(Change, Vec<BusEvent>), TrafficError> {
        let channel = self
            .channels
            .get(&id)
            .ok_or(TrafficError::UnknownChannel(id))?;
        let origin = next_origin(&channel.origin, id, update)?;
        if channel.origin == origin {
            return Ok((Change::Unchanged, Vec::new()));
        }
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.origin = origin;
        }
        Ok((Change::Applied, vec![changed(id)]))
    }

    /// `ChannelReads::channels`, before paging: the channels `filter`
    /// keeps, newest first, after `after`.
    pub(crate) fn channels_matching(
        &self,
        filter: &ChannelFilter,
        after: Option<ChannelId>,
    ) -> Vec<Channel> {
        self.channels
            .values()
            .rev()
            .filter(|channel| after.is_none_or(|after| channel.id < after))
            .filter(|channel| filter.matches(channel))
            .cloned()
            .collect()
    }

    pub(crate) fn confirm(
        &mut self,
        id: ChannelId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> Result<(ChannelId, Vec<BusEvent>), TrafficError> {
        if !self.channels.contains_key(&id) {
            return Err(TrafficError::UnknownChannel(id));
        }
        let canonical = self.canonical(id);
        let channel = self
            .channels
            .get(&canonical)
            .ok_or(TrafficError::UnknownChannel(canonical))?;
        let active = |current: Option<&TrafficDetection>| {
            let since = match current {
                Some(TrafficDetection::Active { since, .. }) => *since,
                Some(
                    TrafficDetection::Observed { .. }
                    | TrafficDetection::Candidate { .. }
                    | TrafficDetection::Dormant { .. },
                )
                | None => at,
            };
            TrafficDetection::Active {
                since,
                last_transmission: transmission,
            }
        };
        let detection = active(channel.origin.traffic());
        let origin = next_origin(
            &channel.origin,
            canonical,
            DetectionUpdate::Traffic(detection),
        )?;
        if let Some(channel) = self.channels.get_mut(&canonical) {
            channel.origin = origin;
        }
        Ok((canonical, vec![changed(canonical)]))
    }
}

/// `origin` with its detection set by `update`.
fn next_origin(
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
