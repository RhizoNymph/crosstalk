//! Declarations, policy decisions, promotion and its preview: the bodies of
//! `ChannelRegistry::declare`, `set_policy`, `promote` and
//! `promotion_coverage`, each run inside one transaction.
//!
//! Each checks before it writes: a refusal returns before the first
//! statement that changes anything, and the transaction rolls back anyway.

use std::collections::HashMap;

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
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l5_flow::{Promoted, RegistryError};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::TxError;
use sqlx::PgConnection;

use super::rows::{self, StoredChannel};
use crate::store::codec::id_text;
use crate::store::error::Fault;
use crate::store::ids::ChannelIdSource;

pub(crate) fn changed(channel: ChannelId) -> BusEvent {
    BusEvent::Changed(Changed::Channel(channel))
}

/// `ChannelRegistry::declare`: refused with `OverlappingDeclaration` when a
/// declared pattern overlaps; otherwise a declared channel awaiting traffic
/// under the id source's pending id, its first history entry the policy's
/// decision when it carries one.
pub(crate) async fn declare(
    conn: &mut PgConnection,
    ids: &dyn ChannelIdSource,
    pattern: ResourcePattern,
    policy: Policy,
    by: PolicyAuthor,
    at: Timestamp,
) -> Result<(ChannelId, Vec<BusEvent>), TxError<RegistryError>> {
    if let Some((existing, _)) = rows::declared(conn)
        .await?
        .into_iter()
        .find(|(_, declared)| declared.overlaps(&pattern))
    {
        return Err(TxError::Abort(RegistryError::OverlappingDeclaration {
            existing,
        }));
    }
    let id = ids.pending(at).map_err(Fault::from)?;
    let decision = PolicyDecision::try_from(policy).ok();
    let mut history = PolicyHistory::empty();
    if let Some(decision) = &decision {
        history.record(decision.clone());
    }
    let origin = ChannelOrigin::Declared {
        declaration: Declaration { pattern, by, at },
        history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
    };
    rows::insert_channel(conn, id, &origin, &history.current()).await?;
    if let Some(decision) = &decision {
        rows::record_decision(conn, id, decision).await?;
    }
    Ok((id, vec![changed(id)]))
}

/// `ChannelRegistry::set_policy`.
pub(crate) async fn set_policy(
    conn: &mut PgConnection,
    id: ChannelId,
    decision: PolicyDecision,
) -> Result<(Recorded, Vec<BusEvent>), TxError<RegistryError>> {
    let channel = rows::channel(conn, id)
        .await?
        .ok_or(TxError::Abort(RegistryError::UnknownChannel(id)))?;
    if let Some(supersession) = channel.origin.supersession() {
        return Err(TxError::Abort(RegistryError::Superseded {
            channel: id,
            by: supersession.by,
        }));
    }
    let (recorded, _) = rows::record_decision(conn, id, &decision).await?;
    let events = match recorded {
        Recorded::Duplicate => Vec::new(),
        Recorded::Current | Recorded::Superseded => vec![changed(id)],
    };
    Ok((recorded, events))
}

/// Every stored channel as promotion reads it, in registry order, with
/// the locators of their seed resources.
struct Snapshot {
    channels: Vec<Channel>,
    seeds: HashMap<ResourceId, Locator>,
}

impl Snapshot {
    async fn read(conn: &mut PgConnection) -> Result<Self, Fault> {
        let stored: Vec<StoredChannel> = rows::all_channels(conn).await?;
        let seed_ids: Vec<String> = stored
            .iter()
            .filter_map(|channel| channel.origin.seed())
            .map(|seed| id_text(seed.resource))
            .collect();
        let seeds = rows::resources_by_id(conn, &seed_ids)
            .await?
            .into_iter()
            .map(|(id, resource)| (id, resource.locator))
            .collect();
        // Promotion reads origins only; resource lists are not needed.
        let channels = stored
            .into_iter()
            .map(|channel| channel.with_resources(Vec::new()))
            .collect();
        Ok(Self { channels, seeds })
    }

    fn registered(&self) -> Vec<Registered<'_>> {
        self.channels
            .iter()
            .map(|channel| Registered {
                channel,
                seed: channel
                    .origin
                    .seed()
                    .and_then(|seed| self.seeds.get(&seed.resource)),
            })
            .collect()
    }
}

/// `ChannelRegistry::promote`: `promotion::plan` over every stored channel;
/// on success, in this transaction, the channel's new origin, the decision
/// recorded in its history and every superseded channel's new origin.
pub(crate) async fn promote(
    conn: &mut PgConnection,
    id: ChannelId,
    promotion: Promotion,
) -> Result<(Result<Promoted, PromotionRefusal>, Vec<BusEvent>), Fault> {
    let snapshot = Snapshot::read(conn).await?;
    let plan = match plan(id, promotion.declaration(), &snapshot.registered()) {
        Ok(plan) => plan,
        Err(refusal) => return Ok((Err(refusal), Vec::new())),
    };
    let superseded: Vec<ChannelId> = plan.superseded_ids().collect();
    let (recorded, _) = rows::record_decision(conn, id, promotion.decision()).await?;
    rows::set_origin(conn, id, &plan.origin).await?;
    for (other, origin) in &plan.superseded {
        rows::set_origin(conn, *other, origin).await?;
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
        Ok(Promoted {
            superseded,
            policy: recorded,
        }),
        events,
    ))
}

/// `ChannelRegistry::promotion_coverage`: `promotion::coverage` over the
/// same snapshot `promote` plans over, the held resources of a channel
/// being every resource stored on it (its seed and its own).
pub(crate) async fn coverage_of(
    conn: &mut PgConnection,
    id: ChannelId,
    declaration: &Declaration,
) -> Result<Result<PromotionCoverage, PromotionRefusal>, Fault> {
    let snapshot = Snapshot::read(conn).await?;
    let registered = snapshot.registered();
    let planned = match plan(id, declaration, &registered) {
        Ok(planned) => planned,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let involved: Vec<String> = std::iter::once(id)
        .chain(planned.superseded_ids())
        .map(id_text)
        .collect();
    let held: HashMap<ChannelId, Vec<Resource>> = rows::held_resources(conn, &involved).await?;
    Ok(coverage(id, declaration, &registered, |channel| {
        held.get(&channel).cloned().unwrap_or_default()
    }))
}
