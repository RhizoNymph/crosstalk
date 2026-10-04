//! Lists and detail reads: channels, agents, alerts, audit, detection
//! quality and dead letters.

use std::collections::{BTreeMap, HashMap, HashSet};

use crosstalk_spec::aggregates::alert::{Alert, AlertState, AlertSubject};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind};
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;
use crate::backend::fixture::world::confirmed;
use crate::contract::agents::{AgentDetail, AgentSummary};
use crate::contract::channels::{
    ChannelListFilter, ChannelSummary, DetectionKind, OriginKind, ResourceUse, policy_kind,
};
use crate::contract::errors::QueryError;
use crate::contract::graph::route_kind;
use crate::contract::lists::{Page, PageRequest};
use crate::contract::research::{
    Actor, AuditEntry, AuditFilter, AuditSubject, MatchKindName, QualityRow,
};
use crate::contract::verdict::Verdict;

use super::Ctx;
use super::page::{self, newest_first, oldest_first};
use super::summaries;

pub fn channels(
    ctx: &Ctx,
    filter: &ChannelListFilter,
    page: &PageRequest,
) -> Result<Page<ChannelSummary>> {
    let items = ctx
        .state
        .channels
        .values()
        .filter(|r| filter.include_superseded || r.superseded.is_none())
        .filter(|r| {
            let origin = &r.channel.origin;
            (filter.origins.is_empty() || filter.origins.contains(&OriginKind::of(origin)))
                && (filter.detections.is_empty()
                    || filter.detections.contains(&DetectionKind::of(origin)))
                && (filter.policies.is_empty()
                    || filter.policies.contains(&policy_kind(&r.channel.policy)))
        })
        .map(|r| {
            (
                oldest_first(r.created, r.channel.id.as_ulid()),
                summaries::channel(ctx, r),
            )
        })
        .collect();
    page::paginate("channels", items, page)
}

pub fn channel(ctx: &Ctx, id: ChannelId) -> Option<ChannelSummary> {
    ctx.state
        .channels
        .get(&id)
        .map(|r| summaries::channel(ctx, r))
}

fn ranked(counts: HashMap<AgentId, u64>) -> Vec<(AgentId, u64)> {
    let mut out: Vec<(AgentId, u64)> = counts.into_iter().collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    out
}

pub fn channel_resources(ctx: &Ctx, id: ChannelId, window: TimeWindow) -> Result<Vec<ResourceUse>> {
    if !ctx.state.channels.contains_key(&id) {
        return Err(QueryError::NotFound);
    }
    let members: HashSet<ChannelId> = ctx.channel_members(id).into_iter().collect();
    /// Accesses per agent, writers then readers.
    type Uses = (HashMap<AgentId, u64>, HashMap<AgentId, u64>);
    let mut uses: BTreeMap<_, Uses> = BTreeMap::new();
    for resource in &ctx.world.resources {
        if ctx
            .world
            .resource_channel
            .get(&resource.id)
            .is_some_and(|c| members.contains(c))
        {
            uses.insert(resource.id, Default::default());
        }
    }
    for access in &ctx.world.accesses {
        if !window.contains(access.at) {
            continue;
        }
        if let Some((writers, readers)) = uses.get_mut(&access.resource) {
            let slot = match access.op.kind() {
                AccessKind::Write => writers,
                AccessKind::Read => readers,
            };
            *slot.entry(ctx.agent(access.agent)).or_default() += 1;
        }
    }
    Ok(uses
        .into_iter()
        .filter_map(|(rid, (writers, readers))| {
            Some(ResourceUse {
                resource: ctx.world.resource(rid)?.clone(),
                writers: ranked(writers),
                readers: ranked(readers),
            })
        })
        .collect())
}

pub fn agents(ctx: &Ctx, page: &PageRequest) -> Result<Page<AgentSummary>> {
    let counts = summaries::global_counts(ctx);
    let items = ctx
        .canonical_agents()
        .filter_map(|id| {
            let summary = summaries::agent(ctx, id, counts.get(&id).copied().unwrap_or_default())?;
            Some(((0, id.as_ulid()), summary))
        })
        .collect();
    page::paginate("agents", items, page)
}

pub fn agent(ctx: &Ctx, id: AgentId) -> Option<AgentDetail> {
    let canonical = ctx.agent(id);
    let record = ctx.state.agents.get(&canonical)?;
    let counts = summaries::global_counts(ctx);
    let summary = summaries::agent(
        ctx,
        canonical,
        counts.get(&canonical).copied().unwrap_or_default(),
    )?;
    let members: HashSet<AgentId> = ctx.members(canonical).iter().copied().collect();
    let aliases = ctx
        .members(canonical)
        .iter()
        .filter(|m| **m != canonical)
        .filter_map(|m| ctx.state.agents.get(m).map(|r| r.agent.clone()))
        .collect();
    let children = ctx
        .canonical_agents()
        .filter(|c| *c != canonical)
        .filter(|c| {
            ctx.state
                .agents
                .get(c)
                .and_then(|r| r.agent.parent)
                .is_some_and(|p| ctx.agent(p) == canonical)
        })
        .collect();
    let involves = |a: &AgentId| members.contains(a) || ctx.agent(*a) == canonical;
    let merges = ctx
        .state
        .merges
        .iter()
        .filter(|m| involves(&m.from) || involves(&m.into) || m.repointed.iter().any(involves))
        .cloned()
        .collect();
    let vetoes = ctx
        .state
        .vetoes
        .iter()
        .filter(|v| involves(&v.a) || involves(&v.b))
        .copied()
        .collect();
    Some(AgentDetail {
        summary,
        agent: record.agent.clone(),
        aliases,
        children,
        merges,
        vetoes,
    })
}

fn alert_kind(state: &AlertState) -> AlertStateKind {
    match state {
        AlertState::Open => AlertStateKind::Open,
        AlertState::Acknowledged { .. } => AlertStateKind::Acknowledged,
        AlertState::Resolved { .. } => AlertStateKind::Resolved,
        AlertState::Suppressed { .. } => AlertStateKind::Suppressed,
    }
}

/// Whether an alert is about `channel`: its subject is the channel, or a
/// transmission routed through it (both after supersession).
fn about_channel(ctx: &Ctx, alert: &Alert, channel: ChannelId) -> bool {
    match alert.subject {
        AlertSubject::Channel(c) => ctx.channel(c) == channel,
        AlertSubject::Transmission(t) => ctx.world.tx(t).is_some_and(
            |r| matches!(r.transmission.route, Route::Channel(c) if ctx.channel(c) == channel),
        ),
        AlertSubject::Agent(_) => false,
    }
}

pub fn alerts(ctx: &Ctx, filter: &AlertFilter, page: &PageRequest) -> Result<Page<Alert>> {
    let channel = filter.channel.map(|c| ctx.channel(c));
    let items = ctx
        .state
        .alerts
        .iter()
        .filter(|a| filter.states.is_empty() || filter.states.contains(&alert_kind(&a.state)))
        .filter(|a| channel.is_none_or(|c| about_channel(ctx, a, c)))
        .map(|a| (newest_first(a.raised_at, a.id.as_ulid()), a.clone()))
        .collect();
    page::paginate("alerts", items, page)
}

fn match_kind_name(kind: &MatchKind) -> MatchKindName {
    match kind {
        MatchKind::Exact => MatchKindName::Exact,
        MatchKind::Normalized => MatchKindName::Normalized,
        MatchKind::Decoded(_) => MatchKindName::Decoded,
        MatchKind::Semantic(_) => MatchKindName::Semantic,
    }
}

/// Labelled and unlabelled confirmed transmissions in `window`, per route
/// kind and match kind. A transmission with several match kinds counts once
/// under each.
pub fn quality(ctx: &Ctx, window: TimeWindow) -> Vec<QualityRow> {
    let route_order = |r| match r {
        crosstalk_spec::aggregates::edge::RouteKind::Channel => 0u8,
        crosstalk_spec::aggregates::edge::RouteKind::Delegation => 1,
        crosstalk_spec::aggregates::edge::RouteKind::Direct => 2,
        crosstalk_spec::aggregates::edge::RouteKind::Unobserved => 3,
    };
    let kind_order = |k| match k {
        MatchKindName::Exact => 0u8,
        MatchKindName::Normalized => 1,
        MatchKindName::Decoded => 2,
        MatchKindName::Semantic => 3,
    };
    let mut rows: BTreeMap<(u8, u8), QualityRow> = BTreeMap::new();
    for record in &ctx.world.transmissions {
        let t = &record.transmission;
        if !window.contains(t.opened_at) {
            continue;
        }
        let Some(c) = confirmed(&t.state) else {
            continue;
        };
        let route = route_kind(&t.route);
        let mut kinds: Vec<MatchKindName> = c
            .content()
            .iter()
            .map(|m| match_kind_name(m.kind()))
            .collect();
        kinds.sort_by_key(|k| kind_order(*k));
        kinds.dedup();
        for kind in kinds {
            let row = rows
                .entry((route_order(route), kind_order(kind)))
                .or_insert(QualityRow {
                    route,
                    match_kind: kind,
                    genuine: 0,
                    false_detection: 0,
                    unlabeled: 0,
                });
            match ctx.verdict(t.id) {
                Some(Verdict::Genuine) => row.genuine += 1,
                Some(Verdict::FalseDetection) => row.false_detection += 1,
                None => row.unlabeled += 1,
            }
        }
    }
    rows.into_values().collect()
}

fn subject_matches(ctx: &Ctx, wanted: AuditSubject, subject: AuditSubject) -> bool {
    match (wanted, subject) {
        (AuditSubject::Agent(a), AuditSubject::Agent(b)) => a == b || ctx.agent(a) == ctx.agent(b),
        (AuditSubject::Channel(a), AuditSubject::Channel(b)) => {
            a == b || ctx.channel(a) == ctx.channel(b)
        }
        (a, b) => a == b,
    }
}

pub fn audit(ctx: &Ctx, filter: &AuditFilter, page: &PageRequest) -> Result<Page<AuditEntry>> {
    let items = ctx
        .state
        .audit
        .iter()
        .filter(|r| {
            filter.operators.is_empty()
                || matches!(r.entry.by, Actor::Operator(op) if filter.operators.contains(&op))
        })
        .filter(|r| filter.window.is_none_or(|w| w.contains(r.entry.at)))
        .filter(|r| {
            filter
                .subject
                .is_none_or(|s| r.subjects.iter().any(|x| subject_matches(ctx, s, *x)))
        })
        .map(|r| {
            (
                newest_first(r.entry.at, r.entry.id.as_ulid()),
                r.entry.clone(),
            )
        })
        .collect();
    page::paginate("audit", items, page)
}

pub fn dead_letters(ctx: &Ctx, page: &PageRequest) -> Result<Page<DeadLetter>> {
    let items = ctx
        .state
        .dead_letters
        .iter()
        .map(|l| {
            (
                oldest_first(l.envelope.at, l.envelope.id.as_ulid()),
                l.clone(),
            )
        })
        .collect();
    page::paginate("dead-letters", items, page)
}
