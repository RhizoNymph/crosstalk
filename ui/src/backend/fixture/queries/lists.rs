//! Lists and detail reads: agents, alerts, audit and dead letters.
//! Channels are in [`super::channels`].

use std::collections::HashSet;

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l8_surface::AlertFilter;

use crate::backend::Result;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::{AgentDetail, AgentListFilter, AgentSummary};
use crate::contract::alerts::Alert;
use crate::contract::research::{
    Actor, AuditEntry, AuditFilter, AuditOutcome, AuditSubject, AuditedAction,
};
use crate::url::ulid::UlidId;
use crosstalk_spec::ids::MergeId;
use crosstalk_spec::paging::{AgentList, AlertList, AuditList, DeadLetterList, Page, PageRequest};

use super::Ctx;
use super::page::{self, newest_first, oldest_first};
use super::summaries;

/// Whether a summary passes the agents list filter.
fn keeps(filter: &AgentListFilter, summary: &AgentSummary) -> bool {
    let text = filter.text.as_ref().map(|t| t.as_str().to_lowercase());
    (filter.states.is_empty() || filter.states.contains(&summary.state))
        && (filter.harness_claims.is_empty()
            || summary
                .claims
                .iter()
                .any(|c| filter.harness_claims.contains(&c.claim.family)))
        && (filter.parents.is_empty()
            || summary.parent.is_some_and(|p| filter.parents.contains(&p)))
        && text.is_none_or(|needle| {
            summary
                .label
                .as_ref()
                .is_some_and(|l| l.as_str().to_lowercase().contains(&needle))
                || summary.id.to_ulid().to_lowercase().contains(&needle)
        })
}

pub fn agents(
    ctx: &Ctx,
    filter: &AgentListFilter,
    page: &PageRequest<AgentList>,
) -> Result<Page<AgentSummary, AgentList>> {
    let counts = summaries::global_counts(ctx);
    // Parents name canonical agents; an alias asks for its canonical agent.
    let filter = AgentListFilter {
        parents: filter.parents.iter().map(|p| ctx.agent(*p)).collect(),
        ..filter.clone()
    };
    let items = ctx
        .canonical_agents()
        .filter_map(|id| {
            let summary = summaries::agent(ctx, id, counts.get(&id).copied().unwrap_or_default())?;
            keeps(&filter, &summary).then(|| ((0, id.as_ulid()), summary))
        })
        .collect();
    page::paginate("agents", page::digest(&filter), items, page)
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

pub fn alerts(
    ctx: &Ctx,
    filter: &AlertFilter,
    page: &PageRequest<AlertList>,
) -> Result<Page<Alert, AlertList>> {
    let channel = filter.channel.map(|c| ctx.channel(c));
    let items = ctx
        .state
        .alerts
        .iter()
        .filter(|a| filter.states.is_empty() || filter.states.contains(&a.state.kind()))
        .filter(|a| channel.is_none_or(|c| about_channel(ctx, a, c)))
        .map(|a| (newest_first(a.raised_at, a.id.as_ulid()), a.clone()))
        .collect();
    page::paginate("alerts", page::digest(filter), items, page)
}

/// Every entity an audit entry concerns: its subject, the agents of a merge
/// or unmerge, and what it created.
fn concerns(ctx: &Ctx, entry: &AuditEntry) -> Vec<AuditSubject> {
    let mut out: Vec<AuditSubject> = entry.subject.into_iter().collect();
    if let AuditedAction::Operator(OperatorAction::MergeAgents(request)) = &entry.action {
        out.push(AuditSubject::Agent(request.source()));
        out.push(AuditSubject::Agent(request.target()));
    }
    if let AuditOutcome::Applied(outcome) = &entry.outcome {
        match outcome {
            ActionOutcome::RuleCreated(id) => out.push(AuditSubject::Rule(*id)),
            ActionOutcome::ChannelPromoted(id) => out.push(AuditSubject::Channel(*id)),
            ActionOutcome::Merged(id) => out.push(AuditSubject::Merge(*id)),
            ActionOutcome::Applied => {}
        }
    }
    let merges: Vec<MergeId> = out
        .iter()
        .filter_map(|s| match s {
            AuditSubject::Merge(id) => Some(*id),
            _ => None,
        })
        .collect();
    for merge in merges {
        if let Some(record) = ctx.state.merges.iter().find(|m| m.id == merge) {
            out.push(AuditSubject::Agent(record.from));
            out.push(AuditSubject::Agent(record.into));
        }
    }
    out
}

/// Whether `subject` is `wanted`, after alias and supersession resolution.
fn subject_matches(ctx: &Ctx, wanted: AuditSubject, subject: AuditSubject) -> bool {
    match (wanted, subject) {
        (AuditSubject::Agent(a), AuditSubject::Agent(b)) => a == b || ctx.agent(a) == ctx.agent(b),
        (AuditSubject::Channel(a), AuditSubject::Channel(b)) => {
            a == b || ctx.channel(a) == ctx.channel(b)
        }
        (a, b) => a == b,
    }
}

pub fn audit(
    ctx: &Ctx,
    filter: &AuditFilter,
    page: &PageRequest<AuditList>,
) -> Result<Page<AuditEntry, AuditList>> {
    let items = ctx
        .state
        .audit
        .iter()
        .filter(|e| {
            filter.operators.is_empty()
                || matches!(e.by, Actor::Operator(op) if filter.operators.contains(&op))
        })
        .filter(|e| filter.window.is_none_or(|w| w.contains(e.at)))
        .filter(|e| {
            filter.subject.is_none_or(|s| {
                concerns(ctx, e)
                    .into_iter()
                    .any(|x| subject_matches(ctx, s, x))
            })
        })
        .map(|e| (newest_first(e.at, e.id.as_ulid()), e.clone()))
        .collect();
    page::paginate("audit", page::digest(filter), items, page)
}

pub fn dead_letters(
    ctx: &Ctx,
    page: &PageRequest<DeadLetterList>,
) -> Result<Page<DeadLetter, DeadLetterList>> {
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
    page::paginate("dead-letters", 0, items, page)
}
