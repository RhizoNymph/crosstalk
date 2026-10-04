//! Lists: audit and dead letters. Agents are in [`super::agents`],
//! channels in [`super::channels`], alerts and rules in [`super::alerts`].

use crosstalk_spec::interfaces::l2_transport::DeadLetter;

use crate::backend::Result;
use crate::contract::research::{
    Actor, AuditEntry, AuditFilter, AuditOutcome, AuditSubject, AuditedAction,
};
use crosstalk_spec::ids::MergeId;
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};
use crosstalk_spec::paging::{AuditList, DeadLetterList, Page, PageRequest};

use super::Ctx;
use super::page::{self, newest_first, oldest_first};

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
            ActionOutcome::ChannelPromoted { channel, .. } => {
                out.push(AuditSubject::Channel(*channel))
            }
            ActionOutcome::Merged(id) => out.push(AuditSubject::Merge(*id)),
            ActionOutcome::Applied | ActionOutcome::Unchanged => {}
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
        if let Some(record) = ctx.state.identity.merges().iter().find(|m| m.id() == merge) {
            out.push(AuditSubject::Agent(record.source()));
            out.push(AuditSubject::Agent(record.target()));
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
