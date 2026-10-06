//! The conversation reads over the fixture's conversations
//! (built on the first conversation read, [`Cv`]): the list, a head, turn windows ([`turns`]),
//! text ([`text`]), a span's readers and the batch locates, as the spec's
//! `QueryApi` defines them (`docs/features/conversation_reads.md`,
//! INV-1000..1029).
//!
//! Every agent id is resolved to its canonical agent at the read, so a
//! merge or unmerge shows on the next read; nothing stored changes.

mod marks;
pub mod text;
pub mod turns;

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::ids::{ConversationId, ExchangeId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::Reader;
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, ConversationTraffic, DelegationLink,
    ExchangePlacement, OriginLink, SpanPoint, Successor, SuccessorKind, TurnIndex, TurnPoint,
};
use crosstalk_spec::observed::agent::ClaimSet;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::paging::{ConversationList, Page, PageRequest, SpanReaderList};

use super::Ctx;
use super::page::{self, Key};
use crate::backend::Result;
use crate::backend::fixture::world::conversations::Conversations;
use crate::backend::fixture::world::conversations::{ConversationRecord, TurnRecord};

pub use marks::span_point;

/// One read's context with its conversations, which the backend builds on
/// the first conversation read. Derefs to the read's [`Ctx`].
pub struct Cv<'c, 'a> {
    ctx: &'c Ctx<'a>,
    pub conversations: std::sync::Arc<Conversations>,
}

impl<'a> std::ops::Deref for Cv<'_, 'a> {
    type Target = Ctx<'a>;

    fn deref(&self) -> &Ctx<'a> {
        self.ctx
    }
}

impl<'c, 'a> Cv<'c, 'a> {
    /// `ctx` with its conversations, built on first use.
    pub fn of(ctx: &'c Ctx<'a>) -> Result<Self> {
        Ok(Self {
            ctx,
            conversations: ctx.conversations()?,
        })
    }
}

fn index(i: usize) -> TurnIndex {
    TurnIndex(u32::try_from(i).unwrap_or(u32::MAX))
}

/// Where a threaded exchange sits.
pub fn point(ctx: &Cv, exchange: ExchangeId) -> Option<TurnPoint> {
    ctx.conversations
        .locate(exchange)
        .map(|(conversation, turn)| TurnPoint {
            conversation,
            turn: TurnIndex(turn),
        })
}

/// The last turn of `parent` whose history lies wholly inside the first
/// `shared_prefix` non-system messages.
fn branch_turn(parent: &ConversationRecord, shared_prefix: u32) -> Option<TurnIndex> {
    let mut seen = 0usize;
    let mut last = None;
    for (i, turn) in parent.turns.iter().enumerate() {
        seen += history_len(turn);
        if seen > shared_prefix as usize {
            break;
        }
        last = Some(index(i));
    }
    last
}

/// The non-system messages a turn adds to its conversation's history.
fn history_len(turn: &TurnRecord) -> usize {
    turn.inputs
        .iter()
        .filter(|e| e.role != crosstalk_spec::observed::message::Role::System)
        .count()
        + usize::from(turn.output.is_some())
}

fn origin_link(ctx: &Cv, record: &ConversationRecord) -> OriginLink {
    let conversations = &ctx.conversations;
    match record.origin {
        ConversationOrigin::Root => OriginLink::Root,
        ConversationOrigin::Fork {
            parent,
            shared_prefix,
        } => {
            let parent_record = conversations.get(parent);
            OriginLink::Fork {
                parent,
                parent_agent: ctx.agent(parent_record.map_or(record.agent, |p| p.agent)),
                shared_prefix,
                branch_turn: parent_record.and_then(|p| branch_turn(p, shared_prefix)),
            }
        }
        ConversationOrigin::Compaction { predecessor } => OriginLink::Compaction {
            predecessor,
            predecessor_agent: ctx.agent(
                conversations
                    .get(predecessor)
                    .map_or(record.agent, |p| p.agent),
            ),
            carried_over: record.turns.first().map_or(0, |t| {
                u32::try_from(t.inputs.iter().filter(|e| e.carried_over).count()).unwrap_or(0)
            }),
        },
    }
}

/// The conversation as the list shows it. `None` for a conversation with
/// no turn (never stored).
fn row(ctx: &Cv, record: &ConversationRecord) -> Option<ConversationRow> {
    let first = record.turns.first()?;
    let last = record.turns.last()?;
    Some(ConversationRow {
        id: record.id,
        agent: ctx.agent(record.agent),
        origin: origin_link(ctx, record),
        started_at: first.started_at,
        last_turn_at: last.started_at,
        turns: u32::try_from(record.turns.len()).unwrap_or(u32::MAX),
        source: record.ingress.source(),
    })
}

/// `QueryApi::conversations`: `ConversationId` descending.
pub fn list(
    ctx: &Cv,
    filter: &ConversationFilter,
    request: &PageRequest<ConversationList>,
) -> Result<Page<ConversationRow, ConversationList>> {
    let agent = filter.agent.map(|a| ctx.agent(a));
    let known = agent.is_none_or(|a| !ctx.members(a).is_empty());
    let items: Vec<(Key, ConversationRow)> = if known {
        ctx.conversations
            .records()
            .filter(|r| agent.is_none_or(|a| ctx.agent(r.agent) == a))
            .filter(|r| filter.origins.is_empty() || filter.origins.contains(&r.origin.kind()))
            .filter(|r| filter.replay.admits(&r.ingress.source()))
            .filter_map(|r| row(ctx, r))
            .map(|row| ((0, u128::MAX - row.id.as_ulid()), row))
            .collect()
    } else {
        Vec::new()
    };
    // The cursor binds the filter with the cluster it resolved to, so a
    // merge between pages is an invalid cursor.
    let cluster = agent.map(|a| ctx.members(a).to_vec());
    page::paginate(
        "conversations",
        page::digest(&(filter, cluster)),
        items,
        request,
    )
}

/// `QueryApi::conversation`: the head.
pub fn head(ctx: &Cv, id: ConversationId) -> Result<Option<ConversationHead>> {
    let conversations = &ctx.conversations;
    let Some(record) = conversations.get(id) else {
        return Ok(None);
    };
    let Some(row) = row(ctx, record) else {
        return Ok(None);
    };
    let successors = conversations
        .successors(id)
        .into_iter()
        .filter_map(|next| {
            let started_at = next.turns.first()?.started_at;
            let kind = match next.origin {
                ConversationOrigin::Fork { shared_prefix, .. } => SuccessorKind::Fork {
                    shared_prefix,
                    branch_turn: branch_turn(record, shared_prefix),
                },
                ConversationOrigin::Compaction { .. } => SuccessorKind::Compaction,
                ConversationOrigin::Root => return None,
            };
            Some(Successor {
                conversation: next.id,
                agent: ctx.agent(next.agent),
                kind,
                started_at,
            })
        })
        .collect();
    let mut claims = ClaimSet::default();
    let mut received: BTreeSet<TransmissionId> = BTreeSet::new();
    let mut sent: BTreeSet<TransmissionId> = BTreeSet::new();
    let mut delegated_from = None;
    for (i, turn) in record.turns.iter().enumerate() {
        if let Some(claim) = &turn.harness {
            claims.observe(claim.clone(), turn.started_at);
        }
        for at in conversations.reads_in(turn.exchange) {
            let Some((tx, content)) = marks::content(ctx, *at) else {
                continue;
            };
            received.insert(tx.transmission.id);
            let delegated = matches!(
                tx.transmission.route,
                Route::Delegation(DelegationDirection::ParentToChild)
            );
            if delegated
                && delegated_from.is_none()
                && let Some(parent) = span_point(ctx, content.origin())
            {
                delegated_from = Some(DelegationLink {
                    transmission: tx.transmission.id,
                    parent,
                    child: TurnPoint {
                        conversation: id,
                        turn: index(i),
                    },
                });
            }
        }
        for (span, _) in conversations.spans_of(turn.exchange) {
            for at in conversations.readers_of(span) {
                if let Some((tx, _)) = marks::content(ctx, *at) {
                    sent.insert(tx.transmission.id);
                }
            }
        }
    }
    let count = |set: &BTreeSet<TransmissionId>| u32::try_from(set.len()).unwrap_or(u32::MAX);
    Ok(Some(ConversationHead {
        row,
        traffic: ConversationTraffic {
            received: count(&received),
            sent: count(&sent),
        },
        successors,
        delegated_from,
        claims,
    }))
}

/// `QueryApi::exchange_turns`.
pub fn exchange_turns(
    ctx: &Cv,
    ids: &IdBatch<ExchangeId>,
) -> BTreeMap<ExchangeId, ExchangePlacement> {
    ids.ids()
        .iter()
        .filter_map(|id| {
            let point = point(ctx, *id)?;
            let turn = ctx.conversations.turn(*id)?;
            Some((
                *id,
                ExchangePlacement {
                    agent: ctx.agent(turn.agent),
                    conversation: point.conversation,
                    turn: point.turn,
                },
            ))
        })
        .collect()
}

/// `QueryApi::span_points`: the recorded (originated) spans of `ids`.
pub fn span_points(ctx: &Cv, ids: &IdBatch<SpanId>) -> BTreeMap<SpanId, SpanPoint> {
    ids.ids()
        .iter()
        .filter_map(|id| span_point(ctx, *id).map(|point| (*id, point)))
        .collect()
}

/// `QueryApi::span_readers`: newest reader exchange first. `None` for a
/// span the fixture never recorded.
pub fn readers(
    ctx: &Cv,
    span: SpanId,
    request: &PageRequest<SpanReaderList>,
) -> Result<Option<Page<Reader, SpanReaderList>>> {
    if ctx.conversations.span(span).is_none() {
        return Ok(None);
    }
    let items: Vec<(Key, Reader)> = marks::readers(ctx, span);
    page::paginate(
        "span_readers",
        page::digest(&span.as_ulid()),
        items,
        request,
    )
    .map(Some)
}

#[cfg(test)]
mod tests;
