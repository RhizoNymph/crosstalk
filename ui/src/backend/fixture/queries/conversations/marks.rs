//! The provenance marks on a turn's parts: matches read there
//! ([`inbound`]), spans cut from an output ([`output_spans`]) with their
//! readers ([`read_by`]), and where a span sits ([`span_point`]).

use std::num::NonZeroU32;

use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::RelaySource;
use crosstalk_spec::ids::{ExchangeId, SpanId};
use crosstalk_spec::interfaces::l8_surface::conversation::SpanPoint;
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    Inbound, OriginatedStatus, OutputSpan, ReadBy, Reader, RelayedFrom, SpanOrigin,
    TransmissionMark,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::observed::message::PartRef;

use super::point;
use crate::backend::fixture::queries::Ctx;
use crate::backend::fixture::queries::page::{Key, newest_first};
use crate::backend::fixture::world::TxRecord;
use crate::backend::fixture::world::conversations::MatchRef;
use crate::backend::fixture::world::states;

/// The match `at` names and its transmission, when the world holds them.
pub fn content<'w>(ctx: &Ctx<'w>, at: MatchRef) -> Option<(&'w TxRecord, &'w ContentMatch)> {
    let tx = ctx.world.tx(at.transmission)?;
    let confirmed = states::confirmed(&tx.transmission.state)?;
    let content = confirmed.content().iter().nth(usize::from(at.index))?;
    Some((tx, content))
}

/// The transmission holding a match, its route resolved.
pub fn mark(ctx: &Ctx, tx: &TxRecord) -> TransmissionMark {
    TransmissionMark {
        id: tx.transmission.id,
        route: ctx.route(&tx.transmission.route),
        state: TransmissionStateKind::of(&tx.transmission.state),
    }
}

/// Where a recorded span sits, its author resolved now.
pub fn span_point(ctx: &Ctx, span: SpanId) -> Option<SpanPoint> {
    let record = ctx.world.conversations.span(span)?;
    Some(SpanPoint {
        span,
        agent: ctx.agent(record.author),
        exchange: record.exchange,
        turn: point(ctx, record.exchange),
        location: record.location,
    })
}

/// When a reader read: its turn's start, or the transmission's opening.
fn read_time(
    ctx: &Ctx,
    tx: &TxRecord,
    content: &ContentMatch,
) -> crosstalk_spec::support::Timestamp {
    ctx.world
        .conversations
        .turn(content.reader_exchange())
        .map_or(tx.transmission.opened_at, |turn| turn.started_at)
}

/// Every reader of `span`, each keyed newest reader exchange first.
pub fn readers(ctx: &Ctx, span: SpanId) -> Vec<(Key, Reader)> {
    let mut out: Vec<(Key, Reader)> = ctx
        .world
        .conversations
        .readers_of(span)
        .iter()
        .filter_map(|at| {
            let (tx, content) = content(ctx, *at)?;
            let id = at.transmission.as_ulid() ^ u128::from(at.index);
            let reader = Reader {
                agent: ctx.agent(content.reader()),
                exchange: content.reader_exchange(),
                turn: point(ctx, content.reader_exchange()),
                read_at: content.read_at(),
                carrier: content.carrier().clone(),
                kind: content.kind().clone(),
                transmission: Some(mark(ctx, tx)),
            };
            Some((newest_first(read_time(ctx, tx, content), id), reader))
        })
        .collect();
    out.sort_by_key(|(key, _)| *key);
    out
}

/// The first [`ReadBy::INLINE`] readers of `span` and how many there are.
pub fn read_by(ctx: &Ctx, span: SpanId) -> ReadBy {
    let all = readers(ctx, span);
    let total = u32::try_from(all.len()).unwrap_or(u32::MAX);
    let first: Vec<Reader> = all
        .into_iter()
        .take(ReadBy::INLINE)
        .map(|(_, r)| r)
        .collect();
    ReadBy::new(first, total).unwrap_or_else(|_| ReadBy::none())
}

/// The matches read in `exchange` at `part`, by range start.
pub fn inbound(ctx: &Ctx, exchange: ExchangeId, part: PartRef) -> Vec<Inbound> {
    let mut out: Vec<Inbound> = ctx
        .world
        .conversations
        .reads_in(exchange)
        .iter()
        .filter_map(|at| {
            let (tx, content) = content(ctx, *at)?;
            let read_at = content.read_at();
            if read_at.part != part {
                return None;
            }
            Some(Inbound {
                range: read_at.range,
                matched_bytes: content.matched_bytes(),
                kind: content.kind().clone(),
                carrier: content.carrier().clone(),
                origin: span_point(ctx, content.origin())?,
                transmission: Some(mark(ctx, tx)),
            })
        })
        .collect();
    out.sort_by_key(|m| (m.range.start(), m.range.end()));
    out
}

/// The spans cut from `exchange`'s output at `part`, by range start.
pub fn output_spans(ctx: &Ctx, exchange: ExchangeId, part: PartRef) -> Vec<OutputSpan> {
    let conversations = &ctx.world.conversations;
    let mut out: Vec<OutputSpan> = conversations
        .spans_of(exchange)
        .filter(|(_, record)| record.location.part == part)
        .map(|(span, record)| {
            let read_by = read_by(ctx, span);
            let first_hit = readers(ctx, span)
                .into_iter()
                .filter_map(|(_, reader)| conversations.turn(reader.exchange))
                .map(|turn| turn.started_at)
                .min();
            let status = match (NonZeroU32::new(read_by.total()), first_hit) {
                (Some(hits), Some(first_hit_at)) => OriginatedStatus::Propagated {
                    indexed_at: record.indexed_at,
                    first_hit_at: first_hit_at.max(record.indexed_at),
                    hits,
                },
                _ => OriginatedStatus::Indexed {
                    at: record.indexed_at,
                },
            };
            OutputSpan {
                span,
                range: record.location.range,
                origin: SpanOrigin::Originated { status, read_by },
            }
        })
        .collect();
    out.extend(
        conversations
            .relayed_in(exchange)
            .iter()
            .filter(|relayed| relayed.location.part == part)
            .map(|relayed| OutputSpan {
                span: relayed.span,
                range: relayed.location.range,
                origin: SpanOrigin::Relayed(match relayed.source {
                    RelaySource::Span(source) => match span_point(ctx, source) {
                        Some(point) => RelayedFrom::Span(point),
                        None => RelayedFrom::Input(relayed.location.part.message),
                    },
                    RelaySource::Input(message) => RelayedFrom::Input(message),
                }),
            }),
    );
    out.sort_by_key(|s| (s.range.start(), s.range.end()));
    out
}
