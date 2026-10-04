//! Transmission lists, the evidence behind one transmission, and search.

use std::collections::HashSet;

use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::support::Similarity;

use crate::backend::Result;
use crate::backend::fixture::text::{self, Theme};
use crate::backend::fixture::world::{TxRecord, co_accesses, confirmed, excerpt_text};
use crate::contract::errors::QueryError;
use crate::contract::evidence::{AccessDetail, MatchEvidence, TransmissionEvidence};
use crate::contract::graph::{
    TransmissionSelector, TransmissionStateKind, TransmissionSummary, route_kind,
};
use crate::contract::lists::{Page, PageRequest};
use crate::contract::scope::Scope;
use crate::contract::search::{SearchMode, SearchRequest};

use super::Ctx;
use super::page::{self, Key, newest_first};
use super::scope::Filter;

pub fn state_kind(state: &TransmissionState) -> TransmissionStateKind {
    match state {
        TransmissionState::Detected => TransmissionStateKind::Detected,
        TransmissionState::AwaitingContent { .. } => TransmissionStateKind::AwaitingContent,
        TransmissionState::Suspected { .. } => TransmissionStateKind::Suspected,
        TransmissionState::Confirmed(_) => TransmissionStateKind::Confirmed,
        TransmissionState::Classified { .. } => TransmissionStateKind::Classified,
        TransmissionState::Aggregated { .. } => TransmissionStateKind::Aggregated,
        TransmissionState::Discarded { .. } => TransmissionStateKind::Discarded,
    }
}

/// A list row, with agents and channel resolved.
pub fn summary(filter: &Filter, record: &TxRecord) -> TransmissionSummary {
    let ctx = filter.ctx;
    let t = &record.transmission;
    TransmissionSummary {
        id: t.id,
        from: record.from.map(|f| ctx.agent(f)),
        to: ctx.agent(t.to),
        route: ctx.route(&t.route),
        route_kind: route_kind(&t.route),
        state: state_kind(&t.state),
        opened_at: t.opened_at,
        topic: record.topic(filter.version),
        matched_bytes: record.matched_bytes,
        verdict: ctx.verdict(t.id),
    }
}

fn key(record: &TxRecord) -> Key {
    newest_first(
        record.transmission.opened_at,
        record.transmission.id.as_ulid(),
    )
}

pub fn list(
    ctx: &Ctx,
    scope: &Scope,
    selector: &TransmissionSelector,
    page: &PageRequest,
) -> Result<Page<TransmissionSummary>> {
    let filter = Filter::new(ctx, scope)?;
    let items: Vec<(Key, TransmissionSummary)> = match selector {
        TransmissionSelector::All => ctx
            .world
            .transmissions
            .iter()
            .filter(|r| filter.keeps(r))
            .map(|r| (key(r), summary(&filter, r)))
            .collect(),
        TransmissionSelector::Ids(ids) => {
            let wanted: HashSet<TransmissionId> = ids.iter().copied().collect();
            let mut seen = HashSet::new();
            wanted
                .iter()
                .filter_map(|id| ctx.world.tx(*id))
                .filter(|r| filter.keeps(r) && seen.insert(r.transmission.id))
                .map(|r| (key(r), summary(&filter, r)))
                .collect()
        }
        TransmissionSelector::Edge { from, to, route } => {
            let (from, to, route) = (ctx.agent(*from), ctx.agent(*to), ctx.route(route));
            super::graph::counted(&filter)
                .into_iter()
                .filter(|c| c.from == from && c.to == to && c.route == route)
                .map(|c| (key(c.record), summary(&filter, c.record)))
                .collect()
        }
    };
    page::paginate("tx", items, page)
}

/// The evidence page: matches with their text, accesses and verdicts.
pub fn evidence(ctx: &Ctx, id: TransmissionId) -> Option<TransmissionEvidence> {
    let record = ctx.world.tx(id)?;
    let t = &record.transmission;
    let matches = confirmed(&t.state)
        .map(|c| {
            c.content()
                .iter()
                .zip(&record.texts)
                .map(|(m, text)| MatchEvidence {
                    content_match: m.clone(),
                    origin: text.origin.clone(),
                    read: text.read.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    let mut access_ids = Vec::new();
    for co in co_accesses(&t.state) {
        for access in [co.write(), co.read()] {
            if !access_ids.contains(&access) {
                access_ids.push(access);
            }
        }
    }
    let accesses = access_ids
        .into_iter()
        .filter_map(|a| ctx.world.access(a))
        .filter_map(|access| {
            Some(AccessDetail {
                access: access.clone(),
                resource: ctx.world.resource(access.resource)?.clone(),
            })
        })
        .collect();
    let verdicts = ctx
        .state
        .verdicts
        .iter()
        .filter(|v| v.transmission == id)
        .cloned()
        .collect();
    Some(TransmissionEvidence {
        transmission: t.clone(),
        matches,
        accesses,
        verdicts,
    })
}

/// About 160 bytes of `text` around byte `at`, on character boundaries.
fn snippet(text: &str, at: usize) -> String {
    let mut start = at.saturating_sub(60);
    while start > 0 && !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = (at + 100).min(text.len());
    while end < text.len() && !text.is_char_boundary(end) {
        end += 1;
    }
    let mut out = String::new();
    if start > 0 {
        out.push('…');
    }
    out.push_str(text.get(start..end).unwrap_or_default());
    if end < text.len() {
        out.push('…');
    }
    out
}

/// Case-insensitive substring hits over a transmission's lowercased
/// excerpts: how many, and the first as (excerpt index, byte offset).
fn text_hits(lower: &[String], needle: &str) -> (usize, Option<(usize, usize)>) {
    let mut count = 0;
    let mut first = None;
    for (i, text) in lower.iter().enumerate() {
        let mut from = 0;
        while let Some(pos) = text.get(from..).and_then(|rest| rest.find(needle)) {
            count += 1;
            first.get_or_insert((i, from + pos));
            from += pos + needle.len().max(1);
        }
    }
    (count, first)
}

/// Each theme's terms and label words: what a semantic query is compared
/// with.
fn vocabularies() -> Vec<HashSet<String>> {
    Theme::ALL
        .iter()
        .map(|theme| {
            let mut words: HashSet<String> =
                theme.terms().iter().map(|(t, _)| (*t).to_owned()).collect();
            words.extend(text::tokens(theme.label()));
            words
        })
        .collect()
}

/// A deterministic stand-in for embedding similarity: the share of query
/// words found among the theme's vocabulary or in the text.
fn semantic_score(record: &TxRecord, vocabulary: &HashSet<String>, words: &[String]) -> f32 {
    if words.is_empty() {
        return 0.0;
    }
    let hits = words
        .iter()
        .filter(|w| vocabulary.contains(*w) || record.lower.iter().any(|t| t.contains(w.as_str())))
        .count();
    if hits == 0 {
        return 0.0;
    }
    0.5 + 0.45 * hits as f32 / words.len() as f32
}

/// A scored record, before its snippet is cut.
struct Scored<'a> {
    record: &'a TxRecord,
    score: Similarity,
    first: Option<(usize, usize)>,
}

pub fn search(
    ctx: &Ctx,
    request: &SearchRequest,
    scope: &Scope,
    page: &PageRequest,
) -> Result<Page<SearchHit>> {
    let filter = Filter::new(ctx, scope)?;
    let needle = request.text.as_str().to_lowercase();
    let words = text::tokens(request.text.as_str());
    let vocabularies = vocabularies();
    let mut items = Vec::new();
    for record in ctx.world.transmissions.iter() {
        if record.texts.is_empty() || !filter.keeps(record) {
            continue;
        }
        let (hits, first) = text_hits(&record.lower, &needle);
        let text_score = if hits == 0 {
            0.0
        } else {
            (0.6 + 0.1 * hits as f32).min(1.0)
        };
        let semantic = vocabularies
            .get(record.theme.index())
            .map_or(0.0, |v| semantic_score(record, v, &words));
        let score = match request.mode {
            SearchMode::Text => text_score,
            SearchMode::Semantic => semantic,
            SearchMode::Hybrid => (text_score + semantic) / 2.0,
        };
        if score <= 0.0 {
            continue;
        }
        let score = Similarity::new(score.min(1.0)).map_err(|e| QueryError::Store {
            reason: format!("score out of range: {}", e.0),
        })?;
        let rank = u64::MAX - (f64::from(score.get()) * 1e9) as u64;
        let key = (rank, u128::MAX - record.transmission.id.as_ulid());
        let first = first.filter(|_| request.mode != SearchMode::Semantic);
        items.push((
            key,
            Scored {
                record,
                score,
                first,
            },
        ));
    }
    let scored = page::paginate("search", items, page)?;
    Ok(Page {
        items: scored.items.into_iter().map(hit).collect(),
        next: scored.next,
    })
}

/// The hit for a scored record, with a snippet around its first text match
/// or, for semantic hits, around the sender's key sentence.
fn hit(scored: Scored) -> SearchHit {
    let record = scored.record;
    let snippet = match scored
        .first
        .and_then(|(i, at)| Some((record.excerpt(i)?, at)))
    {
        // Lowercasing can shift byte offsets in non-ASCII text; `snippet`
        // clamps to character boundaries.
        Some((excerpt, at)) => snippet(&excerpt_text(excerpt), at),
        None => record
            .texts
            .first()
            .map(|t| snippet(&excerpt_text(&t.origin), t.origin.before().len()))
            .unwrap_or_default(),
    };
    SearchHit {
        transmission: record.transmission.id,
        score: scored.score,
        snippet,
    }
}
