//! Search over the text of confirmed transmissions, as a linked view: hits
//! on transmissions confirmed in the window (when given) that the filter
//! admits, filtered before ranking, in descending (score, id), the version
//! resolved on the first page and pinned by the cursor.
//!
//! Scores are a deterministic stand-in for the index: a text score from
//! case-insensitive substring hits, a semantic score from the share of
//! query words in the transmission's theme vocabulary or text, and their
//! mean for hybrid search. Each reads only the query and the transmission,
//! so keyset paging is stable.

use std::collections::HashSet;

use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::interfaces::l6_analysis::{SearchHit, SearchResults};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crosstalk_spec::paging::{Page, PageRequest, SearchList};
use crosstalk_spec::support::{NonEmpty, Similarity, TimeWindow};

use crate::backend::Result;
use crate::backend::fixture::text::{self, Theme};
use crate::backend::fixture::world::TxRecord;

use super::Ctx;
use super::linked::Linked;
use super::page;

const LIST: &str = "search";

/// About 160 bytes of `text` around byte `at`, on character boundaries.
fn snippet(text: &str, at: usize) -> String {
    let at = at.min(text.len());
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
/// texts: how many, and the first as (text index, byte offset).
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
        .map(|theme| text::vocabulary(*theme))
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
    window: Option<TimeWindow>,
    filter: &TopologyFilter,
    page: &PageRequest<SearchList>,
) -> Result<SearchResults> {
    let linked = Linked::paged(ctx, window, filter, page::pinned(LIST, page)?)?;
    let needle = request.text.as_str().to_lowercase();
    let words = text::tokens(request.text.as_str());
    let vocabularies = vocabularies();
    let mut items = Vec::new();
    for counted in linked.admitted() {
        let record = counted.record;
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
    let (scored, next) = page::paginate(
        &page::versioned(LIST, linked.version),
        page::digest(&(request, window, filter)),
        items,
        page,
    )?
    .into_parts();
    let hits = scored.into_iter().map(hit).collect::<Vec<_>>();
    let overflow = |e| QueryError::Store {
        reason: format!("page overflow: {e:?}"),
    };
    let page = match (next, NonEmpty::from_vec(hits)) {
        (Some(next), Some(hits)) => Page::more(page.size, hits, next).map_err(overflow)?,
        (_, hits) => Page::last(page.size, hits.map(NonEmpty::into_vec).unwrap_or_default())
            .map_err(overflow)?,
    };
    Ok(SearchResults {
        topic_version: linked.version,
        page,
    })
}

/// The hit for a scored record, with a snippet around its first text match
/// or, for semantic hits, around the sender's key sentence.
fn hit(scored: Scored) -> SearchHit {
    let record = scored.record;
    let snippet = match scored
        .first
        .and_then(|(i, at)| Some((record.indexed(i)?, at)))
    {
        // Lowercasing can shift byte offsets in non-ASCII text; `snippet`
        // clamps to character boundaries.
        Some((text, at)) => snippet(text, at),
        None => record
            .texts
            .first()
            .map(|t| snippet(&t.origin, t.key_at))
            .unwrap_or_default(),
    };
    SearchHit {
        transmission: record.transmission.id,
        score: scored.score,
        snippet,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snippets_stay_on_character_boundaries() {
        let text = "é".repeat(200);
        let cut = snippet(&text, 101);
        assert!(cut.starts_with('…') && cut.ends_with('…'));
        assert_eq!(snippet("short", 2), "short");
        assert_eq!(snippet("short", 99), "short");
    }

    #[test]
    fn text_hits_count_every_occurrence() {
        let lower = vec!["a deploy and a deploy".to_owned(), "deploy".to_owned()];
        assert_eq!(text_hits(&lower, "deploy"), (3, Some((0, 2))));
        assert_eq!(text_hits(&lower, "rollback"), (0, None));
    }
}
