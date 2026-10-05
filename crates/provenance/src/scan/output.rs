//! The output's spans: segmentation, then each originated candidate
//! resolved against the index.
//!
//! A candidate's fingerprints (of its own text, through its view) are
//! looked up. Hits on live spans (any agent's) mark the k-grams they cover;
//! each maximal covered stretch becomes a `Relayed(Span(s))` span, `s` the
//! span with most hits in it (`provenance.span.originated-absent-from-inputs`:
//! text matching an indexed span is never originated), and when `s` is
//! another agent's, a `ReaderOutput` match over the same bytes
//! (`provenance.match.reader-output-detected`,
//! `provenance.match.reader-output-relayed-span`) when the stretch passes
//! the stricter reader-output rules (`provenance.match.reader-output-strict`:
//! a length floor, and a supporting fingerprint seen in few texts). The
//! uncovered rest is
//! resolved again, since its own fingerprints are a different selection.
//! A candidate with no hits is `Common` when every one of its fingerprints
//! is above the cutoff at the exchange's time
//! (`provenance.span.common-above-cutoff`; a whole short value's short-span
//! hash counts among them), else `Originated`. A candidate with no
//! fingerprint is kept `Originated` when it reaches the short-span floor
//! (a remainder next to a forward, matched through its context k-grams),
//! and gets no span below it. The segmenter guarantees no candidate
//! shares a k-gram with the inputs, so a `ReaderOutput` match is only ever
//! made for text no input explains (`provenance.match.reader-output-unexplained`).

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::{
    Origin, RelaySource, Span, SpanEvent, SpanLocation, SpanState,
};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher, SpanDraft};
use crosstalk_spec::observed::message::{Message, PartRef};
use crosstalk_spec::support::ByteRange;

use super::hits::{extents_by_span, merge};
use super::kind::{is_exact, match_kind};
use super::messages::MessageSource;
use super::{Loaded, ScanError, Scanner, Session};
use crate::segment::{Coverage, TextPart, run_bytes, runs, text_parts, view};
use crate::span::span_id;
use crate::store::ProvenanceStore;
use crate::text::normalize::trimmed_len;
use crate::text::{normalize, trim_range};

/// One resolved stretch of an output part.
#[derive(Debug, Clone)]
struct Piece {
    start: u32,
    end: u32,
    origin: Origin,
    /// A `ReaderOutput` match over the piece.
    found: Option<ContentMatch>,
}

fn slice(text: &str, start: u32, end: u32) -> Option<&str> {
    text.get(usize::try_from(start).ok()?..usize::try_from(end).ok()?)
}

impl Scanner {
    /// The output's spans, classified, and its `ReaderOutput` matches.
    pub(crate) async fn output_spans<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        output: &Message,
        loaded: &Loaded,
    ) -> Result<(Vec<Span>, Vec<ContentMatch>), ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let coverage = self.coverage(&loaded.inputs());
        let drafts = self.segmenter().segment_against(output, &coverage);
        let parts = text_parts(output);
        let mut pieces: Vec<(PartRef, Piece)> = Vec::new();
        for draft in drafts {
            let Some(part) = parts
                .iter()
                .find(|part| part.index == draft.location.part.index)
            else {
                continue;
            };
            match draft.origin {
                Origin::Originated => {
                    for piece in self.resolve(session, part, draft).await? {
                        pieces.push((draft.location.part, piece));
                    }
                }
                origin => pieces.push((
                    draft.location.part,
                    Piece {
                        start: draft.location.range.start(),
                        end: draft.location.range.end(),
                        origin,
                        found: None,
                    },
                )),
            }
        }
        pieces.sort_by_key(|(part, piece)| (part.index, piece.start));
        let mut spans = Vec::with_capacity(pieces.len());
        let mut matches = Vec::new();
        for (part, piece) in pieces {
            let Ok(range) = ByteRange::new(piece.start, piece.end) else {
                continue;
            };
            let location = SpanLocation { part, range };
            let state = SpanState::Extracted
                .advance(SpanEvent::Classify(piece.origin))
                .map_err(|refused| {
                    ScanError::Store(crate::store::ProvenanceStoreError::Transition(refused))
                })?;
            spans.push(Span {
                id: span_id(session.exchange, &location),
                location,
                agent: session.reader,
                exchange: session.exchange,
                state,
            });
            matches.extend(piece.found);
        }
        Ok((spans, matches))
    }

    async fn resolve<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        draft: SpanDraft,
    ) -> Result<Vec<Piece>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let mut pieces = Vec::new();
        let mut queue = vec![(draft.location.range.start(), draft.location.range.end())];
        while let Some((start, end)) = queue.pop() {
            let Some((start, end)) = trim_range(&part.text, start, end) else {
                continue;
            };
            let Some(text) = slice(&part.text, start, end) else {
                continue;
            };
            let seen = view(text, part.kind);
            let kgrams = self.winnowing().winnow_mapped(&seen);
            if kgrams.is_empty() && !self.segmenter().matchable(seen.text()) {
                continue;
            }
            // A whole short value is also judged by its short-span hash.
            let mut fingerprints = kgrams.clone();
            fingerprints.extend(self.short_fingerprint(part, start, end));
            let owned = self.owned(kgrams.clone());
            let hits = session.lookup(&owned).await?;
            let live = &session.live;
            let by_span = extents_by_span(&hits, &owned, |span| live.get(span).is_some());
            if by_span.is_empty() {
                let origin = if !fingerprints.is_empty()
                    && self.all_boilerplate(session, &fingerprints).await?
                {
                    Origin::Common
                } else {
                    Origin::Originated
                };
                pieces.push(Piece {
                    start,
                    end,
                    origin,
                    found: None,
                });
                continue;
            }
            let relayed = self.relayed_runs(session, part, text, &by_span).await?;
            let mut at = 0u32;
            for (run_start, run_end, source) in relayed {
                if run_start > at {
                    queue.push((start + at, start + run_start));
                }
                at = at.max(run_end);
                // The hit fingerprints inside the run that name its source.
                let support: Vec<_> = hits
                    .iter()
                    .filter(|hit| hit.span == source)
                    .filter(|hit| {
                        owned.iter().any(|kgram| {
                            kgram.start == hit.query_offset
                                && kgram.fingerprint == hit.fingerprint
                                && kgram.start >= run_start
                                && kgram.start < run_end
                        })
                    })
                    .map(|hit| hit.fingerprint)
                    .collect();
                let piece = self
                    .relayed_piece(
                        session,
                        part,
                        draft.location.part,
                        source,
                        (start + run_start, start + run_end),
                        &support,
                    )
                    .await?;
                pieces.extend(piece);
            }
            if start + at < end {
                queue.push((start + at, end));
            }
        }
        Ok(pieces)
    }

    /// Whether every fingerprint of `kgrams` is above the cutoff.
    async fn all_boilerplate<I, S, M, L>(
        &self,
        session: &Session<'_, I, S, M, L>,
        kgrams: &[crate::fingerprint::KGram],
    ) -> Result<bool, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        for kgram in kgrams {
            let frequency = session
                .env
                .index
                .frequency(kgram.fingerprint, session.now)
                .await
                .map_err(ScanError::Index)?;
            if frequency <= self.settings().cutoff() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The stretches of `text` (a candidate at some offset of the part)
    /// copied from the hit spans, relative to `text`, disjoint and in order,
    /// each with the span it was copied from.
    ///
    /// When the hit spans' bodies are stored, the stretches are maximal
    /// runs of k-grams found consecutively in one span's text (as the
    /// segmenter follows inputs), kept when they hold a hit; otherwise the
    /// merged hit extents, each attributed to the span with most hits in it.
    async fn relayed_runs<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        text: &str,
        by_span: &BTreeMap<SpanId, Vec<(u32, u32)>>,
    ) -> Result<Vec<(u32, u32, SpanId)>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let hit_extents = merge(by_span.values().flatten().copied().collect());
        let mut coverage = Coverage::default();
        let mut owners: Vec<SpanId> = Vec::new();
        for span in by_span.keys() {
            for origin in session.origin_texts(*span).await? {
                let input = coverage.add_text(self.winnowing(), &origin);
                owners.resize(input + 1, *span);
            }
        }
        let mut relayed = Vec::new();
        if !coverage.is_empty() {
            let seen = view(text, part.kind);
            let kgrams = self.winnowing().kgrams_of(&normalize(seen.text()));
            let mut end_so_far = 0u32;
            for run in runs(&kgrams, &coverage) {
                let (run_start, run_end) = run_bytes(&seen, &kgrams, run);
                let run_start = run_start.max(end_so_far);
                let holds_hit = hit_extents
                    .iter()
                    .any(|(hit_start, hit_end)| *hit_start < run_end && *hit_end > run_start);
                if run_start >= run_end || !holds_hit {
                    continue;
                }
                let Some(source) = owners.get(run.input).copied() else {
                    continue;
                };
                end_so_far = run_end;
                relayed.push((run_start, run_end, source));
            }
        }
        if relayed.is_empty() {
            for (group_start, group_end) in hit_extents {
                let winner = by_span
                    .iter()
                    .map(|(span, extents)| {
                        let inside = extents
                            .iter()
                            .filter(|(start, end)| *start >= group_start && *end <= group_end)
                            .count();
                        (inside, std::cmp::Reverse(*span))
                    })
                    .max()
                    .map(|(_, std::cmp::Reverse(span))| span);
                if let Some(source) = winner {
                    relayed.push((group_start, group_end, source));
                }
            }
        }
        Ok(relayed)
    }

    /// Whether a stretch of the reader's output relayed from another
    /// agent's span passes the stricter `ReaderOutput` rules
    /// (`provenance.match.reader-output-strict`): at least
    /// `ReaderOutputRules::min_chars` normalized characters, and one of the
    /// hit fingerprints supporting it observed in at most
    /// `ReaderOutputRules::cutoff` texts. The stretch stays relayed either
    /// way; only the match is withheld.
    async fn reader_output_admitted<I, S, M, L>(
        &self,
        session: &Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        (start, end): (u32, u32),
        support: &[Fingerprint],
    ) -> Result<bool, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let rules = self.reader_output();
        let text = slice(&part.text, start, end).unwrap_or_default();
        let chars = trimmed_len(&normalize(view(text, part.kind).text()));
        if chars < rules.min_chars() {
            tracing::debug!(exchange = ?session.exchange, chars, min_chars = rules.min_chars(), "reader-output match below the length floor");
            return Ok(false);
        }
        for fingerprint in support {
            let frequency = session
                .env
                .index
                .frequency(*fingerprint, session.now)
                .await
                .map_err(ScanError::Index)?;
            if frequency <= rules.cutoff() {
                return Ok(true);
            }
        }
        tracing::debug!(exchange = ?session.exchange, chars, cutoff = rules.cutoff(), "reader-output match on frequent text only");
        Ok(false)
    }

    /// The piece `[start, end)` relayed from `source`, with a `ReaderOutput`
    /// match when `source` is another agent's and the stretch passes the
    /// stricter reader-output rules.
    async fn relayed_piece<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        part_ref: PartRef,
        source: SpanId,
        (start, end): (u32, u32),
        support: &[Fingerprint],
    ) -> Result<Option<Piece>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let Some(record) = session.live.get(source).cloned() else {
            return Ok(None);
        };
        let mut found = None;
        if record.span.agent != session.reader
            && self
                .reader_output_admitted(session, part, (start, end), support)
                .await?
        {
            let read = slice(&part.text, start, end).unwrap_or_default();
            let origins = session.origin_texts(source).await?;
            let origins: Vec<&str> = origins.iter().map(String::as_str).collect();
            let kind = match_kind(&[], is_exact(read, &origins));
            let mut matched = end - start;
            if kind == MatchKind::Exact {
                matched = matched.min(record.span.location.range.len().get());
            }
            if let (Some(matched), Ok(range)) =
                (NonZeroU32::new(matched), ByteRange::new(start, end))
            {
                let built = ContentMatch::new(
                    source,
                    record.span.agent,
                    session.reader,
                    session.exchange,
                    SpanLocation {
                        part: part_ref,
                        range,
                    },
                    Carrier::ReaderOutput,
                    kind,
                    matched,
                );
                found = built.ok();
            }
        }
        Ok(Some(Piece {
            start,
            end,
            origin: Origin::Relayed(RelaySource::Span(source)),
            found,
        }))
    }
}
