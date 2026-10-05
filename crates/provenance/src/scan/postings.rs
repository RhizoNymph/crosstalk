//! What an indexed span is posted under, beyond its own winnowed
//! fingerprints.
//!
//! **Context k-grams** (`provenance.index.remainder-around-relay-matchable`).
//! A reply that quotes a phrase of the message it answers is cut into a
//! forwarded run (`Relayed` from that input) and originated remainders
//! around it, each often shorter than a shingle. On its own text such a
//! remainder has no k-gram, or too few for winnowing's guarantee, and a
//! reader of the whole reply would match nothing of it. So the indexed
//! spans of one part (originated and forwarded) that sit next to each
//! other, with only whitespace between them, form a run; the run's text is
//! winnowed as one, and each selected k-gram is posted under the span that
//! holds more than half of its characters. A reader holding the whole run
//! selects the same k-grams. A k-gram mostly over relayed text goes to the
//! forwarded span, never to an originated one: relayed text is never
//! re-originated.
//!
//! **Short-span hashes** (`provenance.match.short-span-exact`). An
//! originated span that is a whole value (its whole text part, trimmed, or
//! one whole string value of a tool call's arguments) of
//! `ShortSpans::min_chars..=max_chars` normalized characters is also posted
//! under the exact hash of its normalized text ([`short::whole`]).

use std::collections::HashMap;

use crosstalk_spec::derived::provenance::span::{Span, SpanState};
use crosstalk_spec::ids::SpanId;

use super::Scanner;
use crate::fingerprint::{KGram, short};
use crate::segment::{PartKind, TextPart, keyed_string_values, view};
use crate::text::{normalize, offset_len, trim_range};

fn usize_of(offset: u32) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

/// Whether a span is posted: originated (not yet indexed when the writes
/// are computed) or forwarded.
fn posted(span: &Span) -> bool {
    span.state == SpanState::Originated || span.state.is_forwarded()
}

impl Scanner {
    /// The context k-grams of every posted span in a run of two or more
    /// adjacent posted spans, each k-gram's extent relative to its span's
    /// start (saturating at 0: a context k-gram may begin before the span).
    pub(crate) fn context_kgrams(
        &self,
        parts: &[TextPart<'_>],
        spans: &[Span],
    ) -> HashMap<SpanId, Vec<KGram>> {
        let mut out: HashMap<SpanId, Vec<KGram>> = HashMap::new();
        for part in parts {
            let mut members: Vec<&Span> = spans
                .iter()
                .filter(|span| span.location.part.index == part.index && posted(span))
                .collect();
            members.sort_by_key(|span| span.location.range.start());
            for group in runs(part, &members) {
                self.attribute(part, group, &mut out);
            }
        }
        out
    }

    /// Winnow one run's text and post each selected k-gram under the span
    /// holding most of it.
    fn attribute(
        &self,
        part: &TextPart<'_>,
        group: &[&Span],
        out: &mut HashMap<SpanId, Vec<KGram>>,
    ) {
        let (Some(first), Some(last)) = (group.first(), group.last()) else {
            return;
        };
        let start = first.location.range.start();
        let end = last.location.range.end();
        let Some(text) = part.text.get(usize_of(start)..usize_of(end)) else {
            return;
        };
        let seen = view(text, part.kind);
        let normalized = normalize(seen.text());
        // Each normalized character's offset in the part.
        let at: Vec<u32> = normalized
            .iter()
            .map(|c| start + seen.source(usize_of(c.start)))
            .collect();
        let k = self.winnowing().k();
        let selected = self
            .winnowing()
            .select(&self.winnowing().kgrams_of(&normalized));
        for kgram in selected {
            let Some(window) = at.get(kgram.position..kgram.position + k) else {
                continue;
            };
            let owner = group.iter().find(|span| {
                let range = span.location.range;
                let inside = window
                    .iter()
                    .filter(|offset| range.start() <= **offset && **offset < range.end())
                    .count();
                2 * inside > k
            });
            let Some(owner) = owner else {
                continue;
            };
            let from = owner.location.range.start();
            out.entry(owner.id).or_default().push(KGram {
                start: (start + seen.source(usize_of(kgram.start))).saturating_sub(from),
                end: (start + seen.source(usize_of(kgram.end))).saturating_sub(from),
                ..kgram
            });
        }
    }

    /// The short-span hash of an originated span that is a whole short
    /// value, its extent relative to the span's start.
    pub(crate) fn span_short(&self, parts: &[TextPart<'_>], span: &Span) -> Option<KGram> {
        let part = parts
            .iter()
            .find(|part| part.index == span.location.part.index)?;
        self.short_fingerprint(part, span.location.range.start(), span.location.range.end())
    }

    /// The short-span hash of `[start, end)` of `part` when it is a whole
    /// value of admitted length: the whole part, trimmed, or (in a tool
    /// call's JSON arguments) one whole string value, trimmed. Its extent
    /// is relative to `start`.
    pub(crate) fn short_fingerprint(
        &self,
        part: &TextPart<'_>,
        start: u32,
        end: u32,
    ) -> Option<KGram> {
        let text = part.text.get(usize_of(start)..usize_of(end))?;
        let seen = view(text, part.kind);
        let kgram = short::whole(&normalize(seen.text()), self.short_spans())?;
        let whole_part = || trim_range(&part.text, 0, offset_len(&part.text)?);
        let whole = match part.kind {
            PartKind::ToolArguments => match keyed_string_values(&part.text) {
                Some(values) => values.iter().any(|(_, (value_start, value_end))| {
                    trim_range(&part.text, *value_start, *value_end) == Some((start, end))
                }),
                None => whole_part() == Some((start, end)),
            },
            PartKind::Text | PartKind::ToolResult => whole_part() == Some((start, end)),
        };
        whole.then(|| KGram {
            start: seen.source(usize_of(kgram.start)),
            end: seen.source(usize_of(kgram.end)),
            ..kgram
        })
    }

    /// The short-span hashes of a scanned part's layers that are whole
    /// short values: what the part observes for the short path.
    pub(crate) fn part_short(
        &self,
        text: &str,
        kind: PartKind,
    ) -> std::collections::BTreeSet<crosstalk_spec::derived::provenance::fingerprint::Fingerprint>
    {
        let base = view(text, kind);
        self.pipeline()
            .layers(base.text())
            .iter()
            .filter_map(|layer| short::whole(&normalize(layer.text.text()), self.short_spans()))
            .map(|kgram| kgram.fingerprint)
            .collect()
    }
}

/// `members` (one part's posted spans, in order) cut into runs of two or
/// more spans with only whitespace between neighbours.
fn runs<'a, 'b>(part: &TextPart<'_>, members: &'b [&'a Span]) -> Vec<&'b [&'a Span]> {
    let mut groups = Vec::new();
    let mut first = 0;
    for index in 1..=members.len() {
        let joined = index < members.len() && {
            let gap_start = members[index - 1].location.range.end();
            let gap_end = members[index].location.range.start();
            part.text
                .get(usize_of(gap_start)..usize_of(gap_end))
                .is_some_and(|gap| view(gap, part.kind).text().trim().is_empty())
        };
        if !joined {
            if index - first >= 2 {
                groups.push(&members[first..index]);
            }
            first = index;
        }
    }
    groups
}
