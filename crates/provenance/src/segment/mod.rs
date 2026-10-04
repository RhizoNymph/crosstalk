//! [`NovelRunSegmenter`], the spec's `Segmenter`: cuts an output into
//! spans and classifies each against the exchange's inputs.
//!
//! For each text-bearing part of the output (text, visible reasoning, tool
//! call arguments seen through their JSON-unescaped [`view()`]):
//!
//! 1. Every k-gram of the part is looked up in the [`Coverage`] of the
//!    inputs (every layer of every input part, raw and decoded).
//! 2. Copied runs are followed k-gram by k-gram: a run continues while the
//!    next output k-gram also occurs at the next position of the same input
//!    layer, so a run's text is one contiguous stretch of one input. Each
//!    run becomes a `Relayed(Input(message))` span naming that input
//!    (`provenance.span.relay-source-contains-text`,
//!    `provenance.span.relay-source-exists`). Runs are cut to be disjoint
//!    (`provenance.span.disjoint`): a run starts where the previous ended.
//! 3. Text copied from a server tool's result in the same output (a web
//!    fetch the provider ran) is no one's: it gets no span.
//! 4. Every remaining stretch, trimmed of surrounding whitespace, is a
//!    candidate `Originated` span, kept when it has at least one k-gram
//!    (shorter text can never be matched).
//!    None of its k-grams occurs in any input layer, so it shares no
//!    fingerprint with the inputs (`provenance.span.originated-absent-from-inputs`).
//!
//! Cuts are k-gram boundaries mapped to source bytes, so every range is on
//! character boundaries of the part text (`provenance.span.char-boundaries`)
//! and within it (`provenance.span.within-output-part`). Whether an
//! originated candidate is really boilerplate (`Common`) or another agent's
//! text received through a channel the gateway cannot see (`Relayed(Span)`)
//! needs the index; the scanner decides that (`crate::scan`).
//!
//! Server tool results are scanned as reads, not segmented.

pub mod coverage;
pub mod view;

use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, SpanLocation};
use crosstalk_spec::interfaces::l4_provenance::{Segmenter, SpanDraft};
use crosstalk_spec::observed::message::{Message, PartRef};
use crosstalk_spec::support::ByteRange;

pub use self::coverage::{Coverage, MessageKGrams, Occurrence, message_kgrams};
pub use self::view::{PartKind, TextPart, text_parts, view};
use crate::decode::DecodePipeline;
use crate::fingerprint::{KGram, Winnowing};
use crate::text::{MappedText, normalize, trim_range};

/// The segmenter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NovelRunSegmenter {
    winnowing: Winnowing,
    pipeline: DecodePipeline,
}

/// A run of consecutive output k-grams found consecutively in one input
/// layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    /// The run's first and last k-gram, by index.
    pub first: usize,
    pub last: usize,
    /// The input it was found in (the earliest, when several hold it).
    pub input: usize,
}

impl NovelRunSegmenter {
    pub fn new(winnowing: Winnowing, pipeline: DecodePipeline) -> Self {
        Self {
            winnowing,
            pipeline,
        }
    }

    pub fn winnowing(&self) -> &Winnowing {
        &self.winnowing
    }

    pub fn pipeline(&self) -> &DecodePipeline {
        &self.pipeline
    }

    /// The coverage of `inputs`, for [`NovelRunSegmenter::segment_against`].
    pub fn coverage(&self, inputs: &[&Message]) -> Coverage {
        Coverage::of_messages(&self.winnowing, &self.pipeline, inputs)
    }

    /// Segment `output` against a precomputed input coverage.
    pub fn segment_against(&self, output: &Message, inputs: &Coverage) -> Vec<SpanDraft> {
        let mut own = Coverage::default();
        let parts = text_parts(output);
        let server_results: Vec<u16> = parts
            .iter()
            .filter(|part| part.kind == PartKind::ToolResult)
            .map(|part| part.index)
            .collect();
        if !server_results.is_empty() {
            own.add_message(&self.winnowing, &self.pipeline, output, |index| {
                server_results.contains(&index)
            });
        }
        let mut drafts = Vec::new();
        for part in parts {
            if part.kind == PartKind::ToolResult {
                continue;
            }
            let part_ref = PartRef {
                message: output.hash,
                index: part.index,
            };
            drafts.extend(self.segment_part(&part, part_ref, inputs, &own));
        }
        drafts
    }

    fn segment_part(
        &self,
        part: &TextPart<'_>,
        part_ref: PartRef,
        inputs: &Coverage,
        own: &Coverage,
    ) -> Vec<SpanDraft> {
        let Ok(text_len) = u32::try_from(part.text.len()) else {
            return Vec::new();
        };
        let seen = view(&part.text, part.kind);
        let kgrams = self.winnowing.kgrams_of(&normalize(seen.text()));
        let mut drafts = Vec::new();
        let mut covered: Vec<(u32, u32)> = Vec::new();
        let mut end_so_far = 0u32;
        for run in runs(&kgrams, inputs) {
            let (start, end) = run_bytes(&seen, &kgrams, run);
            let start = start.max(end_so_far);
            if start >= end {
                continue;
            }
            end_so_far = end;
            covered.push((start, end));
            let Some(source) = inputs.input(run.input) else {
                continue;
            };
            if let Some(draft) = draft(
                part_ref,
                start,
                end,
                Origin::Relayed(RelaySource::Input(source)),
            ) {
                drafts.push(draft);
            }
        }
        for run in runs(&kgrams, own) {
            let (start, end) = run_bytes(&seen, &kgrams, run);
            covered.push((start, end));
        }
        for (start, end) in gaps(&mut covered, text_len) {
            let Some((start, end)) = trim_range(&part.text, start, end) else {
                continue;
            };
            let text = &part.text[usize_of(start)..usize_of(end)];
            if self
                .winnowing
                .kgrams(view(text, part.kind).text())
                .is_empty()
            {
                continue;
            }
            if let Some(draft) = draft(part_ref, start, end, Origin::Originated) {
                drafts.push(draft);
            }
        }
        drafts.sort_by_key(|draft| draft.location.range.start());
        drafts
    }
}

impl Segmenter for NovelRunSegmenter {
    fn segment(&self, output: &Message, inputs: &[Message]) -> Vec<SpanDraft> {
        let inputs: Vec<&Message> = inputs.iter().collect();
        let coverage = self.coverage(&inputs);
        self.segment_against(output, &coverage)
    }
}

fn usize_of(offset: u32) -> usize {
    usize::try_from(offset).unwrap_or(usize::MAX)
}

fn draft(part: PartRef, start: u32, end: u32, origin: Origin) -> Option<SpanDraft> {
    let range = ByteRange::new(start, end).ok()?;
    Some(SpanDraft {
        location: SpanLocation { part, range },
        origin,
    })
}

/// The part bytes a run covers.
pub fn run_bytes(seen: &MappedText, kgrams: &[KGram], run: Run) -> (u32, u32) {
    let start = kgrams[run.first].start;
    let end = kgrams[run.last].end;
    (seen.source(usize_of(start)), seen.source(usize_of(end)))
}

/// Maximal runs of consecutive k-grams that occur consecutively in one
/// layer of `coverage`, in order.
pub fn runs(kgrams: &[KGram], coverage: &Coverage) -> Vec<Run> {
    let mut runs = Vec::new();
    if coverage.is_empty() {
        return runs;
    }
    let mut index = 0;
    while index < kgrams.len() {
        let first = coverage.get(kgrams[index].fingerprint);
        if first.is_empty() {
            index += 1;
            continue;
        }
        let start = index;
        let mut candidates: Vec<Occurrence> = first.to_vec();
        while index + 1 < kgrams.len() {
            let next = coverage.get(kgrams[index + 1].fingerprint);
            let advanced: Vec<Occurrence> = candidates
                .iter()
                .filter_map(|candidate| {
                    next.iter()
                        .find(|occurrence| {
                            occurrence.layer == candidate.layer
                                && occurrence.position == candidate.position + 1
                        })
                        .copied()
                })
                .collect();
            if advanced.is_empty() {
                break;
            }
            candidates = advanced;
            index += 1;
        }
        let input = candidates
            .iter()
            .map(|candidate| candidate.input)
            .min()
            .unwrap_or(0);
        runs.push(Run {
            first: start,
            last: index,
            input,
        });
        index += 1;
    }
    runs
}

/// The stretches of `0..len` no interval in `covered` touches.
fn gaps(covered: &mut [(u32, u32)], len: u32) -> Vec<(u32, u32)> {
    covered.sort_unstable();
    let mut gaps = Vec::new();
    let mut at = 0u32;
    for (start, end) in covered.iter().copied() {
        if start > at {
            gaps.push((at, start));
        }
        at = at.max(end);
    }
    if at < len {
        gaps.push((at, len));
    }
    gaps
}
