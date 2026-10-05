//! Reads: an input part (or a server tool result in the output) looked up
//! layer by layer, turned into content matches.
//!
//! The carrier follows from where the part sits
//! (`provenance.match.carrier-from-part`): a tool result (a tool message's,
//! or a server tool's inside an assistant message) is `ToolResult` with its
//! call id, a user message `UserTurn`, a system message `SystemPrompt`
//! (wherever it appears in the request). Other parts of assistant messages
//! replayed as inputs carry nothing.
//!
//! Per origin span, one layer wins: one whose text holds the origin span's
//! whole text (normalized) before one that does not, then the one covering
//! most part bytes, then the shorter chain, so the kind names the chain that
//! really decodes the text (`provenance.decode.codecs-in-decode-order`).
//! `read_at` runs from
//! the first to the last covered byte in the part text, as it arrived;
//! `matched_bytes` counts the covered bytes, at most the origin span's
//! length for an exact match (`provenance.match.bytes-within-span`). Hits
//! on the reader's own spans are skipped (`provenance.match.self-hit-skipped`).

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher};
use crosstalk_spec::observed::message::{AssistantPart, Message, MessageBody, PartRef};
use crosstalk_spec::support::ByteRange;

use super::hits::{covered, extents_by_span, merge};
use super::kind::{is_exact, match_kind};
use super::messages::MessageSource;
use super::{ScanError, Scanner, Session};
use crate::decode::Step;
use crate::segment::{PartKind, TextPart, text_parts, view};
use crate::store::ProvenanceStore;
use crate::text::normalize::normalized_string;

/// The carrier a read in `part` of `message` has; `None` for parts that
/// carry nothing (an assistant message's own text or tool calls).
pub fn carrier(message: &Message, part: &TextPart<'_>) -> Option<Carrier> {
    let at = usize::from(part.index);
    match &message.body {
        MessageBody::Tool(results) => results
            .iter()
            .nth(at)
            .map(|result| Carrier::ToolResult(result.call_id.clone())),
        MessageBody::User(_) => Some(Carrier::UserTurn),
        MessageBody::System(_) => Some(Carrier::SystemPrompt),
        MessageBody::Assistant(parts) => match parts.get(at) {
            Some(AssistantPart::ServerToolResult(result)) => {
                Some(Carrier::ToolResult(result.call_id.clone()))
            }
            _ => None,
        },
    }
}

/// One layer's hits on one origin span.
#[derive(Debug, Clone)]
struct Candidate {
    layer: usize,
    chain: Vec<Step>,
    merged: Vec<(u32, u32)>,
    covered: u32,
}

impl Candidate {
    /// The ranking among one span's candidates: a layer holding the whole
    /// origin text first (the chain that really decodes it), then the most
    /// part bytes covered, then the shorter chain, then the earlier layer.
    fn rank(
        &self,
        complete: bool,
    ) -> (
        bool,
        u32,
        std::cmp::Reverse<usize>,
        std::cmp::Reverse<usize>,
    ) {
        (
            complete,
            self.covered,
            std::cmp::Reverse(self.chain.len()),
            std::cmp::Reverse(self.layer),
        )
    }
}

fn on_boundaries(text: &str, range: ByteRange) -> bool {
    let start = usize::try_from(range.start()).unwrap_or(usize::MAX);
    let end = usize::try_from(range.end()).unwrap_or(usize::MAX);
    end <= text.len() && text.is_char_boundary(start) && text.is_char_boundary(end)
}

impl Scanner {
    /// Every match read in `message`: all its carrying parts, or (for the
    /// output) its server tool results only.
    pub(crate) async fn read_message<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        message: &Message,
        is_output: bool,
    ) -> Result<Vec<ContentMatch>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let mut matches = Vec::new();
        for part in text_parts(message) {
            if is_output && part.kind != PartKind::ToolResult {
                continue;
            }
            let Some(carrier) = carrier(message, &part) else {
                continue;
            };
            let part_ref = PartRef {
                message: message.hash,
                index: part.index,
            };
            matches.extend(self.read_part(session, &part, part_ref, carrier).await?);
        }
        Ok(matches)
    }

    async fn read_part<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        part_ref: PartRef,
        carrier: Carrier,
    ) -> Result<Vec<ContentMatch>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let base = view(&part.text, part.kind);
        let layers = self.pipeline().layers(base.text());
        let mut found: BTreeMap<SpanId, Vec<Candidate>> = BTreeMap::new();
        for (index, layer) in layers.iter().enumerate() {
            let mapped = base.compose(layer.text.clone());
            let kgrams = self.owned(self.winnowing().winnow(layer.text.text()));
            let hits = session.lookup(&kgrams).await?;
            let reader = session.reader;
            let live = &session.live;
            let by_span = extents_by_span(&hits, &kgrams, |span| {
                live.get(span)
                    .is_some_and(|record| record.span.agent != reader)
            });
            for (span, extents) in by_span {
                let merged = merge(
                    extents
                        .into_iter()
                        .map(|(start, end)| {
                            mapped.source_range(
                                usize::try_from(start).unwrap_or(usize::MAX),
                                usize::try_from(end).unwrap_or(usize::MAX),
                            )
                        })
                        .filter(|(start, end)| start < end)
                        .collect(),
                );
                let candidate = Candidate {
                    layer: index,
                    chain: layer.chain.clone(),
                    covered: covered(&merged),
                    merged,
                };
                if candidate.covered == 0 {
                    continue;
                }
                found.entry(span).or_default().push(candidate);
            }
        }
        let mut best: BTreeMap<SpanId, Candidate> = BTreeMap::new();
        for (span, candidates) in found {
            let chosen = if candidates.len() == 1 {
                candidates.into_iter().next()
            } else {
                let origins: Vec<String> = session
                    .origin_texts(span)
                    .await?
                    .iter()
                    .map(|text| normalized_string(text).trim().to_owned())
                    .filter(|text| !text.is_empty())
                    .collect();
                candidates
                    .into_iter()
                    .map(|candidate| {
                        let layer = normalized_string(
                            layers.get(candidate.layer).map_or("", |l| l.text.text()),
                        );
                        let complete = origins.iter().any(|origin| layer.contains(origin.as_str()));
                        (candidate.rank(complete), candidate)
                    })
                    .max_by(|a, b| a.0.cmp(&b.0))
                    .map(|(_, candidate)| candidate)
            };
            if let Some(candidate) = chosen {
                best.insert(span, candidate);
            }
        }
        let mut matches = Vec::new();
        for (span, candidate) in &best {
            if let Some(found) = self
                .fingerprint_match(session, part, part_ref, &carrier, *span, candidate)
                .await?
            {
                matches.push(found);
            }
        }
        matches.extend(
            self.semantic_matches(session, part, part_ref, &carrier, &best)
                .await?,
        );
        Ok(matches)
    }

    async fn fingerprint_match<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        part_ref: PartRef,
        carrier: &Carrier,
        span: SpanId,
        candidate: &Candidate,
    ) -> Result<Option<ContentMatch>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let (Some(first), Some(last)) = (candidate.merged.first(), candidate.merged.last()) else {
            return Ok(None);
        };
        let Ok(range) = ByteRange::new(first.0, last.1) else {
            return Ok(None);
        };
        if !on_boundaries(&part.text, range) {
            return Ok(None);
        }
        let Some(record) = session.live.get(span).cloned() else {
            return Ok(None);
        };
        let mut matched = candidate.covered;
        let kind = if candidate.chain.is_empty() {
            let start = usize::try_from(range.start()).unwrap_or(usize::MAX);
            let end = usize::try_from(range.end()).unwrap_or(usize::MAX);
            let read = part.text.get(start..end).unwrap_or_default();
            let origins = session.origin_texts(span).await?;
            let origins: Vec<&str> = origins.iter().map(String::as_str).collect();
            match_kind(&candidate.chain, is_exact(read, &origins))
        } else {
            match_kind(&candidate.chain, false)
        };
        if kind == MatchKind::Exact {
            matched = matched.min(record.span.location.range.len().get());
        }
        let Some(matched) = NonZeroU32::new(matched) else {
            return Ok(None);
        };
        let built = ContentMatch::new(
            span,
            record.span.agent,
            session.reader,
            session.exchange,
            SpanLocation {
                part: part_ref,
                range,
            },
            carrier.clone(),
            kind,
            matched,
        );
        match built {
            Ok(found) => Ok(Some(found)),
            Err(refused) => {
                tracing::debug!(span = ?span, refused = ?refused, "content match refused");
                Ok(None)
            }
        }
    }

    async fn semantic_matches<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        part: &TextPart<'_>,
        part_ref: PartRef,
        carrier: &Carrier,
        fingerprinted: &BTreeMap<SpanId, Candidate>,
    ) -> Result<Vec<ContentMatch>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let hits = session
            .env
            .semantic
            .lookup(&part.text, self.threshold)
            .await
            .map_err(ScanError::Semantic)?;
        if hits.is_empty() {
            return Ok(Vec::new());
        }
        session.fetch(hits.iter().map(|hit| hit.span)).await?;
        let mut matches = Vec::new();
        for hit in hits {
            if hit.score < self.threshold
                || fingerprinted.contains_key(&hit.span)
                || !on_boundaries(&part.text, hit.read_range)
            {
                continue;
            }
            let Some(record) = session.live.get(hit.span) else {
                continue;
            };
            if record.span.agent == session.reader {
                continue;
            }
            let built = ContentMatch::new(
                hit.span,
                record.span.agent,
                session.reader,
                session.exchange,
                SpanLocation {
                    part: part_ref,
                    range: hit.read_range,
                },
                carrier.clone(),
                MatchKind::Semantic(hit.score),
                hit.read_range.len(),
            );
            if let Ok(found) = built {
                matches.push(found);
            }
        }
        Ok(matches)
    }
}
