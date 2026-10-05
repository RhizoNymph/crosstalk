//! The reference matcher: a deliberately naive detector, not the real L4.
//!
//! It exists to validate labels (a construction-tier label it cannot find is
//! worth a look) and to set a baseline the gateway has to beat. Per world, in
//! virtual-time order, for each exchange of agent `A`:
//!
//! 1. **New inputs** are the request's messages beyond `A`'s previous
//!    request ([`crate::corpus::delta`]). Each non-assistant part of them is
//!    scanned for other agents' indexed spans, then added to what `A` has
//!    seen.
//! 2. **Originated spans** are the runs of `A`'s response text (text parts
//!    and tool-call arguments) whose k-gram shingles `A` has not seen in any
//!    input or earlier output, at least `min_span` folded bytes long. Their
//!    shingles are indexed.
//!
//! A hit is classified `Exact` when the reader's matched bytes occur
//! verbatim in the span, else `Normalized` (equal after [folding](mod@fold): escape
//! unfolding, case and whitespace). Candidate tokens are also decoded
//! (base64, hex, URL encoding) and matched as `Decoded`. Opaque blobs
//! ([`opaque`]) are cut out before spans, matching and decoding. Hits are
//! grouped into one confirmed spec `Transmission` per (reader exchange,
//! sender, route), with the route from where the hit sits ([`route`]).
//!
//! **Boilerplate.** A shingle held by more than `max_postings` distinct
//! originated spans is boilerplate, as L4's frequency cutoff makes it
//! (`interfaces::l4_provenance`): its postings are dropped and it is ignored
//! on lookup from then on. Text many agents originate independently (a
//! wiki's new-page template, a URL every agent's task names) is a shared
//! source, not evidence of who a reader got it from; without the cutoff each
//! occurrence in a read matches every originating span, so matches grow as
//! reads × occurrences × originators. The reference counts only originated
//! spans toward the frequency (L4 also counts scanned inputs) and has no
//! retention window: a world is one replay.

pub mod decode;
pub mod fold;
pub mod opaque;
pub mod route;
pub mod shingle;

use std::collections::{BTreeMap, HashMap, HashSet};

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, DirectCarrier, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{ChannelId, ExchangeId, SpanId};
use crosstalk_spec::observed::message::{Message, MessageBody, ToolName};
use crosstalk_spec::support::NonEmpty;

use crate::corpus::delta::new_inputs;
use crate::corpus::{CorpusExchange, World};
use crate::ids::{channel_id, span_id, transmission_id};
use crate::location::{self, SpanLocationExt};
use crate::truth::kinds::locator_key;
use decode::decode_candidates;
use fold::fold;
use opaque::segments;
use shingle::{covered, shingles};

/// Matcher parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceConfig {
    /// Shingle length in folded bytes.
    pub k: usize,
    /// The shortest span (and match) in folded bytes; at least `k`.
    pub min_span: usize,
    /// The shortest encoded token worth decoding, in raw bytes.
    pub min_decoded: usize,
    /// The fewest letters and digits a span or match must hold, so a window
    /// of mostly JSON syntax (`"}]","reasoning":"the `) never counts.
    pub min_word_chars: usize,
    /// The most distinct originated spans a shingle may be posted for; one
    /// more makes it boilerplate (never indexed or looked up again).
    pub max_postings: usize,
}

impl Default for ReferenceConfig {
    fn default() -> Self {
        Self {
            k: 24,
            min_span: 24,
            min_decoded: 16,
            min_word_chars: 20,
            max_postings: 16,
        }
    }
}

/// What the matcher found in one world.
#[derive(Debug, Clone, PartialEq)]
pub struct ReferenceOutput {
    pub transmissions: Vec<Transmission>,
    /// The channels transmissions were routed through, by resource.
    pub channels: BTreeMap<ChannelId, Vec<Locator>>,
    /// Every originated span the matcher indexed, in index order.
    pub spans: Vec<SpanRecord>,
    pub matches: usize,
}

/// An indexed span: whose output it is in and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanRecord {
    pub id: SpanId,
    pub exchange: ExchangeId,
    pub location: SpanLocation,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReferenceError {
    #[error("exchange of {0} names an agent the world does not declare")]
    UnknownAgent(String),
    #[error("a content match could not be built: {0}")]
    InvalidMatch(String),
    #[error("a transmission could not be built: {0}")]
    InvalidTransmission(String),
}

/// A shingle's postings: the spans whose originated text holds it, until
/// more than `max_postings` do; then it is boilerplate for the rest of the
/// world.
enum Postings {
    Spans(Vec<usize>),
    Boilerplate,
}

impl Postings {
    /// Posts `span` (once), turning boilerplate past `max`.
    fn post(&mut self, span: usize, max: usize) {
        let Self::Spans(spans) = self else {
            return;
        };
        if spans.last() == Some(&span) {
            return;
        }
        if spans.len() >= max {
            *self = Self::Boilerplate;
        } else {
            spans.push(span);
        }
    }

    fn spans(&self) -> &[usize] {
        match self {
            Self::Spans(spans) => spans,
            Self::Boilerplate => &[],
        }
    }
}

struct IndexedSpan {
    id: SpanId,
    agent: usize,
    raw: String,
}

/// A hit before grouping: who sent it, how it travelled, the match.
struct Hit {
    sender: usize,
    route_key: String,
    route: Route,
    content: ContentMatch,
}

struct Matcher<'w> {
    world: &'w World,
    config: ReferenceConfig,
    spans: Vec<IndexedSpan>,
    records: Vec<SpanRecord>,
    index: HashMap<u64, Postings>,
    seen: Vec<HashSet<u64>>,
    channels: BTreeMap<ChannelId, Vec<Locator>>,
    matches: usize,
}

/// Runs the reference matcher over one world.
pub fn run(world: &World, config: ReferenceConfig) -> Result<ReferenceOutput, ReferenceError> {
    let config = ReferenceConfig {
        min_span: config.min_span.max(config.k),
        ..config
    };
    let mut matcher = Matcher {
        world,
        config,
        spans: Vec::new(),
        records: Vec::new(),
        index: HashMap::new(),
        seen: vec![HashSet::new(); world.agents().len()],
        channels: BTreeMap::new(),
        matches: 0,
    };
    let mut previous: Vec<Option<&CorpusExchange>> = vec![None; world.agents().len()];
    let mut transmissions = Vec::new();
    for exchange in world.exchanges() {
        let reader = world
            .agents()
            .iter()
            .position(|agent| &agent.key == exchange.agent())
            .ok_or_else(|| ReferenceError::UnknownAgent(exchange.agent().to_string()))?;
        let mut hits = Vec::new();
        for (_, message) in new_inputs(previous[reader], exchange) {
            if matches!(message.body, MessageBody::Assistant(_)) {
                continue;
            }
            for part in 0..message.part_count() {
                let Ok(part) = u16::try_from(part) else { break };
                hits.extend(matcher.scan(exchange, reader, message, part)?);
            }
        }
        transmissions.extend(matcher.group(exchange, reader, hits)?);
        if let Some(response) = exchange.response() {
            matcher.index_response(exchange, reader, response);
        }
        previous[reader] = Some(exchange);
    }
    tracing::debug!(
        world = %world.key(),
        spans = matcher.spans.len(),
        matches = matcher.matches,
        transmissions = transmissions.len(),
        "reference matcher finished world"
    );
    Ok(ReferenceOutput {
        transmissions,
        channels: matcher.channels,
        spans: matcher.records,
        matches: matcher.matches,
    })
}

/// Letters and digits in bytes `[start, end)` of folded text.
fn word_chars(text: &str, start: usize, end: usize) -> usize {
    text.as_bytes().get(start..end).map_or(0, |bytes| {
        bytes
            .iter()
            .filter(|b| b.is_ascii_alphanumeric() || **b >= 0x80)
            .count()
    })
}

impl Matcher<'_> {
    fn folded_pieces(text: &str) -> Vec<fold::Folded> {
        segments(text)
            .into_iter()
            .map(|(offset, piece)| fold(piece, offset))
            .collect()
    }

    /// Indexes the originated spans of `A`'s response, then marks all of it
    /// seen by `A`.
    fn index_response(&mut self, exchange: &CorpusExchange, agent: usize, response: &Message) {
        let k = self.config.k;
        for part in 0..response.part_count() {
            let Ok(part) = u16::try_from(part) else { break };
            let Ok(text) = response.part_text(part) else {
                continue;
            };
            let mut all = Vec::new();
            for folded in Self::folded_pieces(&text) {
                let windows = shingles(folded.text.as_bytes(), k);
                let novel: Vec<usize> = windows
                    .iter()
                    .filter(|(hash, _)| !self.seen[agent].contains(hash))
                    .map(|&(_, offset)| offset)
                    .collect();
                for (start, end) in covered(&novel, k) {
                    if end - start < self.config.min_span
                        || word_chars(&folded.text, start, end) < self.config.min_word_chars
                    {
                        continue;
                    }
                    let Some((raw_start, raw_end)) = folded.raw_range(start, end) else {
                        continue;
                    };
                    let Ok(location) = location::location(response.hash, part, raw_start, raw_end)
                    else {
                        continue;
                    };
                    let raw = text
                        .get(raw_start as usize..raw_end as usize)
                        .unwrap_or_default()
                        .to_owned();
                    let id = span_id(
                        self.world.dataset(),
                        exchange.id(),
                        &format!(
                            "{}:{part}:{raw_start}-{raw_end}",
                            location.message().digest().to_hex()
                        ),
                    );
                    let span = self.spans.len();
                    self.spans.push(IndexedSpan { id, agent, raw });
                    self.records.push(SpanRecord {
                        id,
                        exchange: exchange.id(),
                        location,
                    });
                    for &(hash, offset) in &windows {
                        if offset >= start && offset + k <= end && !self.seen[agent].contains(&hash)
                        {
                            self.index
                                .entry(hash)
                                .or_insert_with(|| Postings::Spans(Vec::new()))
                                .post(span, self.config.max_postings);
                        }
                    }
                }
                all.extend(windows.into_iter().map(|(hash, _)| hash));
            }
            self.seen[agent].extend(all);
        }
    }

    /// Hits of other agents' spans in one part of one new input of `reader`.
    fn scan(
        &mut self,
        exchange: &CorpusExchange,
        reader: usize,
        message: &Message,
        part: u16,
    ) -> Result<Vec<Hit>, ReferenceError> {
        let Ok(text) = message.part_text(part) else {
            return Ok(Vec::new());
        };
        let k = self.config.k;
        let mut found: BTreeMap<(usize, u32, u32), MatchKind> = BTreeMap::new();
        for (offset, piece) in segments(&text) {
            let folded = fold(piece, offset);
            let windows = shingles(folded.text.as_bytes(), k);
            for (span, positions) in self.lookup(&windows, reader) {
                for (start, end) in covered(&positions, k) {
                    if end - start < self.config.min_span
                        || word_chars(&folded.text, start, end) < self.config.min_word_chars
                    {
                        continue;
                    }
                    let Some((raw_start, raw_end)) = folded.raw_range(start, end) else {
                        continue;
                    };
                    let read = text
                        .get(raw_start as usize..raw_end as usize)
                        .unwrap_or_default();
                    let kind = if self.spans[span].raw.contains(read) {
                        MatchKind::Exact
                    } else {
                        MatchKind::Normalized
                    };
                    found.entry((span, raw_start, raw_end)).or_insert(kind);
                }
            }
            self.seen[reader].extend(windows.iter().map(|&(hash, _)| hash));
            for decoded in decode_candidates(piece, offset, self.config.min_decoded) {
                let folded_decoded = fold(&decoded.text, 0);
                let decoded_windows = shingles(folded_decoded.text.as_bytes(), k);
                for (span, positions) in self.lookup(&decoded_windows, reader) {
                    let longest = covered(&positions, k)
                        .into_iter()
                        .map(|(start, end)| end - start)
                        .max()
                        .unwrap_or(0);
                    if longest < self.config.min_span {
                        continue;
                    }
                    let (Ok(start), Ok(end)) =
                        (u32::try_from(decoded.start), u32::try_from(decoded.end))
                    else {
                        continue;
                    };
                    found
                        .entry((span, start, end))
                        .or_insert(MatchKind::Decoded(NonEmpty::new(decoded.codec)));
                }
            }
        }
        let mut hits = Vec::with_capacity(found.len());
        for ((span, start, end), kind) in found {
            let Ok(location) = location::location(message.hash, part, start, end) else {
                continue;
            };
            hits.push(self.hit(exchange, reader, message, part, span, location, kind)?);
        }
        Ok(hits)
    }

    /// Spans of agents other than `reader` sharing shingles with `windows`,
    /// with the offsets that hit, in span order.
    fn lookup(&self, windows: &[(u64, usize)], reader: usize) -> BTreeMap<usize, Vec<usize>> {
        let mut hits: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for &(hash, offset) in windows {
            if let Some(postings) = self.index.get(&hash) {
                for &span in postings.spans() {
                    if self.spans[span].agent != reader {
                        hits.entry(span).or_default().push(offset);
                    }
                }
            }
        }
        hits
    }

    #[allow(clippy::too_many_arguments)]
    fn hit(
        &mut self,
        exchange: &CorpusExchange,
        reader: usize,
        message: &Message,
        part: u16,
        span: usize,
        read_at: SpanLocation,
        kind: MatchKind,
    ) -> Result<Hit, ReferenceError> {
        let (carrier, route, route_key) = self.route_of(exchange, message, part);
        let sender = self.spans[span].agent;
        let agents = self.world.agents();
        let content = ContentMatch::new(
            self.spans[span].id,
            agents[sender].id,
            agents[reader].id,
            exchange.id(),
            read_at,
            carrier,
            kind,
            read_at.range.len(),
        )
        .map_err(|error| ReferenceError::InvalidMatch(format!("{error:?}")))?;
        self.matches += 1;
        Ok(Hit {
            sender,
            route_key,
            route,
            content,
        })
    }

    fn route_of(
        &mut self,
        exchange: &CorpusExchange,
        message: &Message,
        part: u16,
    ) -> (Carrier, Route, String) {
        match &message.body {
            MessageBody::System(_) => (
                Carrier::SystemPrompt,
                Route::Direct(DirectCarrier::SystemPrompt),
                "direct:system_prompt".into(),
            ),
            MessageBody::Tool(results) => {
                let Some(result) = results.iter().nth(usize::from(part)) else {
                    return (
                        Carrier::UserTurn,
                        Route::Direct(DirectCarrier::UserTurn),
                        "direct:user_turn".into(),
                    );
                };
                let call = route::find_call(exchange.request(), &result.call_id);
                let carrier = Carrier::ToolResult(result.call_id.clone());
                match call.and_then(route::extract_resource) {
                    Some(resource) => {
                        let key = locator_key(&resource);
                        let channel = channel_id(self.world.dataset(), &key);
                        let resources = self.channels.entry(channel).or_default();
                        if !resources.contains(&resource) {
                            resources.push(resource);
                        }
                        (carrier, Route::Channel(channel), format!("channel:{key}"))
                    }
                    None => {
                        let name = call
                            .map_or_else(|| ToolName("unknown".into()), |call| call.name.clone());
                        let key = format!("direct:tool:{}", name.0);
                        (carrier, Route::Direct(DirectCarrier::ToolResult(name)), key)
                    }
                }
            }
            MessageBody::User(_) | MessageBody::Assistant(_) => (
                Carrier::UserTurn,
                Route::Direct(DirectCarrier::UserTurn),
                "direct:user_turn".into(),
            ),
        }
    }

    /// One confirmed transmission per (sender, route) among an exchange's
    /// hits.
    fn group(
        &self,
        exchange: &CorpusExchange,
        reader: usize,
        hits: Vec<Hit>,
    ) -> Result<Vec<Transmission>, ReferenceError> {
        let mut groups: BTreeMap<(usize, String), (Route, Vec<ContentMatch>)> = BTreeMap::new();
        for hit in hits {
            groups
                .entry((hit.sender, hit.route_key))
                .or_insert_with(|| (hit.route, Vec::new()))
                .1
                .push(hit.content);
        }
        let agents = self.world.agents();
        let mut out = Vec::with_capacity(groups.len());
        for ((sender, route_key), (route, matches)) in groups {
            let Some(content) = NonEmpty::from_vec(matches) else {
                continue;
            };
            let confirmed = Confirmed::new(content, Vec::new(), exchange.at())
                .map_err(|error| ReferenceError::InvalidTransmission(format!("{error:?}")))?;
            out.push(Transmission {
                id: transmission_id(
                    self.world.dataset(),
                    exchange.id(),
                    agents[sender].id,
                    &route_key,
                ),
                to: agents[reader].id,
                route,
                opened_at: exchange.at(),
                state: TransmissionState::Confirmed(confirmed),
            });
        }
        Ok(out)
    }
}
