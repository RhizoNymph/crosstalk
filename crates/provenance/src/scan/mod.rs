//! The scanner: what one `ConversationDelta` means for provenance.
//!
//! [`Scanner::scan`] reads (the index, the stored spans, the semantic
//! matcher, origin bodies for exactness) and decides; it writes nothing.
//! Its [`ScanCommit`] holds the delta's spans and content matches:
//!
//! - **Reads** ([`reads`]): every text part of every message the delta
//!   lists in `new_inputs` and `new_system`, and the server tool results in
//!   its output, is expanded into its decode layers, winnowed and looked
//!   up; hits on live spans of other agents become one match per origin
//!   span and part, from the layer that covers most bytes, with the
//!   carrier the part implies. A semantic lookup runs on every part, its
//!   hits kept only for spans no fingerprint matched there.
//! - **Output** ([`output`]): the output is segmented against the
//!   exchange's inputs (its request history, the new system prompt and the
//!   new inputs); each originated candidate is resolved against the index:
//!   stretches matching an indexed span become `Relayed(Span)` (with a
//!   `ReaderOutput` match when the span is another agent's), what is left
//!   is `Common` when every fingerprint is boilerplate, else `Originated`.
//!
//! [`Scanner::index_work`] then gives the index writes for the committed
//! spans: the postings of each originated span and one observation per
//! span and per scanned input part
//! (`provenance.index.originated-indexed`,
//! `provenance.index.scanned-texts-observed`). It is a function of the
//! stored spans and the delta's messages, so a redelivery after a crash
//! redoes exactly the same writes.
//!
//! The time every index call is given is the exchange's start
//! (`started_at`), so a replay scans with the frequencies it scanned with.

pub mod cache;
pub mod hits;
pub mod kind;
pub mod messages;
mod output;
mod reads;

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, PositionedFingerprint};
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanState};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError, SemanticMatcher};
use crosstalk_spec::observed::message::Message;
use crosstalk_spec::support::{Similarity, Timestamp};

use self::cache::KGramCache;
use self::hits::LiveSpans;
use self::messages::{LoadError, MessageSource};
use crate::config::{IndexSettings, ProvenanceConfig};
use crate::decode::DecodePipeline;
use crate::fingerprint::{KGram, Winnowing, positioned};
use crate::segment::{Coverage, NovelRunSegmenter, PartKind, message_kgrams, text_parts, view};
use crate::span::match_id;
use crate::store::{
    ExchangeRecord, ProvenanceStore, ProvenanceStoreError, ScanCommit, ScannedAs, StoredMatch,
};

/// Why a scan could not finish.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScanError {
    #[error("fingerprint index: {0:?}")]
    Index(IndexError),
    #[error("semantic matcher: {0:?}")]
    Semantic(IndexError),
    #[error(transparent)]
    Store(#[from] ProvenanceStoreError),
    #[error(transparent)]
    Load(#[from] LoadError),
}

/// The delta's messages and the exchange's request history, loaded.
#[derive(Debug, Clone, Default)]
pub struct Loaded {
    /// The request's messages that are still stored, in request order.
    pub history: Vec<Message>,
    /// `new_inputs`, in delta order.
    pub new_inputs: Vec<Message>,
    pub new_system: Option<Message>,
    pub output: Option<Message>,
}

impl Loaded {
    /// The messages the output is classified against: the request history,
    /// then the new system prompt and new inputs not already in it.
    pub fn inputs(&self) -> Vec<&Message> {
        let mut seen: BTreeSet<MessageHash> = BTreeSet::new();
        let mut inputs = Vec::new();
        let extra = self.new_system.iter().chain(self.new_inputs.iter());
        for message in self.history.iter().chain(extra) {
            if seen.insert(message.hash) {
                inputs.push(message);
            }
        }
        inputs
    }

    /// The messages the delta lists, as scanned.
    pub fn listed(&self) -> Vec<(&Message, ScannedAs)> {
        let mut listed: Vec<(&Message, ScannedAs)> = self
            .new_inputs
            .iter()
            .map(|message| (message, ScannedAs::Input))
            .collect();
        listed.extend(self.new_system.iter().map(|m| (m, ScannedAs::System)));
        listed.extend(self.output.iter().map(|m| (m, ScannedAs::Output)));
        listed
    }
}

/// What the scan reads through.
pub struct ScanEnv<'a, I, S, M, L> {
    pub index: &'a I,
    pub store: &'a S,
    pub semantic: &'a M,
    pub messages: &'a L,
}

/// The index writes for a committed scan.
#[derive(Debug, Clone, Default)]
pub struct IndexWork {
    /// Each originated span and its fingerprints (owned shards only).
    pub postings: Vec<(OriginatedSpan, Vec<PositionedFingerprint>)>,
    /// One entry per observed text.
    pub observations: Vec<Vec<Fingerprint>>,
}

/// The scanner.
#[derive(Debug)]
pub struct Scanner {
    segmenter: NovelRunSegmenter,
    settings: IndexSettings,
    threshold: Similarity,
    /// Input messages' k-grams. Locked only for a lookup or an insert, never
    /// across an await.
    cache: Mutex<KGramCache>,
}

/// State one scan accumulates: live span records fetched so far, origin
/// bodies read for exactness, and the watermark.
pub(crate) struct Session<'a, I, S, M, L> {
    pub env: ScanEnv<'a, I, S, M, L>,
    pub now: Timestamp,
    pub reader: crosstalk_spec::ids::AgentId,
    pub exchange: crosstalk_spec::ids::ExchangeId,
    pub watermark: u64,
    pub live: LiveSpans,
    pub fetched: BTreeSet<SpanId>,
    pub bodies: HashMap<MessageHash, Option<Message>>,
}

impl<I, S, M, L> Session<'_, I, S, M, L>
where
    I: FingerprintIndex + Sync,
    S: ProvenanceStore + Sync,
    M: SemanticMatcher + Sync,
    L: MessageSource + Sync,
{
    /// Fetch the records of `spans` not fetched yet.
    pub async fn fetch(
        &mut self,
        spans: impl IntoIterator<Item = SpanId>,
    ) -> Result<(), ScanError> {
        let missing: Vec<SpanId> = spans
            .into_iter()
            .filter(|span| self.fetched.insert(*span))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let records = self.env.store.spans(&missing).await?;
        self.live
            .extend(LiveSpans::new(records, self.watermark, self.now));
        Ok(())
    }

    /// The texts of the origin span `span` (its bytes, and its view when it
    /// is a tool call's arguments), when its body is still stored.
    pub async fn origin_texts(&mut self, span: SpanId) -> Result<Vec<String>, ScanError> {
        let Some(record) = self.live.get(span) else {
            return Ok(Vec::new());
        };
        let location = record.span.location;
        let hash = location.part.message;
        if !self.bodies.contains_key(&hash) {
            let message = self.env.messages.message(hash).await?;
            self.bodies.insert(hash, message);
        }
        let Some(Some(message)) = self.bodies.get(&hash) else {
            return Ok(Vec::new());
        };
        let Some(part) = text_parts(message)
            .into_iter()
            .find(|part| part.index == location.part.index)
        else {
            return Ok(Vec::new());
        };
        let start = usize::try_from(location.range.start()).unwrap_or(usize::MAX);
        let end = usize::try_from(location.range.end()).unwrap_or(usize::MAX);
        let Some(text) = part.text.get(start..end) else {
            return Ok(Vec::new());
        };
        let mut texts = vec![text.to_owned()];
        if part.kind == PartKind::ToolArguments {
            texts.push(view(text, part.kind).into_text());
        }
        Ok(texts)
    }

    /// Look up `kgrams` (owned shards only) and fetch the hit spans.
    pub async fn lookup(
        &mut self,
        kgrams: &[KGram],
    ) -> Result<Vec<crosstalk_spec::derived::provenance::fingerprint::FingerprintHit>, ScanError>
    {
        if kgrams.is_empty() {
            return Ok(Vec::new());
        }
        let hits = self
            .env
            .index
            .lookup(&positioned(kgrams), self.now)
            .await
            .map_err(ScanError::Index)?;
        self.fetch(hits.iter().map(|hit| hit.span)).await?;
        Ok(hits)
    }
}

impl Scanner {
    pub fn new(config: &ProvenanceConfig) -> Self {
        let winnowing = Winnowing::new(config.winnow());
        let pipeline = DecodePipeline::new(config.decode());
        Self {
            segmenter: NovelRunSegmenter::new(winnowing, pipeline),
            settings: config.index().clone(),
            threshold: config.semantic_threshold(),
            cache: Mutex::new(KGramCache::new(cache::DEFAULT_BUDGET)),
        }
    }

    /// The coverage of `inputs`, each message's k-grams computed once.
    pub fn coverage(&self, inputs: &[&Message]) -> Coverage {
        let mut coverage = Coverage::default();
        for message in inputs {
            let cached = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(message.hash);
            let kgrams = match cached {
                Some(kgrams) => kgrams,
                None => {
                    let computed = Arc::new(message_kgrams(
                        self.winnowing(),
                        self.pipeline(),
                        message,
                        |_| true,
                    ));
                    self.cache
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .put(message.hash, Arc::clone(&computed));
                    computed
                }
            };
            coverage.add_kgrams(Some(message.hash), &kgrams);
        }
        coverage
    }

    pub fn segmenter(&self) -> &NovelRunSegmenter {
        &self.segmenter
    }

    pub fn winnowing(&self) -> &Winnowing {
        self.segmenter.winnowing()
    }

    pub fn pipeline(&self) -> &DecodePipeline {
        self.segmenter.pipeline()
    }

    pub fn settings(&self) -> &IndexSettings {
        &self.settings
    }

    /// `kgrams` on this node's shards.
    pub(crate) fn owned(&self, mut kgrams: Vec<KGram>) -> Vec<KGram> {
        kgrams.retain(|kgram| self.settings.owns(kgram.fingerprint));
        kgrams
    }

    /// Scan `delta` of `exchange`.
    pub async fn scan<I, S, M, L>(
        &self,
        delta: &ConversationDelta,
        exchange: &ExchangeRecord,
        loaded: &Loaded,
        env: ScanEnv<'_, I, S, M, L>,
    ) -> Result<ScanCommit, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let watermark = env.store.index_watermark().await?;
        let mut session = Session {
            env,
            now: exchange.started_at,
            reader: delta.agent,
            exchange: delta.exchange,
            watermark,
            live: LiveSpans::default(),
            fetched: BTreeSet::new(),
            bodies: HashMap::new(),
        };
        let mut found: Vec<ContentMatch> = Vec::new();
        for (message, scanned_as) in loaded.listed() {
            if scanned_as == ScannedAs::Output {
                continue;
            }
            found.extend(self.read_message(&mut session, message, false).await?);
        }
        let mut spans: Vec<Span> = Vec::new();
        if let Some(output) = &loaded.output {
            found.extend(self.read_message(&mut session, output, true).await?);
            let (output_spans, output_matches) =
                self.output_spans(&mut session, output, loaded).await?;
            spans = output_spans;
            found.extend(output_matches);
        }
        let mut ids = BTreeSet::new();
        let mut matches = Vec::with_capacity(found.len());
        for content in found {
            let id = match_id(delta.exchange, content.origin(), &content.read_at());
            if !ids.insert(id) {
                continue;
            }
            matches.push(StoredMatch {
                id,
                ordinal: u32::try_from(matches.len()).unwrap_or(u32::MAX),
                at: exchange.started_at,
                content,
            });
        }
        let messages = loaded
            .listed()
            .into_iter()
            .map(|(message, scanned_as)| (message.hash, scanned_as))
            .collect();
        Ok(ScanCommit {
            exchange: delta.exchange,
            agent: delta.agent,
            at: exchange.started_at,
            spans,
            matches,
            messages,
        })
    }

    /// The winnowed fingerprints of a span's text (its view), positioned in
    /// the span's text.
    pub fn span_kgrams(&self, output: &Message, span: &Span) -> Vec<KGram> {
        let Some(part) = text_parts(output)
            .into_iter()
            .find(|part| part.index == span.location.part.index)
        else {
            return Vec::new();
        };
        let start = usize::try_from(span.location.range.start()).unwrap_or(usize::MAX);
        let end = usize::try_from(span.location.range.end()).unwrap_or(usize::MAX);
        let Some(text) = part.text.get(start..end) else {
            return Vec::new();
        };
        self.winnowing().winnow_mapped(&view(text, part.kind))
    }

    /// Every winnowed fingerprint of every layer of a scanned part.
    fn part_fingerprints(&self, text: &str, kind: PartKind) -> BTreeSet<Fingerprint> {
        let base = view(text, kind);
        self.pipeline()
            .layers(base.text())
            .iter()
            .flat_map(|layer| self.winnowing().winnow(layer.text.text()))
            .map(|kgram| kgram.fingerprint)
            .collect()
    }

    /// The index writes for `spans` (an exchange's committed spans) and the
    /// delta's scanned input parts.
    pub fn index_work(&self, loaded: &Loaded, spans: &[Span]) -> IndexWork {
        let mut work = IndexWork::default();
        if let Some(output) = &loaded.output {
            for span in spans {
                let kgrams = self.span_kgrams(output, span);
                work.observations
                    .push(kgrams.iter().map(|kgram| kgram.fingerprint).collect());
                if span.state != SpanState::Originated {
                    continue;
                }
                let owned = self.owned(kgrams);
                if let Some(originated) = OriginatedSpan::new(span.clone()) {
                    work.postings.push((originated, positioned(&owned)));
                }
            }
        }
        for (message, scanned_as) in loaded.listed() {
            for part in text_parts(message) {
                let scanned = match scanned_as {
                    ScannedAs::Output => part.kind == PartKind::ToolResult,
                    _ => reads::carrier(message, &part).is_some(),
                };
                if scanned {
                    work.observations
                        .push(self.part_fingerprints(&part.text, part.kind).into_iter().collect());
                }
            }
        }
        work
    }
}
