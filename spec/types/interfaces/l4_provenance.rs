//! L4 provenance: span extraction, fingerprinting and matching. Consumer
//! group `provenance`, sharded by fingerprint, triggered by
//! `ConversationDelta`.
//!
//! For each delta: the output is segmented into spans and the originated
//! ones are indexed; the new inputs (and a new system prompt) are decoded,
//! fingerprinted and looked up, and hits on other agents' spans become
//! content matches. The output is looked up too: another agent's text in an
//! agent's output that none of its visible inputs contained is a match with
//! carrier `ReaderOutput`, evidence of a channel the gateway cannot see.
//!
//! A fingerprint's frequency at `now` is the number of distinct texts
//! (spans of any origin, and scanned input parts) containing it whose
//! observation time is within the retention period before `now`
//! (`now - retention <= at`). Fingerprints above the cutoff are boilerplate.
//! The index takes `now` as an argument on every call that measures that
//! window (the provenance consumer passes the time of the exchange it is
//! scanning), so a replay scans with the frequencies it scanned with.
//!
//! Implementations:
//! - `Segmenter`: `NovelRunSegmenter`.
//! - `Decoder`: `UnicodeNormalizer`, `Base64Decoder`, `HexDecoder`,
//!   `UrlDecoder`.
//! - `Fingerprinter`: `Winnowing`.
//! - `FingerprintIndex`: `PgFingerprintIndex`, `ShardedMemIndex`.
//! - `SemanticMatcher`: `EmbeddingSimilarityMatcher`, an optional second
//!   stage for paraphrase that produces `MatchKind::Semantic`.

use crate::aggregates::topic::Embedding;
use crate::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint, WinnowParams,
};
use crate::derived::provenance::matching::Codec;
use crate::derived::provenance::span::{Origin, OriginatedSpan, SpanLocation};
use crate::ids::SpanId;
use crate::observed::message::Message;
use crate::support::{ByteRange, Similarity, Timestamp};

/// A span before it has an id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanDraft {
    pub location: SpanLocation,
    pub origin: Origin,
}

pub trait Segmenter {
    /// Cut `output` into spans and classify each against `inputs` (the
    /// exchange's full request history).
    fn segment(&self, output: &Message, inputs: &[Message]) -> Vec<SpanDraft>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub codec: Codec,
    /// Where the encoded text sat in the input.
    pub source: ByteRange,
    pub text: String,
}

pub trait Decoder {
    fn codec(&self) -> Codec;

    /// Every substring of `text` this codec can decode, decoded. Decoders run
    /// repeatedly on their own output, up to a fixed depth.
    fn decode(&self, text: &str) -> Vec<Decoded>;
}

pub trait Fingerprinter {
    fn params(&self) -> WinnowParams;

    /// Fingerprints of `text` after whitespace and case normalization.
    fn fingerprints(&self, text: &str) -> Vec<PositionedFingerprint>;
}

pub trait FingerprintIndex {
    /// Only originated spans can be indexed. A fingerprint that is
    /// boilerplate at `now` gets no posting.
    fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// The postings of `fingerprints`, leaving out every fingerprint that is
    /// boilerplate at `now`.
    fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> impl Future<Output = Result<Vec<FingerprintHit>, IndexError>> + Send;

    /// How many observed texts contain `fingerprint` at `now`. Fingerprints
    /// above the cutoff are boilerplate: not indexed, and ignored on lookup.
    fn frequency(
        &self,
        fingerprint: Fingerprint,
        now: Timestamp,
    ) -> impl Future<Output = Result<u64, IndexError>> + Send;

    /// Count every fingerprint of a text scanned at `at` toward
    /// `frequency`, whether or not it is indexed, as of `now`: an
    /// observation already outside the retention period at `now` is not
    /// kept. Observations age out after retention.
    fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// Remove the fingerprints of expired spans; observations outside the
    /// retention period at `now` age out.
    fn evict(
        &mut self,
        spans: &[SpanId],
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;
}

/// A candidate paraphrase: an originated span whose embedding is close to a
/// window of the reader's text.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticHit {
    pub span: SpanId,
    pub read_range: ByteRange,
    pub score: Similarity,
}

pub trait SemanticMatcher {
    /// Only originated spans can be stored.
    fn insert(
        &mut self,
        span: &OriginatedSpan,
        embedding: Embedding,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    fn lookup(
        &self,
        text: &str,
        threshold: Similarity,
    ) -> impl Future<Output = Result<Vec<SemanticHit>, IndexError>> + Send;

    /// Remove expired spans.
    fn evict(&mut self, spans: &[SpanId]) -> impl Future<Output = Result<(), IndexError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    Store {
        reason: String,
    },
    /// The fingerprint belongs to a shard this node does not own.
    WrongShard {
        fingerprint: Fingerprint,
    },
}
