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
//! A fingerprint's frequency is the number of live (unexpired) spans of any
//! origin whose text contains it. Fingerprints above the cutoff are
//! boilerplate.
//!
//! Implementations:
//! - `Segmenter`: `NovelRunSegmenter`.
//! - `Decoder`: `UnicodeNormalizer`, `Base64Decoder`, `HexDecoder`,
//!   `UrlDecoder`.
//! - `Fingerprinter`: `Winnowing`.
//! - `FingerprintIndex`: `PgFingerprintIndex`, `ShardedMemIndex`.
//! - `SemanticMatcher`: `EmbeddingSimilarityMatcher`, an optional second
//!   stage for paraphrase that produces `MatchKind::Semantic`.

use crate::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint, WinnowParams,
};
use crate::derived::provenance::matching::Codec;
use crate::derived::provenance::span::{Origin, OriginatedSpan, SpanLocation};
use crate::ids::SpanId;
use crate::observed::message::Message;
use crate::support::{ByteRange, Similarity};

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
    /// Only originated spans can be indexed.
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
    ) -> Result<(), IndexError>;

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
    ) -> Result<Vec<FingerprintHit>, IndexError>;

    /// How many spans contain `fingerprint`. Fingerprints above the cutoff
    /// are boilerplate: not indexed, and ignored on lookup.
    async fn frequency(&self, fingerprint: Fingerprint) -> Result<u64, IndexError>;

    /// Remove the fingerprints of expired spans.
    async fn evict(&mut self, spans: &[SpanId]) -> Result<(), IndexError>;
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
    async fn lookup(
        &self,
        text: &str,
        threshold: Similarity,
    ) -> Result<Vec<SemanticHit>, IndexError>;
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
