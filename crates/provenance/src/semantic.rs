//! The semantic matcher, a stub until embeddings exist (roadmap P6.2).
//!
//! [`DisabledSemanticMatcher`] implements the spec's `SemanticMatcher`: it
//! stores nothing and finds nothing, so provenance runs fingerprint
//! matching only. The scanner already calls `lookup` for every scanned
//! part that had no fingerprint match and keeps only hits at or above the
//! configured threshold on live, other agents' spans
//! (`provenance.semantic.fallback-only`,
//! `provenance.semantic.score-above-threshold`), so an
//! `EmbeddingSimilarityMatcher` drops in behind the trait. Inserting an
//! originated span needs its embedding, which the scanner cannot compute
//! yet; that call is wired when the embedder lands.

use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{IndexError, SemanticHit, SemanticMatcher};
use crosstalk_spec::support::Similarity;

/// A semantic matcher that holds nothing and never matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DisabledSemanticMatcher;

impl SemanticMatcher for DisabledSemanticMatcher {
    async fn insert(
        &mut self,
        _span: &OriginatedSpan,
        _embedding: Embedding,
    ) -> Result<(), IndexError> {
        Ok(())
    }

    async fn lookup(
        &self,
        _text: &str,
        _threshold: Similarity,
    ) -> Result<Vec<SemanticHit>, IndexError> {
        Ok(Vec::new())
    }

    async fn evict(&mut self, _spans: &[SpanId]) -> Result<(), IndexError> {
        Ok(())
    }
}
