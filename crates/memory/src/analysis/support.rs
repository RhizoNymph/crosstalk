//! The one similarity function every L6 score uses (search, lineage,
//! semantic rules). The shared store building blocks (ids, the outbox,
//! locks, cursors) are in [`crate::support`].

use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::support::Similarity;

/// The similarity of two embeddings: their cosine (the dot product, both
/// being unit vectors) clamped to `0.0..=1.0`, so anti-correlated vectors
/// score 0. `None` when they come from different models, which are never
/// compared.
pub fn similarity(a: &Embedding, b: &Embedding) -> Option<Similarity> {
    if a.model() != b.model() {
        return None;
    }
    let dot: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    // Clamped into the range, so the constructor cannot refuse it. Anything
    // not above zero (negative, -0.0, and NaN, impossible for checked
    // embeddings) becomes +0.0, so equal scores compare equal everywhere.
    let clamped = if dot > 0.0 { dot.min(1.0) } else { 0.0 };
    Similarity::new(clamped).ok()
}
