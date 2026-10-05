//! The world's embedder: a deterministic stand-in for a text embedding
//! model, as the spec's [`Embedder`].
//!
//! Each theme has a unit vector (mostly its own axis, a little seeded
//! noise). A text embeds as the theme vectors weighted by how many of its
//! words are in each theme's vocabulary, plus a little of each word's
//! hashed axis, normalized. Topic centroids are mixes of theme vectors
//! ([`mix`]), so a text on a theme lands near that theme's topics. A host
//! hands the same embedder to the alert store (semantic rules) that the
//! seed used for the search corpus, so query and document vectors agree.

use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::interfaces::l6_analysis::{EmbedError, Embedder};

use crate::error::WorldError;
use crate::rng::Rng;
use crate::text::{self, Theme};

/// Weight of a word's hashed axis, so text that shares no word with any
/// theme still has a direction.
const HASHED_WEIGHT: f32 = 0.1;

/// The world's text embedder.
#[derive(Debug, Clone, PartialEq)]
pub struct WorldEmbedder {
    model: EmbeddingModel,
    seed: u64,
    max_chars: usize,
}

impl WorldEmbedder {
    /// The embedder of the world seeded from `seed`, refusing texts longer
    /// than `max_chars` characters.
    pub fn new(model: EmbeddingModel, seed: u64, max_chars: usize) -> Self {
        Self {
            model,
            seed,
            max_chars,
        }
    }

    /// The embedding of one text.
    pub fn embed_one(&self, text: &str) -> Result<Embedding, EmbedError> {
        if text.chars().count() > self.max_chars {
            return Err(EmbedError::TooLong { index: 0 });
        }
        let dimension = usize::from(self.model.dimension.get());
        let words = text::tokens(text);
        let vectors = theme_vectors(self.seed, dimension);
        let mut sum = vec![0.0f32; dimension];
        for (theme, vector) in Theme::ALL.iter().zip(&vectors) {
            let vocabulary = text::vocabulary(*theme);
            let hits = words.iter().filter(|w| vocabulary.contains(*w)).count();
            for (s, x) in sum.iter_mut().zip(vector) {
                *s += hits as f32 * x;
            }
        }
        let axes = u64::try_from(dimension).unwrap_or(1).max(1);
        for word in &words {
            let axis = usize::try_from(word_hash(word) % axes).unwrap_or(0);
            if let Some(slot) = sum.get_mut(axis) {
                *slot += HASHED_WEIGHT;
            }
        }
        normalized(&self.model, sum).map_err(|e| EmbedError::Model {
            reason: e.to_string(),
        })
    }

    /// The embedding of at most `max_chars` characters of `text`: how the
    /// seed embeds a transmission's text for the search corpus.
    pub fn embed_prefix(&self, text: &str) -> Result<Embedding, EmbedError> {
        let prefix: String = text.chars().take(self.max_chars).collect();
        self.embed_one(&prefix)
    }
}

impl Embedder for WorldEmbedder {
    fn model(&self) -> EmbeddingModel {
        self.model.clone()
    }

    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        if let Some(index) = texts
            .iter()
            .position(|text| text.chars().count() > self.max_chars)
        {
            return Err(EmbedError::TooLong { index });
        }
        texts.iter().map(|text| self.embed_one(text)).collect()
    }
}

/// FNV-1a: a stable hash for a word's axis.
fn word_hash(word: &str) -> u64 {
    word.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A unit-ish vector for `theme`: its own axis, a little shared noise on
/// the axes past the themes'.
fn theme_vector(theme: Theme, dimension: usize, rng: &mut Rng) -> Vec<f32> {
    let mut v = vec![0.0f32; dimension];
    if let Some(slot) = v.get_mut(theme.index()) {
        *slot = 1.0;
    }
    for x in v.iter_mut().skip(Theme::ALL.len()) {
        *x = (rng.gaussian() * 0.03) as f32;
    }
    v
}

/// Every theme's vector, in `Theme::ALL` order.
fn theme_vectors(seed: u64, dimension: usize) -> Vec<Vec<f32>> {
    let mut rng = Rng::fork(seed, "theme-vectors");
    Theme::ALL
        .iter()
        .map(|t| theme_vector(*t, dimension, &mut rng))
        .collect()
}

/// `values` scaled to unit length, as an embedding of `model`. A zero
/// vector (a text without words) becomes the first unit vector.
pub fn normalized(model: &EmbeddingModel, mut values: Vec<f32>) -> Result<Embedding, WorldError> {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut values {
            *v /= norm;
        }
    } else if let Some(first) = values.first_mut() {
        *first = 1.0;
    }
    Embedding::new(model.clone(), values).map_err(|e| WorldError::invalid("Embedding", e))
}

/// The sum of the theme vectors of `themes`, normalized: a topic centroid.
pub fn mix(model: &EmbeddingModel, seed: u64, themes: &[Theme]) -> Result<Embedding, WorldError> {
    let dimension = usize::from(model.dimension.get());
    let vectors = theme_vectors(seed, dimension);
    let mut sum = vec![0.0f32; dimension];
    for theme in themes {
        if let Some(v) = vectors.get(theme.index()) {
            for (s, x) in sum.iter_mut().zip(v) {
                *s += x;
            }
        }
    }
    normalized(model, sum)
}

/// The cosine of two unit vectors clamped to `0..=1`: the similarity the
/// memory catalog's lineage uses.
pub fn similarity(a: &Embedding, b: &Embedding) -> f32 {
    let cos: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    cos.clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EMBED_MAX_CHARS, embedding_model};

    fn embedder(seed: u64) -> Result<WorldEmbedder, WorldError> {
        Ok(WorldEmbedder::new(
            embedding_model()?,
            seed,
            EMBED_MAX_CHARS,
        ))
    }

    #[test]
    fn query_text_embeds_near_its_themes() -> Result<(), Box<dyn std::error::Error>> {
        let model = embedding_model()?;
        let embedder = embedder(7)?;
        let paste = embedder
            .embed_one("credentials or scraped data posted to a paste site")
            .map_err(|e| format!("{e:?}"))?;
        assert_eq!(*paste.model(), model);
        let near = mix(&model, 7, &[Theme::Credentials, Theme::Scraping])?;
        let far = mix(&model, 7, &[Theme::Meetings])?;
        assert!(similarity(&paste, &near) > similarity(&paste, &far));
        assert_eq!(
            embedder.embed_one("credentials"),
            embedder.embed_one("credentials"),
            "deterministic"
        );
        assert!(
            embedder.embed_one("zzz qqq").is_ok(),
            "unrelated text embeds"
        );
        assert!(embedder.embed_one(&"é".repeat(EMBED_MAX_CHARS)).is_ok());
        assert_eq!(
            embedder.embed_one(&"x".repeat(EMBED_MAX_CHARS + 1)),
            Err(EmbedError::TooLong { index: 0 })
        );
        assert!(embedder.embed_prefix(&"x ".repeat(EMBED_MAX_CHARS)).is_ok());
        Ok(())
    }

    #[test]
    fn mixed_centroids_sit_where_the_remap_threshold_expects() -> Result<(), WorldError> {
        let model = embedding_model()?;
        let one = mix(&model, 3, &[Theme::Deploy])?;
        let two = mix(&model, 3, &[Theme::Deploy, Theme::Incidents])?;
        let three = mix(
            &model,
            3,
            &[Theme::CodeReview, Theme::DataPipeline, Theme::Support],
        )?;
        let review = mix(&model, 3, &[Theme::CodeReview])?;
        let threshold = crate::config::REMAP_THRESHOLD;
        assert!(similarity(&two, &one) >= threshold, "two themes carry over");
        assert!(similarity(&three, &review) < threshold, "three do not");
        assert!(similarity(&three, &review) < crate::config::LINEAGE_FLOOR);
        Ok(())
    }
}
