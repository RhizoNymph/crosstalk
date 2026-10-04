//! The topic model: v0 (unfitted, every transmission an outlier), v1 (six
//! broad topics) and v2 (one topic per theme). v1's "Engineering chatter"
//! spans three v2 topics and its best link in v2 is below the remap
//! threshold, which is what leaves a watched-topic rule stale. The version
//! history and the lineage are built in [`super::catalog`].

use std::num::NonZeroU16;

use crosstalk_spec::aggregates::topic::{
    Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion,
};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l6_analysis::EmbedError;
use crosstalk_spec::support::{Similarity, Timestamp};

use crate::backend::fixture::clock::{DAY, Mint, ago};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::text::{self, Theme};

use super::{GenError, TopicModel, catalog};

/// When v1 was activated; its fit returned ten minutes earlier.
pub const V1_AT: Timestamp = ago(6 * DAY);
/// When v2 was activated.
pub const V2_AT: Timestamp = ago(2 * DAY);
/// The watched-topic rules' remap threshold: a topic carries over to its
/// best link in the next version only at or above this similarity.
pub const REMAP_THRESHOLD: f32 = 0.8;
const DIMENSION: u16 = 16;

/// v1's topics, as groups of themes.
const V1_GROUPS: &[(&str, &[Theme])] = &[
    ("Deploys and incidents", &[Theme::Deploy, Theme::Incidents]),
    ("Research notes", &[Theme::Research]),
    ("Credentials", &[Theme::Credentials]),
    ("Web automation", &[Theme::Scraping, Theme::Injection]),
    ("Meetings", &[Theme::Meetings]),
    (
        "Engineering chatter",
        &[Theme::CodeReview, Theme::DataPipeline, Theme::Support],
    ),
];

/// The index of v1's topic that maps to nothing in v2.
pub const V1_UNMAPPED: usize = 5;

pub fn model() -> Result<EmbeddingModel, GenError> {
    Ok(EmbeddingModel {
        name: "fixture-minilm-16".to_owned(),
        dimension: NonZeroU16::new(DIMENSION)
            .ok_or_else(|| GenError::Missing("dimension".to_owned()))?,
    })
}

/// The version that was active at `at`.
pub fn version_at(at: Timestamp) -> TopicModelVersion {
    if at >= V2_AT {
        TopicModelVersion(2)
    } else if at >= V1_AT {
        TopicModelVersion(1)
    } else {
        TopicModelVersion(0)
    }
}

/// A unit vector for `theme`: mostly its own axis, a little shared noise.
fn theme_vector(theme: Theme, rng: &mut Rng) -> Vec<f32> {
    let mut v = vec![0.0f32; usize::from(DIMENSION)];
    if let Some(slot) = v.get_mut(theme.index()) {
        *slot = 1.0;
    }
    for x in v.iter_mut().skip(Theme::ALL.len()) {
        *x = (rng.gaussian() * 0.03) as f32;
    }
    v
}

pub fn embedding(model: &EmbeddingModel, mut values: Vec<f32>) -> Result<Embedding, GenError> {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in &mut values {
            *v /= norm;
        }
    }
    Embedding::new(model.clone(), values).map_err(|e| GenError::invalid("Embedding", e))
}

/// Cosine similarity of two unit vectors, mapped to `0..=1`.
pub fn similarity(a: &Embedding, b: &Embedding) -> f32 {
    let cos: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    ((cos + 1.0) / 2.0).clamp(0.0, 1.0)
}

/// Every theme's vector, in `Theme::ALL` order.
fn theme_vectors(seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng::fork(seed, "theme-vectors");
    Theme::ALL
        .iter()
        .map(|t| theme_vector(*t, &mut rng))
        .collect()
}

/// The sum of the theme vectors of `themes`, normalized.
pub fn mix(model: &EmbeddingModel, seed: u64, themes: &[Theme]) -> Result<Embedding, GenError> {
    let vectors = theme_vectors(seed);
    let mut sum = vec![0.0f32; usize::from(DIMENSION)];
    for theme in themes {
        if let Some(v) = vectors.get(theme.index()) {
            for (s, x) in sum.iter_mut().zip(v) {
                *s += x;
            }
        }
    }
    embedding(model, sum)
}

/// Weight of a word's hashed axis in [`embed`], so text that shares no
/// word with any theme still has a direction.
const HASHED_WEIGHT: f32 = 0.1;

/// FNV-1a: a stable hash for a word's axis.
fn word_hash(word: &str) -> u64 {
    word.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// The longest text, in characters, the fixture's embedder takes: its
/// model's context. Longer text is `EmbedError::TooLong`.
pub const QUERY_CONTEXT_CHARS: usize = 1000;

/// The fixture's embedder for query text (semantic query rules): the theme
/// vectors weighted by how many of the text's words are in each theme's
/// vocabulary, plus a little of each word's hashed axis. Deterministic for
/// a seed. Text longer than [`QUERY_CONTEXT_CHARS`] is `TooLong`; a
/// `model` that is not the fixture's model shape is `Model`.
pub fn embed(model: &EmbeddingModel, seed: u64, query: &str) -> Result<Embedding, EmbedError> {
    if query.chars().count() > QUERY_CONTEXT_CHARS {
        return Err(EmbedError::TooLong { index: 0 });
    }
    let words = text::tokens(query);
    let vectors = theme_vectors(seed);
    let dimension = usize::from(DIMENSION);
    let mut sum = vec![0.0f32; dimension];
    for (theme, vector) in Theme::ALL.iter().zip(&vectors) {
        let vocabulary = text::vocabulary(*theme);
        let hits = words.iter().filter(|w| vocabulary.contains(*w)).count();
        for (s, x) in sum.iter_mut().zip(vector) {
            *s += hits as f32 * x;
        }
    }
    for word in &words {
        let axis = usize::try_from(word_hash(word) % u64::from(DIMENSION)).unwrap_or(0);
        if let Some(slot) = sum.get_mut(axis) {
            *slot += HASHED_WEIGHT;
        }
    }
    embedding(model, sum).map_err(|e| EmbedError::Model {
        reason: e.to_string(),
    })
}

fn terms(themes: &[Theme]) -> Vec<(String, f32)> {
    let mut all: Vec<(String, f32)> = themes
        .iter()
        .flat_map(|t| t.terms().iter())
        .map(|(term, weight)| ((*term).to_owned(), *weight / themes.len() as f32))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(8);
    all
}

pub fn build(seed: u64, mint: &mut Mint) -> Result<TopicModel, GenError> {
    let model = model()?;
    let mut topics = Vec::new();
    let mut v1_theme = vec![None; Theme::ALL.len()];
    for (label, themes) in V1_GROUPS {
        let id = TopicId::from_ulid(mint.ulid(V1_AT));
        for theme in *themes {
            if let Some(slot) = v1_theme.get_mut(theme.index()) {
                *slot = Some(id);
            }
        }
        topics.push(Topic {
            id,
            version: TopicModelVersion(1),
            label: (*label).to_owned(),
            terms: terms(themes),
            centroid: mix(&model, seed, themes)?,
            fitted_at: catalog::fitted_at(V1_AT),
        });
    }
    let mut v2_theme = vec![None; Theme::ALL.len()];
    for theme in Theme::ALL {
        let id = TopicId::from_ulid(mint.ulid(V2_AT));
        if let Some(slot) = v2_theme.get_mut(theme.index()) {
            *slot = Some(id);
        }
        topics.push(Topic {
            id,
            version: TopicModelVersion(2),
            label: theme.label().to_owned(),
            terms: terms(&[theme]),
            centroid: mix(&model, seed, &[theme])?,
            fitted_at: catalog::fitted_at(V2_AT),
        });
    }
    let lineages = vec![
        catalog::lineage(&topics, TopicModelVersion(0), TopicModelVersion(1))?,
        catalog::lineage(&topics, TopicModelVersion(1), TopicModelVersion(2))?,
    ];
    Ok(TopicModel {
        model,
        history: catalog::history(&topics)?,
        topics,
        lineages,
        theme_topics: vec![vec![None; Theme::ALL.len()], v1_theme, v2_theme],
    })
}

/// The assignment of one confirmed transmission on `theme` under every
/// version, indexed by version number.
pub fn assign(
    model: &TopicModel,
    theme: Theme,
    rng: &mut Rng,
) -> Result<Vec<Assignment>, GenError> {
    let versions = model.history.versions();
    let mut out = Vec::with_capacity(versions.len());
    for info in versions {
        let outlier_rate = match (info.version().0, theme) {
            (0, _) => 1.0,
            (1, _) => 0.07,
            (_, Theme::Injection) => 0.1,
            _ => 0.05,
        };
        let topic = model.theme_topic(info.version(), theme);
        let assignment = match topic {
            Some(topic) if !rng.chance(outlier_rate) => Assignment::Topic {
                topic,
                confidence: Similarity::new((0.55 + 0.4 * rng.unit()) as f32)
                    .map_err(|e| GenError::invalid("Similarity", e))?,
            },
            _ => Assignment::Outlier,
        };
        out.push(assignment);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_text_embeds_near_its_themes() {
        let model = model().expect("model");
        let paste = embed(
            &model,
            7,
            "credentials or scraped data posted to a paste site",
        )
        .expect("embed");
        assert_eq!(*paste.model(), model);
        let near = mix(&model, 7, &[Theme::Credentials, Theme::Scraping]).expect("mix");
        let far = mix(&model, 7, &[Theme::Meetings]).expect("mix");
        assert!(similarity(&paste, &near) > similarity(&paste, &far));
        assert_eq!(
            embed(&model, 7, "credentials").expect("embed"),
            embed(&model, 7, "credentials").expect("embed"),
            "deterministic"
        );
        assert!(
            embed(&model, 7, "zzz qqq").is_ok(),
            "unrelated text still embeds"
        );
        assert!(embed(&model, 7, &"é".repeat(QUERY_CONTEXT_CHARS)).is_ok());
        assert_eq!(
            embed(&model, 7, &"x".repeat(QUERY_CONTEXT_CHARS + 1)),
            Err(EmbedError::TooLong { index: 0 }),
            "longer than the model's context"
        );
    }
}
