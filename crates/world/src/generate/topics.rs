//! The topic model: v0 (unfitted, every transmission an outlier), v1 (six
//! broad topics) and v2 (one topic per theme). v1's "Engineering chatter"
//! spans three v2 topics and its best link in v2 is below the remap
//! threshold, which is what leaves a watched-topic rule stale. The catalog
//! computes and stores the lineage when each fit returns.

use crosstalk_spec::aggregates::topic::{Assignment, EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::support::{Finite, Similarity, Timestamp};

use crate::embed;
use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::text::Theme;

use super::times::Times;

pub const V0: TopicModelVersion = TopicModelVersion(0);
pub const V1: TopicModelVersion = TopicModelVersion(1);
pub const V2: TopicModelVersion = TopicModelVersion(2);

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

/// The topic model and the topic each theme is assigned to.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicModel {
    pub model: EmbeddingModel,
    /// v1's topics, then v2's.
    pub topics: Vec<Topic>,
    /// Per version, the topic each theme is assigned to (`None`: outlier).
    pub theme_topics: Vec<Vec<Option<TopicId>>>,
}

impl TopicModel {
    pub fn topics_of(&self, version: TopicModelVersion) -> impl Iterator<Item = &Topic> {
        self.topics.iter().filter(move |t| t.version == version)
    }

    pub fn theme_topic(&self, version: TopicModelVersion, theme: Theme) -> Option<TopicId> {
        let index = usize::try_from(version.0).ok()?;
        self.theme_topics
            .get(index)?
            .get(theme.index())
            .copied()
            .flatten()
    }

    /// v1's "Engineering chatter".
    pub fn unmapped(&self) -> Result<TopicId, WorldError> {
        self.topics_of(V1)
            .nth(V1_UNMAPPED)
            .map(|t| t.id)
            .ok_or_else(|| WorldError::missing("unmapped v1 topic"))
    }
}

/// The version that was active at `at`.
pub fn version_at(times: &Times, at: Timestamp) -> TopicModelVersion {
    if at >= times.v2_at {
        V2
    } else if at >= times.v1_at {
        V1
    } else {
        V0
    }
}

fn terms(themes: &[Theme]) -> Result<Vec<(String, Finite)>, WorldError> {
    let mut all: Vec<(String, f32)> = themes
        .iter()
        .flat_map(|t| t.terms().iter())
        .map(|(term, weight)| ((*term).to_owned(), *weight / themes.len() as f32))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(8);
    all.into_iter()
        .map(|(term, weight)| {
            Finite::new(weight)
                .map(|weight| (term, weight))
                .map_err(|e| WorldError::invalid("term weight", e))
        })
        .collect()
}

pub fn build(
    seed: u64,
    times: &Times,
    model: EmbeddingModel,
    mint: &mut Mint,
) -> Result<TopicModel, WorldError> {
    let mut topics = Vec::new();
    let v1_fitted = Times::fitted_at(times.v1_at);
    let v2_fitted = Times::fitted_at(times.v2_at);
    let mut v1_theme = vec![None; Theme::ALL.len()];
    for (label, themes) in V1_GROUPS {
        let id: TopicId = mint.at(times.v1_at)?;
        for theme in *themes {
            if let Some(slot) = v1_theme.get_mut(theme.index()) {
                *slot = Some(id);
            }
        }
        topics.push(Topic {
            id,
            version: V1,
            label: (*label).to_owned(),
            terms: terms(themes)?,
            centroid: embed::mix(&model, seed, themes)?,
            fitted_at: v1_fitted,
        });
    }
    let mut v2_theme = vec![None; Theme::ALL.len()];
    for theme in Theme::ALL {
        let id: TopicId = mint.at(times.v2_at)?;
        if let Some(slot) = v2_theme.get_mut(theme.index()) {
            *slot = Some(id);
        }
        topics.push(Topic {
            id,
            version: V2,
            label: theme.label().to_owned(),
            terms: terms(&[theme])?,
            centroid: embed::mix(&model, seed, &[theme])?,
            fitted_at: v2_fitted,
        });
    }
    Ok(TopicModel {
        model,
        topics,
        theme_topics: vec![vec![None; Theme::ALL.len()], v1_theme, v2_theme],
    })
}

/// The assignment of one confirmed transmission on `theme` under every
/// version, indexed by version number.
pub fn assign(
    model: &TopicModel,
    theme: Theme,
    rng: &mut Rng,
) -> Result<Vec<Assignment>, WorldError> {
    let versions = u32::try_from(model.theme_topics.len())
        .map_err(|e| WorldError::invalid("version count", e))?;
    let mut out = Vec::with_capacity(model.theme_topics.len());
    for version in (0..versions).map(TopicModelVersion) {
        let outlier_rate = match (version.0, theme) {
            (0, _) => 1.0,
            (1, _) => 0.07,
            (_, Theme::Injection) => 0.1,
            _ => 0.05,
        };
        let topic = model.theme_topic(version, theme);
        let assignment = match topic {
            Some(topic) if !rng.chance(outlier_rate) => Assignment::Topic {
                topic,
                confidence: Similarity::new((0.55 + 0.4 * rng.unit()) as f32)
                    .map_err(|e| WorldError::invalid("Similarity", e))?,
            },
            _ => Assignment::Outlier,
        };
        out.push(assignment);
    }
    Ok(out)
}
