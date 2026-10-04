//! The topics page's view models: version tabs, topic rows and the remap
//! to the next version with the rules it leaves stale.

use std::collections::HashSet;

use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::ids::TopicId;

use crate::components::{format_time, short_id};
use crate::contract::rules::{RuleDef, RuleKind, RuleStatus, StaleReason, UserRule};
use crate::contract::topics::{TopicStats, TopicVersionInfo, TopicVersionRemap};
use crate::pages::common::links::rule_url;
use crate::pages::common::transmissions::Named;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// Terms shown per topic.
const TERMS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionTab {
    pub version: u32,
    pub label: String,
    pub detail: String,
    pub pinned: bool,
    pub newest: bool,
    pub in_view: bool,
}

pub fn version_tabs(versions: &[TopicVersionInfo], in_view: TopicModelVersion) -> Vec<VersionTab> {
    let newest = versions.iter().map(|v| v.version.0).max();
    let mut tabs: Vec<VersionTab> = versions
        .iter()
        .map(|v| VersionTab {
            version: v.version.0,
            label: format!("v{}", v.version.0),
            detail: format!("{} topics · fitted {}", v.topics, format_time(v.fitted_at)),
            pinned: v.pinned,
            newest: Some(v.version.0) == newest,
            in_view: v.version == in_view,
        })
        .collect();
    tabs.sort_by_key(|t| t.version);
    tabs
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRow {
    pub id: TopicId,
    pub label: String,
    /// `(term, weight)` with the weight formatted.
    pub terms: Vec<(String, String)>,
    pub transmissions: u64,
    pub trend: Vec<u64>,
    pub watch: Option<String>,
}

/// One row per topic, largest first; `watch` builds the watch link when
/// the version is the one new rules target.
pub fn topic_rows(
    topics: &[Topic],
    stats: &[TopicStats],
    watch: impl Fn(TopicId) -> Option<String>,
) -> Vec<TopicRow> {
    let mut rows: Vec<TopicRow> = topics
        .iter()
        .map(|t| {
            let stat = stats.iter().find(|s| s.topic == Some(t.id));
            TopicRow {
                id: t.id,
                label: t.label.clone(),
                terms: t
                    .terms
                    .iter()
                    .take(TERMS)
                    .map(|(term, weight)| (term.clone(), format!("{weight:.2}")))
                    .collect(),
                transmissions: stat.map_or(0, |s| s.transmissions),
                trend: stat.map(|s| s.trend.clone()).unwrap_or_default(),
                watch: watch(t.id),
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        b.transmissions
            .cmp(&a.transmissions)
            .then_with(|| a.label.cmp(&b.label))
    });
    rows
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemapRow {
    pub from: String,
    /// The topic it maps to and the similarity, or `None` when unmapped.
    pub to: Option<(String, String)>,
    /// Rules that watch the unmapped topic and are (or will be) stale.
    pub stale_rules: Vec<Named>,
}

fn label_of(topics: &[Topic], id: TopicId) -> String {
    topics.iter().find(|t| t.id == id).map_or_else(
        || format!("topic {}", short_id(id.to_ulid())),
        |t| t.label.clone(),
    )
}

/// The topics a rule watches that the remap leaves without a match: those
/// a watched-topic rule on the remap's source version names, and those a
/// stale rule reports.
fn stranded(rule: &RuleDef, from: TopicModelVersion, unmapped: &HashSet<TopicId>) -> bool {
    let watched = match &rule.rule {
        RuleKind::User(UserRule::WatchedTopic {
            version, topics, ..
        }) if *version == from => topics.iter().any(|t| unmapped.contains(t)),
        _ => false,
    };
    let reported = match &rule.status {
        RuleStatus::Stale(StaleReason::TopicsUnmapped { topics }) => {
            topics.iter().any(|t| unmapped.contains(t))
        }
        _ => false,
    };
    watched || reported
}

pub fn remap_rows(
    remap: &TopicVersionRemap,
    from_topics: &[Topic],
    to_topics: &[Topic],
    rules: &[RuleDef],
    state: &ViewState,
) -> Vec<RemapRow> {
    let mut rows: Vec<RemapRow> = remap
        .remaps
        .iter()
        .map(|r| RemapRow {
            from: label_of(from_topics, r.from),
            to: r.to.map(|(id, similarity)| {
                (label_of(to_topics, id), format!("{:.2}", similarity.get()))
            }),
            stale_rules: if r.to.is_some() {
                Vec::new()
            } else {
                let unmapped = HashSet::from([r.from]);
                rules
                    .iter()
                    .filter(|rule| stranded(rule, remap.from, &unmapped))
                    .map(|rule| Named {
                        url: rule_url(rule.id, state),
                        name: rule.name.as_str().to_owned(),
                    })
                    .collect()
            },
        })
        .collect();
    // Unmapped first: they are what needs attention.
    rows.sort_by(|a, b| {
        a.to.is_some()
            .cmp(&b.to.is_some())
            .then_with(|| a.from.cmp(&b.from))
    });
    rows
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
    use crosstalk_spec::ids::AlertRuleId;
    use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::rules::{RuleAuthor, RuleName};
    use crate::contract::topics::TopicRemap;

    fn model() -> EmbeddingModel {
        EmbeddingModel {
            name: "m".into(),
            dimension: std::num::NonZeroU16::new(1).expect("n"),
        }
    }

    fn topic(id: u128, version: u32, label: &str) -> Topic {
        Topic {
            id: TopicId::from_ulid(id),
            version: TopicModelVersion(version),
            label: label.to_owned(),
            terms: vec![("token".into(), 0.4213), ("key".into(), 0.3)],
            centroid: Embedding::new(model(), vec![1.0]).expect("embedding"),
            fitted_at: Timestamp::from_micros(0),
        }
    }

    fn watched(id: u128, topics: Vec<u128>) -> RuleDef {
        RuleDef {
            id: AlertRuleId::from_ulid(id),
            name: RuleName::new("chatter").expect("name"),
            rule: RuleKind::User(UserRule::WatchedTopic {
                version: TopicModelVersion(1),
                topics: NonEmpty::from_vec(topics.into_iter().map(TopicId::from_ulid).collect())
                    .expect("topics"),
                remap_threshold: Similarity::new(0.8).expect("threshold"),
            }),
            status: RuleStatus::Enabled,
            created: (RuleAuthor::Config, Timestamp::from_micros(0)),
            sinks: Vec::new(),
        }
    }

    #[test]
    fn tabs_mark_pinned_newest_and_in_view() {
        let model = model();
        let info = |v, pinned| TopicVersionInfo {
            version: TopicModelVersion(v),
            fitted_at: Timestamp::from_micros(0),
            embedding_model: model.clone(),
            topics: 3,
            pinned,
        };
        let tabs = version_tabs(&[info(2, false), info(1, true)], TopicModelVersion(1));
        assert_eq!(tabs[0].label, "v1");
        assert!(tabs[0].pinned && tabs[0].in_view && !tabs[0].newest);
        assert!(tabs[1].newest);
    }

    #[test]
    fn rows_are_largest_first_with_formatted_terms() {
        let topics = vec![topic(1, 1, "Chatter"), topic(2, 1, "Keys")];
        let stats = vec![TopicStats {
            topic: Some(TopicId::from_ulid(2)),
            transmissions: 7,
            trend: vec![3, 4],
        }];
        let rows = topic_rows(&topics, &stats, |_| None);
        assert_eq!(rows[0].label, "Keys");
        assert_eq!(rows[0].terms[0], ("token".to_owned(), "0.42".to_owned()));
        assert_eq!(rows[1].transmissions, 0);
    }

    #[test]
    fn unmapped_topics_come_first_with_their_stale_rules() {
        let from = vec![topic(1, 1, "Chatter"), topic(2, 1, "Keys")];
        let to = vec![topic(3, 2, "Credentials")];
        let remap = TopicVersionRemap {
            from: TopicModelVersion(1),
            to: TopicModelVersion(2),
            remaps: vec![
                TopicRemap {
                    from: TopicId::from_ulid(2),
                    to: Some((TopicId::from_ulid(3), Similarity::new(0.91).expect("s"))),
                },
                TopicRemap {
                    from: TopicId::from_ulid(1),
                    to: None,
                },
            ],
        };
        let rules = vec![watched(9, vec![1]), watched(8, vec![2])];
        let rows = remap_rows(&remap, &from, &to, &rules, &state());
        assert_eq!(rows[0].from, "Chatter");
        assert_eq!(rows[0].to, None);
        assert_eq!(rows[0].stale_rules.len(), 1);
        assert!(rows[0].stale_rules[0].url.starts_with("/alerts/rules/"));
        assert_eq!(
            rows[1].to,
            Some(("Credentials".to_owned(), "0.91".to_owned()))
        );
        assert!(rows[1].stale_rules.is_empty());
    }
}
