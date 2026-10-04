//! The topics page's view models: version tabs from the topic history,
//! topic rows from a version's sizes and trends, and the lineage to the
//! next version with the rules it leaves stale.

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, ContentRule, StaleReason, TopicWatch, WatchedTopics,
};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    FitRecord, TopicLineage, TopicSizes, TopicVersionHistory, TopicVersionInfo, TopicVersionStatus,
};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::support::{NonEmpty, Similarity};

use crate::components::{format_time, short_id};
use crate::pages::common::links::rule_url;
use crate::pages::common::topics::Trends;
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
    /// Retention dropped its data: sizes over a window and linked views
    /// refuse it.
    pub dropped: bool,
    /// Still being fitted: it cannot be pinned yet.
    pub fitting: bool,
    /// A view can read it: retained and activated at some point (a linked
    /// view refuses a dropped version with `VersionNotRetained` and a
    /// never-activated one with `TopicVersionNotActivated`).
    pub readable: bool,
    pub in_view: bool,
}

/// How a version came to be, in words.
fn fit_detail(info: &TopicVersionInfo) -> String {
    let fitted = |topics: u32, at| format!("{topics} topics · fitted {}", format_time(at));
    match info.status() {
        TopicVersionStatus::Fitting { started_at } => {
            format!("fitting since {}", format_time(*started_at))
        }
        TopicVersionStatus::Ready { fit }
        | TopicVersionStatus::Active {
            fit: FitRecord::Fitted(fit),
            ..
        }
        | TopicVersionStatus::Superseded {
            fit: FitRecord::Fitted(fit),
            ..
        } => fitted(fit.topics, fit.fitted_at),
        TopicVersionStatus::Active {
            fit: FitRecord::Unfitted,
            ..
        }
        | TopicVersionStatus::Superseded {
            fit: FitRecord::Unfitted,
            ..
        } => "unfitted".to_owned(),
    }
}

/// One tab per version of the history, oldest first.
pub fn version_tabs(history: &TopicVersionHistory, in_view: TopicModelVersion) -> Vec<VersionTab> {
    let newest = history.versions().last().map(TopicVersionInfo::version);
    history
        .versions()
        .iter()
        .map(|info| {
            let retention = info.retention();
            VersionTab {
                version: info.version().0,
                label: format!("v{}", info.version().0),
                detail: fit_detail(info),
                pinned: retention.pin().is_some(),
                newest: Some(info.version()) == newest,
                dropped: !retention.is_retained(),
                fitting: matches!(info.status(), TopicVersionStatus::Fitting { .. }),
                readable: retention.is_retained() && info.status().activated_at().is_some(),
                in_view: info.version() == in_view,
            }
        })
        .collect()
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

/// The transmissions `sizes` counts for `topic` (`None`: the outliers).
pub fn size_of(sizes: &TopicSizes, topic: Option<TopicId>) -> u64 {
    match topic {
        Some(topic) => sizes
            .topics()
            .iter()
            .find(|size| size.topic == topic)
            .and_then(|size| size.stats),
        None => sizes.outliers(),
    }
    .map_or(0, |stats| stats.transmissions.get())
}

/// One row per topic, largest first; `watch` builds the watch link when
/// the version is the one new rules target.
pub fn topic_rows(
    topics: &[Topic],
    sizes: &TopicSizes,
    trends: &Trends,
    watch: impl Fn(TopicId) -> Option<String>,
) -> Vec<TopicRow> {
    let mut rows: Vec<TopicRow> = topics
        .iter()
        .map(|t| TopicRow {
            id: t.id,
            label: t.label.clone(),
            terms: t
                .terms
                .iter()
                .take(TERMS)
                .map(|(term, weight)| (term.clone(), format!("{:.2}", weight.get())))
                .collect(),
            transmissions: size_of(sizes, Some(t.id)),
            trend: trends.of(Some(t.id)),
            watch: watch(t.id),
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

/// Where a rule watching only `topic` with `threshold` goes: its topic in
/// the next version, or `None` when the remap leaves it stale. Exactly
/// [`TopicLineage::remap`].
fn carried(lineage: &TopicLineage, topic: TopicId, threshold: Similarity) -> Option<TopicId> {
    let watched = WatchedTopics {
        version: lineage.from(),
        topics: NonEmpty::new(topic),
    };
    match lineage.remap(&watched, threshold) {
        Ok(TopicWatch::Current(next)) => Some(*next.topics.first()),
        Ok(TopicWatch::Stale { .. }) | Err(_) => None,
    }
}

/// Whether the remap leaves `rule` stale over `topic`: a current
/// watched-topic rule on the lineage's source version that
/// [`TopicLineage::remap`] (with the rule's own threshold) leaves stale
/// with `topic` unmapped, or a rule already stale over it in the lineage's
/// target version.
fn stranded(rule: &AlertRuleDef, lineage: &TopicLineage, topic: TopicId) -> bool {
    let remapped = match rule.rule() {
        AlertRule::User {
            content:
                ContentRule::WatchedTopic {
                    watch: TopicWatch::Current(watched),
                    remap_threshold,
                },
            ..
        } if watched.version == lineage.from() => matches!(
            lineage.remap(watched, *remap_threshold),
            Ok(TopicWatch::Stale { unmapped, .. }) if unmapped.iter().any(|t| *t == topic)
        ),
        _ => false,
    };
    let reported = matches!(
        rule.stale_reason(),
        Some(StaleReason::TopicsUnmapped { version, topics })
            if version == lineage.to() && topics.iter().any(|t| *t == topic)
    );
    remapped || reported
}

/// One row per entry of the lineage, mapped at `threshold` (the rule
/// form's default) exactly as [`TopicLineage::remap`] maps; unmapped first.
pub fn remap_rows(
    lineage: &TopicLineage,
    from_topics: &[Topic],
    to_topics: &[Topic],
    rules: &[AlertRuleDef],
    threshold: Similarity,
    state: &ViewState,
) -> Vec<RemapRow> {
    let mut rows: Vec<RemapRow> = lineage
        .entries()
        .iter()
        .map(|entry| {
            let topic = entry.topic();
            let to = carried(lineage, topic, threshold).map(|next| {
                let similarity = entry
                    .best()
                    .filter(|best| best.topic == next)
                    .map_or_else(String::new, |best| format!("{:.2}", best.similarity.get()));
                (label_of(to_topics, next), similarity)
            });
            let stale_rules = if to.is_some() {
                Vec::new()
            } else {
                rules
                    .iter()
                    .filter(|rule| stranded(rule, lineage, topic))
                    .map(|rule| Named {
                        url: rule_url(rule.id(), state),
                        name: rule.name().to_owned(),
                    })
                    .collect()
            };
            RemapRow {
                from: label_of(from_topics, topic),
                to,
                stale_rules,
            }
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
    use std::collections::HashMap;
    use std::num::NonZeroU64;

    use crosstalk_spec::aggregates::alert::{RuleDefinition, RuleName, RuleStatus};
    use crosstalk_spec::aggregates::edge::EdgeStats;
    use crosstalk_spec::aggregates::retention::{Pin, Retention};
    use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
    use crosstalk_spec::aggregates::topic_history::{
        CompletedFit, LineageEntry, LineageLink, TopicSize,
    };
    use crosstalk_spec::ids::{AlertRuleId, OperatorId};
    use crosstalk_spec::support::{Finite, Timestamp};

    use super::*;
    use crate::components::href::tests::state;

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
            terms: vec![
                ("token".into(), Finite::new(0.4213).expect("finite")),
                ("key".into(), Finite::new(0.3).expect("finite")),
            ],
            centroid: Embedding::new(model(), vec![1.0]).expect("embedding"),
            fitted_at: Timestamp::from_micros(0),
        }
    }

    fn similarity(value: f32) -> Similarity {
        Similarity::new(value).expect("similarity")
    }

    fn watched(id: u128, topics: Vec<u128>) -> AlertRuleDef {
        AlertRuleDef::user(
            AlertRuleId::from_ulid((1 << 100) + id),
            RuleName::new("chatter").expect("name"),
            (OperatorId::from_ulid(1), Timestamp::from_micros(0)),
            RuleDefinition::WatchedTopic {
                topics: WatchedTopics {
                    version: TopicModelVersion(1),
                    topics: NonEmpty::from_vec(
                        topics.into_iter().map(TopicId::from_ulid).collect(),
                    )
                    .expect("topics"),
                },
                remap_threshold: similarity(0.8),
            },
            Vec::new(),
        )
        .expect("user rule")
    }

    fn hours(h: u64) -> Timestamp {
        Timestamp::from_micros(h * 3_600_000_000)
    }

    fn fitted(h: u64, topics: u32) -> FitRecord {
        FitRecord::Fitted(CompletedFit {
            started_at: hours(h),
            fitted_at: hours(h),
            ready_at: hours(h),
            topics,
        })
    }

    #[test]
    fn tabs_mark_pinned_newest_dropped_and_in_view() {
        let mut history = TopicVersionHistory::new(vec![
            TopicVersionInfo::with_retention(
                TopicModelVersion(0),
                TopicVersionStatus::Superseded {
                    fit: FitRecord::Unfitted,
                    activated_at: Some(hours(0)),
                    by: TopicModelVersion(1),
                    superseded_at: hours(1),
                },
                Retention::Dropped { at: hours(2) },
            )
            .expect("v0"),
            TopicVersionInfo::new(
                TopicModelVersion(1),
                TopicVersionStatus::Superseded {
                    fit: fitted(1, 3),
                    activated_at: Some(hours(1)),
                    by: TopicModelVersion(2),
                    superseded_at: hours(2),
                },
            )
            .expect("v1"),
            TopicVersionInfo::new(
                TopicModelVersion(2),
                TopicVersionStatus::Active {
                    fit: fitted(2, 5),
                    activated_at: hours(2),
                },
            )
            .expect("v2"),
        ])
        .expect("history");
        history
            .pin(
                TopicModelVersion(1),
                Pin {
                    by: OperatorId::from_ulid(1),
                    at: hours(3),
                },
            )
            .expect("pin");
        let tabs = version_tabs(&history, TopicModelVersion(1));
        assert_eq!(tabs[0].label, "v0");
        assert!(tabs[0].dropped && !tabs[0].pinned && !tabs[0].readable);
        assert_eq!(tabs[0].detail, "unfitted");
        assert!(tabs[1].pinned && tabs[1].in_view && !tabs[1].newest && !tabs[1].dropped);
        assert!(tabs[1].detail.starts_with("3 topics · fitted "));
        assert!(tabs[1].readable && tabs[2].readable);
        assert!(tabs[2].newest);
    }

    #[test]
    fn a_version_never_activated_is_not_readable() {
        let history = TopicVersionHistory::new(vec![
            TopicVersionInfo::new(
                TopicModelVersion(0),
                TopicVersionStatus::Active {
                    fit: FitRecord::Unfitted,
                    activated_at: hours(1),
                },
            )
            .expect("v0"),
            TopicVersionInfo::new(
                TopicModelVersion(1),
                TopicVersionStatus::Ready {
                    fit: CompletedFit {
                        started_at: hours(2),
                        fitted_at: hours(2),
                        ready_at: hours(2),
                        topics: 2,
                    },
                },
            )
            .expect("v1"),
        ])
        .expect("history");
        let tabs = version_tabs(&history, TopicModelVersion(0));
        assert!(tabs[0].readable);
        assert!(!tabs[1].readable && !tabs[1].dropped);
    }

    #[test]
    fn rows_are_largest_first_with_formatted_terms() {
        let topics = vec![topic(1, 1, "Chatter"), topic(2, 1, "Keys")];
        let seven = NonZeroU64::new(7).expect("n");
        let sizes = TopicSizes::new(
            TopicModelVersion(1),
            None,
            vec![
                TopicSize {
                    topic: TopicId::from_ulid(1),
                    stats: None,
                },
                TopicSize {
                    topic: TopicId::from_ulid(2),
                    stats: Some(EdgeStats {
                        transmissions: seven,
                        matched_bytes: seven,
                    }),
                },
            ],
            None,
        )
        .expect("sizes");
        let trends = Trends::new(
            2,
            HashMap::from([(Some(TopicId::from_ulid(2)), vec![3, 4])]),
        );
        let rows = topic_rows(&topics, &sizes, &trends, |_| None);
        assert_eq!(rows[0].label, "Keys");
        assert_eq!(rows[0].transmissions, 7);
        assert_eq!(rows[0].trend, vec![3, 4]);
        assert_eq!(rows[0].terms[0], ("token".to_owned(), "0.42".to_owned()));
        assert_eq!(rows[1].transmissions, 0);
        assert_eq!(rows[1].trend, vec![0, 0], "no series reads as zeros");
    }

    #[test]
    fn unmapped_topics_come_first_with_their_stale_rules() {
        let from = vec![topic(1, 1, "Chatter"), topic(2, 1, "Keys")];
        let to = vec![topic(3, 2, "Credentials")];
        let link = |s: f32| LineageLink {
            topic: TopicId::from_ulid(3),
            similarity: similarity(s),
        };
        let lineage = TopicLineage::new(
            TopicModelVersion(1),
            TopicModelVersion(2),
            similarity(0.6),
            vec![
                LineageEntry::new(TopicId::from_ulid(2), Some(link(0.91)), Vec::new())
                    .expect("keys"),
                LineageEntry::new(TopicId::from_ulid(1), Some(link(0.7)), Vec::new())
                    .expect("chatter"),
            ],
        )
        .expect("lineage");
        let rules = vec![
            watched(9, vec![1]),
            watched(8, vec![2]),
            watched(7, vec![1, 2]),
        ];
        let rows = remap_rows(&lineage, &from, &to, &rules, similarity(0.8), &state());
        assert_eq!(rows[0].from, "Chatter");
        assert_eq!(rows[0].to, None);
        assert_eq!(
            rows[0].stale_rules.len(),
            2,
            "every rule watching it goes stale"
        );
        assert!(rows[0].stale_rules[0].url.starts_with("/alerts/rules/"));
        assert_eq!(
            rows[1].to,
            Some(("Credentials".to_owned(), "0.91".to_owned()))
        );
        assert!(rows[1].stale_rules.is_empty());
        // A rule the re-fit already left stale over the topic is listed too.
        let mut stranded = watched(6, vec![1]);
        stranded
            .remap(&lineage)
            .expect("a current rule on the lineage's version");
        assert!(stranded.stale_reason().is_some());
        stranded.set_enabled(false).expect("disabling is allowed");
        assert_eq!(stranded.status, RuleStatus::Disabled);
        let rows = remap_rows(&lineage, &from, &to, &[stranded], similarity(0.8), &state());
        assert_eq!(rows[0].stale_rules.len(), 1);
        // A lower threshold carries Chatter over too.
        let lenient = remap_rows(&lineage, &from, &to, &[], similarity(0.7), &state());
        assert!(lenient.iter().all(|row| row.to.is_some()));
    }
}
