//! Alert rules on the wire: `QueryApi::alert_rules` (a page of
//! `AlertRuleDef`), the `UserRule` a client writes in `CreateRule` and
//! `UpdateRule`, and the kinds, statuses and revisions around them.

use serde_json::{Value, json};

use super::super::harness::{
    assert_golden, assert_rejected, assert_request_golden, assert_round_trips,
};
use super::super::{ULID_A, ULID_B, id};
use super::{at, embedding, model, operator, other_model, sim, topic, version};
use crate::aggregates::alert::{
    AlertRule, AlertRuleDef, AlertRuleKind, BuiltinRule, ContentRule, QueryWatch, RuleName,
    RuleRevision, RuleStatus, SemanticQuery, TopicWatch, UserRule, WatchedTopics,
};
use crate::ids::{AlertRuleId, SinkId};
use crate::paging::{AlertRuleList, Cursor, Page, PageSize};
use crate::support::{NonBlank, NonEmpty};

const AREA: &str = "rules";

fn rule_id() -> AlertRuleId {
    id(AlertRuleId::from_ulid_text, ULID_A)
}

fn sink() -> SinkId {
    id(SinkId::from_ulid_text, ULID_B)
}

fn name(text: &str) -> RuleName {
    RuleName::new(text).expect("a short rule name")
}

fn watched(version_n: u32, topics: [usize; 2]) -> WatchedTopics {
    WatchedTopics {
        version: version(version_n),
        topics: NonEmpty::from_vec(topics.into_iter().map(topic).collect()).expect("two topics"),
    }
}

fn query() -> SemanticQuery {
    SemanticQuery {
        text: NonBlank::new("credentials pasted into a shared doc").expect("not blank"),
        embedding: embedding(model()),
    }
}

/// A stored user rule with `content`, enabled unless it is stale.
fn user_rule(rule_name: &str, content: ContentRule) -> AlertRuleDef {
    let status = if content.stale_reason().is_some() {
        RuleStatus::Disabled
    } else {
        RuleStatus::Enabled
    };
    AlertRuleDef::load(
        rule_id(),
        name(rule_name),
        (operator(), at("09:15:00")),
        content,
        status,
        vec![sink()],
    )
    .expect("a generated id is not reserved")
}

/// One stored user rule per content and watch state.
fn every_content() -> Vec<(&'static str, ContentRule)> {
    fn declared(content: ContentRule) -> ContentRule {
        match content {
            ContentRule::WatchedTopic {
                watch: TopicWatch::Current(_) | TopicWatch::Stale { .. },
                ..
            }
            | ContentRule::SemanticQuery {
                watch: QueryWatch::Current(_) | QueryWatch::Stale { .. },
                ..
            } => content,
        }
    }
    [
        (
            "alert_rule_watched_topic_current",
            ContentRule::WatchedTopic {
                watch: TopicWatch::Current(watched(3, [0, 1])),
                remap_threshold: sim(0.75),
            },
        ),
        (
            "alert_rule_watched_topic_stale",
            ContentRule::WatchedTopic {
                watch: TopicWatch::Stale {
                    last: watched(3, [0, 1]),
                    unmapped_in: version(4),
                    unmapped: NonEmpty::new(topic(1)),
                },
                remap_threshold: sim(0.75),
            },
        ),
        (
            "alert_rule_semantic_query_current",
            ContentRule::SemanticQuery {
                watch: QueryWatch::under(query(), &model()),
                threshold: sim(0.5),
            },
        ),
        (
            "alert_rule_semantic_query_stale",
            ContentRule::SemanticQuery {
                watch: QueryWatch::under(query(), &other_model()),
                threshold: sim(0.5),
            },
        ),
    ]
    .into_iter()
    .map(|(golden, content)| (golden, declared(content)))
    .collect()
}

#[test]
fn stored_rules_golden_for_every_kind_and_watch_state() {
    let builtin = AlertRuleDef::builtin(
        BuiltinRule::UnsanctionedTraffic,
        RuleStatus::Enabled,
        vec![sink()],
    );
    assert_golden(AREA, "alert_rule_builtin", &builtin);
    for (golden, content) in every_content() {
        assert_golden(AREA, golden, &user_rule("Leaked credentials", content));
    }
}

/// `QueryApi::alert_rules`: built-in rules first, then user rules.
#[test]
fn alert_rules_page_golden() {
    let size = PageSize::new(2).expect("a valid size");
    let items = NonEmpty::from_vec(vec![
        AlertRuleDef::builtin(BuiltinRule::NewChannel, RuleStatus::Disabled, Vec::new()),
        AlertRuleDef::builtin(
            BuiltinRule::UnreviewedTraffic,
            RuleStatus::Enabled,
            vec![sink()],
        ),
    ])
    .expect("two rules");
    let next: Cursor<AlertRuleList> =
        Cursor::from_token("cnVsZXMtYWZ0ZXItMg".into()).expect("URL-safe base64");
    let page = Page::more(size, items, next).expect("two rules fit a page of two");
    assert_golden(AREA, "alert_rules_page", &page);
}

/// What a client writes in `CreateRule` and `UpdateRule`: a request, so it
/// carries no author or time.
#[test]
fn user_rules_golden_as_requests() {
    fn declared(rule: UserRule) -> UserRule {
        match rule {
            UserRule::WatchedTopic { .. } | UserRule::SemanticQuery { .. } => rule,
        }
    }
    let watched_topic = declared(UserRule::WatchedTopic {
        topics: watched(3, [0, 1]),
        remap_threshold: Some(sim(0.75)),
    });
    assert_request_golden(AREA, "user_rule_watched_topic", &watched_topic);
    let watch_one = declared(UserRule::watch_topic(version(3), topic(2)));
    assert_request_golden(AREA, "user_rule_watch_topic_default_threshold", &watch_one);
    let semantic = declared(UserRule::SemanticQuery {
        text: NonBlank::new("credentials pasted into a shared doc").expect("not blank"),
        threshold: sim(0.5),
    });
    assert_request_golden(AREA, "user_rule_semantic_query", &semantic);
}

#[test]
fn rule_kinds_statuses_and_revisions_golden() {
    fn kind(kind: AlertRuleKind) -> AlertRuleKind {
        match kind {
            AlertRuleKind::NewChannel
            | AlertRuleKind::UnreviewedTraffic
            | AlertRuleKind::UnsanctionedTraffic
            | AlertRuleKind::SanctionedUnused
            | AlertRuleKind::SuspectedTransmission
            | AlertRuleKind::WatchedTopic
            | AlertRuleKind::SemanticQuery => kind,
        }
    }
    let kinds = [
        AlertRuleKind::NewChannel,
        AlertRuleKind::UnreviewedTraffic,
        AlertRuleKind::UnsanctionedTraffic,
        AlertRuleKind::SanctionedUnused,
        AlertRuleKind::SuspectedTransmission,
        AlertRuleKind::WatchedTopic,
        AlertRuleKind::SemanticQuery,
    ]
    .map(kind);
    assert_golden(AREA, "alert_rule_kinds", &kinds.to_vec());

    fn builtin(rule: BuiltinRule) -> BuiltinRule {
        match rule {
            BuiltinRule::NewChannel
            | BuiltinRule::UnreviewedTraffic
            | BuiltinRule::UnsanctionedTraffic
            | BuiltinRule::SanctionedUnused
            | BuiltinRule::SuspectedTransmission => rule,
        }
    }
    assert_golden(
        AREA,
        "builtin_rules",
        &BuiltinRule::ALL.map(builtin).to_vec(),
    );
    // Every built-in rule round-trips as a stored rule under its fixed id.
    for rule in BuiltinRule::ALL {
        assert_round_trips(&AlertRuleDef::builtin(
            rule,
            RuleStatus::Enabled,
            Vec::new(),
        ));
    }

    fn status(status: RuleStatus) -> RuleStatus {
        match status {
            RuleStatus::Enabled | RuleStatus::Disabled => status,
        }
    }
    let statuses = [RuleStatus::Enabled, RuleStatus::Disabled].map(status);
    assert_golden(AREA, "rule_statuses", &statuses.to_vec());

    let revision = RuleRevision::CREATED
        .next()
        .and_then(RuleRevision::next)
        .expect("3 fits");
    assert_golden(AREA, "rule_revision", &revision);
}

fn builtin_json(rule: &str, rule_id: &str) -> String {
    json!({
        "id": rule_id,
        "rule": {"type": "builtin", "data": rule},
        "status": "enabled",
        "sinks": [],
    })
    .to_string()
}

/// `AlertRuleDef` decodes through `AlertRuleDef::builtin` or `load`, so its
/// ids are checked as when it was built.
#[test]
fn stored_rules_refuse_ids_their_constructors_refuse() {
    // A built-in rule under another built-in's id, and under a generated id.
    assert_rejected::<AlertRuleDef>(
        &builtin_json("new_channel", "00000000000000000000000002"),
        "invalid alert rule: BuiltinId { rule: NewChannel",
    );
    assert_rejected::<AlertRuleDef>(
        &builtin_json("suspected_transmission", ULID_A),
        "invalid alert rule: BuiltinId { rule: SuspectedTransmission",
    );
    // Its own fixed id decodes.
    let fixed = serde_json::from_str::<AlertRuleDef>(&builtin_json(
        "suspected_transmission",
        "00000000000000000000000005",
    ))
    .map(|rule| rule.id());
    assert_eq!(fixed.ok(), Some(BuiltinRule::SuspectedTransmission.id()));

    // A user rule under an id reserved for built-in rules, including one no
    // built-in rule has.
    let (_, content) = every_content().remove(0);
    let user =
        serde_json::to_value(user_rule("Leaked credentials", content)).expect("a rule encodes");
    for reserved in ["00000000000000000000000001", "00000000000000000000000009"] {
        let mut json = user.clone();
        json["id"] = Value::from(reserved);
        assert_rejected::<AlertRuleDef>(&json.to_string(), "invalid alert rule: Reserved");
    }
}

#[test]
fn rules_refuse_unknown_fields_variants_and_out_of_range_values() {
    let builtin = AlertRuleDef::builtin(BuiltinRule::NewChannel, RuleStatus::Enabled, Vec::new());
    let mut json = serde_json::to_value(&builtin).expect("a rule encodes");
    json["severity"] = Value::from("high");
    assert_rejected::<AlertRuleDef>(&json.to_string(), "unknown field `severity`");
    assert_rejected::<AlertRuleDef>(
        &builtin_json("idle_agent", "00000000000000000000000001"),
        "unknown variant `idle_agent`",
    );
    assert_rejected::<AlertRule>(
        r#"{"type": "builtin", "data": "new_channel", "name": "x"}"#,
        r#"expected "type" or "data""#,
    );
    assert_rejected::<AlertRuleKind>(r#""keyword""#, "unknown variant `keyword`");
    assert_rejected::<RuleStatus>(r#""stale""#, "unknown variant `stale`");
    assert_rejected::<RuleRevision>("0", "invalid value: integer `0`");
    assert_rejected::<TopicWatch>(
        r#"{"type": "remapped", "data": {"version": 3, "topics": []}}"#,
        "unknown variant `remapped`",
    );
    assert_rejected::<QueryWatch>(r#"{"type": "current"}"#, "missing field `data`");
}

#[test]
fn user_rules_refuse_what_a_client_must_not_send() {
    let watched = |threshold: &str| {
        format!(
            r#"{{"type": "watched_topic", "data": {{
                "topics": {{"version": 3, "topics": ["{ULID_A}"]}},
                "remap_threshold": {threshold}}}}}"#
        )
    };
    assert_rejected::<UserRule>(&watched("1.5"), "invalid similarity");
    // Too large for an `f32`: decodes to infinity, which is out of range.
    assert_rejected::<UserRule>(&watched("1e39"), "invalid similarity");
    assert_rejected::<UserRule>(&watched("-1e39"), "invalid similarity");
    assert_rejected::<UserRule>(
        r#"{"type": "watched_topic", "data": {
            "topics": {"version": 3, "topics": []}, "remap_threshold": null}}"#,
        "invalid non-empty list",
    );
    assert_rejected::<UserRule>(
        r#"{"type": "semantic_query", "data": {"text": "   ", "threshold": 0.5}}"#,
        "invalid non-blank text",
    );
    // The author is stamped from the caller, never sent.
    assert_rejected::<UserRule>(
        &format!(
            r#"{{"type": "semantic_query", "data": {{"text": "x", "threshold": 0.5,
                "created": ["{ULID_A}", "2026-10-04T09:15:00.000000Z"]}}}}"#
        ),
        "unknown field `created`",
    );
    assert_rejected::<UserRule>(
        r#"{"type": "keyword", "data": {"text": "x"}}"#,
        "unknown variant `keyword`",
    );
    assert_rejected::<WatchedTopics>(
        &format!(r#"{{"version": 3, "topics": ["{ULID_A}"], "since": 2}}"#),
        "unknown field `since`",
    );
    // A stored semantic query holds a unit-norm embedding of its model.
    assert_rejected::<SemanticQuery>(
        r#"{"text": "x", "embedding": {"model": {"name": "m", "dimension": 2},
            "values": [1.0, 1.0]}}"#,
        "invalid embedding: NotNormalized",
    );
}
