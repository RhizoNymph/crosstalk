//! Channels: origins, detection, policy and its history, and the coverage
//! a promotion preview shows.

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::{
    AREA, ULID_C, ULID_G, co_access, operator, page, scratch, transmission_id, wiki, wiki_locator,
    write_access,
};
use crate::derived::flow::channel::detection::{
    DeclaredDetection, DetectionKind, TrafficDetection,
};
use crate::derived::flow::channel::policy::{
    Decision, Policy, PolicyAuthor, PolicyDecision, PolicyHistory, PolicyKind,
};
use crate::derived::flow::channel::promotion::{self, Promotion, PromotionCoverage, Registered};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crate::ids::{AccessId, ChannelId, ResourceId};
use crate::support::Timestamp;
use crate::tests::wire::{id, ts};

fn config_decision() -> Decision {
    Decision {
        by: PolicyAuthor::Config,
        at: ts("2026-10-01T00:00:00.000000Z"),
        note: None,
    }
}

fn operator_decision() -> Decision {
    Decision {
        by: PolicyAuthor::Operator(operator()),
        at: promoted_at(),
        note: Some("the planner briefs the coder through the wiki".into()),
    }
}

fn promoted_at() -> Timestamp {
    ts("2026-10-04T13:00:00.000000Z")
}

fn wiki_pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("wiki.internal.example".into()),
        path_prefix: "/projects/crosstalk".into(),
    }
}

fn wiki_seed() -> Seed {
    Seed {
        resource: page(),
        first_access: write_access().id,
    }
}

fn notes() -> ResourceId {
    id(ResourceId::from_ulid_text, ULID_G)
}

fn scratch_seed() -> Seed {
    Seed {
        resource: notes(),
        first_access: id(AccessId::from_ulid_text, ULID_C),
    }
}

fn promotion() -> Promotion {
    Promotion::new(
        wiki_pattern(),
        PolicyKind::Unsanctioned,
        operator(),
        promoted_at(),
        Some("the planner briefs the coder through the wiki".into()),
    )
}

fn active() -> TrafficDetection {
    TrafficDetection::Active {
        since: ts("2026-10-04T12:00:30.250000Z"),
        last_transmission: transmission_id(),
    }
}

/// One channel of every origin (and every declared history).
fn every_channel() -> Vec<(&'static str, Channel)> {
    fn declared(channel: Channel) -> Channel {
        match &channel.origin {
            ChannelOrigin::Declared {
                history: DeclaredHistory::BeforeTraffic(_) | DeclaredHistory::Promoted { .. },
                ..
            }
            | ChannelOrigin::Discovered { .. }
            | ChannelOrigin::Superseded { .. } => channel,
        }
    }
    [
        (
            "channel_declared_before_traffic",
            Channel {
                id: wiki(),
                origin: ChannelOrigin::Declared {
                    declaration: Declaration {
                        pattern: wiki_pattern(),
                        by: PolicyAuthor::Config,
                        at: config_decision().at,
                    },
                    history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
                },
                resources: Vec::new(),
                policy: Policy::Sanctioned(config_decision()),
            },
        ),
        (
            "channel_promoted",
            Channel {
                id: wiki(),
                origin: ChannelOrigin::Declared {
                    declaration: promotion().declaration().clone(),
                    history: DeclaredHistory::Promoted {
                        from: wiki_seed(),
                        detection: active(),
                    },
                },
                resources: vec![notes()],
                policy: promotion().decision().policy(),
            },
        ),
        (
            "channel_discovered",
            Channel {
                id: wiki(),
                origin: ChannelOrigin::Discovered {
                    seed: wiki_seed(),
                    detection: TrafficDetection::Candidate {
                        first_cross_access: co_access(),
                    },
                },
                resources: Vec::new(),
                policy: Policy::Unreviewed(None),
            },
        ),
        (
            "channel_superseded",
            Channel {
                id: scratch(),
                origin: ChannelOrigin::Superseded {
                    seed: scratch_seed(),
                    detection: TrafficDetection::Observed {
                        first_access: scratch_seed().first_access,
                    },
                    supersession: Supersession {
                        by: wiki(),
                        at: promoted_at(),
                    },
                },
                resources: Vec::new(),
                policy: Policy::Unreviewed(None),
            },
        ),
    ]
    .into_iter()
    .map(|(name, channel)| (name, declared(channel)))
    .collect()
}

#[test]
fn channels_golden_in_every_origin() {
    for (name, channel) in every_channel() {
        assert_golden(AREA, name, &channel);
    }
}

#[test]
fn detections_golden_in_every_state() {
    fn traffic(detection: TrafficDetection) -> TrafficDetection {
        match detection {
            TrafficDetection::Observed { .. }
            | TrafficDetection::Candidate { .. }
            | TrafficDetection::Active { .. }
            | TrafficDetection::Dormant { .. } => detection,
        }
    }
    let traffic_states = [
        TrafficDetection::Observed {
            first_access: write_access().id,
        },
        TrafficDetection::Candidate {
            first_cross_access: co_access(),
        },
        active(),
        TrafficDetection::Dormant {
            since: ts("2026-10-11T12:00:30.250000Z"),
            last_transmission: transmission_id(),
        },
    ]
    .map(traffic);
    assert_golden(AREA, "traffic_detections", &traffic_states.to_vec());

    fn declared(detection: DeclaredDetection) -> DeclaredDetection {
        match detection {
            DeclaredDetection::AwaitingTraffic
            | DeclaredDetection::Unused { .. }
            | DeclaredDetection::InUse(_) => detection,
        }
    }
    let declared_states = [
        DeclaredDetection::AwaitingTraffic,
        DeclaredDetection::Unused {
            since: ts("2026-10-08T00:00:00.000000Z"),
        },
        DeclaredDetection::InUse(active()),
    ]
    .map(declared);
    assert_golden(AREA, "declared_detections", &declared_states.to_vec());

    fn kind(kind: DetectionKind) -> DetectionKind {
        match kind {
            DetectionKind::AwaitingTraffic
            | DetectionKind::Unused
            | DetectionKind::Observed
            | DetectionKind::Candidate
            | DetectionKind::Active
            | DetectionKind::Dormant => kind,
        }
    }
    let kinds = [
        DetectionKind::AwaitingTraffic,
        DetectionKind::Unused,
        DetectionKind::Observed,
        DetectionKind::Candidate,
        DetectionKind::Active,
        DetectionKind::Dormant,
    ]
    .map(kind);
    assert_golden(AREA, "detection_kinds", &kinds.to_vec());
}

#[test]
fn policies_golden_with_every_variant_and_author() {
    fn declared(policy: Policy) -> Policy {
        match &policy {
            Policy::Unreviewed(None | Some(_))
            | Policy::Sanctioned(_)
            | Policy::Unsanctioned(_) => policy,
        }
    }
    let policies = [
        Policy::Unreviewed(None),
        Policy::Unreviewed(Some(operator_decision())),
        Policy::Sanctioned(config_decision()),
        Policy::Unsanctioned(operator_decision()),
    ]
    .map(declared);
    assert_golden(AREA, "policies", &policies.to_vec());

    fn kind(kind: PolicyKind) -> PolicyKind {
        match kind {
            PolicyKind::Unreviewed | PolicyKind::Sanctioned | PolicyKind::Unsanctioned => kind,
        }
    }
    let kinds = [
        PolicyKind::Unreviewed,
        PolicyKind::Sanctioned,
        PolicyKind::Unsanctioned,
    ]
    .map(kind);
    assert_golden(AREA, "policy_kinds", &kinds.to_vec());
}

fn history() -> PolicyHistory {
    PolicyHistory::from_entries(vec![
        PolicyDecision {
            kind: PolicyKind::Sanctioned,
            decision: config_decision(),
        },
        PolicyDecision {
            kind: PolicyKind::Unsanctioned,
            decision: operator_decision(),
        },
    ])
    .expect("in time order, no repeats")
}

#[test]
fn policy_history_golden() {
    assert_golden(AREA, "policy_history", &history());
    assert_golden(AREA, "policy_history_empty", &PolicyHistory::empty());
}

#[test]
fn policy_histories_refuse_what_from_entries_refuses() {
    let entries = serde_json::to_value(history()).expect("a history encodes")["entries"].clone();
    let (first, second) = (entries[0].clone(), entries[1].clone());
    assert_rejected::<PolicyHistory>(
        &json!({"entries": [second, first.clone()]}).to_string(),
        "invalid policy history: OutOfOrder { index: 1 }",
    );
    assert_rejected::<PolicyHistory>(
        &json!({"entries": [first.clone(), first.clone()]}).to_string(),
        "invalid policy history: Duplicate { index: 1 }",
    );
    assert_rejected::<PolicyHistory>(
        &json!({"entries": [first], "current": null}).to_string(),
        "unknown field `current`",
    );
}

/// What a promotion of the wiki channel with the `/projects/crosstalk`
/// prefix would take in: the scratch channel, two wiki pages, and a file
/// outside the pattern.
fn coverage() -> PromotionCoverage {
    let wiki_channel = Channel {
        id: wiki(),
        origin: ChannelOrigin::Discovered {
            seed: wiki_seed(),
            detection: active(),
        },
        resources: vec![id(ResourceId::from_ulid_text, ULID_C)],
        policy: Policy::Unreviewed(None),
    };
    let scratch_channel = Channel {
        id: scratch(),
        origin: ChannelOrigin::Discovered {
            seed: scratch_seed(),
            detection: TrafficDetection::Observed {
                first_access: scratch_seed().first_access,
            },
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    };
    let plan = wiki_locator("/projects/crosstalk/plan");
    let notes_locator = wiki_locator("/projects/crosstalk/notes");
    let registry = [
        Registered {
            channel: &wiki_channel,
            seed: Some(&plan),
        },
        Registered {
            channel: &scratch_channel,
            seed: Some(&notes_locator),
        },
    ];
    let held = |channel: ChannelId| -> Vec<Resource> {
        let resource = |id, locator| Resource {
            id,
            locator,
            first_seen: ts("2026-10-04T11:58:12.000000Z"),
        };
        if channel == wiki() {
            vec![
                resource(page(), wiki_locator("/projects/crosstalk/plan")),
                resource(
                    id(ResourceId::from_ulid_text, ULID_C),
                    Locator::File {
                        host: None,
                        path: "/srv/shared/handoff.md".into(),
                    },
                ),
            ]
        } else {
            vec![resource(notes(), wiki_locator("/projects/crosstalk/notes"))]
        }
    };
    promotion::coverage(wiki(), promotion().declaration(), &registry, held)
        .expect("a discovered channel, a pattern matching its seed, no declared overlap")
}

#[test]
fn promotion_coverage_golden() {
    let coverage = coverage();
    assert_eq!(coverage.superseded(), [scratch()]);
    assert_eq!(coverage.covered().total(), 2);
    assert_eq!(coverage.uncovered().total(), 1);
    assert_golden(AREA, "promotion_coverage", &coverage);
}

#[test]
fn promotion_coverage_refuses_what_coverage_never_builds() {
    let valid = serde_json::to_value(coverage()).expect("a coverage encodes");
    let edit = |change: &dyn Fn(&mut Value)| {
        let mut value = valid.clone();
        change(&mut value);
        value.to_string()
    };
    assert_rejected::<PromotionCoverage>(
        &edit(&|v| v["superseded"] = json!([scratch(), scratch()])),
        "invalid promotion coverage: RepeatedChannel { index: 1 }",
    );
    assert_rejected::<PromotionCoverage>(
        &edit(&|v| {
            let shown = v["covered"]["shown"].as_array_mut().expect("an array");
            shown.reverse();
        }),
        "invalid promotion coverage: CoveredNotNewestFirst",
    );
    assert_rejected::<PromotionCoverage>(
        &edit(&|v| {
            let both = v["covered"]["shown"][0].clone();
            v["uncovered"]["shown"] = json!([v["uncovered"]["shown"][0].clone(), both]);
            v["uncovered"]["total"] = json!(2);
        }),
        "invalid promotion coverage: UncoveredNotNewestFirst",
    );
    assert_rejected::<PromotionCoverage>(
        &edit(&|v| {
            let both = v["covered"]["shown"][0].clone();
            v["uncovered"]["shown"] = json!([both]);
        }),
        "invalid promotion coverage: CoveredAndUncovered",
    );
    assert_rejected::<PromotionCoverage>(
        &edit(&|v| v["promoted"] = json!(wiki())),
        "unknown field `promoted`",
    );
}

#[test]
fn channels_refuse_unknown_fields_and_variants() {
    let (_, discovered) = every_channel()
        .into_iter()
        .find(|(name, _)| *name == "channel_discovered")
        .expect("a discovered channel");
    let valid = serde_json::to_value(discovered).expect("a channel encodes");
    let mut origin = valid.clone();
    origin["origin"]["type"] = json!("archived");
    assert_rejected::<Channel>(&origin.to_string(), "unknown variant `archived`");
    let mut seed = valid.clone();
    seed["origin"]["data"]["seed"]["first_seen"] = json!("2026-10-04T11:58:12.000000Z");
    assert_rejected::<Channel>(&seed.to_string(), "unknown field `first_seen`");
    let mut extra = valid;
    extra["name"] = json!("wiki");
    assert_rejected::<Channel>(&extra.to_string(), "unknown field `name`");
    assert_rejected::<TrafficDetection>(
        r#"{"type": "retired", "data": {"since": "2026-10-04T12:00:00.000000Z"}}"#,
        "unknown variant `retired`",
    );
    assert_rejected::<DeclaredDetection>(r#"{"type": "in_use"}"#, "missing field `data`");
    assert_rejected::<DetectionKind>(r#""retired""#, "unknown variant `retired`");
    assert_rejected::<Policy>(
        r#"{"type": "blocked", "data": null}"#,
        "unknown variant `blocked`",
    );
    assert_rejected::<PolicyAuthor>(
        r#"{"type": "rule", "data": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}"#,
        "unknown variant `rule`",
    );
    assert_rejected::<PolicyKind>(r#""blocked""#, "unknown variant `blocked`");
    assert_rejected::<Decision>(
        r#"{"by": {"type": "config"}, "at": "2026-10-01T00:00:00.000000Z", "note": null, "reason": "x"}"#,
        "unknown field `reason`",
    );
}
