//! Properties of reads: list filters, the audit log, detection quality, the
//! topic history, and what a View caller can read.

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter, AuditSubject};
use crosstalk_spec::interfaces::l8_surface::lists::{AlertRuleFilter, ChannelFilter};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::interfaces::l8_surface::{OperatorAction, OperatorActions, QueryApi};
use crosstalk_spec::paging::{AuditList, ChannelList, PageRequest};
use crosstalk_spec::support::TimeWindow;
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder};
use proptest::collection::vec;
use proptest::sample::select;

use super::{ensure, equal, property};
use crate::tests::page;
use crate::tests::world::{Fixture, Who, minute, minutes};

const KINDS: [PolicyKind; 3] = [
    PolicyKind::Unreviewed,
    PolicyKind::Sanctioned,
    PolicyKind::Unsanctioned,
];

/// INV-408: every channel and rule on every page satisfies its filter.
#[test]
fn prop_list_pages_satisfy_filter() {
    let world = vec(0_usize..3, 1..7);
    let filters = (
        vec(select(KINDS.to_vec()), 0..3),
        vec(0_usize..5, 0..3),
        1_u16..4,
    );
    property(
        24,
        (world, filters),
        |(policies, (wanted, disabled, size))| async move {
            let fixture = Fixture::new().await;
            let mut scene = fixture.scene().await;
            let admin = fixture.caller(Who::Admin).await;
            for (n, policy) in policies.into_iter().enumerate() {
                let resource = ResourceBuilder::new(&mut scene.ids)
                    .url("https", &format!("host{n}.example"), "/", None)
                    .first_seen(minute(1))
                    .build();
                let channel = scene.ids.channel();
                fixture
                    .channel(
                        &mut scene.ids,
                        channel,
                        &resource,
                        scene.a1,
                        scene.a2,
                        minute(1),
                    )
                    .await;
                fixture.clock.set(minute(2 + n as u64));
                let action = OperatorAction::SetPolicy {
                    channel,
                    policy: KINDS[policy],
                    note: None,
                };
                ensure(fixture.surface.act(&admin, action).await.is_ok(), || {
                    "set policy".to_owned()
                })?;
            }
            for rule in disabled {
                let id = crosstalk_spec::aggregates::alert::BuiltinRule::ALL[rule].id();
                let _ = fixture
                    .surface
                    .act(
                        &admin,
                        OperatorAction::SetRuleEnabled { id, enabled: false },
                    )
                    .await;
            }
            let filter = ChannelFilter {
                policies: wanted,
                ..ChannelFilter::default()
            };
            let mut request: PageRequest<ChannelList> = page(size);
            loop {
                let page = fixture
                    .surface
                    .channels(&admin, &filter, &request)
                    .await
                    .map_err(|error| format!("channels: {error:?}"))?;
                for row in page.value.items() {
                    ensure(filter.matches(row), || {
                        format!("{:?} listed", row.channel().id)
                    })?;
                }
                match page.value.next() {
                    Some(next) => request.after = Some(next.clone()),
                    None => break,
                }
            }
            for statuses in [
                vec![crosstalk_spec::aggregates::alert::RuleStatus::Disabled],
                vec![crosstalk_spec::aggregates::alert::RuleStatus::Enabled],
            ] {
                let rules = AlertRuleFilter {
                    statuses,
                    stale: None,
                };
                let listed = fixture
                    .surface
                    .alert_rules(&admin, &rules, &page(50))
                    .await
                    .map_err(|error| format!("rules: {error:?}"))?;
                for rule in listed.items() {
                    ensure(rules.matches(rule), || format!("{:?} listed", rule.id()))?;
                }
            }
            Ok(())
        },
    );
}

/// INV-408: a traversal of the audit log under a filter is the filter's
/// matches over the whole log, newest first.
#[test]
fn prop_audit_pages_match_filter_model() {
    let calls = vec((select(Who::ALL.to_vec()), 0_u8..3), 1..10);
    let filter = (
        proptest::bits::u8::masked(0b111),
        proptest::option::of(0_usize..3),
        proptest::option::of((0_u64..12, 1_u64..12)),
        1_u16..4,
    );
    property(
        24,
        (calls, filter),
        |(calls, (by, subject, window, size))| async move {
            let fixture = Fixture::new().await;
            let scene = fixture.scene().await;
            for (step, (who, pick)) in calls.into_iter().enumerate() {
                fixture.clock.set(minute(1 + step as u64));
                let caller = fixture.caller(who).await;
                let action = match pick {
                    0 => OperatorAction::Acknowledge { alert: scene.alert },
                    1 => OperatorAction::RenameAgent {
                        agent: scene.a2,
                        label: None,
                    },
                    _ => OperatorAction::SetPolicy {
                        channel: scene.c1,
                        policy: PolicyKind::Sanctioned,
                        note: None,
                    },
                };
                let _ = fixture.surface.act(&caller, action).await;
            }
            let authors = [
                PolicyAuthor::Config,
                PolicyAuthor::Operator(Who::Admin.id()),
                PolicyAuthor::Operator(Who::Viewer.id()),
            ];
            let subjects = [
                AuditSubject::Alert(scene.alert),
                AuditSubject::Agent(scene.a2),
                AuditSubject::Channel(scene.c1),
            ];
            let filter = AuditFilter {
                by: authors
                    .iter()
                    .enumerate()
                    .filter(|(bit, _)| by & (1 << bit) != 0)
                    .map(|(_, author)| *author)
                    .collect(),
                subject: subject.map(|index| subjects[index]),
                window: window.and_then(|(start, length)| {
                    TimeWindow::new(minute(start), minute(start + length)).ok()
                }),
            };
            let everything: Vec<AuditEntry> = fixture.audit_entries().await;
            let expected: Vec<AuditEntry> = everything
                .into_iter()
                .filter(|entry| filter.matches(entry))
                .collect();
            let auditor = fixture.caller(Who::Auditor).await;
            let mut request: PageRequest<AuditList> = page(size);
            let mut listed = Vec::new();
            loop {
                let page = fixture
                    .surface
                    .audit(&auditor, &filter, &request)
                    .await
                    .map_err(|error| format!("audit: {error:?}"))?;
                let (items, next) = page.into_parts();
                listed.extend(items);
                match next {
                    Some(next) => request.after = Some(next),
                    None => break,
                }
            }
            equal("audit traversal", &listed, &expected)
        },
    );
}

const STATES: [TransmissionStateKind; 7] = [
    TransmissionStateKind::Detected,
    TransmissionStateKind::AwaitingContent,
    TransmissionStateKind::Suspected,
    TransmissionStateKind::Confirmed,
    TransmissionStateKind::Classified,
    TransmissionStateKind::Aggregated,
    TransmissionStateKind::Discarded,
];

/// INV-532: detection quality for a window is `DetectionQuality::tally`
/// over every stored transmission with its current verdict.
#[test]
fn prop_detection_quality_matches_tally() {
    let transmissions = vec((0_usize..7, 0_u64..10, 0_u8..3), 1..10);
    let window = (0_u64..8, 1_u64..10);
    property(
        32,
        (transmissions, window),
        |(transmissions, (start, length))| async move {
            let fixture = Fixture::new().await;
            let mut scene = fixture.scene().await;
            let triager = fixture.caller(Who::Triager).await;
            let mut stored = vec![scene.t1.transmission.clone()];
            for (state, opened, verdict) in transmissions {
                let transmission = TransmissionBuilder::new(&mut scene.ids)
                    .between(scene.a1, scene.a2)
                    .opened_at(minute(opened))
                    .state(STATES[state])
                    .build()
                    .map_err(|error| format!("{error:?}"))?;
                let mut store = fixture.world.transmissions.clone();
                store
                    .save(transmission.clone())
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                let verdict = match verdict {
                    0 => None,
                    1 => Some(Verdict::Genuine),
                    _ => Some(Verdict::FalseDetection),
                };
                if verdict.is_some() {
                    let _ = fixture
                        .surface
                        .act(
                            &triager,
                            OperatorAction::SetVerdict {
                                transmission: transmission.id,
                                verdict,
                                note: None,
                            },
                        )
                        .await;
                }
                stored.push(transmission);
            }
            let window = minutes(start, start + length);
            let mut pairs = Vec::new();
            for transmission in &stored {
                let verdict = fixture
                    .surface
                    .verdicts(&triager, transmission.id)
                    .await
                    .map_err(|error| format!("{error:?}"))?
                    .and_then(|log| log.current());
                pairs.push((transmission, verdict));
            }
            let expected = DetectionQuality::tally(
                window,
                pairs.iter().map(|(t, v)| (*t, *v)),
                fixture.surface.aliases(),
            );
            let got = fixture
                .surface
                .detection_quality(&triager, window)
                .await
                .map_err(|error| format!("{error:?}"))?;
            equal("quality", &got, &expected)
        },
    );
}

/// INV-434: the topic history, lineages and sizes are the catalog's, the
/// active version standing in for a missing one.
#[test]
fn prop_topic_history_matches_catalog() {
    let fits = vec(proptest::bool::ANY, 0..5);
    property(24, fits, |fits| async move {
        let fixture = Fixture::new().await;
        let viewer = fixture.caller(Who::Viewer).await;
        let mut next_topic = 1_u64;
        let mut last = 0_u32;
        for (n, activate) in fits.into_iter().enumerate() {
            let topics = [next_topic, next_topic + 1];
            next_topic += 2;
            let version = fixture
                .fit(minute(10 * (n as u64 + 1)), &topics, activate)
                .await;
            last = version.0;
        }
        let history = fixture
            .world
            .catalog
            .versions()
            .await
            .map_err(|error| format!("{error:?}"))?;
        let surfaced = fixture
            .surface
            .topic_versions(&viewer)
            .await
            .map_err(|error| format!("{error:?}"))?;
        equal("history", &surfaced, &history)?;
        for version in 0..=last + 1 {
            let version = TopicModelVersion(version);
            let catalog = fixture
                .world
                .catalog
                .lineage(version)
                .await
                .map_err(crosstalk_spec::interfaces::l8_surface::QueryError::from);
            let surfaced = fixture.surface.topic_lineage(&viewer, version).await;
            equal(&format!("lineage of {version:?}"), &surfaced, &catalog)?;
        }
        let active = history.active().version();
        let catalog = fixture
            .world
            .catalog
            .sizes(active, None)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let surfaced = fixture
            .surface
            .topic_sizes(&viewer, None, None)
            .await
            .map_err(|error| format!("{error:?}"))?;
        equal("sizes", &surfaced.value, &catalog)
    });
}

/// INV-380: no response to a View caller, errors included, holds message
/// text: the bodies in the blob store never show up in what it reads.
#[test]
fn prop_view_responses_contain_no_blob_bytes() {
    let marker = "[a-z]{6}";
    property(16, marker, |marker| async move {
        let marker = format!("MARKER-{marker}-MARKER");
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        use crosstalk_spec::interfaces::l2_transport::BlobStore;
        let body = crosstalk_testkit::build::message::user_text(&marker);
        let encoded = crosstalk_spec::observed::message::encoding::encode(&body);
        let hash = fixture
            .world
            .blobs
            .put(&encoded)
            .await
            .map_err(|error| format!("{error:?}"))?;
        // The transmission's sender wrote the marker: its span points at it.
        let range =
            crosstalk_spec::support::ByteRange::new(0, u32::try_from(marker.len()).unwrap_or(1))
                .map_err(|error| format!("{error:?}"))?;
        for content in scene.t1.content.iter() {
            fixture
                .world
                .evidence
                .span(crosstalk_spec::derived::provenance::span::Span {
                    id: content.origin(),
                    location: crosstalk_spec::derived::provenance::span::SpanLocation {
                        part: crosstalk_spec::observed::message::PartRef {
                            message: hash,
                            index: 0,
                        },
                        range,
                    },
                    agent: scene.a1,
                    exchange: crosstalk_spec::ids::ExchangeId::from_ulid(0xE0),
                    state: crosstalk_spec::derived::provenance::span::SpanState::Originated,
                });
        }
        fixture.world.evidence.access(scene.t1.write.clone());
        fixture.world.evidence.access(scene.t1.read.clone());
        fixture.world.evidence.resource(scene.r1.clone());
        // A caller with Content reads the text: the marker is reachable.
        let reader = fixture.caller(Who::Reader).await;
        let quoted = format!(
            "{:?}",
            fixture
                .surface
                .transmission_evidence(&reader, scene.t1.transmission.id, Default::default())
                .await
        );
        ensure(quoted.contains(&marker), || {
            format!("the evidence does not quote the marker: {quoted}")
        })?;
        let viewer = fixture.caller(Who::Viewer).await;
        let window = minutes(0, 10);
        let selection =
            crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection::new(vec![
                scene.t1.transmission.id,
            ])
            .map_err(|error| format!("{error:?}"))?;
        let mut answers = Vec::new();
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .channels(&viewer, &ChannelFilter::default(), &page(50))
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture.surface.channel(&viewer, scene.c1, None).await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .alerts(&viewer, &Default::default(), &page(50))
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .topology(
                    &viewer,
                    window,
                    crosstalk_spec::aggregates::edge::Weighting::Transmissions,
                    &Default::default()
                )
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .channel_topology(
                    &viewer,
                    window,
                    crosstalk_spec::aggregates::edge::Weighting::Transmissions,
                    &Default::default()
                )
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .overview(&viewer, window, &Default::default())
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .transmissions_by_id(&viewer, &selection, Default::default(), &page(10))
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .verdicts(&viewer, scene.t1.transmission.id)
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .transmission_evidence(&viewer, scene.t1.transmission.id, Default::default())
                .await
        ));
        answers.push(format!(
            "{:?}",
            fixture
                .surface
                .agents(&viewer, &Default::default(), window, &page(10))
                .await
        ));
        for answer in answers {
            ensure(!answer.contains(&marker), || {
                format!("a View response holds message text: {answer}")
            })?;
        }
        Ok(())
    });
}
