//! The world's past as the stores recorded it: transmissions and their
//! verdict logs, bodies content retention dropped, the audit log of config
//! changes and operator actions (two of them refused), the operators, the
//! sinks' last deliveries, the dead letters, and search over the corpus.

mod support;

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::{SearchIndex, SearchQuery};
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditOutcome, ConfigChange};
use crosstalk_spec::interfaces::l8_surface::operators::OperatorStore;
use crosstalk_spec::interfaces::l8_surface::sinks::SinkRegistry;
use crosstalk_spec::interfaces::l8_surface::{ActionKind, SinkKind};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::NonBlank;
use crosstalk_world::BodySide;

use support::{read, run, shared};

type Result<T = ()> = std::result::Result<T, String>;

#[test]
fn dropped_bodies_are_gone_on_their_side_only() -> Result {
    let seeded = shared();
    let dropped = seeded.scenario.dropped();
    assert_eq!(dropped.len(), 6);
    assert!(dropped.iter().any(|(_, side)| *side == BodySide::Sender));
    assert!(dropped.iter().any(|(_, side)| *side == BodySide::Reader));
    run(async {
        for (id, side) in dropped {
            let transmission = seeded
                .stores
                .transmissions
                .transmission(*id)
                .await
                .map_err(|e| format!("{e:?}"))?
                .ok_or("a stored transmission")?;
            let confirmed = match &transmission.state {
                TransmissionState::Confirmed(c)
                | TransmissionState::Classified { confirmed: c, .. }
                | TransmissionState::Aggregated { confirmed: c, .. } => c.clone(),
                other => return Err(format!("confirmed, not {other:?}")),
            };
            for content in confirmed.content().iter() {
                let read = seeded
                    .stores
                    .blobs
                    .get(content.read_at().part.message)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                // A reader-side drop has no body; a sender-side drop kept
                // the reader's.
                assert_eq!(read.is_none(), *side == BodySide::Reader, "{id:?}");
            }
        }
        Ok(())
    })
}

/// INV-1076: every content match's origin span is recorded through L4's
/// `SpanIndex`, written by the match's sender, and its location names the
/// sender's body: present unless retention dropped the sender side.
#[test]
fn every_origin_span_is_recorded_by_its_sender() -> Result {
    use crosstalk_spec::batch::IdBatch;
    use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
    use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionQuery;
    use crosstalk_spec::support::{TimeWindow, Timestamp};

    let seeded = shared();
    let dropped: BTreeSet<_> = seeded
        .scenario
        .dropped()
        .iter()
        .filter(|(_, side)| *side == BodySide::Sender)
        .map(|(id, _)| *id)
        .collect();
    run(async {
        let query = TransmissionQuery {
            window: TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX)
                .map_err(|e| format!("{e:?}"))?,
            states: None,
            channel: None,
        };
        let mut request = PageRequest {
            size: PageSize::new(PageSize::MAX).map_err(|e| format!("{e:?}"))?,
            after: None,
        };
        let mut stored = Vec::new();
        loop {
            let page = seeded
                .stores
                .transmissions
                .list(&query, &request)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let (items, next) = page.into_parts();
            stored.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let mut checked = 0usize;
        for transmission in stored {
            let Some(confirmed) = transmission.state.confirmed() else {
                continue;
            };
            for content in confirmed.content().iter() {
                let batch = IdBatch::new([content.origin()]).map_err(|e| format!("{e:?}"))?;
                let spans = seeded
                    .stores
                    .spans
                    .spans(&batch)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                let span = spans
                    .get(&content.origin())
                    .ok_or_else(|| format!("span {:?} recorded", content.origin()))?;
                assert_eq!(span.author, confirmed.from(), "{:?}", transmission.id);
                let body = seeded
                    .stores
                    .blobs
                    .get(span.location.part.message)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                assert_eq!(
                    body.is_none(),
                    dropped.contains(&transmission.id),
                    "{:?}",
                    transmission.id
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "content matches were checked");
        Ok(())
    })
}

#[test]
fn verdict_logs_hold_both_verdicts_and_a_withdrawal() -> Result {
    let seeded = shared();
    let audit = run(read::audit(seeded))?;
    let judged: BTreeSet<_> = audit
        .iter()
        .filter_map(|entry| match &entry.body {
            AuditBody::Operator(record) => match record.action() {
                crosstalk_spec::interfaces::l8_surface::OperatorAction::SetVerdict {
                    transmission,
                    ..
                } => Some(*transmission),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(judged.len() >= 30, "{} judged transmissions", judged.len());
    let mut verdicts = BTreeSet::new();
    let mut withdrawn = 0;
    run(async {
        for id in &judged {
            let log = seeded
                .stores
                .transmissions
                .log(*id)
                .await
                .map_err(|e| format!("{e:?}"))?;
            for record in log.records() {
                match record.verdict() {
                    Some(verdict) => {
                        verdicts.insert(format!("{verdict:?}"));
                    }
                    None => withdrawn += 1,
                }
            }
        }
        Ok::<_, String>(())
    })?;
    assert!(verdicts.contains(&format!("{:?}", Verdict::Genuine)));
    assert!(verdicts.contains(&format!("{:?}", Verdict::FalseDetection)));
    assert_eq!(withdrawn, 1);
    let quality = run(seeded.stores.transmissions.quality(
        seeded
            .world
            .anchor()
            .all_time()
            .map_err(|e| format!("{e:?}"))?,
    ))
    .map_err(|e| format!("{e:?}"))?;
    let genuine: u64 = quality.rows().iter().map(|row| row.genuine).sum();
    let false_detection: u64 = quality.rows().iter().map(|row| row.false_detection).sum();
    assert!(
        genuine > 0,
        "the detector's quality counts the genuine verdicts"
    );
    assert!(false_detection > 0, "and the false detections");
    Ok(())
}

#[test]
fn the_audit_log_records_config_operator_actions_and_two_refusals() -> Result {
    let seeded = shared();
    let audit = run(read::audit(seeded))?;
    assert!(audit.len() >= 200, "{} entries", audit.len());
    let mut hashes = BTreeSet::new();
    let mut changes = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    let (mut forbidden, mut rejected) = (0, 0);
    for entry in &audit {
        match &entry.body {
            AuditBody::Config(record) => {
                hashes.insert(record.config);
                let name = format!("{:?}", record.change);
                changes.insert(name.split([' ', '(', '{']).next().unwrap_or("").to_owned());
            }
            AuditBody::Operator(record) => {
                kinds.insert(format!("{:?}", record.action().kind()));
                match record.outcome() {
                    AuditOutcome::Forbidden { .. } => forbidden += 1,
                    AuditOutcome::Rejected(_) => rejected += 1,
                    AuditOutcome::Succeeded(_) => {}
                }
            }
            AuditBody::Export(_) => {}
        }
    }
    assert_eq!(hashes.len(), 2, "two config documents");
    for change in [
        "SetAccessMode",
        "SetOperator",
        "DeclareChannel",
        "RegisterAgent",
        "ProvisionRule",
        "SetSink",
        "SetTopicRetention",
        "SetFrameRetention",
    ] {
        assert!(changes.contains(change), "{change}: {changes:?}");
    }
    for kind in [
        ActionKind::SetPolicy,
        ActionKind::MergeAgents,
        ActionKind::Unmerge,
        ActionKind::RenameAgent,
        ActionKind::PromoteChannel,
        ActionKind::Acknowledge,
        ActionKind::Resolve,
        ActionKind::SetVerdict,
        ActionKind::CreateRule,
        ActionKind::SetRuleEnabled,
        ActionKind::PinTopicVersion,
    ] {
        assert!(kinds.contains(&format!("{kind:?}")), "{kind:?}");
    }
    assert_eq!(forbidden, 1, "the on-call operator's policy change");
    assert_eq!(rejected, 1, "acknowledging a resolved alert");
    let declared = audit
        .iter()
        .filter(|e| {
            matches!(
                &e.body,
                AuditBody::Config(r) if matches!(r.change, ConfigChange::DeclareChannel { .. })
            )
        })
        .count();
    assert_eq!(declared, 5);
    Ok(())
}

#[test]
fn two_operators_and_three_sinks_with_their_last_delivery() -> Result {
    let seeded = shared();
    let operators = run(seeded.stores.operators.operators()).map_err(|e| format!("{e:?}"))?;
    let names: BTreeSet<_> = operators
        .iter()
        .map(|o| o.name.as_str().to_owned())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from(["oncall".to_owned(), "researcher".to_owned()])
    );
    let sinks = run(seeded.stores.sinks.sinks()).map_err(|e| format!("{e:?}"))?;
    assert_eq!(sinks.len(), 3);
    for sink in &sinks {
        match sink.kind {
            SinkKind::Webhook => assert!(matches!(sink.last_delivery, Some(Err(_)))),
            SinkKind::Slack | SinkKind::Log => {
                assert!(matches!(sink.last_delivery, Some(Ok(_))));
            }
        }
    }
    Ok(())
}

#[test]
fn four_dead_letters_one_per_consumer_group() -> Result {
    let seeded = shared();
    let letters = run(read::letters(seeded))?;
    let groups: BTreeSet<_> = letters.iter().map(|l| l.group.0.clone()).collect();
    assert_eq!(
        groups,
        BTreeSet::from(["alerts", "analyze", "flow", "topology"].map(str::to_owned))
    );
    Ok(())
}

#[test]
fn the_corpus_answers_a_text_search() -> Result {
    let seeded = shared();
    let text = NonBlank::new("credentials").map_err(|e| format!("{e:?}"))?;
    let results = run(seeded.stores.search.query(
        &SearchQuery::Text(text),
        None,
        &TopologyFilter::default(),
        &PageRequest {
            size: PageSize::new(20).map_err(|e| format!("{e:?}"))?,
            after: None,
        },
    ))
    .map_err(|e| format!("{e:?}"))?;
    assert!(!results.page.items().is_empty());
    Ok(())
}
