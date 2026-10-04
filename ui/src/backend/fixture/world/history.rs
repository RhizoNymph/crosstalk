//! Operators and the operator history: policy decisions, merges, renames,
//! triage, verdicts (one withdrawn), two rejected actions, configuration
//! changes and a few dead letters. Every past operator action is in the
//! audit log.

use std::num::{NonZeroU32, NonZeroU64};

use crosstalk_spec::aggregates::edge::{EdgeKey, TopicSlot};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{EventId, OperatorId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::fixture::actions::effects;
use crate::backend::fixture::clock::{DAY, HOUR, MINUTE, NOW, START, ago, minus, plus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::store::State;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::research::{Actor, AuditOutcome, AuditSubject, AuditedAction, Operator};
use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::derived::flow::verdict::{TransmissionVerdict, Verdict, VerdictRecorded};
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryError};

use super::channels::{ChannelKey, ChannelPlan, DESIGN_DOCS_AT, PASTEBIN_DECIDED_AT};
use super::drafts::{decisions, team_notes_promotion};
use super::states::confirmed;
use super::{GenError, World};

/// When the deployment's configuration was first applied.
pub const CONFIG_AT: Timestamp = minus(START, 30 * DAY);

/// The researcher: every permission. The same id as the trusted operator in
/// `ui/config.json`, so the UI's own actions sit next to this history.
pub const OPERATOR_RESEARCHER: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0001);
/// The on-call triager: view, content and triage only.
pub const OPERATOR_ONCALL: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0002);

pub fn operators() -> Vec<Operator> {
    vec![
        Operator {
            id: OPERATOR_RESEARCHER,
            name: "researcher".to_owned(),
            permissions: vec![
                Permission::View,
                Permission::Content,
                Permission::Govern,
                Permission::Triage,
                Permission::Operate,
            ],
        },
        Operator {
            id: OPERATOR_ONCALL,
            name: "oncall".to_owned(),
            permissions: vec![Permission::View, Permission::Content, Permission::Triage],
        },
    ]
}

/// Records an applied operator action in the audit log, with what it
/// produced.
pub fn operator_action(
    state: &mut State,
    at: Timestamp,
    by: OperatorId,
    action: OperatorAction,
    outcome: ActionOutcome,
) {
    let subject = effects::subject(&action, Some(&outcome));
    effects::audit(
        state,
        at,
        Actor::Operator(by),
        AuditedAction::Operator(action),
        subject,
        AuditOutcome::Applied(outcome),
    );
}

fn config(state: &mut State, at: Timestamp, summary: &str, subject: Option<AuditSubject>) {
    effects::audit(
        state,
        at,
        Actor::Config,
        AuditedAction::Config {
            summary: summary.to_owned(),
        },
        subject,
        AuditOutcome::Applied(ActionOutcome::Applied),
    );
}

fn note(text: &str) -> Option<String> {
    Some(text.to_owned())
}

pub fn populate(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    configuration(world, state, plan)?;
    policies(state, plan)?;
    agents(world, state)?;
    triage(state);
    verdicts(world, state)?;
    rejected(state, plan)?;
    dead_letters(world, state)?;
    state.audit.sort_by_key(|e| (e.at, e.id));
    Ok(())
}

fn configuration(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    use ChannelKey as K;
    for (key, summary) in [
        (
            K::InternalWiki,
            "declared channel wiki.corp.internal/eng (sanctioned)",
        ),
        (
            K::Monorepo,
            "declared channel git.corp.internal/platform/monorepo (sanctioned)",
        ),
        (
            K::IssueTracker,
            "declared channel issues.corp.internal (sanctioned)",
        ),
        (
            K::ReleaseBucket,
            "declared channel nfs-01:/mnt/shared/releases (sanctioned)",
        ),
    ] {
        config(
            state,
            CONFIG_AT,
            summary,
            Some(AuditSubject::Channel(plan.id(key)?)),
        );
    }
    config(
        state,
        DESIGN_DOCS_AT,
        "declared channel docs.corp.internal/design (sanctioned)",
        Some(AuditSubject::Channel(plan.id(K::DesignDocs)?)),
    );
    for key in ["reg0", "reg1", "reg2"] {
        let id = world.scenario.cast.id(key)?;
        config(
            state,
            CONFIG_AT,
            "registered agent from config",
            Some(AuditSubject::Agent(id)),
        );
    }
    config(
        state,
        CONFIG_AT,
        "enabled the five built-in alert rules",
        None,
    );
    config(
        state,
        CONFIG_AT,
        "added sinks soc-webhook, #agent-alerts, local-log",
        None,
    );
    config(
        state,
        CONFIG_AT,
        "topic versions: retain unpinned versions for 14 days",
        None,
    );
    Ok(())
}

/// The operator decisions in the channels' policy histories, and the
/// promotion, as the audit log recorded the actions that made them.
fn policies(state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    for (key, decision) in decisions() {
        let PolicyAuthor::Operator(by) = decision.decision.by else {
            continue;
        };
        let action = OperatorAction::SetPolicy {
            channel: plan.id(key)?,
            policy: decision.kind,
            note: decision.decision.note.clone(),
        };
        operator_action(
            state,
            decision.decision.at,
            by,
            action,
            ActionOutcome::Applied,
        );
    }
    let (key, promotion) = team_notes_promotion();
    let channel = plan.id(key)?;
    let PolicyAuthor::Operator(by) = promotion.declaration().by else {
        return Err(GenError::Missing("the promotion's operator".to_owned()));
    };
    let promote = OperatorAction::PromoteChannel {
        channel,
        pattern: promotion.pattern().clone(),
        policy: promotion.decision().kind,
        note: promotion.decision().decision.note.clone(),
    };
    operator_action(
        state,
        promotion.at(),
        by,
        promote,
        ActionOutcome::ChannelPromoted(channel),
    );
    Ok(())
}

fn agents(world: &World, state: &mut State) -> Result<(), GenError> {
    let merges = state.identity.merges().to_vec();
    for merge in &merges {
        if let MergeAuthor::Operator(by) = merge.by() {
            let request = MergeRequest::new(merge.source(), merge.target(), merge.by())
                .map_err(|e| GenError::invalid("MergeRequest", e))?;
            let action = OperatorAction::MergeAgents(request);
            operator_action(
                state,
                merge.at(),
                by,
                action,
                ActionOutcome::Merged(merge.id()),
            );
        }
        if let Some(reversal) = merge.reverted() {
            let action = OperatorAction::Unmerge { merge: merge.id() };
            operator_action(
                state,
                reversal.at,
                reversal.by,
                action,
                ActionOutcome::Applied,
            );
        }
    }
    let mut rng = Rng::fork(world.seed, "renames");
    let labelled: Vec<_> = state
        .identity
        .agents()
        .filter(|a| !world.scenario.cast.is_registered(a.id))
        .filter_map(|a| a.label.clone().map(|l| (a.id, l)))
        .collect();
    for (agent, label) in labelled {
        let at = plus(START, rng.below(5 * DAY));
        let action = OperatorAction::RenameAgent {
            agent,
            label: Some(label),
        };
        operator_action(
            state,
            at,
            OPERATOR_RESEARCHER,
            action,
            ActionOutcome::Applied,
        );
    }
    Ok(())
}

/// Audit entries for the acknowledgements and resolutions in the alert
/// history.
fn triage(state: &mut State) {
    let steps: Vec<(OperatorId, Timestamp, OperatorAction)> = state
        .alerts
        .iter()
        .filter_map(|a| match &a.state {
            AlertState::Acknowledged { by, at } => {
                Some((*by, *at, OperatorAction::Acknowledge { alert: a.id }))
            }
            AlertState::Resolved { by, at, note } => Some((
                *by,
                *at,
                OperatorAction::Resolve {
                    alert: a.id,
                    note: note.clone(),
                },
            )),
            _ => None,
        })
        .collect();
    for (by, at, action) in steps {
        operator_action(state, at, by, action, ActionOutcome::Applied);
    }
}

fn verdicts(world: &World, state: &mut State) -> Result<(), GenError> {
    let mut rng = Rng::fork(world.seed, "verdicts");
    let mut log: Vec<(
        OperatorId,
        Timestamp,
        crosstalk_spec::ids::TransmissionId,
        Option<Verdict>,
        &str,
    )> = Vec::new();
    let mut withdrawn = false;
    for record in &world.transmissions {
        let t = &record.transmission;
        let age = NOW.as_micros().saturating_sub(t.opened_at.as_micros());
        if age < 2 * HOUR || !rng.chance(0.012) {
            continue;
        }
        let at = plus(t.opened_at, rng.between(HOUR, 20 * HOUR)).min(ago(30 * MINUTE));
        let by = if rng.chance(0.6) {
            OPERATOR_ONCALL
        } else {
            OPERATOR_RESEARCHER
        };
        let semantic = confirmed(&t.state).is_some_and(|c| {
            c.content()
                .iter()
                .any(|m| matches!(m.kind(), MatchKind::Semantic(_)))
        });
        let (verdict, text) = match &t.state {
            crosstalk_spec::derived::flow::transmission::TransmissionState::Suspected {
                ..
            } => (Verdict::FalseDetection, "unrelated read of the same page"),
            crosstalk_spec::derived::flow::transmission::TransmissionState::Discarded {
                ..
            } => (Verdict::Genuine, "content was paraphrased beyond matching"),
            _ if semantic && rng.chance(0.6) => {
                (Verdict::FalseDetection, "same topic, different text")
            }
            _ if record.is_confirmed() => (Verdict::Genuine, "checked the excerpts"),
            _ => continue,
        };
        log.push((by, at, t.id, Some(verdict), text));
        if !withdrawn && verdict == Verdict::Genuine && record.is_confirmed() {
            withdrawn = true;
            log.push((
                by,
                plus(at, 20 * MINUTE).min(ago(10 * MINUTE)),
                t.id,
                None,
                "judged the wrong row",
            ));
        }
    }
    for (by, at, transmission, verdict, text) in log {
        let record = world
            .tx(transmission)
            .ok_or_else(|| GenError::Missing(format!("judged transmission {transmission:?}")))?;
        let entry = TransmissionVerdict::new(&record.transmission, verdict, by, at, note(text))
            .map_err(|e| GenError::invalid("TransmissionVerdict", e))?;
        let recorded = crate::backend::fixture::actions::record_verdict(state, entry)
            .map_err(|e| GenError::invalid("VerdictLog", e))?;
        operator_action(
            state,
            at,
            by,
            OperatorAction::SetVerdict {
                transmission,
                verdict,
                note: note(text),
            },
            ActionOutcome::Applied,
        );
        if recorded != VerdictRecorded::Unchanged && verdict == Some(Verdict::FalseDetection) {
            effects::reject_transmission_alerts(state, transmission, at);
        }
    }
    Ok(())
}

/// Two actions the log shows as rejected: a policy change by an operator
/// without `Govern`, and an acknowledgement of a resolved alert.
fn rejected(state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    let wiki = plan.id(ChannelKey::HijackedWiki)?;
    let action = OperatorAction::SetPolicy {
        channel: wiki,
        policy: PolicyKind::Sanctioned,
        note: note("looks like a normal wiki"),
    };
    let subject = effects::subject(&action, None);
    effects::audit(
        state,
        ago(DAY),
        Actor::Operator(OPERATOR_ONCALL),
        AuditedAction::Operator(action),
        subject,
        AuditOutcome::Rejected(QueryError::Forbidden {
            missing: Permission::Govern,
        }),
    );
    if let Some(alert) = state
        .alerts
        .iter()
        .find(|a| matches!(a.state, AlertState::Resolved { at, .. } if at < ago(DAY)))
        .map(|a| a.id)
    {
        effects::audit(
            state,
            ago(20 * HOUR),
            Actor::Operator(OPERATOR_ONCALL),
            AuditedAction::Operator(OperatorAction::Acknowledge { alert }),
            Some(AuditSubject::Alert(alert)),
            AuditOutcome::Rejected(QueryError::Conflict(ConflictKind::AlertNotActive { alert })),
        );
    }
    Ok(())
}

fn dead_letters(world: &World, state: &mut State) -> Result<(), GenError> {
    let mut letters = Vec::new();
    let envelope = |state: &mut State, at: Timestamp, event: BusEvent| Envelope {
        id: EventId::from_ulid(state.mint.ulid(at)),
        at,
        event,
    };
    if let Some(record) = world.transmissions.iter().rev().find(|t| t.is_confirmed())
        && let (Some(from), Some(bytes)) = (record.from, NonZeroU64::new(record.matched_bytes))
    {
        let at = record.transmission.opened_at;
        letters.push(DeadLetter {
            group: ConsumerGroup("analyze".to_owned()),
            envelope: envelope(
                state,
                at,
                BusEvent::Detect(DetectEvent::TransmissionConfirmed {
                    transmission: record.transmission.id,
                    from,
                    to: record.transmission.to,
                    route: record.transmission.route.clone(),
                    at,
                    matched_bytes: bytes,
                }),
            ),
            attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            last_error: "embedder: request timed out after 30s".to_owned(),
        });
        let bucket_start = minus(at, at.as_micros() % HOUR);
        let bucket = TimeWindow::new(bucket_start, plus(bucket_start, HOUR))
            .map_err(|e| GenError::invalid("TimeWindow", e))?;
        if let Ok(key) = EdgeKey::new(
            from,
            record.transmission.to,
            record.transmission.route.clone(),
            TopicSlot {
                version: TopicModelVersion(2),
                topic: record.topic(TopicModelVersion(2)),
            },
            bucket,
        ) {
            letters.push(DeadLetter {
                group: ConsumerGroup("topology".to_owned()),
                envelope: envelope(state, at, BusEvent::Insight(InsightEvent::EdgeUpdated(key))),
                attempts: NonZeroU32::new(3).unwrap_or(NonZeroU32::MIN),
                last_error: "edge store: deadlock detected, transaction rolled back".to_owned(),
            });
        }
    }
    let pastebin = world
        .scenario
        .channel(ChannelKey::Pastebin)
        .ok_or_else(|| GenError::Missing("pastebin".to_owned()))?;
    if let Some(policy) = state
        .channels
        .get(&pastebin)
        .map(|r| r.channel().policy.clone())
    {
        let at = PASTEBIN_DECIDED_AT;
        letters.push(DeadLetter {
            group: ConsumerGroup("alerts".to_owned()),
            envelope: envelope(
                state,
                at,
                BusEvent::Insight(InsightEvent::PolicyChanged {
                    channel: pastebin,
                    policy,
                }),
            ),
            attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            last_error: "sink soc-webhook rejected the delivery: HTTP 503".to_owned(),
        });
    }
    let recorded = world.accesses.iter().rev().nth(3).and_then(|access| {
        let channel = *world.resource_channel.get(&access.resource)?;
        Some((access, channel))
    });
    if let Some((access, channel)) = recorded {
        letters.push(DeadLetter {
            group: ConsumerGroup("flow".to_owned()),
            envelope: envelope(
                state,
                access.at,
                BusEvent::Detect(DetectEvent::AccessRecorded {
                    access: access.clone(),
                    channel,
                }),
            ),
            attempts: NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN),
            last_error: "resource extractor: unparseable bash command".to_owned(),
        });
    }
    letters.sort_by_key(|l| (l.envelope.at, l.envelope.id));
    state.dead_letters = letters;
    Ok(())
}
