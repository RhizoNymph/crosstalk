//! The alerts the rules raised over the week: every alert state, both
//! suppress reasons, deduplicated occurrence counts and every subject kind,
//! consistent with each channel's policy history.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::{Route, TransmissionState};
use crosstalk_spec::ids::{AlertId, AlertRuleId, ChannelId, OperatorId};
use crosstalk_spec::support::Timestamp;

use crate::backend::fixture::actions::effects;
use crate::backend::fixture::clock::{DAY, HOUR, MINUTE, NOW, START, ago, minus, plus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::store::State;
use crate::backend::fixture::text::Theme;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::alerts::{Alert, AlertState, SuppressReason};
use crate::contract::rules::{BuiltinRule, OperatorRuleStatus, RuleStatus};

use super::channels::{
    ChannelKey, ChannelPlan, MCP_RESET_AT, PASTEBIN_DECIDED_AT, PROMOTE_AT, SHARED_FILE_DECIDED_AT,
};
use super::history::{OPERATOR_ONCALL, OPERATOR_RESEARCHER, operator_action};
use super::rules::{
    self, MCP_SANCTIONED_AT, OFF_RULE_AT, OFF_RULE_DISABLED_AT, Rules, SEMANTIC_RULE_AT,
    STALE_RULE_AT,
};
use super::states::EXPIRY;
use super::topics::V2_AT;
use super::{GenError, TxRecord, World};

const RESEARCHER: OperatorId = OPERATOR_RESEARCHER;
const ONCALL: OperatorId = OPERATOR_ONCALL;

fn raise(
    state: &mut State,
    rule: AlertRuleId,
    subject: AlertSubject,
    raised_at: Timestamp,
    occurrences: u32,
    alert_state: AlertState,
) {
    let id = AlertId::from_ulid(state.mint.ulid(raised_at));
    state.alerts.push(Alert {
        id,
        rule,
        subject,
        raised_at,
        occurrences: occurrences.max(1),
        state: alert_state,
    });
}

fn resolved(by: OperatorId, at: Timestamp, note: &str) -> AlertState {
    AlertState::Resolved {
        by,
        at,
        note: Some(note.to_owned()),
    }
}

fn suppressed(at: Timestamp) -> AlertState {
    AlertState::Suppressed {
        at,
        reason: SuppressReason::ChannelSanctioned,
    }
}

fn acknowledged(by: OperatorId, at: Timestamp) -> AlertState {
    AlertState::Acknowledged { by, at }
}

fn confirmed_on(
    world: &World,
    channel: ChannelId,
    from: Timestamp,
    until: Timestamp,
) -> Vec<&TxRecord> {
    world
        .transmissions
        .iter()
        .filter(|t| t.transmission.route == Route::Channel(channel) && t.is_confirmed())
        .filter(|t| t.transmission.opened_at >= from && t.transmission.opened_at < until)
        .collect()
}

/// Rules, then every alert, then the rule an operator disabled.
pub fn populate(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    let rules = rules::build(world, state)?;
    let mut rng = Rng::fork(world.seed, "alerts");
    channel_alerts(world, state, plan, &rules)?;
    suspected(world, state, &rules, &mut rng);
    content(world, state, plan, &rules, &mut rng)?;
    disable_refunds(world, state, &rules, &mut rng);
    state.alerts.sort_by_key(|a| (a.raised_at, a.id));
    Ok(())
}

/// New channels, traffic on unreviewed and unsanctioned channels
/// (deduplicated per channel while active) and the unused sanctioned
/// channel.
fn channel_alerts(
    world: &World,
    state: &mut State,
    plan: &ChannelPlan,
    rules: &Rules,
) -> Result<(), GenError> {
    use ChannelKey as K;
    let new_channel = rules.builtin(BuiltinRule::NewChannel);
    let deleted = || resolved(RESEARCHER, ago(3 * DAY), "the gist was deleted");
    let unsanctioned = || resolved(RESEARCHER, PASTEBIN_DECIDED_AT, "marked unsanctioned");
    for (key, alert_state) in [
        (K::HijackedWiki, None),
        (K::WikiTalk, None),
        (K::Pastebin, Some(unsanctioned())),
        (K::McpMemory, None),
        (K::SharedFile, Some(suppressed(SHARED_FILE_DECIDED_AT))),
        (K::Gist, Some(deleted())),
        (K::KvScratch, None),
        (K::S3Handoff, None),
        (K::OldTeamNotes, Some(suppressed(PROMOTE_AT))),
    ] {
        let id = plan.id(key)?;
        let at = state
            .channels
            .get(&id)
            .map(|r| r.created)
            .ok_or_else(|| GenError::Missing(format!("channel record {key:?}")))?;
        let alert_state = match (key, alert_state) {
            (K::McpMemory, _) => acknowledged(ONCALL, plus(at, 3 * HOUR)),
            (_, Some(s)) => s,
            (_, None) => AlertState::Open,
        };
        raise(
            state,
            new_channel,
            AlertSubject::Channel(id),
            at,
            1,
            alert_state,
        );
    }

    let unreviewed = rules.builtin(BuiltinRule::UnreviewedTraffic);
    let windows = [
        (K::HijackedWiki, unreviewed, START, NOW, AlertState::Open),
        (K::WikiTalk, unreviewed, START, NOW, AlertState::Open),
        (
            K::McpMemory,
            unreviewed,
            START,
            MCP_SANCTIONED_AT,
            suppressed(MCP_SANCTIONED_AT),
        ),
        (
            K::McpMemory,
            unreviewed,
            MCP_RESET_AT,
            NOW,
            acknowledged(ONCALL, plus(MCP_RESET_AT, 4 * HOUR)),
        ),
        (K::Gist, unreviewed, START, NOW, deleted()),
        (
            K::OldTeamNotes,
            unreviewed,
            START,
            NOW,
            suppressed(PROMOTE_AT),
        ),
        (
            K::Pastebin,
            unreviewed,
            START,
            PASTEBIN_DECIDED_AT,
            unsanctioned(),
        ),
        (
            K::SharedFile,
            unreviewed,
            START,
            SHARED_FILE_DECIDED_AT,
            suppressed(SHARED_FILE_DECIDED_AT),
        ),
        (
            K::Pastebin,
            rules.builtin(BuiltinRule::UnsanctionedTraffic),
            PASTEBIN_DECIDED_AT,
            NOW,
            AlertState::Open,
        ),
    ];
    for (key, rule, from, until, alert_state) in windows {
        let id = plan.id(key)?;
        let txs = confirmed_on(world, id, from, until);
        let Some(first) = txs.first() else {
            continue;
        };
        let occurrences = u32::try_from(txs.len()).unwrap_or(u32::MAX);
        let at = first.transmission.opened_at;
        raise(
            state,
            rule,
            AlertSubject::Channel(id),
            at,
            occurrences,
            alert_state,
        );
    }

    let unused = plan.id(K::ReleaseBucket)?;
    let rule = rules.builtin(BuiltinRule::SanctionedUnused);
    raise(
        state,
        rule,
        AlertSubject::Channel(unused),
        ago(6 * DAY),
        1,
        AlertState::Open,
    );
    Ok(())
}

/// Suspected (and since discarded) transmissions of the last three days.
/// A second co-access deduplicates into the same alert.
fn suspected(world: &World, state: &mut State, rules: &Rules, rng: &mut Rng) {
    let rule = rules.builtin(BuiltinRule::SuspectedTransmission);
    for record in &world.transmissions {
        let t = &record.transmission;
        let (since, occurrences, alert_state) = match &t.state {
            TransmissionState::Suspected { since, co_access } => {
                let alert_state = if rng.chance(0.3) && plus(*since, HOUR) < NOW {
                    acknowledged(ONCALL, plus(*since, HOUR))
                } else {
                    AlertState::Open
                };
                (*since, co_access.count().get(), alert_state)
            }
            TransmissionState::Discarded { at, co_access } => (
                minus(*at, EXPIRY),
                co_access.count().get(),
                resolved(ONCALL, *at, "expired without content evidence"),
            ),
            _ => continue,
        };
        if since >= ago(3 * DAY) {
            let subject = AlertSubject::Transmission(t.id);
            raise(state, rule, subject, since, occurrences, alert_state);
        }
    }
}

/// Content rules: watched topics on v2, the stale rule's alerts from before
/// the re-fit (still active), and the semantic query, including one alert
/// about an agent that keeps matching it.
fn content(
    world: &World,
    state: &mut State,
    plan: &ChannelPlan,
    rules: &Rules,
    rng: &mut Rng,
) -> Result<(), GenError> {
    for record in &world.transmissions {
        let watched = match &record.transmission.state {
            TransmissionState::Classified { classification, .. }
            | TransmissionState::Aggregated { classification, .. } => classification.watched,
            _ => false,
        };
        if !watched {
            continue;
        }
        let at = record.transmission.opened_at;
        let alert_state = if at > ago(DAY) {
            AlertState::Open
        } else if rng.chance(0.5) {
            resolved(
                ONCALL,
                plus(at, 2 * HOUR),
                "expected: the security team's drill",
            )
        } else {
            acknowledged(ONCALL, plus(at, HOUR))
        };
        let subject = AlertSubject::Transmission(record.transmission.id);
        raise(state, rules.watch, subject, at, 1, alert_state);
    }

    let stale_themes = [Theme::CodeReview, Theme::DataPipeline, Theme::Support];
    let stale: Vec<&TxRecord> = world
        .transmissions
        .iter()
        .filter(|t| t.is_confirmed() && stale_themes.contains(&t.theme))
        .filter(|t| t.transmission.opened_at >= STALE_RULE_AT && t.transmission.opened_at < V2_AT)
        .filter(|t| t.topic(TopicModelVersion(1)).is_some())
        .take(6)
        .collect();
    for record in stale {
        let at = record.transmission.opened_at;
        let subject = AlertSubject::Transmission(record.transmission.id);
        let alert_state = acknowledged(RESEARCHER, plus(at, 30 * MINUTE));
        raise(state, rules.stale, subject, at, 1, alert_state);
    }

    let pastebin = plan.id(ChannelKey::Pastebin)?;
    for record in confirmed_on(world, pastebin, SEMANTIC_RULE_AT, NOW) {
        if record.theme != Theme::Credentials {
            continue;
        }
        let at = record.transmission.opened_at;
        let alert_state = if at > ago(DAY) || rng.chance(0.5) {
            AlertState::Open
        } else {
            acknowledged(ONCALL, plus(at, 3 * HOUR))
        };
        let subject = AlertSubject::Transmission(record.transmission.id);
        raise(state, rules.semantic, subject, at, 1, alert_state);
    }
    let pi1 = world
        .scenario
        .agent("pi1")
        .ok_or_else(|| GenError::Missing("agent pi1".to_owned()))?;
    let at = plus(SEMANTIC_RULE_AT, 6 * HOUR);
    raise(
        state,
        rules.semantic,
        AlertSubject::Agent(pi1),
        at,
        7,
        AlertState::Open,
    );
    Ok(())
}

/// The refund rule raised a few alerts and was then disabled, which
/// suppressed the ones still active.
fn disable_refunds(world: &World, state: &mut State, rules: &Rules, rng: &mut Rng) {
    let refunds: Vec<&TxRecord> = world
        .transmissions
        .iter()
        .filter(|t| t.is_confirmed() && t.theme == Theme::Support)
        .filter(|t| {
            t.transmission.opened_at >= OFF_RULE_AT
                && t.transmission.opened_at < OFF_RULE_DISABLED_AT
        })
        .take(5)
        .collect();
    for record in refunds {
        let at = record.transmission.opened_at;
        let alert_state = if rng.chance(0.5) {
            AlertState::Open
        } else {
            acknowledged(ONCALL, plus(at, HOUR))
        };
        let subject = AlertSubject::Transmission(record.transmission.id);
        raise(state, rules.off, subject, at, 1, alert_state);
    }
    let action = OperatorAction::SetRuleEnabled {
        id: rules.off,
        status: OperatorRuleStatus::Disabled,
    };
    operator_action(
        state,
        OFF_RULE_DISABLED_AT,
        RESEARCHER,
        action,
        ActionOutcome::Applied,
    );
    if let Some(rule) = state.rules.iter_mut().find(|r| r.id == rules.off) {
        rule.status = RuleStatus::Disabled;
    }
    effects::suppress_rule_alerts(state, rules.off, OFF_RULE_DISABLED_AT);
}
