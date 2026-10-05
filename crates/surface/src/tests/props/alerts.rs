//! Properties of alerts: acknowledge and resolve against the lifecycle
//! model, and the alerts list against its filter.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::alert::{
    AlertState, AlertStateKind, AlertSubject, BuiltinRule, SuppressReason,
};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AlertId, ChannelId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, AlertFilter, ConflictKind, OperatorAction, OperatorActions,
    QueryApi,
};
use crosstalk_spec::paging::{AlertList, PageRequest};
use crosstalk_spec::support::Timestamp;
use proptest::collection::vec;
use proptest::sample::select;

use super::{equal, property};
use crate::tests::page;
use crate::tests::world::{Fixture, Who, minute};

/// The alert lifecycle as INV-363 states it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Model {
    Open,
    Acknowledged(OperatorId, Timestamp),
    Resolved(OperatorId, Timestamp, Option<String>),
    Suppressed(Timestamp),
}

impl Model {
    fn active(&self) -> bool {
        matches!(self, Self::Open | Self::Acknowledged(..))
    }

    fn matches(&self, state: &AlertState) -> bool {
        match (self, state) {
            (Self::Open, AlertState::Open) => true,
            (Self::Acknowledged(by, at), AlertState::Acknowledged { by: b, at: a }) => {
                by == b && at == a
            }
            (
                Self::Resolved(by, at, note),
                AlertState::Resolved {
                    by: b,
                    at: a,
                    note: n,
                },
            ) => by == b && at == a && note == n,
            (Self::Suppressed(at), AlertState::Suppressed { at: a, reason }) => {
                at == a && *reason == SuppressReason::RuleDisabled
            }
            _ => false,
        }
    }
}

/// INV-363: random acknowledgements, resolves and rule disables by random
/// callers leave each alert as the lifecycle model does, and each call
/// returns what the model says.
#[test]
fn prop_alert_actions_match_lifecycle_model() {
    let ops = vec(
        (0_usize..2, 0_u8..3, select(vec![Who::Admin, Who::Triager])),
        1..12,
    );
    property(32, ops, |ops| async move {
        let fixture = Fixture::new().await;
        let rules = [BuiltinRule::NewChannel, BuiltinRule::SuspectedTransmission];
        let mut alerts = Vec::new();
        for (n, rule) in rules.iter().enumerate() {
            let subject =
                AlertSubject::Agent(crosstalk_spec::ids::AgentId::from_ulid(0xA10 + n as u128));
            alerts.push(fixture.alert(*rule, subject, minute(0)).await);
        }
        let mut models = [Model::Open, Model::Open];
        let mut disabled = [false, false];
        let admin = fixture.caller(Who::Admin).await;
        for (step, (index, op, who)) in ops.into_iter().enumerate() {
            let at = minute(1 + step as u64);
            fixture.clock.set(at);
            let caller = fixture.caller(who).await;
            let alert = alerts[index];
            let model = models[index].clone();
            match op {
                0 => {
                    let result = fixture
                        .surface
                        .act(&caller, OperatorAction::Acknowledge { alert })
                        .await;
                    let expected = match model {
                        Model::Open => {
                            models[index] = Model::Acknowledged(caller.operator(), at);
                            Ok(ActionOutcome::Applied)
                        }
                        Model::Acknowledged(..) => Ok(ActionOutcome::Unchanged),
                        Model::Resolved(..) | Model::Suppressed(_) => {
                            Err(ActionError::Conflict(ConflictKind::AlertNotActive {
                                alert,
                            }))
                        }
                    };
                    equal(&format!("step {step} acknowledge"), &result, &expected)?;
                }
                1 => {
                    let note = Some(format!("note {step}"));
                    let result = fixture
                        .surface
                        .act(
                            &caller,
                            OperatorAction::Resolve {
                                alert,
                                note: note.clone(),
                            },
                        )
                        .await;
                    let expected = match model {
                        Model::Open => {
                            Err(ActionError::Conflict(ConflictKind::AlertNotAcknowledged {
                                alert,
                            }))
                        }
                        Model::Acknowledged(..) => {
                            models[index] = Model::Resolved(caller.operator(), at, note);
                            Ok(ActionOutcome::Applied)
                        }
                        Model::Resolved(..) | Model::Suppressed(_) => {
                            Err(ActionError::Conflict(ConflictKind::AlertNotActive {
                                alert,
                            }))
                        }
                    };
                    equal(&format!("step {step} resolve"), &result, &expected)?;
                }
                _ => {
                    let disable = OperatorAction::SetRuleEnabled {
                        id: rules[index].id(),
                        enabled: false,
                    };
                    let result = fixture.surface.act(&admin, disable).await;
                    let expected = if disabled[index] {
                        ActionOutcome::Unchanged
                    } else {
                        ActionOutcome::Applied
                    };
                    equal(&format!("step {step} disable"), &result, &Ok(expected))?;
                    disabled[index] = true;
                    if model.active() {
                        models[index] = Model::Suppressed(at);
                    }
                }
            }
            for (alert, model) in alerts.iter().zip(&models) {
                let stored = fixture
                    .surface
                    .alert(&admin, *alert)
                    .await
                    .map_err(|error| format!("alert: {error:?}"))?
                    .ok_or("alert gone")?;
                if !model.matches(&stored.state) {
                    return Err(format!(
                        "step {step}: {alert:?} is {:?}, model {model:?}",
                        stored.state
                    ));
                }
            }
        }
        Ok(())
    });
}

/// Every alert a full traversal of `filter` lists, page by page.
async fn traverse(fixture: &Fixture, filter: &AlertFilter) -> Result<BTreeSet<AlertId>, String> {
    let caller = fixture.caller(Who::Viewer).await;
    let mut request: PageRequest<AlertList> = page(2);
    let mut listed = BTreeSet::new();
    loop {
        let page = fixture
            .surface
            .alerts(&caller, filter, &request)
            .await
            .map_err(|error| format!("alerts: {error:?}"))?;
        for alert in page.items() {
            if !listed.insert(alert.id) {
                return Err(format!("{:?} listed twice", alert.id));
            }
        }
        match page.next() {
            Some(next) => request.after = Some(next.clone()),
            None => return Ok(listed),
        }
    }
}

/// INV-379: a full traversal lists exactly the alerts whose state kind is
/// listed (any when none) and, with a channel, whose subject or whose
/// transmission's route resolves to that channel's canonical channel.
#[test]
fn prop_alerts_match_filter_model() {
    let alerts = vec((0_usize..4, 0_usize..5, 0_u8..3), 1..8);
    let filters = (
        proptest::bits::u8::masked(0b1111),
        proptest::option::of(0_usize..2),
    );
    property(
        32,
        (alerts, filters),
        |(raised, (states, channel))| async move {
            let fixture = Fixture::new().await;
            let mut scene = fixture.scene().await;
            let resource = crosstalk_testkit::build::ResourceBuilder::new(&mut scene.ids)
                .url("https", "other.example", "/x", None)
                .first_seen(minute(0))
                .build();
            let c2 = scene.ids.channel();
            fixture
                .channel(&mut scene.ids, c2, &resource, scene.a2, scene.a3, minute(0))
                .await;
            let admin = fixture.caller(Who::Admin).await;
            let subjects = [
                AlertSubject::Channel(scene.c1),
                AlertSubject::Channel(c2),
                AlertSubject::Agent(scene.a1),
                AlertSubject::Transmission(scene.t1.transmission.id),
            ];
            // The scene's own alert is open on c1.
            let mut model: BTreeMap<AlertId, (AlertSubject, AlertStateKind)> =
                BTreeMap::from([(scene.alert, (subjects[0], AlertStateKind::Open))]);
            for (step, (subject, rule, fate)) in raised.into_iter().enumerate() {
                fixture.clock.set(minute(1 + step as u64));
                let subject = subjects[subject];
                let alert = fixture
                    .alert(BuiltinRule::ALL[rule], subject, minute(1 + step as u64))
                    .await;
                let kind = model
                    .get(&alert)
                    .map_or(AlertStateKind::Open, |(_, kind)| *kind);
                let kind = match (fate, kind) {
                    (1, AlertStateKind::Open) => {
                        let _ = fixture
                            .surface
                            .act(&admin, OperatorAction::Acknowledge { alert })
                            .await;
                        AlertStateKind::Acknowledged
                    }
                    (2, AlertStateKind::Open) => {
                        let _ = fixture
                            .surface
                            .act(&admin, OperatorAction::Acknowledge { alert })
                            .await;
                        let _ = fixture
                            .surface
                            .act(&admin, OperatorAction::Resolve { alert, note: None })
                            .await;
                        AlertStateKind::Resolved
                    }
                    (_, kind) => kind,
                };
                model.insert(alert, (subject, kind));
            }
            let kinds = [
                AlertStateKind::Open,
                AlertStateKind::Acknowledged,
                AlertStateKind::Resolved,
                AlertStateKind::Suppressed,
            ];
            let states: Vec<AlertStateKind> = kinds
                .iter()
                .enumerate()
                .filter(|(bit, _)| states & (1 << bit) != 0)
                .map(|(_, kind)| *kind)
                .collect();
            let channels: [ChannelId; 2] = [scene.c1, c2];
            let filter = AlertFilter {
                states: states.clone(),
                channel: channel.map(|index| channels[index]),
            };
            let expected: BTreeSet<AlertId> = model
                .iter()
                .filter(|(_, (_, kind))| states.is_empty() || states.contains(kind))
                .filter(|(_, (subject, _))| match filter.channel {
                    None => true,
                    Some(channel) => match subject {
                        AlertSubject::Channel(on) => *on == channel,
                        AlertSubject::Transmission(_) => {
                            scene.t1.transmission.route == Route::Channel(channel)
                        }
                        AlertSubject::Agent(_) => false,
                    },
                })
                .map(|(id, _)| *id)
                .collect();
            equal("listed", &traverse(&fixture, &filter).await?, &expected)
        },
    );
}
