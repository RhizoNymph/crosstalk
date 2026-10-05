//! Rules and the alerts they raised over the week, as the alerts consumer
//! triaged them and operators handled them.
//!
//! Each planned alert is one or more triage drafts (deduplicated
//! occurrences), raised when the event behind each happened, then the
//! operators' acknowledgements and resolutions. Suppressions are not
//! planned: they follow from the store's own semantics when a channel is
//! sanctioned (or promoted sanctioned), a rule is disabled, or a
//! transmission is judged a false detection. An action planned on an alert
//! a suppression already ended is skipped when the script runs.
//!
//! - **Channels.** New channels; traffic on unreviewed and unsanctioned
//!   channels, deduplicated per channel while active; the unused sanctioned
//!   channel.
//! - **Suspected transmissions** of the last three days, one occurrence
//!   per co-access.
//! - **Content.** The v2 watched-topic rule's transmissions, the v1 rule's
//!   from before the re-fit (still acknowledged), the semantic query's, an
//!   agent that keeps matching it, and the refund rule's before it was
//!   disabled.

use crosstalk_spec::aggregates::alert::{AlertSubject, BuiltinRule};
use crosstalk_spec::derived::flow::transmission::{Route, TransmissionState};
use crosstalk_spec::ids::{ChannelId, OperatorId};
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, HOUR, MINUTE, minus, plus};
use crate::config::{EXPIRY, OPERATOR_ONCALL, OPERATOR_RESEARCHER, WorldConfig};
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::rules;
use crate::generate::states::TxRecord;
use crate::generate::topics::V1;
use crate::rng::Rng;
use crate::scenario::{ChannelKey, RuleKey};
use crate::script::{AlertKey, Op, RuleRef, Script};
use crate::text::Theme;

const RESEARCHER: OperatorId = OPERATOR_RESEARCHER;
const ONCALL: OperatorId = OPERATOR_ONCALL;

/// What an operator did with a planned alert.
enum Handling {
    Open,
    Acknowledged {
        by: OperatorId,
        at: Timestamp,
    },
    Resolved {
        by: OperatorId,
        at: Timestamp,
        note: &'static str,
    },
}

/// Planned alerts, keyed in planning order.
struct Planner<'a> {
    script: &'a mut Script,
    next: u32,
    /// The alert the on-call operator later tries to acknowledge again.
    refused: Option<AlertKey>,
}

impl Planner<'_> {
    /// One alert: a draft at each of `occurrences` (oldest first), then
    /// `handling`, never before its first draft.
    fn raise(
        &mut self,
        rule: RuleRef,
        subject: AlertSubject,
        occurrences: &[Timestamp],
        handling: Handling,
    ) -> Option<AlertKey> {
        let first = *occurrences.first()?;
        let alert = AlertKey(self.next);
        self.next += 1;
        for at in occurrences {
            self.script.push(
                *at,
                Op::Triage {
                    alert,
                    rule,
                    subject,
                },
            );
        }
        match handling {
            Handling::Open => {}
            Handling::Acknowledged { by, at } => {
                self.script
                    .push(at.max(first), Op::Acknowledge { alert, by });
            }
            Handling::Resolved { by, at, note } => {
                let at = at.max(first);
                self.script.push(at, Op::Acknowledge { alert, by });
                self.script.push(
                    at,
                    Op::Resolve {
                        alert,
                        by,
                        note: Some(note.to_owned()),
                    },
                );
            }
        }
        Some(alert)
    }
}

pub fn assemble(
    generated: &Generated,
    config: &WorldConfig,
    created: &std::collections::BTreeMap<ChannelId, Timestamp>,
    script: &mut Script,
) -> Result<(), WorldError> {
    rule_steps(generated, config, script)?;
    let mut planner = Planner {
        script,
        next: 0,
        refused: None,
    };
    let mut rng = Rng::fork(generated.seed, "alerts");
    channel_alerts(generated, created, &mut planner)?;
    suspected(generated, &mut planner, &mut rng);
    content(generated, &mut planner, &mut rng)?;
    refunds(generated, &mut planner, &mut rng);
    let refused = planner
        .refused
        .ok_or_else(|| WorldError::missing("a resolved alert to acknowledge again"))?;
    planner.script.push(
        minus(generated.times.now, 20 * HOUR),
        Op::RefusedAcknowledge {
            alert: refused,
            by: ONCALL,
        },
    );
    Ok(())
}

/// The user rules, created by the researcher, and the refund rule's
/// disabling.
fn rule_steps(
    generated: &Generated,
    config: &WorldConfig,
    script: &mut Script,
) -> Result<(), WorldError> {
    for rule in rules::planned(&generated.times, &generated.topics, config)? {
        script.push(
            rule.at,
            Op::CreateRule {
                key: rule.key,
                name: rule.name,
                rule: rule.rule,
                sinks: rule.sinks,
                by: RESEARCHER,
            },
        );
    }
    script.push(
        generated.times.off_rule_disabled_at,
        Op::SetRuleEnabled {
            rule: RuleKey::Refunds,
            enabled: false,
            by: RESEARCHER,
        },
    );
    Ok(())
}

/// The confirmation times of the confirmed transmissions routed to
/// `channel` that opened in `[from, until)`, oldest first.
fn confirmed_on(
    generated: &Generated,
    channel: ChannelId,
    from: Timestamp,
    until: Timestamp,
) -> Vec<&TxRecord> {
    generated
        .traffic
        .transmissions
        .iter()
        .filter(|t| t.transmission.route == Route::Channel(channel) && t.is_confirmed())
        .filter(|t| t.transmission.opened_at >= from && t.transmission.opened_at < until)
        .collect()
}

fn confirmation_times(records: &[&TxRecord]) -> Vec<Timestamp> {
    let mut times: Vec<Timestamp> = records
        .iter()
        .filter_map(|record| record.confirmed().map(|c| c.at()))
        .collect();
    times.sort();
    times
}

fn channel_alerts(
    generated: &Generated,
    created: &std::collections::BTreeMap<ChannelId, Timestamp>,
    planner: &mut Planner<'_>,
) -> Result<(), WorldError> {
    use ChannelKey as K;
    let times = &generated.times;
    let id = |key| generated.plan.id(key);
    let new_channel = RuleRef::Builtin(BuiltinRule::NewChannel);
    let deleted = Handling::Resolved {
        by: RESEARCHER,
        at: minus(times.now, 3 * DAY),
        note: "the gist was deleted",
    };
    let unsanctioned = || Handling::Resolved {
        by: RESEARCHER,
        at: times.pastebin_decided_at,
        note: "marked unsanctioned",
    };
    for key in [
        K::HijackedWiki,
        K::WikiTalk,
        K::Pastebin,
        K::McpMemory,
        K::SharedFile,
        K::Gist,
        K::S3Handoff,
        K::SelfNotes,
        K::OldTeamNotes,
    ] {
        let channel = id(key)?;
        let at = *created
            .get(&channel)
            .ok_or_else(|| WorldError::missing(format!("discovery of {key:?}")))?;
        let handling = match key {
            K::Pastebin => unsanctioned(),
            K::McpMemory => Handling::Acknowledged {
                by: ONCALL,
                at: plus(at, 3 * HOUR),
            },
            K::Gist => Handling::Resolved {
                by: RESEARCHER,
                at: minus(times.now, 3 * DAY),
                note: "the gist was deleted",
            },
            _ => Handling::Open,
        };
        let alert = planner.raise(new_channel, AlertSubject::Channel(channel), &[at], handling);
        if key == K::Gist {
            planner.refused = alert;
        }
    }

    let unreviewed = RuleRef::Builtin(BuiltinRule::UnreviewedTraffic);
    let (start, now) = (times.start, times.now);
    let windows = [
        (K::HijackedWiki, unreviewed, start, now, Handling::Open),
        (K::WikiTalk, unreviewed, start, now, Handling::Open),
        (
            K::McpMemory,
            unreviewed,
            start,
            times.mcp_sanctioned_at,
            Handling::Open,
        ),
        (
            K::McpMemory,
            unreviewed,
            times.mcp_reset_at,
            now,
            Handling::Acknowledged {
                by: ONCALL,
                at: plus(times.mcp_reset_at, 4 * HOUR),
            },
        ),
        (K::Gist, unreviewed, start, now, deleted),
        (K::OldTeamNotes, unreviewed, start, now, Handling::Open),
        (
            K::Pastebin,
            unreviewed,
            start,
            times.pastebin_decided_at,
            unsanctioned(),
        ),
        (
            K::SharedFile,
            unreviewed,
            start,
            times.shared_file_decided_at,
            Handling::Open,
        ),
        (
            K::Pastebin,
            RuleRef::Builtin(BuiltinRule::UnsanctionedTraffic),
            times.pastebin_decided_at,
            now,
            Handling::Open,
        ),
    ];
    for (key, rule, from, until, handling) in windows {
        let channel = id(key)?;
        let records = confirmed_on(generated, channel, from, until);
        // Drafts stop when the policy changes: a confirmation landing just
        // after the decision is not unreviewed traffic any more.
        let occurrences: Vec<Timestamp> = confirmation_times(&records)
            .into_iter()
            .filter(|at| *at < until)
            .collect();
        planner.raise(rule, AlertSubject::Channel(channel), &occurrences, handling);
    }

    planner.raise(
        RuleRef::Builtin(BuiltinRule::SanctionedUnused),
        AlertSubject::Channel(id(K::ReleaseBucket)?),
        &[minus(now, 6 * DAY)],
        Handling::Open,
    );
    Ok(())
}

/// Suspected (and since discarded) transmissions of the last three days.
/// A second co-access deduplicates into the same alert.
fn suspected(generated: &Generated, planner: &mut Planner<'_>, rng: &mut Rng) {
    let now = generated.times.now;
    let rule = RuleRef::Builtin(BuiltinRule::SuspectedTransmission);
    for record in &generated.traffic.transmissions {
        let t = &record.transmission;
        let (since, occurrences, handling) = match &t.state {
            TransmissionState::Suspected { since, co_access } => {
                let handling = if rng.chance(0.3) && plus(*since, HOUR) < now {
                    Handling::Acknowledged {
                        by: ONCALL,
                        at: plus(*since, HOUR),
                    }
                } else {
                    Handling::Open
                };
                (*since, co_access.count().get(), handling)
            }
            TransmissionState::Discarded { at, co_access } => (
                minus(*at, EXPIRY),
                co_access.count().get(),
                Handling::Resolved {
                    by: ONCALL,
                    at: *at,
                    note: "expired without content evidence",
                },
            ),
            _ => continue,
        };
        if since >= minus(now, 3 * DAY) {
            let count = usize::try_from(occurrences).unwrap_or(1).max(1);
            planner.raise(
                rule,
                AlertSubject::Transmission(t.id),
                &vec![since; count],
                handling,
            );
        }
    }
}

/// The content rules' alerts.
fn content(
    generated: &Generated,
    planner: &mut Planner<'_>,
    rng: &mut Rng,
) -> Result<(), WorldError> {
    let times = &generated.times;
    let now = times.now;
    for record in &generated.traffic.transmissions {
        let (Some(classification), Some(confirmed)) = (record.classification(), record.confirmed())
        else {
            continue;
        };
        if !classification.watched {
            continue;
        }
        let at = record.transmission.opened_at;
        let handling = if at > minus(now, DAY) {
            Handling::Open
        } else if rng.chance(0.5) {
            Handling::Resolved {
                by: ONCALL,
                at: plus(at, 2 * HOUR),
                note: "expected: the security team's drill",
            }
        } else {
            Handling::Acknowledged {
                by: ONCALL,
                at: plus(at, HOUR),
            }
        };
        planner.raise(
            RuleRef::User(RuleKey::Watch),
            AlertSubject::Transmission(record.id()),
            &[confirmed.at()],
            handling,
        );
    }

    let stale_themes = [Theme::CodeReview, Theme::DataPipeline, Theme::Support];
    let stale: Vec<&TxRecord> = generated
        .traffic
        .transmissions
        .iter()
        .filter(|t| t.classification().is_some() && stale_themes.contains(&t.theme))
        .filter(|t| t.transmission.opened_at >= times.stale_rule_at)
        .filter(|t| t.confirmed().is_some_and(|c| c.at() < times.v2_at))
        .filter(|t| t.topic(V1).is_some())
        .take(6)
        .collect();
    for record in stale {
        let at = record.transmission.opened_at;
        let confirmed_at = record.confirmed().map_or(at, |c| c.at());
        planner.raise(
            RuleRef::User(RuleKey::Stale),
            AlertSubject::Transmission(record.id()),
            &[confirmed_at],
            Handling::Acknowledged {
                by: RESEARCHER,
                at: plus(at, 30 * MINUTE),
            },
        );
    }

    let pastebin = generated.plan.id(ChannelKey::Pastebin)?;
    for record in confirmed_on(generated, pastebin, times.semantic_rule_at, now) {
        if record.theme != Theme::Credentials {
            continue;
        }
        let at = record.transmission.opened_at;
        let handling = if at > minus(now, DAY) || rng.chance(0.5) {
            Handling::Open
        } else {
            Handling::Acknowledged {
                by: ONCALL,
                at: plus(at, 3 * HOUR),
            }
        };
        let confirmed_at = record.confirmed().map_or(at, |c| c.at());
        planner.raise(
            RuleRef::User(RuleKey::Semantic),
            AlertSubject::Transmission(record.id()),
            &[confirmed_at],
            handling,
        );
    }
    let pi1 = generated.cast.id("pi1")?;
    let at = plus(times.semantic_rule_at, 6 * HOUR);
    planner.raise(
        RuleRef::User(RuleKey::Semantic),
        AlertSubject::Agent(pi1),
        &[at; 7],
        Handling::Open,
    );
    Ok(())
}

/// The refund rule raised a few alerts before it was disabled, which
/// suppressed the ones still active.
fn refunds(generated: &Generated, planner: &mut Planner<'_>, rng: &mut Rng) {
    let times = &generated.times;
    let refunds: Vec<&TxRecord> = generated
        .traffic
        .transmissions
        .iter()
        .filter(|t| t.is_confirmed() && t.theme == Theme::Support)
        .filter(|t| {
            t.transmission.opened_at >= times.off_rule_at
                && t.confirmed()
                    .is_some_and(|c| c.at() < times.off_rule_disabled_at)
        })
        .take(5)
        .collect();
    for record in refunds {
        let at = record.transmission.opened_at;
        let handling = if rng.chance(0.5) {
            Handling::Open
        } else {
            Handling::Acknowledged {
                by: ONCALL,
                at: plus(at, HOUR),
            }
        };
        let confirmed_at = record.confirmed().map_or(at, |c| c.at());
        planner.raise(
            RuleRef::User(RuleKey::Refunds),
            AlertSubject::Transmission(record.id()),
            &[confirmed_at],
            handling,
        );
    }
}
