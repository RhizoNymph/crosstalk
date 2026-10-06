//! One audit entry as the pages show it: when, who, what in words, the
//! note, every subject it touched (as the spec's `AuditEntry::subjects`
//! lists them) and what came of it. The audit page and an alert's history
//! both read entries through [`entry_view`].

use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditOutcome, AuditSubject, ConfigOutcome,
};
use crosstalk_spec::interfaces::l8_surface::export::{ExportEnd, ExportEvent};
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, Permission, QueryError};
use topcoat::Result;
use topcoat::view::{View, component, view};

use super::describe::{describe, note};
use crate::components::format_time;
use crate::error::{describe as describe_error, export_failure, rejection};
use crate::pages::common::lookup::OperatorNames;

/// Something an outcome named that the request did not: what the action
/// created, or what a promotion superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Created {
    pub verb: &'static str,
    pub subject: AuditSubject,
}

/// What came of the recorded call or change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeView {
    /// It took effect, naming what it created.
    Applied { created: Vec<Created> },
    /// Accepted, but the state already matched.
    Unchanged,
    /// Refused or failed, with the typed error in words.
    Rejected(String),
    /// The caller lacked this permission; nothing was attempted.
    Forbidden(Permission),
    /// The call began but the gateway stopped before recording what came
    /// of it (`AuditOutcome::Interrupted`): the effect may or may not have
    /// applied. Neither a success nor a refusal.
    Interrupted,
}

/// How an interrupted call reads.
pub const INTERRUPTED_LABEL: &str = "interrupted, outcome unknown";
/// Why, and what to do about it.
pub const INTERRUPTED_TITLE: &str = "The action began but the gateway restarted before recording its outcome; check the subject's current state.";

/// The badge of an interrupted call: amber, neither the applied nor the
/// rejected styling, with the reason as its tooltip.
#[component]
pub async fn interrupted_badge() -> Result<impl View> {
    Ok(view! {
        <span class="rounded bg-amber-100 px-1.5 py-0.5 text-xs text-amber-800 dark:bg-amber-950 dark:text-amber-300" title=(INTERRUPTED_TITLE)>(INTERRUPTED_LABEL)</span>
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryView {
    pub at: String,
    /// The operator's name, or "config".
    pub actor: String,
    pub what: String,
    pub note: Option<String>,
    /// Every entity the entry touched, as recorded.
    pub subjects: Vec<AuditSubject>,
    pub outcome: OutcomeView,
}

/// What an accepted action's outcome created: the rule or merge record, or
/// the promoted channel and the channels it superseded.
fn created(outcome: &ActionOutcome) -> Vec<Created> {
    match outcome {
        ActionOutcome::Applied | ActionOutcome::Unchanged => Vec::new(),
        ActionOutcome::RuleCreated(rule) => vec![Created {
            verb: "created",
            subject: AuditSubject::Rule(*rule),
        }],
        ActionOutcome::Merged(merge) => vec![Created {
            verb: "recorded",
            subject: AuditSubject::Merge(*merge),
        }],
        ActionOutcome::ChannelPromoted {
            channel,
            superseded,
        } => std::iter::once(Created {
            verb: "declared",
            subject: AuditSubject::Channel(*channel),
        })
        .chain(superseded.iter().map(|channel| Created {
            verb: "superseded",
            subject: AuditSubject::Channel(channel),
        }))
        .collect(),
    }
}

fn refused(error: &QueryError) -> OutcomeView {
    match error {
        QueryError::Forbidden { missing } => OutcomeView::Forbidden(*missing),
        other => OutcomeView::Rejected(describe_error(other)),
    }
}

fn outcome(body: &AuditBody) -> OutcomeView {
    match body {
        AuditBody::Operator(record) => match record.outcome() {
            AuditOutcome::Succeeded(ActionOutcome::Unchanged) => OutcomeView::Unchanged,
            AuditOutcome::Succeeded(outcome) => OutcomeView::Applied {
                created: created(outcome),
            },
            AuditOutcome::Forbidden { missing } => OutcomeView::Forbidden(*missing),
            AuditOutcome::Rejected(why) => OutcomeView::Rejected(describe_error(&rejection(why))),
            AuditOutcome::Interrupted => OutcomeView::Interrupted,
        },
        AuditBody::Config(record) => match &record.outcome {
            ConfigOutcome::Applied => OutcomeView::Applied {
                created: Vec::new(),
            },
            ConfigOutcome::Rejected(why) => OutcomeView::Rejected(describe_error(&rejection(why))),
        },
        AuditBody::Export(record) => match record.event() {
            ExportEvent::Refused(error) => refused(error),
            ExportEvent::Started(header) => OutcomeView::Applied {
                created: vec![Created {
                    verb: "started",
                    subject: AuditSubject::Export(header.id()),
                }],
            },
            ExportEvent::Ended(trailer) => match trailer.end() {
                ExportEnd::Complete => OutcomeView::Applied {
                    created: Vec::new(),
                },
                ExportEnd::Failed(failure) => OutcomeView::Rejected(export_failure(failure)),
            },
            ExportEvent::Abandoned { .. } => {
                OutcomeView::Rejected("the client went away before the export ended".to_owned())
            }
        },
    }
}

pub fn entry_view(entry: &AuditEntry, operators: &OperatorNames) -> EntryView {
    EntryView {
        at: format_time(entry.at),
        actor: operators.policy_author(entry.by()),
        what: describe(&entry.body),
        note: note(&entry.body).map(str::to_owned),
        subjects: entry.subjects(),
        outcome: outcome(&entry.body),
    }
}

#[cfg(test)]
pub mod tests {
    use crosstalk_spec::ids::{AuditId, ChannelId, MergeId, OperatorId};
    use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
    use crosstalk_spec::interfaces::l8_surface::audit::{
        ConfigChange, ConfigRecord, OperatorRecord, Rejection,
    };
    use crosstalk_spec::interfaces::l8_surface::operators::AccessMode;
    use crosstalk_spec::interfaces::l8_surface::{
        ActionError, ConflictKind, OperatorAction, PolicyKind,
    };
    use crosstalk_spec::support::{Blake3, Timestamp};

    use super::*;

    pub const ADA: OperatorId = OperatorId::from_ulid(2);

    /// An operator entry: `ada` (holding `permissions`) asked for
    /// `action` and got `result`.
    pub fn operator_entry(
        permissions: &[Permission],
        action: OperatorAction,
        result: Result<ActionOutcome, ActionError>,
    ) -> AuditEntry {
        let caller = crate::testing::caller_of(ADA, permissions);
        let record =
            OperatorRecord::new(caller, action, AuditOutcome::of(&result)).expect("record");
        AuditEntry {
            id: AuditId::from_ulid(1),
            at: Timestamp::from_micros(1_790_985_600_000_000),
            body: AuditBody::Operator(record),
        }
    }

    pub fn policy(channel: u128) -> OperatorAction {
        OperatorAction::SetPolicy {
            channel: ChannelId::from_ulid(channel),
            policy: PolicyKind::Sanctioned,
            note: Some("ok".into()),
        }
    }

    pub fn names() -> OperatorNames {
        OperatorNames::new([(ADA, "ada".to_owned())])
    }

    #[test]
    fn rejections_keep_their_typed_error() {
        let superseded = ActionError::Conflict(ConflictKind::ChannelSuperseded {
            channel: ChannelId::from_ulid(1),
            by: ChannelId::from_ulid(2),
        });
        let entry = operator_entry(&[Permission::Govern], policy(1), Err(superseded));
        let view = entry_view(&entry, &names());
        assert_eq!(view.actor, "ada");
        assert_eq!(view.what, "set policy to sanctioned");
        assert_eq!(view.note.as_deref(), Some("ok"));
        assert_eq!(
            view.subjects,
            [AuditSubject::Channel(ChannelId::from_ulid(1))]
        );
        assert_eq!(
            view.outcome,
            OutcomeView::Rejected("the channel is superseded: 00000000000000000000000001 resolves to 00000000000000000000000002; act on that channel instead".to_owned())
        );
        let forbidden = operator_entry(
            &[Permission::View],
            policy(1),
            Err(ActionError::Forbidden {
                missing: Permission::Govern,
            }),
        );
        assert_eq!(
            entry_view(&forbidden, &names()).outcome,
            OutcomeView::Forbidden(Permission::Govern)
        );
        let unchanged = operator_entry(
            &[Permission::Govern],
            policy(1),
            Ok(ActionOutcome::Unchanged),
        );
        assert_eq!(
            entry_view(&unchanged, &names()).outcome,
            OutcomeView::Unchanged
        );
    }

    #[test]
    fn promotions_name_every_channel_they_touched() {
        let (promoted, other) = (ChannelId::from_ulid(3), ChannelId::from_ulid(4));
        let action = OperatorAction::PromoteChannel {
            channel: promoted,
            pattern: crosstalk_spec::derived::flow::resource::ResourcePattern::Host(
                crosstalk_spec::derived::flow::resource::Host("wiki.example.org".into()),
            ),
            policy: PolicyKind::Sanctioned,
            note: None,
        };
        let outcome = ActionOutcome::ChannelPromoted {
            channel: promoted,
            superseded: SupersededChannels::new([other]),
        };
        let view = entry_view(
            &operator_entry(&[Permission::Govern], action, Ok(outcome)),
            &names(),
        );
        assert_eq!(
            view.subjects,
            [
                AuditSubject::Channel(promoted),
                AuditSubject::Channel(other)
            ]
        );
        assert_eq!(
            view.outcome,
            OutcomeView::Applied {
                created: vec![
                    Created {
                        verb: "declared",
                        subject: AuditSubject::Channel(promoted)
                    },
                    Created {
                        verb: "superseded",
                        subject: AuditSubject::Channel(other)
                    },
                ]
            }
        );
        let merge = OperatorAction::Unmerge {
            merge: MergeId::from_ulid(9),
        };
        let view = entry_view(
            &operator_entry(&[Permission::Govern], merge, Ok(ActionOutcome::Applied)),
            &names(),
        );
        assert_eq!(
            view.outcome,
            OutcomeView::Applied {
                created: Vec::new()
            }
        );
    }

    #[test]
    fn config_entries_are_by_config() {
        let entry = AuditEntry {
            id: AuditId::from_ulid(1),
            at: Timestamp::from_micros(1),
            body: AuditBody::Config(ConfigRecord {
                config: crosstalk_spec::ids::ConfigHash::from_digest(Blake3::from_bytes([1; 32])),
                change: ConfigChange::SetAccessMode(AccessMode::Authenticated),
                outcome: ConfigOutcome::Rejected(Rejection::Failed {
                    reason: "disk full".into(),
                }),
            }),
        };
        let view = entry_view(&entry, &names());
        assert_eq!(view.actor, "config");
        assert!(view.subjects.is_empty());
        assert_eq!(
            view.outcome,
            OutcomeView::Rejected("the gateway's store failed: disk full".to_owned())
        );
    }

    #[test]
    fn an_interrupted_call_is_neither_applied_nor_rejected() {
        use crosstalk_spec::ids::AlertId;
        let caller = crate::testing::caller_of(ADA, &[Permission::View, Permission::Triage]);
        let record = OperatorRecord::new(
            caller,
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(7),
            },
            AuditOutcome::Interrupted,
        )
        .expect("record");
        let entry = AuditEntry {
            id: AuditId::from_ulid(1),
            at: Timestamp::from_micros(1_790_985_600_000_000),
            body: AuditBody::Operator(record),
        };
        assert_eq!(
            entry_view(&entry, &names()).outcome,
            OutcomeView::Interrupted
        );
    }
}
