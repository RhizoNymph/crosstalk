use crate::aggregates::alert::AlertRuleKind;
use crate::aggregates::projection::{FitFailure, FrameRetention, ProjectionStatusKind};
use crate::aggregates::retention::RetentionPolicy;
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{
    AccountHash, AlertId, AlertRuleId, AuditId, ConfigHash, EventId, MergeId, ProjectionId,
    SecretVersion, SinkId, TopicId,
};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::actions::SupersededChannels;
use crate::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditFilter, AuditOutcome, AuditSubject, ConfigChange, ConfigOutcome,
    ConfigRecord, InvalidOperatorRecord, OperatorRecord, OutcomeKind, Rejection,
};
use crate::interfaces::l8_surface::export::ExportFormat;
use crate::interfaces::l8_surface::operators::{AccessMode, OperatorName};
use crate::interfaces::l8_surface::sinks::SinkKind;
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, CallerSnapshot, ConflictKind, InputError,
    OperatorAction, Permission, PermissionSet, QueryError,
};
use crate::observed::agent::{AgentLabel, IdentityEvidence, MergeAuthor, MergeRequest};
use crate::support::{Blake3, NonEmpty, TimeWindow};
use crate::tests::fixtures::{agent, at, channel, transmission};
use crate::tests::operators::{caller, operator};
use crate::wire::DecodeErrorKind;

fn set_policy() -> OperatorAction {
    OperatorAction::SetPolicy {
        channel: channel(1),
        policy: PolicyKind::Sanctioned,
        note: Some("expected traffic".into()),
    }
}

fn merge() -> MergeRequest {
    MergeRequest::new(agent(1), agent(2), MergeAuthor::Operator(operator(1)))
        .expect("different agents")
}

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".into()))
}

/// One action of every variant whose arguments need no fixtures beyond ids,
/// with its kind, required permission and subjects. Rule creation and
/// update are covered by `tests::surface`; an update's subject is the rule
/// id, and a creation's is the id its outcome carries.
fn every_action() -> Vec<(OperatorAction, ActionKind, Permission, Vec<AuditSubject>)> {
    let rule = AlertRuleId::from_ulid(4);
    let alert = AlertId::from_ulid(3);
    vec![
        (
            set_policy(),
            ActionKind::SetPolicy,
            Permission::Govern,
            vec![AuditSubject::Channel(channel(1))],
        ),
        (
            OperatorAction::MergeAgents(merge()),
            ActionKind::MergeAgents,
            Permission::Govern,
            vec![AuditSubject::Agent(agent(1)), AuditSubject::Agent(agent(2))],
        ),
        (
            OperatorAction::Unmerge {
                merge: MergeId::from_ulid(5),
            },
            ActionKind::Unmerge,
            Permission::Govern,
            vec![AuditSubject::Merge(MergeId::from_ulid(5))],
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(6),
                label: Some(AgentLabel::new("planner").expect("valid")),
            },
            ActionKind::RenameAgent,
            Permission::Govern,
            vec![AuditSubject::Agent(agent(6))],
        ),
        (
            OperatorAction::PromoteChannel {
                channel: channel(2),
                pattern: wiki(),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
            ActionKind::PromoteChannel,
            Permission::Govern,
            vec![AuditSubject::Channel(channel(2))],
        ),
        (
            OperatorAction::SetRuleEnabled {
                id: rule,
                enabled: false,
            },
            ActionKind::SetRuleEnabled,
            Permission::Govern,
            vec![AuditSubject::Rule(rule)],
        ),
        (
            OperatorAction::Acknowledge { alert },
            ActionKind::Acknowledge,
            Permission::Triage,
            vec![AuditSubject::Alert(alert)],
        ),
        (
            OperatorAction::Resolve { alert, note: None },
            ActionKind::Resolve,
            Permission::Triage,
            vec![AuditSubject::Alert(alert)],
        ),
        (
            OperatorAction::SetVerdict {
                transmission: transmission(8),
                verdict: Some(Verdict::FalseDetection),
                note: None,
            },
            ActionKind::SetVerdict,
            Permission::Triage,
            vec![AuditSubject::Transmission(transmission(8))],
        ),
        (
            OperatorAction::ReplayDeadLetter {
                group: ConsumerGroup("flow".into()),
                id: EventId::from_ulid(1),
            },
            ActionKind::ReplayDeadLetter,
            Permission::Operate,
            Vec::new(),
        ),
        (
            OperatorAction::PinTopicVersion {
                version: TopicModelVersion(3),
            },
            ActionKind::PinTopicVersion,
            Permission::Govern,
            vec![AuditSubject::TopicVersion(TopicModelVersion(3))],
        ),
    ]
}

#[test]
fn every_action_reports_its_kind_permission_and_subjects() {
    for (action, kind, permission, subjects) in every_action() {
        assert_eq!(action.kind(), kind);
        assert_eq!(action.required_permission(), permission);
        assert_eq!(action.subjects(), subjects, "{action:?}");
    }
}

#[test]
fn outcomes_name_the_ids_they_created() {
    let rule = AlertRuleId::from_ulid(1);
    let merge = MergeId::from_ulid(2);
    assert_eq!(ActionOutcome::Applied.subjects(), Vec::new());
    assert_eq!(ActionOutcome::Unchanged.subjects(), Vec::new());
    assert_eq!(
        ActionOutcome::RuleCreated(rule).subjects(),
        vec![AuditSubject::Rule(rule)]
    );
    assert_eq!(
        ActionOutcome::ChannelPromoted {
            channel: channel(1),
            superseded: SupersededChannels::default(),
        }
        .subjects(),
        vec![AuditSubject::Channel(channel(1))]
    );
    assert_eq!(
        ActionOutcome::Merged(merge).subjects(),
        vec![AuditSubject::Merge(merge)]
    );
}

#[test]
fn forbidden_record_requires_missing_permission() {
    let auditor = caller(1, &[Permission::Audit]);
    for (action, _, required, _) in every_action() {
        assert_eq!(
            OperatorRecord::new(
                caller(1, &[required]),
                action.clone(),
                AuditOutcome::Forbidden { missing: required }
            ),
            Err(InvalidOperatorRecord::ForbiddenButPermitted { required })
        );
        let record = OperatorRecord::new(
            auditor.clone(),
            action.clone(),
            AuditOutcome::Forbidden { missing: required },
        )
        .expect("an auditor holds no action permission");
        assert_eq!(record.caller(), &CallerSnapshot::of(&auditor));
        assert_eq!(record.action(), &action);
        let other = if required == Permission::Govern {
            Permission::Triage
        } else {
            Permission::Govern
        };
        assert_eq!(
            OperatorRecord::new(
                auditor.clone(),
                action.clone(),
                AuditOutcome::Forbidden { missing: other }
            ),
            Err(InvalidOperatorRecord::WrongMissingPermission { required })
        );
    }
}

#[test]
fn attempted_record_requires_permission() {
    let attempted = [
        AuditOutcome::Succeeded(ActionOutcome::Applied),
        AuditOutcome::Succeeded(ActionOutcome::Unchanged),
        AuditOutcome::Rejected(Rejection::NotFound),
    ];
    for (action, _, required, _) in every_action() {
        for outcome in attempted.clone() {
            assert_eq!(
                OperatorRecord::new(
                    caller(1, &[Permission::View]),
                    action.clone(),
                    outcome.clone()
                ),
                Err(InvalidOperatorRecord::AttemptedWithoutPermission { required })
            );
            let record =
                OperatorRecord::new(caller(1, &[required]), action.clone(), outcome.clone())
                    .expect("caller holds the permission");
            assert_eq!(record.outcome(), &outcome);
        }
    }
}

#[test]
fn outcome_maps_every_result_and_back() {
    let results = [
        Ok(ActionOutcome::Applied),
        Ok(ActionOutcome::Unchanged),
        Ok(ActionOutcome::RuleCreated(AlertRuleId::from_ulid(1))),
        Ok(ActionOutcome::Merged(MergeId::from_ulid(1))),
        Err(ActionError::Forbidden {
            missing: Permission::Govern,
        }),
        Err(ActionError::NotFound),
        Err(ActionError::Conflict(ConflictKind::AlertNotActive {
            alert: AlertId::from_ulid(1),
        })),
        Err(ActionError::InvalidInput(InputError::PatternMissesSeed)),
        Err(ActionError::Store {
            reason: "connection reset".into(),
        }),
    ];
    let kinds = [
        OutcomeKind::Applied,
        OutcomeKind::Unchanged,
        OutcomeKind::Applied,
        OutcomeKind::Applied,
        OutcomeKind::Forbidden,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
    ];
    for (result, kind) in results.into_iter().zip(kinds) {
        let outcome = AuditOutcome::of(&result);
        assert_eq!(outcome.kind(), kind);
        assert_eq!(outcome.result(), result);
    }
}

#[test]
fn outcome_keeps_rejection_detail() {
    let result = Err(ActionError::Conflict(ConflictKind::AlertNotActive {
        alert: AlertId::from_ulid(7),
    }));
    assert_eq!(
        AuditOutcome::of(&result),
        AuditOutcome::Rejected(Rejection::Conflict(ConflictKind::AlertNotActive {
            alert: AlertId::from_ulid(7)
        }))
    );
}

#[test]
fn every_action_error_is_a_query_error() {
    let missing = Permission::Operate;
    assert_eq!(
        QueryError::from(ActionError::Forbidden { missing }),
        QueryError::Forbidden { missing }
    );
    assert_eq!(
        QueryError::from(ActionError::InvalidInput(InputError::UnalignedWindow)),
        QueryError::InvalidInput(InputError::UnalignedWindow)
    );
}

fn operator_entry(
    n: u128,
    when: u64,
    by: u128,
    action: OperatorAction,
    outcome: AuditOutcome,
) -> AuditEntry {
    AuditEntry {
        id: AuditId::from_ulid(n),
        at: at(when),
        body: AuditBody::Operator(
            OperatorRecord::new(caller(by, &[Permission::Govern]), action, outcome)
                .expect("caller holds Govern"),
        ),
    }
}

fn config_entry(n: u128, when: u64, change: ConfigChange) -> AuditEntry {
    AuditEntry {
        id: AuditId::from_ulid(n),
        at: at(when),
        body: AuditBody::Config(ConfigRecord {
            config: ConfigHash::from_digest(Blake3::from_bytes([1; 32])),
            change,
            outcome: ConfigOutcome::Applied,
        }),
    }
}

fn declare(n: u128) -> ConfigChange {
    ConfigChange::DeclareChannel {
        channel: channel(n),
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    }
}

fn applied() -> AuditOutcome {
    AuditOutcome::Succeeded(ActionOutcome::Applied)
}

#[test]
fn entry_author_follows_its_body() {
    assert_eq!(
        operator_entry(1, 1, 9, set_policy(), applied()).by(),
        PolicyAuthor::Operator(operator(9))
    );
    assert_eq!(config_entry(2, 1, declare(1)).by(), PolicyAuthor::Config);
}

#[test]
fn operator_entry_subjects_include_created_ids() {
    let merged = MergeId::from_ulid(5);
    let entry = operator_entry(
        1,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Succeeded(ActionOutcome::Merged(merged)),
    );
    assert_eq!(
        entry.subjects(),
        vec![
            AuditSubject::Agent(agent(1)),
            AuditSubject::Agent(agent(2)),
            AuditSubject::Merge(merged),
        ]
    );
    let promoted = operator_entry(
        2,
        1,
        1,
        OperatorAction::PromoteChannel {
            channel: channel(3),
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None,
        },
        AuditOutcome::Succeeded(ActionOutcome::ChannelPromoted {
            channel: channel(3),
            superseded: SupersededChannels::default(),
        }),
    );
    assert_eq!(promoted.subjects(), vec![AuditSubject::Channel(channel(3))]);
    let refused = operator_entry(
        3,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Rejected(Rejection::NotFound),
    );
    assert_eq!(
        refused.subjects(),
        vec![AuditSubject::Agent(agent(1)), AuditSubject::Agent(agent(2))]
    );
}

#[test]
fn every_config_change_names_its_subjects() {
    let rule = AlertRuleId::from_ulid(2);
    let evidence = NonEmpty::new(IdentityEvidence::Account(AccountHash::from_keyed_digest(
        SecretVersion(1),
        Blake3::from_bytes([2; 32]),
    )));
    let cases = [
        (declare(1), vec![AuditSubject::Channel(channel(1))]),
        (
            ConfigChange::SetPolicy {
                channel: channel(2),
                policy: PolicyKind::Unsanctioned,
                note: None,
            },
            vec![AuditSubject::Channel(channel(2))],
        ),
        (
            ConfigChange::RegisterAgent {
                agent: agent(3),
                evidence,
            },
            vec![AuditSubject::Agent(agent(3))],
        ),
        (
            ConfigChange::ProvisionRule {
                rule,
                kind: AlertRuleKind::NewChannel,
            },
            vec![AuditSubject::Rule(rule)],
        ),
        (ConfigChange::SetAccessMode(AccessMode::Trusted), Vec::new()),
        (
            ConfigChange::SetOperator {
                operator: operator(4),
                name: OperatorName::new("Ada").expect("valid"),
                permissions: PermissionSet::ALL,
            },
            vec![AuditSubject::Operator(operator(4))],
        ),
        (
            ConfigChange::RemoveOperator {
                operator: operator(5),
            },
            vec![AuditSubject::Operator(operator(5))],
        ),
        (
            ConfigChange::SetSink {
                sink: SinkId::from_ulid(6),
                kind: SinkKind::Slack,
                name: "security".into(),
            },
            vec![AuditSubject::Sink(SinkId::from_ulid(6))],
        ),
        (
            ConfigChange::RemoveSink {
                sink: SinkId::from_ulid(7),
            },
            vec![AuditSubject::Sink(SinkId::from_ulid(7))],
        ),
        (
            ConfigChange::SetTopicRetention(RetentionPolicy::new(3).expect("at least 2")),
            Vec::new(),
        ),
        (
            ConfigChange::SetFrameRetention {
                frame_retention_micros: FrameRetention::default(),
            },
            Vec::new(),
        ),
    ];
    for (change, subjects) in cases {
        assert_eq!(change.subjects(), subjects, "{change:?}");
        assert_eq!(config_entry(1, 1, change).subjects(), subjects);
    }
}

#[test]
fn empty_audit_filter_matches_everything() {
    let filter = AuditFilter::default();
    assert!(filter.matches(&operator_entry(
        1,
        1,
        1,
        set_policy(),
        AuditOutcome::Rejected(Rejection::NotFound)
    )));
    assert!(filter.matches(&config_entry(2, 2, declare(1))));
}

#[test]
fn audit_filter_selects_by_author() {
    let by_operator = operator_entry(1, 10, 1, set_policy(), applied());
    let by_config = config_entry(2, 10, declare(1));
    let config_only = AuditFilter {
        by: vec![PolicyAuthor::Config],
        ..AuditFilter::default()
    };
    assert!(config_only.matches(&by_config));
    assert!(!config_only.matches(&by_operator));
    let operator_one = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(1))],
        ..AuditFilter::default()
    };
    assert!(operator_one.matches(&by_operator));
    assert!(!operator_one.matches(&by_config));
    let operator_two = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(2))],
        ..AuditFilter::default()
    };
    assert!(!operator_two.matches(&by_operator));
}

#[test]
fn audit_filter_selects_by_subject() {
    let merged = MergeId::from_ulid(5);
    let entry = operator_entry(
        1,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Succeeded(ActionOutcome::Merged(merged)),
    );
    for subject in [
        AuditSubject::Agent(agent(1)),
        AuditSubject::Agent(agent(2)),
        AuditSubject::Merge(merged),
    ] {
        let filter = AuditFilter {
            subject: Some(subject),
            ..AuditFilter::default()
        };
        assert!(filter.matches(&entry), "{subject:?}");
    }
    let other = AuditFilter {
        subject: Some(AuditSubject::Agent(agent(3))),
        ..AuditFilter::default()
    };
    assert!(!other.matches(&entry));
    let channel_filter = AuditFilter {
        subject: Some(AuditSubject::Channel(channel(1))),
        ..AuditFilter::default()
    };
    assert!(channel_filter.matches(&config_entry(2, 1, declare(1))));
    assert!(!channel_filter.matches(&config_entry(3, 1, declare(2))));
}

#[test]
fn audit_filter_combines_fields_with_and() {
    let filter = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(1))],
        subject: Some(AuditSubject::Channel(channel(1))),
        window: Some(TimeWindow::new(at(5), at(15)).expect("non-empty")),
    };
    assert!(filter.matches(&operator_entry(1, 10, 1, set_policy(), applied())));
    assert!(!filter.matches(&operator_entry(2, 20, 1, set_policy(), applied())));
    assert!(!filter.matches(&operator_entry(3, 10, 2, set_policy(), applied())));
    let elsewhere = OperatorAction::SetPolicy {
        channel: channel(2),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(!filter.matches(&operator_entry(4, 10, 1, elsewhere, applied())));
}

/// One value of every `ConflictKind`. The exhaustive match in `declared` is
/// the reminder to sample a new variant here.
fn every_conflict() -> Vec<ConflictKind> {
    fn declared(kind: ConflictKind) -> ConflictKind {
        match kind {
            ConflictKind::AlertNotActive { .. }
            | ConflictKind::AlertNotAcknowledged { .. }
            | ConflictKind::AgentMerged { .. }
            | ConflictKind::MergeAlreadyReverted { .. }
            | ConflictKind::MergeIntoSelf { .. }
            | ConflictKind::ChannelSuperseded { .. }
            | ConflictKind::ChannelNotDiscovered { .. }
            | ConflictKind::PatternOverlaps { .. }
            | ConflictKind::RuleNotEditable { .. }
            | ConflictKind::RuleStale { .. }
            | ConflictKind::TopicVersionNotCurrent { .. }
            | ConflictKind::TransmissionNotJudgeable { .. }
            | ConflictKind::TopicVersionFitting { .. }
            | ConflictKind::TopicVersionDropped { .. }
            | ConflictKind::TopicVersionNotActivated { .. }
            | ConflictKind::TopicsNotInVersion { .. }
            | ConflictKind::EmbeddingModelChanged
            | ConflictKind::ProjectionNotReady { .. }
            | ConflictKind::ProjectionFailed { .. }
            | ConflictKind::ProjectionQueueFull
            | ConflictKind::ExportTooLarge { .. } => kind,
        }
    }
    let version = TopicModelVersion(2);
    let projection = ProjectionId::from_ulid(9);
    [
        ConflictKind::AlertNotActive {
            alert: AlertId::from_ulid(1),
        },
        ConflictKind::AlertNotAcknowledged {
            alert: AlertId::from_ulid(1),
        },
        ConflictKind::AgentMerged {
            agent: agent(1),
            into: agent(2),
        },
        ConflictKind::MergeAlreadyReverted {
            merge: MergeId::from_ulid(3),
        },
        ConflictKind::MergeIntoSelf {
            from: agent(1),
            into: agent(3),
            canonical: agent(2),
        },
        ConflictKind::ChannelSuperseded {
            channel: channel(2),
            by: channel(1),
        },
        ConflictKind::ChannelNotDiscovered {
            channel: channel(1),
        },
        ConflictKind::PatternOverlaps {
            existing: channel(4),
        },
        ConflictKind::RuleNotEditable {
            rule: AlertRuleId::from_ulid(5),
        },
        ConflictKind::RuleStale {
            rule: AlertRuleId::from_ulid(7),
        },
        ConflictKind::TopicVersionNotCurrent {
            requested: version,
            current: TopicModelVersion(3),
        },
        ConflictKind::TransmissionNotJudgeable {
            transmission: transmission(1),
        },
        ConflictKind::TopicVersionFitting { version },
        ConflictKind::TopicVersionDropped { version },
        ConflictKind::TopicVersionNotActivated { version },
        ConflictKind::TopicsNotInVersion {
            version,
            topics: vec![TopicId::from_ulid(6)],
        },
        ConflictKind::EmbeddingModelChanged,
        ConflictKind::ProjectionNotReady {
            projection,
            status: ProjectionStatusKind::Fitting,
        },
        ConflictKind::ProjectionFailed {
            projection,
            failure: FitFailure::NonFiniteLayout,
        },
        ConflictKind::ProjectionQueueFull,
        ConflictKind::ExportTooLarge {
            rows: 11,
            limit: 10,
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

/// One value of every `InputError`, as for `every_conflict`.
fn every_input_error() -> Vec<InputError> {
    fn declared(input: InputError) -> InputError {
        match input {
            InputError::UnalignedWindow
            | InputError::BucketWidthMismatch
            | InputError::PatternMissesSeed
            | InputError::UnknownTopics
            | InputError::UnknownSink { .. }
            | InputError::QueryTooLong
            | InputError::SelfMerge
            | InputError::EmptySelection
            | InputError::ExcerptContextTooLong { .. }
            | InputError::TooManyIds { .. }
            | InputError::UnsupportedFormat { .. }
            | InputError::MalformedRequest { .. } => input,
        }
    }
    [
        InputError::UnalignedWindow,
        InputError::BucketWidthMismatch,
        InputError::PatternMissesSeed,
        InputError::UnknownTopics,
        InputError::UnknownSink {
            sink: SinkId::from_ulid(1),
        },
        InputError::QueryTooLong,
        InputError::SelfMerge,
        InputError::EmptySelection,
        InputError::ExcerptContextTooLong {
            max: 2048,
            got: 4096,
        },
        InputError::TooManyIds {
            max: 1000,
            got: 1001,
        },
        InputError::UnsupportedFormat {
            format: ExportFormat::Parquet,
        },
        InputError::MalformedRequest {
            kind: DecodeErrorKind::Data,
            reason: "unknown field `stats`, expected `states` at line 1 column 8".into(),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

/// Every result `act` can return: each `ActionOutcome`, and each
/// `ActionError` with every conflict and input reason. The exhaustive
/// matches are the reminder to extend it.
fn every_act_result() -> Vec<Result<ActionOutcome, ActionError>> {
    fn outcome(outcome: ActionOutcome) -> ActionOutcome {
        match outcome {
            ActionOutcome::Applied
            | ActionOutcome::Unchanged
            | ActionOutcome::RuleCreated(_)
            | ActionOutcome::ChannelPromoted { .. }
            | ActionOutcome::Merged(_) => outcome,
        }
    }
    fn error(error: ActionError) -> ActionError {
        match error {
            ActionError::Store { .. }
            | ActionError::NotFound
            | ActionError::Forbidden { .. }
            | ActionError::Conflict(_)
            | ActionError::InvalidInput(_) => error,
        }
    }
    let outcomes = [
        ActionOutcome::Applied,
        ActionOutcome::Unchanged,
        ActionOutcome::RuleCreated(AlertRuleId::from_ulid(1)),
        ActionOutcome::ChannelPromoted {
            channel: channel(1),
            superseded: SupersededChannels::new([channel(3), channel(2)]),
        },
        ActionOutcome::Merged(MergeId::from_ulid(1)),
    ]
    .map(|o| Ok(outcome(o)));
    let errors = [
        ActionError::Store {
            reason: "connection reset".into(),
        },
        ActionError::NotFound,
    ]
    .into_iter()
    .chain(Permission::ALL.map(|missing| ActionError::Forbidden { missing }))
    .chain(every_conflict().into_iter().map(ActionError::Conflict))
    .chain(
        every_input_error()
            .into_iter()
            .map(ActionError::InvalidInput),
    )
    .map(|e| Err(error(e)));
    outcomes.into_iter().chain(errors).collect()
}

#[test]
fn audit_outcome_inverts_every_act_result() {
    for result in every_act_result() {
        let outcome = AuditOutcome::of(&result);
        assert_eq!(outcome.result(), result, "{outcome:?}");
        let expected = match &result {
            Ok(ActionOutcome::Unchanged) => OutcomeKind::Unchanged,
            Ok(_) => OutcomeKind::Applied,
            Err(ActionError::Forbidden { .. }) => OutcomeKind::Forbidden,
            Err(_) => OutcomeKind::Rejected,
        };
        assert_eq!(outcome.kind(), expected, "{result:?}");
    }
}

#[test]
fn every_action_error_keeps_its_variant_as_a_query_error() {
    for result in every_act_result() {
        let Err(error) = result else { continue };
        let expected = match error.clone() {
            ActionError::Store { reason } => QueryError::Store { reason },
            ActionError::NotFound => QueryError::NotFound,
            ActionError::Forbidden { missing } => QueryError::Forbidden { missing },
            ActionError::Conflict(kind) => QueryError::Conflict(kind),
            ActionError::InvalidInput(input) => QueryError::InvalidInput(input),
        };
        assert_eq!(QueryError::from(error), expected);
    }
}

fn promote(channel_n: u128) -> OperatorAction {
    OperatorAction::PromoteChannel {
        channel: channel(channel_n),
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    }
}

#[test]
fn superseded_channels_are_sorted_and_listed_once() {
    let superseded = SupersededChannels::new([channel(4), channel(2), channel(4), channel(3)]);
    assert_eq!(superseded.as_slice(), &[channel(2), channel(3), channel(4)]);
    assert_eq!(
        superseded,
        SupersededChannels::new([channel(3), channel(4), channel(2)])
    );
    assert!(SupersededChannels::new([]).is_empty());
}

#[test]
fn a_promotion_entry_names_every_channel_it_superseded() {
    let entry = operator_entry(
        1,
        1,
        1,
        promote(1),
        AuditOutcome::Succeeded(ActionOutcome::ChannelPromoted {
            channel: channel(1),
            superseded: SupersededChannels::new([channel(3), channel(2)]),
        }),
    );
    assert_eq!(
        entry.subjects(),
        vec![
            AuditSubject::Channel(channel(1)),
            AuditSubject::Channel(channel(2)),
            AuditSubject::Channel(channel(3)),
        ]
    );
    for superseded in [channel(2), channel(3)] {
        let history = AuditFilter {
            subject: Some(AuditSubject::Channel(superseded)),
            ..AuditFilter::default()
        };
        assert!(history.matches(&entry), "{superseded:?}");
    }
    let unrelated = AuditFilter {
        subject: Some(AuditSubject::Channel(channel(4))),
        ..AuditFilter::default()
    };
    assert!(!unrelated.matches(&entry));
}

#[test]
fn a_refused_promotion_names_only_the_requested_channel() {
    let entry = operator_entry(
        1,
        1,
        1,
        promote(1),
        AuditOutcome::Rejected(Rejection::Conflict(ConflictKind::ChannelSuperseded {
            channel: channel(1),
            by: channel(5),
        })),
    );
    assert_eq!(entry.subjects(), vec![AuditSubject::Channel(channel(1))]);
}

#[test]
fn a_promotion_outcome_round_trips_through_the_audit_log() {
    let result = Ok(ActionOutcome::ChannelPromoted {
        channel: channel(1),
        superseded: SupersededChannels::new([channel(2)]),
    });
    let outcome = AuditOutcome::of(&result);
    assert_eq!(outcome.kind(), OutcomeKind::Applied);
    assert_eq!(outcome.result(), result);
}
