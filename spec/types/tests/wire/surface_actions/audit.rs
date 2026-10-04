//! The audit log on the wire: `QueryApi::audit` takes an `AuditFilter` (a
//! request) and returns a page of `AuditEntry`s, one golden per body
//! variant (an operator's call, a config change, each export event), plus
//! one golden of every outcome, rejection, config change and subject.
//! A record keeps a `CallerSnapshot` of its caller, never a `Caller`.

use std::num::NonZeroU16;

use serde_json::Value;

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden_allowing};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use super::actions::every_action;
use super::{caller, operator, operator_name};
use crate::aggregates::alert::AlertRuleKind;
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::derived::flow::channel::policy::PolicyAuthor;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::ids::{
    AccountHash, AgentId, AlertId, AlertRuleId, AuditId, ChannelId, ConfigHash, ExportId, MergeId,
    OperatorId, ProjectionId, SecretVersion, TransmissionId,
};
use crate::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditFilter, AuditOutcome, AuditSubject, ConfigChange, ConfigOutcome,
    ConfigRecord, OperatorRecord, Rejection,
};
use crate::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportEvent, ExportFormat, ExportHeader, ExportHeaderParts,
    ExportRecord, ExportRequest, ExportTrailer, GatewayVersion, settled_window,
};
use crate::interfaces::l8_surface::operators::AccessMode;
use crate::interfaces::l8_surface::{
    ActionOutcome, CallerSnapshot, ConflictKind, InputError, OperatorAction, Permission,
    PermissionSet, PolicyKind, QueryError,
};
use crate::observed::agent::{IdentityEvidence, IdentityScope};
use crate::paging::{AuditList, Cursor, Page, PageSize};
use crate::support::{Blake3, NonEmpty, TimeWindow, Watermark};

const AREA: &str = "surface_actions/audit";

fn audit_id(text: &str) -> AuditId {
    id(AuditId::from_ulid_text, text)
}

fn channel(text: &str) -> ChannelId {
    id(ChannelId::from_ulid_text, text)
}

fn set_policy() -> OperatorAction {
    OperatorAction::SetPolicy {
        channel: channel(ULID_A),
        policy: PolicyKind::Unsanctioned,
        note: Some("public paste site".into()),
    }
}

fn operator_entry(at: &str, record: OperatorRecord) -> AuditEntry {
    AuditEntry {
        id: audit_id(ULID_A),
        at: ts(at),
        body: AuditBody::Operator(record),
    }
}

/// An operator's call in each outcome: applied, refused by the store, and
/// forbidden before anything ran. Exhaustive over `AuditOutcome`.
fn every_operator_entry() -> Vec<(&'static str, AuditEntry)> {
    let governor = caller(&[Permission::View, Permission::Govern]);
    let viewer = caller(&[Permission::View]);
    let outcomes = [
        AuditOutcome::Succeeded(ActionOutcome::Applied),
        AuditOutcome::Rejected(Rejection::Conflict(ConflictKind::ChannelSuperseded {
            channel: channel(ULID_A),
            by: channel(ULID_B),
        })),
        AuditOutcome::Forbidden {
            missing: Permission::Govern,
        },
    ];
    outcomes
        .into_iter()
        .map(|outcome| {
            let (name, by) = match &outcome {
                AuditOutcome::Succeeded(_) => ("entry_operator_succeeded", &governor),
                AuditOutcome::Rejected(_) => ("entry_operator_rejected", &governor),
                AuditOutcome::Forbidden { .. } => ("entry_operator_forbidden", &viewer),
            };
            let record = OperatorRecord::new(by, set_policy(), outcome.clone())
                .expect("the outcome matches the caller's permissions");
            (name, operator_entry("2026-10-04T12:34:56.789012Z", record))
        })
        .collect()
}

fn config_hash() -> ConfigHash {
    ConfigHash::from_digest(Blake3::from_bytes([0x5a; 32]))
}

fn wiki() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("wiki.corp.internal".into()),
        path_prefix: "/eng".into(),
    }
}

/// One config change of every variant.
fn every_config_change() -> Vec<ConfigChange> {
    fn declared(change: ConfigChange) -> ConfigChange {
        match change {
            ConfigChange::DeclareChannel { .. }
            | ConfigChange::SetPolicy { .. }
            | ConfigChange::RegisterAgent { .. }
            | ConfigChange::ProvisionRule { .. }
            | ConfigChange::SetAccessMode(_)
            | ConfigChange::SetOperator { .. }
            | ConfigChange::RemoveOperator { .. } => change,
        }
    }
    let account = AccountHash::from_keyed_digest(SecretVersion(1), Blake3::from_bytes([0x11; 32]));
    vec![
        ConfigChange::DeclareChannel {
            channel: channel(ULID_A),
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: Some("the engineering wiki".into()),
        },
        ConfigChange::SetPolicy {
            channel: channel(ULID_B),
            policy: PolicyKind::Unsanctioned,
            note: None,
        },
        ConfigChange::RegisterAgent {
            agent: id(AgentId::from_ulid_text, ULID_C),
            evidence: NonEmpty::new(IdentityEvidence::HarnessAgent {
                scope: IdentityScope::Account(account),
                agent: "planner-7".into(),
            }),
        },
        ConfigChange::ProvisionRule {
            rule: id(AlertRuleId::from_ulid_text, ULID_A),
            kind: AlertRuleKind::NewChannel,
        },
        ConfigChange::SetAccessMode(AccessMode::Authenticated),
        ConfigChange::SetOperator {
            operator: operator(),
            name: operator_name(),
            permissions: PermissionSet::of([Permission::View, Permission::Audit]),
        },
        ConfigChange::RemoveOperator {
            operator: id(OperatorId::from_ulid_text, ULID_B),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

fn verdicts_window() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-03T00:00:00.000000Z"),
        ts("2026-10-04T00:00:00.000000Z"),
    )
    .expect("a day")
}

fn export_request() -> ExportRequest {
    ExportRequest::new(
        ExportDataset::Verdicts(verdicts_window()),
        ExportFormat::Jsonl,
        false,
    )
    .expect("verdicts without content")
}

fn export_id() -> ExportId {
    id(ExportId::from_ulid_text, ULID_B)
}

fn export_header() -> ExportHeader {
    let watermark = Watermark(ts("2026-10-03T23:50:00.000000Z"));
    ExportHeader::new(ExportHeaderParts {
        id: export_id(),
        request: export_request(),
        by: operator(),
        started_at: ts("2026-10-04T12:34:56.789012Z"),
        watermark,
        basis: ExportBasis::Verdicts {
            settled: settled_window(verdicts_window(), watermark),
        },
        embedding_model: EmbeddingModel {
            name: "nomic-embed-text-v1.5".into(),
            dimension: NonZeroU16::new(768).unwrap_or(NonZeroU16::MIN),
        },
        gateway: GatewayVersion::new("0.4.1").expect("not blank"),
        rows: 2,
    })
    .expect("the basis is the request's")
}

/// A trailer as a reader receives it: only the sealer builds one, so the
/// fixture decodes it.
fn export_trailer() -> ExportTrailer {
    let json = format!(
        r#"{{"export": "{}", "rows": 2, "digest": "{}", "end": {{"type": "complete"}}}}"#,
        export_id().ulid_text(),
        "7c".repeat(32)
    );
    serde_json::from_str(&json).expect("a trailer")
}

/// One export entry of every `ExportEvent` variant.
fn every_export_entry() -> Vec<(&'static str, AuditEntry)> {
    let reader = caller(&[Permission::View, Permission::Audit]);
    let auditor = caller(&[Permission::Audit]);
    let events = [
        ExportEvent::Refused(QueryError::Forbidden {
            missing: Permission::View,
        }),
        ExportEvent::Started(Box::new(export_header())),
        ExportEvent::Ended(export_trailer()),
        ExportEvent::Abandoned {
            export: export_id(),
            rows: 1,
        },
    ];
    events
        .into_iter()
        .map(|event| {
            let (name, by) = match &event {
                ExportEvent::Refused(_) => ("entry_export_refused", &auditor),
                ExportEvent::Started(_) => ("entry_export_started", &reader),
                ExportEvent::Ended(_) => ("entry_export_ended", &reader),
                ExportEvent::Abandoned { .. } => ("entry_export_abandoned", &reader),
            };
            let record = ExportRecord::new(by, export_request(), event)
                .expect("the event matches the caller");
            let entry = AuditEntry {
                id: audit_id(ULID_C),
                at: ts("2026-10-04T12:35:02.000000Z"),
                body: AuditBody::Export(record),
            };
            (name, entry)
        })
        .collect()
}

/// One entry per `AuditBody` variant (and, within them, per operator
/// outcome and export event). Exhaustive over the body.
#[test]
fn audit_entries_golden_for_every_body() {
    let config = AuditEntry {
        id: audit_id(ULID_B),
        at: ts("2026-10-04T00:00:00.000000Z"),
        body: AuditBody::Config(ConfigRecord {
            config: config_hash(),
            change: every_config_change().remove(0),
            outcome: ConfigOutcome::Applied,
        }),
    };
    let entries = every_operator_entry()
        .into_iter()
        .chain([("entry_config", config)])
        .chain(every_export_entry());
    for (name, entry) in entries {
        match &entry.body {
            AuditBody::Operator(_) | AuditBody::Config(_) | AuditBody::Export(_) => {}
        }
        assert_golden(AREA, name, &entry);
    }
}

/// Every action as an audit record stores it: the stamped action, a merge
/// with its author.
#[test]
fn operator_records_golden_for_every_action() {
    let governor = caller(&[Permission::View, Permission::Govern]);
    let triager = caller(&[Permission::View, Permission::Triage]);
    let operations = caller(&[Permission::View, Permission::Operate]);
    let records: Vec<OperatorRecord> = every_action()
        .into_iter()
        .map(|action| {
            let by = match action.required_permission() {
                Permission::Govern => &governor,
                Permission::Triage => &triager,
                Permission::Operate => &operations,
                Permission::View | Permission::Content | Permission::Audit => {
                    panic!("no action needs a read permission")
                }
            };
            OperatorRecord::new(by, action, AuditOutcome::Succeeded(ActionOutcome::Applied))
                .expect("the caller holds the action's permission")
        })
        .collect();
    assert_golden(AREA, "operator_records", &records);
}

#[test]
fn audit_outcomes_and_rejections_golden_with_every_variant() {
    fn declared(rejection: Rejection) -> Rejection {
        match rejection {
            Rejection::NotFound
            | Rejection::Conflict(_)
            | Rejection::InvalidInput(_)
            | Rejection::Failed { .. } => rejection,
        }
    }
    let rejections: Vec<Rejection> = [
        Rejection::NotFound,
        Rejection::Conflict(ConflictKind::AlertNotActive {
            alert: id(AlertId::from_ulid_text, ULID_A),
        }),
        Rejection::InvalidInput(InputError::SelfMerge),
        Rejection::Failed {
            reason: "audit store unreachable".into(),
        },
    ]
    .into_iter()
    .map(declared)
    .collect();
    assert_golden(AREA, "rejections", &rejections);
    let outcomes: Vec<AuditOutcome> = every_operator_entry()
        .into_iter()
        .map(|(_, entry)| match entry.body {
            AuditBody::Operator(record) => record.outcome().clone(),
            AuditBody::Config(_) | AuditBody::Export(_) => panic!("operator entries only"),
        })
        .collect();
    assert_golden(AREA, "audit_outcomes", &outcomes);
}

#[test]
fn config_changes_and_outcomes_golden_with_every_variant() {
    assert_golden(AREA, "config_changes", &every_config_change());
    fn declared(outcome: ConfigOutcome) -> ConfigOutcome {
        match outcome {
            ConfigOutcome::Applied | ConfigOutcome::Rejected(_) => outcome,
        }
    }
    let outcomes: Vec<ConfigOutcome> = [
        ConfigOutcome::Applied,
        ConfigOutcome::Rejected(Rejection::Conflict(ConflictKind::PatternOverlaps {
            existing: channel(ULID_B),
        })),
    ]
    .into_iter()
    .map(declared)
    .collect();
    assert_golden(AREA, "config_outcomes", &outcomes);
}

fn every_subject() -> Vec<AuditSubject> {
    fn declared(subject: AuditSubject) -> AuditSubject {
        match subject {
            AuditSubject::Agent(_)
            | AuditSubject::Channel(_)
            | AuditSubject::Alert(_)
            | AuditSubject::Rule(_)
            | AuditSubject::Transmission(_)
            | AuditSubject::Merge(_)
            | AuditSubject::Operator(_)
            | AuditSubject::TopicVersion(_)
            | AuditSubject::Export(_)
            | AuditSubject::Projection(_) => subject,
        }
    }
    [
        AuditSubject::Agent(id(AgentId::from_ulid_text, ULID_A)),
        AuditSubject::Channel(channel(ULID_B)),
        AuditSubject::Alert(id(AlertId::from_ulid_text, ULID_C)),
        AuditSubject::Rule(id(AlertRuleId::from_ulid_text, ULID_A)),
        AuditSubject::Transmission(id(TransmissionId::from_ulid_text, ULID_B)),
        AuditSubject::Merge(id(MergeId::from_ulid_text, ULID_C)),
        AuditSubject::Operator(operator()),
        AuditSubject::TopicVersion(TopicModelVersion(4)),
        AuditSubject::Export(export_id()),
        AuditSubject::Projection(id(ProjectionId::from_ulid_text, ULID_A)),
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn audit_subjects_golden_with_every_variant() {
    assert_golden(AREA, "audit_subjects", &every_subject());
}

/// `QueryApi::audit`: the filter in, a page of entries out.
#[test]
fn audit_list_request_and_response_golden() {
    let filter = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator()), PolicyAuthor::Config],
        subject: Some(AuditSubject::Channel(channel(ULID_A))),
        window: Some(verdicts_window()),
    };
    // `by` is the client's to choose: which authors to list, not who is
    // asking. The caller comes from the session, never from the filter.
    assert_request_golden_allowing(AREA, "audit_filter", &filter, &["by"]);
    assert_request_golden_allowing(
        AREA,
        "audit_filter_everything",
        &AuditFilter::default(),
        &["by"],
    );

    let size = PageSize::new(2).expect("a valid size");
    let entries: Vec<AuditEntry> = every_operator_entry()
        .into_iter()
        .map(|(_, entry)| entry)
        .take(2)
        .collect();
    let next: Cursor<AuditList> =
        Cursor::from_token("YXVkaXQtYWZ0ZXItMDFKOVo".into()).expect("URL-safe base64");
    let page = Page::more(
        size,
        NonEmpty::from_vec(entries).expect("two entries"),
        next,
    )
    .expect("two entries fit a page of two");
    assert_golden(AREA, "audit_page", &page);
}

#[test]
fn caller_snapshot_golden_and_of_a_caller() {
    let auditor = caller(&[Permission::View, Permission::Audit]);
    let snapshot = CallerSnapshot::of(&auditor);
    assert_eq!(snapshot.operator(), auditor.operator());
    assert_eq!(snapshot.permissions(), auditor.permissions());
    assert!(snapshot.has(Permission::Audit) && !snapshot.has(Permission::Govern));
    assert_eq!(CallerSnapshot::from(&auditor), snapshot);
    assert_eq!(
        CallerSnapshot::new(auditor.operator(), auditor.permissions()),
        Ok(snapshot)
    );
    assert_golden(AREA, "caller_snapshot", &snapshot);
}

/// A golden value's JSON, to edit into JSON the decoder must refuse.
fn json_of<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("encodes")
}

#[test]
fn caller_snapshots_refuse_no_permissions_and_unknown_fields() {
    assert_rejected::<CallerSnapshot>(
        &format!(r#"{{"operator": "{ULID_C}", "permissions": []}}"#),
        "invalid caller snapshot: NoPermissions",
    );
    assert_rejected::<CallerSnapshot>(
        &format!(r#"{{"operator": "{ULID_C}", "permissions": ["view"], "session": "abc"}}"#),
        "unknown field `session`",
    );
    assert_rejected::<CallerSnapshot>(
        &format!(r#"{{"operator": "{ULID_C}", "permissions": ["root"]}}"#),
        "unknown variant `root`",
    );
}

/// Each `InvalidOperatorRecord` variant is a decode error: a record whose
/// outcome disagrees with the recorded caller's permissions never decodes.
#[test]
fn operator_records_refuse_outcomes_their_caller_contradicts() {
    let viewer = r#"["view"]"#;
    let governor = r#"["view", "govern"]"#;
    let action = json_of(&set_policy());
    let record = |permissions: &str, outcome: &str| {
        format!(
            r#"{{"caller": {{"operator": "{ULID_C}", "permissions": {permissions}}},
                "action": {action}, "outcome": {outcome}}}"#
        )
    };
    let forbidden =
        |missing: &str| format!(r#"{{"type": "forbidden", "data": {{"missing": "{missing}"}}}}"#);
    let applied = r#"{"type": "succeeded", "data": {"type": "applied"}}"#;
    assert_rejected::<OperatorRecord>(
        &record(governor, &forbidden("govern")),
        "invalid operator record: ForbiddenButPermitted { required: Govern }",
    );
    assert_rejected::<OperatorRecord>(
        &record(viewer, applied),
        "invalid operator record: AttemptedWithoutPermission { required: Govern }",
    );
    assert_rejected::<OperatorRecord>(
        &record(viewer, &forbidden("triage")),
        "invalid operator record: WrongMissingPermission { required: Govern }",
    );
    assert_rejected::<OperatorRecord>(
        &record("[]", &forbidden("govern")),
        "invalid caller snapshot: NoPermissions",
    );
    let mut extra = json_of(
        &OperatorRecord::new(
            caller(&[Permission::Govern]),
            set_policy(),
            AuditOutcome::Succeeded(ActionOutcome::Applied),
        )
        .expect("permitted"),
    );
    if let Value::Object(map) = &mut extra {
        map.insert("note".into(), Value::Null);
    }
    assert_rejected::<OperatorRecord>(&extra.to_string(), "unknown field `note`");
}

#[test]
fn audit_entries_refuse_unknown_fields_and_variants() {
    let mut entry = json_of(&every_operator_entry().remove(0).1);
    if let Value::Object(map) = &mut entry {
        map.insert("by".into(), Value::String(ULID_C.into()));
    }
    assert_rejected::<AuditEntry>(&entry.to_string(), "unknown field `by`");
    assert_rejected::<AuditBody>(
        r#"{"type": "login", "data": {}}"#,
        "unknown variant `login`",
    );
    assert_rejected::<AuditSubject>(
        &format!(r#"{{"type": "session", "data": "{ULID_A}"}}"#),
        "unknown variant `session`",
    );
    assert_rejected::<AuditOutcome>(r#"{"type": "skipped"}"#, "unknown variant `skipped`");
    assert_rejected::<Rejection>(r#"{"type": "timeout"}"#, "unknown variant `timeout`");
    assert_rejected::<ConfigChange>(
        &format!(r#"{{"type": "drop_channel", "data": {{"channel": "{ULID_A}"}}}}"#),
        "unknown variant `drop_channel`",
    );
    assert_rejected::<ConfigRecord>(
        &format!(
            r#"{{"config": "{}", "change": {{"type": "set_access_mode", "data": "trusted"}},
                "outcome": {{"type": "applied"}}, "by": "config"}}"#,
            "5a".repeat(32)
        ),
        "unknown field `by`",
    );
}

#[test]
fn audit_filters_refuse_unknown_fields() {
    assert_rejected::<AuditFilter>(
        r#"{"by": [], "subject": null, "window": null, "caller": null}"#,
        "unknown field `caller`",
    );
    assert_rejected::<AuditFilter>(
        r#"{"by": [{"type": "resolver"}], "subject": null, "window": null}"#,
        "unknown variant `resolver`",
    );
}
