//! Write-ahead audit intents and the `Interrupted` outcome
//! (`crate::interfaces::l8_surface::audit`): an intent is recorded only
//! for a permitted call, its entry keeps its id, time, caller and action,
//! and an interrupted call reads back as a store failure
//! (`surface.audit.no-silent-effect`).

use crate::derived::flow::channel::policy::PolicyKind;
use crate::ids::{AlertId, AuditId};
use crate::interfaces::l8_surface::audit::{
    AuditBody, AuditIntent, AuditOutcome, INTERRUPTED_REASON, InvalidOperatorRecord,
    OperatorRecord, OutcomeKind, Rejection,
};
use crate::interfaces::l8_surface::{
    ActionError, ActionOutcome, CallerSnapshot, OperatorAction, Permission,
};
use crate::tests::fixtures::{at, channel};
use crate::tests::operators::caller;

fn set_policy() -> OperatorAction {
    OperatorAction::SetPolicy {
        channel: channel(1),
        policy: PolicyKind::Sanctioned,
        note: None,
    }
}

fn acknowledge() -> OperatorAction {
    OperatorAction::Acknowledge {
        alert: AlertId::from_ulid(3),
    }
}

/// `surface.audit.outcome-matches-permission` for `Interrupted`: only a
/// permitted call can have been interrupted mid-effect.
#[test]
fn interrupted_record_requires_permission() {
    for (action, required) in [
        (set_policy(), Permission::Govern),
        (acknowledge(), Permission::Triage),
    ] {
        assert_eq!(
            OperatorRecord::new(
                caller(1, &[Permission::View]),
                action.clone(),
                AuditOutcome::Interrupted
            ),
            Err(InvalidOperatorRecord::AttemptedWithoutPermission { required })
        );
        let record = OperatorRecord::new(
            caller(1, &[required]),
            action.clone(),
            AuditOutcome::Interrupted,
        )
        .expect("caller holds the permission");
        assert_eq!(record.outcome(), &AuditOutcome::Interrupted);
        assert_eq!(record.subjects(), action.subjects());
    }
}

/// An intent is recorded after the permission check passed, so one for a
/// caller without the permission cannot be built.
#[test]
fn intent_requires_permission() {
    assert_eq!(
        AuditIntent::new(
            AuditId::from_ulid(1),
            at(5),
            caller(1, &[Permission::View]),
            set_policy()
        ),
        Err(InvalidOperatorRecord::AttemptedWithoutPermission {
            required: Permission::Govern
        })
    );
}

/// The entry an intent completes to, and the one recovery appends for it,
/// keep the intent's id, time, caller and action; neither can be
/// `Forbidden`.
#[test]
fn intent_entries_keep_the_call() {
    let governor = caller(2, &[Permission::View, Permission::Govern]);
    let intent = AuditIntent::new(AuditId::from_ulid(7), at(9), governor.clone(), set_policy())
        .expect("governor holds Govern");
    assert_eq!(intent.id(), AuditId::from_ulid(7));
    assert_eq!(intent.at(), at(9));
    assert_eq!(intent.caller(), &CallerSnapshot::of(&governor));
    assert_eq!(intent.action(), &set_policy());

    let applied = intent
        .entry(AuditOutcome::Succeeded(ActionOutcome::Applied))
        .expect("a permitted call can succeed");
    assert_eq!((applied.id, applied.at), (intent.id(), intent.at()));

    let interrupted = intent.interrupted();
    assert_eq!(
        Ok(interrupted.clone()),
        intent.entry(AuditOutcome::Interrupted)
    );
    assert_eq!((interrupted.id, interrupted.at), (intent.id(), intent.at()));
    let AuditBody::Operator(record) = &interrupted.body else {
        panic!("an operator entry");
    };
    assert_eq!(record.caller(), intent.caller());
    assert_eq!(record.action(), intent.action());

    assert_eq!(
        intent.entry(AuditOutcome::Forbidden {
            missing: Permission::Govern
        }),
        Err(InvalidOperatorRecord::ForbiddenButPermitted {
            required: Permission::Govern
        })
    );
}

/// An interrupted call returned nothing; it reads back as the store
/// failure a caller that lost its connection must assume, and has its own
/// kind. `AuditOutcome::of` never yields it.
#[test]
fn interrupted_reads_back_as_a_store_failure() {
    let outcome = AuditOutcome::Interrupted;
    assert_eq!(
        outcome.result(),
        Err(ActionError::Store {
            reason: INTERRUPTED_REASON.to_owned()
        })
    );
    assert_eq!(outcome.kind(), OutcomeKind::Interrupted);
    assert_eq!(
        AuditOutcome::of(&outcome.result()),
        AuditOutcome::Rejected(Rejection::Failed {
            reason: INTERRUPTED_REASON.to_owned()
        })
    );
}

/// An intent round-trips through its wire form, and decoding re-checks
/// the permission.
#[test]
fn intent_wire_form_decodes_checked() {
    let intent = AuditIntent::new(
        AuditId::from_ulid(7),
        at(9),
        caller(2, &[Permission::Triage]),
        acknowledge(),
    )
    .expect("caller holds Triage");
    let json = serde_json::to_string(&intent).unwrap_or_else(|error| panic!("{error}"));
    let back: AuditIntent = serde_json::from_str(&json).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(back, intent);

    let forged = json.replace("\"triage\"", "\"view\"");
    assert_ne!(forged, json, "{json}");
    assert!(
        serde_json::from_str::<AuditIntent>(&forged).is_err(),
        "{forged}"
    );
}
