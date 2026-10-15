use crate::ids::OperatorId;
use crate::interfaces::l8_surface::audit::ConfigChange;
use crate::interfaces::l8_surface::operators::{
    AccessConfig, AccessMode, InvalidAccessConfig, InvalidOperatorName, OperatorConfig,
    OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator, Unauthenticated,
};
use crate::interfaces::l8_surface::{Caller, Permission, PermissionSet};

pub(super) fn operator(n: u128) -> OperatorId {
    OperatorId::from_ulid(n)
}

fn name(text: &str) -> OperatorName {
    OperatorName::new(text).expect("valid name")
}

fn configured(n: u128, permissions: &[Permission]) -> OperatorConfig {
    OperatorConfig {
        id: operator(n),
        name: name(&format!("operator {n}")),
        permissions: PermissionSet::of(permissions.iter().copied()),
    }
}

fn authenticated(operators: Vec<OperatorConfig>) -> AccessConfig {
    AccessConfig::Authenticated(operators)
}

fn trusted(n: u128) -> AccessConfig {
    AccessConfig::Trusted(TrustedOperator {
        id: operator(n),
        name: name("me"),
    })
}

fn load(previous: Option<&OperatorDirectory>, config: &AccessConfig) -> OperatorDirectory {
    OperatorDirectory::load(previous, config)
        .expect("valid config")
        .0
}

/// A caller for operator `n` holding `permissions` (at least one), built the
/// only way a caller can be: through a directory.
pub(super) fn caller(n: u128, permissions: &[Permission]) -> Caller {
    load(None, &authenticated(vec![configured(n, permissions)]))
        .caller(RequestIdentity::Verified(operator(n)))
        .expect("configured operator")
}

#[test]
fn permission_set_all_holds_every_permission() {
    for permission in Permission::ALL {
        assert!(PermissionSet::ALL.contains(permission), "{permission:?}");
        assert!(!PermissionSet::EMPTY.contains(permission), "{permission:?}");
    }
    assert_eq!(
        PermissionSet::ALL.iter().collect::<Vec<_>>(),
        Permission::ALL.to_vec()
    );
    assert_eq!(PermissionSet::of(Permission::ALL), PermissionSet::ALL);
    assert!(PermissionSet::EMPTY.is_empty());
}

#[test]
fn permission_set_holds_what_was_given() {
    let set = PermissionSet::of([Permission::Audit, Permission::View, Permission::View]);
    assert_eq!(
        set.iter().collect::<Vec<_>>(),
        vec![Permission::View, Permission::Audit]
    );
    assert!(!set.contains(Permission::Govern));
    assert!(!set.is_empty());
}

#[test]
fn operator_name_is_trimmed_and_checked() {
    assert_eq!(name("  Ada  ").as_str(), "Ada");
    assert_eq!(OperatorName::new("   "), Err(InvalidOperatorName::Blank));
    assert_eq!(
        OperatorName::new("a\u{7}b"),
        Err(InvalidOperatorName::ControlCharacter)
    );
    let long = "x".repeat(OperatorName::MAX_CHARS + 1);
    assert_eq!(
        OperatorName::new(&long),
        Err(InvalidOperatorName::TooLong {
            max: OperatorName::MAX_CHARS,
            got: OperatorName::MAX_CHARS + 1
        })
    );
    assert!(OperatorName::new(&"é".repeat(OperatorName::MAX_CHARS)).is_ok());
}

#[test]
fn load_rejects_unusable_configs() {
    assert_eq!(
        OperatorDirectory::load(None, &authenticated(Vec::new())),
        Err(InvalidAccessConfig::NoOperators)
    );
    assert_eq!(
        OperatorDirectory::load(
            None,
            &authenticated(vec![
                configured(1, &[Permission::View]),
                configured(1, &[Permission::Audit]),
            ])
        ),
        Err(InvalidAccessConfig::DuplicateOperator(operator(1)))
    );
    assert_eq!(
        OperatorDirectory::load(None, &authenticated(vec![configured(2, &[])])),
        Err(InvalidAccessConfig::NoPermissions(operator(2)))
    );
}

#[test]
fn trusted_mode_gives_every_request_the_trusted_caller() {
    let directory = load(None, &trusted(7));
    assert_eq!(directory.mode(), AccessMode::Trusted);
    let identities = [
        RequestIdentity::Anonymous,
        RequestIdentity::Verified(operator(7)),
        RequestIdentity::Verified(operator(99)),
    ];
    for identity in identities {
        let caller = directory.caller(identity).expect("trusted mode");
        assert_eq!(caller.operator(), operator(7), "{identity:?}");
        assert_eq!(caller.permissions(), PermissionSet::ALL, "{identity:?}");
    }
}

#[test]
fn trusted_mode_has_one_operator_with_permissions() {
    let before = load(
        None,
        &authenticated(vec![
            configured(1, &[Permission::View]),
            configured(2, &[Permission::Govern]),
        ]),
    );
    let directory = load(Some(&before), &trusted(3));
    let active: Vec<_> = directory
        .operators()
        .filter(|operator| !operator.permissions.is_empty())
        .collect();
    assert_eq!(active.len(), 1);
    let only = active.first().expect("one active operator");
    assert_eq!(only.id, operator(3));
    assert_eq!(only.permissions, PermissionSet::ALL);
    assert_eq!(directory.operators().count(), 3);
}

#[test]
fn authenticated_caller_has_its_configured_permissions() {
    let directory = load(
        None,
        &authenticated(vec![
            configured(1, &[Permission::View, Permission::Triage]),
            configured(2, &[Permission::Audit]),
        ]),
    );
    let caller = directory
        .caller(RequestIdentity::Verified(operator(1)))
        .expect("configured");
    assert_eq!(caller.operator(), operator(1));
    assert_eq!(
        caller.permissions(),
        PermissionSet::of([Permission::View, Permission::Triage])
    );
    assert!(caller.has(Permission::Triage));
    assert!(!caller.has(Permission::Audit));
}

#[test]
fn authenticated_mode_refuses_unknown_requests() {
    let directory = load(
        None,
        &authenticated(vec![configured(1, &[Permission::View])]),
    );
    assert_eq!(
        directory.caller(RequestIdentity::Anonymous),
        Err(Unauthenticated::NoSession)
    );
    assert_eq!(
        directory.caller(RequestIdentity::Verified(operator(5))),
        Err(Unauthenticated::UnknownOperator(operator(5)))
    );
}

#[test]
fn first_load_records_mode_then_operators_by_id() {
    let (_, changes) = OperatorDirectory::load(
        None,
        &authenticated(vec![
            configured(2, &[Permission::Govern]),
            configured(1, &[Permission::View]),
        ]),
    )
    .expect("valid");
    assert_eq!(
        changes,
        vec![
            ConfigChange::SetAccessMode(AccessMode::Authenticated),
            ConfigChange::SetOperator {
                operator: operator(1),
                name: name("operator 1"),
                permissions: PermissionSet::of([Permission::View]),
            },
            ConfigChange::SetOperator {
                operator: operator(2),
                name: name("operator 2"),
                permissions: PermissionSet::of([Permission::Govern]),
            },
        ]
    );
}

#[test]
fn reloading_the_same_config_changes_nothing() {
    for config in [
        trusted(1),
        authenticated(vec![
            configured(1, &[Permission::View]),
            configured(2, &[Permission::Govern, Permission::Audit]),
        ]),
    ] {
        let first = load(None, &config);
        let (second, changes) = OperatorDirectory::load(Some(&first), &config).expect("valid");
        assert_eq!(changes, Vec::new(), "{config:?}");
        assert_eq!(second, first);
    }
}

#[test]
fn removed_operator_keeps_its_name_and_loses_its_caller() {
    let first = load(
        None,
        &authenticated(vec![
            configured(1, &[Permission::View]),
            configured(2, &[Permission::Govern]),
        ]),
    );
    let config = authenticated(vec![configured(1, &[Permission::View])]);
    let (second, changes) = OperatorDirectory::load(Some(&first), &config).expect("valid");
    assert_eq!(
        changes,
        vec![ConfigChange::RemoveOperator {
            operator: operator(2)
        }]
    );
    let former = second.get(operator(2)).expect("kept");
    assert_eq!(former.name, name("operator 2"));
    assert!(former.permissions.is_empty());
    assert_eq!(
        second.caller(RequestIdentity::Verified(operator(2))),
        Err(Unauthenticated::FormerOperator(operator(2)))
    );
    let (third, changes) = OperatorDirectory::load(Some(&second), &config).expect("valid");
    assert_eq!(changes, Vec::new());
    assert_eq!(third, second);
}

#[test]
fn changed_permissions_are_recorded() {
    let first = load(
        None,
        &authenticated(vec![configured(1, &[Permission::View])]),
    );
    let (second, changes) = OperatorDirectory::load(
        Some(&first),
        &authenticated(vec![configured(1, &[Permission::View, Permission::Audit])]),
    )
    .expect("valid");
    assert_eq!(
        changes,
        vec![ConfigChange::SetOperator {
            operator: operator(1),
            name: name("operator 1"),
            permissions: PermissionSet::of([Permission::View, Permission::Audit]),
        }]
    );
    assert!(
        second
            .caller(RequestIdentity::Verified(operator(1)))
            .expect("configured")
            .has(Permission::Audit)
    );
}

#[test]
fn switching_to_trusted_records_mode_first() {
    let first = load(
        None,
        &authenticated(vec![configured(5, &[Permission::View])]),
    );
    let (_, changes) = OperatorDirectory::load(Some(&first), &trusted(3)).expect("valid");
    assert_eq!(
        changes,
        vec![
            ConfigChange::SetAccessMode(AccessMode::Trusted),
            ConfigChange::SetOperator {
                operator: operator(3),
                name: name("me"),
                permissions: PermissionSet::ALL,
            },
            ConfigChange::RemoveOperator {
                operator: operator(5)
            },
        ]
    );
}

#[test]
fn former_operator_returns_when_configured_again() {
    let first = load(
        None,
        &authenticated(vec![configured(1, &[Permission::View])]),
    );
    let second = load(Some(&first), &trusted(2));
    let (third, changes) = OperatorDirectory::load(
        Some(&second),
        &authenticated(vec![configured(1, &[Permission::View])]),
    )
    .expect("valid");
    assert_eq!(
        changes,
        vec![
            ConfigChange::SetAccessMode(AccessMode::Authenticated),
            ConfigChange::SetOperator {
                operator: operator(1),
                name: name("operator 1"),
                permissions: PermissionSet::of([Permission::View]),
            },
            ConfigChange::RemoveOperator {
                operator: operator(2)
            },
        ]
    );
    assert!(third.caller(RequestIdentity::Verified(operator(1))).is_ok());
}
