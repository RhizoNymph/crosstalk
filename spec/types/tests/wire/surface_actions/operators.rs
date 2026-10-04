//! The operator directory and permissions on the wire: `QueryApi::operators`
//! returns `Operator`s, whose `PermissionSet` is an array of permission
//! strings in `Permission::ALL` order, and whose `OperatorName` is checked
//! text.

use super::super::harness::{assert_golden, assert_rejected};
use super::super::{ULID_B, id};
use super::{operator, operator_name};
use crate::ids::OperatorId;
use crate::interfaces::l8_surface::operators::{AccessMode, Operator, OperatorName};
use crate::interfaces::l8_surface::{Permission, PermissionSet};

const AREA: &str = "surface_actions/operators";

/// `QueryApi::operators`: a current operator and a former one, which keeps
/// its name and holds no permission.
#[test]
fn operators_golden() {
    let operators = vec![
        Operator {
            id: operator(),
            name: operator_name(),
            permissions: PermissionSet::of([Permission::View, Permission::Triage]),
        },
        Operator {
            id: id(OperatorId::from_ulid_text, ULID_B),
            name: OperatorName::new("Ada Lovelace").expect("a valid name"),
            permissions: PermissionSet::EMPTY,
        },
    ];
    assert_golden(AREA, "operators", &operators);

    fn mode(mode: AccessMode) -> AccessMode {
        match mode {
            AccessMode::Trusted | AccessMode::Authenticated => mode,
        }
    }
    let modes = [AccessMode::Trusted, AccessMode::Authenticated].map(mode);
    assert_golden(AREA, "access_modes", &modes.to_vec());
}

#[test]
fn permission_sets_golden() {
    assert_golden(AREA, "permission_set_all", &PermissionSet::ALL);
    assert_golden(AREA, "permission_set_empty", &PermissionSet::EMPTY);
    let some = PermissionSet::of([Permission::Audit, Permission::View]);
    assert_golden(AREA, "permission_set_view_audit", &some);
}

/// A set decodes from any order with repeats, as `PermissionSet::of`
/// builds one, and encodes in `Permission::ALL` order, each once.
#[test]
fn permission_sets_decode_any_order_and_encode_canonically() {
    let decoded: PermissionSet =
        serde_json::from_str(r#"["audit", "view", "audit"]"#).expect("permissions");
    assert_eq!(
        decoded,
        PermissionSet::of([Permission::View, Permission::Audit])
    );
    assert_eq!(
        serde_json::to_string(&decoded).expect("encodes"),
        r#"["view","audit"]"#
    );
    assert_rejected::<PermissionSet>(r#"["view", "admin"]"#, "unknown variant `admin`");
    assert_rejected::<PermissionSet>(r#""view""#, "invalid type: string");
}

#[test]
fn operator_names_refuse_what_their_constructor_refuses() {
    let decoded: OperatorName = serde_json::from_str(r#""  Grace Hopper ""#).expect("trims");
    assert_eq!(decoded, operator_name());
    assert_rejected::<OperatorName>(r#""   ""#, "invalid operator name: Blank");
    let long = "g".repeat(OperatorName::MAX_CHARS + 1);
    assert_rejected::<OperatorName>(
        &format!("\"{long}\""),
        "invalid operator name: TooLong { max: 64, got: 65 }",
    );
    assert_rejected::<OperatorName>(
        r#""Grace\u0007Hopper""#,
        "invalid operator name: ControlCharacter",
    );
}

#[test]
fn operators_refuse_unknown_fields_and_modes() {
    let operator = operator().ulid_text();
    assert_rejected::<Operator>(
        &format!(
            r#"{{"id": "{operator}", "name": "Grace Hopper", "permissions": ["view"], "token": "x"}}"#
        ),
        "unknown field `token`",
    );
    assert_rejected::<Operator>(
        &format!(r#"{{"id": "{operator}", "name": "", "permissions": []}}"#),
        "invalid operator name: Blank",
    );
    assert_rejected::<AccessMode>(r#""anonymous""#, "unknown variant `anonymous`");
}
