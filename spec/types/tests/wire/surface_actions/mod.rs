//! The operator surface on the wire: action requests and the actions they
//! become, the audit log, the live feed, the operator directory and
//! permissions, alert sinks, the list filters and the overview. Goldens
//! under `golden/surface_actions/<area>/`.

mod actions;
mod audit;
mod lists;
mod live;
mod operators;

use super::{ULID_C, id};
use crate::ids::OperatorId;
use crate::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName, RequestIdentity,
};
use crate::interfaces::l8_surface::{Caller, Permission, PermissionSet};

/// The operator every fixture caller is.
fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

fn operator_name() -> OperatorName {
    OperatorName::new("Grace Hopper").expect("a valid name")
}

/// The caller of `operator()` holding `permissions` (at least one), built
/// the only way a caller can be: through a directory.
fn caller(permissions: &[Permission]) -> Caller {
    let config = AccessConfig::Authenticated(vec![OperatorConfig {
        id: operator(),
        name: operator_name(),
        permissions: PermissionSet::of(permissions.iter().copied()),
    }]);
    let (directory, _) = OperatorDirectory::load(None, &config).expect("a valid config");
    directory
        .caller(RequestIdentity::Verified(operator()))
        .expect("a configured operator")
}
