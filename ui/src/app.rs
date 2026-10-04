//! Per-request access to the configured backend and caller.

use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::context::{Cx, app_context};

use crate::backend::fixture::FixtureBackend;
use crate::config::TrustedOperator;

/// The backend pages read from. An enum over implementations once there is
/// more than one.
pub type AppBackend = FixtureBackend;

pub fn backend(cx: &Cx) -> &AppBackend {
    app_context::<AppBackend>(cx)
}

/// The caller of this request. Trusted mode: the configured operator, with
/// every permission. Shards and procedures call this themselves, since page
/// guards do not run for their endpoints.
pub fn caller(cx: &Cx) -> Caller {
    app_context::<TrustedOperator>(cx).caller()
}

pub fn operator(cx: &Cx) -> &TrustedOperator {
    app_context::<TrustedOperator>(cx)
}

pub fn can(caller: &Caller, permission: Permission) -> bool {
    caller.permissions.contains(&permission)
}
