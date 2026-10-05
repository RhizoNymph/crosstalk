//! Callers for tests, built the only way the spec builds one: by an
//! [`OperatorDirectory`] for a verified request.

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, InvalidAccessConfig, InvalidOperatorName, OperatorConfig, OperatorDirectory,
    OperatorName, RequestIdentity, Unauthenticated,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, PermissionSet};

/// A caller the spec's directory refused to build.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallerError {
    #[error("operator name: {0:?}")]
    Name(InvalidOperatorName),
    #[error("access config: {0:?}")]
    Config(InvalidAccessConfig),
    #[error("caller: {0:?}")]
    Unauthenticated(Unauthenticated),
}

/// `operator` holding exactly `permissions`, as an authenticated directory
/// hands it to a verified request.
pub fn caller(operator: OperatorId, permissions: PermissionSet) -> Result<Caller, CallerError> {
    let config = OperatorConfig {
        id: operator,
        name: OperatorName::new("conformance").map_err(CallerError::Name)?,
        permissions,
    };
    let (directory, _) = OperatorDirectory::load(None, &AccessConfig::Authenticated(vec![config]))
        .map_err(CallerError::Config)?;
    directory
        .caller(RequestIdentity::Verified(operator))
        .map_err(CallerError::Unauthenticated)
}
