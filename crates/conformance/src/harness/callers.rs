//! Callers for tests, built the only way the spec builds one: by an
//! [`OperatorDirectory`] for a verified request.

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, InvalidAccessConfig, InvalidOperatorName, OperatorConfig, OperatorDirectory,
    OperatorName, RequestIdentity, Unauthenticated,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet};

use super::Operators;

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

/// Builds callers for the harness's operators.
#[derive(Debug, Clone, Copy)]
pub struct Callers {
    operators: Operators,
}

impl Callers {
    pub fn new(operators: Operators) -> Self {
        Self { operators }
    }

    pub fn operators(&self) -> Operators {
        self.operators
    }

    /// The lead operator with every permission.
    pub fn lead(&self) -> Result<Caller, CallerError> {
        self.of(self.operators.lead, &Permission::ALL)
    }

    /// The other operator holding exactly `permissions`.
    pub fn with(&self, permissions: &[Permission]) -> Result<Caller, CallerError> {
        self.of(self.operators.other, permissions)
    }

    /// `operator` holding exactly `permissions`, as an authenticated
    /// directory hands it to a verified request.
    pub fn of(
        &self,
        operator: OperatorId,
        permissions: &[Permission],
    ) -> Result<Caller, CallerError> {
        let config = OperatorConfig {
            id: operator,
            name: OperatorName::new("conformance").map_err(CallerError::Name)?,
            permissions: PermissionSet::of(permissions.iter().copied()),
        };
        let (directory, _) =
            OperatorDirectory::load(None, &AccessConfig::Authenticated(vec![config]))
                .map_err(CallerError::Config)?;
        directory
            .caller(RequestIdentity::Verified(operator))
            .map_err(CallerError::Unauthenticated)
    }
}
