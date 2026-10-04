//! The operator directory: who may use the surface, under what name, with
//! which permissions.
//!
//! Config defines the operators ([`AccessConfig`]). On every load the
//! surface applies the config to the directory it already holds
//! ([`OperatorDirectory::load`]), which yields the new directory and the
//! [`ConfigChange`]s that turned one into the other; each change is recorded
//! in the audit log as a config entry, in the same transaction that stores
//! the new directory.
//!
//! ```text
//! AccessConfig ─▶ OperatorDirectory::load(previous) ─┬─▶ directory (stored)
//!                                                    └─▶ ConfigChange* ─▶ audit (by Config)
//! request ─ session verification ─▶ RequestIdentity ─▶ directory.caller() ─▶ Caller
//! ```
//!
//! **Modes.**
//! - `Trusted`: a single-user deployment with no login. Config names one
//!   operator, which holds every permission. Every request, whatever it
//!   carries, gets a `Caller` for that operator, so actions are still
//!   attributed and audited to a named operator.
//! - `Authenticated`: each request carries a session or token that the
//!   surface verifies; the operator it names gets a `Caller` with the
//!   permissions config gives it. Anything else gets none.
//!
//! **Former operators.** An operator config no longer defines stays in the
//! directory with no permissions, so the names of past decisions and audit
//! entries still resolve; it gets no `Caller`.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::OperatorId;
use crate::interfaces::l8_surface::audit::ConfigChange;
use crate::interfaces::l8_surface::{Caller, PermissionSet};
use crate::wire::decode_text;

/// An operator's display name: trimmed, non-empty, at most
/// [`OperatorName::MAX_CHARS`] characters, and free of control characters.
/// On the wire, a string; decoding goes through [`OperatorName::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperatorName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidOperatorName {
    Blank,
    TooLong { max: usize, got: usize },
    ControlCharacter,
}

impl OperatorName {
    pub const MAX_CHARS: usize = 64;

    pub fn new(text: &str) -> Result<Self, InvalidOperatorName> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(InvalidOperatorName::Blank);
        }
        let chars = trimmed.chars().count();
        if chars > Self::MAX_CHARS {
            return Err(InvalidOperatorName::TooLong {
                max: Self::MAX_CHARS,
                got: chars,
            });
        }
        if trimmed.chars().any(char::is_control) {
            return Err(InvalidOperatorName::ControlCharacter);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A JSON string.
impl Serialize for OperatorName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// A JSON string that [`OperatorName::new`] accepts, trimmed as it trims.
impl<'de> Deserialize<'de> for OperatorName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "operator name", |text| Self::new(&text))
    }
}

/// One directory entry, as `QueryApi::operators` returns it. A former
/// operator has no permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Operator {
    pub id: OperatorId,
    pub name: OperatorName,
    pub permissions: PermissionSet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    Trusted,
    Authenticated,
}

/// The one operator of a trusted deployment. It holds every permission;
/// config cannot give it fewer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedOperator {
    pub id: OperatorId,
    pub name: OperatorName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorConfig {
    pub id: OperatorId,
    pub name: OperatorName,
    pub permissions: PermissionSet,
}

/// The `access` section of the surface config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessConfig {
    Trusted(TrustedOperator),
    Authenticated(Vec<OperatorConfig>),
}

impl AccessConfig {
    pub fn mode(&self) -> AccessMode {
        match self {
            Self::Trusted(_) => AccessMode::Trusted,
            Self::Authenticated(_) => AccessMode::Authenticated,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidAccessConfig {
    /// `Authenticated` with no operators: nobody could use the surface.
    NoOperators,
    DuplicateOperator(OperatorId),
    /// An operator with no permissions could not even view. Remove it from
    /// config instead; it stays in the directory as a former operator.
    NoPermissions(OperatorId),
}

/// What session verification established about a request, before the
/// directory is consulted. In trusted mode it is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestIdentity {
    /// No session or token, or one that failed verification.
    Anonymous,
    /// A verified session or token for this operator.
    Verified(OperatorId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unauthenticated {
    NoSession,
    UnknownOperator(OperatorId),
    /// An operator config no longer defines.
    FormerOperator(OperatorId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Trusted(OperatorId),
    Authenticated,
}

/// The operators the surface knows, current and former.
///
/// Built only through [`OperatorDirectory::load`], so:
/// - in trusted mode, exactly one operator has any permission: the trusted
///   one, which has every permission;
/// - in authenticated mode, the operators with permissions are exactly the
///   ones config defines, with the permissions it gives them;
/// - every operator any earlier load defined is still listed, with no
///   permissions if config no longer defines it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorDirectory {
    mode: Mode,
    operators: BTreeMap<OperatorId, Operator>,
}

impl OperatorDirectory {
    /// Apply `config` to the directory the surface held before (`None` on
    /// the first load). Returns the new directory and the changes, in order:
    /// a mode change first, then operator changes by id. Loading a config
    /// the directory already reflects returns no changes.
    pub fn load(
        previous: Option<&Self>,
        config: &AccessConfig,
    ) -> Result<(Self, Vec<ConfigChange>), InvalidAccessConfig> {
        let (mode, current) = Self::current(config)?;
        let mut changes = Vec::new();
        if previous.is_none_or(|previous| previous.mode() != config.mode()) {
            changes.push(ConfigChange::SetAccessMode(config.mode()));
        }
        // At most one change per operator, ordered by id.
        let mut by_operator = BTreeMap::new();
        let mut operators = BTreeMap::new();
        for operator in current.values() {
            let unchanged = previous
                .and_then(|previous| previous.operators.get(&operator.id))
                .is_some_and(|before| before == operator);
            if !unchanged {
                by_operator.insert(
                    operator.id,
                    ConfigChange::SetOperator {
                        operator: operator.id,
                        name: operator.name.clone(),
                        permissions: operator.permissions,
                    },
                );
            }
            operators.insert(operator.id, operator.clone());
        }
        let former = previous
            .into_iter()
            .flat_map(|previous| previous.operators.values())
            .filter(|before| !current.contains_key(&before.id));
        for before in former {
            if !before.permissions.is_empty() {
                by_operator.insert(
                    before.id,
                    ConfigChange::RemoveOperator {
                        operator: before.id,
                    },
                );
            }
            operators.insert(
                before.id,
                Operator {
                    permissions: PermissionSet::EMPTY,
                    ..before.clone()
                },
            );
        }
        changes.extend(by_operator.into_values());
        Ok((Self { mode, operators }, changes))
    }

    fn current(
        config: &AccessConfig,
    ) -> Result<(Mode, BTreeMap<OperatorId, Operator>), InvalidAccessConfig> {
        match config {
            AccessConfig::Trusted(trusted) => {
                let operator = Operator {
                    id: trusted.id,
                    name: trusted.name.clone(),
                    permissions: PermissionSet::ALL,
                };
                Ok((
                    Mode::Trusted(trusted.id),
                    BTreeMap::from([(trusted.id, operator)]),
                ))
            }
            AccessConfig::Authenticated(configs) => {
                if configs.is_empty() {
                    return Err(InvalidAccessConfig::NoOperators);
                }
                let mut operators = BTreeMap::new();
                for config in configs {
                    if config.permissions.is_empty() {
                        return Err(InvalidAccessConfig::NoPermissions(config.id));
                    }
                    let operator = Operator {
                        id: config.id,
                        name: config.name.clone(),
                        permissions: config.permissions,
                    };
                    if operators.insert(config.id, operator).is_some() {
                        return Err(InvalidAccessConfig::DuplicateOperator(config.id));
                    }
                }
                Ok((Mode::Authenticated, operators))
            }
        }
    }

    pub fn mode(&self) -> AccessMode {
        match self.mode {
            Mode::Trusted(_) => AccessMode::Trusted,
            Mode::Authenticated => AccessMode::Authenticated,
        }
    }

    /// Current and former operators, by id.
    pub fn operators(&self) -> impl Iterator<Item = &Operator> {
        self.operators.values()
    }

    pub fn get(&self, id: OperatorId) -> Option<&Operator> {
        self.operators.get(&id)
    }

    /// The caller for one request. In trusted mode, always the trusted
    /// operator with every permission, whatever `identity` is. In
    /// authenticated mode, the verified operator with its configured
    /// permissions, if config defines it.
    pub fn caller(&self, identity: RequestIdentity) -> Result<Caller, Unauthenticated> {
        let id = match (self.mode, identity) {
            (Mode::Trusted(trusted), _) => trusted,
            (Mode::Authenticated, RequestIdentity::Anonymous) => {
                return Err(Unauthenticated::NoSession);
            }
            (Mode::Authenticated, RequestIdentity::Verified(id)) => id,
        };
        let operator = self
            .operators
            .get(&id)
            .ok_or(Unauthenticated::UnknownOperator(id))?;
        if operator.permissions.is_empty() {
            return Err(Unauthenticated::FormerOperator(id));
        }
        Ok(Caller {
            operator: id,
            permissions: operator.permissions,
        })
    }
}
