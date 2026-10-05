//! Who the http backend acts as, read from the server.
//!
//! The spec has no "who am I" read, and over HTTP the caller argument of
//! every trait method is ignored: the server derives the caller from the
//! bearer token (`surface.api.caller-from-session`). So the UI finds its
//! operator in `QueryApi::operators`, the server's directory, current and
//! former operators alike:
//!
//! ```text
//! operators() ─▶ the current ones (non-empty permissions)
//!   OperatorPick::TheOnlyOne ─▶ exactly one, else NoOperator / Several
//!   OperatorPick::Id(id)     ─▶ that one, else NotListed / Former
//!   ─▶ Access::of_operator: its name, and a caller with exactly its permissions
//! ```
//!
//! [`resolve`] runs once at startup (an error stops the UI: a bad token is
//! a 401 there) and then every [`REFRESH`] by [`spawn_refresh`], by the id
//! found at startup, so the UI's permission gating follows the server's
//! directory. A failed refresh keeps the last access and logs a warning:
//! the server still refuses what the token may not do.

use std::time::Duration;

use crosstalk_client::HttpClient;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    InvalidOperatorName, Operator, OperatorName, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi, QueryError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::{Access, AccessError, OperatorPick};
use crate::url::ulid::UlidId;

/// How often the operator's permissions are read again while serving.
pub const REFRESH: Duration = Duration::from_secs(30);

/// Why the server's directory does not say who the UI is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// Listing the operators failed: the server is unreachable, refused
    /// the token (`401`, which the client reports as a store failure), or
    /// the token's operator may not view (`403`).
    #[error("listing the server's operators failed: {}", crate::error::describe(.0))]
    Read(QueryError),
    #[error("the server lists no current operator")]
    NoOperator,
    #[error(
        "the server lists {} current operators ({}); name the token's in backend.http.operator",
        .ids.len(),
        .ids.iter().map(|id| id.to_ulid()).collect::<Vec<_>>().join(", ")
    )]
    Several { ids: Vec<OperatorId> },
    #[error("the server lists no operator {}", .0.to_ulid())]
    NotListed(OperatorId),
    #[error("operator {} is a former operator on the server (no permissions)", .0.to_ulid())]
    Former(OperatorId),
    #[error(transparent)]
    Access(#[from] AccessError),
    /// The placeholder caller's name; "unsent" is valid, so never.
    #[error("the placeholder operator name: {0:?}")]
    Placeholder(InvalidOperatorName),
}

/// The operator `pick` names among the server's.
pub fn pick(operators: &[Operator], pick: OperatorPick) -> Result<&Operator, IdentityError> {
    let current = || operators.iter().filter(|o| !o.permissions.is_empty());
    match pick {
        OperatorPick::TheOnlyOne => {
            let mut found = current();
            match (found.next(), found.next()) {
                (None, _) => Err(IdentityError::NoOperator),
                (Some(one), None) => Ok(one),
                (Some(_), Some(_)) => Err(IdentityError::Several {
                    ids: current().map(|o| o.id).collect(),
                }),
            }
        }
        OperatorPick::Id(id) => {
            let operator = operators
                .iter()
                .find(|o| o.id == id)
                .ok_or(IdentityError::NotListed(id))?;
            if operator.permissions.is_empty() {
                return Err(IdentityError::Former(id));
            }
            Ok(operator)
        }
    }
}

/// The caller the trait methods are given before the UI knows who it is.
/// It never travels: the client ignores it and the server reads the
/// token.
fn unsent_caller() -> Result<Caller, IdentityError> {
    let name = OperatorName::new("unsent").map_err(IdentityError::Placeholder)?;
    let access = Access::trusted(TrustedOperator {
        id: OperatorId::from_raw(0),
        name,
    })?;
    Ok(access.caller())
}

/// The access the server's directory gives the operator `which` names.
pub async fn resolve(client: &HttpClient, which: OperatorPick) -> Result<Access, IdentityError> {
    let operators = client
        .operators(&unsent_caller()?)
        .await
        .map_err(IdentityError::Read)?;
    let operator = pick(&operators, which)?;
    Ok(Access::of_operator(operator)?)
}

/// Reads the operator `id`'s access again every `every`, sending it when
/// it changed. Ends when every receiver is gone.
pub fn spawn_refresh(
    client: HttpClient,
    id: OperatorId,
    sender: watch::Sender<Access>,
    every: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(every);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick is immediate; startup has just resolved.
        ticks.tick().await;
        while !sender.is_closed() {
            ticks.tick().await;
            match resolve(&client, OperatorPick::Id(id)).await {
                Ok(access) => {
                    let changed = sender.send_if_modified(|current| {
                        let changed = *current != access;
                        if changed {
                            *current = access.clone();
                        }
                        changed
                    });
                    if changed {
                        tracing::info!(
                            operator = %id.to_ulid(),
                            name = access.name(),
                            permissions = ?access.caller().permissions(),
                            "operator access changed on the server"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    operator = %id.to_ulid(),
                    error = %error,
                    "refreshing the operator's access failed; keeping the last"
                ),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet};

    use super::*;

    fn operator(n: u128, permissions: &[Permission]) -> Operator {
        Operator {
            id: OperatorId::from_raw(n),
            name: OperatorName::new(&format!("op{n}")).expect("name"),
            permissions: PermissionSet::of(permissions.iter().copied()),
        }
    }

    #[test]
    fn the_only_current_operator_is_the_tokens() {
        let operators = [operator(1, &[]), operator(2, &[Permission::View])];
        let found = pick(&operators, OperatorPick::TheOnlyOne).expect("one");
        assert_eq!(found.id, OperatorId::from_raw(2));
    }

    #[test]
    fn several_or_no_current_operators_need_an_id() {
        let operators = [
            operator(1, &[Permission::View]),
            operator(2, &[Permission::View]),
        ];
        assert_eq!(
            pick(&operators, OperatorPick::TheOnlyOne),
            Err(IdentityError::Several {
                ids: vec![OperatorId::from_raw(1), OperatorId::from_raw(2)]
            })
        );
        assert_eq!(
            pick(&[operator(1, &[])], OperatorPick::TheOnlyOne),
            Err(IdentityError::NoOperator)
        );
        let chosen = pick(&operators, OperatorPick::Id(OperatorId::from_raw(2))).expect("by id");
        assert_eq!(chosen.name.as_str(), "op2");
    }

    #[test]
    fn a_named_operator_must_be_current() {
        let operators = [operator(1, &[]), operator(2, &[Permission::View])];
        assert_eq!(
            pick(&operators, OperatorPick::Id(OperatorId::from_raw(1))),
            Err(IdentityError::Former(OperatorId::from_raw(1)))
        );
        assert_eq!(
            pick(&operators, OperatorPick::Id(OperatorId::from_raw(3))),
            Err(IdentityError::NotListed(OperatorId::from_raw(3)))
        );
    }

    #[test]
    fn the_access_carries_exactly_the_servers_permissions() {
        let listed = operator(2, &[Permission::View, Permission::Content]);
        let access = Access::of_operator(&listed).expect("access");
        assert_eq!(access.name(), "op2");
        assert_eq!(access.caller().operator(), listed.id);
        assert_eq!(access.caller().permissions(), listed.permissions);
        assert!(!access.caller().has(Permission::Triage));
    }
}
