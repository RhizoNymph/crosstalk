//! Who the http backend acts as, read from the server.
//!
//! Over HTTP the caller argument of every trait method is ignored: the
//! server derives the caller from the bearer token
//! (`surface.api.caller-from-session`). The UI asks the server who that is
//! with `QueryApi::me` (`GET /me`), which any caller may read:
//!
//! ```text
//! me() ─▶ Operator { id, name, permissions }: the token's operator as the
//!         server's directory names it, with the permissions this request
//!         was authenticated with (never empty)
//!   ─▶ Access::of_operator: its name, a caller with exactly those permissions
//! ```
//!
//! [`resolve`] runs once at startup (an error stops the UI: a bad token is
//! a 401 there) and then every [`REFRESH`] by [`spawn_refresh`], so the
//! UI's permission gating and its "signed in as" label follow the server.
//! A failed refresh keeps the last access and logs a warning: the server
//! still refuses what the token may not do.

use std::time::Duration;

use crosstalk_client::HttpClient;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    InvalidOperatorName, OperatorName, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi, QueryError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::{Access, AccessError};
use crate::url::ulid::UlidId;

/// How often the operator's access is read again while serving.
pub const REFRESH: Duration = Duration::from_secs(30);

/// Why the server did not say who the UI is.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// Reading `me` failed: the server is unreachable, or refused the
    /// token (`401`, which the client reports as a store failure).
    #[error("asking the server who the token is failed: {}", crate::error::describe(.0))]
    Read(QueryError),
    /// The server answered with an operator no directory gives a caller
    /// (no permissions), which `me` never does.
    #[error(transparent)]
    Access(#[from] AccessError),
    /// The placeholder caller's name; "unsent" is valid, so never.
    #[error("the placeholder operator name: {0:?}")]
    Placeholder(InvalidOperatorName),
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

/// The access the server gives the client's token.
pub async fn resolve(client: &HttpClient) -> Result<Access, IdentityError> {
    let operator = client
        .me(&unsent_caller()?)
        .await
        .map_err(IdentityError::Read)?;
    Ok(Access::of_operator(&operator)?)
}

/// Reads the token's access again every `every`, sending it when it
/// changed. Ends when every receiver is gone.
pub fn spawn_refresh(
    client: HttpClient,
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
            match resolve(&client).await {
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
                            operator = %access.caller().operator().to_ulid(),
                            name = access.name(),
                            permissions = ?access.caller().permissions(),
                            "operator access changed on the server"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    error = %error,
                    "refreshing the operator's access failed; keeping the last"
                ),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::interfaces::l8_surface::operators::Operator;
    use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet};

    use super::*;

    #[test]
    fn the_access_carries_exactly_the_servers_permissions() {
        let listed = Operator {
            id: OperatorId::from_raw(2),
            name: OperatorName::new("op2").expect("name"),
            permissions: PermissionSet::of([Permission::View, Permission::Content]),
        };
        let access = Access::of_operator(&listed).expect("access");
        assert_eq!(access.name(), "op2");
        assert_eq!(access.caller().operator(), listed.id);
        assert_eq!(access.caller().permissions(), listed.permissions);
        assert!(!access.caller().has(Permission::Triage));
    }

    #[test]
    fn an_operator_without_permissions_has_no_access() {
        let former = Operator {
            id: OperatorId::from_raw(3),
            name: OperatorName::new("former").expect("name"),
            permissions: PermissionSet::EMPTY,
        };
        assert!(Access::of_operator(&former).is_err());
    }
}
