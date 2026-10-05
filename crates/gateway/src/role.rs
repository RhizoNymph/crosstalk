//! Process roles: which of the gateway's tasks one process runs.
//!
//! | Role | Runs |
//! | --- | --- |
//! | `all` | everything below that exists: the proxy feeding a [`Live`](crate::live::Live) process, the exchange log, the HTTP API |
//! | `proxy` | the reverse proxy and the capture stage (L0 and L1: normalize, store bodies, publish `ExchangeCaptured`) into its own `Live` |
//! | `pipeline` | a `Live` process (L3 to L7 over the memory stores) and the exchange log (the P3 stopgap) |
//! | `api` | a `Live` process and the L8 HTTP binding on `api.listen` over its surface |
//! | `analysis` | L6 analysis: not built yet (P6), so nothing |
//!
//! Every role serves the ops listener, and every role but `analysis` runs a
//! `Live` process. The bus and the stores are in-process until the
//! cross-node bus (P9) and the Postgres stores are wired, so processes of
//! different roles do not reach each other: only `all` captures, detects
//! and serves what it detected end to end.

use std::fmt;
use std::str::FromStr;

/// A process role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    All,
    Proxy,
    Pipeline,
    Api,
    Analysis,
}

/// The role text is not one of the five.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown role {0:?} (expected all, proxy, pipeline, api or analysis)")]
pub struct UnknownRole(pub String);

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "all" => Ok(Self::All),
            "proxy" => Ok(Self::Proxy),
            "pipeline" => Ok(Self::Pipeline),
            "api" => Ok(Self::Api),
            "analysis" => Ok(Self::Analysis),
            other => Err(UnknownRole(other.to_owned())),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::All => "all",
            Self::Proxy => "proxy",
            Self::Pipeline => "pipeline",
            Self::Api => "api",
            Self::Analysis => "analysis",
        })
    }
}

impl Role {
    /// Whether this process runs the proxy and the capture stage.
    pub fn runs_proxy(self) -> bool {
        matches!(self, Self::All | Self::Proxy)
    }

    /// Whether this process keeps the exchange log.
    pub fn runs_pipeline(self) -> bool {
        matches!(self, Self::All | Self::Pipeline)
    }

    /// Whether this process runs a `Live` process (the layer consumers and
    /// the surface over the memory stores).
    pub fn runs_live(self) -> bool {
        !matches!(self, Self::Analysis)
    }

    /// Whether this process serves the HTTP API (when `api` is
    /// configured).
    pub fn runs_api(self) -> bool {
        matches!(self, Self::All | Self::Api)
    }

    /// What this role would run that does not exist yet, for the startup
    /// log.
    pub fn not_built(self) -> &'static [&'static str] {
        match self {
            Self::All | Self::Analysis => &["analysis: L6 (P6)"],
            Self::Proxy | Self::Pipeline | Self::Api => &[],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_role_round_trips_through_its_text() {
        for role in [
            Role::All,
            Role::Proxy,
            Role::Pipeline,
            Role::Api,
            Role::Analysis,
        ] {
            assert_eq!(role.to_string().parse::<Role>(), Ok(role));
        }
        assert_eq!(
            "everything".parse::<Role>(),
            Err(UnknownRole("everything".to_owned()))
        );
    }

    #[test]
    fn roles_select_their_tasks() {
        assert!(Role::All.runs_proxy() && Role::All.runs_pipeline());
        assert!(Role::Proxy.runs_proxy() && !Role::Proxy.runs_pipeline());
        assert!(!Role::Pipeline.runs_proxy() && Role::Pipeline.runs_pipeline());
        for role in [Role::Api, Role::Analysis] {
            assert!(!role.runs_proxy() && !role.runs_pipeline());
        }
        // The API is built (P7.1): only analysis is still missing.
        assert!(Role::Api.not_built().is_empty() && Role::Api.runs_api());
        assert!(!Role::Analysis.not_built().is_empty() && !Role::Analysis.runs_live());
    }
}
