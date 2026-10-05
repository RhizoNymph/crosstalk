//! **Provisional.** The gateway's full composition, hosted by the UI
//! binary: `crosstalk_gateway::live::Live::start(config)`, one in-process
//! composition of the capture proxy (on `proxy_listen`), the pipeline, the
//! stores and the surface, which the gateway will provide. Until it does,
//! starting it is a typed error, and [`LiveBackend`] has no values, so an
//! `AppBackend::Live` cannot exist.
//!
//! Built only with the `live` cargo feature (off by default). When the
//! gateway's composition lands, `LiveBackend` holds its surface,
//! `start` calls it, and `crate::backend::Service` keeps its pipeline and
//! proxy running beside the Topcoat server.

use crate::config::LiveConfig;

/// A running live composition. None can be started yet.
#[derive(Debug)]
pub enum LiveBackend {}

/// Why the live composition did not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveStartError {
    #[error("Live composition not available yet (proxy would listen on {proxy_listen})")]
    NotAvailable { proxy_listen: std::net::SocketAddr },
}

impl LiveBackend {
    /// Starts the composition `config` describes.
    pub fn start(config: LiveConfig) -> Result<Self, LiveStartError> {
        Err(LiveStartError::NotAvailable {
            proxy_listen: config.proxy_listen,
        })
    }
}
