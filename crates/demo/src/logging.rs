//! JSON log lines on stderr (stdout carries the swarm's report), one
//! object per line with a top-level `level`, filtered by `RUST_LOG`
//! (default `info`).

use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Installs the global subscriber; a second call is reported and ignored.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let installed = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .finish()
        .try_init();
    if let Err(error) = installed {
        eprintln!("crosstalk-demo: logging not initialised: {error}");
    }
}
