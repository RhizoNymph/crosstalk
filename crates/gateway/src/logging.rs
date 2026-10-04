//! Log output: JSON, one object per line, with a top-level `level` field,
//! filtered by `RUST_LOG` (default `info`). `serve` and `migrate` log to
//! stdout, as the deployment collects it; `inspect` prints its own output
//! on stdout, so it logs warnings and errors to stderr.
//!
//! Every event's fields are flattened to the top level of its object
//! (`{"timestamp": .., "level": "INFO", "message": .., "exchange": ..,
//! "target": ..}`). Nothing logs secrets, credentials, headers or bodies.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::{SubscriberInitExt as _, TryInitError};

/// Where log lines go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sink {
    Stdout,
    Stderr,
}

/// Install the global subscriber, at `fallback` (`info`, `warn`, ...)
/// unless `RUST_LOG` says otherwise. A second call (or a subscriber someone
/// else installed) is reported and ignored.
pub fn init(sink: Sink, fallback: &str) {
    let installed = match sink {
        Sink::Stdout => try_init(std::io::stdout, fallback),
        Sink::Stderr => try_init(std::io::stderr, fallback),
    };
    if let Err(error) = installed {
        eprintln!("crosstalk: logging not initialised: {error}");
    }
}

/// Install the global JSON subscriber writing to `writer`, filtered by
/// `RUST_LOG`, or by `fallback` when it is unset or invalid.
pub fn try_init<W>(writer: W, fallback: &str) -> Result<(), TryInitError>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fallback));
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(filter)
        .with_writer(writer)
        .finish()
        .try_init()
}
