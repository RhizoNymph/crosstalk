//! What every smoke test shares.

use crosstalk_e2e::scenario::{DEFAULT_START, Scenario, WireExchange};
use crosstalk_e2e::{Capture, CaptureError};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;

/// A test's failure, from any of the harness's typed errors.
pub type Failure = Box<dyn std::error::Error>;

/// A failure that is only a message (a bus error without `Error`, an
/// unexpected value).
#[derive(Debug)]
pub struct Unexpected(pub String);

impl std::fmt::Display for Unexpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unexpected {}

pub fn unexpected(what: impl Into<String>) -> Failure {
    Box::new(Unexpected(what.into()))
}

/// The relay at its default start.
pub fn relay() -> Scenario {
    Scenario::wiki_relay(DEFAULT_START)
}

/// Every exchange of `scenario` through L0 and L1, with the scripted one.
pub fn normalized(
    scenario: &Scenario,
) -> Result<Vec<(&WireExchange, NormalizedExchange)>, CaptureError> {
    let capture = Capture::new()?;
    scenario
        .exchanges
        .iter()
        .map(|exchange| Ok((exchange, capture.normalized(exchange)?)))
        .collect()
}
