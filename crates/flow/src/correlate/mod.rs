//! The L5 correlator: co-access plus content match gives a transmission.
//!
//! ```text
//!   write (A, r) ─┐ pairing::co_access (within correlation_window, A ≠ B, write pairs)
//!   read  (B, r) ─┤   or pairing::content_co_access (a held match in B's read that
//!                 │   A's write explains, within content_retention)
//!                 └──▶ OpenChannel (AwaitingContent until window_closes_at(read.at))
//!                          │ window closes, a held match explained by A's write
//!                          ├──────────────▶ Confirm (every such match, every co-access)
//!                          │ window closes, none
//!                          └─▶ Suspect ──late match──▶ Confirm
//!                                 └── expires_at(since) ──▶ Discard (final)
//!   ContentMatched ─┬─ tool result in B's read of r ──▶ held in r's medium
//!                   ├─ parent and child ─────────────▶ Delegation ┐
//!                   ├─ tool result, no read by close ─▶ Direct(ToolResult)
//!                   ├─ user turn / system prompt ────▶ Direct      ├▶ OpenConfirmed at the close, then Extend
//!                   └─ reader output ────────────────▶ Unobserved ┘
//! ```
//!
//! - [`pairing`]: every rule deciding whether evidence counts (write
//!   outcomes, settling, co-access, carriage, explanation).
//! - [`route`]: route precedence.
//! - [`kinship`]: parent links for `Delegation`.
//! - [`lifecycle`]: which update may follow which.
//! - [`retention`]: how long a write can still be confirmed by content.
//! - [`WindowedCorrelator`]: one shard's correlator, implementing the
//!   spec's `Correlator`.

mod decide;
mod ids;
pub mod key;
pub mod kinship;
pub mod lifecycle;
mod medium;
pub mod pairing;
pub mod retention;
pub mod route;
mod windowed;

pub(crate) use ids::Derive;
pub use key::MediumKey;
pub use kinship::{Kin, Kinship};
pub use medium::Medium;
pub use retention::{ContentRetention, DEFAULT_CONTENT_RETENTION, InvalidRetention};
pub use windowed::{Decided, MediumEvidence, ReadPart, UNKNOWN_TOOL, WindowedCorrelator};

#[cfg(test)]
pub(crate) mod tests;
