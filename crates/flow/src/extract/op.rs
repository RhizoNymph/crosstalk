//! What an extraction yields, with each write's outcome.
//!
//! The eval spec PR adds `WriteOutcome` (on `AccessOp::Write`) and
//! `ExtractedOp` (on `ExtractedAccess`, replacing `kind`). Until it merges,
//! [`WriteOutcome`] and [`ExtractedOp`] mirror those types here, and
//! [`Classified::into_spec`] drops the outcome to fit today's
//! [`ExtractedAccess`]. Binding them at merge is: delete the two local
//! enums, import the spec's, and build `ExtractedAccess { op, .. }` in
//! `into_spec`.

use crosstalk_spec::derived::flow::access::{AccessKind, Extraction};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::interfaces::l5_flow::ExtractedAccess;

/// What became of a write: whether the call's arguments reached the
/// resource. Mirrors the eval spec PR's `WriteOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteOutcome {
    /// The tool reported success.
    Delivered,
    /// The tool refused or failed the call: recorded, never paired.
    Rejected,
    /// Nothing says whether it reached the resource. Paired at lower
    /// confidence.
    Unknown,
}

impl WriteOutcome {
    /// Whether a write with this outcome can pair with a read: every
    /// outcome but `Rejected`.
    pub fn pairs(self) -> bool {
        match self {
            Self::Delivered | Self::Unknown => true,
            Self::Rejected => false,
        }
    }
}

/// An extracted access's operation: a write carries its outcome, a read
/// none. Mirrors the eval spec PR's `ExtractedOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtractedOp {
    Write(WriteOutcome),
    Read,
}

impl ExtractedOp {
    pub fn kind(self) -> AccessKind {
        match self {
            Self::Write(_) => AccessKind::Write,
            Self::Read => AccessKind::Read,
        }
    }
}

/// One access a call implies, with its operation's outcome. The shape
/// `ExtractedAccess` takes once the eval spec PR merges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    pub op: ExtractedOp,
    pub locator: Locator,
    pub via: Extraction,
}

impl Classified {
    /// Today's spec shape, which carries no write outcome.
    pub fn into_spec(self) -> ExtractedAccess {
        ExtractedAccess {
            kind: self.op.kind(),
            locator: self.locator,
            via: self.via,
        }
    }
}

/// An access a call names, before its result is judged: the composite
/// extractor turns it into a [`Classified`] (a write with its outcome, a
/// read only when the result delivered).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub kind: AccessKind,
    pub locator: Locator,
    pub via: Extraction,
}

impl Candidate {
    pub fn read(locator: Locator, via: Extraction) -> Self {
        Self {
            kind: AccessKind::Read,
            locator,
            via,
        }
    }

    pub fn write(locator: Locator, via: Extraction) -> Self {
        Self {
            kind: AccessKind::Write,
            locator,
            via,
        }
    }
}
