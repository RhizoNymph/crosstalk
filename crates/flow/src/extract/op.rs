//! What an extraction yields, with each write's outcome: the spec's
//! [`WriteOutcome`] and [`ExtractedOp`], carried by [`Classified`] into an
//! [`ExtractedAccess`].

pub use crosstalk_spec::derived::flow::access::WriteOutcome;
use crosstalk_spec::derived::flow::access::{AccessKind, Extraction};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::interfaces::l5_flow::ExtractedAccess;
pub use crosstalk_spec::interfaces::l5_flow::{ExtractedOp, WritePayload};

use crate::extract::outcome::CommandRule;

/// One access a call implies, with its operation's outcome: the fields of
/// an `ExtractedAccess`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    pub op: ExtractedOp,
    pub locator: Locator,
    pub via: Extraction,
}

impl Classified {
    /// The spec's shape, the write's outcome included.
    pub fn into_spec(self) -> ExtractedAccess {
        ExtractedAccess {
            op: self.op,
            locator: self.locator,
            via: self.via,
        }
    }
}

/// An access a call names, before its result is judged: the composite
/// extractor turns it into a [`Classified`] (a write with its outcome, a
/// read only when the result delivered).
///
/// `payload` is where a write's content is (a read ignores it); `rule` is
/// the command whose output judges the result, for an access a shell
/// command makes (`outcome::CommandRule`), `None` for the tool's own rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub kind: AccessKind,
    pub locator: Locator,
    pub via: Extraction,
    pub payload: WritePayload,
    pub rule: Option<CommandRule>,
}

impl Candidate {
    pub fn read(locator: Locator, via: Extraction) -> Self {
        Self {
            kind: AccessKind::Read,
            locator,
            via,
            payload: WritePayload::CallArguments,
            rule: None,
        }
    }

    pub fn write(locator: Locator, via: Extraction) -> Self {
        Self {
            kind: AccessKind::Write,
            locator,
            via,
            payload: WritePayload::CallArguments,
            rule: None,
        }
    }

    /// This write's content is not in the call (`git push`).
    pub fn unseen(mut self) -> Self {
        self.payload = WritePayload::Unseen;
        self
    }

    /// The command whose output judges this access's result.
    pub fn judged_by(mut self, rule: CommandRule) -> Self {
        self.rule = Some(rule);
        self
    }
}
