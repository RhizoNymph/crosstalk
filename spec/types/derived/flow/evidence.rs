//! Evidence that one agent communicated with another.

use std::time::Duration;

use crate::derived::provenance::matching::ContentMatch;
use crate::ids::AccessId;

#[derive(Debug, Clone, PartialEq)]
pub enum Evidence {
    /// The reader's input contains text the writer originated.
    Content(ContentMatch),
    /// The access pattern alone: see [`CoAccess`].
    CoAccess(CoAccess),
}

/// Agent A wrote a resource, then a different agent B read the same
/// resource, within the correlation window.
///
/// On its own this only makes a transmission suspected: B may have read
/// something unrelated, or A's text may be there but encoded, paraphrased or
/// truncated beyond what matching catches.
///
/// Invariants (checked by the correlator that builds it): the two accesses
/// are on the same resource, by different agents, the write is a
/// [`crate::derived::flow::access::AccessOp::Write`] and the read a `Read`,
/// and the write happened `lag` before the read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoAccess {
    pub write: AccessId,
    pub read: AccessId,
    pub lag: Duration,
}
