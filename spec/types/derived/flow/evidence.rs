//! Evidence that one agent communicated with another.

use std::time::Duration;

use crate::derived::flow::access::{Access, AccessOp};
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
/// Built only through [`CoAccess::new`], which checks the two accesses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoAccess {
    write: AccessId,
    read: AccessId,
    lag: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCoAccess {
    DifferentResources,
    SameAgent,
    /// `write` is not a write, or `read` is not a read.
    WrongOperations,
    ReadNotAfterWrite,
    OutsideWindow,
}

impl CoAccess {
    pub fn new(write: &Access, read: &Access, window: Duration) -> Result<Self, InvalidCoAccess> {
        if write.resource != read.resource {
            return Err(InvalidCoAccess::DifferentResources);
        }
        if write.agent == read.agent {
            return Err(InvalidCoAccess::SameAgent);
        }
        if !matches!(write.op, AccessOp::Write { .. }) || !matches!(read.op, AccessOp::Read { .. })
        {
            return Err(InvalidCoAccess::WrongOperations);
        }
        if read.at <= write.at {
            return Err(InvalidCoAccess::ReadNotAfterWrite);
        }
        let lag = Duration::from_micros(read.at.as_micros() - write.at.as_micros());
        if lag > window {
            return Err(InvalidCoAccess::OutsideWindow);
        }
        Ok(Self {
            write: write.id,
            read: read.id,
            lag,
        })
    }

    pub fn write(&self) -> AccessId {
        self.write
    }

    pub fn read(&self) -> AccessId {
        self.read
    }

    pub fn lag(&self) -> Duration {
        self.lag
    }
}
