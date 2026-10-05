//! Evidence that one agent communicated with another.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::derived::flow::access::{Access, AccessOp};
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AccessId, AgentId};
use crate::wire::Rejected;

#[derive(Debug, Clone, PartialEq)]
pub enum Evidence {
    /// The reader's input contains text the writer originated.
    Content(ContentMatch),
    /// The access pattern alone: see [`CoAccess`].
    CoAccess(CoAccess),
}

/// Agent A wrote a resource, then a different agent B read the same
/// resource, within the correlation window. A's write is one that pairs: a
/// rejected write never becomes a co-access (`flow.coaccess.write-not-rejected`);
/// an `Unknown` one does, at the lower confidence its outcome records.
///
/// On its own this only makes a transmission suspected: B may have read
/// something unrelated, or A's text may be there but encoded, paraphrased or
/// truncated beyond what matching catches.
///
/// Built only through [`CoAccess::new`], which checks the two accesses.
///
/// It names the writer (the write access's agent, as attributed), so a
/// transmission backed only by co-accesses says who its senders were
/// without looking the accesses up: what
/// [`Transmission::crossing`](crate::derived::flow::transmission::Transmission::crossing)
/// and a channel's transmission rows read.
///
/// On the wire, `{"write": .., "writer": .., "read": .., "lag_micros":
/// 30000000}`: the lag in whole microseconds ([`crate::wire::duration`]).
/// Decoding cannot rerun [`CoAccess::new`], whose checks read the two
/// accesses (their resources, agents, operations and times) and the
/// correlation window, none of which the value holds. It checks what the value can know about itself: the
/// write and the read are two accesses (`WrongOperations`, since one access
/// is not both a write and a read), and the lag is positive
/// (`ReadNotAfterWrite`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawCoAccess")]
pub struct CoAccess {
    write: AccessId,
    writer: AgentId,
    read: AccessId,
    #[serde(with = "crate::wire::duration")]
    lag_micros: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCoAccess {
    DifferentResources,
    SameAgent,
    /// `write` is not a write, or `read` is not a read.
    WrongOperations,
    /// `write` is a write whose outcome is `WriteOutcome::Rejected`: it
    /// delivered nothing, so no read can have received it.
    RejectedWrite,
    ReadNotAfterWrite,
    OutsideWindow,
}

/// [`CoAccess`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawCoAccess {
    write: AccessId,
    writer: AgentId,
    read: AccessId,
    #[serde(with = "crate::wire::duration")]
    lag_micros: Duration,
}

impl TryFrom<RawCoAccess> for CoAccess {
    type Error = Rejected<InvalidCoAccess>;

    fn try_from(raw: RawCoAccess) -> Result<Self, Self::Error> {
        if raw.write == raw.read {
            return Err(Rejected::new("co-access", InvalidCoAccess::WrongOperations));
        }
        if raw.lag_micros.is_zero() {
            return Err(Rejected::new(
                "co-access",
                InvalidCoAccess::ReadNotAfterWrite,
            ));
        }
        Ok(Self {
            write: raw.write,
            writer: raw.writer,
            read: raw.read,
            lag_micros: raw.lag_micros,
        })
    }
}

impl CoAccess {
    pub fn new(write: &Access, read: &Access, window: Duration) -> Result<Self, InvalidCoAccess> {
        if write.resource != read.resource {
            return Err(InvalidCoAccess::DifferentResources);
        }
        if write.agent == read.agent {
            return Err(InvalidCoAccess::SameAgent);
        }
        let outcome = match (&write.op, &read.op) {
            (AccessOp::Write { outcome, .. }, AccessOp::Read { .. }) => *outcome,
            _ => return Err(InvalidCoAccess::WrongOperations),
        };
        if !outcome.pairs() {
            return Err(InvalidCoAccess::RejectedWrite);
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
            writer: write.agent,
            read: read.id,
            lag_micros: lag,
        })
    }

    pub fn write(&self) -> AccessId {
        self.write
    }

    /// The agent the write was attributed to: the sender this co-access
    /// names. Resolved through merges by readers, at read time.
    pub fn writer(&self) -> AgentId {
        self.writer
    }

    pub fn read(&self) -> AccessId {
        self.read
    }

    /// How long after the write the read was, in whole microseconds.
    pub fn lag(&self) -> Duration {
        self.lag_micros
    }
}
