//! What the extraction step hands the flow consumer.
//!
//! The spec has no event for an extracted access: the flow consumer turns
//! the tool calls and results of each `ConversationDelta` into accesses
//! with a `ResourceExtractor`, and records them itself. Until that step is
//! wired, its output reaches the consumer as [`Extracted`] over a channel:
//! one tool call's accesses, with the context the spec's `Access` needs
//! (agent, exchange, time) and the locator still unresolved to a resource.

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, SpanId};
use crosstalk_spec::observed::message::{PartRef, ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use crate::correlate::pairing::WriteOutcome;

/// One extracted access before its locator is resolved to a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed<Op> {
    /// Minted by the extraction step, once per access.
    pub id: AccessId,
    /// The agent the exchange was attributed to.
    pub agent: AgentId,
    pub exchange: ExchangeId,
    /// The exchange's start.
    pub at: Timestamp,
    pub locator: Locator,
    pub via: Extraction,
    pub op: Op,
}

/// A write: the tool call part and the spans its arguments hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteCall {
    pub call: PartRef,
    pub spans: Vec<SpanId>,
}

/// A read: the tool result part that returned the resource's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadResult {
    pub result: PartRef,
}

/// One input from the extraction step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Extracted {
    /// A read, extracted with its result (reads are only recorded with
    /// one).
    Read(Observed<ReadResult>),
    /// A write tool call. `outcome` is `None` while its result has not
    /// arrived: the consumer holds it until [`Extracted::WriteResult`] or
    /// the settle window closes, when it is released as `Unknown`.
    Write {
        write: Observed<WriteCall>,
        outcome: Option<WriteOutcome>,
    },
    /// The result of a held write arrived, classified. Ignored once the
    /// write was released: an access is recorded once.
    WriteResult {
        access: AccessId,
        outcome: WriteOutcome,
    },
    /// A tool call an agent made: the name a `Direct(ToolResult)`
    /// transmission for its result carries.
    ToolCall {
        agent: AgentId,
        call: ToolCallId,
        name: ToolName,
        at: Timestamp,
    },
}

/// Why the flow consumer did not confirm a batch of extracted inputs
/// durable. Either way the caller keeps its own input (the delta) unacked
/// and retries it: every input is idempotent at the consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotDurable {
    #[error("the flow consumer has stopped")]
    Stopped,
    /// A step failed transiently (a store unavailable) before every input
    /// of the batch was recorded.
    #[error("the flow consumer could not record every input yet")]
    Backlogged,
}

/// Where the extraction step hands its inputs: the flow consumer's side of
/// the L4 seam. `deliver` returns `Ok` once the consumer holds the inputs
/// as durably as it holds anything (recorded accesses, held writes and tool
/// calls in its stores), so the caller may then commit its own progress
/// and ack its delta (`docs/features/postgres_stores.md`, "L5: flow
/// checkpoint and restore").
pub trait FlowInputs: Send {
    fn deliver(
        &mut self,
        inputs: Vec<Extracted>,
    ) -> impl Future<Output = Result<(), NotDurable>> + Send;
}

/// A volatile consumer's inputs (memory mode): handed over in order and
/// confirmed at once; durable only as long as the process lives.
impl FlowInputs for UnboundedSender<Extracted> {
    async fn deliver(&mut self, inputs: Vec<Extracted>) -> Result<(), NotDurable> {
        for input in inputs {
            self.send(input).map_err(|_| NotDurable::Stopped)?;
        }
        Ok(())
    }
}
