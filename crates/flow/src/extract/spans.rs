//! What a write wrote: the spans an `AccessOp::Write` carries, from L4's
//! spans of the writer's response.
//!
//! The spans are the writer's spans located in the call's part (its
//! arguments) that it originated or forwarded from an input, plus, for
//! every span there that provenance classified
//! `Relayed(RelaySource::Span(s))` where `s` is the writer's own earlier
//! span, the span `s`:
//!
//! - originated spans: what the writer wrote;
//! - spans relayed from an input (`Relayed(RelaySource::Input(_))`), such
//!   as a document the writer fetched and now posts to a channel:
//!   forwarding counts as writing (`flow.access.write-spans-include-forwarded-input`).
//!   Provenance indexes such a span as authored by the relaying agent, so a
//!   reader's match on it names the writer and links to this write;
//! - the writer's own relayed sources (the eval spec's
//!   `flow.access.write-spans-include-self-relay`): a retry of a rejected
//!   write relays the rejected attempt's text, and a reader's match on it
//!   must link to the retry too.
//!
//! Spans relayed from another agent's indexed span stay out; the match
//! belongs to its originator.
//!
//! L4's spans arrive as an input: the flow consumer passes the writer's
//! spans of the exchange and a lookup of a relay source's agent. Reading
//! them through the spec's `SpanIndex::spans` is not wired into the
//! consumer yet.

use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, Span};
use crosstalk_spec::ids::{AgentId, SpanId};
use crosstalk_spec::observed::message::PartRef;

use crate::extract::op::ExtractedOp;

/// The spans a write by `writer` through the tool call at `call` carries,
/// in the order of `spans`, each once. `source_agent` names the agent whose
/// output holds a relay source span, `None` when unknown (then it is left
/// out).
pub fn write_spans(
    call: PartRef,
    writer: AgentId,
    spans: &[Span],
    source_agent: impl Fn(SpanId) -> Option<AgentId>,
) -> Vec<SpanId> {
    let mut written: Vec<SpanId> = Vec::new();
    for span in spans {
        if span.location.part != call || span.agent != writer {
            continue;
        }
        let carried = match span.state.origin() {
            Some(Origin::Originated | Origin::Relayed(RelaySource::Input(_))) => Some(span.id),
            Some(Origin::Relayed(RelaySource::Span(source)))
                if source_agent(source) == Some(writer) =>
            {
                Some(source)
            }
            Some(Origin::Relayed(RelaySource::Span(_)) | Origin::Common) | None => None,
        };
        if let Some(id) = carried
            && !written.contains(&id)
        {
            written.push(id);
        }
    }
    written
}

/// Why an access operation cannot be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccessOpError {
    #[error("a read needs the tool result part that returned the content")]
    ReadWithoutResult,
}

/// The stored operation of an extracted access: a write names the call
/// part and carries `spans` ([`write_spans`]); a read names the result
/// part, with the write's outcome.
pub fn access_op(
    op: ExtractedOp,
    call: PartRef,
    result: Option<PartRef>,
    spans: Vec<SpanId>,
) -> Result<AccessOp, AccessOpError> {
    match op {
        ExtractedOp::Write(outcome) => Ok(AccessOp::Write {
            call,
            spans,
            outcome,
        }),
        ExtractedOp::Read => result
            .map(|result| AccessOp::Read { result })
            .ok_or(AccessOpError::ReadWithoutResult),
    }
}
