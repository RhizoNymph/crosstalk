//! What a write wrote: the spans an `AccessOp::Write` carries, from L4's
//! spans of the writer's response.
//!
//! The spans are the originated spans located in the call's part (its
//! arguments), plus, for every span there that provenance classified
//! `Relayed(RelaySource::Span(s))` where `s` is the writer's own earlier
//! span, the span `s` (the eval spec's
//! `flow.access.write-spans-include-self-relay`): a retry of a rejected
//! write relays the rejected attempt's text, and a reader's match on it
//! must link to the retry too. Spans relayed from another agent's output
//! stay out; the match belongs to its originator.
//!
//! L4's spans arrive as an input: the flow consumer passes the writer's
//! spans of the exchange and a lookup of a relay source's agent. Reading
//! them through the spec's `SpanIndex::spans` is not wired into the
//! consumer yet.

use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, Span};
use crosstalk_spec::ids::{AgentId, SpanId};
use crosstalk_spec::observed::message::PartRef;

use crate::extract::op::{ExtractedOp, WritePayload};

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
            Some(Origin::Originated) => Some(span.id),
            Some(Origin::Relayed(RelaySource::Span(source)))
                if source_agent(source) == Some(writer) =>
            {
                Some(source)
            }
            Some(Origin::Relayed(_) | Origin::Common) | None => None,
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
/// part and carries `spans` ([`write_spans`]), none when its content is
/// not in the call (`WritePayload::Unseen`); a read names the result part.
pub fn access_op(
    op: ExtractedOp,
    call: PartRef,
    result: Option<PartRef>,
    spans: Vec<SpanId>,
) -> Result<AccessOp, AccessOpError> {
    match op {
        ExtractedOp::Write {
            outcome,
            payload: WritePayload::CallArguments,
        } => Ok(AccessOp::Write {
            call,
            spans,
            outcome,
        }),
        ExtractedOp::Write {
            outcome,
            payload: WritePayload::Unseen,
        } => Ok(AccessOp::Write {
            call,
            spans: Vec::new(),
            outcome,
        }),
        ExtractedOp::Read => result
            .map(|result| AccessOp::Read { result })
            .ok_or(AccessOpError::ReadWithoutResult),
    }
}
