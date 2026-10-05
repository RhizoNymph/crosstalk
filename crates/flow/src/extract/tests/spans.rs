//! The spans a write carries, and the stored access operation.

use proptest::prelude::*;

use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::derived::provenance::span::{RelaySource, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, ByteRange, Timestamp};

use crate::extract::{AccessOpError, ExtractedOp, WriteOutcome, access_op, write_spans};

pub fn writer() -> AgentId {
    AgentId::from_ulid(1)
}

fn other() -> AgentId {
    AgentId::from_ulid(2)
}

fn message() -> MessageHash {
    MessageHash::from_digest(Blake3::from_bytes([7; 32]))
}

pub fn call_part() -> PartRef {
    PartRef {
        message: message(),
        index: 3,
    }
}

fn text_part() -> PartRef {
    PartRef {
        message: message(),
        index: 1,
    }
}

/// Source spans 100..110 are the writer's, 110..120 another agent's, the
/// rest unknown.
pub fn agent_of(span: SpanId) -> Option<AgentId> {
    match span.as_ulid() {
        100..110 => Some(writer()),
        110..120 => Some(other()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Extracted,
    Common,
    Originated,
    Indexed,
    RelayedSpan {
        source: SpanId,
        source_by_writer: bool,
    },
    RelayedInput,
}

#[derive(Debug, Clone)]
pub struct DraftSpan {
    pub n: u128,
    pub in_call: bool,
    pub by_writer: bool,
    pub kind: Kind,
}

impl DraftSpan {
    pub fn id(&self) -> SpanId {
        SpanId::from_ulid(self.n)
    }

    pub fn span(&self) -> Span {
        let state = match self.kind {
            Kind::Extracted => SpanState::Extracted,
            Kind::Common => SpanState::Common,
            Kind::Originated => SpanState::Originated,
            Kind::Indexed => SpanState::Indexed {
                at: Timestamp::from_micros(1),
            },
            Kind::RelayedSpan { source, .. } => SpanState::Relayed {
                source: RelaySource::Span(source),
            },
            Kind::RelayedInput => SpanState::Relayed {
                source: RelaySource::Input(message()),
            },
        };
        span(
            self.n,
            if self.in_call {
                call_part()
            } else {
                text_part()
            },
            if self.by_writer { writer() } else { other() },
            state,
        )
    }
}

fn span(n: u128, part: PartRef, agent: AgentId, state: SpanState) -> Span {
    Span {
        id: SpanId::from_ulid(n),
        location: SpanLocation {
            part,
            range: ByteRange::new(0, 4).expect("non-empty"),
        },
        agent,
        exchange: ExchangeId::from_ulid(9),
        state,
    }
}

pub fn span_draft() -> impl Strategy<Value = DraftSpan> {
    let kind = prop_oneof![
        Just(Kind::Extracted),
        Just(Kind::Common),
        Just(Kind::Originated),
        Just(Kind::Indexed),
        Just(Kind::RelayedInput),
        (100u128..125).prop_map(|n| Kind::RelayedSpan {
            source: SpanId::from_ulid(n),
            source_by_writer: (100..110).contains(&n),
        }),
    ];
    (0u128..8, any::<bool>(), any::<bool>(), kind).prop_map(|(n, in_call, by_writer, kind)| {
        DraftSpan {
            n,
            in_call,
            by_writer,
            kind,
        }
    })
}

#[test]
fn write_spans_are_the_calls_originated_and_self_relayed_spans() {
    let spans = vec![
        span(1, call_part(), writer(), SpanState::Originated),
        span(2, text_part(), writer(), SpanState::Originated),
        span(
            3,
            call_part(),
            writer(),
            SpanState::Relayed {
                source: RelaySource::Span(SpanId::from_ulid(101)),
            },
        ),
        span(
            4,
            call_part(),
            writer(),
            SpanState::Relayed {
                source: RelaySource::Span(SpanId::from_ulid(111)),
            },
        ),
        span(5, call_part(), writer(), SpanState::Common),
        span(
            6,
            call_part(),
            writer(),
            SpanState::Expired {
                at: Timestamp::from_micros(9),
            },
        ),
        span(
            7,
            call_part(),
            writer(),
            SpanState::Relayed {
                source: RelaySource::Span(SpanId::from_ulid(101)),
            },
        ),
    ];
    assert_eq!(
        write_spans(call_part(), writer(), &spans, agent_of),
        vec![
            SpanId::from_ulid(1),
            SpanId::from_ulid(101),
            SpanId::from_ulid(6)
        ],
    );
}

#[test]
fn access_operations() {
    let result = PartRef {
        message: message(),
        index: 0,
    };
    assert_eq!(
        access_op(
            ExtractedOp::write(WriteOutcome::Rejected),
            call_part(),
            None,
            vec![SpanId::from_ulid(1)],
        ),
        Ok(AccessOp::Write {
            call: call_part(),
            spans: vec![SpanId::from_ulid(1)],
            outcome: WriteOutcome::Rejected,
        }),
    );
    assert_eq!(
        access_op(ExtractedOp::Read, call_part(), Some(result), vec![]),
        Ok(AccessOp::Read { result }),
    );
    assert_eq!(
        access_op(ExtractedOp::Read, call_part(), None, vec![]),
        Err(AccessOpError::ReadWithoutResult),
    );
}
