//! From the spec's conversation read models to the view model: words,
//! canonical names and links for the head, each turn, its messages, parts
//! and marks. Pure: every lookup it needs is in a [`Labels`].

use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::ids::{AgentId, ConversationId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::reads::ForwardStatus;
use crosstalk_spec::interfaces::l8_surface::conversation::text::{BodyText, MessageText, TurnText};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    Inbound, IncrementHistory, MessageParts, MessagePlacement, OriginatedStatus, OutputSpan,
    PartKind, ReadBy, Reader, RelayedFrom, SpanOrigin, TransmissionMark, Turn, TurnContinuation,
    TurnMessage, TurnOutcome,
};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationHead, OriginLink, SpanPoint, SuccessorKind, TrafficSource, TurnIndex,
};
use crosstalk_spec::observed::client::IngressMode;
use crosstalk_spec::observed::exchange::{ExchangeFailure, StopReason, Transport};
use crosstalk_spec::observed::message::{MediaKind, Role, ToolOutcome};

use super::model::{
    BoundaryView, HeadView, InboundView, Link, MarkView, Marked, MessageView, OriginView,
    OriginatedView, OutcomeView, PartView, ReaderView, RelayedView, SpawnedBy, SuccessorView,
    TextView, Tone, TurnView, range_text, segments,
};
use crate::components::badge::Badge;
use crate::components::{format_bytes, format_time, short_id};
use crate::pages::common::links::{channel_url, conversation_url, span_url, transmission_url};
use crate::pages::common::lookup::AgentNames;
use crate::pages::common::transmissions::{ChannelNames, Named, route_channel, route_text};
use crate::pages::transmission::model::{carrier_label, kind_label};
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// What the mapping looks up.
pub struct Labels<'a> {
    pub names: &'a AgentNames,
    pub channels: &'a ChannelNames,
    pub state: &'a ViewState,
    /// The span the URL highlights.
    pub highlight: Option<SpanId>,
    /// The reader list's link for a span: this page with `hl` set.
    pub readers_url: &'a dyn Fn(SpanId) -> String,
}

impl Labels<'_> {
    fn named(&self, id: AgentId) -> Named {
        crate::pages::transmission::model::named(id, self.names, self.state)
    }

    fn conversation(&self, id: ConversationId, turn: Option<TurnIndex>) -> Link {
        Link {
            label: match turn {
                Some(turn) => format!("turn {} of {}", turn.0, short_id(id.to_ulid())),
                None => format!("conversation {}", short_id(id.to_ulid())),
            },
            url: conversation_url(id, turn.map(|t| t.0), self.state),
        }
    }

    fn transmission(&self, mark: &TransmissionMark) -> Link {
        Link {
            label: format!("transmission {}", short_id(mark.id.to_ulid())),
            url: transmission_url(mark.id, self.state),
        }
    }

    /// The turn a span sits in, or its redirect when not threaded.
    fn span_turn(&self, point: &SpanPoint) -> String {
        match point.turn {
            Some(turn) => conversation_url(turn.conversation, Some(turn.turn.0), self.state),
            None => span_url(point.span, self.state),
        }
    }
}

/// Every agent and channel a head and its turns name, for one name lookup
/// each.
pub fn referenced(
    head: &ConversationHead,
    turns: &[Turn],
) -> (Vec<AgentId>, Vec<crosstalk_spec::ids::ChannelId>) {
    let mut agents = vec![head.row.agent];
    let mut channels = Vec::new();
    match &head.row.origin {
        OriginLink::Root => {}
        OriginLink::Fork { parent_agent, .. } => agents.push(*parent_agent),
        OriginLink::Compaction {
            predecessor_agent, ..
        } => agents.push(*predecessor_agent),
    }
    agents.extend(head.successors.iter().map(|s| s.agent));
    if let Some(link) = &head.delegated_from {
        agents.push(link.parent.agent);
    }
    for turn in turns {
        agents.push(turn.agent);
        for message in turn.inputs.iter().chain(turn.output.iter()) {
            for (inbound, spans) in message_marks(message) {
                for i in inbound {
                    agents.push(i.origin.agent);
                    if let Some(m) = &i.transmission {
                        channels.extend(route_channel(&m.route));
                    }
                }
                for span in spans {
                    match &span.origin {
                        SpanOrigin::Originated { read_by, .. }
                        | SpanOrigin::Forwarded { read_by, .. } => {
                            for reader in read_by.first() {
                                agents.push(reader.agent);
                                if let Some(m) = &reader.transmission {
                                    channels.extend(route_channel(&m.route));
                                }
                            }
                        }
                        SpanOrigin::Relayed(RelayedFrom::Span(point)) => agents.push(point.agent),
                        SpanOrigin::Relayed(RelayedFrom::Input(_)) => {}
                    }
                }
            }
        }
    }
    agents.sort();
    agents.dedup();
    channels.sort();
    channels.dedup();
    (agents, channels)
}

/// Each part's marks, shown or dropped alike.
fn message_marks(message: &TurnMessage) -> Vec<(&[Inbound], &[OutputSpan])> {
    match &message.parts {
        MessageParts::Shown(parts) => parts
            .iter()
            .map(|p| (p.inbound.as_slice(), p.spans.as_slice()))
            .collect(),
        MessageParts::BodyDropped(parts) => parts
            .iter()
            .map(|p| (p.inbound.as_slice(), p.spans.as_slice()))
            .collect(),
    }
}

pub fn head_view(head: &ConversationHead, labels: &Labels) -> HeadView {
    let row = &head.row;
    let origin = match &row.origin {
        OriginLink::Root => OriginView::Root,
        OriginLink::Fork {
            parent,
            shared_prefix,
            branch_turn,
            ..
        } => OriginView::Fork {
            parent: labels.conversation(*parent, None),
            branch: branch_turn.map(|t| labels.conversation(*parent, Some(t))),
            shared: *shared_prefix,
        },
        OriginLink::Compaction {
            predecessor,
            carried_over,
            ..
        } => OriginView::Compaction {
            predecessor: labels.conversation(*predecessor, None),
            carried: *carried_over,
        },
    };
    let spawned_by = head.delegated_from.as_ref().map(|link| SpawnedBy {
        parent: labels.named(link.parent.agent),
        turn: match link.parent.turn {
            Some(turn) => labels.conversation(turn.conversation, Some(turn.turn)),
            None => Link {
                label: "the parent's span".to_owned(),
                url: span_url(link.parent.span, labels.state),
            },
        },
        transmission: Link {
            label: format!("transmission {}", short_id(link.transmission.to_ulid())),
            url: transmission_url(link.transmission, labels.state),
        },
    });
    let successors = head
        .successors
        .iter()
        .map(|next| SuccessorView {
            kind: match next.kind {
                SuccessorKind::Fork {
                    branch_turn: Some(turn),
                    ..
                } => format!("fork after turn {}", turn.0),
                SuccessorKind::Fork {
                    branch_turn: None, ..
                } => "fork".to_owned(),
                SuccessorKind::Compaction => "compaction".to_owned(),
            },
            link: labels.conversation(next.conversation, None),
            started: format_time(next.started_at),
        })
        .collect();
    HeadView {
        short: short_id(row.id.to_ulid()),
        full: row.id.to_ulid(),
        agent: labels.named(row.agent),
        claims: head
            .claims
            .entries()
            .iter()
            .map(|c| c.claim.clone())
            .collect(),
        started: format_time(row.started_at),
        last: format_time(row.last_turn_at),
        turns: row.turns,
        received: head.traffic.received,
        sent: head.traffic.sent,
        origin,
        spawned_by,
        successors,
        replayed: match &row.source {
            TrafficSource::Live => None,
            TrafficSource::Replay { corpus } => Some(corpus.0.clone()),
        },
    }
}

fn stop_text(stop: StopReason) -> &'static str {
    match stop {
        StopReason::EndTurn => "end of turn",
        StopReason::ToolUse => "tool use",
        StopReason::MaxTokens => "max tokens",
        StopReason::StopSequence => "stop sequence",
        StopReason::Refusal => "refusal",
        StopReason::Aborted => "aborted",
        StopReason::Other => "other",
    }
}

fn failure_text(failure: &ExchangeFailure) -> String {
    match failure {
        ExchangeFailure::Upstream { status } => format!("upstream answered {status}"),
        ExchangeFailure::UpstreamUnreachable => "upstream unreachable".to_owned(),
        ExchangeFailure::StreamTruncated => "the stream ended early".to_owned(),
        ExchangeFailure::MalformedStream { offset } => format!("malformed stream at byte {offset}"),
        ExchangeFailure::UpstreamErrorEvent => "the upstream sent an error event".to_owned(),
        ExchangeFailure::UnparseableResponse => "the response did not parse".to_owned(),
        ExchangeFailure::ClientDisconnected => "the client disconnected".to_owned(),
        ExchangeFailure::Timeout => "timed out".to_owned(),
    }
}

fn transport_text(transport: Transport) -> &'static str {
    match transport {
        Transport::Http => "http",
        Transport::Sse => "sse",
        Transport::WebSocket => "WebSocket",
    }
}

fn role_text(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

/// A part's kind in words and what it names.
fn kind_text(kind: &PartKind) -> (String, Option<String>) {
    match kind {
        PartKind::Text => ("text".into(), None),
        PartKind::Reasoning { visible: true } => ("reasoning".into(), None),
        PartKind::Reasoning { visible: false } => ("reasoning (opaque)".into(), None),
        PartKind::ToolCall { call, name, .. } => {
            ("tool call".into(), Some(format!("{} ({})", name.0, call.0)))
        }
        PartKind::ToolResult { call, outcome } => (
            match outcome {
                ToolOutcome::Success => "tool result".into(),
                ToolOutcome::Error => "tool result (error)".into(),
                ToolOutcome::Unknown => "tool result (outcome unknown)".into(),
            },
            Some(call.0.clone()),
        ),
        PartKind::Media(kind) => (
            match kind {
                MediaKind::Image => "image".into(),
                MediaKind::Audio => "audio".into(),
                MediaKind::Document => "document".into(),
            },
            None,
        ),
        PartKind::Unknown { kind } => ("unknown block".into(), Some(kind.clone())),
    }
}

fn inbound_view(inbound: &Inbound, labels: &Labels) -> InboundView {
    let route = inbound.transmission.as_ref().map(|m| &m.route);
    InboundView {
        from: labels.named(inbound.origin.agent),
        route: route.map(|r| route_text(r, labels.channels)),
        route_url: route
            .and_then(route_channel)
            .map(|c| channel_url(c, labels.state)),
        kind: kind_label(&inbound.kind),
        carrier: carrier_label(&inbound.carrier),
        matched: format_bytes(u64::from(inbound.matched_bytes.get())),
        range: range_text(&(inbound.range.start()..inbound.range.end())),
        sender_turn: Some(labels.span_turn(&inbound.origin)),
        transmission: inbound
            .transmission
            .as_ref()
            .map(|m| labels.transmission(m)),
        state: inbound
            .transmission
            .as_ref()
            .map(|m| m.state.label().to_owned()),
        delegation: match route {
            Some(Route::Delegation(DelegationDirection::ChildToParent)) => {
                Some("returned by sub-agent".to_owned())
            }
            Some(Route::Delegation(DelegationDirection::ParentToChild)) => {
                Some("task from parent".to_owned())
            }
            _ => None,
        },
    }
}

pub fn reader_view(reader: &Reader, labels: &Labels) -> ReaderView {
    let delegated = matches!(
        reader.transmission.as_ref().map(|m| &m.route),
        Some(Route::Delegation(DelegationDirection::ParentToChild))
    );
    ReaderView {
        agent: labels.named(reader.agent),
        turn: reader
            .turn
            .map(|t| conversation_url(t.conversation, Some(t.turn.0), labels.state)),
        transmission: reader.transmission.as_ref().map(|m| labels.transmission(m)),
        carrier: carrier_label(&reader.carrier),
        delegation: delegated.then(|| "delegated to sub-agent".to_owned()),
    }
}

fn read_by_view(
    span: &OutputSpan,
    status: String,
    read_by: &ReadBy,
    expired: bool,
    labels: &Labels,
) -> OriginatedView {
    OriginatedView {
        status,
        range: range_text(&(span.range.start()..span.range.end())),
        readers: read_by
            .first()
            .iter()
            .map(|r| reader_view(r, labels))
            .collect(),
        more: read_by.more(),
        more_url: (read_by.more() > 0).then(|| (labels.readers_url)(span.span)),
        expired,
        highlighted: labels.highlight == Some(span.span),
    }
}

fn span_mark(span: &OutputSpan, labels: &Labels) -> MarkView {
    match &span.origin {
        SpanOrigin::Originated { status, read_by } => {
            let (text, expired) = match status {
                OriginatedStatus::Pending => ("originated, not indexed yet".to_owned(), false),
                OriginatedStatus::Indexed { .. } => ("originated".to_owned(), false),
                OriginatedStatus::Propagated { hits, .. } => (
                    format!("originated, propagated ({} hits)", hits.get()),
                    false,
                ),
                OriginatedStatus::Expired { .. } => ("originated, expired".to_owned(), true),
            };
            MarkView::Originated(read_by_view(span, text, read_by, expired, labels))
        }
        SpanOrigin::Forwarded {
            status, read_by, ..
        } => {
            let (text, expired) = match status {
                ForwardStatus::Pending => {
                    ("forwarded from an input, not indexed yet".to_owned(), false)
                }
                ForwardStatus::Indexed { .. } => ("forwarded from an input".to_owned(), false),
                ForwardStatus::Expired { .. } => {
                    ("forwarded from an input, expired".to_owned(), true)
                }
            };
            MarkView::Originated(read_by_view(span, text, read_by, expired, labels))
        }
        SpanOrigin::Relayed(from) => MarkView::Relayed(RelayedView {
            range: range_text(&(span.range.start()..span.range.end())),
            source: match from {
                RelayedFrom::Span(point) => Some(Link {
                    label: format!("{}'s span", labels.names.name(point.agent)),
                    url: labels.span_turn(point),
                }),
                RelayedFrom::Input(_) => None,
            },
        }),
    }
}

/// The marked ranges of one part, for its text.
fn ranges(inbound: &[Inbound], spans: &[OutputSpan], highlight: Option<SpanId>) -> Vec<Marked> {
    let mut out: Vec<Marked> = inbound
        .iter()
        .map(|i| Marked {
            range: i.range.start()..i.range.end(),
            tone: Tone::Inbound,
        })
        .collect();
    for span in spans {
        let tone = match span.origin {
            SpanOrigin::Originated { .. } | SpanOrigin::Forwarded { .. } => Tone::Originated,
            SpanOrigin::Relayed(_) => Tone::Relayed,
        };
        out.push(Marked {
            range: span.range.start()..span.range.end(),
            tone,
        });
        if highlight == Some(span.span) {
            out.push(Marked {
                range: span.range.start()..span.range.end(),
                tone: Tone::Highlight,
            });
        }
    }
    out
}

fn message_view(
    message: &TurnMessage,
    text: Option<&MessageText>,
    content: bool,
    later_turn: bool,
    labels: &Labels,
) -> MessageView {
    let parts = match &message.parts {
        MessageParts::Shown(parts) => parts
            .iter()
            .map(|part| {
                let (kind, detail) = kind_text(&part.kind);
                let marks: Vec<MarkView> = part
                    .inbound
                    .iter()
                    .map(|i| MarkView::Inbound(Box::new(inbound_view(i, labels))))
                    .chain(part.spans.iter().map(|s| span_mark(s, labels)))
                    .collect();
                let text = if !content {
                    if part.text_bytes.is_some() {
                        TextView::Hidden
                    } else {
                        TextView::NoText
                    }
                } else {
                    match text.map(|t| &t.body) {
                        Some(BodyText::Shown(texts)) => {
                            match texts.get(usize::from(part.index)).cloned().flatten() {
                                Some(slice) => TextView::Shown {
                                    segments: segments(
                                        slice.from(),
                                        slice.text(),
                                        &ranges(&part.inbound, &part.spans, labels.highlight),
                                    ),
                                    remaining: slice.remaining(),
                                },
                                None => TextView::NoText,
                            }
                        }
                        Some(BodyText::BodyDropped) => TextView::Dropped,
                        None => TextView::Hidden,
                    }
                };
                PartView {
                    kind,
                    detail,
                    size: part.text_bytes.map(|b| format_bytes(u64::from(b))),
                    text,
                    marks,
                }
            })
            .collect(),
        MessageParts::BodyDropped(parts) => parts
            .iter()
            .map(|part| PartView {
                kind: format!("part {}", part.index),
                detail: None,
                size: None,
                text: TextView::Dropped,
                marks: part
                    .inbound
                    .iter()
                    .map(|i| MarkView::Inbound(Box::new(inbound_view(i, labels))))
                    .chain(part.spans.iter().map(|s| span_mark(s, labels)))
                    .collect(),
            })
            .collect::<Vec<_>>(),
    };
    let parts = if parts.is_empty() && matches!(message.parts, MessageParts::BodyDropped(_)) {
        vec![PartView {
            kind: "message".into(),
            detail: None,
            size: None,
            text: TextView::Dropped,
            marks: Vec::new(),
        }]
    } else {
        parts
    };
    MessageView {
        role: role_text(message.role).to_owned(),
        system_turn: later_turn && message.role == Role::System,
        parts,
    }
}

/// One turn, with its text when the page read it (`Content`).
pub fn turn_view(
    turn: &Turn,
    text: Option<&TurnText>,
    content: bool,
    head: &ConversationHead,
    labels: &Labels,
) -> TurnView {
    let mut boundaries = Vec::new();
    if turn.index.0 == 0 {
        match &head.row.origin {
            OriginLink::Compaction {
                predecessor,
                carried_over,
                ..
            } => boundaries.push(BoundaryView::Compaction {
                predecessor: labels.conversation(*predecessor, None),
                carried: *carried_over,
            }),
            OriginLink::Fork {
                parent,
                branch_turn,
                ..
            } => boundaries.push(BoundaryView::Fork {
                parent: labels.conversation(*parent, None),
                branch: branch_turn.map(|t| labels.conversation(*parent, Some(t))),
            }),
            OriginLink::Root => {}
        }
    }
    let connection = match &turn.continuation {
        TurnContinuation::Increment {
            connection,
            history,
        } => {
            let short = connection.map(|c| short_id(c.ulid_text()));
            if *history == IncrementHistory::Unseen {
                boundaries.push(BoundaryView::UnseenHistory {
                    connection: short.clone(),
                });
            }
            short
        }
        TurnContinuation::FullHistory => None,
    };
    let later = turn.index.0 > 0;
    let input_text = |i: usize| text.and_then(|t| t.inputs.get(i));
    let mut inputs = Vec::new();
    let mut carried = Vec::new();
    for (i, message) in turn.inputs.iter().enumerate() {
        let view = message_view(message, input_text(i), content, later, labels);
        if message.placement == MessagePlacement::CarriedOver {
            carried.push(view);
        } else {
            inputs.push(view);
        }
    }
    let output = turn.output.as_ref().map(|message| {
        message_view(
            message,
            text.and_then(|t| t.output.as_ref()),
            content,
            later,
            labels,
        )
    });
    TurnView {
        index: turn.index.0,
        time: format_time(turn.started_at),
        model: turn.model.0.clone(),
        transport: transport_text(turn.transport).to_owned(),
        connection,
        outcome: match &turn.outcome {
            TurnOutcome::Completed {
                finished_at, stop, ..
            } => OutcomeView::Completed {
                stop: stop_text(*stop).to_owned(),
                finished: format_time(*finished_at),
            },
            TurnOutcome::Failed { failed_at, failure } => OutcomeView::Failed {
                failure: failure_text(failure),
                at: format_time(*failed_at),
            },
        },
        usage: match &turn.outcome {
            TurnOutcome::Completed {
                usage: Some(usage), ..
            } => Some(format!("{} in · {} out", usage.input(), usage.output())),
            _ => None,
        },
        claim: turn.harness.clone(),
        agent: (turn.agent != head.row.agent).then(|| labels.named(turn.agent)),
        replayed: match &turn.ingress {
            IngressMode::Replay { corpus } => Some(corpus.0.clone()),
            _ => None,
        },
        boundaries,
        pending: !turn.provenance.marks_complete(),
        inputs,
        carried,
        output,
    }
}
