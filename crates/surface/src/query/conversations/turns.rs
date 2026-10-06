//! One window of a conversation's turns: L3's stored turns, each turn's
//! exchange record (L1), its message bodies (the blob store, structure
//! only), and L4's marks with what they point at.
//!
//! A turn's inputs are its non-output transcript entries in ordinal order
//! and its output the output entry (`surface.conversation.inputs-are-transcript`,
//! `surface.conversation.output-is-response`). Each message's parts come
//! from its body; each part lists the content matches read in it and, on
//! the output, the spans cut from it, both ordered by range start. A body
//! the blob store no longer holds keeps its marks by part
//! (`surface.conversation.body-dropped`).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::{RelaySource, SpanState};
use crosstalk_spec::ids::{ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{ExchangeReads, StoredExchange};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    StoredTurn, ThreadOutcomeKind, TranscriptEntry,
};
use crosstalk_spec::interfaces::l4_provenance::reads::{ProvenanceReads, ScanStatus, StoredSpan};
use crosstalk_spec::interfaces::l5_flow::transmissions::MatchKey;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    Inbound, IncrementHistory, MessageParts, MessagePlacement, OriginatedStatus, OutputSpan,
    PartKind, PartMarks, PartShape, ReadBy, RelayedFrom, SpanOrigin, Turn, TurnContinuation,
    TurnMessage, TurnOutcome,
};
use crosstalk_spec::observed::exchange::{Continuation, ExchangeOutcome};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, PartRef, Reasoning, SystemPart, UserPart, encoding,
};
use crosstalk_spec::paging::{PageRequest, PageSize};

use super::marks::{Pointers, missing};
use crate::service::Surface;
use crate::stores::SurfaceStores;

/// A window's turns with their exchange records and message bodies.
pub(super) struct Window {
    pub(super) turns: Vec<StoredTurn>,
    pub(super) exchanges: BTreeMap<ExchangeId, StoredExchange>,
    /// Each message's body; `None` when the blob store no longer holds it.
    pub(super) bodies: BTreeMap<MessageHash, Option<Message>>,
}

impl Window {
    pub(super) fn exchange_ids(&self) -> BTreeSet<ExchangeId> {
        self.turns.iter().map(|turn| turn.exchange).collect()
    }

    /// The body of `message`, if still stored.
    pub(super) fn body(&self, message: MessageHash) -> Option<&Message> {
        self.bodies.get(&message).and_then(Option::as_ref)
    }
}

/// A turn's inputs (its non-output entries, by ordinal) and its output.
pub(super) fn split(turn: &StoredTurn) -> (Vec<&TranscriptEntry>, Option<&TranscriptEntry>) {
    let inputs = turn.entries.iter().filter(|entry| !entry.output).collect();
    let output = turn.entries.iter().find(|entry| entry.output);
    (inputs, output)
}

fn placement(entry: &TranscriptEntry) -> MessagePlacement {
    if entry.output {
        MessagePlacement::Output
    } else if entry.carried_over {
        MessagePlacement::CarriedOver
    } else {
        MessagePlacement::New
    }
}

/// What each part of `body` is, in part order.
fn part_kinds(body: &MessageBody) -> Vec<PartKind> {
    fn unknown(kind: &str) -> PartKind {
        PartKind::Unknown {
            kind: kind.to_owned(),
        }
    }
    match body {
        MessageBody::System(parts) => parts
            .iter()
            .map(|part| match part {
                SystemPart::Text(_) => PartKind::Text,
                SystemPart::Unknown(block) => unknown(&block.kind),
            })
            .collect(),
        MessageBody::User(parts) => parts
            .iter()
            .map(|part| match part {
                UserPart::Text(_) => PartKind::Text,
                UserPart::Media(media) => PartKind::Media(media.kind),
                UserPart::Unknown(block) => unknown(&block.kind),
            })
            .collect(),
        MessageBody::Assistant(parts) => parts
            .iter()
            .map(|part| match part {
                AssistantPart::Text(_) => PartKind::Text,
                AssistantPart::Reasoning(Reasoning::Visible { .. }) => {
                    PartKind::Reasoning { visible: true }
                }
                AssistantPart::Reasoning(Reasoning::Opaque { .. }) => {
                    PartKind::Reasoning { visible: false }
                }
                AssistantPart::ToolCall(call) => PartKind::ToolCall {
                    call: call.id.clone(),
                    name: call.name.clone(),
                    execution: call.execution,
                },
                AssistantPart::ServerToolResult(result) => PartKind::ToolResult {
                    call: result.call_id.clone(),
                    outcome: result.outcome,
                },
                AssistantPart::Unknown(block) => unknown(&block.kind),
            })
            .collect(),
        MessageBody::Tool(results) => results
            .iter()
            .map(|result| PartKind::ToolResult {
                call: result.call_id.clone(),
                outcome: result.outcome,
            })
            .collect(),
    }
}

fn continuation(exchange: &StoredExchange, turn: &StoredTurn) -> TurnContinuation {
    match &exchange.exchange.continuation {
        Continuation::FullHistory => TurnContinuation::FullHistory,
        Continuation::Increment { connection, .. } => TurnContinuation::Increment {
            connection: *connection,
            history: if turn.outcome == ThreadOutcomeKind::Starts {
                IncrementHistory::Unseen
            } else {
                IncrementHistory::Resolved
            },
        },
    }
}

fn outcome(exchange: &StoredExchange) -> TurnOutcome {
    match &exchange.exchange.outcome {
        ExchangeOutcome::Completed {
            finished_at,
            stop,
            usage,
            ..
        } => TurnOutcome::Completed {
            finished_at: *finished_at,
            stop: *stop,
            usage: *usage,
        },
        ExchangeOutcome::Failed {
            failed_at, failure, ..
        } => TurnOutcome::Failed {
            failed_at: *failed_at,
            failure: *failure,
        },
    }
}

/// L4's marks of a window and what they point at.
struct Marks {
    status: BTreeMap<ExchangeId, ScanStatus>,
    matches: BTreeMap<ExchangeId, Vec<ContentMatch>>,
    spans: BTreeMap<ExchangeId, Vec<StoredSpan>>,
    /// The inline readers of each indexed output span, and their total.
    readers: BTreeMap<SpanId, (u32, Vec<ContentMatch>)>,
    pointers: Pointers,
}

impl Marks {
    fn inbound(&self, content: &ContentMatch) -> Result<Inbound, QueryError> {
        Ok(Inbound {
            range: content.read_at().range,
            matched_bytes: content.matched_bytes(),
            kind: content.kind().clone(),
            carrier: content.carrier().clone(),
            origin: self.pointers.point(content.origin())?,
            transmission: self.pointers.marks.get(&MatchKey::of(content)).cloned(),
        })
    }

    fn read_by(&self, span: SpanId, aliases: impl Aliases + Copy) -> Result<ReadBy, QueryError> {
        let Some((total, first)) = self.readers.get(&span) else {
            return Ok(ReadBy::none());
        };
        let readers = first
            .iter()
            .map(|content| self.pointers.reader(content, aliases))
            .collect();
        ReadBy::new(readers, *total).map_err(|error| QueryError::Store {
            reason: format!("readers of {} do not fit: {error:?}", span.ulid_text()),
        })
    }

    fn output_span(
        &self,
        stored: &StoredSpan,
        aliases: impl Aliases + Copy,
    ) -> Result<OutputSpan, QueryError> {
        let span = &stored.span;
        let origin = match (&span.state, stored.forward) {
            (
                SpanState::Relayed {
                    source: RelaySource::Input(input),
                },
                Some(status),
            ) => SpanOrigin::Forwarded {
                input: *input,
                status,
                read_by: self.read_by(span.id, aliases)?,
            },
            (
                SpanState::Relayed {
                    source: RelaySource::Input(input),
                },
                None,
            ) => SpanOrigin::Relayed(RelayedFrom::Input(*input)),
            (
                SpanState::Relayed {
                    source: RelaySource::Span(source),
                },
                _,
            ) => SpanOrigin::Relayed(RelayedFrom::Span(self.pointers.point(*source)?)),
            (state, _) => match OriginatedStatus::of(state) {
                Some(status) => SpanOrigin::Originated {
                    status,
                    read_by: self.read_by(span.id, aliases)?,
                },
                None => {
                    return Err(missing(format!(
                        "span {} is listed in its output as {state:?}",
                        span.id.ulid_text()
                    )));
                }
            },
        };
        Ok(OutputSpan {
            span: span.id,
            range: span.location.range,
            origin,
        })
    }
}

/// Whether L4 indexes a span under its author, so later readers match it.
fn has_readers(stored: &StoredSpan) -> bool {
    stored.forward.is_some() || OriginatedStatus::of(&stored.span.state).is_some()
}

impl<S: SurfaceStores> Surface<S> {
    /// The exchange records and message bodies of `turns`.
    pub(super) async fn window(&self, turns: Vec<StoredTurn>) -> Result<Window, QueryError> {
        let ids: BTreeSet<ExchangeId> = turns.iter().map(|turn| turn.exchange).collect();
        let mut exchanges = BTreeMap::new();
        let ids: Vec<ExchangeId> = ids.into_iter().collect();
        for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied())?;
            exchanges.extend(self.stores.exchanges().exchanges(&batch).await?);
        }
        if let Some(turn) = turns
            .iter()
            .find(|turn| !exchanges.contains_key(&turn.exchange))
        {
            return Err(missing(format!(
                "turn {} names exchange {}, which L1 does not keep",
                turn.index.0,
                turn.exchange.ulid_text()
            )));
        }
        let messages: BTreeSet<MessageHash> = turns
            .iter()
            .flat_map(|turn| turn.entries.iter().map(|entry| entry.message))
            .collect();
        let mut bodies = BTreeMap::new();
        for hash in messages {
            let body = match self.stores.blobs().get(hash).await? {
                None => None,
                Some(bytes) => {
                    let body = encoding::decode(&bytes).map_err(|error| QueryError::Store {
                        reason: format!("body {hash:?} does not decode: {error:?}"),
                    })?;
                    Some(encoding::message(body))
                }
            };
            bodies.insert(hash, body);
        }
        Ok(Window {
            turns,
            exchanges,
            bodies,
        })
    }

    async fn marks(&self, window: &Window) -> Result<Marks, QueryError> {
        let ids: Vec<ExchangeId> = window.exchange_ids().into_iter().collect();
        let mut status = BTreeMap::new();
        let mut matches = BTreeMap::new();
        let mut spans = BTreeMap::new();
        for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied())?;
            let provenance = self.stores.provenance();
            status.extend(provenance.scan_status(&batch).await?);
            matches.extend(provenance.matches_read_in(&batch).await?);
            spans.extend(provenance.output_spans(&batch).await?);
        }
        let inline = PageSize::new(ReadBy::INLINE as u16).map_err(|error| QueryError::Store {
            reason: format!("inline readers page refused: {error:?}"),
        })?;
        let mut readers = BTreeMap::new();
        let mut relay_sources = BTreeSet::new();
        for stored in spans.values().flatten() {
            if let SpanState::Relayed {
                source: RelaySource::Span(source),
            } = stored.span.state
            {
                relay_sources.insert(source);
            }
            if !has_readers(stored) {
                continue;
            }
            let page = PageRequest {
                size: inline,
                after: None,
            };
            if let Some(read) = self.reader_page(stored.span.id, &page).await? {
                let (first, _) = read.page.into_parts();
                readers.insert(stored.span.id, (read.total, first));
            }
        }
        let pointed: Vec<&ContentMatch> = matches
            .values()
            .flatten()
            .chain(readers.values().flat_map(|(_, first)| first.iter()))
            .collect();
        let pointers = self.pointers(&pointed, relay_sources).await?;
        Ok(Marks {
            status,
            matches,
            spans,
            readers,
            pointers,
        })
    }

    /// The turns of `window` with their structure and marks.
    pub(super) async fn turns_of(
        &self,
        conversation: ConversationId,
        window: &Window,
    ) -> Result<Vec<Turn>, QueryError> {
        let marks = self.marks(window).await?;
        let aliases = self.aliases();
        let mut turns = Vec::with_capacity(window.turns.len());
        for turn in &window.turns {
            let exchange = window.exchanges.get(&turn.exchange).ok_or_else(|| {
                missing(format!(
                    "turn {} of {} has no exchange record",
                    turn.index.0,
                    conversation.ulid_text()
                ))
            })?;
            let read_in = marks
                .matches
                .get(&turn.exchange)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let cut = marks
                .spans
                .get(&turn.exchange)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let message = |entry: &TranscriptEntry| -> Result<TurnMessage, QueryError> {
                let on = |part: PartRef| part.message == entry.message;
                let mut inbound: Vec<&ContentMatch> = read_in
                    .iter()
                    .filter(|content| on(content.read_at().part))
                    .collect();
                inbound.sort_by_key(|content| content.read_at());
                let mut spans: Vec<&StoredSpan> = if entry.output {
                    cut.iter()
                        .filter(|stored| on(stored.span.location.part))
                        .collect()
                } else {
                    Vec::new()
                };
                spans.sort_by_key(|stored| stored.span.location);
                let marks_on = |index: u16| -> Result<(Vec<Inbound>, Vec<OutputSpan>), QueryError> {
                    let inbound = inbound
                        .iter()
                        .filter(|content| content.read_at().part.index == index)
                        .map(|content| marks.inbound(content))
                        .collect::<Result<_, _>>()?;
                    let spans = spans
                        .iter()
                        .filter(|stored| stored.span.location.part.index == index)
                        .map(|stored| marks.output_span(stored, aliases))
                        .collect::<Result<_, _>>()?;
                    Ok((inbound, spans))
                };
                let parts = match window.body(entry.message) {
                    Some(body) => {
                        let kinds = part_kinds(&body.body);
                        let mut shapes = Vec::with_capacity(kinds.len());
                        for (index, kind) in kinds.into_iter().enumerate() {
                            let index = u16::try_from(index).map_err(|_| QueryError::Store {
                                reason: format!(
                                    "message {:?} has more parts than a part index",
                                    entry.message
                                ),
                            })?;
                            let text_bytes = body
                                .part_text(index)
                                .ok()
                                .map(|text| u32::try_from(text.len()).unwrap_or(u32::MAX));
                            let (inbound, spans) = marks_on(index)?;
                            shapes.push(PartShape {
                                index,
                                kind,
                                text_bytes,
                                inbound,
                                spans,
                            });
                        }
                        MessageParts::Shown(shapes)
                    }
                    None => {
                        let indexes: BTreeSet<u16> = inbound
                            .iter()
                            .map(|content| content.read_at().part.index)
                            .chain(spans.iter().map(|stored| stored.span.location.part.index))
                            .collect();
                        let mut dropped = Vec::with_capacity(indexes.len());
                        for index in indexes {
                            let (inbound, spans) = marks_on(index)?;
                            dropped.push(PartMarks {
                                index,
                                inbound,
                                spans,
                            });
                        }
                        MessageParts::BodyDropped(dropped)
                    }
                };
                Ok(TurnMessage {
                    hash: entry.message,
                    role: entry.role,
                    placement: placement(entry),
                    parts,
                })
            };
            let (inputs, output) = split(turn);
            let meta = &exchange.exchange.meta;
            turns.push(Turn {
                index: turn.index,
                exchange: turn.exchange,
                agent: aliases.agent(turn.agent),
                started_at: meta.started_at,
                protocol: meta.protocol,
                transport: meta.transport,
                model: meta.model.clone(),
                harness: meta.client.harness.clone(),
                ingress: meta.client.ingress.clone(),
                continuation: continuation(exchange, turn),
                outcome: outcome(exchange),
                inputs: inputs.into_iter().map(&message).collect::<Result<_, _>>()?,
                output: output.map(message).transpose()?,
                provenance: marks
                    .status
                    .get(&turn.exchange)
                    .copied()
                    .unwrap_or(ScanStatus::Pending),
            });
        }
        Ok(turns)
    }
}
