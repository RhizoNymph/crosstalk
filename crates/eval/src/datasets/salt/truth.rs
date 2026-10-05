//! SALT ground truth.
//!
//! **Positives (Construction).** Every `channel_transcript` entry is a
//! delivered message: the sender's `send_message` argument, verbatim, in the
//! receiver's next user turn after `[round=r/n][from=…][type=…]` and a blank
//! line. Its label: sender to receiver, Direct route, UserTurn carrier, at
//! the receiver's first call after the turn, located at the content bytes.
//! It needs an `Exact` match unless the content holds a character JSON
//! escapes (a quote, a backslash, a control character): the sender's copy
//! sits escaped inside canonical tool-call arguments, so then it needs one
//! level of JSON string decoding, `Decoded([JsonString])`
//! (`provenance.match.string-serialised-decoded`).
//!
//! **Negative controls.**
//! - `RejectedSend` (Construction): a `send_message` whose event failed (the
//!   200-character limit) was never delivered. The converter gives its
//!   result `ToolOutcome::Error`, so a gateway's L5 records it as a
//!   `WriteOutcome::Rejected` write that never pairs; the label checks that
//!   no detector credits it anyway. Its origin is the failed
//!   call's arguments: a prediction from the sender to the receiver whose
//!   matched span lies there, and which no delivered label explains, claims
//!   text arrived that never did.
//! - `NoSenderExchange` (Construction): in `controlled_peer` Bob is scripted
//!   and makes no calls, so his delivered messages have no originating
//!   exchange; a prediction naming him as sender at that location is wrong.
//!   (Alice's messages to a scripted Bob have no reader exchange and are not
//!   labelled.)
//! - `SharedSource` (Structural): each agent's system prompt (about 95%
//!   shared with the peer's), and results of tools that read the shared task
//!   resources (`inspect_database`, `query_database`, `read_code`,
//!   `read_source`, `resolve_records`).
//! - `Boilerplate` (Structural): the harness's own user turns (phase
//!   headers, round prompts, feedback), including the peer's task text the
//!   communication header quotes.

use std::collections::BTreeSet;

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody};

use super::SaltError;
use super::episode::{AgentEpisode, delivered_turn};
use super::messages::{argument, content_text};
use super::schema::Episode;
use crate::keys::{AgentKey, SourceRef};
use crate::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl,
    NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};

/// Tools whose results come from resources both agents read.
pub const SHARED_TOOLS: &[&str] = &[
    "inspect_database",
    "query_database",
    "read_code",
    "read_source",
    "resolve_records",
];

/// One agent's reconstructed episode with the exchange refs its turns got.
pub struct Labelled<'a> {
    pub key: AgentKey,
    pub episode: &'a AgentEpisode,
    /// The exchange of each turn, in turn order; empty for a scripted agent.
    pub exchanges: Vec<ExchangeId>,
    pub scripted: bool,
}

impl Labelled<'_> {
    /// The exchange of the first call after message `index`.
    fn reader_exchange(&self, index: usize) -> Option<ExchangeId> {
        let position = self
            .episode
            .turns
            .iter()
            .position(|turn| turn.index > index)?;
        self.exchanges.get(position).copied()
    }

    /// The exchange of the turn whose response is message `index`.
    fn exchange_of_response(&self, index: usize) -> Option<ExchangeId> {
        let position = self
            .episode
            .turns
            .iter()
            .position(|turn| turn.index == index)?;
        self.exchanges.get(position).copied()
    }

    fn message_hash(&self, index: usize) -> Option<MessageHash> {
        self.episode.messages.get(index).map(|m| m.hash())
    }
}

/// Labels for one episode, given both agents' reconstructions.
pub struct EpisodeLabels<'a> {
    pub file: &'a str,
    pub position: usize,
    pub episode: &'a Episode,
    pub agents: &'a [Labelled<'a>],
    /// System prompts already labelled in this world, per reader.
    pub seen_system: &'a mut BTreeSet<(AgentKey, MessageHash)>,
}

impl EpisodeLabels<'_> {
    fn agent(&self, name: &str) -> Option<&Labelled<'_>> {
        self.agents.iter().find(|agent| agent.key.name == name)
    }

    fn source(&self, path: String) -> SourceRef {
        SourceRef::new(self.file, path)
    }

    pub fn label(mut self) -> Result<Vec<Expectation>, SaltError> {
        let mut out = Vec::new();
        self.deliveries(&mut out)?;
        self.rejected_sends(&mut out)?;
        self.shared_and_boilerplate(&mut out)?;
        Ok(out)
    }

    fn deliveries(&self, out: &mut Vec<Expectation>) -> Result<(), SaltError> {
        for (at, delivery) in self.episode.channel_transcript.iter().enumerate() {
            let (Some(sender), Some(receiver)) =
                (self.agent(&delivery.sender), self.agent(&delivery.receiver))
            else {
                continue;
            };
            if receiver.scripted {
                continue;
            }
            let Some((&index, _)) = receiver
                .episode
                .delivered
                .iter()
                .find(|(_, event)| **event == delivery.event_id)
            else {
                continue;
            };
            let Some(reader_exchange) = receiver.reader_exchange(index) else {
                continue;
            };
            let raw = &receiver.episode.raw[index];
            let text = content_text(&raw.content);
            let Some(turn) = delivered_turn(&text) else {
                continue;
            };
            let (Some(hash), Ok(start), Ok(end)) = (
                receiver.message_hash(index),
                u32::try_from(turn.content_start),
                u32::try_from(text.len()),
            ) else {
                continue;
            };
            let Ok(location) = location::location(hash, 0, start, end) else {
                continue;
            };
            let source = self.source(format!(
                "/results/{}/channel_transcript/{at}",
                self.position
            ));
            if sender.scripted {
                out.push(Expectation::NoTransmission(NegativeControl::new(
                    NegativeLabel {
                        from: sender.key.clone(),
                        to: receiver.key.clone(),
                        reader_exchange: Some(reader_exchange),
                        at: Some(location),
                        origin: None,
                        text: Some(delivery.content.clone()),
                        reason: NegativeReason::NoSenderExchange,
                        tier: Tier::Construction,
                        source,
                    },
                )?));
                continue;
            }
            let sender_exchange = sender
                .episode
                .call_of_event(delivery.event_id)
                .and_then(|(message, _)| sender.exchange_of_response(message));
            let needs = MatchNeed::through_json_string(&delivery.content);
            out.push(Expectation::Transmission(ExpectedTransmission::new(
                TransmissionLabel {
                    from: sender.key.clone(),
                    to: receiver.key.clone(),
                    sender_exchange,
                    reader_exchange,
                    route: RouteExpectation::Direct,
                    carrier: CarrierKind::UserTurn,
                    content: ExpectedContent {
                        text: delivery.content.clone(),
                        at: location,
                    },
                    needs,
                    tier: Tier::Construction,
                    source,
                },
            )?));
        }
        Ok(())
    }

    /// A failed send is a rejected write the detector sees itself (its
    /// result is `ToolOutcome::Error`, so L5 records `WriteOutcome::Rejected`
    /// and never pairs it); this label checks no detector credits it.
    fn rejected_sends(&self, out: &mut Vec<Expectation>) -> Result<(), SaltError> {
        for (at, event) in self.episode.events.iter().enumerate() {
            if event.tool.as_deref() != Some("send_message") || event.success != Some(false) {
                continue;
            }
            let Some(recipient) = event.recipient.as_deref() else {
                continue;
            };
            let (Some(sender), Some(receiver)) = (self.agent(&event.actor), self.agent(recipient))
            else {
                continue;
            };
            if sender.scripted || receiver.scripted {
                continue;
            }
            let Some((message, call)) = sender.episode.call_of_event(event.event_id) else {
                continue;
            };
            let Some(raw_call) = sender.episode.raw[message]
                .tool_calls
                .as_ref()
                .and_then(|calls| calls.get(call))
            else {
                continue;
            };
            let Some(origin) = call_location(sender.episode, message, &raw_call.id) else {
                continue;
            };
            let text =
                argument(&raw_call.function.arguments, "content").or_else(|| event.content.clone());
            out.push(Expectation::NoTransmission(NegativeControl::new(
                NegativeLabel {
                    from: sender.key.clone(),
                    to: receiver.key.clone(),
                    reader_exchange: None,
                    at: None,
                    origin: Some(origin),
                    text,
                    reason: NegativeReason::RejectedSend,
                    tier: Tier::Construction,
                    source: self.source(format!("/results/{}/events/{at}", self.position)),
                },
            )?));
        }
        Ok(())
    }

    fn shared_and_boilerplate(&mut self, out: &mut Vec<Expectation>) -> Result<(), SaltError> {
        let mut labels = Vec::new();
        for reader in self.agents.iter().filter(|agent| !agent.scripted) {
            for peer in self.agents.iter().filter(|agent| agent.key != reader.key) {
                let episode = reader.episode;
                for (index, raw) in episode.raw.iter().enumerate() {
                    let Some(hash) = reader.message_hash(index) else {
                        continue;
                    };
                    let path = format!(
                        "/results/{}/agents/{}/messages/{index}",
                        self.position, reader.key.name
                    );
                    let text = content_text(&raw.content);
                    if text.trim().is_empty() {
                        continue;
                    }
                    let (reason, reader_exchange) = match raw.role.as_str() {
                        "system" if index == 0 => {
                            if !self.seen_system.insert((reader.key.clone(), hash)) {
                                continue;
                            }
                            (NegativeReason::SharedSource, None)
                        }
                        "tool"
                            if index >= episode.start
                                && raw
                                    .tool_call_id
                                    .as_deref()
                                    .is_some_and(|id| shared_tool_call(episode, id)) =>
                        {
                            (NegativeReason::SharedSource, reader.reader_exchange(index))
                        }
                        "user"
                            if index >= episode.start
                                && !episode.delivered.contains_key(&index) =>
                        {
                            (NegativeReason::Boilerplate, reader.reader_exchange(index))
                        }
                        _ => continue,
                    };
                    if reason != NegativeReason::SharedSource && reader_exchange.is_none() {
                        continue;
                    }
                    let Ok(len) = u32::try_from(text.len()) else {
                        continue;
                    };
                    let Ok(at) = location::location(hash, 0, 0, len) else {
                        continue;
                    };
                    let tier = Tier::Structural;
                    labels.push(NegativeLabel {
                        from: peer.key.clone(),
                        to: reader.key.clone(),
                        reader_exchange,
                        at: Some(at),
                        origin: None,
                        text: None,
                        reason,
                        tier,
                        source: self.source(path.clone()),
                    });
                }
            }
        }
        for label in labels {
            out.push(Expectation::NoTransmission(NegativeControl::new(label)?));
        }
        Ok(())
    }
}

/// The location of tool call `id`'s arguments (its part text) in message
/// `index` of the episode.
fn call_location(episode: &AgentEpisode, index: usize, id: &str) -> Option<SpanLocation> {
    let message = episode.messages.get(index)?.message();
    let MessageBody::Assistant(parts) = &message.body else {
        return None;
    };
    let part = parts
        .iter()
        .position(|part| matches!(part, AssistantPart::ToolCall(call) if call.id.0 == id))?;
    location::whole_part(message, u16::try_from(part).ok()?).ok()
}

/// Whether call `id` in the episode's region is to a shared-resource tool.
fn shared_tool_call(episode: &AgentEpisode, id: &str) -> bool {
    episode.raw.iter().skip(episode.start).any(|message| {
        message
            .tool_calls
            .iter()
            .flatten()
            .any(|call| call.id == id && SHARED_TOOLS.contains(&call.function.name.as_str()))
    })
}
