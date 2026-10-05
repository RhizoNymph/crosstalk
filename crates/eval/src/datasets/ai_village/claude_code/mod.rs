//! The Claude Code agent's stream: its exact model calls and the chat it
//! read through the village MCP server, as worlds with construction-tier
//! labels.
//!
//! **Exchanges.** One world per context (a session cut at compaction
//! boundaries, [`entries::contexts`]); its exchanges are the agent's calls
//! ([`calls::context`]), `Reconstructed`: each SDK message id is one API
//! call, its request rebuilt from the entries before it. The system prompt
//! and per-query prompts are not recorded, so requests carry neither.
//!
//! **Originating exchanges.** Each chat message the agent read was written
//! by another agent. Its `AGENT_TALK` event keeps the author's raw model
//! response (`data.output`); the world gets one exchange of that author
//! with that response, at the event's time, and an empty request
//! (`Synthetic`: the author's request is not rebuilt here).
//!
//! **Labels (construction).** The first `get_events` result that delivers
//! an `AGENT_TALK` event (keyed by event id) to a call is a transmission
//! from the author to the Claude Code agent: route `Direct`, carrier
//! `ToolResult`, at the reader call that first carries the result, located
//! at the content's JSON-escaped bytes in the result text.
//!
//! Why `Direct` and not a `Channel` on `Locator::Mcp { server: "village",
//! tool: "get_events" }`: the spec's `Channel` is a shared resource "written
//! by the sender, read by the reader through a tool call the gateway
//! resolved to a channel". The authors never call `get_events` (or any MCP
//! tool): their messages enter the village through the scaffolding's own
//! chat action, and `get_events` takes no argument naming a resource, it is
//! the agent's personal feed of what the scaffolding chose to deliver. That
//! is the spec's `Direct` ("placed into the reader's context by something
//! the gateway sees but that is not a resource: … a tool whose call touches
//! no extracted resource"), with `DirectCarrier::ToolResult(get_events)`.
//! A `Direct` label also aligns with a detector that does route the hit
//! through an MCP channel, so the choice does not penalise one.
//!
//! **Coverage** is partial: the agent also reads other agents' work
//! through repositories and the web, which this stream does not label.
//!
//! The agent's own `chat_message` calls are matched to `chat_messages` rows
//! by exact content ([`ClaudeCodeStats::chat_writes_matched`]): its writes to
//! the other agents, which only a window over its period could label (the
//! readers' requests are not rebuilt here).

pub mod calls;
pub mod entries;
pub mod events;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};

use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::provider;
use super::schema::{ChatRow, EventRow};
use super::stream::Table;
use super::tables::{AgentInfo, Directory, events_by_id, load_directory};
use super::text::need;
use super::time::parse_timestamp;
use super::{AiVillageError, DATASET};
use crate::corpus::{
    Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, World, WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, RouteExpectation, Tier,
    TransmissionLabel,
};
use calls::Context;
use entries::{Entry, EntryKind};
use events::{CHAT_MESSAGE, GET_EVENTS, talks};

/// The Claude Code agent's model string prefix.
pub const MODEL_PREFIX: &str = "claude-code::";

/// What the stream saw, summed over the worlds produced so far.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeCodeStats {
    pub contexts: u64,
    pub calls: u64,
    pub get_events_results: u64,
    /// Results the agent never read: the context was compacted first.
    pub get_events_unread: u64,
    pub get_events_unreadable: u64,
    /// `AGENT_TALK` deliveries from other agents, counting repeats.
    pub talk_deliveries: u64,
    pub talks_without_id: u64,
    pub own_talks: u64,
    /// Deliveries of an event already labelled at an earlier call.
    pub redeliveries: u64,
    pub unknown_speaker: u64,
    /// Events with no raw model output to originate from.
    pub unoriginated: u64,
    /// Contents not found in the result text.
    pub unlocated: u64,
    pub labels: u64,
    pub labels_by_need: BTreeMap<String, u64>,
    pub originating_exchanges: u64,
    pub chat_writes: u64,
    pub chat_writes_matched: u64,
}

/// A stream of Claude Code worlds, one per context.
pub struct ClaudeCodeStream {
    directory: Directory,
    agent: AgentInfo,
    entries: Vec<Entry>,
    contexts: Vec<Range<usize>>,
    events: HashMap<String, EventRow>,
    chat: HashMap<String, Vec<String>>,
    delivered: HashSet<String>,
    next: usize,
    limit: Option<usize>,
    stats: ClaudeCodeStats,
    root: PathBuf,
}

impl ClaudeCodeStream {
    /// Reads the Claude Code table, the events it delivered, and the
    /// agent's chat messages. `limit` caps the number of contexts.
    pub fn open(root: &Path, limit: Option<usize>) -> Result<Self, AiVillageError> {
        let directory = load_directory(root)?;
        let agent = directory
            .agents
            .values()
            .find(|agent| agent.model.starts_with(MODEL_PREFIX))
            .cloned()
            .ok_or_else(|| AiVillageError::NoAgent("Claude Code".to_owned()))?;
        let entries = entries::load(root)?;
        let contexts = entries::contexts(&entries);
        let ids = delivered_events(&entries);
        tracing::info!(
            entries = entries.len(),
            contexts = contexts.len(),
            events = ids.len(),
            "read the Claude Code stream"
        );
        let events = events_by_id(root, &ids)?;
        let mut chat: HashMap<String, Vec<String>> = HashMap::new();
        Table::ChatMessages.scan::<AiVillageError>(root, |line| {
            if !line.contains(&agent.id) {
                return Ok(());
            }
            let row: ChatRow = super::stream::decode(Table::ChatMessages, line)?;
            if row.agent_speaker_id.as_deref() == Some(agent.id.as_str()) {
                chat.entry(row.content).or_default().push(row.id);
            }
            Ok(())
        })?;
        Ok(Self {
            directory,
            agent,
            entries,
            contexts,
            events,
            chat,
            delivered: HashSet::new(),
            next: 0,
            limit,
            stats: ClaudeCodeStats::default(),
            root: root.to_path_buf(),
        })
    }

    pub fn stats(&self) -> &ClaudeCodeStats {
        &self.stats
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The next context's world, or `None` after the last.
    pub fn next_world(&mut self) -> Option<Result<World, AiVillageError>> {
        let end = self
            .limit
            .map_or(self.contexts.len(), |limit| limit.min(self.contexts.len()));
        if self.next >= end {
            return None;
        }
        let index = self.next;
        self.next += 1;
        let range = self.contexts[index].clone();
        Some(self.world(index, range))
    }

    fn world(&mut self, index: usize, range: Range<usize>) -> Result<World, AiVillageError> {
        let context = calls::context(&self.entries[range.clone()]);
        let first = self.entries.get(range.start).map(|e| e.at);
        let name = match first {
            Some(at) => format!(
                "claude-code/{index:04}-{}",
                super::time::format_seconds(at).replace(' ', "T")
            ),
            None => format!("claude-code/{index:04}"),
        };
        let dataset = DatasetId::new(DATASET);
        let mut builder = WorldBuilder::new(dataset, WorldKey::new(name));
        let reader = builder.agent(&self.agent.name, Driven::Model, &self.agent.model)?;
        let file = Table::ClaudeCodeMessages.file_name();
        let mut exchanges = Vec::with_capacity(context.calls.len());
        let mut last: Option<Timestamp> = None;
        for call in &context.calls {
            let at = after(last, call.at);
            last = Some(at);
            exchanges.push(builder.exchange(ExchangeDraft {
                agent: reader.clone(),
                at,
                protocol: crosstalk_spec::observed::exchange::WireProtocol::AnthropicMessages,
                model: call.model.clone(),
                request: call.request.clone(),
                response: call.response.clone(),
                stop: call.stop,
                usage: call.usage,
                fidelity: Fidelity::Reconstructed,
                source: SourceRef::new(
                    file.clone(),
                    format!("/{}#message={}", call.row, call.message_id),
                ),
            })?);
        }
        self.stats.contexts += 1;
        self.stats.calls += context.calls.len() as u64;
        self.label(&mut builder, &reader, &context, &exchanges)?;
        self.chat_writes(&context);
        Ok(builder.finish(Coverage::Partial))
    }

    fn label(
        &mut self,
        builder: &mut WorldBuilder,
        reader: &AgentKey,
        context: &Context,
        exchanges: &[crosstalk_spec::ids::ExchangeId],
    ) -> Result<(), AiVillageError> {
        let mut speakers: BTreeMap<String, (AgentKey, Option<Timestamp>)> = BTreeMap::new();
        for result in context.results.iter().filter(|r| r.tool == GET_EVENTS) {
            self.stats.get_events_results += 1;
            let Some(reader_call) = result.reader else {
                self.stats.get_events_unread += 1;
                continue;
            };
            let Ok(text) = result.message.message().part_text(result.part) else {
                self.stats.get_events_unreadable += 1;
                continue;
            };
            let Ok((delivered, without_id)) = talks(&text) else {
                self.stats.get_events_unreadable += 1;
                continue;
            };
            self.stats.talks_without_id += without_id as u64;
            for talk in delivered {
                if talk.speaker == self.agent.name {
                    self.stats.own_talks += 1;
                    continue;
                }
                self.stats.talk_deliveries += 1;
                if !self.delivered.insert(talk.event.clone()) {
                    self.stats.redeliveries += 1;
                    continue;
                }
                let Some(speaker) = self.directory.by_name(&talk.speaker).cloned() else {
                    self.stats.unknown_speaker += 1;
                    continue;
                };
                let Some((event_at, output)) = self.events.get(&talk.event).and_then(|row| {
                    let output = row.data.get("output").filter(|o| !o.is_null())?;
                    Some((parse_timestamp(&row.created_at).ok()?, output.clone()))
                }) else {
                    self.stats.unoriginated += 1;
                    continue;
                };
                let Some((start, end)) = talk.range else {
                    self.stats.unlocated += 1;
                    continue;
                };
                let (key, last) = match speakers.get(&speaker.id) {
                    Some(entry) => entry.clone(),
                    None => (
                        builder.agent(&speaker.name, Driven::Model, &speaker.model)?,
                        None,
                    ),
                };
                let response = provider::response(&output, &talk.event);
                let message = HashedMessage::new(response.body());
                let at = after(last, event_at);
                let sender_exchange = builder.exchange(ExchangeDraft {
                    agent: key.clone(),
                    at,
                    protocol: response.protocol,
                    model: speaker.model.clone(),
                    request: Vec::new(),
                    response: message.clone(),
                    stop: response.stop,
                    usage: None,
                    fidelity: Fidelity::Synthetic,
                    source: SourceRef::new(
                        Table::Events.file_name(),
                        format!("/{}/data/output", talk.event),
                    ),
                })?;
                speakers.insert(speaker.id.clone(), (key.clone(), Some(at)));
                self.stats.originating_exchanges += 1;
                let (Ok(start), Ok(end)) = (u32::try_from(start), u32::try_from(end)) else {
                    self.stats.unlocated += 1;
                    continue;
                };
                let at_location =
                    location::in_message(result.message.message(), result.part, start, end)?;
                let needs = need(message.message(), &talk.escaped);
                let tier = needs.tier(Tier::Construction);
                *self
                    .stats
                    .labels_by_need
                    .entry(format!("{:?}", needs.class()).to_ascii_lowercase())
                    .or_default() += 1;
                self.stats.labels += 1;
                builder.expect(Expectation::Transmission(ExpectedTransmission::new(
                    TransmissionLabel {
                        from: key,
                        to: reader.clone(),
                        sender_exchange: Some(sender_exchange),
                        reader_exchange: exchanges[reader_call],
                        route: RouteExpectation::Direct,
                        carrier: CarrierKind::ToolResult,
                        content: ExpectedContent {
                            text: talk.escaped.clone(),
                            at: at_location,
                        },
                        needs,
                        tier,
                        source: SourceRef::new(
                            Table::ClaudeCodeMessages.file_name(),
                            format!(
                                "/{}/content/message/content/{}#event={}",
                                result.row, result.block, talk.event
                            ),
                        ),
                    },
                )?));
            }
        }
        Ok(())
    }

    fn chat_writes(&mut self, context: &Context) {
        for tool_use in context.tool_uses.iter().filter(|u| u.name == CHAT_MESSAGE) {
            self.stats.chat_writes += 1;
            if events::chat_content(&tool_use.arguments)
                .is_some_and(|content| self.chat.contains_key(&content))
            {
                self.stats.chat_writes_matched += 1;
            }
        }
    }
}

/// `at`, or one microsecond after `last` when it is not later.
pub fn after(last: Option<Timestamp>, at: Timestamp) -> Timestamp {
    match last {
        Some(last) if at <= last => Timestamp::from_micros(last.as_micros() + 1),
        _ => at,
    }
}

/// The ids of every `AGENT_TALK` event a `get_events` result delivered.
fn delivered_events(entries: &[Entry]) -> HashSet<String> {
    let mut calls: HashSet<&str> = HashSet::new();
    for entry in entries {
        if let EntryKind::Assistant { parts, .. } = &entry.kind {
            for part in parts {
                if let crosstalk_spec::observed::message::AssistantPart::ToolCall(call) = part
                    && call.name.0 == GET_EVENTS
                {
                    calls.insert(call.id.0.as_str());
                }
            }
        }
    }
    let mut ids = HashSet::new();
    for entry in entries {
        let EntryKind::ToolResults(results) = &entry.kind else {
            continue;
        };
        for (_, result) in results {
            if !calls.contains(result.call_id.0.as_str()) {
                continue;
            }
            for content in &result.content {
                if let crosstalk_spec::observed::message::ToolResultContent::Text(text) = content
                    && let Ok(value) = serde_json::from_str::<Value>(&text.0)
                {
                    for event in value
                        .get("events")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if event.get("actionType").and_then(Value::as_str) == Some("AGENT_TALK")
                            && let Some(id) = event.get("id").and_then(Value::as_str)
                        {
                            ids.insert(id.to_owned());
                        }
                    }
                }
            }
        }
    }
    ids
}
