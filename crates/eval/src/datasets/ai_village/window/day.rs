//! One village day as a world: every agent's calls with rebuilt requests,
//! chat labels and repository labels.
//!
//! **Requests** (`Reconstructed`; `llm_calls` is withheld, so the requests
//! are rebuilt from what the responses imply):
//!
//! ```text
//! [system prompt of the session]                         prompt::system
//! for each earlier call of the same computer-use session:
//!     [chat user turn of that call]                      prompt::chat
//!     assistant: that call's response
//!     tool: that call's output (and error) as the result of its first tool call
//! [chat user turn: the room's messages since the agent's previous call]
//! ```
//!
//! A call that only exists as an `AGENT_TALK` event's output has an empty
//! request (`Synthetic`).
//!
//! **Chat labels** (Structural): a chat message reaches every other member
//! of its room through the chat user turn of their next call; a label sits
//! there when that call is within [`CHAT_HORIZON_MICROS`] of the message,
//! from the speaker's call that sent it (the turn whose
//! `send_message_back_to_chat` carried exactly the content).
//!
//! **Repository labels** (Heuristic): see [`super::repo`]. Both accesses of
//! a pair must fall in the day: a pair across days has its write in another
//! world and is counted, not labelled.

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::support::Timestamp;

use super::super::claude_code::after;
use super::super::schema::TurnRow;
use super::super::stream::{Table, decode};
use super::super::text::need;
use super::super::time::{Day, parse_timestamp};
use super::super::{AiVillageError, DATASET};
use super::calls::{self, Call, Origin};
use super::prompt::{self, ChatLine};
use super::repo::payload_line;
use super::{Shared, WindowStats};
use crate::corpus::{
    Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, World, WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, RouteExpectation, Tier,
    TransmissionLabel,
};

/// A chat message is labelled at a reader call at most four hours later.
pub const CHAT_HORIZON_MICROS: u64 = 4 * 3_600 * 1_000_000;

/// Where one chat message reached one reader.
struct Delivery {
    reader: String,
    call: usize,
    message: HashedMessage,
    start: u32,
    end: u32,
}

/// The calls of the day, per agent, with the chat each one carries.
fn day_calls(
    shared: &Shared,
    day: Day,
    lines: &[String],
    stats: &mut WindowStats,
) -> Result<BTreeMap<String, Vec<Call>>, AiVillageError> {
    let mut list = Vec::with_capacity(lines.len());
    for line in lines {
        let row: TurnRow = decode(Table::ComputerUseTurns, line)?;
        let Ok(at) = parse_timestamp(&row.created_at) else {
            continue;
        };
        let Some(agent) = shared.sessions.get(&row.session_id) else {
            stats.turns_unknown_session += 1;
            continue;
        };
        stats.turns += 1;
        list.push(calls::turn_call(agent, at, row));
    }
    let mut by_agent = calls::by_agent(list);
    let talks = shared.talks_on(day);
    let found = calls::senders(&by_agent, &talks);
    let mut added = false;
    for (at, event) in &talks {
        let Some(message) = event.str("messageId") else {
            continue;
        };
        if found.contains_key(message) {
            continue;
        }
        let Some(speaker) = event.str("speakerId") else {
            continue;
        };
        if let Some(call) = calls::talk_call(speaker, *at, event) {
            by_agent.entry(speaker.to_owned()).or_default().push(call);
            added = true;
        }
    }
    if added {
        let all: Vec<Call> = by_agent.into_values().flatten().collect();
        by_agent = calls::by_agent(all);
    }
    Ok(by_agent)
}

/// Builds the world of village day `day` from its turn lines.
pub fn build(
    shared: &Shared,
    day: Day,
    lines: &[String],
    stats: &mut WindowStats,
) -> Result<World, AiVillageError> {
    let by_agent = day_calls(shared, day, lines, stats)?;
    let talks = shared.talks_on(day);
    let senders = calls::senders(&by_agent, &talks);
    let dataset = DatasetId::new(DATASET);
    let mut builder = WorldBuilder::new(dataset, WorldKey::new(format!("window/{day}")));
    let day_start = day.village_start()?;
    let chat = shared.chat_on(day);
    let mut keys: BTreeMap<String, AgentKey> = BTreeMap::new();
    let mut exchanges: HashMap<(String, usize), ExchangeId> = HashMap::new();
    let mut turn_index: HashMap<String, (String, usize)> = HashMap::new();
    let mut deliveries: HashMap<String, Vec<Delivery>> = HashMap::new();
    let mut results: HashMap<(String, usize), HashedMessage> = HashMap::new();
    for (agent, list) in &by_agent {
        let info = shared.directory.agent(agent);
        let name = info.map_or(agent.as_str(), |info| info.name.as_str());
        let model = info.map_or("unknown", |info| info.model.as_str());
        let key = builder.agent(name, Driven::Model, model)?;
        keys.insert(agent.clone(), key.clone());
        let mut history: Vec<HashedMessage> = Vec::new();
        let mut system: Option<HashedMessage> = None;
        let mut session: Option<String> = None;
        let mut previous = day_start;
        let mut cursor = 0usize;
        let mut last: Option<Timestamp> = None;
        for (index, call) in list.iter().enumerate() {
            let at = after(last, call.at);
            last = Some(at);
            if let Some(turn) = call.turn_id() {
                turn_index.insert(turn.to_owned(), (agent.clone(), index));
            }
            let (request, fidelity, source) = match &call.origin {
                Origin::Turn {
                    id, session: own, ..
                } => {
                    if session.as_deref() != Some(own.as_str()) {
                        session = Some(own.clone());
                        history.clear();
                        system = Some(prompt::system(
                            name,
                            shared.goals.village(call.at),
                            &shared.goals.agent(agent, call.at),
                            shared.memories.latest(agent, call.at),
                        ));
                    }
                    // Chat posted in the agent's room since its previous call.
                    let mut lines = Vec::new();
                    while cursor < chat.len() && chat[cursor].0 <= call.at {
                        let (posted, row) = chat[cursor];
                        cursor += 1;
                        if posted <= previous {
                            continue;
                        }
                        let room = shared
                            .rooms
                            .room_of(agent, posted)
                            .or(shared.rooms.general());
                        if room != Some(row.room_id.as_str()) {
                            continue;
                        }
                        lines.push(ChatLine {
                            id: &row.id,
                            at: posted,
                            room: shared.directory.room_name(&row.room_id),
                            speaker: shared.speaker(row),
                            content: &row.content,
                        });
                    }
                    previous = call.at;
                    let mut request: Vec<HashedMessage> = system.iter().cloned().collect();
                    request.extend(history.iter().cloned());
                    if !lines.is_empty() {
                        let (message, ranges) = prompt::chat(&lines);
                        for (id, start, end) in ranges {
                            deliveries.entry(id).or_default().push(Delivery {
                                reader: agent.clone(),
                                call: index,
                                message: message.clone(),
                                start,
                                end,
                            });
                        }
                        request.push(message.clone());
                        history.push(message);
                    }
                    history.push(call.message.clone());
                    if let Some(result) = &call.result {
                        history.push(result.clone());
                        results.insert((agent.clone(), index), result.clone());
                    }
                    (
                        request,
                        Fidelity::Reconstructed,
                        SourceRef::new(Table::ComputerUseTurns.file_name(), format!("/{id}")),
                    )
                }
                Origin::Talk { event } => (
                    Vec::new(),
                    Fidelity::Synthetic,
                    SourceRef::new(Table::Events.file_name(), format!("/{event}/data/output")),
                ),
            };
            let id = builder.exchange(ExchangeDraft {
                agent: key.clone(),
                at,
                protocol: call.response.protocol,
                model: model.to_owned(),
                request,
                response: call.message.clone(),
                stop: call.response.stop,
                usage: None,
                fidelity,
                source,
            })?;
            exchanges.insert((agent.clone(), index), id);
            stats.exchanges += 1;
        }
    }
    let context = Labels {
        shared,
        by_agent: &by_agent,
        keys: &keys,
        exchanges: &exchanges,
    };
    context.chat(&mut builder, &talks, &senders, &deliveries, stats)?;
    context.repo(&mut builder, day, &turn_index, &results, stats)?;
    Ok(builder.finish(Coverage::Partial))
}

struct Labels<'a> {
    shared: &'a Shared,
    by_agent: &'a BTreeMap<String, Vec<Call>>,
    keys: &'a BTreeMap<String, AgentKey>,
    exchanges: &'a HashMap<(String, usize), ExchangeId>,
}

impl Labels<'_> {
    fn chat(
        &self,
        builder: &mut WorldBuilder,
        talks: &[(Timestamp, &super::super::schema::EventRow)],
        senders: &HashMap<String, (String, usize)>,
        deliveries: &HashMap<String, Vec<Delivery>>,
        stats: &mut WindowStats,
    ) -> Result<(), AiVillageError> {
        for (posted, event) in talks {
            let (Some(message), Some(content)) = (event.str("messageId"), event.str("content"))
            else {
                continue;
            };
            stats.talks += 1;
            let Some((speaker, index)) = senders.get(message) else {
                stats.talks_without_sender += 1;
                continue;
            };
            let (Some(from), Some(sender_exchange), Some(sender_call)) = (
                self.keys.get(speaker),
                self.exchanges.get(&(speaker.clone(), *index)),
                self.by_agent.get(speaker).and_then(|list| list.get(*index)),
            ) else {
                stats.talks_without_sender += 1;
                continue;
            };
            let mut read = false;
            for delivery in deliveries.get(message).into_iter().flatten() {
                if &delivery.reader == speaker {
                    continue;
                }
                let (Some(to), Some(reader_exchange), Some(reader_call)) = (
                    self.keys.get(&delivery.reader),
                    self.exchanges
                        .get(&(delivery.reader.clone(), delivery.call)),
                    self.by_agent
                        .get(&delivery.reader)
                        .and_then(|list| list.get(delivery.call)),
                ) else {
                    continue;
                };
                read = true;
                if reader_call
                    .at
                    .as_micros()
                    .saturating_sub(posted.as_micros())
                    > CHAT_HORIZON_MICROS
                {
                    stats.chat_late += 1;
                    continue;
                }
                let at = location::in_message(
                    delivery.message.message(),
                    0,
                    delivery.start,
                    delivery.end,
                )?;
                let needs = need(sender_call.message.message(), content);
                let tier = needs.tier(Tier::Structural);
                stats.count_need(&needs);
                stats.chat_labels += 1;
                builder.expect(Expectation::Transmission(ExpectedTransmission::new(
                    TransmissionLabel {
                        from: from.clone(),
                        to: to.clone(),
                        sender_exchange: Some(*sender_exchange),
                        reader_exchange: *reader_exchange,
                        route: RouteExpectation::Direct,
                        carrier: CarrierKind::UserTurn,
                        content: ExpectedContent {
                            text: content.to_owned(),
                            at,
                        },
                        needs,
                        tier,
                        source: SourceRef::new(
                            Table::ChatMessages.file_name(),
                            format!("/{message}#reader={}", to.name),
                        ),
                    },
                )?));
            }
            if !read {
                stats.talks_unread += 1;
            }
        }
        Ok(())
    }

    fn repo(
        &self,
        builder: &mut WorldBuilder,
        day: Day,
        turn_index: &HashMap<String, (String, usize)>,
        results: &HashMap<(String, usize), HashedMessage>,
        stats: &mut WindowStats,
    ) -> Result<(), AiVillageError> {
        let log = &self.shared.access;
        for pair in &log.pairs {
            let (write, read) = (&log.records[pair.write], &log.records[pair.read]);
            if read.day != day {
                continue;
            }
            if write.day != day {
                stats.repo_cross_day += 1;
                continue;
            }
            let (Some((writer, write_index)), Some((reader, read_index))) =
                (turn_index.get(&write.turn), turn_index.get(&read.turn))
            else {
                continue;
            };
            let Some(list) = self.by_agent.get(reader) else {
                continue;
            };
            // The reader's next call in the same session carries the output.
            let next = list
                .iter()
                .enumerate()
                .skip(read_index + 1)
                .find(|(_, call)| call.session().is_some())
                .filter(|(_, call)| call.session() == Some(read.session.as_str()));
            let Some((next_index, _)) = next else {
                stats.repo_no_next_call += 1;
                continue;
            };
            let Some(result) = results.get(&(reader.clone(), *read_index)) else {
                stats.repo_co_access += 1;
                continue;
            };
            let Ok(text) = result.message().part_text(0) else {
                stats.repo_co_access += 1;
                continue;
            };
            let Some((line, start, end)) = payload_line(&write.access.payload, &text) else {
                stats.repo_co_access += 1;
                continue;
            };
            let (Some(from), Some(to), Some(sender), Some(reader_exchange), Some(writer_call)) = (
                self.keys.get(writer),
                self.keys.get(reader),
                self.exchanges.get(&(writer.clone(), *write_index)),
                self.exchanges.get(&(reader.clone(), next_index)),
                self.by_agent.get(writer).and_then(|l| l.get(*write_index)),
            ) else {
                continue;
            };
            let (Ok(start), Ok(end)) = (u32::try_from(start), u32::try_from(end)) else {
                continue;
            };
            let at = location::in_message(result.message(), 0, start, end)?;
            let needs = need(writer_call.message.message(), &line);
            let tier = needs.tier(Tier::Heuristic);
            stats.count_need(&needs);
            stats.repo_labels += 1;
            if write.access.http_visible() && read.access.http_visible() {
                stats.repo_labels_http_visible += 1;
            } else {
                stats.repo_labels_bash_only += 1;
            }
            *stats
                .repo_labels_by_verbs
                .entry(format!("{} -> {}", write.access.verb, read.access.verb))
                .or_default() += 1;
            builder.expect(Expectation::Transmission(ExpectedTransmission::new(
                TransmissionLabel {
                    from: from.clone(),
                    to: to.clone(),
                    sender_exchange: Some(*sender),
                    reader_exchange: *reader_exchange,
                    route: RouteExpectation::Channel {
                        resource: read.access.resource.clone(),
                    },
                    carrier: CarrierKind::ToolResult,
                    content: ExpectedContent { text: line, at },
                    needs,
                    tier,
                    source: SourceRef::new(
                        Table::ComputerUseTurns.file_name(),
                        format!(
                            "/{}#write={}&verb={}",
                            read.turn, write.turn, read.access.verb
                        ),
                    ),
                },
            )?));
        }
        Ok(())
    }
}
