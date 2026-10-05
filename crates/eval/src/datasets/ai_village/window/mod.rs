//! Every agent over a window of village days, one world per day.
//!
//! [`WindowStream::open`] makes the full streaming passes once (turns,
//! events, chat, memories; [`super::tables`]), keeps the window's turns as
//! raw lines bucketed by village day, tags every bash command's resource
//! accesses in time order ([`repo::AccessLog`]) and counts GUI edits
//! ([`gui`]). Each world is then built on demand from its day's lines
//! ([`day::build`]), so at most one day's exchanges are in memory.
//!
//! The Claude Code agent is not in the window: its calls are in its own
//! stream ([`super::claude_code`]), not in `computer_use_turns`.

pub mod calls;
pub mod day;
pub mod gui;
pub mod prompt;
pub mod repo;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use super::rooms::RoomTimeline;
use super::schema::{ChatRow, EventRow, TurnRow};
use super::stream::{Table, decode};
use super::tables::{
    Directory, Goals, Memories, load_directory, load_goals, load_sessions, scan_chat, scan_events,
    scan_memories, scan_turns,
};
use super::text::visible_text;
use super::time::{Day, Window, village_day};
use super::{AiVillageError, provider};
use crate::corpus::World;
use crate::truth::MatchNeed;
use repo::{AccessLog, AccessStats, TurnRef};

/// What the window held and what became labels.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowStats {
    pub days: u64,
    pub turns: u64,
    pub turns_unknown_session: u64,
    pub exchanges: u64,
    pub agents: u64,
    /// Agent chat messages (`AGENT_TALK`) in the window.
    pub talks: u64,
    pub talks_without_sender: u64,
    /// Messages no other agent's call carried in the same day.
    pub talks_unread: u64,
    pub chat_labels: u64,
    /// Deliveries whose reader call came after the horizon.
    pub chat_late: u64,
    pub repo_labels: u64,
    /// Pairs whose read output holds none of the write's payload.
    pub repo_co_access: u64,
    pub repo_cross_day: u64,
    pub repo_no_next_call: u64,
    pub labels_by_need: BTreeMap<String, u64>,
    pub access: AccessStats,
    pub gui: gui::GuiStats,
}

impl WindowStats {
    fn count_need(&mut self, need: &MatchNeed) {
        *self
            .labels_by_need
            .entry(format!("{:?}", need.class()).to_ascii_lowercase())
            .or_default() += 1;
    }
}

/// What every day's world is built from.
pub struct Shared {
    pub directory: Directory,
    pub sessions: HashMap<String, String>,
    pub goals: Goals,
    pub memories: Memories,
    pub rooms: RoomTimeline,
    /// `AGENT_TALK` events in the window, by time.
    pub talks: Vec<(Timestamp, EventRow)>,
    /// Chat messages in the window, by time.
    pub chat: Vec<(Timestamp, ChatRow)>,
    /// Display names of humans' messages, by chat message id.
    pub human_names: HashMap<String, String>,
    pub access: AccessLog,
}

impl Shared {
    /// The day's `AGENT_TALK` events, by time.
    pub fn talks_on(&self, day: Day) -> Vec<(Timestamp, &EventRow)> {
        self.talks
            .iter()
            .filter(|(at, _)| village_day(*at) == day)
            .map(|(at, row)| (*at, row))
            .collect()
    }

    /// The day's chat messages, by time.
    pub fn chat_on(&self, day: Day) -> Vec<(Timestamp, &ChatRow)> {
        self.chat
            .iter()
            .filter(|(at, _)| village_day(*at) == day)
            .map(|(at, row)| (*at, row))
            .collect()
    }

    /// Who posted a chat message: the agent's name, a human's display name,
    /// or `human`.
    pub fn speaker<'a>(&'a self, row: &'a ChatRow) -> &'a str {
        if let Some(agent) = row
            .agent_speaker_id
            .as_deref()
            .and_then(|id| self.directory.agent(id))
        {
            return &agent.name;
        }
        self.human_names
            .get(&row.id)
            .map_or("human", String::as_str)
    }
}

/// The window as a stream of day worlds.
pub struct WindowStream {
    shared: Shared,
    days: Vec<(Day, Vec<String>)>,
    next: usize,
    stats: WindowStats,
}

impl WindowStream {
    /// Makes the passes over `root` for village days `first..=last`.
    pub fn open(root: &Path, first: Day, last: Day) -> Result<Self, AiVillageError> {
        let window = Window::days(first, last)?;
        let started = std::time::Instant::now();
        let directory = load_directory(root)?;
        let sessions = load_sessions(root)?;
        let goals = load_goals(root)?;
        let mut buckets: BTreeMap<Day, Vec<String>> = BTreeMap::new();
        let turns = scan_turns(root, window, |at, line| {
            buckets
                .entry(village_day(at))
                .or_default()
                .push(line.to_owned());
        })?;
        tracing::info!(
            turns,
            elapsed_s = started.elapsed().as_secs(),
            "scanned turns"
        );
        let mut human_names = HashMap::new();
        let mut events = scan_events(root, window, |row| {
            matches!(row.action(), Some("AGENT_TALK" | "USER_TALK"))
        })?;
        if let Some(general) = directory
            .rooms
            .iter()
            .find(|(_, name)| name.as_str() == "general")
            .map(|(id, _)| id.clone())
        {
            events.rooms.set_general(&general);
        }
        let mut talks = Vec::new();
        for (at, row) in events.events {
            match row.action() {
                Some("USER_TALK") => {
                    if let (Some(message), Some(name)) =
                        (row.str("messageId"), row.str("speakerName"))
                    {
                        human_names.insert(message.to_owned(), name.to_owned());
                    }
                }
                _ => talks.push((at, row)),
            }
        }
        tracing::info!(
            talks = talks.len(),
            elapsed_s = started.elapsed().as_secs(),
            "scanned events"
        );
        let chat = scan_chat(root, window)?;
        let memories = scan_memories(root, window)?;
        tracing::info!(
            chat = chat.len(),
            elapsed_s = started.elapsed().as_secs(),
            "scanned chat and memories"
        );
        let mut stats = WindowStats::default();
        let mut access = AccessLog::default();
        tag(&sessions, &buckets, &mut access, &mut stats)?;
        stats.access = access.stats.clone();
        let mut agents = std::collections::BTreeSet::new();
        for lines in buckets.values() {
            for line in lines {
                if let Some(session) = super::stream::string_field(line, "session_id")
                    && let Some(agent) = sessions.get(session)
                {
                    agents.insert(agent.clone());
                }
            }
        }
        stats.agents = agents.len() as u64;
        Ok(Self {
            shared: Shared {
                directory,
                sessions,
                goals,
                memories,
                rooms: events.rooms,
                talks,
                chat,
                human_names,
                access,
            },
            days: buckets.into_iter().collect(),
            next: 0,
            stats,
        })
    }

    pub fn stats(&self) -> &WindowStats {
        &self.stats
    }

    pub fn shared(&self) -> &Shared {
        &self.shared
    }

    /// The next day's world, or `None` after the last.
    pub fn next_world(&mut self) -> Option<Result<World, AiVillageError>> {
        let (day, lines) = self.days.get_mut(self.next)?;
        let day = *day;
        let lines = std::mem::take(lines);
        self.next += 1;
        self.stats.days += 1;
        Some(day::build(&self.shared, day, &lines, &mut self.stats))
    }
}

/// Tags every bash command's accesses, per agent in time order, and counts
/// GUI edits.
fn tag(
    sessions: &HashMap<String, String>,
    buckets: &BTreeMap<Day, Vec<String>>,
    log: &mut AccessLog,
    stats: &mut WindowStats,
) -> Result<(), AiVillageError> {
    for (day, lines) in buckets {
        let mut turns: Vec<(Timestamp, String, TurnRow)> = Vec::new();
        for line in lines {
            let row: TurnRow = decode(Table::ComputerUseTurns, line)?;
            let (Ok(at), Some(agent)) = (
                super::time::parse_timestamp(&row.created_at),
                sessions.get(&row.session_id),
            ) else {
                continue;
            };
            turns.push((at, agent.clone(), row));
        }
        turns.sort_by(|a, b| (&a.1, a.0, &a.2.id).cmp(&(&b.1, b.0, &b.2.id)));
        for (at, agent, row) in &turns {
            let action = row.agent_action.as_ref();
            if let Some(command) = action
                .and_then(|a| a.get("command"))
                .and_then(serde_json::Value::as_str)
            {
                let output = calls::output_text(row.output.as_deref(), row.error.as_deref());
                log.command(
                    TurnRef {
                        agent,
                        turn: &row.id,
                        session: &row.session_id,
                        at: *at,
                        day: *day,
                    },
                    command,
                    &output,
                );
            } else {
                let response = provider::response(&row.agent_messages, &row.id);
                stats
                    .gui
                    .count(action, &visible_text(&response.into_body()));
            }
        }
    }
    log.finish();
    Ok(())
}
