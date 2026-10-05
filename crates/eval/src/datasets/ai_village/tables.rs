//! Reusable passes over the tables.
//!
//! Each function makes one streaming pass ([`Table::scan`]) and keeps only
//! what a time window needs, so a caller can compose the passes it wants
//! (the converter here; topology demos later). Rows outside the window are
//! dropped from the raw line's `created_at` before any JSON is decoded.
//!
//! | Pass | Keeps |
//! | --- | --- |
//! | [`load_directory`] | agents (id, name, model) and chat rooms |
//! | [`load_sessions`] | computer-use session → agent |
//! | [`scan_turns`] | each computer-use turn in the window, as its raw line |
//! | [`scan_events`] | events in the window (filtered), and the room every agent was last seen in before it |
//! | [`scan_chat`] | chat messages in the window |
//! | [`scan_memories`] | each agent's memories in the window plus its latest one before it |
//! | [`load_goals`] | village goals and per-agent goals |

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crosstalk_spec::support::Timestamp;

use super::AiVillageError;
use super::rooms::RoomTimeline;
use super::schema::{
    AgentGoalRow, AgentRow, ChatRow, EventRow, MemoryRow, RoomRow, SessionRow, VillageGoalRow,
};
use super::stream::{StreamError, Table, created_at, decode, string_field};
use super::time::{Window, parse_timestamp};

/// One agent of the village.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInfo {
    pub id: String,
    pub name: String,
    pub model: String,
}

/// Agents and rooms, by id.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    pub agents: BTreeMap<String, AgentInfo>,
    pub rooms: BTreeMap<String, String>,
    by_name: BTreeMap<String, String>,
}

impl Directory {
    pub fn new(agents: Vec<AgentRow>, rooms: Vec<RoomRow>) -> Self {
        let mut directory = Self::default();
        for agent in agents {
            directory
                .by_name
                .insert(agent.name.clone(), agent.id.clone());
            directory.agents.insert(
                agent.id.clone(),
                AgentInfo {
                    model: agent.model_string.unwrap_or_else(|| "unknown".to_owned()),
                    id: agent.id,
                    name: agent.name,
                },
            );
        }
        directory.rooms = rooms.into_iter().map(|room| (room.id, room.name)).collect();
        directory
    }

    pub fn agent(&self, id: &str) -> Option<&AgentInfo> {
        self.agents.get(id)
    }

    pub fn by_name(&self, name: &str) -> Option<&AgentInfo> {
        self.by_name.get(name).and_then(|id| self.agents.get(id))
    }

    /// A room's name, or its id when unknown.
    pub fn room_name<'a>(&'a self, id: &'a str) -> &'a str {
        self.rooms.get(id).map_or(id, String::as_str)
    }
}

pub fn load_directory(root: &Path) -> Result<Directory, AiVillageError> {
    let agents: Vec<AgentRow> = Table::Agents.load(root)?;
    let rooms: Vec<RoomRow> = Table::ChatRooms.load(root)?;
    Ok(Directory::new(agents, rooms))
}

/// Computer-use session id → agent id.
pub fn load_sessions(root: &Path) -> Result<HashMap<String, String>, AiVillageError> {
    let mut sessions = HashMap::new();
    Table::ComputerUseSessions.scan::<AiVillageError>(root, |line| {
        let row: SessionRow = decode(Table::ComputerUseSessions, line)?;
        sessions.insert(row.id, row.agent_id);
        Ok(())
    })?;
    Ok(sessions)
}

/// The row's time: from the raw line when it parses, else from the decoded
/// `created_at` text.
fn row_time(line: &str, decoded: impl FnOnce() -> Option<String>) -> Option<Timestamp> {
    created_at(line).or_else(|| decoded().and_then(|text| parse_timestamp(&text).ok()))
}

/// Calls `visit(time, line)` for every computer-use turn in `window`, as
/// its raw line (decode it with [`decode`] when needed).
pub fn scan_turns(
    root: &Path,
    window: Window,
    mut visit: impl FnMut(Timestamp, &str),
) -> Result<u64, AiVillageError> {
    let mut kept = 0u64;
    Table::ComputerUseTurns.scan::<AiVillageError>(root, |line| {
        let at = row_time(line, || {
            decode::<super::schema::TurnRow>(Table::ComputerUseTurns, line)
                .ok()
                .map(|row| row.created_at)
        });
        if let Some(at) = at
            && window.contains(at)
        {
            kept += 1;
            visit(at, line);
        }
        Ok(())
    })?;
    Ok(kept)
}

/// Events in a window, and where every agent was before it.
#[derive(Debug, Clone, Default)]
pub struct EventScan {
    /// Kept events in the window, by time (ties by event index).
    pub events: Vec<(Timestamp, EventRow)>,
    /// Room membership: every agent's last room before the window and each
    /// change inside it.
    pub rooms: RoomTimeline,
}

/// One pass over the events: those in `window` that `keep` accepts, plus
/// the room timeline (from every event carrying an agent and a room up to
/// the window's end).
pub fn scan_events(
    root: &Path,
    window: Window,
    mut keep: impl FnMut(&EventRow) -> bool,
) -> Result<EventScan, AiVillageError> {
    let mut scan = EventScan::default();
    Table::Events.scan::<AiVillageError>(root, |line| {
        let at = created_at(line);
        if at.is_some_and(|at| at >= window.to) {
            return Ok(());
        }
        let row: EventRow = decode(Table::Events, line)?;
        let Some(at) = at.or_else(|| parse_timestamp(&row.created_at).ok()) else {
            return Ok(());
        };
        if let (Some(agent), Some(room)) = (row.agent(), row.str("roomId")) {
            // A move says where the agent was until then.
            if row.action() == Some("ENTER_ROOM")
                && let Some(previous) = row.str("previousRoomId")
            {
                let before = Timestamp::from_micros(at.as_micros().saturating_sub(1));
                scan.rooms.record(agent, before, previous, window.from);
            }
            scan.rooms.record(agent, at, room, window.from);
        }
        if window.contains(at) && keep(&row) {
            scan.events.push((at, row));
        }
        Ok(())
    })?;
    scan.events
        .sort_by(|(a, x), (b, y)| (a, x.event_index, &x.id).cmp(&(b, y.event_index, &y.id)));
    scan.rooms.finish();
    Ok(scan)
}

/// Events whose id is in `ids`, at any time.
pub fn events_by_id(
    root: &Path,
    ids: &std::collections::HashSet<String>,
) -> Result<HashMap<String, EventRow>, AiVillageError> {
    let mut out = HashMap::new();
    Table::Events.scan::<AiVillageError>(root, |line| {
        // The dump writes `id` first; any other order is decoded to find it.
        if let Some(id) = leading_id(line)
            && !ids.contains(id)
        {
            return Ok(());
        }
        let row: EventRow = decode(Table::Events, line)?;
        if ids.contains(&row.id) {
            out.insert(row.id.clone(), row);
        }
        Ok(())
    })?;
    Ok(out)
}

/// The row's `id` when the line starts with it, as the dump writes rows.
pub fn leading_id(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("{\"id\":\"")?;
    rest.find('"').map(|end| &rest[..end])
}

/// Chat messages in `window`, by time.
pub fn scan_chat(root: &Path, window: Window) -> Result<Vec<(Timestamp, ChatRow)>, AiVillageError> {
    let mut out = Vec::new();
    Table::ChatMessages.scan::<AiVillageError>(root, |line| {
        if created_at(line).is_some_and(|at| !window.contains(at)) {
            return Ok(());
        }
        let row: ChatRow = decode(Table::ChatMessages, line)?;
        if let Ok(at) = parse_timestamp(&row.created_at)
            && window.contains(at)
        {
            out.push((at, row));
        }
        Ok(())
    })?;
    out.sort_by(|(a, x), (b, y)| (a, &x.id).cmp(&(b, &y.id)));
    Ok(out)
}

/// Each agent's memories, by time: the latest before a window and all in
/// it.
#[derive(Debug, Clone, Default)]
pub struct Memories {
    by_agent: BTreeMap<String, Vec<(Timestamp, String)>>,
}

impl Memories {
    pub fn insert(&mut self, agent: &str, at: Timestamp, content: String) {
        self.by_agent
            .entry(agent.to_owned())
            .or_default()
            .push((at, content));
    }

    fn finish(&mut self) {
        for memories in self.by_agent.values_mut() {
            memories.sort_by_key(|memory| memory.0);
        }
    }

    /// The agent's latest memory written at or before `at`.
    pub fn latest(&self, agent: &str, at: Timestamp) -> Option<&str> {
        let memories = self.by_agent.get(agent)?;
        let end = memories.partition_point(|(written, _)| *written <= at);
        end.checked_sub(1)
            .and_then(|i| memories.get(i))
            .map(|(_, content)| content.as_str())
    }
}

pub fn scan_memories(root: &Path, window: Window) -> Result<Memories, AiVillageError> {
    let mut memories = Memories::default();
    let mut before: BTreeMap<String, (Timestamp, String)> = BTreeMap::new();
    Table::AgentMemories.scan::<AiVillageError>(root, |line| {
        let at = created_at(line);
        if at.is_some_and(|at| at >= window.to) {
            return Ok(());
        }
        if let (Some(at), Some(agent)) = (at, string_field(line, "agent_id"))
            && at < window.from
            && before.get(agent).is_some_and(|(seen, _)| *seen >= at)
        {
            return Ok(());
        }
        let row: MemoryRow = decode(Table::AgentMemories, line)?;
        let Ok(at) = parse_timestamp(&row.created_at) else {
            return Ok(());
        };
        if at >= window.to {
            return Ok(());
        }
        if at < window.from {
            if before.get(&row.agent_id).is_none_or(|(seen, _)| *seen < at) {
                before.insert(row.agent_id, (at, row.content));
            }
        } else {
            memories.insert(&row.agent_id, at, row.content);
        }
        Ok(())
    })?;
    for (agent, (at, content)) in before {
        memories.insert(&agent, at, content);
    }
    memories.finish();
    Ok(memories)
}

/// A goal with its optional start and end.
type Dated = (Option<Timestamp>, Option<Timestamp>, String);

/// The village's goals and each agent's own.
#[derive(Debug, Clone, Default)]
pub struct Goals {
    village: Vec<Dated>,
    agents: BTreeMap<String, Vec<Dated>>,
}

fn active(start: Option<Timestamp>, end: Option<Timestamp>, at: Timestamp) -> bool {
    start.is_none_or(|start| start <= at) && end.is_none_or(|end| at < end)
}

impl Goals {
    pub fn new(village: Vec<VillageGoalRow>, agents: Vec<AgentGoalRow>) -> Self {
        let time = |text: &Option<String>| text.as_deref().and_then(|t| parse_timestamp(t).ok());
        let mut goals = Self::default();
        for goal in village {
            goals
                .village
                .push((time(&goal.start_time), time(&goal.end_time), goal.goal));
        }
        goals
            .village
            .sort_by(|a, b| (a.0, a.1, &a.2).cmp(&(b.0, b.1, &b.2)));
        for goal in agents {
            let text = match goal.description.as_deref() {
                Some(description) if !description.is_empty() => {
                    format!("{}: {description}", goal.name)
                }
                _ => goal.name.clone(),
            };
            goals.agents.entry(goal.agent_id).or_default().push((
                time(&goal.start_time),
                time(&goal.end_time),
                text,
            ));
        }
        goals
    }

    /// The village goal active at `at` (the latest started).
    pub fn village(&self, at: Timestamp) -> Option<&str> {
        self.village
            .iter()
            .rfind(|(start, end, _)| active(*start, *end, at))
            .map(|(_, _, goal)| goal.as_str())
    }

    /// The agent's goals active at `at`.
    pub fn agent(&self, agent: &str, at: Timestamp) -> Vec<&str> {
        self.agents
            .get(agent)
            .into_iter()
            .flatten()
            .filter(|(start, end, _)| active(*start, *end, at))
            .map(|(_, _, goal)| goal.as_str())
            .collect()
    }
}

pub fn load_goals(root: &Path) -> Result<Goals, AiVillageError> {
    let village: Vec<VillageGoalRow> = Table::VillageGoals.load(root)?;
    let agents: Vec<AgentGoalRow> = match Table::AgentGoals.load(root) {
        Ok(rows) => rows,
        Err(StreamError::Io { .. }) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    Ok(Goals::new(village, agents))
}
