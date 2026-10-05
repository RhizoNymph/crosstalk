//! Chat room membership over time.
//!
//! Before rooms existed (2026-02-25, "Rooms v1") every agent was in
//! `#general`. After that an agent is in the room of its latest event that
//! names one: every agent action carries `roomId`, and `ENTER_ROOM` moves
//! it. An agent with no event before a time is taken to be in the room of
//! its first event after it.

use std::collections::BTreeMap;

use crosstalk_spec::support::Timestamp;

/// 2026-02-25T00:00:00Z: rooms v1.
pub const ROOMS_V1_MICROS: u64 = 1_771_977_600_000_000;

/// Each agent's rooms over time.
#[derive(Debug, Clone, Default)]
pub struct RoomTimeline {
    by_agent: BTreeMap<String, Vec<(Timestamp, String)>>,
    before: BTreeMap<String, (Timestamp, String)>,
    general: Option<String>,
}

impl RoomTimeline {
    /// Records that `agent` was in `room` at `at`. Before `from` only the
    /// latest record per agent is kept.
    pub fn record(&mut self, agent: &str, at: Timestamp, room: &str, from: Timestamp) {
        if at < from {
            if self.before.get(agent).is_none_or(|(seen, _)| *seen <= at) {
                self.before.insert(agent.to_owned(), (at, room.to_owned()));
            }
            return;
        }
        self.by_agent
            .entry(agent.to_owned())
            .or_default()
            .push((at, room.to_owned()));
    }

    /// Sorts the records; call once after the last `record`.
    pub fn finish(&mut self) {
        for (agent, (at, room)) in std::mem::take(&mut self.before) {
            self.by_agent.entry(agent).or_default().push((at, room));
        }
        for records in self.by_agent.values_mut() {
            records.sort();
            records.dedup_by(|b, a| a.1 == b.1);
        }
    }

    /// Names the `#general` room (where everyone was before rooms v1).
    pub fn set_general(&mut self, room: &str) {
        self.general = Some(room.to_owned());
    }

    pub fn general(&self) -> Option<&str> {
        self.general.as_deref()
    }

    /// The room `agent` was in at `at`.
    pub fn room_of(&self, agent: &str, at: Timestamp) -> Option<&str> {
        if at.as_micros() < ROOMS_V1_MICROS {
            return self.general();
        }
        let Some(records) = self.by_agent.get(agent) else {
            return self.general();
        };
        let end = records.partition_point(|(seen, _)| *seen <= at);
        match end.checked_sub(1) {
            Some(i) => records.get(i).map(|(_, room)| room.as_str()),
            None => records.first().map(|(_, room)| room.as_str()),
        }
    }
}
