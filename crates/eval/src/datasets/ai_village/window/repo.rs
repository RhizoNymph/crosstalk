//! Repository and web channels: who wrote and read which resource, and the
//! cross-agent pairs that become heuristic labels.
//!
//! Every bash turn in the window is tagged ([`super::super::access`]) in
//! time order, each agent with its own [`Shell`]. A **pair** is a read whose
//! latest earlier write to the same canonical resource came from another
//! agent. A pair becomes a label (Heuristic tier, `Channel` route on the
//! resource, `ToolResult` carrier) when the read's output holds a line of
//! the write's payload (at least [`MIN_LINE`] bytes and [`MIN_WORD_CHARS`]
//! letters or digits); the label sits at that line, at the reader's next
//! call in the same session (the first whose request carries the output).
//! Other pairs are co-accesses only: the spec would hold them as suspected,
//! and the eval has no label kind for them, so they are counted.
//!
//! A `Rejected` write (the spec's `WriteOutcome`) is recorded and counted
//! but never pairs, and does not hide an earlier write; `Unknown` writes
//! pair like `Delivered` ones.

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use crosstalk_spec::derived::flow::access::WriteOutcome;

use super::super::access::{Access, Op, Shell};
use super::super::time::Day;
use crate::truth::kinds::locator_key;

pub const MIN_LINE: usize = 24;
pub const MIN_WORD_CHARS: usize = 20;

/// One access, with the turn that made it.
#[derive(Debug, Clone, PartialEq)]
pub struct AccessRecord {
    pub agent: String,
    pub turn: String,
    pub session: String,
    pub at: Timestamp,
    pub day: Day,
    pub access: Access,
}

/// A read and the other agent's write it may have read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pair {
    pub write: usize,
    pub read: usize,
}

/// Counts over the access log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessStats {
    pub reads: u64,
    pub writes: u64,
    pub delivered_writes: u64,
    pub rejected_writes: u64,
    pub unknown_writes: u64,
    /// Accesses with an `http_request` equivalent (L5's `HttpTool` would
    /// see them).
    pub http_visible: u64,
    /// Accesses only a Bash extractor could see (git, forge CLI issue and
    /// review commands).
    pub bash_only: u64,
    pub by_verb: BTreeMap<String, u64>,
    pub resources: u64,
    pub pairs: u64,
    pub pairs_cross_day: u64,
}

/// Every access in the window, in time order, and the cross-agent pairs.
#[derive(Debug, Clone, Default)]
pub struct AccessLog {
    pub records: Vec<AccessRecord>,
    pub pairs: Vec<Pair>,
    pub stats: AccessStats,
    shells: HashMap<String, Shell>,
}

/// The turn a command ran in.
#[derive(Debug, Clone, Copy)]
pub struct TurnRef<'a> {
    pub agent: &'a str,
    pub turn: &'a str,
    pub session: &'a str,
    pub at: Timestamp,
    pub day: Day,
}

impl AccessLog {
    /// Tags one bash command (call in time order per agent).
    pub fn command(&mut self, turn: TurnRef<'_>, command: &str, output: &str) {
        let shell = self.shells.entry(turn.agent.to_owned()).or_default();
        for access in shell.accesses(command, output) {
            self.records.push(AccessRecord {
                agent: turn.agent.to_owned(),
                turn: turn.turn.to_owned(),
                session: turn.session.to_owned(),
                at: turn.at,
                day: turn.day,
                access,
            });
        }
    }

    /// Orders the records and finds the pairs; call once after the last
    /// command.
    pub fn finish(&mut self) {
        self.records
            .sort_by(|a, b| (a.at, &a.agent, &a.turn).cmp(&(b.at, &b.agent, &b.turn)));
        let mut latest_write: HashMap<String, usize> = HashMap::new();
        let mut resources = std::collections::HashSet::new();
        for (index, record) in self.records.iter().enumerate() {
            let key = locator_key(&record.access.resource);
            resources.insert(key.clone());
            *self
                .stats
                .by_verb
                .entry(record.access.verb.clone())
                .or_default() += 1;
            if record.access.http_visible() {
                self.stats.http_visible += 1;
            } else {
                self.stats.bash_only += 1;
            }
            match record.access.op {
                Op::Write(outcome) => {
                    self.stats.writes += 1;
                    match outcome {
                        WriteOutcome::Delivered => self.stats.delivered_writes += 1,
                        WriteOutcome::Rejected => self.stats.rejected_writes += 1,
                        WriteOutcome::Unknown => self.stats.unknown_writes += 1,
                    }
                    if outcome.pairs() {
                        latest_write.insert(key, index);
                    }
                }
                Op::Read => {
                    self.stats.reads += 1;
                    if let Some(&write) = latest_write.get(&key)
                        && self.records[write].agent != record.agent
                    {
                        self.stats.pairs += 1;
                        if self.records[write].day != record.day {
                            self.stats.pairs_cross_day += 1;
                        }
                        self.pairs.push(Pair { write, read: index });
                    }
                }
            }
        }
        self.stats.resources = resources.len() as u64;
    }
}

fn word_chars(text: &str) -> usize {
    text.chars().filter(|c| c.is_alphanumeric()).count()
}

/// The longest payload line (long and wordy enough) found verbatim in
/// `output`, with its byte range there.
pub fn payload_line(payload: &[String], output: &str) -> Option<(String, usize, usize)> {
    let mut best: Option<(String, usize, usize)> = None;
    for value in payload {
        for line in value.lines() {
            let line = line.trim();
            if line.len() < MIN_LINE || word_chars(line) < MIN_WORD_CHARS {
                continue;
            }
            if best
                .as_ref()
                .is_some_and(|(found, _, _)| found.len() >= line.len())
            {
                continue;
            }
            if let Some(start) = output.find(line) {
                best = Some((line.to_owned(), start, start + line.len()));
            }
        }
    }
    best
}
