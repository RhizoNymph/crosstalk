//! Repository and web channels: who wrote and read which shared resource,
//! and the cross-agent pairs that become heuristic labels.
//!
//! Every bash turn in the window goes through L5's extractor
//! ([`super::super::access`]) in time order, each agent with its own
//! [`Shell`]. A **pair** is a read whose latest earlier write to the same
//! resource (the same extractor locator) came from another agent. What a
//! pair becomes depends on what the write carried:
//!
//! - **Content** (`Payload::Authored`): a label (Heuristic, `Channel` on
//!   the resource, `ToolResult` carrier) when the read's output holds a
//!   line of the write's authored text (at least [`MIN_LINE`] bytes and
//!   [`MIN_WORD_CHARS`] letters or digits), at that line: a file written
//!   in a clone and read from another clone or a raw URL of the same
//!   repository file, an issue comment read back. Without such a line it
//!   is a co-access only, counted.
//! - **Access only** (`Payload::Unseen`, a `git push`): the gateway
//!   records the write without spans, so co-access alone links it, and the
//!   spec keeps that Suspected. The pair is an access-only label on the
//!   repository, over the read's whole output.
//!
//! Either label sits at the reader's next call in the same session (the
//! first whose request carries the output). A `Rejected` write (the
//! spec's `WriteOutcome`) is recorded and counted but never pairs, and
//! does not hide an earlier write; `Unknown` writes pair like `Delivered`
//! ones.

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use crosstalk_spec::derived::flow::access::WriteOutcome;

use super::super::access::{Access, Op, Payload, Shell};
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

/// What links a pair: content the writer typed, or the co-access alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    /// The write carried authored text a reader can get back.
    Content,
    /// The write's content is not in its call (`git push`).
    AccessOnly,
}

/// A read and the other agent's write it may have read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pair {
    pub write: usize,
    pub read: usize,
    pub link: Link,
}

/// Counts over the access log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessStats {
    pub reads: u64,
    pub writes: u64,
    pub delivered_writes: u64,
    pub rejected_writes: u64,
    pub unknown_writes: u64,
    /// Writes whose content is not in the call (`git push`).
    pub unseen_writes: u64,
    /// Accesses by `<read|write> <resource kind>`.
    pub by_kind: BTreeMap<String, u64>,
    pub resources: u64,
    pub pairs: u64,
    /// Pairs whose write is a `git push`: access-only.
    pub pairs_access_only: u64,
    pub pairs_cross_day: u64,
    /// Bash commands the extractor refused.
    pub unextracted_commands: u64,
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
            *self.stats.by_kind.entry(record.access.label()).or_default() += 1;
            match &record.access.op {
                Op::Write { outcome, payload } => {
                    self.stats.writes += 1;
                    match outcome {
                        WriteOutcome::Delivered => self.stats.delivered_writes += 1,
                        WriteOutcome::Rejected => self.stats.rejected_writes += 1,
                        WriteOutcome::Unknown => self.stats.unknown_writes += 1,
                    }
                    if *payload == Payload::Unseen {
                        self.stats.unseen_writes += 1;
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
                        let link = match &self.records[write].access.op {
                            Op::Write {
                                payload: Payload::Unseen,
                                ..
                            } => Link::AccessOnly,
                            _ => Link::Content,
                        };
                        self.stats.pairs += 1;
                        if link == Link::AccessOnly {
                            self.stats.pairs_access_only += 1;
                        }
                        if self.records[write].day != record.day {
                            self.stats.pairs_cross_day += 1;
                        }
                        self.pairs.push(Pair {
                            write,
                            read: index,
                            link,
                        });
                    }
                }
            }
        }
        self.stats.resources = resources.len() as u64;
        self.stats.unextracted_commands = self.shells.values().map(Shell::unextracted).sum();
    }
}

fn word_chars(text: &str) -> usize {
    text.chars().filter(|c| c.is_alphanumeric()).count()
}

/// The longest line of `payload` (long and wordy enough) found verbatim in
/// `output`, with its byte range there. Only an `Authored` payload has
/// lines.
pub fn payload_line(payload: &Payload, output: &str) -> Option<(String, usize, usize)> {
    let Payload::Authored(payload) = payload else {
        return None;
    };
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
