//! Join failures between the truth file, the exchange log and the
//! gateway's export, as a typed table. Nothing that fails to join is
//! dropped silently: every failure is one [`Diagnostic`] saying what failed
//! and what became of the row.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crosstalk_spec::ids::{AgentId, ExchangeId, TransmissionId};
use serde::Serialize;

use super::truth_file::DeliveryKind;

/// The kind of truth row a diagnostic is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowKind {
    Session,
    Transmission,
    SelfRead,
    Reread,
    Miss,
    UnattributedRead,
    AgentCluster,
}

impl From<DeliveryKind> for RowKind {
    fn from(kind: DeliveryKind) -> Self {
        match kind {
            DeliveryKind::Transmission => Self::Transmission,
            DeliveryKind::SelfRead => Self::SelfRead,
            DeliveryKind::Reread => Self::Reread,
        }
    }
}

/// Which side of the row failed to join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Reader,
    Writer,
    Row,
}

/// What failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "failure", rename_all = "snake_case")]
pub enum JoinFailure {
    /// No exchange in the log carries this session id.
    UnknownSession { session: String },
    /// The session has fewer exchanges than the turn, and no exchange of
    /// it holds the tool use.
    TurnOutOfRange {
        session: String,
        turn: u32,
        exchanges: usize,
    },
    /// The tool use is in the session, but at another ordinal than the
    /// truth's turn. The row was joined to the exchange holding it.
    TurnMismatch {
        session: String,
        turn: u32,
        found_turn: u32,
        exchange: ExchangeId,
    },
    /// The tool use is where the truth says, but its bytes do not hash to
    /// `content.blake3`.
    HashMismatch {
        session: String,
        turn: u32,
        tool_use_id: String,
        exchange: ExchangeId,
    },
    /// No exchange of the session holds the tool use.
    ToolUseMissing {
        session: String,
        turn: u32,
        tool_use_id: String,
    },
    /// The writer's tool call is not a `PUT` with a string body.
    NotAPut {
        session: String,
        tool_use_id: String,
        exchange: ExchangeId,
    },
    /// A body the exchange names could not be read.
    Body {
        exchange: ExchangeId,
        reason: String,
    },
    /// The route's URL is not a URL a locator can be made from.
    BadUrl { url: String, reason: String },
    /// The checked label constructor refused the row.
    InvalidLabel { reason: String },
    /// One session id is claimed by two agents in the truth.
    SessionConflict {
        session: String,
        agents: Vec<String>,
    },
    /// A key group of one agent, or agents sharing a key: not an
    /// `AgentCluster` (agents that are one agent), so not labelled.
    KeyGroupNotACluster { key_group: u32, agents: usize },
    /// An exported transmission with no evidence in the evidence file.
    MissingEvidence { transmission: TransmissionId },
    /// A detected agent id no exchange of the log could be tied to.
    UnknownDetectedAgent {
        transmission: TransmissionId,
        agent: AgentId,
    },
    /// One detected agent id tied to two truth agents.
    DetectedAgentConflict { agent: AgentId, agents: Vec<String> },
    /// A transmission the export's records can't turn into predictions
    /// (an access the evidence doesn't carry, or one of the wrong kind).
    Unpredictable {
        transmission: TransmissionId,
        reason: String,
    },
}

impl JoinFailure {
    /// The variant's name, for the table.
    pub fn name(&self) -> &'static str {
        match self {
            Self::UnknownSession { .. } => "unknown_session",
            Self::TurnOutOfRange { .. } => "turn_out_of_range",
            Self::TurnMismatch { .. } => "turn_mismatch",
            Self::HashMismatch { .. } => "hash_mismatch",
            Self::ToolUseMissing { .. } => "tool_use_missing",
            Self::NotAPut { .. } => "not_a_put",
            Self::Body { .. } => "body",
            Self::BadUrl { .. } => "bad_url",
            Self::InvalidLabel { .. } => "invalid_label",
            Self::SessionConflict { .. } => "session_conflict",
            Self::KeyGroupNotACluster { .. } => "key_group_not_a_cluster",
            Self::MissingEvidence { .. } => "missing_evidence",
            Self::UnknownDetectedAgent { .. } => "unknown_detected_agent",
            Self::DetectedAgentConflict { .. } => "detected_agent_conflict",
            Self::Unpredictable { .. } => "unpredictable",
        }
    }
}

/// What became of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// No label or control was made from the row.
    Dropped,
    /// The label was made, joined as the failure says.
    Kept,
    /// The label was made without a sender exchange.
    KeptWithoutSender,
    /// Predictions of a detected transmission were left out.
    PredictionsDropped,
    /// Reported only; nothing was left out.
    Noted,
}

/// One join failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Diagnostic {
    /// The truth line (from 1), when the failure is about one.
    pub line: Option<usize>,
    pub row: Option<RowKind>,
    pub side: Side,
    #[serde(flatten)]
    pub failure: JoinFailure,
    pub effect: Effect,
}

/// Every diagnostic of a run.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct Diagnostics {
    pub entries: Vec<Diagnostic>,
}

/// One row of the summary table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct DiagnosticCount {
    pub row: Option<RowKind>,
    pub side: Side,
    pub failure: &'static str,
    pub effect: Effect,
    pub count: u64,
}

impl Diagnostics {
    pub fn push(&mut self, diagnostic: Diagnostic) {
        self.entries.push(diagnostic);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The entries with failure `name`.
    pub fn named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Diagnostic> + 'a {
        self.entries
            .iter()
            .filter(move |entry| entry.failure.name() == name)
    }

    /// Counts by row kind, side, failure and effect, in that order.
    pub fn table(&self) -> Vec<DiagnosticCount> {
        let mut counts: BTreeMap<(Option<RowKind>, Side, &'static str, Effect), u64> =
            BTreeMap::new();
        for entry in &self.entries {
            *counts
                .entry((entry.row, entry.side, entry.failure.name(), entry.effect))
                .or_default() += 1;
        }
        counts
            .into_iter()
            .map(|((row, side, failure, effect), count)| DiagnosticCount {
                row,
                side,
                failure,
                effect,
                count,
            })
            .collect()
    }

    /// The table as Markdown.
    pub fn render(&self) -> String {
        let mut out = String::from("join diagnostics\n\n");
        if self.entries.is_empty() {
            out.push_str("none: every row joined\n");
            return out;
        }
        out.push_str(
            "| row | side | failure | effect | count |\n| --- | --- | --- | --- | ---: |\n",
        );
        for count in self.table() {
            let row = count.row.map_or_else(|| "-".to_owned(), |row| snake(&row));
            let _ = writeln!(
                out,
                "| {row} | {} | {} | {} | {} |",
                snake(&count.side),
                count.failure,
                snake(&count.effect),
                count.count
            );
        }
        out
    }
}

fn snake(value: &impl Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => text,
        _ => "?".to_owned(),
    }
}
