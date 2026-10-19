//! The demo swarm's ground-truth file, version 2: one JSON object per line,
//! tagged by `kind`, written by `swarm --ground-truth PATH` (crates/demo).
//!
//! ```text
//! {"kind":"header","version":2,"scenario":"headline",…}         first line, once
//! {"kind":"session",…}        a conversation starting: its session id and agent
//! {"kind":"transmission",…}   another agent's page version reached the reader
//! {"kind":"self_read",…}      the same fields; writer == reader (always, even on a repeat read)
//! {"kind":"reread",…}         the same fields; another agent's version the reader already
//!                             read earlier in the same session
//! {"kind":"miss",…}           a read that found no page
//! {"kind":"agent_cluster",…}  the agents sharing one API key
//! {"kind":"unattributed_read",…} a read whose write was never logged (at the end)
//! ```
//!
//! Decoding is strict: an unknown `kind`, an unknown field or a missing
//! one is an error, and only `version` 2 is read.
//!
//! - `*_turn` is the 0-based ordinal of the generation requests (`POST
//!   /v1/messages`) the agent sent in that session, failed and retried ones
//!   included.
//! - `writer_turn` is the request whose response held the `PUT`
//!   `http_request` tool use; `reader_turn` is the reader's first request
//!   carrying the `GET`'s tool result.
//! - Names are opaque (`agent-NNN`, `<topic>-<n>` today); nothing parses
//!   them. `run` is a ULID; `at_unix_ms` is `started_at_unix_ms + at_ms`;
//!   `written_at_unix_ms` is when the `PUT`'s response reached the writer.
//! - A read whose write event never arrived is an `unattributed_read`
//!   row: the reader side and the content, no writer. No row is written
//!   for a read whose follow-up request was never sent, or a failed `PUT`.
//! - The header's `scenario` (`headline` or `boilerplate`) is optional;
//!   missing means `headline`. A run is written under the dataset id
//!   `demo-swarm/<scenario>` ([`Scenario::dataset`]).
//! - A `session` row is written once per conversation, when it starts.
//!   It is the primary agent ↔ session map: a conversation that no read,
//!   write or miss row names (one that only talked to the model) still
//!   belongs to its agent.
//! - `content.blake3` and `content.sha256` hash the page body's bytes,
//!   which are exactly the tool result's content; `content.at` indexes the
//!   wire `messages` array of the reader's request.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::DATASET_PREFIX;

/// The one version this reader understands.
pub const VERSION: u32 = 2;

/// One line of the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TruthLine {
    Header(Header),
    Session(SessionStart),
    Transmission(Delivery),
    SelfRead(Delivery),
    Reread(Delivery),
    Miss(Miss),
    UnattributedRead(UnattributedRead),
    AgentCluster(KeyGroup),
}

/// The run the file describes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Header {
    pub version: u32,
    /// Which swarm scenario ran, as written; [`Header::scenario`] reads
    /// it (missing: [`Scenario::Headline`]).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_scenario"
    )]
    pub scenario: Option<Scenario>,
    pub world: String,
    pub run: String,
    pub seed: u64,
    pub agents: u32,
    pub keys: u32,
    pub agents_per_key: u32,
    pub claude_code_shape: bool,
    pub started_at_unix_ms: u64,
    pub gateway_url: String,
    pub wiki_url: String,
}

/// A `scenario` key that is present names a scenario: `null` is refused,
/// only a missing key means the default.
fn present_scenario<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Scenario>, D::Error> {
    Scenario::deserialize(deserializer).map(Some)
}

impl Header {
    /// The run's scenario: [`Scenario::Headline`] when the header names
    /// none.
    pub fn scenario(&self) -> Scenario {
        self.scenario.unwrap_or_default()
    }
}

/// The swarm scenario a run used: what its agents were prompted to write.
/// Each agent's system prompt carries the tag `[style:<scenario>]`,
/// identical across agents: shared prompt text, never a label.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// The headline bench: agents pass distinct content through the wiki.
    #[default]
    Headline,
    /// Agents write template-heavy boilerplate: a false-positive bench.
    Boilerplate,
}

impl Scenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Headline => "headline",
            Self::Boilerplate => "boilerplate",
        }
    }

    /// The dataset id a run of this scenario is written under (and the
    /// bench scores it under): `demo-swarm/<scenario>`.
    pub fn dataset(self) -> String {
        format!("{DATASET_PREFIX}/{}", self.as_str())
    }
}

/// A conversation starting: the harness session id (`x-claude-code-session-id`)
/// and the agent it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SessionStart {
    pub world: String,
    pub agent: String,
    pub key_group: u32,
    pub session: String,
    pub started_at_unix_ms: u64,
}

/// A read of a page version someone wrote: a transmission, a self-read or
/// a reread, by the line's kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Delivery {
    pub world: String,
    pub writer: String,
    pub reader: String,
    pub page: String,
    pub version: u64,
    pub writer_key_group: u32,
    pub reader_key_group: u32,
    pub writer_session: String,
    pub writer_turn: u32,
    pub writer_tool_use_id: String,
    pub reader_session: String,
    pub reader_turn: u32,
    pub reader_tool_use_id: String,
    pub route: TruthRoute,
    pub carrier: TruthCarrier,
    pub read_tool: ReadTool,
    pub content: Content,
    pub at_ms: u64,
    pub at_unix_ms: u64,
    pub written_at_unix_ms: u64,
    pub read_at_unix_ms: u64,
}

/// A read that found no page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Miss {
    pub world: String,
    pub reader: String,
    pub reader_key_group: u32,
    pub page: String,
    pub reader_session: String,
    pub reader_turn: u32,
    pub reader_tool_use_id: String,
    pub read_tool: ReadTool,
    pub at_ms: u64,
    pub at_unix_ms: u64,
}

/// A read of a page version whose write the swarm never logged: content
/// from another agent, writer unknown. Written at the run's end, after
/// every other row, in read order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct UnattributedRead {
    pub world: String,
    pub reader: String,
    pub reader_key_group: u32,
    pub page: String,
    /// The version the wiki's response header reported.
    pub version: u64,
    pub reader_session: String,
    pub reader_turn: u32,
    pub reader_tool_use_id: String,
    pub read_tool: ReadTool,
    pub content: Content,
    pub at_ms: u64,
    pub at_unix_ms: u64,
}

/// The agents that share one API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct KeyGroup {
    pub world: String,
    pub key_group: u32,
    pub agents: Vec<String>,
}

/// How the page travelled: through the wiki page's canonical URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TruthRoute {
    Channel { url: String },
}

/// Where the page sits in the reader's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruthCarrier {
    ToolResult,
}

/// The reader's tool call, as it made it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ReadTool {
    pub name: String,
    pub input: serde_json::Value,
}

/// The page body that was read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Content {
    pub blake3: HexDigest,
    pub sha256: HexDigest,
    /// About 80 characters of the body, for reports.
    pub excerpt: String,
    pub at: WireAt,
}

/// Where the tool result sits in the reader's request as sent: an index
/// into the wire `messages` array (a mid-array system turn included under
/// `claude_code_shape`) and a block within it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct WireAt {
    pub message: u32,
    pub block: u32,
    pub tool_use_id: String,
}

/// A 32-byte digest, written as 64 lower- or upper-case hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct HexDigest([u8; 32]);

impl HexDigest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The BLAKE3 digest of `bytes`.
    pub fn blake3_of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub fn parse(text: &str) -> Result<Self, InvalidDigest> {
        let bytes = text.as_bytes();
        if bytes.len() != 64 {
            return Err(InvalidDigest::Length(bytes.len()));
        }
        let mut out = [0u8; 32];
        for (at, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
            let high = nibble(pair[0]).ok_or(InvalidDigest::NotHex)?;
            let low = nibble(pair[1]).ok_or(InvalidDigest::NotHex)?;
            out[at] = (high << 4) | low;
        }
        Ok(Self(out))
    }
}

fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidDigest {
    #[error("a digest is 64 hex digits, not {0} characters")]
    Length(usize),
    #[error("a digest holds only hex digits")]
    NotHex,
}

impl fmt::Display for HexDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for HexDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HexDigest({self})")
    }
}

impl Serialize for HexDigest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HexDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}
