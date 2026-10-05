//! Ground truth, schema v2: what the swarm knows happened, from the one
//! place that sees both sides of every wiki read (the collector).
//!
//! Agents report a [`WriteRecord`] for every page version the wiki accepted
//! and a [`ReadRecord`] for every read once its result is in a request
//! being sent. A [`TruthBook`] pairs each read with the write of the page
//! version it got and classifies it:
//!
//! - `miss`: the page did not exist;
//! - `self_read`: the reader wrote that version itself;
//! - `reread`: another agent wrote it, and this reader already read this
//!   page version earlier in the same session;
//! - `transmission`: another agent wrote it, first read in this session.
//!
//! A read whose write has not been reported yet waits for it; one whose
//! write never arrives (a version written before this run, or by a writer
//! cut off before reporting) is written at the end as `unattributed_read`:
//! its content is known, its writer is not, so a scorer can leave a
//! detection of it unjudged instead of counting it as a false positive.
//!
//! [`Row`] is the file's JSON-lines schema: a `header` first, one
//! `agent_cluster` per key group, then, in the order they happen, a
//! `session` row when each conversation starts (so every session the
//! gateway sees maps to its agent, wiki traffic or not) and a row per read.

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::Serialize;
use serde_json::Value;
use sha2::Digest;

use crate::protocol::{HTTP_TOOL, PageSlug};

/// The schema version in the header.
pub const VERSION: u32 = 2;
/// The longest excerpt, in characters.
pub const EXCERPT_CHARS: usize = 80;

/// What the header records about the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunInfo {
    /// A ULID, minted at start.
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

impl RunInfo {
    /// `swarm-<run>`.
    pub fn world(&self) -> String {
        format!("swarm-{}", self.run)
    }
}

/// A page version the wiki accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteRecord {
    pub writer: String,
    pub key_group: u32,
    pub session: String,
    /// The writer's request whose response held the PUT tool_use.
    pub turn: u32,
    pub tool_use_id: String,
    pub page: PageSlug,
    pub version: u64,
    /// When the wiki's answer to the PUT arrived.
    pub written_at_unix_ms: u64,
}

/// Where a tool result sits in the reader's request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct At {
    pub message: usize,
    pub block: usize,
    pub tool_use_id: String,
}

/// The text a read delivered, as the reader's request carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Content {
    pub blake3: String,
    pub sha256: String,
    pub excerpt: String,
    pub at: At,
}

impl Content {
    /// Digests and an excerpt of `text`, the tool_result content string.
    pub fn of(text: &str, at: At) -> Self {
        Self {
            blake3: blake3::hash(text.as_bytes()).to_hex().to_string(),
            sha256: hex(&sha2::Sha256::digest(text.as_bytes())),
            excerpt: excerpt(text),
            at,
        }
    }
}

/// Lower-case hex.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 0xf)]])
        .map(char::from)
        .collect()
}

/// A substring of `text` of at most [`EXCERPT_CHARS`] characters, taken
/// from a third of the way in (past the generic opening of a page) and
/// starting at a word: generated pages practically never share it.
pub fn excerpt(text: &str) -> String {
    let chars = text.chars().count();
    if chars <= EXCERPT_CHARS {
        return text.to_owned();
    }
    let skip = (chars / 3).min(chars - EXCERPT_CHARS);
    let mut start = text.char_indices().nth(skip).map_or(0, |(i, _)| i);
    // Move to the start of the next word when one begins soon enough.
    if let Some(space) = text[start..].find(' ') {
        let candidate = start + space + 1;
        if text[candidate..].chars().count() >= EXCERPT_CHARS {
            start = candidate;
        }
    }
    let piece: String = text[start..].chars().take(EXCERPT_CHARS).collect();
    piece.trim_end().to_owned()
}

/// What a read got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The page did not exist.
    Missing,
    /// The page's text, by `author` (the wiki's header), at `version`.
    Found {
        author: String,
        version: u64,
        content: Content,
    },
}

/// Who read what, and when: the half of a read row every kind shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    pub reader: String,
    pub key_group: u32,
    pub session: String,
    /// The first request carrying the result.
    pub turn: u32,
    /// The GET tool_use's id.
    pub tool_use_id: String,
    pub page: PageSlug,
    /// The page's URL ([`crate::protocol::page_url`]).
    pub url: String,
    /// The GET tool_use's input, as the model sent it.
    pub input: Value,
    /// Since the run started, when the wiki's answer arrived.
    pub at_ms: u64,
    pub read_at_unix_ms: u64,
}

impl Reader {
    fn read_tool(&self) -> ReadTool {
        ReadTool {
            name: HTTP_TOOL.to_owned(),
            input: self.input.clone(),
        }
    }
}

/// A read whose result is in a request being sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRecord {
    pub by: Reader,
    pub outcome: ReadOutcome,
}

/// How content travelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Route {
    Channel { url: String },
}

/// Where the content sits in the reader's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Carrier {
    ToolResult,
}

/// The tool call that read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadTool {
    pub name: String,
    pub input: Value,
}

/// The header line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Header {
    pub version: u32,
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

/// A read that delivered another version's text: a transmission, a
/// self-read or a reread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivered {
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
    pub route: Route,
    pub carrier: Carrier,
    pub read_tool: ReadTool,
    pub content: Content,
    pub at_ms: u64,
    pub at_unix_ms: u64,
    pub written_at_unix_ms: u64,
    pub read_at_unix_ms: u64,
}

/// A read of a page that did not exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

/// A read of a version whose write this run never saw: written when the
/// run ends, after every other row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Unattributed {
    pub world: String,
    pub reader: String,
    pub reader_key_group: u32,
    pub page: String,
    pub version: u64,
    pub reader_session: String,
    pub reader_turn: u32,
    pub reader_tool_use_id: String,
    pub read_tool: ReadTool,
    pub content: Content,
    pub at_ms: u64,
    pub at_unix_ms: u64,
}

/// A conversation starting: its session id names one agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionRow {
    pub world: String,
    pub agent: String,
    pub key_group: u32,
    pub session: String,
    pub started_at_unix_ms: u64,
}

/// The agents sharing one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Cluster {
    pub world: String,
    pub key_group: u32,
    pub agents: Vec<String>,
}

/// One line of the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Row {
    Header(Header),
    Transmission(Delivered),
    SelfRead(Delivered),
    Reread(Delivered),
    Miss(Miss),
    UnattributedRead(Unattributed),
    AgentCluster(Cluster),
    Session(SessionRow),
}

impl Row {
    /// The header, then one cluster per key group (singletons included);
    /// `name` names agent `i`.
    pub fn opening(info: &RunInfo, name: impl Fn(u32) -> String) -> Vec<Row> {
        let world = info.world();
        let mut rows = vec![Row::Header(Header {
            version: VERSION,
            world: world.clone(),
            run: info.run.clone(),
            seed: info.seed,
            agents: info.agents,
            keys: info.keys,
            agents_per_key: info.agents_per_key,
            claude_code_shape: info.claude_code_shape,
            started_at_unix_ms: info.started_at_unix_ms,
            gateway_url: info.gateway_url.clone(),
            wiki_url: info.wiki_url.clone(),
        })];
        let per = info.agents_per_key.max(1);
        for group in 0..info.keys {
            let first = group.saturating_mul(per);
            let last = first.saturating_add(per).min(info.agents);
            rows.push(Row::AgentCluster(Cluster {
                world: world.clone(),
                key_group: group,
                agents: (first..last).map(&name).collect(),
            }));
        }
        rows
    }
}

/// The counts the report shows; they equal the file's rows of each kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub transmissions: u64,
    pub self_reads: u64,
    pub rereads: u64,
    pub misses: u64,
    /// Reads of a version whose write this run never reported.
    pub unattributed: u64,
    /// Conversations started (one `session` row each).
    pub sessions: u64,
}

type VersionKey = (PageSlug, u64);

/// A found read waiting for its write.
#[derive(Debug, Clone)]
struct Found {
    by: Reader,
    author: String,
    content: Content,
    /// Whether its session had read that version before.
    seen_before: bool,
}

/// Pairs reads with writes and classifies them.
#[derive(Debug)]
pub struct TruthBook {
    world: String,
    writes: HashMap<VersionKey, WriteRecord>,
    /// Per session, the versions already read.
    seen: HashMap<String, HashSet<VersionKey>>,
    waiting: HashMap<VersionKey, Vec<Found>>,
    pairs: BTreeSet<(String, String)>,
    counts: Counts,
}

impl TruthBook {
    pub fn new(world: String) -> Self {
        Self {
            world,
            writes: HashMap::new(),
            seen: HashMap::new(),
            waiting: HashMap::new(),
            pairs: BTreeSet::new(),
            counts: Counts::default(),
        }
    }

    pub fn counts(&self) -> Counts {
        self.counts
    }

    /// Distinct writer -> reader pairs over transmissions.
    pub fn pairs(&self) -> usize {
        self.pairs.len()
    }

    /// Records a write; returns the rows of reads that waited for it.
    pub fn write(&mut self, write: WriteRecord) -> Vec<Row> {
        let key = (write.page.clone(), write.version);
        let waiting = self.waiting.remove(&key).unwrap_or_default();
        let rows = waiting
            .into_iter()
            .map(|found| self.classify(&write, found))
            .collect();
        self.writes.insert(key, write);
        rows
    }

    /// Records a read; returns its row unless it waits for its write.
    pub fn read(&mut self, read: ReadRecord) -> Option<Row> {
        let ReadRecord { by, outcome } = read;
        let (author, version, content) = match outcome {
            ReadOutcome::Missing => {
                self.counts.misses += 1;
                return Some(Row::Miss(Miss {
                    world: self.world.clone(),
                    read_tool: by.read_tool(),
                    reader: by.reader,
                    reader_key_group: by.key_group,
                    page: by.page.to_string(),
                    reader_session: by.session,
                    reader_turn: by.turn,
                    reader_tool_use_id: by.tool_use_id,
                    at_ms: by.at_ms,
                    at_unix_ms: by.read_at_unix_ms,
                }));
            }
            ReadOutcome::Found {
                author,
                version,
                content,
            } => (author, version, content),
        };
        let key = (by.page.clone(), version);
        let seen_before = !self
            .seen
            .entry(by.session.clone())
            .or_default()
            .insert(key.clone());
        let found = Found {
            by,
            author,
            content,
            seen_before,
        };
        match self.writes.get(&key).cloned() {
            Some(write) => Some(self.classify(&write, found)),
            None => {
                self.waiting.entry(key).or_default().push(found);
                None
            }
        }
    }

    /// A conversation started; returns its `session` row.
    pub fn start_session(
        &mut self,
        agent: String,
        key_group: u32,
        session: String,
        started_at_unix_ms: u64,
    ) -> Row {
        self.counts.sessions += 1;
        Row::Session(SessionRow {
            world: self.world.clone(),
            agent,
            key_group,
            session,
            started_at_unix_ms,
        })
    }

    /// A conversation ended: nothing more is read in it.
    pub fn end_session(&mut self, session: &str) {
        self.seen.remove(session);
    }

    /// Ends the book: reads still waiting for their write are unattributed.
    /// Returns their rows in read order (time, then tool_use id).
    pub fn finish(&mut self) -> Vec<Row> {
        let mut left: Vec<Unattributed> = self
            .waiting
            .drain()
            .flat_map(|((_, version), waiting)| {
                waiting.into_iter().map(move |found| (version, found))
            })
            .map(|(version, found)| {
                let by = found.by;
                Unattributed {
                    world: self.world.clone(),
                    read_tool: by.read_tool(),
                    reader: by.reader,
                    reader_key_group: by.key_group,
                    page: by.page.to_string(),
                    version,
                    reader_session: by.session,
                    reader_turn: by.turn,
                    reader_tool_use_id: by.tool_use_id,
                    content: found.content,
                    at_ms: by.at_ms,
                    at_unix_ms: by.read_at_unix_ms,
                }
            })
            .collect();
        left.sort_by(|a, b| {
            (a.at_ms, &a.reader_tool_use_id).cmp(&(b.at_ms, &b.reader_tool_use_id))
        });
        self.counts.unattributed += left.len() as u64;
        left.into_iter().map(Row::UnattributedRead).collect()
    }

    fn classify(&mut self, write: &WriteRecord, found: Found) -> Row {
        let Found {
            by,
            author,
            content,
            seen_before,
        } = found;
        if author != write.writer {
            tracing::warn!(
                page = %by.page,
                author = %author,
                writer = %write.writer,
                "the wiki's author header disagrees with the reported write"
            );
        }
        let delivered = Delivered {
            world: self.world.clone(),
            writer: write.writer.clone(),
            read_tool: by.read_tool(),
            reader: by.reader,
            page: by.page.to_string(),
            version: write.version,
            writer_key_group: write.key_group,
            reader_key_group: by.key_group,
            writer_session: write.session.clone(),
            writer_turn: write.turn,
            writer_tool_use_id: write.tool_use_id.clone(),
            reader_session: by.session,
            reader_turn: by.turn,
            reader_tool_use_id: by.tool_use_id,
            route: Route::Channel { url: by.url },
            carrier: Carrier::ToolResult,
            content,
            at_ms: by.at_ms,
            at_unix_ms: by.read_at_unix_ms,
            written_at_unix_ms: write.written_at_unix_ms,
            read_at_unix_ms: by.read_at_unix_ms,
        };
        if delivered.writer == delivered.reader {
            self.counts.self_reads += 1;
            Row::SelfRead(delivered)
        } else if seen_before {
            self.counts.rereads += 1;
            Row::Reread(delivered)
        } else {
            self.counts.transmissions += 1;
            self.pairs
                .insert((delivered.writer.clone(), delivered.reader.clone()));
            Row::Transmission(delivered)
        }
    }
}
