//! What the swarm and the fake model agree on: the two wiki tools, page
//! names, topics, and the task marker a user turn ends with.
//!
//! The swarm plays the user: each turn's prompt is prose followed by one
//! marker line, `[task:write page=<slug> topic=<n>]`, `[task:read
//! page=<slug>]` or `[task:chat topic=<n>]`. The fake model plays an
//! obedient model: it reads the marker of the last user turn and answers
//! with the matching tool call (or prose), so the swarm's knobs decide how
//! often the wiki is written and read while the words still come from the
//! model.

use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};

/// The tool that writes a wiki page: `{"page": slug, "content": text}`.
pub const WIKI_WRITE: &str = "wiki_write";
/// The tool that reads a wiki page: `{"page": slug}`.
pub const WIKI_READ: &str = "wiki_read";

/// The `tools` array every swarm request declares.
pub fn tool_definitions() -> Value {
    json!([
        {
            "name": WIKI_READ,
            "description": "Read a page of the team wiki. Returns the page text.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "page": {"type": "string", "description": "The page name, e.g. rate-limiting-3"}
                },
                "required": ["page"],
                "additionalProperties": false
            }
        },
        {
            "name": WIKI_WRITE,
            "description": "Create or replace a page of the team wiki.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "page": {"type": "string", "description": "The page name"},
                    "content": {"type": "string", "description": "The full page text"}
                },
                "required": ["page", "content"],
                "additionalProperties": false
            }
        }
    ])
}

/// A wiki page name: 1 to 96 of `a-z`, `0-9` and `-`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageSlug(String);

/// Why a page name is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not a page name (1-96 of a-z, 0-9, -)")]
pub struct BadSlug(pub String);

impl PageSlug {
    pub const MAX_LEN: usize = 96;

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `index`th page of the page space: its topic's slug and number.
    pub fn of_page(index: u32, topics: u32) -> Self {
        let topic = Topic::of(index % topics.max(1));
        Self(format!("{}-{}", topic.slug, index))
    }
}

impl FromStr for PageSlug {
    type Err = BadSlug;

    fn from_str(text: &str) -> Result<Self, BadSlug> {
        let ok = !text.is_empty()
            && text.len() <= Self::MAX_LEN
            && text
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if ok {
            Ok(Self(text.to_owned()))
        } else {
            Err(BadSlug(text.to_owned()))
        }
    }
}

impl fmt::Display for PageSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A subject agents write about. The first [`TOPICS`]`.len()` have names
/// and vocabulary; later ones reuse them with a number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub index: u32,
    pub slug: String,
    pub label: String,
    pub terms: &'static [&'static str],
}

/// `(slug, label, terms)` of the named topics.
pub const TOPICS: &[(&str, &str, &[&str])] = &[
    (
        "rate-limiting",
        "rate limiting",
        &[
            "token bucket",
            "burst budget",
            "429 responses",
            "per-tenant quota",
            "leaky bucket",
            "retry-after header",
        ],
    ),
    (
        "vacuum-tuning",
        "Postgres vacuum tuning",
        &[
            "autovacuum",
            "dead tuples",
            "freeze age",
            "table bloat",
            "cost delay",
            "visibility map",
        ],
    ),
    (
        "cache-invalidation",
        "cache invalidation",
        &[
            "TTL",
            "write-through",
            "stampede",
            "versioned keys",
            "stale reads",
            "purge queue",
        ],
    ),
    (
        "incident-review",
        "the incident review",
        &[
            "timeline",
            "root cause",
            "pager alert",
            "rollback",
            "blast radius",
            "follow-up items",
        ],
    ),
    (
        "schema-migration",
        "the schema migration",
        &[
            "backfill",
            "dual writes",
            "online index build",
            "lock timeout",
            "expand and contract",
            "column default",
        ],
    ),
    (
        "search-ranking",
        "search ranking",
        &[
            "BM25",
            "query rewrite",
            "click model",
            "recall",
            "reranker",
            "embedding drift",
        ],
    ),
    (
        "auth-rotation",
        "credential rotation",
        &[
            "key overlap",
            "token expiry",
            "revocation list",
            "signing key",
            "grace window",
            "audit trail",
        ],
    ),
    (
        "queue-backpressure",
        "queue backpressure",
        &[
            "consumer lag",
            "bounded buffer",
            "dead letters",
            "visibility timeout",
            "redelivery",
            "shed load",
        ],
    ),
    (
        "release-plan",
        "the release plan",
        &[
            "feature flag",
            "canary",
            "staged rollout",
            "changelog",
            "freeze window",
            "smoke test",
        ],
    ),
    (
        "cost-report",
        "the cloud cost report",
        &[
            "reserved instances",
            "egress",
            "idle volumes",
            "rightsizing",
            "spot capacity",
            "tagging gaps",
        ],
    ),
    (
        "latency-budget",
        "the latency budget",
        &[
            "p99",
            "tail latency",
            "fan-out",
            "timeout chain",
            "hedged requests",
            "cold start",
        ],
    ),
    (
        "data-retention",
        "data retention",
        &[
            "retention window",
            "legal hold",
            "partition drop",
            "tombstones",
            "export job",
            "deletion proof",
        ],
    ),
    (
        "onboarding-docs",
        "the onboarding docs",
        &[
            "setup script",
            "first PR",
            "glossary",
            "service map",
            "runbook links",
            "access requests",
        ],
    ),
    (
        "gpu-scheduling",
        "GPU scheduling",
        &[
            "bin packing",
            "preemption",
            "MIG slices",
            "queue fairness",
            "memory headroom",
            "batch size",
        ],
    ),
    (
        "api-versioning",
        "API versioning",
        &[
            "deprecation notice",
            "sunset header",
            "compat shim",
            "breaking change",
            "client SDK",
            "version pin",
        ],
    ),
    (
        "observability",
        "observability",
        &[
            "trace sampling",
            "cardinality",
            "log levels",
            "dashboards",
            "SLO burn rate",
            "exemplars",
        ],
    ),
];

/// Words any topic's sentences use.
const COMMON: &[&str] = &[
    "the staging cluster",
    "last week's numbers",
    "the on-call notes",
    "our current design",
    "the open question",
    "the second experiment",
    "the dashboard",
    "the team",
];

impl Topic {
    /// The topic numbered `index`.
    pub fn of(index: u32) -> Self {
        let count = TOPICS.len() as u32;
        let (slug, label, terms) = TOPICS[(index % count) as usize];
        let round = index / count;
        if round == 0 {
            Self {
                index,
                slug: slug.to_owned(),
                label: label.to_owned(),
                terms,
            }
        } else {
            Self {
                index,
                slug: format!("{slug}{round}"),
                label: format!("{label} (track {round})"),
                terms,
            }
        }
    }

    /// Generic phrases every topic's text may use.
    pub fn common() -> &'static [&'static str] {
        COMMON
    }
}

/// What one user turn asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Task {
    /// Answer in prose about a topic.
    Chat { topic: u32 },
    /// Write the findings on `topic` to `page`.
    Write { page: PageSlug, topic: u32 },
    /// Read `page` and use it.
    Read { page: PageSlug },
}

impl Task {
    /// The marker line closing the user turn.
    pub fn marker(&self) -> String {
        match self {
            Task::Chat { topic } => format!("[task:chat topic={topic}]"),
            Task::Write { page, topic } => format!("[task:write page={page} topic={topic}]"),
            Task::Read { page } => format!("[task:read page={page}]"),
        }
    }

    /// The whole user prompt: a sentence of prose, then the marker.
    pub fn prompt(&self) -> String {
        let prose = match self {
            Task::Chat { topic } => format!(
                "What should we look at next on {}? Keep it short.",
                Topic::of(*topic).label
            ),
            Task::Write { page, topic } => format!(
                "Please write up your current findings on {} in the team wiki, page `{page}`.",
                Topic::of(*topic).label
            ),
            Task::Read { page } => format!(
                "Before you continue, read the wiki page `{page}` and tell me what matters for us."
            ),
        };
        format!("{prose}\n\n{}", self.marker())
    }

    /// The task of the last marker in `text`, if any.
    pub fn find(text: &str) -> Option<Task> {
        text.lines().rev().find_map(|line| Self::parse(line.trim()))
    }

    /// One marker line.
    pub fn parse(line: &str) -> Option<Task> {
        let inner = line.strip_prefix("[task:")?.strip_suffix(']')?;
        let mut words = inner.split_whitespace();
        let kind = words.next()?;
        let mut page = None;
        let mut topic = None;
        for word in words {
            match word.split_once('=')? {
                ("page", value) => page = Some(value.parse::<PageSlug>().ok()?),
                ("topic", value) => topic = Some(value.parse::<u32>().ok()?),
                _ => return None,
            }
        }
        match kind {
            "chat" => Some(Task::Chat { topic: topic? }),
            "write" => Some(Task::Write {
                page: page?,
                topic: topic?,
            }),
            "read" => Some(Task::Read { page: page? }),
            _ => None,
        }
    }
}
