//! What the swarm and the fake model agree on: the one tool
//! (`http_request`, the HTTP tool contract L5 recognises), the wiki's page
//! URLs, page names, topics, and the task marker a user turn ends with.
//!
//! The swarm plays the user: each turn's prompt is prose followed by one
//! marker line, `[task:write page=<slug> topic=<n> base=<wiki url>]`,
//! `[task:read page=<slug> base=<wiki url>]` or `[task:chat topic=<n>]`.
//! The fake model plays an obedient model: it reads the marker of the last
//! user turn and answers with the matching `http_request` call against the
//! wiki at `base` (or prose), so the swarm's knobs decide how often the
//! wiki is written and read while the words still come from the model.
//!
//! Every page URL, in a tool call, in the agent's HTTP call and in the
//! ground truth, comes from [`page_url`], so it is one string everywhere.
//!
//! The run's [`Scenario`] reaches the fake model the same way: each agent's
//! system prompt ends with a style marker, `[style:headline]` or
//! `[style:boilerplate]`, and the model picks its prose generator from it.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;
use serde_json::{Value, json};

use crate::http::BaseUrl;

/// The one tool every swarm request declares: an HTTP request,
/// `{"method", "url", "body"?}`.
pub const HTTP_TOOL: &str = "http_request";

/// The `tools` array every swarm request declares.
pub fn tool_definitions() -> Value {
    json!([
        {
            "name": HTTP_TOOL,
            "description": "Send an HTTP request. The team wiki serves pages at <wiki>/pages/<name>: \
                            GET returns the page text, PUT with a body creates or replaces it.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["GET", "PUT"],
                        "description": "The HTTP method"
                    },
                    "url": {"type": "string", "description": "The absolute URL, e.g. http://wiki:8090/pages/rate-limiting-3"},
                    "body": {"type": "string", "description": "The request body (the full page text for a PUT)"}
                },
                "required": ["method", "url"],
                "additionalProperties": false
            }
        }
    ])
}

/// The URL of `page` on the wiki at `base`: `<base>/pages/<page>`, no
/// trailing slash, no query. The only place a page URL is built.
pub fn page_url(base: &BaseUrl, page: &PageSlug) -> String {
    format!("{}/pages/{page}", base.url().trim_end_matches('/'))
}

/// The `http_request` input that reads `page`.
pub fn read_input(base: &BaseUrl, page: &PageSlug) -> Value {
    json!({"method": "GET", "url": page_url(base, page)})
}

/// The `http_request` input that writes `body` to `page`.
pub fn write_input(base: &BaseUrl, page: &PageSlug, body: &str) -> Value {
    json!({"method": "PUT", "url": page_url(base, page), "body": body})
}

/// A tool call the swarm can run against the wiki.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WikiCall {
    Read { page: PageSlug },
    Write { page: PageSlug, body: String },
}

/// Why a tool call is not one the wiki can answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallRefused {
    #[error("no tool named {0}")]
    UnknownTool(String),
    #[error("`method` must be text")]
    NoMethod,
    #[error("method {0} is not supported; use GET or PUT")]
    Method(String),
    #[error("`url` must be text")]
    NoUrl,
    #[error("{url} is not a page of the team wiki ({base}/pages/<name>)")]
    NotWiki { url: String, base: String },
    #[error("a PUT needs a text `body`")]
    NoBody,
}

impl WikiCall {
    /// Reads the tool call `name(input)` as a wiki call against `base`.
    pub fn parse(name: &str, input: &Value, base: &BaseUrl) -> Result<Self, CallRefused> {
        if name != HTTP_TOOL {
            return Err(CallRefused::UnknownTool(name.to_owned()));
        }
        let method = input
            .get("method")
            .and_then(Value::as_str)
            .ok_or(CallRefused::NoMethod)?;
        let url = input
            .get("url")
            .and_then(Value::as_str)
            .ok_or(CallRefused::NoUrl)?;
        let not_wiki = || CallRefused::NotWiki {
            url: url.to_owned(),
            base: base.url(),
        };
        let page: PageSlug = url
            .strip_prefix(base.url().trim_end_matches('/'))
            .and_then(|rest| rest.strip_prefix("/pages/"))
            .ok_or_else(not_wiki)?
            .parse()
            .map_err(|_| not_wiki())?;
        if method.eq_ignore_ascii_case("GET") {
            Ok(WikiCall::Read { page })
        } else if method.eq_ignore_ascii_case("PUT") {
            let body = input
                .get("body")
                .and_then(Value::as_str)
                .ok_or(CallRefused::NoBody)?;
            Ok(WikiCall::Write {
                page,
                body: body.to_owned(),
            })
        } else {
            Err(CallRefused::Method(method.to_owned()))
        }
    }
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

/// Which benchmark a swarm run is, and so which prose the fake model
/// writes for it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// High-entropy prose: unrelated outputs share no long run of bytes,
    /// so every shared span is a real copy. The headline benchmark.
    #[default]
    Headline,
    /// Templated prose: unrelated outputs share template fragments, as
    /// real agents share boilerplate. A regression scenario for false
    /// positives on shared text.
    Boilerplate,
}

/// Why a scenario name is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0:?} is not a scenario (headline or boilerplate)")]
pub struct BadScenario(pub String);

impl Scenario {
    pub const ALL: [Scenario; 2] = [Scenario::Headline, Scenario::Boilerplate];

    /// The wire name: `headline` or `boilerplate`.
    pub const fn name(self) -> &'static str {
        match self {
            Scenario::Headline => "headline",
            Scenario::Boilerplate => "boilerplate",
        }
    }

    /// The marker that ends an agent's system prompt.
    pub fn marker(self) -> String {
        format!("[style:{}]", self.name())
    }

    /// The scenario a system prompt asks for: boilerplate only when it
    /// carries `[style:boilerplate]`; headline otherwise, with or without a
    /// marker.
    pub fn of_system(text: &str) -> Scenario {
        if text.contains(&Scenario::Boilerplate.marker()) {
            Scenario::Boilerplate
        } else {
            Scenario::Headline
        }
    }
}

impl FromStr for Scenario {
    type Err = BadScenario;

    fn from_str(text: &str) -> Result<Self, BadScenario> {
        Scenario::ALL
            .into_iter()
            .find(|scenario| scenario.name() == text)
            .ok_or_else(|| BadScenario(text.to_owned()))
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What one user turn asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Task {
    /// Answer in prose about a topic.
    Chat { topic: u32 },
    /// Write the findings on `topic` to `page` of the wiki at `base`.
    Write {
        page: PageSlug,
        topic: u32,
        base: BaseUrl,
    },
    /// Read `page` of the wiki at `base` and use it.
    Read { page: PageSlug, base: BaseUrl },
}

impl Task {
    /// The marker line closing the user turn.
    pub fn marker(&self) -> String {
        match self {
            Task::Chat { topic } => format!("[task:chat topic={topic}]"),
            Task::Write { page, topic, base } => {
                format!("[task:write page={page} topic={topic} base={}]", base.url())
            }
            Task::Read { page, base } => format!("[task:read page={page} base={}]", base.url()),
        }
    }

    /// The whole user prompt: a sentence of prose, then the marker.
    pub fn prompt(&self) -> String {
        let prose = match self {
            Task::Chat { topic } => format!(
                "What should we look at next on {}? Keep it short.",
                Topic::of(*topic).label
            ),
            Task::Write { page, topic, .. } => format!(
                "Please write up your current findings on {} in the team wiki, page `{page}`.",
                Topic::of(*topic).label
            ),
            Task::Read { page, .. } => format!(
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
        let mut base = None;
        for word in words {
            match word.split_once('=')? {
                ("page", value) => page = Some(value.parse::<PageSlug>().ok()?),
                ("topic", value) => topic = Some(value.parse::<u32>().ok()?),
                ("base", value) => base = Some(value.parse::<BaseUrl>().ok()?),
                _ => return None,
            }
        }
        match kind {
            "chat" if page.is_none() && base.is_none() => Some(Task::Chat { topic: topic? }),
            "write" => Some(Task::Write {
                page: page?,
                topic: topic?,
                base: base?,
            }),
            "read" if topic.is_none() => Some(Task::Read {
                page: page?,
                base: base?,
            }),
            _ => None,
        }
    }
}
