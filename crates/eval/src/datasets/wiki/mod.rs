//! collusion-wiki: real agent swarms using public wikis as dead drops.
//!
//! The export (`revisions.jsonl.gz`, `pages.jsonl.gz`) records page edits on
//! public UseMod/ProWiki wikis that AI agents wrote to and read from. There
//! are no model calls, so exchanges are **synthesised** ([`Fidelity::Synthetic`]),
//! in the agreed L5 `HttpTool` shape ([`tools`]):
//!
//! - Each agent **identity** (the chosen username, or the saving `/16`
//!   address when the username is blank) is one agent.
//! - Each revision is a **write**: an `http_request` `POST` of the page URL
//!   whose `body` is the lines that revision inserted, so that text is
//!   originated there.
//! - When a revision's author differs from the previous author of the page,
//!   a **read** is synthesised just before the edit: an `http_request` `GET`
//!   of the same URL whose tool result is the page body as of the previous
//!   revision. This is the read-before-edit assumption: an agent that edits
//!   a page after another agent must have fetched it first.
//! - Exchanges take the shape a real harness gives them ([`build`]): each
//!   agent is one continuous conversation, a call is the response of one
//!   exchange and its tool result arrives in the agent's next request, and
//!   calls are one paced step apart ([`Pace`]).
//!
//! One [`World`] is one connected component of the agent–page graph (agents
//! linked by a page they both edited), so a world is a set of agents that
//! could only have reached each other through the pages they share.
//!
//! Expected transmissions are Heuristic tier, Channel route: the resource is
//! the page's public [`Locator::Url`], the carrier a `ToolResult`, and the
//! edge runs from each earlier distinct author whose inserted lines are
//! still present in the body the reader read. A relay (the reader's own edit
//! quoting an earlier author's line) is also expected, at a
//! [`CarrierKind::ReaderOutput`].
//!
//! [`Fidelity::Synthetic`]: crate::corpus::Fidelity
//! [`Locator::Url`]: crosstalk_spec::derived::flow::resource::Locator
//! [`CarrierKind::ReaderOutput`]: crate::truth::CarrierKind

pub mod attribution;
pub mod build;
pub mod resource;
pub mod schema;
pub mod tally;
pub mod tools;

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::Path;

use flate2::read::GzDecoder;

use crate::corpus::clock::Pace;
use crate::corpus::{CorpusError, SourceError, TraceSource, World};
use crate::keys::{DatasetId, WorldKey};
use attribution::AttributionError;
use schema::{Page, Revision};
pub use tally::{FamilyStats, FamilyTally};

/// The dataset's id.
pub const DATASET: &str = "collusion-wiki";

/// The logical source file revisions are cited from (ids derive from it).
const REVISIONS_FILE: &str = "revisions.jsonl";

#[derive(Debug, thiserror::Error)]
pub enum WikiError {
    #[error("{root} has no {file}")]
    Missing { root: String, file: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} line {line} is not a wiki record: {source}")]
    Json {
        path: String,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("attribution: {0}")]
    Attribution(#[from] AttributionError),
    #[error("virtual clock: {0}")]
    Clock(#[source] crate::corpus::clock::ClockError),
    #[error("tool-call arguments are not valid JSON: {0}")]
    Arguments(#[source] crosstalk_spec::observed::message::json::JsonError),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
    #[error("label: {0}")]
    Label(#[from] crate::truth::InvalidLabel),
    #[error("location: {0}")]
    Location(#[from] crate::location::LocationError),
}

/// Which worlds a run reads.
#[derive(Debug, Clone, Default)]
pub struct WikiSelection {
    /// Keep only pages in these task clusters (any when empty).
    pub families: Vec<String>,
    /// Keep only pages on these wikis (any when empty).
    pub wikis: Vec<String>,
    /// Keep only worlds with at least this many agents.
    pub min_agents: Option<usize>,
    /// Drop worlds with more than this many agents (the giant swarm
    /// component is costly to score; this bounds a demo run).
    pub max_agents: Option<usize>,
    /// Emit at most this many worlds (the largest first).
    pub limit: Option<usize>,
}

impl WikiSelection {
    /// The demo subset: the five largest relay-coordination worlds of 2 to
    /// 12 agents, the M2 "undeclared public wiki channel" case at a size a
    /// demo can show end to end.
    pub fn demo() -> Self {
        Self {
            families: vec!["relay-coordination".to_owned()],
            wikis: Vec::new(),
            min_agents: Some(2),
            max_agents: Some(12),
            limit: Some(5),
        }
    }

    fn keep_page(&self, wiki: &str, family: Option<&str>) -> bool {
        let family_ok = self.families.is_empty()
            || family.is_some_and(|f| self.families.iter().any(|want| want == f));
        let wiki_ok = self.wikis.is_empty() || self.wikis.iter().any(|want| want == wiki);
        family_ok && wiki_ok
    }
}

/// One world's revisions: indices into [`WikiSource::revisions`], already in
/// the world's processing order (by time, then page, then seq).
#[derive(Debug, Clone)]
struct WorldSpec {
    key: WorldKey,
    revisions: Vec<usize>,
    agents: usize,
}

/// collusion-wiki as a stream of worlds, one per connected component.
pub struct WikiSource {
    revisions: Vec<Revision>,
    worlds: Vec<WorldSpec>,
    families: FamilyTally,
    pace: Pace,
}

impl WikiSource {
    /// Reads the export under `root` and plans one world per connected
    /// component of the agent–page graph, after `selection`.
    pub fn open(root: &Path, selection: &WikiSelection) -> Result<Self, WikiError> {
        let families = read_page_families(root)?;
        let mut revisions = read_revisions(root)?;
        revisions.retain(|rev| {
            selection.keep_page(
                &rev.wiki,
                families.get(&rev.page_id).and_then(Option::as_deref),
            )
        });
        let worlds = plan_worlds(&revisions, selection);
        let families = FamilyTally::new(&revisions, |page| {
            families.get(page).and_then(Option::as_deref)
        });
        Ok(Self {
            revisions,
            worlds,
            families,
            pace: Pace::DEFAULT,
        })
    }

    /// These worlds with calls `pace` apart.
    pub fn with_pace(mut self, pace: Pace) -> Self {
        self.pace = pace;
        self
    }

    /// Pages and multi-author pages per `page_family`, over the selected
    /// pages (before the world-size filters and cap).
    pub fn families(&self) -> &FamilyTally {
        &self.families
    }

    /// How many worlds were planned.
    pub fn world_count(&self) -> usize {
        self.worlds.len()
    }
}

impl TraceSource for WikiSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let revisions = &self.revisions;
        let pace = self.pace;
        self.worlds.iter().map(move |spec| {
            let revs: Vec<&Revision> = spec.revisions.iter().map(|&at| &revisions[at]).collect();
            build::world(spec.key.clone(), &revs, pace)
                .map_err(|error| SourceError::from(Box::new(error)))
        })
    }
}

/// Reads `revisions.jsonl[.gz]` into revisions, in file order.
fn read_revisions(root: &Path) -> Result<Vec<Revision>, WikiError> {
    let bytes = read_member(root, REVISIONS_FILE)?;
    parse_lines(&bytes, REVISIONS_FILE)
}

/// Reads `pages.jsonl[.gz]` into a page-id → task-cluster map.
fn read_page_families(root: &Path) -> Result<BTreeMap<String, Option<String>>, WikiError> {
    let bytes = read_member(root, "pages.jsonl")?;
    let pages: Vec<Page> = parse_lines(&bytes, "pages.jsonl")?;
    Ok(pages
        .into_iter()
        .map(|page| (page.page_id, page.page_family))
        .collect())
}

/// The bytes of `name` or `name.gz` under `root`, decompressed.
fn read_member(root: &Path, name: &str) -> Result<Vec<u8>, WikiError> {
    let gz = root.join(format!("{name}.gz"));
    let plain = root.join(name);
    if gz.exists() {
        let raw = fs::read(&gz).map_err(|source| WikiError::Io {
            path: gz.display().to_string(),
            source,
        })?;
        let mut out = Vec::with_capacity(raw.len() * 8);
        GzDecoder::new(raw.as_slice())
            .read_to_end(&mut out)
            .map_err(|source| WikiError::Io {
                path: gz.display().to_string(),
                source,
            })?;
        Ok(out)
    } else if plain.exists() {
        fs::read(&plain).map_err(|source| WikiError::Io {
            path: plain.display().to_string(),
            source,
        })
    } else {
        Err(WikiError::Missing {
            root: root.display().to_string(),
            file: name.to_owned(),
        })
    }
}

fn parse_lines<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    file: &str,
) -> Result<Vec<T>, WikiError> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    for (at, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str(line).map_err(|source| WikiError::Json {
            path: file.to_owned(),
            line: at + 1,
            source,
        })?;
        out.push(value);
    }
    Ok(out)
}

/// Plans worlds: group revisions into connected components of the agent–page
/// graph, order each component's revisions, and apply the selection's
/// agent-count floor and world cap.
fn plan_worlds(revisions: &[Revision], selection: &WikiSelection) -> Vec<WorldSpec> {
    let mut dsu = Dsu::default();
    // Link every identity that edited a page to the first identity seen on it.
    let mut first_on_page: BTreeMap<&str, usize> = BTreeMap::new();
    for rev in revisions {
        let node = dsu.node(rev.identity());
        match first_on_page.get(rev.page_id.as_str()) {
            Some(&anchor) => dsu.union(anchor, node),
            None => {
                first_on_page.insert(&rev.page_id, node);
            }
        }
    }
    // Group revision indices by their identity's component root.
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (at, rev) in revisions.iter().enumerate() {
        let node = dsu.node(rev.identity());
        let root = dsu.find(node);
        groups.entry(root).or_default().push(at);
    }
    let mut specs = Vec::new();
    for revs in groups.into_values() {
        let mut agents = std::collections::BTreeSet::new();
        for &at in &revs {
            agents.insert(revisions[at].identity());
        }
        if selection.min_agents.is_some_and(|min| agents.len() < min)
            || selection.max_agents.is_some_and(|max| agents.len() > max)
        {
            continue;
        }
        let mut revs = revs;
        revs.sort_by(|&a, &b| {
            let (ra, rb) = (&revisions[a], &revisions[b]);
            (&ra.time, &ra.page_id, ra.seq).cmp(&(&rb.time, &rb.page_id, rb.seq))
        });
        // The world's key is its lexicographically smallest page id, which is
        // unique to the component (pages never cross components).
        let key = revs
            .iter()
            .map(|&at| revisions[at].page_id.as_str())
            .min()
            .unwrap_or("")
            .to_owned();
        specs.push(WorldSpec {
            key: WorldKey::new(key),
            revisions: revs,
            agents: agents.len(),
        });
    }
    // Largest worlds first, then by key for a deterministic tie-break.
    specs.sort_by(|a, b| {
        b.agents
            .cmp(&a.agents)
            .then_with(|| a.key.as_str().cmp(b.key.as_str()))
    });
    if let Some(limit) = selection.limit {
        specs.truncate(limit);
    }
    specs
}

/// A disjoint-set forest over identity strings.
#[derive(Default)]
struct Dsu {
    index: BTreeMap<String, usize>,
    parent: Vec<usize>,
}

impl Dsu {
    fn node(&mut self, identity: String) -> usize {
        if let Some(&at) = self.index.get(&identity) {
            return at;
        }
        let at = self.parent.len();
        self.parent.push(at);
        self.index.insert(identity, at);
        at
    }

    fn find(&mut self, mut at: usize) -> usize {
        while self.parent[at] != at {
            self.parent[at] = self.parent[self.parent[at]];
            at = self.parent[at];
        }
        at
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.parent[rb] = ra;
        }
    }
}
