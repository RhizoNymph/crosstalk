//! The channels: declared and discovered, in every detection state and
//! policy, each with its resources, the agents that use it and its policy
//! history.
//!
//! Detection is filled in after traffic is generated ([`finish`]), from the
//! channel's cross-agent transmissions: a discovered channel is created by
//! its first one (its seed transmission), which is also when `NewChannel`
//! fires. Whether that traffic is confirmed is not stored: queries read it
//! from the transmissions with merges resolved, so `SelfNotes` is hidden
//! while `al1` stays merged into `cx1`.
//! The world's one past promotion is then applied as the spec plans it
//! ([`promotion::plan`]): the team-notes channel keeps its id and gains the
//! pattern, and the discovered channels the pattern covers are superseded,
//! their detection frozen at the promotion while later confirmations on
//! them advance the promoted channel's ("Detection follows resolution").

use std::collections::HashMap;

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory,
};
use crosstalk_spec::derived::flow::channel::promotion::{self, Registered};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};
use crosstalk_spec::support::Timestamp;

use crate::backend::fixture::clock::{DAY, Mint, NOW, minus, plus};
use crate::backend::fixture::store::ChannelRecord;
use crate::backend::fixture::text::Theme;

use super::drafts::{DraftOrigin, Target, decisions, drafts, team_notes_promotion};
use super::traffic::{ChannelStats, Traffic};
use super::{Cast, GenError};

pub use super::drafts::{MCP_RESET_AT, PASTEBIN_DECIDED_AT, PROMOTE_AT, SHARED_FILE_DECIDED_AT};

/// Every channel in the world, by role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChannelKey {
    /// Declared, sanctioned, active: `wiki.corp.internal/eng`.
    InternalWiki,
    /// Declared, sanctioned, active: `git.corp.internal/platform/monorepo`.
    Monorepo,
    /// Declared, sanctioned, active: host `issues.corp.internal`.
    IssueTracker,
    /// Declared, sanctioned, awaiting traffic: `docs.corp.internal/design`.
    DesignDocs,
    /// Declared, sanctioned, unused: `/mnt/shared/releases` on `nfs-01`.
    ReleaseBucket,
    /// Discovered at `notes.corp.internal/team-a/retro`, then promoted by
    /// the researcher with the `/team-a` prefix (sanctioned): declared,
    /// same id, active. Its promotion superseded `OldTeamNotes`.
    TeamNotes,
    /// Discovered, unreviewed, active: a public wiki page agents use to
    /// coordinate (`wiki.example.org`).
    HijackedWiki,
    /// Discovered, unreviewed, active: the same wiki's talk page.
    WikiTalk,
    /// Discovered, unsanctioned, active: `paste.example.net`.
    Pastebin,
    /// Discovered, reset to unreviewed, active: the `memory` MCP server.
    McpMemory,
    /// Discovered, sanctioned, active: `/tmp/agent-handoff` on `devbox-3`.
    SharedFile,
    /// Discovered, unreviewed, dormant: `gist.example.com`.
    Gist,
    /// Discovered, unreviewed, active and unconfirmed: an S3 object one
    /// agent writes and two others read, with no content match, so every
    /// transmission through it is suspected (or discarded, or awaiting
    /// content).
    S3Handoff,
    /// Discovered at `/home/dev/.codex/handoff.md` on `devbox-7`,
    /// unreviewed, dormant: its only cross-agent traffic is between `al1`
    /// and `cx1`, which an operator later merged, so it is hidden while the
    /// merge stands and listed again if it is reverted.
    SelfNotes,
    /// Discovered at `notes.corp.internal/team-a/standup`, unreviewed,
    /// superseded by `TeamNotes`'s promotion (detection frozen there).
    OldTeamNotes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSpec {
    pub key: ChannelKey,
    pub id: ChannelId,
    origin: DraftOrigin,
    target: Target,
    /// Resources, as locators with their ids. A discovered channel's first
    /// resource is its seed.
    pub resources: Vec<(ResourceId, Locator)>,
    /// When the channel carries traffic.
    pub from: Timestamp,
    pub until: Timestamp,
    /// Relative share of channel-routed transmissions.
    pub weight: f64,
    pub writers: Vec<AgentId>,
    pub readers: Vec<AgentId>,
    pub themes: Vec<(Theme, f64)>,
    /// Whether transmissions on it can be confirmed.
    pub confirms: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelPlan {
    pub specs: Vec<ChannelSpec>,
}

impl ChannelPlan {
    pub fn ids(&self) -> HashMap<ChannelKey, ChannelId> {
        self.specs.iter().map(|s| (s.key, s.id)).collect()
    }

    pub fn id(&self, key: ChannelKey) -> Result<ChannelId, GenError> {
        self.spec(key).map(|s| s.id)
    }

    fn spec(&self, key: ChannelKey) -> Result<&ChannelSpec, GenError> {
        self.specs
            .iter()
            .find(|s| s.key == key)
            .ok_or_else(|| GenError::Missing(format!("channel {key:?}")))
    }
}

pub fn plan(mint: &mut Mint, cast: &Cast) -> Result<ChannelPlan, GenError> {
    let mut specs = Vec::new();
    for draft in drafts() {
        let id = ChannelId::from_ulid(mint.ulid(draft.created));
        // A discovered channel holds exactly its seed: a resource joins a
        // channel only through a declared pattern, so its other planned
        // locators are dropped (as the world seed drops them).
        let locators = match draft.origin {
            DraftOrigin::Discovered => draft.locators.into_iter().take(1).collect(),
            DraftOrigin::Declared { .. } => draft.locators,
        };
        let resources = locators
            .into_iter()
            .map(|l| (ResourceId::from_ulid(mint.ulid(draft.window.0)), l))
            .collect();
        specs.push(ChannelSpec {
            key: draft.key,
            id,
            origin: draft.origin,
            target: draft.target,
            resources,
            from: draft.window.0,
            until: draft.window.1,
            weight: draft.weight,
            writers: cast.ids(draft.writers)?,
            readers: cast.ids(draft.readers)?,
            themes: draft.themes.to_vec(),
            confirms: draft.confirms,
        });
    }
    Ok(ChannelPlan { specs })
}

/// The traffic detection `spec`'s target state takes from `stats`: active
/// since its first cross-agent transmission, dormant a day after its last.
fn traffic_detection(
    spec: &ChannelSpec,
    stats: &ChannelStats,
) -> Result<TrafficDetection, GenError> {
    let missing = |what: &str| GenError::Missing(format!("{what} on {:?}", spec.key));
    let (since, _) = stats
        .first
        .ok_or_else(|| missing("cross-agent transmission"))?;
    let (last_at, last) = stats
        .last
        .ok_or_else(|| missing("cross-agent transmission"))?;
    Ok(match spec.target {
        Target::Active => TrafficDetection::Active {
            since,
            last_transmission: last,
        },
        Target::Dormant => TrafficDetection::Dormant {
            since: plus(last_at, DAY),
            last_transmission: last,
        },
        Target::Awaiting | Target::Unused => {
            return Err(GenError::Missing(format!(
                "a traffic state for {:?}, which has traffic",
                spec.key
            )));
        }
    })
}

/// A channel as discovered or declared, before any promotion, with its
/// detection over `stats`.
fn first_origin(
    spec: &ChannelSpec,
    traffic: &Traffic,
    stats: &ChannelStats,
) -> Result<(ChannelOrigin, Vec<ResourceId>, Timestamp), GenError> {
    let resource_ids: Vec<ResourceId> = spec.resources.iter().map(|(id, _)| *id).collect();
    match &spec.origin {
        DraftOrigin::Declared { pattern, at } => {
            let detection = match spec.target {
                Target::Awaiting => DeclaredDetection::AwaitingTraffic,
                Target::Unused => DeclaredDetection::Unused {
                    since: minus(NOW, 6 * DAY),
                },
                _ => DeclaredDetection::InUse(traffic_detection(spec, stats)?),
            };
            let origin = ChannelOrigin::Declared {
                declaration: Declaration {
                    pattern: pattern.clone(),
                    by: PolicyAuthor::Config,
                    at: *at,
                },
                history: DeclaredHistory::BeforeTraffic(detection),
            };
            Ok((origin, resource_ids, *at))
        }
        DraftOrigin::Discovered => {
            let missing = |what: &str| GenError::Missing(format!("{what} of {:?}", spec.key));
            let first = stats
                .first
                .ok_or_else(|| missing("cross-agent transmission"))?;
            let (seed, rest) = resource_ids.split_first().ok_or_else(|| missing("seed"))?;
            // The first cross-agent transmission through the seed created
            // the channel; a seed the generator happened never to route one
            // through falls back to the channel's first.
            let (created, first_transmission) =
                traffic.first_crossing_through(*seed).unwrap_or(first);
            let origin = ChannelOrigin::Discovered {
                seed: Seed {
                    resource: *seed,
                    first_transmission,
                    opened_at: created,
                },
                detection: traffic_detection(spec, stats)?,
            };
            Ok((origin, rest.to_vec(), created))
        }
    }
}

/// Each channel's policy history from the decisions table, checked.
fn histories(plan: &ChannelPlan) -> Result<HashMap<ChannelId, PolicyHistory>, GenError> {
    let mut entries: HashMap<ChannelId, Vec<PolicyDecision>> = HashMap::new();
    for (key, decision) in decisions() {
        entries.entry(plan.id(key)?).or_default().push(decision);
    }
    entries
        .into_iter()
        .map(|(id, mut list)| {
            list.sort_by_key(|d| d.decision.at);
            PolicyHistory::from_entries(list)
                .map(|history| (id, history))
                .map_err(|e| GenError::invalid("PolicyHistory", e))
        })
        .collect()
}

/// Fills in each channel's detection state and policy history from its
/// generated traffic, then applies the team-notes promotion.
pub fn finish(plan: &ChannelPlan, traffic: &Traffic) -> Result<Vec<ChannelRecord>, GenError> {
    let mut histories = histories(plan)?;
    let mut out = Vec::new();
    for spec in &plan.specs {
        let own = traffic.channel_stats(|routed, _| routed == spec.id);
        let (origin, resources, created) = first_origin(spec, traffic, &own)?;
        let channel = Channel {
            id: spec.id,
            origin,
            resources,
            policy: Policy::Unreviewed(None),
        };
        let history = histories.remove(&spec.id).unwrap_or_default();
        out.push(ChannelRecord::new(channel, history, created));
    }
    promote(plan, traffic, &mut out)?;
    Ok(out)
}

/// Applies the world's past promotion exactly as `promotion::plan` decides
/// it over the generated channels, recording its decision in the promoted
/// channel's history.
fn promote(
    plan: &ChannelPlan,
    traffic: &Traffic,
    records: &mut [ChannelRecord],
) -> Result<(), GenError> {
    let (key, promotion) = team_notes_promotion();
    let target = plan.id(key)?;
    let seeds: HashMap<ResourceId, &Locator> = plan
        .specs
        .iter()
        .flat_map(|s| s.resources.iter().map(|(id, l)| (*id, l)))
        .collect();
    let registry: Vec<Registered<'_>> = records
        .iter()
        .map(|r| Registered {
            channel: r.channel(),
            seed: r
                .channel()
                .origin
                .seed()
                .and_then(|seed| seeds.get(&seed.resource).copied()),
        })
        .collect();
    let planned = promotion::plan(target, promotion.declaration(), &registry)
        .map_err(|e| GenError::invalid("team notes promotion", e))?;
    let absorbed: Vec<ChannelId> = planned.superseded_ids().collect();
    let at = promotion.at();
    let supersession = Supersession { by: target, at };
    for record in records.iter_mut() {
        let id = record.channel().id;
        let spec = plan
            .specs
            .iter()
            .find(|s| s.id == id)
            .ok_or_else(|| GenError::Missing(format!("spec of channel {id:?}")))?;
        let stats = if id == target {
            // Its own traffic, and that on the channels it absorbed that
            // came after the promotion.
            traffic.channel_stats(|routed, advanced| {
                routed == id || (absorbed.contains(&routed) && advanced > at)
            })
        } else if absorbed.contains(&id) {
            traffic.channel_stats(|routed, advanced| routed == id && advanced <= at)
        } else {
            continue;
        };
        let (discovered, _, _) = first_origin(spec, traffic, &stats)?;
        let origin = if id == target {
            discovered
                .promoted(promotion.declaration().clone())
                .map_err(|e| GenError::invalid("promoted origin", e))?
        } else {
            discovered
                .superseded(supersession)
                .map_err(|e| GenError::invalid("superseded origin", e))?
        };
        record.set_origin(origin);
        if id == target {
            record.record(promotion.decision().clone());
        }
    }
    Ok(())
}
