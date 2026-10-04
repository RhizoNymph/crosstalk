//! The channels: declared and discovered, in every detection state and
//! policy, each with its resources and the agents that use it.
//!
//! Detection is filled in after traffic is generated ([`finish`]), from the
//! channel's actual first access, first cross access and transmissions.

use std::collections::HashMap;

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed,
};
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};
use crosstalk_spec::support::Timestamp;

use crate::backend::fixture::clock::{DAY, Mint, NOW, minus, plus};
use crate::backend::fixture::store::ChannelRecord;
use crate::backend::fixture::text::Theme;
use crate::contract::channels::Supersession;

use super::drafts::{Target, drafts};
use super::history::OPERATOR_RESEARCHER;
use super::traffic::Traffic;
use super::{Cast, GenError};

pub use super::drafts::{
    DESIGN_DOCS_AT, MCP_RESET_AT, PASTEBIN_DECIDED_AT, PROMOTE_AT, SHARED_FILE_DECIDED_AT,
};

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
    /// Declared by an operator's promotion; supersedes `OldTeamNotes`.
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
    /// Discovered, unreviewed, observed: a key-value tool only one agent
    /// uses.
    KvScratch,
    /// Discovered, unreviewed, candidate: an S3 prefix with cross accesses
    /// but no content match.
    S3Handoff,
    /// Discovered, superseded by `TeamNotes`.
    OldTeamNotes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSpec {
    pub key: ChannelKey,
    pub id: ChannelId,
    /// `Some` for declared channels.
    declared: Option<(ResourcePattern, PolicyAuthor, Timestamp)>,
    policy: Policy,
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
        self.specs
            .iter()
            .find(|s| s.key == key)
            .map(|s| s.id)
            .ok_or_else(|| GenError::Missing(format!("channel {key:?}")))
    }
}

pub fn plan(mint: &mut Mint, cast: &Cast) -> Result<ChannelPlan, GenError> {
    let mut specs = Vec::new();
    for draft in drafts() {
        let id = ChannelId::from_ulid(mint.ulid(draft.created));
        let resources = draft
            .locators
            .into_iter()
            .map(|l| (ResourceId::from_ulid(mint.ulid(draft.window.0)), l))
            .collect();
        specs.push(ChannelSpec {
            key: draft.key,
            id,
            declared: draft.declared,
            policy: draft.policy,
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

/// Fills in each channel's detection state from its generated traffic.
pub fn finish(plan: &ChannelPlan, traffic: &Traffic) -> Result<Vec<ChannelRecord>, GenError> {
    let team_notes = plan.id(ChannelKey::TeamNotes)?;
    let mut out = Vec::new();
    for spec in &plan.specs {
        let stats = traffic.channel_stats(spec.id);
        let detection = || -> Result<TrafficDetection, GenError> {
            let missing = |what: &str| GenError::Missing(format!("{what} on {:?}", spec.key));
            Ok(match spec.target {
                Target::Observed | Target::Awaiting | Target::Unused => {
                    TrafficDetection::Observed {
                        first_access: stats.first_access.ok_or_else(|| missing("access"))?.0,
                    }
                }
                Target::Candidate => TrafficDetection::Candidate {
                    first_cross_access: stats
                        .first_cross_access
                        .ok_or_else(|| missing("cross access"))?,
                },
                Target::Active => {
                    let (since, _) = stats.first_confirmed.ok_or_else(|| missing("confirm"))?;
                    let (_, last) = stats.last_confirmed.ok_or_else(|| missing("confirm"))?;
                    TrafficDetection::Active {
                        since,
                        last_transmission: last,
                    }
                }
                Target::Dormant => {
                    let (at, last) = stats.last_confirmed.ok_or_else(|| missing("confirm"))?;
                    TrafficDetection::Dormant {
                        since: plus(at, DAY),
                        last_transmission: last,
                    }
                }
            })
        };
        let resource_ids: Vec<ResourceId> = spec.resources.iter().map(|(id, _)| *id).collect();
        let (origin, resources, created) = match &spec.declared {
            Some((pattern, by, at)) => {
                let detection = match spec.target {
                    Target::Awaiting => DeclaredDetection::AwaitingTraffic,
                    Target::Unused => DeclaredDetection::Unused {
                        since: minus(NOW, 6 * DAY),
                    },
                    _ => DeclaredDetection::InUse(detection()?),
                };
                let origin = ChannelOrigin::Declared {
                    declaration: Declaration {
                        pattern: pattern.clone(),
                        by: *by,
                        at: *at,
                    },
                    history: DeclaredHistory::BeforeTraffic(detection),
                };
                (origin, resource_ids, *at)
            }
            None => {
                let (first_access, first_at) = stats
                    .first_access
                    .ok_or_else(|| GenError::Missing(format!("access on {:?}", spec.key)))?;
                let seed = *resource_ids
                    .first()
                    .ok_or_else(|| GenError::Missing(format!("seed of {:?}", spec.key)))?;
                let origin = ChannelOrigin::Discovered {
                    seed: Seed {
                        resource: seed,
                        first_access,
                    },
                    detection: detection()?,
                };
                (origin, resource_ids[1..].to_vec(), first_at)
            }
        };
        let superseded = (spec.key == ChannelKey::OldTeamNotes).then_some(Supersession {
            into: team_notes,
            by: OPERATOR_RESEARCHER,
            at: PROMOTE_AT,
        });
        out.push(ChannelRecord {
            channel: Channel {
                id: spec.id,
                origin,
                resources,
                policy: spec.policy.clone(),
            },
            superseded,
            created,
        });
    }
    // A declared channel holds the resources of every channel it superseded.
    let moved: Vec<(ChannelId, Vec<ResourceId>)> = out
        .iter()
        .filter_map(|r| {
            let into = r.superseded?.into;
            let mut ids = Vec::new();
            if let ChannelOrigin::Discovered { seed, .. } = &r.channel.origin {
                ids.push(seed.resource);
            }
            ids.extend(r.channel.resources.iter().copied());
            Some((into, ids))
        })
        .collect();
    for (into, ids) in moved {
        if let Some(target) = out.iter_mut().find(|r| r.channel.id == into) {
            target.channel.resources.extend(ids);
        }
    }
    Ok(out)
}
