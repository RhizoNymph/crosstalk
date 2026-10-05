//! The channel table: every channel's origin, resources, traffic window
//! and the agents that use it, the policy decisions made about each, and
//! the one promotion in the world's past. [`super::channels`] turns it into
//! a plan with ids.

use crosstalk_spec::derived::flow::channel::policy::{
    Decision, PolicyAuthor, PolicyDecision, PolicyKind,
};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::Timestamp;

use crate::clock::{HOUR, plus};
use crate::config::OPERATOR_RESEARCHER;
use crate::scenario::ChannelKey;
use crate::text::Theme;

use super::times::Times;

/// The detection state a channel is generated into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Awaiting,
    Unused,
    Active,
    Dormant,
}

fn url(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host(host.to_owned()),
        path: path.to_owned(),
        query: None,
    }
}

fn decision(kind: PolicyKind, by: PolicyAuthor, at: Timestamp, note: &str) -> PolicyDecision {
    PolicyDecision {
        kind,
        decision: Decision {
            by,
            at,
            note: Some(note.to_owned()),
        },
    }
}

/// How a channel came to exist, with the resources its traffic uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftOrigin {
    /// Declared in config at `at`, before any traffic; `locators` are on it
    /// from their first sighting, through the pattern.
    Declared {
        pattern: ResourcePattern,
        at: Timestamp,
        locators: Vec<Locator>,
    },
    /// Discovered by the first cross-agent transmission through `seed`. A
    /// discovered channel holds exactly its seed: another resource joins a
    /// channel only through a declared pattern, and would otherwise
    /// discover a channel of its own.
    Discovered { seed: Locator },
}

impl DraftOrigin {
    /// The resources the channel's traffic uses: a declared channel's
    /// locators, a discovered channel's seed.
    pub fn locators(&self) -> Vec<Locator> {
        match self {
            Self::Declared { locators, .. } => locators.clone(),
            Self::Discovered { seed } => vec![seed.clone()],
        }
    }

    pub fn is_discovered(&self) -> bool {
        matches!(self, Self::Discovered { .. })
    }
}

/// Everything one channel needs before traffic exists.
pub struct Draft {
    pub key: ChannelKey,
    /// When the channel was declared, or when its traffic window opens (a
    /// discovered channel is created by its first cross-agent transmission
    /// in that window; its id carries this time).
    pub created: Timestamp,
    pub origin: DraftOrigin,
    pub target: Target,
    pub window: (Timestamp, Timestamp),
    pub weight: f64,
    pub writers: &'static [&'static str],
    pub readers: &'static [&'static str],
    pub themes: &'static [(Theme, f64)],
    pub confirms: bool,
}

/// The pattern the researcher promoted the team-notes channel with.
pub fn team_notes_pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("notes.corp.internal".to_owned()),
        path_prefix: "/team-a".to_owned(),
    }
}

/// The world's one past promotion: the researcher promoted the discovered
/// team-notes channel with a `/team-a` prefix, sanctioning it, which
/// superseded the standup page's channel (`OldTeamNotes`).
pub fn team_notes_promotion(times: &Times) -> Promotion {
    Promotion::new(
        team_notes_pattern(),
        PolicyKind::Sanctioned,
        OPERATOR_RESEARCHER,
        times.promote_at,
        Some("team notes are an approved handoff space".to_owned()),
    )
}

/// The decision config gives each declared channel.
pub fn config_decision(at: Timestamp) -> PolicyDecision {
    decision(
        PolicyKind::Sanctioned,
        PolicyAuthor::Config,
        at,
        "declared in config",
    )
}

/// The researcher's policy decisions, oldest first: the memory server
/// sanctioned, the pastebin unsanctioned, the handoff directory sanctioned,
/// the memory server reset.
pub fn operator_decisions(times: &Times) -> Vec<(ChannelKey, PolicyDecision)> {
    use ChannelKey as K;
    use PolicyKind::{Sanctioned, Unreviewed, Unsanctioned};
    let researcher = PolicyAuthor::Operator(OPERATOR_RESEARCHER);
    let mut out = vec![
        (
            K::McpMemory,
            decision(
                Sanctioned,
                researcher,
                times.mcp_sanctioned_at,
                "internal memory server",
            ),
        ),
        (
            K::Pastebin,
            decision(
                Unsanctioned,
                researcher,
                times.pastebin_decided_at,
                "credentials leaked through public pastes",
            ),
        ),
        (
            K::SharedFile,
            decision(
                Sanctioned,
                researcher,
                times.shared_file_decided_at,
                "handoff directory used by the infra team",
            ),
        ),
        (
            K::McpMemory,
            decision(
                Unreviewed,
                researcher,
                times.mcp_reset_at,
                "reset: the memory server was upgraded and needs a new review",
            ),
        ),
    ];
    out.sort_by_key(|(_, d)| d.decision.at);
    out
}

pub fn drafts(times: &Times) -> Vec<Draft> {
    use ChannelKey as K;
    use Theme as T;
    let declared = |pattern, locators| DraftOrigin::Declared {
        pattern,
        at: times.config_at,
        locators,
    };
    let discovered = |seed| DraftOrigin::Discovered { seed };
    let (start, now) = (times.start, times.now);
    let whole = (start, now);
    vec![
        Draft {
            key: K::InternalWiki,
            created: times.config_at,
            origin: declared(
                ResourcePattern::UrlPrefix {
                    host: Host("wiki.corp.internal".to_owned()),
                    path_prefix: "/eng".to_owned(),
                },
                vec![
                    url("wiki.corp.internal", "/eng/runbooks/deploy"),
                    url("wiki.corp.internal", "/eng/research/agent-memory"),
                    url("wiki.corp.internal", "/eng/meetings/2026-09-29"),
                    url("wiki.corp.internal", "/eng/oncall/handbook"),
                ],
            ),
            target: Target::Active,
            window: whole,
            weight: 15.0,
            writers: &["cc0", "cc1", "cc2", "cx0", "cx2", "cc7"],
            readers: &["cc0.a", "cc1.a", "cc3", "cx1", "cx4", "cc4", "cc0", "cx0"],
            themes: &[(T::Research, 3.0), (T::Meetings, 3.0), (T::Deploy, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Monorepo,
            created: times.config_at,
            origin: declared(
                ResourcePattern::UrlPrefix {
                    host: Host("git.corp.internal".to_owned()),
                    path_prefix: "/platform/monorepo".to_owned(),
                },
                vec![
                    url("git.corp.internal", "/platform/monorepo/pull/4182"),
                    url("git.corp.internal", "/platform/monorepo/pull/4190"),
                    url(
                        "git.corp.internal",
                        "/platform/monorepo/blob/main/README.md",
                    ),
                ],
            ),
            target: Target::Active,
            window: whole,
            weight: 15.0,
            writers: &["cc0", "cc2", "cx0", "cx1", "cc0.b", "cx0.a"],
            readers: &["cc1", "cc3", "cx2", "cx0.b", "cc2.a", "cc4.a"],
            themes: &[
                (T::CodeReview, 4.0),
                (T::Deploy, 2.0),
                (T::DataPipeline, 1.0),
            ],
            confirms: true,
        },
        Draft {
            key: K::IssueTracker,
            created: times.config_at,
            origin: declared(
                ResourcePattern::Host(Host("issues.corp.internal".to_owned())),
                vec![
                    url("issues.corp.internal", "/browse/INC-4471"),
                    url("issues.corp.internal", "/browse/SUP-1029"),
                    url("issues.corp.internal", "/browse/PLAT-880"),
                ],
            ),
            target: Target::Active,
            window: whole,
            weight: 10.0,
            writers: &["cc2", "cx4", "omp1", "cc3"],
            readers: &["cc0", "cc5", "cx3", "omp1", "cc2.b"],
            themes: &[(T::Incidents, 3.0), (T::Support, 3.0)],
            confirms: true,
        },
        Draft {
            key: K::DesignDocs,
            created: times.design_docs_at,
            origin: DraftOrigin::Declared {
                pattern: ResourcePattern::UrlPrefix {
                    host: Host("docs.corp.internal".to_owned()),
                    path_prefix: "/design".to_owned(),
                },
                at: times.design_docs_at,
                locators: Vec::new(),
            },
            target: Target::Awaiting,
            window: whole,
            weight: 0.0,
            writers: &[],
            readers: &[],
            themes: &[],
            confirms: false,
        },
        Draft {
            key: K::ReleaseBucket,
            created: times.config_at,
            origin: declared(
                ResourcePattern::PathPrefix {
                    host: Some(Host("nfs-01".to_owned())),
                    prefix: "/mnt/shared/releases".to_owned(),
                },
                Vec::new(),
            ),
            target: Target::Unused,
            window: whole,
            weight: 0.0,
            writers: &[],
            readers: &[],
            themes: &[],
            confirms: false,
        },
        Draft {
            key: K::TeamNotes,
            created: times.team_notes_from,
            origin: discovered(url("notes.corp.internal", "/team-a/retro")),
            target: Target::Active,
            window: (times.team_notes_from, now),
            weight: 6.0,
            writers: &["cc4", "cx1", "cc7"],
            readers: &["cc1.b", "cx1.a", "cc7", "cc4"],
            themes: &[(T::Meetings, 3.0), (T::Deploy, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::HijackedWiki,
            created: times.hijack_from,
            origin: discovered(url("wiki.example.org", "/wiki/Agent_Coordination")),
            target: Target::Active,
            window: (times.hijack_from, now),
            weight: 18.0,
            writers: &["pi0", "omp2", "sh1"],
            readers: &[
                "cc0.c", "cc3.a", "cx2", "pi1", "omp0.a", "cc6", "cx3", "cc5",
            ],
            themes: &[
                (T::Injection, 5.0),
                (T::Scraping, 2.0),
                (T::Credentials, 2.0),
            ],
            confirms: true,
        },
        Draft {
            key: K::WikiTalk,
            created: crate::clock::minus(now, 4 * crate::clock::DAY),
            origin: discovered(url("wiki.example.org", "/wiki/Talk:Agent_Coordination")),
            target: Target::Active,
            window: (crate::clock::minus(now, 4 * crate::clock::DAY), now),
            weight: 3.0,
            writers: &["pi0", "cx2"],
            readers: &["cc6", "omp0.b"],
            themes: &[(T::Injection, 2.0), (T::Scraping, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Pastebin,
            created: plus(start, 5 * HOUR),
            origin: discovered(url("paste.example.net", "/raw/q8Zt3LmK")),
            target: Target::Active,
            window: (plus(start, 5 * HOUR), now),
            weight: 8.0,
            writers: &["pi1", "omp0", "al0"],
            readers: &["cc5", "cx3", "cc0", "pi3"],
            themes: &[(T::Credentials, 4.0), (T::Scraping, 2.0)],
            confirms: true,
        },
        Draft {
            key: K::McpMemory,
            created: plus(start, 2 * HOUR),
            origin: discovered(Locator::Mcp {
                server: "memory".to_owned(),
                tool: ToolName("create_entities".to_owned()),
                target: Some("project-atlas".to_owned()),
            }),
            target: Target::Active,
            window: (plus(start, 2 * HOUR), now),
            weight: 8.0,
            writers: &["cc0", "cc0.a", "omp1", "al0"],
            readers: &["cc0.b", "cc1", "omp1", "cc0"],
            themes: &[(T::Research, 3.0), (T::DataPipeline, 2.0)],
            confirms: true,
        },
        Draft {
            key: K::SharedFile,
            created: plus(start, 9 * HOUR),
            origin: discovered(Locator::File {
                host: Some(Host("devbox-3".to_owned())),
                path: "/tmp/agent-handoff/plan.md".to_owned(),
            }),
            target: Target::Active,
            window: (plus(start, 9 * HOUR), now),
            weight: 7.0,
            writers: &["cc2", "cc3"],
            readers: &["cx4", "cc3.a", "cc2"],
            themes: &[(T::Deploy, 3.0), (T::Meetings, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Gist,
            created: plus(start, HOUR),
            origin: discovered(url("gist.example.com", "/anon/5d41402abc4b2a76")),
            target: Target::Dormant,
            window: (plus(start, HOUR), times.gist_until),
            weight: 3.0,
            writers: &["pi3", "cx0"],
            readers: &["cx1", "cc1"],
            themes: &[(T::DataPipeline, 2.0), (T::Credentials, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::S3Handoff,
            created: crate::clock::minus(now, 2 * crate::clock::DAY),
            origin: discovered(Locator::Url {
                scheme: "s3".to_owned(),
                host: Host("agent-scratch".to_owned()),
                path: "/handoff/batch-0412.jsonl".to_owned(),
                query: None,
            }),
            target: Target::Active,
            window: (crate::clock::minus(now, 2 * crate::clock::DAY), now),
            weight: 3.0,
            writers: &["sh0"],
            readers: &["cc6", "sh1"],
            themes: &[(T::DataPipeline, 1.0)],
            confirms: false,
        },
        Draft {
            key: K::SelfNotes,
            created: times.self_notes_from,
            origin: discovered(Locator::File {
                host: Some(Host("devbox-7".to_owned())),
                path: "/home/dev/.codex/handoff.md".to_owned(),
            }),
            target: Target::Dormant,
            window: (times.self_notes_from, times.self_notes_until),
            weight: 2.0,
            writers: &["al1", "cx1"],
            readers: &["cx1", "al1"],
            themes: &[(T::Deploy, 1.0), (T::CodeReview, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::OldTeamNotes,
            created: plus(start, 3 * HOUR),
            origin: discovered(url("notes.corp.internal", "/team-a/standup")),
            target: Target::Active,
            window: (plus(start, 3 * HOUR), times.promote_at),
            weight: 4.0,
            writers: &["cc4", "cx1", "al1"],
            readers: &["cc1.b", "cx1.a", "cc7"],
            themes: &[(T::Meetings, 3.0)],
            confirms: true,
        },
    ]
}

/// The key-value entry only `cc7` writes and reads.
pub fn lone_locator() -> Locator {
    Locator::Opaque {
        tool: ToolName("kv_put".to_owned()),
        key: "scratch/notes".to_owned(),
    }
}
