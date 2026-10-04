//! The channel table: every channel's origin, resources, traffic window
//! and the agents that use it, the policy decisions made about each, and
//! the one promotion in the world's past. [`super::channels`] turns it into
//! a plan and, after traffic exists, into channel records.

use crosstalk_spec::derived::flow::channel::policy::{
    Decision, PolicyAuthor, PolicyDecision, PolicyKind,
};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, HOUR, NOW, START, ago, plus};
use crate::text::Theme;

use super::channels::ChannelKey;
use super::history::{CONFIG_AT, OPERATOR_RESEARCHER};
use super::rules::MCP_SANCTIONED_AT;

/// When the researcher promoted the team-notes channel.
pub const PROMOTE_AT: Timestamp = ago(3 * DAY);
/// When the pastebin was marked unsanctioned.
pub const PASTEBIN_DECIDED_AT: Timestamp = ago(5 * DAY);
/// When the shared handoff directory was sanctioned.
pub const SHARED_FILE_DECIDED_AT: Timestamp = ago(4 * DAY);
/// When the MCP memory server's policy was reset to unreviewed.
pub const MCP_RESET_AT: Timestamp = ago(2 * DAY + 12 * HOUR);
/// When the gist channel went quiet.
const GIST_UNTIL: Timestamp = ago(4 * DAY);
/// When the design-docs channel was added to config.
pub const DESIGN_DOCS_AT: Timestamp = ago(20 * HOUR);
/// When the team-notes retro page was first used, before its promotion.
pub const TEAM_NOTES_FROM: Timestamp = ago(5 * DAY + 6 * HOUR);
/// When the hijacked wiki was first written to.
pub const HIJACK_FROM: Timestamp = ago(5 * DAY + 14 * HOUR);
/// When `al1` and `cx1` started passing notes through a local handoff
/// file, two ids of one Codex agent before an operator merged them.
pub const SELF_NOTES_FROM: Timestamp = ago(6 * DAY);
/// When `al1` was merged into `cx1` (the merge in `agents.rs`); its last
/// traffic is just before.
pub const SELF_NOTES_UNTIL: Timestamp = ago(3 * DAY + HOUR);

/// The detection state a channel is generated into. Whether its traffic
/// is confirmed is not a detection state: it follows from the
/// transmissions (`Draft::confirms`) and is read at query time.
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

/// How a channel came to exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftOrigin {
    /// Declared in config at `at`, before any traffic.
    Declared {
        pattern: ResourcePattern,
        at: Timestamp,
    },
    /// Discovered from traffic; its first locator is its seed.
    Discovered,
}

/// Everything one channel needs before traffic exists.
pub struct Draft {
    pub key: ChannelKey,
    pub created: Timestamp,
    pub origin: DraftOrigin,
    pub target: Target,
    pub locators: Vec<Locator>,
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
/// team-notes channel (`TeamNotes`) with a `/team-a` prefix, sanctioning
/// it, which superseded the standup page's channel (`OldTeamNotes`).
pub fn team_notes_promotion() -> (ChannelKey, Promotion) {
    let promotion = Promotion::new(
        team_notes_pattern(),
        PolicyKind::Sanctioned,
        OPERATOR_RESEARCHER,
        PROMOTE_AT,
        Some("team notes are an approved handoff space".to_owned()),
    );
    (ChannelKey::TeamNotes, promotion)
}

/// Every policy decision in the world's past besides the promotion's:
/// config declarations of the declared channels, and the researcher's
/// decisions (the memory server sanctioned then reset, the pastebin
/// unsanctioned, the handoff directory sanctioned). Each channel's history
/// is built from these in time order.
pub fn decisions() -> Vec<(ChannelKey, PolicyDecision)> {
    use ChannelKey as K;
    use PolicyKind::{Sanctioned, Unreviewed, Unsanctioned};
    let config = PolicyAuthor::Config;
    let researcher = PolicyAuthor::Operator(OPERATOR_RESEARCHER);
    let mut out: Vec<(ChannelKey, PolicyDecision)> = drafts()
        .into_iter()
        .filter_map(|draft| match draft.origin {
            DraftOrigin::Declared { at, .. } => Some((
                draft.key,
                decision(Sanctioned, config, at, "declared in config"),
            )),
            DraftOrigin::Discovered => None,
        })
        .collect();
    out.extend([
        (
            K::McpMemory,
            decision(
                Sanctioned,
                researcher,
                MCP_SANCTIONED_AT,
                "internal memory server",
            ),
        ),
        (
            K::Pastebin,
            decision(
                Unsanctioned,
                researcher,
                PASTEBIN_DECIDED_AT,
                "credentials leaked through public pastes",
            ),
        ),
        (
            K::SharedFile,
            decision(
                Sanctioned,
                researcher,
                SHARED_FILE_DECIDED_AT,
                "handoff directory used by the infra team",
            ),
        ),
        (
            K::McpMemory,
            decision(
                Unreviewed,
                researcher,
                MCP_RESET_AT,
                "reset: the memory server was upgraded and needs a new review",
            ),
        ),
    ]);
    out
}

pub fn drafts() -> Vec<Draft> {
    use ChannelKey as K;
    use Theme as T;
    let declared = |pattern| DraftOrigin::Declared {
        pattern,
        at: CONFIG_AT,
    };
    let whole = (START, NOW);
    vec![
        Draft {
            key: K::InternalWiki,
            created: CONFIG_AT,
            origin: declared(ResourcePattern::UrlPrefix {
                host: Host("wiki.corp.internal".to_owned()),
                path_prefix: "/eng".to_owned(),
            }),
            target: Target::Active,
            locators: vec![
                url("wiki.corp.internal", "/eng/runbooks/deploy"),
                url("wiki.corp.internal", "/eng/research/agent-memory"),
                url("wiki.corp.internal", "/eng/meetings/2026-09-29"),
                url("wiki.corp.internal", "/eng/oncall/handbook"),
            ],
            window: whole,
            weight: 15.0,
            writers: &["cc0", "cc1", "cc2", "cx0", "cx2", "cc7"],
            readers: &["cc0.a", "cc1.a", "cc3", "cx1", "cx4", "cc4", "cc0", "cx0"],
            themes: &[(T::Research, 3.0), (T::Meetings, 3.0), (T::Deploy, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Monorepo,
            created: CONFIG_AT,
            origin: declared(ResourcePattern::UrlPrefix {
                host: Host("git.corp.internal".to_owned()),
                path_prefix: "/platform/monorepo".to_owned(),
            }),
            target: Target::Active,
            locators: vec![
                url("git.corp.internal", "/platform/monorepo/pull/4182"),
                url("git.corp.internal", "/platform/monorepo/pull/4190"),
                url(
                    "git.corp.internal",
                    "/platform/monorepo/blob/main/README.md",
                ),
            ],
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
            created: CONFIG_AT,
            origin: declared(ResourcePattern::Host(Host(
                "issues.corp.internal".to_owned(),
            ))),
            target: Target::Active,
            locators: vec![
                url("issues.corp.internal", "/browse/INC-4471"),
                url("issues.corp.internal", "/browse/SUP-1029"),
                url("issues.corp.internal", "/browse/PLAT-880"),
            ],
            window: whole,
            weight: 10.0,
            writers: &["cc2", "cx4", "omp1", "cc3"],
            readers: &["cc0", "cc5", "cx3", "omp1", "cc2.b"],
            themes: &[(T::Incidents, 3.0), (T::Support, 3.0)],
            confirms: true,
        },
        Draft {
            key: K::DesignDocs,
            created: DESIGN_DOCS_AT,
            origin: DraftOrigin::Declared {
                pattern: ResourcePattern::UrlPrefix {
                    host: Host("docs.corp.internal".to_owned()),
                    path_prefix: "/design".to_owned(),
                },
                at: DESIGN_DOCS_AT,
            },
            target: Target::Awaiting,
            locators: Vec::new(),
            window: whole,
            weight: 0.0,
            writers: &[],
            readers: &[],
            themes: &[],
            confirms: false,
        },
        Draft {
            key: K::ReleaseBucket,
            created: CONFIG_AT,
            origin: declared(ResourcePattern::PathPrefix {
                host: Some(Host("nfs-01".to_owned())),
                prefix: "/mnt/shared/releases".to_owned(),
            }),
            target: Target::Unused,
            locators: Vec::new(),
            window: whole,
            weight: 0.0,
            writers: &[],
            readers: &[],
            themes: &[],
            confirms: false,
        },
        Draft {
            key: K::TeamNotes,
            created: TEAM_NOTES_FROM,
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![
                url("notes.corp.internal", "/team-a/retro"),
                url("notes.corp.internal", "/team-a/plans"),
            ],
            window: (TEAM_NOTES_FROM, NOW),
            weight: 6.0,
            writers: &["cc4", "cx1", "cc7"],
            readers: &["cc1.b", "cx1.a", "cc7", "cc4"],
            themes: &[(T::Meetings, 3.0), (T::Deploy, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::HijackedWiki,
            created: HIJACK_FROM,
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![
                url("wiki.example.org", "/wiki/Agent_Coordination"),
                url("wiki.example.org", "/wiki/Agent_Coordination/Handoff"),
                Locator::Url {
                    scheme: "https".to_owned(),
                    host: Host("wiki.example.org".to_owned()),
                    path: "/w/index.php".to_owned(),
                    query: Some("action=raw&title=Agent_Coordination".to_owned()),
                },
            ],
            window: (HIJACK_FROM, NOW),
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
            created: ago(4 * DAY),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![url("wiki.example.org", "/wiki/Talk:Agent_Coordination")],
            window: (ago(4 * DAY), NOW),
            weight: 3.0,
            writers: &["pi0", "cx2"],
            readers: &["cc6", "omp0.b"],
            themes: &[(T::Injection, 2.0), (T::Scraping, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Pastebin,
            created: plus(START, 5 * HOUR),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![
                url("paste.example.net", "/raw/q8Zt3LmK"),
                url("paste.example.net", "/raw/Hx71bPwe"),
                url("paste.example.net", "/raw/3nVd0aRc"),
            ],
            window: (plus(START, 5 * HOUR), NOW),
            weight: 8.0,
            writers: &["pi1", "omp0", "al0"],
            readers: &["cc5", "cx3", "cc0", "pi3"],
            themes: &[(T::Credentials, 4.0), (T::Scraping, 2.0)],
            confirms: true,
        },
        Draft {
            key: K::McpMemory,
            created: plus(START, 2 * HOUR),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![
                Locator::Mcp {
                    server: "memory".to_owned(),
                    tool: ToolName("create_entities".to_owned()),
                    target: Some("project-atlas".to_owned()),
                },
                Locator::Mcp {
                    server: "memory".to_owned(),
                    tool: ToolName("search_nodes".to_owned()),
                    target: Some("project-atlas".to_owned()),
                },
                Locator::Mcp {
                    server: "memory".to_owned(),
                    tool: ToolName("read_graph".to_owned()),
                    target: None,
                },
            ],
            window: (plus(START, 2 * HOUR), NOW),
            weight: 8.0,
            writers: &["cc0", "cc0.a", "omp1", "al0"],
            readers: &["cc0.b", "cc1", "omp1", "cc0"],
            themes: &[(T::Research, 3.0), (T::DataPipeline, 2.0)],
            confirms: true,
        },
        Draft {
            key: K::SharedFile,
            created: plus(START, 9 * HOUR),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![
                Locator::File {
                    host: Some(Host("devbox-3".to_owned())),
                    path: "/tmp/agent-handoff/plan.md".to_owned(),
                },
                Locator::File {
                    host: Some(Host("devbox-3".to_owned())),
                    path: "/tmp/agent-handoff/status.json".to_owned(),
                },
            ],
            window: (plus(START, 9 * HOUR), NOW),
            weight: 7.0,
            writers: &["cc2", "cc3"],
            readers: &["cx4", "cc3.a", "cc2"],
            themes: &[(T::Deploy, 3.0), (T::Meetings, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::Gist,
            created: plus(START, HOUR),
            origin: DraftOrigin::Discovered,
            target: Target::Dormant,
            locators: vec![url("gist.example.com", "/anon/5d41402abc4b2a76")],
            window: (plus(START, HOUR), GIST_UNTIL),
            weight: 3.0,
            writers: &["pi3", "cx0"],
            readers: &["cx1", "cc1"],
            themes: &[(T::DataPipeline, 2.0), (T::Credentials, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::S3Handoff,
            created: ago(2 * DAY),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![Locator::Url {
                scheme: "s3".to_owned(),
                host: Host("agent-scratch".to_owned()),
                path: "/handoff/batch-0412.jsonl".to_owned(),
                query: None,
            }],
            window: (ago(2 * DAY), NOW),
            weight: 3.0,
            writers: &["sh0"],
            readers: &["cc6", "sh1"],
            themes: &[(T::DataPipeline, 1.0)],
            confirms: false,
        },
        Draft {
            key: K::SelfNotes,
            created: SELF_NOTES_FROM,
            origin: DraftOrigin::Discovered,
            target: Target::Dormant,
            locators: vec![Locator::File {
                host: Some(Host("devbox-7".to_owned())),
                path: "/home/dev/.codex/handoff.md".to_owned(),
            }],
            window: (SELF_NOTES_FROM, SELF_NOTES_UNTIL),
            weight: 2.0,
            writers: &["al1", "cx1"],
            readers: &["cx1", "al1"],
            themes: &[(T::Deploy, 1.0), (T::CodeReview, 1.0)],
            confirms: true,
        },
        Draft {
            key: K::OldTeamNotes,
            created: plus(START, 3 * HOUR),
            origin: DraftOrigin::Discovered,
            target: Target::Active,
            locators: vec![url("notes.corp.internal", "/team-a/standup")],
            window: (plus(START, 3 * HOUR), PROMOTE_AT),
            weight: 4.0,
            writers: &["cc4", "cx1", "al1"],
            readers: &["cc1.b", "cx1.a", "cc7"],
            themes: &[(T::Meetings, 3.0)],
            confirms: true,
        },
    ]
}
