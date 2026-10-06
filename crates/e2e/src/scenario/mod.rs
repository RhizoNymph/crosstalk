//! The wiki relay: the smallest traffic that is agent-to-agent
//! communication through a shared resource.
//!
//! | # | Agent | Exchange | What it carries |
//! | - | ----- | -------- | --------------- |
//! | 1 | A | `a1-write` | user asks A to publish the runbook; A answers with a tool call that writes the wiki page, its content carrying [`SENTENCE`] |
//! | 2 | A | `a2-ack` | the tool result (page saved) comes back; A confirms in text |
//! | 3 | B | `b1-read` | user asks B about the rollback rule; B answers with a tool call that reads the same page |
//! | 4 | B | `b2-repeat` | the tool result is the page, [`SENTENCE`] in it; B's answer repeats [`SENTENCE`] |
//!
//! A and B are separate Claude Code sessions with separate API keys and
//! session ids, so L3 resolves two agents. [`Scenario::wiki_relay_subscription`]
//! is the same relay on Claude Pro/Max logins: each session sends a fake
//! OAuth access token with the OAuth capability, and A's token is
//! refreshed between its two exchanges, so only the session id carries A
//! across them. The sentence is A's own output
//! (the write call's input), so L4 indexes it as originated by A and
//! matches it in B's tool result (`ContentMatched`). L5 extracts a write
//! access from exchange 1 and a read access from exchange 4 (reads are
//! recorded once their result is back) on one URL locator, discovers the
//! channel at the first cross-agent access, and correlates the write and
//! the read into a channel transmission that the content match confirms.
//! L7 counts it on the edge from A to B, routed through the channel.
//!
//! Times are fixed offsets from a start the caller picks
//! ([`Scenario::wiki_relay`]); ids and bodies depend on nothing else, so
//! the same start gives byte-identical traffic.

mod tools;
pub mod wire;

use std::time::Duration;

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::support::Timestamp;

pub use self::tools::{WIKI_PAGE, read_call, write_call};
use self::wire::{
    Block, Credential, HttpRequest, HttpResponse, Role, SessionHeaders, Stop, ToolSpec, Turn, Usage,
};

/// How the scenario's sessions authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    /// Console API keys, one per session.
    ApiKeys,
    /// Claude Pro/Max logins: fake OAuth access tokens, A's refreshed
    /// between its two exchanges.
    Subscription,
}

/// The fake access tokens of [`Auth::Subscription`]: A's before and after
/// its refresh, then B's. Shaped like real ones; never real.
pub const SUBSCRIPTION_TOKENS: [&str; 3] = [
    "sk-ant-oat01-TEST-e2e-agent-a-Hq4Wz8Lk2Vn6Rt9Yb3Mc7Xp1",
    "sk-ant-oat01-TEST-e2e-agent-a-refreshed-Jd5Gs8Ku2Ne6Qw",
    "sk-ant-oat01-TEST-e2e-agent-b-Zf3Tm7Ry1Pc5Lh9Vx2Kb6Sn",
];

/// The sentence A originates and B repeats: distinctive enough that no
/// other text in the scenario (or anywhere) shares a shingle with it.
pub const SENTENCE: &str = "Before any rollback of the ledger service, drain the amber retry queue on shard nine and page the on-call heron rotation twice.";

/// 2026-10-01T09:00:00Z: the default start.
pub const DEFAULT_START: Timestamp = Timestamp::from_micros(1_790_845_200_000_000);

/// One agent of the scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioAgent {
    /// `a` or `b`.
    pub name: &'static str,
    pub headers: SessionHeaders,
}

/// One scripted exchange: the request Claude Code sent, the response it
/// got, and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireExchange {
    /// A stable name (`a1-write`).
    pub label: &'static str,
    /// The scenario agent that made it (`a` or `b`).
    pub agent: &'static str,
    /// The id the proxy minted for it.
    pub id: ExchangeId,
    pub started_at: Timestamp,
    pub first_chunk_at: Timestamp,
    pub ended_at: Timestamp,
    pub request: HttpRequest,
    pub response: HttpResponse,
}

/// The scripted traffic, in time order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub start: Timestamp,
    pub agents: [ScenarioAgent; 2],
    pub exchanges: Vec<WireExchange>,
}

/// One exchange's offsets from the start.
struct Timing {
    started: Duration,
    first_chunk: Duration,
    ended: Duration,
}

fn at(start: Timestamp, offset: Duration) -> Timestamp {
    let micros = u64::try_from(offset.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(start.as_micros().saturating_add(micros))
}

/// The exchange id the proxy would mint at `started_at`: a ULID whose time
/// part is the start's millisecond and whose random part is fixed per
/// exchange.
fn exchange_id(started_at: Timestamp, ordinal: u128) -> ExchangeId {
    let millis = u128::from(started_at.as_micros() / 1_000);
    ExchangeId::from_ulid((millis << 80) | (0xE2E0_0000 << 32) | ordinal)
}

impl Scenario {
    /// The wiki relay starting at `start`.
    pub fn wiki_relay(start: Timestamp) -> Self {
        Self::relay_with(start, tools::page_as_read(), relay_answer(), Auth::ApiKeys)
    }

    /// The wiki relay on Claude Pro/Max logins ([`Auth::Subscription`]):
    /// the same traffic but for the credentials, with A's access token
    /// refreshed between `a1-write` and `a2-ack`. `agents` holds each
    /// session's first token.
    pub fn wiki_relay_subscription(start: Timestamp) -> Self {
        Self::relay_with(
            start,
            tools::page_as_read(),
            relay_answer(),
            Auth::Subscription,
        )
    }

    /// The wiki relay where nothing crosses: A writes the page as before,
    /// but by the time B reads it the page holds only a placeholder
    /// (`tools::withheld_as_read`), and B's answer says so. B's read of
    /// A's page is a co-access with no content match: the correlator opens
    /// a channel transmission, suspects it when its evidence window closes
    /// and discards it when its suspicion expires.
    pub fn wiki_silent_read(start: Timestamp) -> Self {
        Self::relay_with(
            start,
            tools::withheld_as_read(),
            "The runbook page is under review and has no steps in it yet; I can't tell you what has to happen before a rollback.".to_owned(),
            Auth::ApiKeys,
        )
    }

    /// The relay with B's `Read` returning `read_result`, B answering
    /// `answer`, and the sessions authenticating by `auth`.
    fn relay_with(start: Timestamp, read_result: String, answer: String, auth: Auth) -> Self {
        let credential = |api_key: &str, token: &str| match auth {
            Auth::ApiKeys => Credential::ApiKey(api_key.to_owned()),
            Auth::Subscription => Credential::Subscription {
                access_token: token.to_owned(),
            },
        };
        let a = ScenarioAgent {
            name: "a",
            headers: SessionHeaders {
                credential: credential(
                    "sk-ant-api03-e2e-agent-a-0000000000000000000000000000000000000000000000000000000000000000-AAAAAAAA",
                    SUBSCRIPTION_TOKENS[0],
                ),
                session_id: "3f1c6a52-8d0e-4b7a-9c21-5e6f0a1b2c3d".to_owned(),
                user_hash: "a1".repeat(32),
                cwd: "/home/ops/ledger".to_owned(),
            },
        };
        let b = ScenarioAgent {
            name: "b",
            headers: SessionHeaders {
                credential: credential(
                    "sk-ant-api03-e2e-agent-b-1111111111111111111111111111111111111111111111111111111111111111-BBBBBBBB",
                    SUBSCRIPTION_TOKENS[2],
                ),
                session_id: "9b7e2d14-6c3f-4e8a-b5d9-0f1e2a3b4c5d".to_owned(),
                user_hash: "b2".repeat(32),
                cwd: "/home/sre/oncall".to_owned(),
            },
        };
        // A after its token refresh: on API keys, nothing changes.
        let a_later = match auth {
            Auth::ApiKeys => a.clone(),
            Auth::Subscription => ScenarioAgent {
                name: a.name,
                headers: SessionHeaders {
                    credential: credential("", SUBSCRIPTION_TOKENS[1]),
                    ..a.headers.clone()
                },
            },
        };
        let tools = tools::declared();
        let mut exchanges = Vec::with_capacity(4);

        // A: publish the runbook.
        let (write_id, write_name, write_input) = write_call();
        let a_ask = Turn {
            role: Role::User,
            blocks: vec![Block::Text(
                "Publish the ledger rollback runbook to the team wiki so the on-call rotation can find it."
                    .to_owned(),
            )],
        };
        let a_call = Turn {
            role: Role::Assistant,
            blocks: vec![
                Block::Text("I'll publish the runbook to the team wiki.".to_owned()),
                Block::ToolUse {
                    id: write_id.clone(),
                    name: write_name,
                    input: write_input,
                },
            ],
        };
        exchanges.push(exchange(
            &a,
            start,
            &tools,
            Step {
                label: "a1-write",
                ordinal: 1,
                timing: Timing {
                    started: Duration::ZERO,
                    first_chunk: Duration::from_millis(900),
                    ended: Duration::from_millis(4_200),
                },
                history: std::slice::from_ref(&a_ask),
                answer: &a_call.blocks,
                stop: Stop::ToolUse,
                message_id: "msg_01E2EA1WriteRunbook000001",
            },
        ));
        let a_saved = Turn {
            role: Role::User,
            blocks: vec![Block::ToolResult {
                tool_use_id: write_id,
                content: tools::WRITE_RESULT.to_owned(),
            }],
        };
        let a_done = vec![Block::Text(format!(
            "Published the runbook at {WIKI_PAGE}."
        ))];
        exchanges.push(exchange(
            &a_later,
            start,
            &tools,
            Step {
                label: "a2-ack",
                ordinal: 2,
                timing: Timing {
                    started: Duration::from_millis(5_000),
                    first_chunk: Duration::from_millis(5_700),
                    ended: Duration::from_millis(6_300),
                },
                history: &[a_ask, a_call, a_saved],
                answer: &a_done,
                stop: Stop::EndTurn,
                message_id: "msg_01E2EA2AckPublished000002",
            },
        ));

        // B, two minutes later: read the page, repeat the rule.
        let (read_id, read_name, read_input) = read_call();
        let b_ask = Turn {
            role: Role::User,
            blocks: vec![Block::Text(
                "Check the team wiki's ledger rollback runbook and tell me what has to happen before a rollback."
                    .to_owned(),
            )],
        };
        let b_call = Turn {
            role: Role::Assistant,
            blocks: vec![
                Block::Text("Let me read the runbook on the wiki.".to_owned()),
                Block::ToolUse {
                    id: read_id.clone(),
                    name: read_name,
                    input: read_input,
                },
            ],
        };
        exchanges.push(exchange(
            &b,
            start,
            &tools,
            Step {
                label: "b1-read",
                ordinal: 3,
                timing: Timing {
                    started: Duration::from_secs(120),
                    first_chunk: Duration::from_millis(120_800),
                    ended: Duration::from_millis(122_100),
                },
                history: std::slice::from_ref(&b_ask),
                answer: &b_call.blocks,
                stop: Stop::ToolUse,
                message_id: "msg_01E2EB1ReadRunbook0000003",
            },
        ));
        let b_page = Turn {
            role: Role::User,
            blocks: vec![Block::ToolResult {
                tool_use_id: read_id,
                content: read_result,
            }],
        };
        let b_answer = vec![Block::Text(answer)];
        exchanges.push(exchange(
            &b,
            start,
            &tools,
            Step {
                label: "b2-repeat",
                ordinal: 4,
                timing: Timing {
                    started: Duration::from_millis(123_000),
                    first_chunk: Duration::from_millis(124_100),
                    ended: Duration::from_millis(126_900),
                },
                history: &[b_ask, b_call, b_page],
                answer: &b_answer,
                stop: Stop::EndTurn,
                message_id: "msg_01E2EB2RepeatRule00000004",
            },
        ));

        Self {
            start,
            agents: [a, b],
            exchanges,
        }
    }

    /// The end of the last exchange.
    pub fn ends_at(&self) -> Timestamp {
        self.exchanges
            .last()
            .map_or(self.start, |exchange| exchange.ended_at)
    }
}

/// B's answer in the relay: it repeats [`SENTENCE`].
fn relay_answer() -> String {
    format!(
        "The runbook is explicit about it: {SENTENCE} After that the rollback itself is a normal deploy of the previous tag."
    )
}

/// One scripted exchange before it is put on the wire.
struct Step<'s> {
    label: &'static str,
    /// Its position in the scenario, from 1: the random part of its id.
    ordinal: u128,
    timing: Timing,
    /// The conversation the request carries, ending in a user turn.
    history: &'s [Turn],
    /// The assistant blocks the response streams.
    answer: &'s [Block],
    stop: Stop,
    message_id: &'static str,
}

fn exchange(
    agent: &ScenarioAgent,
    start: Timestamp,
    tools: &[ToolSpec],
    step: Step<'_>,
) -> WireExchange {
    let Step {
        label,
        ordinal,
        timing,
        history,
        answer,
        stop,
        message_id,
    } = step;
    let started_at = at(start, timing.started);
    let request_id = format!("req_011E2E{label}{ordinal:04}");
    let output = answer
        .iter()
        .map(|block| match block {
            Block::Text(text) => text.split_whitespace().count(),
            _ => 40,
        })
        .sum::<usize>();
    let usage = Usage {
        input: 180 + 60 * u32::try_from(history.len()).unwrap_or(u32::MAX),
        cache_read: 3_871,
        output: u32::try_from(output).unwrap_or(u32::MAX).saturating_mul(2),
    };
    WireExchange {
        label,
        agent: agent.name,
        id: exchange_id(started_at, ordinal),
        started_at,
        first_chunk_at: at(start, timing.first_chunk),
        ended_at: at(start, timing.ended),
        request: wire::request(&agent.headers, history, tools),
        response: wire::response(message_id, &request_id, answer, stop, usage),
    }
}
