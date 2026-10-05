//! Exchange reconstruction for one agent in one episode.
//!
//! An agent's message list only grows within an episode, and each assistant
//! message is one call's response whose request is everything before it.
//! The episode's own calls are the assistant messages after the last
//! `## Episode N: task phase` user turn: in carried-over lists (main, cross
//! model, warm-up) that skips earlier episodes, and in rewritten ones
//! (memory length and scope, sliding-window truncation) it skips the
//! replayed memory. The count is checked against the episode's accepted
//! `llm_usage` calls for the agent: equal means `Reconstructed`, else
//! `Synthetic`.
//!
//! **Clock.** Nothing has a time, so the virtual clock orders exchanges by
//! episode-global event ids, one paced call step per event
//! ([`Pace`], 1 to 5 s by default). Tool calls
//! are matched to the agent's events in order. Episodes follow each other:
//! each starts at the step after the previous one's last
//! ([`episode_steps`]), and within it:
//!
//! - an exchange whose response makes a tool call happens just before that
//!   call's event `e`: step `e + 1`, sub 0;
//! - any other exchange happens after every input it saw: step `floor`,
//!   sub 1, where `floor` is one past the latest event among the delivered
//!   peer messages and tool results before it, and never before the agent's
//!   previous exchange.
//!
//! So a sender's exchange (at its send event) always precedes the reader's
//! exchange that first carries the delivered message.

use std::collections::BTreeMap;

use crosstalk_spec::observed::exchange::{StopReason, TokenCounts, TokenUsage};
use crosstalk_spec::support::Timestamp;

use super::SaltError;
use super::messages::convert;
use super::schema::{Delivery, Episode, RawMessage, Usage};
use crate::corpus::clock::{ClockError, Pace};
use crate::corpus::{Fidelity, HashedMessage};

/// Where an episode's calls fall: the pace of a step and the step the
/// episode starts at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpisodeClock {
    pub pace: Pace,
    pub start: u64,
}

/// The call steps an episode takes: every time within it is at most one
/// past its last event (step `e + 1`), and one more step separates it from
/// the next episode.
pub fn episode_steps(episode: &Episode) -> u64 {
    let events = episode.events.iter().map(|event| event.event_id);
    let deliveries = episode
        .channel_transcript
        .iter()
        .map(|delivery| delivery.event_id);
    events
        .chain(deliveries)
        .max()
        .map_or(2, |last| last.saturating_add(3))
}

/// A peer message as the harness delivers it: `[round=r/n][from=x][type=t]`,
/// a blank line, then the content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredTurn {
    pub from: String,
    /// Byte offset of the content in the user turn.
    pub content_start: usize,
}

/// Parses a delivered-message header.
pub fn delivered_turn(text: &str) -> Option<DeliveredTurn> {
    let break_at = text.find("\n\n")?;
    let header = &text[..break_at];
    if !header.starts_with("[round=") || !header.ends_with(']') {
        return None;
    }
    let from_at = header.find("[from=")? + "[from=".len();
    let from_end = header[from_at..].find(']')? + from_at;
    Some(DeliveredTurn {
        from: header[from_at..from_end].to_owned(),
        content_start: break_at + 2,
    })
}

/// Whether a user turn opens an episode's task phase.
pub fn is_task_marker(text: &str) -> bool {
    let first_line = text.lines().next().unwrap_or_default();
    first_line.starts_with("## Episode ") && first_line.ends_with(": task phase")
}

/// One call of the agent in the episode.
#[derive(Debug, Clone)]
pub struct Turn {
    /// Index of the response in the agent's message list.
    pub index: usize,
    pub at: Timestamp,
    pub usage: Option<Usage>,
}

/// The reconstruction of one agent's episode.
#[derive(Debug, Clone)]
pub struct AgentEpisode {
    pub agent: String,
    pub raw: Vec<RawMessage>,
    pub messages: Vec<HashedMessage>,
    /// First message of this episode's own region.
    pub start: usize,
    pub turns: Vec<Turn>,
    pub fidelity: Fidelity,
    /// `(message index, call index)` of each region tool call → its event.
    pub call_events: BTreeMap<(usize, usize), u64>,
    /// Message index of each delivered peer turn in the region → the
    /// transcript entry's event.
    pub delivered: BTreeMap<usize, u64>,
}

impl AgentEpisode {
    /// The turn whose request first carries message `index`: the first call
    /// after it.
    pub fn first_turn_after(&self, index: usize) -> Option<&Turn> {
        self.turns.iter().find(|turn| turn.index > index)
    }

    /// The first turn later than `at`.
    pub fn first_turn_at_or_after(&self, at: Timestamp) -> Option<&Turn> {
        self.turns.iter().find(|turn| turn.at >= at)
    }

    /// The message index and call index of the tool call that made `event`.
    pub fn call_of_event(&self, event: u64) -> Option<(usize, usize)> {
        self.call_events
            .iter()
            .find(|(_, e)| **e == event)
            .map(|(key, _)| *key)
    }
}

pub fn stop_reason(finish: Option<&str>) -> StopReason {
    match finish {
        Some("stop") => StopReason::EndTurn,
        Some("tool_calls") | Some("function_call") => StopReason::ToolUse,
        Some("length") => StopReason::MaxTokens,
        Some("content_filter") => StopReason::Refusal,
        _ => StopReason::Other,
    }
}

pub fn token_usage(usage: &Usage) -> Option<TokenUsage> {
    let clamp = |n: Option<u64>| u32::try_from(n.unwrap_or(0)).unwrap_or(u32::MAX);
    if usage.input_tokens.is_none() && usage.output_tokens.is_none() {
        return None;
    }
    // OpenAI-style counts: `input` holds the cached part; no cache writes.
    TokenUsage::new(TokenCounts {
        input: clamp(usage.input_tokens),
        output: clamp(usage.output_tokens),
        cache_read: clamp(usage.cached_input_tokens),
        cache_write: None,
        reasoning: usage.reasoning_tokens.and_then(|n| u32::try_from(n).ok()),
    })
    .ok()
}

/// Reconstructs `agent`'s calls in `episode`.
pub fn reconstruct(
    agent: &str,
    episode: &Episode,
    file: &str,
    scripted: bool,
    clock_at: EpisodeClock,
) -> Result<AgentEpisode, SaltError> {
    let raw = episode
        .agents
        .get(agent)
        .map(|record| record.messages.clone())
        .unwrap_or_default();
    let messages = raw
        .iter()
        .map(|message| convert(message).map(HashedMessage::new))
        .collect::<Result<Vec<_>, _>>()?;
    let start = raw
        .iter()
        .rposition(|message| {
            message.role == "user"
                && super::messages::content_text(&message.content)
                    .lines()
                    .next()
                    .is_some_and(is_task_marker)
        })
        .unwrap_or(0);

    let usage: Vec<Usage> = episode
        .llm_usage
        .iter()
        .filter(|usage| usage.actor == agent && usage.accepted())
        .cloned()
        .collect();
    let responses: Vec<usize> = (start..raw.len())
        .filter(|&at| raw[at].role == "assistant")
        .collect();
    let fidelity = if scripted || responses.len() == usage.len() {
        Fidelity::Reconstructed
    } else {
        tracing::warn!(
            file,
            episode = episode.episode_index,
            agent,
            responses = responses.len(),
            accepted_calls = usage.len(),
            "assistant messages do not match accepted calls; exchanges are synthetic"
        );
        Fidelity::Synthetic
    };

    let mut events: Vec<u64> = episode
        .events
        .iter()
        .filter(|event| event.actor == agent)
        .map(|event| event.event_id)
        .collect();
    events.sort_unstable();
    let calls: Vec<(usize, usize)> = responses
        .iter()
        .flat_map(|&at| {
            let count = raw[at].tool_calls.as_ref().map_or(0, Vec::len);
            (0..count).map(move |call| (at, call))
        })
        .collect();
    if calls.len() != events.len() {
        tracing::debug!(
            file,
            episode = episode.episode_index,
            agent,
            calls = calls.len(),
            events = events.len(),
            "tool calls and events differ in number; matched in order"
        );
    }
    let call_events: BTreeMap<(usize, usize), u64> = calls.into_iter().zip(events).collect();
    let delivered = delivered_turns(agent, &raw, start, &episode.channel_transcript);
    let turns = clock(
        &raw,
        start,
        &responses,
        &call_events,
        &delivered,
        usage,
        clock_at,
    )?;
    Ok(AgentEpisode {
        agent: agent.to_owned(),
        raw,
        messages,
        start,
        turns,
        fidelity,
        call_events,
        delivered,
    })
}

/// Each delivered peer turn in the region, matched to its transcript entry
/// (same sender and content, each entry used once).
fn delivered_turns(
    agent: &str,
    raw: &[RawMessage],
    start: usize,
    transcript: &[Delivery],
) -> BTreeMap<usize, u64> {
    let mut used = vec![false; transcript.len()];
    let mut out = BTreeMap::new();
    for (at, message) in raw.iter().enumerate().skip(start) {
        if message.role != "user" {
            continue;
        }
        let text = super::messages::content_text(&message.content);
        let Some(turn) = delivered_turn(&text) else {
            continue;
        };
        let content = &text[turn.content_start..];
        let found = transcript.iter().enumerate().position(|(i, delivery)| {
            !used[i]
                && delivery.receiver == agent
                && delivery.sender == turn.from
                && delivery.content == content
        });
        if let Some(i) = found {
            used[i] = true;
            out.insert(at, transcript[i].event_id);
        }
    }
    out
}

fn clock(
    raw: &[RawMessage],
    start: usize,
    responses: &[usize],
    call_events: &BTreeMap<(usize, usize), u64>,
    delivered: &BTreeMap<usize, u64>,
    usage: Vec<Usage>,
    clock_at: EpisodeClock,
) -> Result<Vec<Turn>, SaltError> {
    let EpisodeClock {
        pace,
        start: episode_start,
    } = clock_at;
    let mut call_event_by_id: BTreeMap<&str, u64> = BTreeMap::new();
    for ((at, call), event) in call_events {
        if let Some(tool_call) = raw[*at]
            .tool_calls
            .as_ref()
            .and_then(|calls| calls.get(*call))
        {
            call_event_by_id.insert(tool_call.id.as_str(), *event);
        }
    }
    let mut usage = usage.into_iter();
    let mut floor: u64 = 0;
    let mut last: Option<Timestamp> = None;
    let mut turns = Vec::with_capacity(responses.len());
    for (at, message) in raw.iter().enumerate().skip(start) {
        if let Some(event) = delivered.get(&at) {
            floor = floor.max(event + 1);
        }
        if message.role == "tool"
            && let Some(event) = message
                .tool_call_id
                .as_deref()
                .and_then(|id| call_event_by_id.get(id))
        {
            floor = floor.max(event + 1);
        }
        if message.role != "assistant" {
            continue;
        }
        let own = call_events.get(&(at, 0)).copied();
        let (minor, sub) = match own {
            Some(event) if event + 1 >= floor => (event + 1, 0),
            _ => (floor, 1),
        };
        let step = episode_start
            .checked_add(minor)
            .ok_or(SaltError::Clock(ClockError::Major(minor)))?;
        let mut time = pace.at(step, 0, sub).map_err(SaltError::Clock)?;
        if let Some(previous) = last
            && time <= previous
        {
            time = Timestamp::from_micros(previous.as_micros() + 1);
        }
        last = Some(time);
        floor = floor.max(minor);
        turns.push(Turn {
            index: at,
            at: time,
            usage: usage.next(),
        });
    }
    debug_assert_eq!(turns.len(), responses.len());
    Ok(turns)
}
