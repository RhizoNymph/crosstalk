//! The swarm's knobs.

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use crate::http::BaseUrl;
use crate::knobs::{Fraction, PositiveSpan, Span};
use crate::protocol::Scenario;

/// How often a turn writes, reads or just chats: `write + read <= 1`, the
/// rest is chat.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TaskMix {
    write: Fraction,
    read: Fraction,
}

/// Why a mix is refused.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("write fraction {write} plus read fraction {read} is over 1")]
pub struct MixError {
    pub write: f64,
    pub read: f64,
}

impl TaskMix {
    pub fn new(write: Fraction, read: Fraction) -> Result<Self, MixError> {
        if write.get() + read.get() > 1.0 + 1e-9 {
            return Err(MixError {
                write: write.get(),
                read: read.get(),
            });
        }
        Ok(Self { write, read })
    }

    pub fn write(self) -> Fraction {
        self.write
    }

    pub fn read(self) -> Fraction {
        self.read
    }
}

/// Everything a swarm run is configured with.
#[derive(Debug, Clone, PartialEq)]
pub struct SwarmConfig {
    /// The gateway's Anthropic base URL, e.g. `http://crosstalk:8080/anthropic`.
    pub gateway: BaseUrl,
    /// The wiki, e.g. `http://wiki:8090`.
    pub wiki: BaseUrl,
    pub agents: NonZeroU32,
    /// Agents sharing one `x-api-key` (1: a key per agent).
    pub agents_per_key: NonZeroU32,
    /// Milliseconds between an answer and the next prompt.
    pub think_ms: Span,
    /// Prompts per conversation before a new one starts.
    pub turns: PositiveSpan,
    pub mix: TaskMix,
    /// Distinct wiki pages.
    pub pages: NonZeroU32,
    /// Distinct topics the pages and prompts are about.
    pub topics: NonZeroU32,
    pub duration: Duration,
    /// Agents start evenly spread over this.
    pub ramp: Duration,
    pub seed: u64,
    /// Which benchmark this is: the prose style every agent's system prompt
    /// asks the model for.
    pub scenario: Scenario,
    /// Puts a `role: "system"` turn inside `messages` before each prompt.
    pub claude_code_shape: bool,
    /// Requests that ask for a stream.
    pub stream_fraction: Fraction,
    pub model: String,
    pub max_tokens: NonZeroU32,
    /// Give up on a response when nothing arrives for this long.
    pub idle_timeout: Duration,
    /// After the run, how long in-flight requests may take to finish.
    pub grace: Duration,
    /// Where to write the expected transmissions (JSON lines), if anywhere.
    pub ground_truth: Option<PathBuf>,
}

impl SwarmConfig {
    /// The defaults, against `gateway` and `wiki`.
    pub fn new(gateway: BaseUrl, wiki: BaseUrl) -> Self {
        Self {
            gateway,
            wiki,
            agents: NonZeroU32::MIN.saturating_add(149),
            agents_per_key: NonZeroU32::MIN,
            think_ms: Span::ordered(2_000, 8_000),
            turns: PositiveSpan::ordered(4, 12),
            mix: TaskMix {
                write: Fraction::new(0.25).unwrap_or(Fraction::ZERO),
                read: Fraction::new(0.35).unwrap_or(Fraction::ZERO),
            },
            pages: NonZeroU32::MIN.saturating_add(39),
            topics: NonZeroU32::MIN.saturating_add(7),
            duration: Duration::from_secs(300),
            ramp: Duration::from_secs(20),
            seed: 42,
            scenario: Scenario::Headline,
            claude_code_shape: false,
            stream_fraction: Fraction::ONE,
            model: "claude-opus-5-5".to_owned(),
            max_tokens: NonZeroU32::MIN.saturating_add(4095),
            idle_timeout: Duration::from_secs(120),
            grace: Duration::from_secs(30),
            ground_truth: None,
        }
    }
}
