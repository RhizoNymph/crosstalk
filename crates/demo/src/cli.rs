//! The command line: one binary, four subcommands.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::str::FromStr;

use crate::anthropic::sse::Split;
use crate::http::BaseUrl;
use crate::knobs::{Fraction, PositiveSpan, Span, parse_duration};
use crate::swarm::config::{SwarmConfig, TaskMix};
use crate::upstream::UpstreamConfig;
use crate::upstream::generate::GenConfig;
use crate::wiki::WikiConfig;

pub const USAGE: &str = "\
crosstalk-demo: a fake Anthropic upstream, a shared wiki and a swarm of agents

usage:
  crosstalk-demo upstream [--listen ADDR] [--seed N] [--words A..B]
                          [--first-byte-ms A..B] [--stream-ms A..B]
  crosstalk-demo wiki [--listen ADDR] [--max-page-bytes N] [--max-pages N]
  crosstalk-demo swarm [--gateway URL] [--wiki URL] [--agents N] [--agents-per-key N]
                       [--think-ms A..B] [--turns A..B] [--write-fraction F]
                       [--read-fraction F] [--pages N] [--topics N] [--duration D]
                       [--ramp D] [--seed N] [--stream-fraction F] [--model NAME]
                       [--max-tokens N] [--idle-timeout D] [--grace D]
                       [--ground-truth PATH] [--json] [--claude-code-shape]
  crosstalk-demo healthcheck --url URL
  crosstalk-demo help

A..B is an inclusive range (or one number), F a fraction in [0, 1], D a
duration (500ms, 90s, 5m, 1h). Defaults: upstream 0.0.0.0:8070, seed 7,
words 40..160, first byte 300..1500 ms, stream 1000..10000 ms; wiki
0.0.0.0:8090, 262144-byte pages, 100000 pages; swarm against
http://127.0.0.1:8080/anthropic and http://127.0.0.1:8090, 150 agents, a key
each, think 2000..8000 ms, 4..12 prompts per conversation, write 0.25, read
0.35, 40 pages, 8 topics, 5m, ramp 20s, seed 42, all streaming, model
claude-opus-5-5, max tokens 4096, idle timeout 120s, grace 30s.
";

/// A parsed command line.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Upstream(UpstreamConfig),
    Wiki(WikiConfig),
    Swarm {
        config: Box<SwarmConfig>,
        json: bool,
    },
    Healthcheck {
        url: BaseUrl,
    },
    Help,
}

/// A command line that cannot be run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UsageError {
    #[error("no subcommand")]
    Missing,
    #[error("unknown subcommand {0:?}")]
    UnknownCommand(String),
    #[error("unknown option {0}")]
    UnknownOption(String),
    #[error("{0} needs a value")]
    NoValue(String),
    #[error("{0} given twice")]
    Twice(String),
    #[error("unexpected argument {0:?}")]
    Positional(String),
    #[error("{name}: {reason}")]
    Value { name: String, reason: String },
    #[error("{0} is required")]
    Required(&'static str),
}

/// The options given to one subcommand.
#[derive(Debug, Default)]
struct Flags {
    values: BTreeMap<String, String>,
    switches: BTreeSet<String>,
}

impl Flags {
    fn parse(args: &[String], valued: &[&str], switches: &[&str]) -> Result<Self, UsageError> {
        let mut flags = Self::default();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            let Some(option) = arg.strip_prefix("--") else {
                return Err(UsageError::Positional(arg.clone()));
            };
            let (name, inline) = match option.split_once('=') {
                Some((name, value)) => (name, Some(value.to_owned())),
                None => (option, None),
            };
            if switches.contains(&name) && inline.is_none() {
                if !flags.switches.insert(name.to_owned()) {
                    return Err(UsageError::Twice(arg.clone()));
                }
            } else if valued.contains(&name) {
                let value = match inline {
                    Some(value) => value,
                    None => args
                        .next()
                        .cloned()
                        .ok_or_else(|| UsageError::NoValue(format!("--{name}")))?,
                };
                if flags.values.insert(name.to_owned(), value).is_some() {
                    return Err(UsageError::Twice(format!("--{name}")));
                }
            } else {
                return Err(UsageError::UnknownOption(arg.clone()));
            }
        }
        Ok(flags)
    }

    fn get<T>(&self, name: &str, default: T) -> Result<T, UsageError>
    where
        T: FromStr,
        T::Err: Display,
    {
        self.get_with(name, default, |text| {
            text.parse::<T>().map_err(|e| e.to_string())
        })
    }

    fn get_with<T>(
        &self,
        name: &str,
        default: T,
        parse: impl Fn(&str) -> Result<T, String>,
    ) -> Result<T, UsageError> {
        match self.values.get(name) {
            None => Ok(default),
            Some(text) => parse(text).map_err(|reason| UsageError::Value {
                name: format!("--{name}"),
                reason,
            }),
        }
    }

    fn duration(
        &self,
        name: &str,
        default: std::time::Duration,
    ) -> Result<std::time::Duration, UsageError> {
        self.get_with(name, default, |text| {
            parse_duration(text).map_err(|e| e.to_string())
        })
    }

    fn switch(&self, name: &str) -> bool {
        self.switches.contains(name)
    }
}

impl Command {
    /// Parses the arguments after the program name.
    pub fn parse(args: &[String]) -> Result<Self, UsageError> {
        let (command, rest) = args.split_first().ok_or(UsageError::Missing)?;
        match command.as_str() {
            "upstream" => upstream(rest),
            "wiki" => wiki(rest),
            "swarm" => swarm(rest),
            "healthcheck" => {
                let flags = Flags::parse(rest, &["url"], &[])?;
                let text = flags
                    .values
                    .get("url")
                    .ok_or(UsageError::Required("--url"))?;
                let url = text
                    .parse()
                    .map_err(|e: crate::http::UrlError| UsageError::Value {
                        name: "--url".to_owned(),
                        reason: e.to_string(),
                    })?;
                Ok(Command::Healthcheck { url })
            }
            "help" | "--help" | "-h" => Ok(Command::Help),
            other => Err(UsageError::UnknownCommand(other.to_owned())),
        }
    }
}

fn upstream(args: &[String]) -> Result<Command, UsageError> {
    let flags = Flags::parse(
        args,
        &["listen", "seed", "words", "first-byte-ms", "stream-ms"],
        &[],
    )?;
    let defaults = GenConfig::default();
    Ok(Command::Upstream(UpstreamConfig {
        listen: flags.get("listen", SocketAddr::from(([0, 0, 0, 0], 8070)))?,
        generation: GenConfig {
            seed: flags.get("seed", defaults.seed)?,
            words: flags
                .get::<PositiveSpan>("words", PositiveSpan::ordered(40, 160))?
                .get(),
            first_byte_ms: flags.get::<Span>("first-byte-ms", defaults.first_byte_ms)?,
            stream_ms: flags.get::<Span>("stream-ms", defaults.stream_ms)?,
        },
        split: Split::default(),
    }))
}

fn wiki(args: &[String]) -> Result<Command, UsageError> {
    let flags = Flags::parse(args, &["listen", "max-page-bytes", "max-pages"], &[])?;
    Ok(Command::Wiki(WikiConfig {
        listen: flags.get("listen", SocketAddr::from(([0, 0, 0, 0], 8090)))?,
        max_page_bytes: flags
            .get::<NonZeroU32>("max-page-bytes", NonZeroU32::MIN.saturating_add(262_143))?
            .get() as usize,
        max_pages: flags
            .get::<NonZeroU32>("max-pages", NonZeroU32::MIN.saturating_add(99_999))?
            .get() as usize,
    }))
}

fn swarm(args: &[String]) -> Result<Command, UsageError> {
    let flags = Flags::parse(
        args,
        &[
            "gateway",
            "wiki",
            "agents",
            "agents-per-key",
            "think-ms",
            "turns",
            "write-fraction",
            "read-fraction",
            "pages",
            "topics",
            "duration",
            "ramp",
            "seed",
            "stream-fraction",
            "model",
            "max-tokens",
            "idle-timeout",
            "grace",
            "ground-truth",
        ],
        &["json", "claude-code-shape"],
    )?;
    let url = |name: &str, default: &str| {
        flags
            .get_with(name, None, |text| {
                text.parse::<BaseUrl>().map(Some).map_err(|e| e.to_string())
            })
            .and_then(|url| match url {
                Some(url) => Ok(url),
                None => default
                    .parse()
                    .map_err(|e: crate::http::UrlError| UsageError::Value {
                        name: format!("--{name}"),
                        reason: e.to_string(),
                    }),
            })
    };
    let d = SwarmConfig::new(
        url("gateway", "http://127.0.0.1:8080/anthropic")?,
        url("wiki", "http://127.0.0.1:8090")?,
    );
    let write: Fraction = flags.get("write-fraction", d.mix.write())?;
    let read: Fraction = flags.get("read-fraction", d.mix.read())?;
    let mix = TaskMix::new(write, read).map_err(|e| UsageError::Value {
        name: "--write-fraction/--read-fraction".to_owned(),
        reason: e.to_string(),
    })?;
    let model: String = flags.get("model", d.model.clone())?;
    if model.is_empty() {
        return Err(UsageError::Value {
            name: "--model".to_owned(),
            reason: "empty".to_owned(),
        });
    }
    let config = SwarmConfig {
        agents: flags.get("agents", d.agents)?,
        agents_per_key: flags.get("agents-per-key", d.agents_per_key)?,
        think_ms: flags.get("think-ms", d.think_ms)?,
        turns: flags.get("turns", d.turns)?,
        mix,
        pages: flags.get("pages", d.pages)?,
        topics: flags.get("topics", d.topics)?,
        duration: flags.duration("duration", d.duration)?,
        ramp: flags.duration("ramp", d.ramp)?,
        seed: flags.get("seed", d.seed)?,
        claude_code_shape: flags.switch("claude-code-shape"),
        stream_fraction: flags.get("stream-fraction", d.stream_fraction)?,
        model,
        max_tokens: flags.get("max-tokens", d.max_tokens)?,
        idle_timeout: flags.duration("idle-timeout", d.idle_timeout)?,
        grace: flags.duration("grace", d.grace)?,
        ground_truth: flags.get_with("ground-truth", None, |text| Ok(Some(PathBuf::from(text))))?,
        ..d
    };
    Ok(Command::Swarm {
        config: Box::new(config),
        json: flags.switch("json"),
    })
}
