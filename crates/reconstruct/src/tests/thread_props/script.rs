//! Generated harness behaviour: scripts of exchanges a set of harness
//! "lines" (request histories) send, with continuations, retries,
//! branches, rewritten tool results, system prompt changes, mid-array
//! system turns, failures and compactions, over three agents of which two
//! form one cluster.

use crosstalk_spec::ids::{AgentId, MessageHash};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::observed::message::Role;
use crosstalk_testkit::build::ExchangeBuilder;
use proptest::prelude::*;

use super::super::support::{Clusters, Scene};
use crate::thread::Entry;

/// How a continuation changes the system messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SystemChange {
    Same,
    /// Replace the leading system prompt (or add one) with prompt `n`.
    Replace(u8),
    Remove,
    /// Append a `system` turn after the history (mid-conversation).
    Append(u8),
}

/// How an exchange ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ending {
    Completed,
    FailedPartial,
    FailedEmpty,
}

/// What a continuation adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Input {
    Tool,
    User(u8),
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    Start {
        agent: u8,
        system: Option<u8>,
        user: u8,
    },
    Continue {
        line: u8,
        input: Input,
        system: SystemChange,
        ending: Ending,
    },
    Retry {
        line: u8,
        cut: u8,
    },
    Branch {
        line: u8,
        cut: u8,
        user: u8,
    },
    Rewrite {
        line: u8,
        at: u8,
    },
    Compact {
        line: u8,
        keep: u8,
    },
}

fn system_change() -> impl Strategy<Value = SystemChange> {
    prop_oneof![
        6 => Just(SystemChange::Same),
        1 => (0u8..3).prop_map(SystemChange::Replace),
        1 => Just(SystemChange::Remove),
        1 => (0u8..3).prop_map(SystemChange::Append),
    ]
}

fn ending() -> impl Strategy<Value = Ending> {
    prop_oneof![
        8 => Just(Ending::Completed),
        1 => Just(Ending::FailedPartial),
        1 => Just(Ending::FailedEmpty),
    ]
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        4 => Just(Input::Tool),
        2 => (0u8..4).prop_map(Input::User),
        1 => Just(Input::Nothing),
    ]
}

/// How often each kind of step is drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mix {
    pub(crate) start: u32,
    pub(crate) carry_on: u32,
    pub(crate) retry: u32,
    pub(crate) branch: u32,
    pub(crate) rewrite: u32,
    pub(crate) compact: u32,
}

impl Mix {
    /// Mostly continuations, every other kind now and then.
    pub(crate) const GENERAL: Mix = Mix {
        start: 2,
        carry_on: 8,
        retry: 1,
        branch: 2,
        rewrite: 1,
        compact: 1,
    };
    /// Many retries, branches and rewrites: forks.
    pub(crate) const FORKS: Mix = Mix {
        start: 1,
        carry_on: 5,
        retry: 3,
        branch: 4,
        rewrite: 3,
        compact: 1,
    };
    /// Many compactions, chained ones included.
    pub(crate) const COMPACTIONS: Mix = Mix {
        start: 1,
        carry_on: 6,
        retry: 1,
        branch: 1,
        rewrite: 1,
        compact: 4,
    };
}

pub(crate) fn op(mix: Mix) -> impl Strategy<Value = Op> {
    prop_oneof![
        mix.start => (0u8..3, proptest::option::of(0u8..3), 0u8..3)
            .prop_map(|(agent, system, user)| Op::Start { agent, system, user }),
        mix.carry_on => (any::<u8>(), input(), system_change(), ending())
            .prop_map(|(line, input, system, ending)| Op::Continue { line, input, system, ending }),
        mix.retry => (any::<u8>(), any::<u8>()).prop_map(|(line, cut)| Op::Retry { line, cut }),
        mix.branch => (any::<u8>(), any::<u8>(), 0u8..4)
            .prop_map(|(line, cut, user)| Op::Branch { line, cut, user }),
        mix.rewrite => (any::<u8>(), any::<u8>()).prop_map(|(line, at)| Op::Rewrite { line, at }),
        mix.compact => (any::<u8>(), 0u8..4).prop_map(|(line, keep)| Op::Compact { line, keep }),
    ]
}

/// A script: a first start, then up to `max` steps drawn from `mix`.
pub(crate) fn script(mix: Mix, max: usize) -> impl Strategy<Value = Vec<Op>> {
    (
        (0u8..3, proptest::option::of(0u8..3), 0u8..3),
        proptest::collection::vec(op(mix), 1..max),
    )
        .prop_map(|((agent, system, user), mut ops)| {
            ops.insert(0, Op::Start { agent, system, user });
            ops
        })
}

/// One harness line: the history it sends next, and whose it is.
#[derive(Debug, Clone)]
pub(crate) struct Line {
    pub(crate) agent: AgentId,
    pub(crate) history: Vec<Entry>,
}

/// One step's exchange, with what the checks need.
#[derive(Debug, Clone)]
pub(crate) struct Step {
    pub(crate) exchange: Exchange,
    pub(crate) agent: AgentId,
    pub(crate) request: Vec<Entry>,
    pub(crate) output: Option<MessageHash>,
    /// The request's summary turn, when it is a compaction.
    pub(crate) summary: Option<MessageHash>,
}

/// Interprets a script into exchanges, playing the harness.
pub(crate) struct Harness {
    pub(crate) scene: Scene,
    pub(crate) agents: [AgentId; 3],
    pub(crate) lines: Vec<Line>,
    fresh: u64,
}

impl Harness {
    pub(crate) fn new() -> Self {
        let mut scene = Scene::new();
        let agents = [scene.ids.agent(), scene.ids.agent(), scene.ids.agent()];
        Self {
            scene,
            agents,
            lines: Vec::new(),
            fresh: 0,
        }
    }

    /// Agents 0 and 1 form one cluster; agent 2 is alone.
    pub(crate) fn clusters(&self) -> Clusters {
        let mut clusters = Clusters::default();
        clusters.join(&self.agents[..2]);
        clusters
    }

    /// The agents of `agent`'s cluster.
    pub(crate) fn cluster_of(&self, agent: AgentId) -> Vec<AgentId> {
        if self.agents[..2].contains(&agent) {
            self.agents[..2].to_vec()
        } else {
            vec![agent]
        }
    }

    fn next(&mut self) -> u64 {
        self.fresh += 1;
        self.fresh
    }

    async fn entry(&self, role: Role, text: &str) -> Entry {
        let message = match role {
            Role::System => self.scene.system(text).await,
            Role::User => self.scene.user(text).await,
            Role::Assistant => self.scene.assistant(text).await,
            Role::Tool => self.scene.tool("call", text).await,
        };
        Entry { message, role }
    }

    async fn output(&mut self, ending: Ending) -> Option<Entry> {
        let n = self.next();
        match ending {
            Ending::Completed => Some(self.entry(Role::Assistant, &format!("answer {n}")).await),
            Ending::FailedPartial => Some(self.entry(Role::Assistant, &format!("partial {n}")).await),
            Ending::FailedEmpty => None,
        }
    }

    fn line_index(&self, line: u8) -> usize {
        usize::from(line) % self.lines.len().max(1)
    }

    /// The exchange `op` sends, after which the line it continued (or the
    /// new line it opened) holds the request and its output.
    pub(crate) async fn step(&mut self, op: &Op) -> Step {
        let (index, request, ending, summary) = match op {
            Op::Start {
                agent,
                system,
                user,
            } => {
                let mut request = Vec::new();
                if let Some(system) = system {
                    request.push(self.entry(Role::System, &format!("prompt {system}")).await);
                }
                request.push(self.entry(Role::User, &format!("task {user}")).await);
                self.lines.push(Line {
                    agent: self.agents[usize::from(*agent) % 3],
                    history: Vec::new(),
                });
                (self.lines.len() - 1, request, Ending::Completed, None)
            }
            Op::Continue {
                line,
                input,
                system,
                ending,
            } => {
                let index = self.line_index(*line);
                let mut request = self.lines[index].history.clone();
                match system {
                    SystemChange::Same => {}
                    SystemChange::Replace(n) => {
                        let prompt = self.entry(Role::System, &format!("prompt {n}")).await;
                        match request.iter().position(|entry| entry.role == Role::System) {
                            Some(at) => request[at] = prompt,
                            None => request.insert(0, prompt),
                        }
                    }
                    SystemChange::Remove => request.retain(|entry| entry.role != Role::System),
                    SystemChange::Append(n) => {
                        request.push(self.entry(Role::System, &format!("reminder {n}")).await);
                    }
                }
                match input {
                    Input::Tool => {
                        let n = self.next();
                        request.push(self.entry(Role::Tool, &format!("result {n}")).await);
                    }
                    Input::User(n) => {
                        request.push(self.entry(Role::User, &format!("follow-up {n}")).await);
                    }
                    Input::Nothing => {}
                }
                (index, request, *ending, None)
            }
            Op::Retry { line, cut } => {
                let index = self.line_index(*line);
                let history = &self.lines[index].history;
                let keep = 1 + usize::from(*cut) % history.len().max(1);
                let request = history[..keep.min(history.len())].to_vec();
                let agent = self.lines[index].agent;
                self.lines.push(Line {
                    agent,
                    history: Vec::new(),
                });
                (self.lines.len() - 1, request, Ending::Completed, None)
            }
            Op::Branch { line, cut, user } => {
                let index = self.line_index(*line);
                let history = &self.lines[index].history;
                let keep = usize::from(*cut) % (history.len() + 1);
                let mut request = history[..keep].to_vec();
                let n = self.next();
                request.push(self.entry(Role::User, &format!("branch {user} {n}")).await);
                let agent = self.lines[index].agent;
                self.lines.push(Line {
                    agent,
                    history: Vec::new(),
                });
                (self.lines.len() - 1, request, Ending::Completed, None)
            }
            Op::Rewrite { line, at } => {
                let index = self.line_index(*line);
                let mut request = self.lines[index].history.clone();
                if !request.is_empty() {
                    let at = usize::from(*at) % request.len();
                    let n = self.next();
                    request[at] = self.entry(Role::Tool, &format!("rewritten {n}")).await;
                }
                (index, request, Ending::Completed, None)
            }
            Op::Compact { line, keep } => {
                let index = self.line_index(*line);
                let history = self.lines[index].history.clone();
                let mut request: Vec<Entry> = history
                    .iter()
                    .filter(|entry| entry.role == Role::System)
                    .take(1)
                    .copied()
                    .collect();
                let non_system: Vec<Entry> = history
                    .iter()
                    .filter(|entry| entry.role != Role::System)
                    .copied()
                    .collect();
                let carried = usize::from(*keep).min(non_system.len());
                request.extend_from_slice(&non_system[non_system.len() - carried..]);
                let n = self.next();
                let summary = self
                    .entry(
                        Role::User,
                        &format!(
                            "This session is being continued from a previous conversation that ran out of context. Summary {n}."
                        ),
                    )
                    .await;
                request.push(summary);
                let agent = self.lines[index].agent;
                self.lines.push(Line {
                    agent,
                    history: Vec::new(),
                });
                (
                    self.lines.len() - 1,
                    request,
                    Ending::Completed,
                    Some(summary.message),
                )
            }
        };
        let output = self.output(ending).await;
        let mut history = request.clone();
        history.extend(output);
        self.lines[index].history = history;
        let agent = self.lines[index].agent;
        let hashes: Vec<MessageHash> = request.iter().map(|entry| entry.message).collect();
        let at = self.scene.tick();
        let builder = ExchangeBuilder::new(&mut self.scene.ids)
            .started_at(at)
            .request(hashes);
        let exchange = match (ending, output) {
            (Ending::Completed, Some(output)) => builder.response(output.message),
            (_, Some(partial)) => builder.failed_after(
                partial.message,
                crosstalk_spec::observed::exchange::ExchangeFailure::StreamTruncated,
            ),
            (_, None) => {
                builder.failed(crosstalk_spec::observed::exchange::ExchangeFailure::Timeout)
            }
        }
        .build();
        Step {
            exchange,
            agent,
            request,
            output: output.map(|entry| entry.message),
            summary,
        }
    }
}
