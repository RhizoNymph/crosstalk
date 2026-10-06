//! Generated delta sequences over a small vocabulary of agents,
//! conversations, call ids, tools and result texts, so calls, results,
//! history, replays and redeliveries meet often.

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::observed::message::{MessageBody, ToolCall};
use proptest::prelude::*;

use super::support::{Delta, bash, calls, get, post, result};

const IDS: [&str; 3] = ["c1", "c2", "c3"];
const TEXTS: [&str; 4] = ["the relay index", "saved", "", "Cloning into '/w/atlas'..."];
const COMMANDS: [&str; 4] = [
    "git clone https://gitlab.com/ai-village-agents/village/atlas.git /w/atlas && cd /w/atlas",
    "echo done >> NOTES.md",
    "cd /w && cat notes.txt",
    "git push",
];

/// One call of the vocabulary.
#[derive(Debug, Clone, Copy)]
pub(super) enum Call {
    Get(usize),
    Post(usize),
    Bash(usize, usize),
}

impl Call {
    fn tool_call(self) -> ToolCall {
        match self {
            Self::Get(id) => get(IDS[id]),
            Self::Post(id) => post(IDS[id]),
            Self::Bash(id, command) => bash(IDS[id], COMMANDS[command]),
        }
    }
}

/// One new input of the vocabulary.
#[derive(Debug, Clone)]
pub(super) enum Input {
    Calls(Vec<Call>),
    Result(usize, usize),
}

impl Input {
    fn body(&self) -> MessageBody {
        match self {
            Self::Calls(list) => calls(list.iter().map(|call| call.tool_call()).collect()),
            Self::Result(id, text) => result(IDS[*id], TEXTS[*text]),
        }
    }
}

/// One generated delta; `again` runs it a second time right after
/// (a redelivery).
#[derive(Debug, Clone)]
pub(super) struct Step {
    agent: u128,
    conversation: u128,
    system: bool,
    inputs: Vec<Input>,
    output: Option<Vec<Call>>,
    pub(super) again: bool,
}

impl Step {
    /// The delta, as exchange `exchange`.
    pub(super) fn delta(&self, exchange: u128) -> Delta {
        Delta {
            agent: AgentId::from_ulid(self.agent),
            exchange,
            conversation: self.conversation,
            system: self.system,
            inputs: self.inputs.iter().map(Input::body).collect(),
            output: self
                .output
                .as_ref()
                .map(|list| calls(list.iter().map(|call| call.tool_call()).collect())),
        }
    }
}

fn call() -> impl Strategy<Value = Call> {
    prop_oneof![
        (0..IDS.len()).prop_map(Call::Get),
        (0..IDS.len()).prop_map(Call::Post),
        (0..IDS.len(), 0..COMMANDS.len()).prop_map(|(id, command)| Call::Bash(id, command)),
    ]
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        prop::collection::vec(call(), 1..3).prop_map(Input::Calls),
        (0..IDS.len(), 0..TEXTS.len()).prop_map(|(id, text)| Input::Result(id, text)),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    (
        1u128..3,
        prop::sample::select(vec![100u128, 200, 300]),
        any::<bool>(),
        prop::collection::vec(input(), 0..4),
        prop::option::of(prop::collection::vec(call(), 1..3)),
        prop::bool::weighted(0.1),
    )
        .prop_map(
            |(agent, conversation, system, inputs, output, again)| Step {
                agent,
                conversation,
                system,
                inputs,
                output,
                again,
            },
        )
}

/// A sequence of up to `max` deltas.
pub(super) fn script(max: usize) -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step(), 1..max)
}
