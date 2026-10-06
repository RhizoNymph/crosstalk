//! What the step tests share: message bodies and spans in memory, a flow
//! side that records or refuses what it is handed, and a harness that runs
//! hand-built deltas through a step over any ledger.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AccessId, AgentId, ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Message, MessageBody, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use serde_json::{Value, json};

use crate::consumer::{Extracted, FlowInputs, NotDurable};
use crate::extract::step::{
    DeltaOutcome, ExtractStepError, ExtractionLedger, ExtractionStep, MessageReader, PortError,
    SpanReader,
};
use crate::extract::{ExtractConfig, WriteOutcome};

pub(super) const AGENT: AgentId = AgentId::from_ulid(1);
pub(super) const PAGE: &str = "https://www.prowiki.org/dse/RelayIndexAlpha";

/// Message bodies by hash. Clones share them.
#[derive(Debug, Clone, Default)]
pub(super) struct Messages(Arc<Mutex<BTreeMap<MessageHash, Message>>>);

impl Messages {
    pub(super) fn put(&self, body: MessageBody) -> MessageHash {
        let message = Message::new(body);
        let hash = message.hash;
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hash, message);
        hash
    }
}

impl MessageReader for Messages {
    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, PortError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .cloned())
    }
}

/// No spans: these deltas carry no provenance.
#[derive(Debug, Clone, Default)]
pub(super) struct NoSpans;

impl SpanReader for NoSpans {
    async fn exchange_spans(&self, _exchange: ExchangeId) -> Result<Vec<Span>, PortError> {
        Ok(Vec::new())
    }

    async fn span_agent(&self, _span: SpanId) -> Result<Option<AgentId>, PortError> {
        Ok(None)
    }
}

/// A flow side that takes everything and keeps it.
#[derive(Debug, Default)]
pub(super) struct Recorder(pub(super) Vec<Extracted>);

impl FlowInputs for Recorder {
    async fn deliver(&mut self, inputs: Vec<Extracted>) -> Result<(), NotDurable> {
        self.0.extend(inputs);
        Ok(())
    }
}

/// A flow side that confirms nothing.
#[derive(Debug, Default)]
pub(super) struct Refuser;

impl FlowInputs for Refuser {
    async fn deliver(&mut self, _inputs: Vec<Extracted>) -> Result<(), NotDurable> {
        Err(NotDurable::Backlogged)
    }
}

/// A step over `L`, with the bodies it reads.
pub(super) struct Harness<L> {
    pub(super) messages: Messages,
    pub(super) step: ExtractionStep<L, NoSpans, Messages>,
}

/// One hand-built delta.
#[derive(Debug, Clone)]
pub(super) struct Delta {
    pub(super) agent: AgentId,
    pub(super) exchange: u128,
    pub(super) conversation: u128,
    pub(super) system: bool,
    pub(super) inputs: Vec<MessageBody>,
    pub(super) output: Option<MessageBody>,
}

impl Delta {
    /// `AGENT`'s delta with the wiki system prompt.
    pub(super) fn new(
        exchange: u128,
        conversation: u128,
        inputs: Vec<MessageBody>,
        output: Option<MessageBody>,
    ) -> Self {
        Self {
            agent: AGENT,
            exchange,
            conversation,
            system: true,
            inputs,
            output,
        }
    }

    /// `agent`'s delta, without a system prompt.
    pub(super) fn of(
        agent: AgentId,
        exchange: u128,
        conversation: u128,
        inputs: Vec<MessageBody>,
        output: Option<MessageBody>,
    ) -> Self {
        Self {
            agent,
            exchange,
            conversation,
            system: false,
            inputs,
            output,
        }
    }

    pub(super) fn at(&self) -> Timestamp {
        Timestamp::from_micros(u64::try_from(self.exchange).unwrap_or(0) * 1_000_000)
    }
}

impl<L: ExtractionLedger> Harness<L> {
    pub(super) fn new(ledger: L, config: ExtractConfig) -> Self {
        let messages = Messages::default();
        Self {
            step: ExtractionStep::new(ledger, NoSpans, messages.clone(), config),
            messages,
        }
    }

    /// The spec delta `delta` stands for, its bodies stored.
    pub(super) fn spec_delta(&self, delta: &Delta) -> ConversationDelta {
        let new_system = delta.system.then(|| {
            self.messages
                .put(MessageBody::System(vec![SystemPart::Text(Text(
                    "You are a wiki agent.".to_owned(),
                ))]))
        });
        let new_inputs = delta
            .inputs
            .iter()
            .map(|body| self.messages.put(body.clone()))
            .collect();
        let output = delta
            .output
            .as_ref()
            .map(|body| self.messages.put(body.clone()));
        ConversationDelta {
            exchange: ExchangeId::from_ulid(delta.exchange),
            agent: delta.agent,
            conversation: ConversationId::from_ulid(delta.conversation),
            new_inputs,
            new_system,
            output,
        }
    }

    /// Run `delta` into `flow`.
    pub(super) async fn run<F: FlowInputs>(
        &self,
        delta: &Delta,
        flow: &mut F,
    ) -> Result<DeltaOutcome, ExtractStepError> {
        let spec = self.spec_delta(delta);
        self.step.delta(&spec, delta.at(), flow).await
    }

    /// Run `delta`; what it handed the flow consumer.
    pub(super) async fn delta(&self, delta: Delta) -> Vec<Extracted> {
        let mut flow = Recorder::default();
        self.run(&delta, &mut flow)
            .await
            .unwrap_or_else(|error| panic!("delta: {error}"));
        flow.0
    }
}

pub(super) fn tool_call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: ToolCallId(id.to_owned()),
        name: ToolName(name.to_owned()),
        arguments: ToolArguments::Json(CanonicalJson(arguments.to_string())),
        execution: ToolExecution::Client,
        signature: None,
    }
}

pub(super) fn get(id: &str) -> ToolCall {
    tool_call(id, "http_request", json!({ "method": "GET", "url": PAGE }))
}

pub(super) fn post(id: &str) -> ToolCall {
    tool_call(
        id,
        "http_request",
        json!({ "method": "POST", "url": PAGE, "body": "the relay index" }),
    )
}

pub(super) fn bash(id: &str, command: &str) -> ToolCall {
    tool_call(id, "bash", json!({ "command": command }))
}

pub(super) fn calls(calls: Vec<ToolCall>) -> MessageBody {
    MessageBody::Assistant(calls.into_iter().map(AssistantPart::ToolCall).collect())
}

pub(super) fn result(id: &str, text: &str) -> MessageBody {
    MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(id.to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    }))
}

pub(super) fn page() -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host("www.prowiki.org".to_owned()),
        path: "/dse/RelayIndexAlpha".to_owned(),
        query: None,
    }
}

/// The reads in `out`, as (exchange, locator).
pub(super) fn reads(out: &[Extracted]) -> Vec<(ExchangeId, Locator)> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::Read(read) => Some((read.exchange, read.locator.clone())),
            _ => None,
        })
        .collect()
}

/// The held writes in `out`, by access id.
pub(super) fn held_writes(out: &[Extracted]) -> Vec<AccessId> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::Write {
                write,
                outcome: None,
            } => Some(write.id),
            _ => None,
        })
        .collect()
}

pub(super) fn write_results(out: &[Extracted]) -> Vec<(AccessId, WriteOutcome)> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::WriteResult { access, outcome } => Some((*access, *outcome)),
            _ => None,
        })
        .collect()
}

/// The written locators of the held writes in `out`.
pub(super) fn held_locators(out: &[Extracted]) -> Vec<Locator> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::Write {
                write,
                outcome: None,
            } => Some(write.locator.clone()),
            _ => None,
        })
        .collect()
}
