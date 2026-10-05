//! The extraction step over hand-built deltas: which tool results pair with
//! which calls (`flow.extract.result-pairs-with-history-call`), replayed
//! results (`flow.extract.replayed-result-read-once`) and the configured
//! extractors.

use crosstalk_flow::consumer::Extracted;
use crosstalk_flow::extract::{ExtractConfig, WriteOutcome};
use crosstalk_provenance::store::MemoryProvenanceStore;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Message, MessageBody, SystemPart, Text, ToolArguments, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, encoding,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};
use crosstalk_transport::blob::MemoryBlobStore;
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use super::Extraction;
use crate::live::blobs::LiveBlobs;

const AGENT: AgentId = AgentId::from_ulid(1);
const PAGE: &str = "https://www.prowiki.org/dse/RelayIndexAlpha";

struct Step {
    blobs: LiveBlobs,
    extraction: Extraction,
    extracted: UnboundedReceiver<Extracted>,
}

impl Step {
    fn new(config: ExtractConfig) -> Self {
        let blobs = LiveBlobs::Memory(MemoryBlobStore::new());
        let (flow, extracted) = mpsc::unbounded_channel();
        Self {
            extraction: Extraction::new(blobs.clone(), MemoryProvenanceStore::new(), config, flow),
            blobs,
            extracted,
        }
    }

    async fn put(&self, body: MessageBody) -> MessageHash {
        let message = Message::new(body);
        let stored = self
            .blobs
            .put(&encoding::encode(&message.body))
            .await
            .unwrap_or_else(|error| panic!("put: {error:?}"));
        assert_eq!(stored, message.hash);
        message.hash
    }

    /// Run one delta of exchange `exchange` in `conversation`, with the
    /// system prompt `system`, the new inputs `inputs` and the output
    /// `output`; what it handed the flow consumer.
    async fn delta(
        &mut self,
        exchange: u128,
        conversation: u128,
        inputs: Vec<MessageBody>,
        output: Option<MessageBody>,
    ) -> Vec<Extracted> {
        let system = self
            .put(MessageBody::System(vec![SystemPart::Text(Text(
                "You are a wiki agent.".to_owned(),
            ))]))
            .await;
        let mut new_inputs = Vec::new();
        for input in inputs {
            new_inputs.push(self.put(input).await);
        }
        let output = match output {
            Some(body) => Some(self.put(body).await),
            None => None,
        };
        let delta = ConversationDelta {
            exchange: ExchangeId::from_ulid(exchange),
            agent: AGENT,
            conversation: ConversationId::from_ulid(conversation),
            new_inputs,
            new_system: Some(system),
            output,
        };
        let at = Timestamp::from_micros(u64::try_from(exchange).unwrap_or(0) * 1_000_000);
        self.extraction
            .delta(&delta, at)
            .await
            .unwrap_or_else(|error| panic!("delta: {error}"));
        let mut out = Vec::new();
        while let Ok(input) = self.extracted.try_recv() {
            out.push(input);
        }
        out
    }
}

fn tool_call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: ToolCallId(id.to_owned()),
        name: ToolName(name.to_owned()),
        arguments: ToolArguments::Json(CanonicalJson(arguments.to_string())),
        execution: ToolExecution::Client,
        signature: None,
    }
}

fn get(id: &str) -> ToolCall {
    tool_call(id, "http_request", json!({ "method": "GET", "url": PAGE }))
}

fn post(id: &str) -> ToolCall {
    tool_call(
        id,
        "http_request",
        json!({ "method": "POST", "url": PAGE, "body": "the relay index" }),
    )
}

fn calls(calls: Vec<ToolCall>) -> MessageBody {
    MessageBody::Assistant(calls.into_iter().map(AssistantPart::ToolCall).collect())
}

fn result(id: &str, text: &str) -> MessageBody {
    MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(id.to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    }))
}

fn page() -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host("www.prowiki.org".to_owned()),
        path: "/dse/RelayIndexAlpha".to_owned(),
        query: None,
    }
}

/// The reads in `out`, as (exchange, locator).
fn reads(out: &[Extracted]) -> Vec<(ExchangeId, Locator)> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::Read(read) => Some((read.exchange, read.locator.clone())),
            _ => None,
        })
        .collect()
}

/// The held writes in `out`, by access id.
fn held_writes(out: &[Extracted]) -> Vec<crosstalk_spec::ids::AccessId> {
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

fn write_results(out: &[Extracted]) -> Vec<(crosstalk_spec::ids::AccessId, WriteOutcome)> {
    out.iter()
        .filter_map(|input| match input {
            Extracted::WriteResult { access, outcome } => Some((*access, *outcome)),
            _ => None,
        })
        .collect()
}

/// The wiki converter's shape: a new conversation whose request carries
/// the GET and its result.
#[tokio::test]
async fn a_result_pairs_with_its_call_in_the_same_request() {
    let mut step = Step::new(ExtractConfig::default());
    let out = step
        .delta(
            10,
            100,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            Some(MessageBody::Assistant(vec![AssistantPart::Text(Text(
                "Read it.".to_owned(),
            ))])),
        )
        .await;
    assert_eq!(reads(&out), vec![(ExchangeId::from_ulid(10), page())]);
}

/// A call carried by an earlier delta's inputs of the same conversation
/// (a compacted history) pairs with a later result.
#[tokio::test]
async fn a_result_pairs_with_a_history_call_of_an_earlier_delta() {
    let mut step = Step::new(ExtractConfig::default());
    let first = step
        .delta(10, 100, vec![calls(vec![get("call_1")])], None)
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(11, 100, vec![result("call_1", "the relay index")], None)
        .await;
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
}

/// The call in an output, its result in the next delta: as before.
#[tokio::test]
async fn a_result_pairs_with_the_call_of_an_earlier_output() {
    let mut step = Step::new(ExtractConfig::default());
    let first = step
        .delta(10, 100, vec![], Some(calls(vec![get("call_1")])))
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(11, 100, vec![result("call_1", "the relay index")], None)
        .await;
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
}

/// A POST made in one conversation's output, whose result arrives in a new
/// conversation's request together with the same call: the held write is
/// released with its outcome.
#[tokio::test]
async fn a_held_write_is_released_by_its_result_in_another_conversation() {
    let mut step = Step::new(ExtractConfig::default());
    let first = step
        .delta(10, 100, vec![], Some(calls(vec![post("call_1")])))
        .await;
    let held = held_writes(&first);
    assert_eq!(held.len(), 1);
    let second = step
        .delta(
            11,
            200,
            vec![calls(vec![post("call_1")]), result("call_1", "saved")],
            None,
        )
        .await;
    assert_eq!(
        write_results(&second),
        vec![(held[0], WriteOutcome::Delivered)]
    );
    // Not extracted again as a fresh write.
    assert!(held_writes(&second).is_empty());
}

/// Another call that reuses the id in another conversation (different
/// arguments) does not take the held write; its own reads are extracted.
#[tokio::test]
async fn a_reused_call_id_does_not_release_another_calls_write() {
    let mut step = Step::new(ExtractConfig::default());
    let first = step
        .delta(10, 100, vec![], Some(calls(vec![post("call_1")])))
        .await;
    assert_eq!(held_writes(&first).len(), 1);
    let second = step
        .delta(
            11,
            200,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            None,
        )
        .await;
    assert!(write_results(&second).is_empty());
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
    // The held write still takes its own result in its conversation.
    let third = step
        .delta(12, 100, vec![result("call_1", "saved")], None)
        .await;
    assert_eq!(
        write_results(&third),
        vec![(held_writes(&first)[0], WriteOutcome::Delivered)]
    );
}

/// A new conversation that replays a transcript the agent already received
/// reads nothing again.
#[tokio::test]
async fn a_replayed_result_is_read_once() {
    let mut step = Step::new(ExtractConfig::default());
    let first = step
        .delta(10, 100, vec![], Some(calls(vec![get("call_1")])))
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(11, 100, vec![result("call_1", "the relay index")], None)
        .await;
    assert_eq!(reads(&second).len(), 1);
    let replay = step
        .delta(
            12,
            200,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            None,
        )
        .await;
    assert!(reads(&replay).is_empty());
    // Replayed twice in one request, or again later: still once.
    let again = step
        .delta(
            13,
            300,
            vec![
                calls(vec![get("call_2")]),
                result("call_2", "v2"),
                calls(vec![get("call_2")]),
                result("call_2", "v2"),
            ],
            None,
        )
        .await;
    assert_eq!(reads(&again).len(), 1);
    // The same call with another result is a new delivery.
    let changed = step
        .delta(
            14,
            400,
            vec![calls(vec![get("call_1")]), result("call_1", "edited")],
            None,
        )
        .await;
    assert_eq!(reads(&changed), vec![(ExchangeId::from_ulid(14), page())]);
}

/// A result whose call the step never saw is dropped.
#[tokio::test]
async fn a_result_without_a_call_is_dropped() {
    let mut step = Step::new(ExtractConfig::default());
    let out = step
        .delta(10, 100, vec![result("call_1", "the relay index")], None)
        .await;
    assert!(out.is_empty());
}

/// The step extracts with its configuration: a fetch tool configured by
/// name reads its URL.
#[tokio::test]
async fn the_configured_fetch_tools_are_extracted() {
    let get_webpage = |id: &str| tool_call(id, "get_webpage", json!({ "url": PAGE }));
    let mut step = Step::new(ExtractConfig::default());
    let out = step
        .delta(
            10,
            100,
            vec![calls(vec![get_webpage("call_1")]), result("call_1", "page")],
            None,
        )
        .await;
    assert!(reads(&out).is_empty());
    let config = ExtractConfig::from_json(r#"{"fetch_tools": ["get_webpage"]}"#)
        .unwrap_or_else(|error| panic!("config: {error}"));
    let mut step = Step::new(config);
    let out = step
        .delta(
            10,
            100,
            vec![calls(vec![get_webpage("call_1")]), result("call_1", "page")],
            None,
        )
        .await;
    assert_eq!(reads(&out), vec![(ExchangeId::from_ulid(10), page())]);
}
