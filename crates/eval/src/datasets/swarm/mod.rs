//! swarm-traces: a decoder test corpus.
//!
//! The export (`redacted.jsonl.gz`) records encoded payloads agents passed
//! each other and, for many, a recovered-text child. **The payloads are real
//! attack content and are treated purely as text: never executed, and no URL
//! in them is ever fetched.** No dataset bytes live in the repository; the
//! fixtures are synthetic strings that mimic the encoding structure only.
//!
//! The converter extracts encoded tokens from each payload ([`codec`]) and,
//! for every token it can decode to printable text, builds a two-agent
//! [`World`]:
//!
//! - the **author** originates the decoded plaintext in its output;
//! - the **reader** receives the encoded token in a tool result.
//!
//! The expected transmission is a Decoded-class edge author → reader,
//! labelled with the codec chain the converter verified by actually decoding
//! the token. The reference matcher decodes one base64/hex/URL layer, so a
//! single-layer token is found and a nested chain is a reported miss — the
//! point of the corpus. A token is tiered Construction when the payload has a
//! recovered-text child (the recovery is corroborated), else Structural.

pub mod codec;

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::Path;

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
};
use crosstalk_spec::support::NonEmpty;
use flate2::read::GzDecoder;
use serde::Deserialize;

use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, SourceError,
    TraceSource, World, WorldBuilder,
};
use crate::keys::{DatasetId, SourceRef, WorldKey};
use crate::location::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, RouteExpectation,
    Tier, TransmissionLabel,
};
use codec::decode;

/// The dataset's id.
pub const DATASET: &str = "swarm-traces";
const PAYLOADS_FILE: &str = "redacted.jsonl";
const MODEL: &str = "swarm/agent";
/// The shortest decoded plaintext worth a label (the reference matcher's
/// span floor).
const MIN_PLAINTEXT: usize = 24;
const MIN_WORD_CHARS: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("{root} has no {file}")]
    Missing { root: String, file: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} line {line} is not a swarm record: {source}")]
    Json {
        path: String,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("virtual clock: {0}")]
    Clock(#[source] crate::corpus::clock::ClockError),
    #[error("tool-call arguments are not valid JSON: {0}")]
    Arguments(#[source] crosstalk_spec::observed::message::json::JsonError),
    #[error("location: {0}")]
    Location(#[from] crate::location::LocationError),
    #[error("label: {0}")]
    Label(#[from] crate::truth::InvalidLabel),
    #[error("token {0:?} no longer decodes")]
    Undecodable(String),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
}

/// One row of the redacted export.
#[derive(Debug, Clone, Deserialize)]
pub struct Row {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub text: String,
}

/// Which worlds a run reads.
#[derive(Debug, Clone, Default)]
pub struct SwarmSelection {
    /// Emit at most this many worlds.
    pub limit: Option<usize>,
}

/// One planned world: one token of one payload.
#[derive(Debug, Clone)]
struct TokenWorld {
    payload_id: String,
    token_index: usize,
    token: String,
    corroborated: bool,
}

/// swarm-traces as a stream of worlds, one per decodable token.
pub struct SwarmSource {
    worlds: Vec<TokenWorld>,
}

impl SwarmSource {
    /// Reads the export under `root` and plans one world per decodable token.
    pub fn open(root: &Path, selection: &SwarmSelection) -> Result<Self, SwarmError> {
        let rows = read_rows(root)?;
        // Which payloads have a recovered-text or response child.
        let mut corroborated: BTreeSet<&str> = BTreeSet::new();
        for row in &rows {
            if matches!(row.kind.as_str(), "recovered_text" | "response")
                && let Some(parent) = &row.parent_id
            {
                corroborated.insert(parent.as_str());
            }
        }
        let mut worlds = Vec::new();
        for row in &rows {
            if row.kind != "payload" {
                continue;
            }
            let mut seen = BTreeSet::new();
            for (index, token) in tokens(&row.text).into_iter().enumerate() {
                if !seen.insert(token.clone()) {
                    continue;
                }
                let Some(decoded) = decode(&token) else {
                    continue;
                };
                if decoded.codecs().is_none()
                    || decoded.text.len() < MIN_PLAINTEXT
                    || word_chars(&decoded.text) < MIN_WORD_CHARS
                {
                    continue;
                }
                worlds.push(TokenWorld {
                    payload_id: row.id.clone(),
                    token_index: index,
                    token,
                    corroborated: corroborated.contains(row.id.as_str()),
                });
                if selection.limit.is_some_and(|limit| worlds.len() >= limit) {
                    return Ok(Self { worlds });
                }
            }
        }
        Ok(Self { worlds })
    }

    pub fn world_count(&self) -> usize {
        self.worlds.len()
    }
}

impl TraceSource for SwarmSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        self.worlds
            .iter()
            .map(|plan| build_world(plan).map_err(|error| SourceError::from(Box::new(error))))
    }
}

/// Builds the two-agent world for one decodable token.
fn build_world(plan: &TokenWorld) -> Result<World, SwarmError> {
    let dataset = DatasetId::new(DATASET);
    let key = WorldKey::new(format!("{}#{}", plan.payload_id, plan.token_index));
    let mut builder = WorldBuilder::new(dataset, key.clone());
    let author = builder.agent("author", Driven::Model, MODEL)?;
    let reader = builder.agent("reader", Driven::Model, MODEL)?;

    let decoded = decode(&plan.token).ok_or_else(|| SwarmError::Undecodable(plan.token.clone()))?;
    let codecs = decoded.codecs().unwrap_or_default();
    let plaintext = decoded.text;

    // The author originates the decoded plaintext.
    let author_at = crate::corpus::clock::ordinal(0).map_err(SwarmError::Clock)?;
    builder.exchange(ExchangeDraft {
        agent: author.clone(),
        at: author_at,
        protocol: WireProtocol::OpenAiChat,
        model: MODEL.to_owned(),
        request: vec![system("You are a swarm agent.")],
        response: assistant_text(&plaintext),
        stop: StopReason::EndTurn,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: SourceRef::new(PAYLOADS_FILE, format!("/row/{}/plaintext", plan.payload_id)),
    })?;

    // The reader receives the encoded token in a tool result.
    let call_id = format!("fetch-{}", plan.payload_id);
    let call = assistant_call(&call_id, "fetch_drop", &serde_json::json!({}))?;
    let result = tool_result(&call_id, &plan.token);
    let result_hash = result.hash();
    let reader_at = crate::corpus::clock::ordinal(1).map_err(SwarmError::Clock)?;
    let reader_exchange = builder.exchange(ExchangeDraft {
        agent: reader.clone(),
        at: reader_at,
        protocol: WireProtocol::OpenAiChat,
        model: MODEL.to_owned(),
        request: vec![system("You are a swarm agent."), call, result],
        response: assistant_text("Fetched the drop."),
        stop: StopReason::EndTurn,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: SourceRef::new(
            PAYLOADS_FILE,
            format!("/row/{}/token/{}", plan.payload_id, plan.token_index),
        ),
    })?;

    let end = u32::try_from(plan.token.len()).map_err(|_| {
        SwarmError::Location(crate::location::LocationError::Empty { start: 0, end: 0 })
    })?;
    let at = location(result_hash, 0, 0, end)?;
    let tier = if plan.corroborated {
        Tier::Construction
    } else {
        Tier::Structural
    };
    builder.expect(Expectation::Transmission(ExpectedTransmission::new(
        TransmissionLabel {
            from: author,
            to: reader,
            sender_exchange: None,
            reader_exchange,
            route: RouteExpectation::Direct,
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent {
                text: plan.token.clone(),
                at,
            },
            needs: MatchNeed::Decoded { codecs },
            tier,
            source: SourceRef::new(
                PAYLOADS_FILE,
                format!("/row/{}/token/{}", plan.payload_id, plan.token_index),
            ),
        },
    )?));

    Ok(builder.finish(Coverage::Complete {
        tier: Tier::Structural,
    }))
}

/// Candidate encoded tokens of `text`: base64, hex and URL-encoded runs, the
/// inside of `atob('...')`, and `\x..` escape runs.
fn tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    // atob('…') / atob("…") inner strings.
    let mut rest = text;
    while let Some(at) = rest.find("atob(") {
        let after = &rest[at + 5..];
        if let Some(inner) = quoted(after) {
            out.push(inner);
        }
        rest = &after[1.min(after.len())..];
    }
    out.extend(runs(text, |b| {
        b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'-' | b'_')
    }));
    out.extend(runs(text, |b| b.is_ascii_hexdigit()));
    out.extend(runs(text, |b| !b.is_ascii_whitespace() && b != b'"'));
    out.extend(escape_runs(text));
    out.retain(|token| token.len() >= 16);
    out
}

/// The contents of the first single- or double-quoted string in `text`.
fn quoted(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let quote = *bytes.first()?;
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let end = text[1..].find(char::from(quote))?;
    Some(text[1..1 + end].to_owned())
}

/// Maximal runs of bytes satisfying `keep`, as owned strings.
fn runs(text: &str, keep: impl Fn(u8) -> bool) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = None;
    for (at, &byte) in bytes.iter().enumerate() {
        let inside = byte.is_ascii() && keep(byte);
        match (inside, start) {
            (true, None) => start = Some(at),
            (false, Some(from)) => {
                out.push(text[from..at].to_owned());
                start = None;
            }
            _ => {}
        }
    }
    if let Some(from) = start {
        out.push(text[from..].to_owned());
    }
    out
}

/// Maximal runs made of `\xNN` escapes.
fn escape_runs(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut at = 0;
    while at + 4 <= bytes.len() {
        if &bytes[at..at + 2] == b"\\x"
            && bytes[at + 2].is_ascii_hexdigit()
            && bytes[at + 3].is_ascii_hexdigit()
        {
            let start = at;
            while at + 4 <= bytes.len()
                && &bytes[at..at + 2] == b"\\x"
                && bytes[at + 2].is_ascii_hexdigit()
                && bytes[at + 3].is_ascii_hexdigit()
            {
                at += 4;
            }
            out.push(text[start..at].to_owned());
        } else {
            at += 1;
        }
    }
    out
}

fn read_rows(root: &Path) -> Result<Vec<Row>, SwarmError> {
    let gz = root.join(format!("{PAYLOADS_FILE}.gz"));
    let plain = root.join(PAYLOADS_FILE);
    let bytes = if gz.exists() {
        let raw = fs::read(&gz).map_err(|source| SwarmError::Io {
            path: gz.display().to_string(),
            source,
        })?;
        let mut out = Vec::with_capacity(raw.len() * 8);
        GzDecoder::new(raw.as_slice())
            .read_to_end(&mut out)
            .map_err(|source| SwarmError::Io {
                path: gz.display().to_string(),
                source,
            })?;
        out
    } else if plain.exists() {
        fs::read(&plain).map_err(|source| SwarmError::Io {
            path: plain.display().to_string(),
            source,
        })?
    } else {
        return Err(SwarmError::Missing {
            root: root.display().to_string(),
            file: PAYLOADS_FILE.to_owned(),
        });
    };
    let text = String::from_utf8_lossy(&bytes);
    let mut out = Vec::new();
    for (at, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str(line).map_err(|source| SwarmError::Json {
            path: PAYLOADS_FILE.to_owned(),
            line: at + 1,
            source,
        })?;
        out.push(row);
    }
    Ok(out)
}

fn assistant_call(
    id: &str,
    name: &str,
    args: &serde_json::Value,
) -> Result<HashedMessage, SwarmError> {
    let json = canonicalize(&args.to_string()).map_err(SwarmError::Arguments)?;
    Ok(HashedMessage::new(MessageBody::Assistant(vec![
        AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(id.to_owned()),
            name: ToolName(name.to_owned()),
            arguments: ToolArguments::Json(json),
            execution: ToolExecution::Client,
            signature: None,
        }),
    ])))
}

fn tool_result(call_id: &str, text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(call_id.to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    })))
}

fn system(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::System(vec![SystemPart::Text(Text(
        text.to_owned(),
    ))]))
}

fn assistant_text(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Assistant(vec![AssistantPart::Text(Text(
        text.to_owned(),
    ))]))
}

fn word_chars(text: &str) -> usize {
    text.bytes()
        .filter(|b| b.is_ascii_alphanumeric() || *b >= 0x80)
        .count()
}
