//! The Anthropic Messages corpus: `corpus/anthropic/messages/`.
//!
//! [`cases`] loads every case and checks it against its `meta.json`, so a
//! case that loads is internally consistent: its credential placeholder and
//! claim headers are in the request, the body's model and stream flag match
//! the metadata, the response is framed the way its status and the request
//! say, and its content blocks, stop reason, response id or error agree with
//! the expected outcome.

use std::path::{Path, PathBuf};

use crosstalk_spec::interfaces::l0_ingress::ResponseFraming;
use crosstalk_spec::observed::exchange::{ExchangeFailure, StopReason};
use serde_json::Value;

use crate::corpus::http::{self, HttpFileError};
use crate::corpus::meta::{BlockKind, CaseMeta, Endpoint, Expect};
use crate::corpus::sse::EventStream;
use crate::corpus::{Case, ResponseBody, root};

/// The directory holding the Anthropic Messages cases.
pub fn dir() -> PathBuf {
    root().join("anthropic").join("messages")
}

/// Why the corpus could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum CorpusError {
    #[error("listing {path}: {source}")]
    List {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    File(#[from] HttpFileError),
    #[error("reading {path}: {source}")]
    MetaIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Meta {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("case {case}: {problem}")]
    Inconsistent {
        case: String,
        problem: Inconsistency,
    },
    #[error("no case named {0}")]
    NoSuchCase(String),
}

/// How a case disagrees with its own metadata.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Inconsistency {
    #[error("the request has no {header} header holding the credential placeholder")]
    MissingCredential { header: String },
    #[error("the case declares no credential but the request sends {header}")]
    UndeclaredCredential { header: String },
    #[error("claim header {header} is not in the request")]
    MissingClaimHeader { header: String },
    #[error("harness id {field} is {expected:?} in meta.json but {found:?} in the headers")]
    HarnessId {
        field: &'static str,
        expected: Option<String>,
        found: Option<String>,
    },
    #[error("the request body is not a JSON object")]
    RequestNotJson,
    #[error("the body's model is {found:?}, meta.json says {expected:?}")]
    Model {
        expected: String,
        found: Option<String>,
    },
    #[error("the body's stream flag is {found}, meta.json says {expected}")]
    Stream { expected: bool, found: bool },
    #[error("the response is framed {found:?} but should be {expected:?}")]
    Framing {
        expected: ResponseFraming,
        found: ResponseFraming,
    },
    #[error("the response status is {status}, which does not fit the expected outcome")]
    Status { status: u16 },
    #[error("the response body is not the JSON its outcome needs")]
    ResponseNotJson,
    #[error("the event stream does not end with {expected}")]
    Terminal { expected: &'static str },
    #[error("an event's data is not JSON: {event}")]
    EventNotJson { event: String },
    #[error("response blocks are {found:?}, meta.json says {expected:?}")]
    Blocks {
        expected: Vec<BlockKind>,
        found: Vec<String>,
    },
    #[error("stop reason is {found:?}, meta.json says {expected:?}")]
    Stop {
        expected: StopReason,
        found: Option<String>,
    },
    #[error("response id is {found:?}, meta.json says {expected:?}")]
    ResponseId {
        expected: String,
        found: Option<String>,
    },
    #[error("follows names {0}, which is not a case")]
    Follows(String),
}

/// Every case, sorted by name, each checked against its metadata.
pub fn cases() -> Result<Vec<Case>, CorpusError> {
    load_all(&dir())
}

/// The case named `name`.
pub fn case(name: &str) -> Result<Case, CorpusError> {
    cases()?
        .into_iter()
        .find(|case| case.name == name)
        .ok_or_else(|| CorpusError::NoSuchCase(name.to_owned()))
}

/// Every case under `dir`, sorted by name, checked.
pub fn load_all(dir: &Path) -> Result<Vec<Case>, CorpusError> {
    let list = |source| CorpusError::List {
        path: dir.to_owned(),
        source,
    };
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(list)? {
        let path = entry.map_err(list)?.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    dirs.sort();
    let cases = dirs
        .iter()
        .map(|path| load(path))
        .collect::<Result<Vec<_>, _>>()?;
    for case in &cases {
        if let Some(follows) = &case.meta.follows
            && cases.iter().all(|other| &other.name != follows)
        {
            return Err(CorpusError::Inconsistent {
                case: case.name.clone(),
                problem: Inconsistency::Follows(follows.clone()),
            });
        }
    }
    Ok(cases)
}

/// The case in `dir`, checked against its metadata.
pub fn load(dir: &Path) -> Result<Case, CorpusError> {
    let name = dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let request = http::read_request(&dir.join("request.http"))?;
    let response = http::read_response(&dir.join("response.http"))?;
    let meta_path = dir.join("meta.json");
    let text = std::fs::read(&meta_path).map_err(|source| CorpusError::MetaIo {
        path: meta_path.clone(),
        source,
    })?;
    let meta: CaseMeta = serde_json::from_slice(&text).map_err(|source| CorpusError::Meta {
        path: meta_path,
        source,
    })?;
    let case = Case {
        name,
        dir: dir.to_owned(),
        meta,
        request,
        response,
    };
    check(&case).map_err(|problem| CorpusError::Inconsistent {
        case: case.name.clone(),
        problem,
    })?;
    Ok(case)
}

/// Check a case against its metadata.
pub fn check(case: &Case) -> Result<(), Inconsistency> {
    check_client(case)?;
    match &case.meta.endpoint {
        Endpoint::Generation {
            model,
            stream,
            expect,
        } => {
            let body = case
                .request
                .json()
                .ok()
                .filter(Value::is_object)
                .ok_or(Inconsistency::RequestNotJson)?;
            let found = body.get("model").and_then(Value::as_str);
            if found != Some(model.0.as_str()) {
                return Err(Inconsistency::Model {
                    expected: model.0.clone(),
                    found: found.map(str::to_owned),
                });
            }
            let found = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
            if found != *stream {
                return Err(Inconsistency::Stream {
                    expected: *stream,
                    found,
                });
            }
            let success = case.response.status.is_success();
            let framing = if *stream && success {
                ResponseFraming::EventStream
            } else {
                ResponseFraming::Whole
            };
            check_framing(case, framing)?;
            check_outcome(case, expect)
        }
        Endpoint::TokenCount | Endpoint::ModelList => {
            check_framing(case, ResponseFraming::Whole)?;
            match case.response.json() {
                Some(Ok(value)) if value.is_object() => Ok(()),
                _ => Err(Inconsistency::ResponseNotJson),
            }
        }
        Endpoint::Probe => check_framing(case, ResponseFraming::Whole),
    }
}

/// The credential placeholder, claim headers and harness ids.
fn check_client(case: &Case) -> Result<(), Inconsistency> {
    let headers = &case.request.headers;
    match &case.meta.credential {
        Some(credential) => {
            let holds = headers
                .get_str(&credential.header)
                .is_some_and(|value| value.contains(&credential.placeholder));
            if !holds {
                return Err(Inconsistency::MissingCredential {
                    header: credential.header.clone(),
                });
            }
        }
        None => {
            if let Some(header) = ["x-api-key", "authorization"]
                .into_iter()
                .find(|header| headers.get(header).is_some())
            {
                return Err(Inconsistency::UndeclaredCredential {
                    header: header.to_owned(),
                });
            }
        }
    }
    let harness = &case.meta.harness;
    if let Some(header) = harness
        .claim_headers
        .iter()
        .find(|header| headers.get(header).is_none())
    {
        return Err(Inconsistency::MissingClaimHeader {
            header: header.clone(),
        });
    }
    let ids = [
        ("session", &harness.ids.session, "x-claude-code-session-id"),
        ("agent", &harness.ids.agent, "x-claude-code-agent-id"),
        (
            "parent_agent",
            &harness.ids.parent_agent,
            "x-claude-code-parent-agent-id",
        ),
    ];
    for (field, expected, header) in ids {
        let found = headers.get_str(header).map(str::to_owned);
        if &found != expected {
            return Err(Inconsistency::HarnessId {
                field,
                expected: expected.clone(),
                found,
            });
        }
    }
    Ok(())
}

fn check_framing(case: &Case, expected: ResponseFraming) -> Result<(), Inconsistency> {
    let found = case.response.framing();
    if found == expected {
        Ok(())
    } else {
        Err(Inconsistency::Framing { expected, found })
    }
}

/// What a response's content says: block types in order, stop reason and
/// message id.
struct Content {
    blocks: Vec<String>,
    stop: Option<String>,
    id: Option<String>,
    /// The stream ended with an `error` event.
    error_event: bool,
    /// The stream ended with `message_stop`.
    stopped: bool,
}

fn stream_content(stream: &EventStream) -> Result<Content, Inconsistency> {
    let mut content = Content {
        blocks: Vec::new(),
        stop: None,
        id: None,
        error_event: false,
        stopped: false,
    };
    for event in stream.dispatched() {
        let data = event.json().map_err(|_| Inconsistency::EventNotJson {
            event: event.kind().to_owned(),
        })?;
        let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
        match event.kind() {
            "message_start" => content.id = text(data.pointer("/message/id")),
            "content_block_start" => {
                content
                    .blocks
                    .extend(text(data.pointer("/content_block/type")));
            }
            "message_delta" => {
                if let Some(stop) = text(data.pointer("/delta/stop_reason")) {
                    content.stop = Some(stop);
                }
            }
            _ => {}
        }
    }
    let last = stream.dispatched().last().map(|event| event.kind());
    content.error_event = last == Some("error");
    content.stopped = last == Some("message_stop");
    Ok(content)
}

fn whole_content(body: &Value) -> Content {
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    Content {
        blocks: body
            .get("content")
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|block| text(block.get("type")))
                    .collect()
            })
            .unwrap_or_default(),
        stop: text(body.get("stop_reason")),
        id: text(body.get("id")),
        error_event: false,
        stopped: true,
    }
}

fn block_names(blocks: &[BlockKind]) -> Vec<String> {
    blocks.iter().map(wire_name).collect()
}

/// The snake_case wire name of a unit enum value.
fn wire_name<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn check_outcome(case: &Case, expect: &Expect) -> Result<(), Inconsistency> {
    let response = &case.response;
    let content = match &response.body {
        ResponseBody::EventStream(stream) => stream_content(stream)?,
        ResponseBody::Whole(_) => match response.json() {
            Some(Ok(body)) if body.is_object() => whole_content(&body),
            _ => return Err(Inconsistency::ResponseNotJson),
        },
    };
    match expect {
        Expect::Completed {
            stop,
            response_id,
            blocks,
        } => {
            if !response.status.is_success() {
                return Err(Inconsistency::Status {
                    status: response.status.as_u16(),
                });
            }
            if !content.stopped {
                return Err(Inconsistency::Terminal {
                    expected: "message_stop",
                });
            }
            if content.blocks != block_names(blocks) {
                return Err(Inconsistency::Blocks {
                    expected: blocks.clone(),
                    found: content.blocks,
                });
            }
            if content.stop.as_deref() != Some(wire_name(stop).as_str()) {
                return Err(Inconsistency::Stop {
                    expected: *stop,
                    found: content.stop,
                });
            }
            if content.id.as_deref() != Some(response_id.0.as_str()) {
                return Err(Inconsistency::ResponseId {
                    expected: response_id.0.clone(),
                    found: content.id,
                });
            }
            Ok(())
        }
        Expect::Failed {
            failure,
            partial_blocks,
        } => {
            match failure {
                ExchangeFailure::Upstream { status } => {
                    if response.status.as_u16() != *status || response.status.is_success() {
                        return Err(Inconsistency::Status {
                            status: response.status.as_u16(),
                        });
                    }
                    let is_error = response.json().is_some_and(|body| {
                        body.is_ok_and(|body| body.get("type") == Some(&Value::from("error")))
                    });
                    if !is_error {
                        return Err(Inconsistency::ResponseNotJson);
                    }
                }
                ExchangeFailure::UpstreamErrorEvent => {
                    if !content.error_event {
                        return Err(Inconsistency::Terminal { expected: "error" });
                    }
                }
                ExchangeFailure::UpstreamUnreachable
                | ExchangeFailure::StreamTruncated
                | ExchangeFailure::MalformedStream { .. }
                | ExchangeFailure::UnparseableResponse
                | ExchangeFailure::ClientDisconnected
                | ExchangeFailure::Timeout => {}
            }
            if content.blocks != block_names(partial_blocks) {
                return Err(Inconsistency::Blocks {
                    expected: partial_blocks.clone(),
                    found: content.blocks,
                });
            }
            Ok(())
        }
    }
}
