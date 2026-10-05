//! Finding the truth's tool uses in captured exchanges.
//!
//! - **Reader.** A request's tool result for a call id: the `Tool` message
//!   holding it, its position among that message's results (the part
//!   index of a `SpanLocation`), and its text (`Message::part_text`, the
//!   results' text contents joined; one text content for a page read).
//! - **Writer.** A response's `PUT` tool call for a call id, and the page
//!   body inside its JSON arguments (`input.body`).

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody, ToolArguments};

use super::bodies::{Bodies, BodyError, Cached};
use super::schema::HexDigest;
use crate::location::{LocationError, whole_part};

/// A tool result found in a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundResult {
    /// The whole result's text.
    pub at: SpanLocation,
    pub text: String,
}

impl FoundResult {
    pub fn digest(&self) -> HexDigest {
        HexDigest::blake3_of(self.text.as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocateError {
    #[error(transparent)]
    Body(#[from] BodyError),
    #[error("the tool result has no text: {0}")]
    NoText(LocationError),
    #[error("a tool message holds more than {} results", u16::MAX)]
    TooManyParts,
}

/// The tool result for `call_id` in `exchange`'s request; the last one if
/// the request repeats it.
pub fn tool_result<B: Bodies>(
    exchange: &Exchange,
    call_id: &str,
    bodies: &mut Cached<B>,
) -> Result<Option<FoundResult>, LocateError> {
    let mut found = None;
    for hash in &exchange.request {
        let message = bodies.get(*hash)?;
        let MessageBody::Tool(results) = &message.body else {
            continue;
        };
        for (index, result) in results.iter().enumerate() {
            if result.call_id.0 != call_id {
                continue;
            }
            let part = u16::try_from(index).map_err(|_| LocateError::TooManyParts)?;
            let at = whole_part(message, part).map_err(LocateError::NoText)?;
            let text = message
                .part_text(part)
                .map_err(|_| LocateError::NoText(LocationError::NoText { part }))?
                .into_owned();
            found = Some(FoundResult { at, text });
        }
    }
    Ok(found)
}

/// A write tool call found in a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoundCall {
    /// A `PUT` whose arguments carry a string `body`.
    Put { url: String, body: String },
    /// The call exists but is not a `PUT` with a string body.
    NotAPut,
}

/// The tool call `call_id` in `exchange`'s response (its partial response
/// when it failed), if any.
pub fn write_call<B: Bodies>(
    exchange: &Exchange,
    call_id: &str,
    bodies: &mut Cached<B>,
) -> Result<Option<FoundCall>, LocateError> {
    let response = match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => *partial_response,
    };
    let Some(response) = response else {
        return Ok(None);
    };
    let message = bodies.get(response)?;
    let MessageBody::Assistant(parts) = &message.body else {
        return Ok(None);
    };
    for part in parts {
        let AssistantPart::ToolCall(call) = part else {
            continue;
        };
        if call.id.0 != call_id {
            continue;
        }
        let ToolArguments::Json(json) = &call.arguments else {
            return Ok(Some(FoundCall::NotAPut));
        };
        return Ok(Some(put_of(&json.0)));
    }
    Ok(None)
}

fn put_of(arguments: &str) -> FoundCall {
    let Ok(serde_json::Value::Object(members)) = serde_json::from_str(arguments) else {
        return FoundCall::NotAPut;
    };
    let text = |name: &str| match members.get(name) {
        Some(serde_json::Value::String(text)) => Some(text.clone()),
        _ => None,
    };
    match (text("method"), text("url"), text("body")) {
        (Some(method), Some(url), Some(body)) if method.eq_ignore_ascii_case("PUT") => {
            FoundCall::Put { url, body }
        }
        _ => FoundCall::NotAPut,
    }
}
