//! `crosstalk inspect --config <path> [<exchange-id>]`: read what the
//! gateway captured, from the exchange log and the blob store the config
//! names.
//!
//! Without an id, one line per logged exchange: id, start time, model,
//! transport, outcome and request message count. With an id, that
//! exchange's envelope and every message it names (request, then response
//! or partial response), each body read from the blob store, checked
//! against the canonical encoding and printed as its JSON. Reads only; the
//! gateway may be running.

use std::fmt::Write as _;

use crosstalk_canonical::encoding;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_transport::blob::{FsBlobStore, OpenError};
use serde_json::{Value, json};

use crate::config::{ConfigError, GatewayConfig};
use crate::log::{self, LogError};

/// Why an inspection failed.
#[derive(Debug, thiserror::Error)]
pub enum InspectError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("opening the blob store: {0}")]
    Blobs(#[from] OpenError),
    #[error("reading blob {hash}: {error:?}")]
    Blob { hash: String, error: BlobError },
    #[error("no captured exchange {0} in the log")]
    UnknownExchange(String),
    #[error("encoding the output: {0}")]
    Encode(#[from] serde_json::Error),
}

/// Every logged exchange, one line each, oldest first.
pub async fn list(config: &GatewayConfig) -> Result<String, InspectError> {
    let path = config.exchange_log_path()?;
    let contents = log::read(&path).await?;
    let mut out = String::new();
    let exchanges: Vec<_> = contents.entries.iter().filter_map(exchange_of).collect();
    // Writing to a String cannot fail.
    let _ = writeln!(
        out,
        "{} captured exchange(s) in {}",
        exchanges.len(),
        path.display()
    );
    for exchange in exchanges {
        let _ = writeln!(
            out,
            "{}  {}  {}  {}  {}  {} request message(s)",
            exchange.meta.id.ulid_text(),
            text_of(&exchange.meta.started_at),
            exchange.meta.model.0,
            text_of(&exchange.meta.transport),
            outcome(&exchange.outcome),
            exchange.request.len()
        );
    }
    if contents.torn_tail > 0 {
        let _ = writeln!(
            out,
            "(the log ends with {} bytes of an unfinished entry, ignored)",
            contents.torn_tail
        );
    }
    Ok(out)
}

/// One exchange and its message bodies, as pretty JSON.
pub async fn show(config: &GatewayConfig, exchange: &str) -> Result<String, InspectError> {
    let wanted = ExchangeId::from_ulid_text(exchange)
        .map_err(|_| InspectError::UnknownExchange(exchange.to_owned()))?;
    let contents = log::read(&config.exchange_log_path()?).await?;
    let Some(envelope) = contents
        .entries
        .iter()
        .find(|envelope| exchange_of(envelope).is_some_and(|found| found.meta.id == wanted))
    else {
        return Err(InspectError::UnknownExchange(exchange.to_owned()));
    };
    let blobs = FsBlobStore::open(&config.blobs.root).await?;
    let mut messages = Vec::new();
    if let Some(exchange) = exchange_of(envelope) {
        for (role, hash) in named_messages(exchange) {
            messages.push(message(&blobs, role, hash).await?);
        }
    }
    let shown = json!({"envelope": envelope, "messages": messages});
    Ok(serde_json::to_string_pretty(&shown)?)
}

fn exchange_of(envelope: &Envelope) -> Option<&Exchange> {
    match &envelope.event {
        BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => Some(exchange),
        _ => None,
    }
}

/// The request's messages, then the response or partial response.
fn named_messages(exchange: &Exchange) -> Vec<(&'static str, MessageHash)> {
    let mut named: Vec<_> = exchange
        .request
        .iter()
        .map(|hash| ("request", *hash))
        .collect();
    match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => named.push(("response", *response)),
        ExchangeOutcome::Failed {
            partial_response: Some(partial),
            ..
        } => named.push(("partial_response", *partial)),
        ExchangeOutcome::Failed { .. } => {}
    }
    named
}

async fn message(
    blobs: &FsBlobStore,
    role: &str,
    hash: MessageHash,
) -> Result<Value, InspectError> {
    let hex = hash.digest().to_hex();
    let bytes = blobs.get(hash).await.map_err(|error| InspectError::Blob {
        hash: hex.clone(),
        error,
    })?;
    let body = match bytes {
        None => json!({"missing": true}),
        Some(bytes) => match encoding::decode(&bytes) {
            Ok(_) => serde_json::from_slice::<Value>(&bytes)
                .unwrap_or_else(|_| json!({"undecodable": true})),
            Err(_) => json!({"not_canonical": true}),
        },
    };
    Ok(json!({"role": role, "hash": hex, "body": body}))
}

fn outcome(outcome: &ExchangeOutcome) -> String {
    match outcome {
        ExchangeOutcome::Completed { stop, .. } => format!("completed({})", text_of(stop)),
        ExchangeOutcome::Failed { failure, .. } => format!("failed({})", text_of(failure)),
    }
}

/// A value's wire JSON, without quotes when it is a string.
fn text_of<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(text)) => text,
        Ok(other) => other.to_string(),
        Err(_) => "?".to_owned(),
    }
}
