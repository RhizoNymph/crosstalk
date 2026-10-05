//! The gateway's exchange log (`<data dir>/exchanges/exchange-log.jsonl`,
//! crosstalk-gateway's `log` module): one spec `Envelope` per line, each an
//! `ingest`/`exchange_captured` event carrying one `Exchange` (meta, request
//! message hashes, outcome). The message bodies are in the gateway's blob
//! store, not the log.
//!
//! A last line without its newline is a torn append and is ignored, as the
//! gateway does. Exchanges are grouped by the harness session id
//! (`meta.client.ids.session`, from `x-claude-code-session-id`) and
//! ordered by (`started_at`, id) within it: the position in that order is
//! the session's generation-request ordinal, the truth file's `turn`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{CredentialHash, ExchangeId};
use crosstalk_spec::observed::exchange::Exchange;

#[derive(Debug, thiserror::Error)]
pub enum ExchangeLogError {
    #[error("reading the exchange log {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("exchange log line {line} is not an envelope: {source}")]
    Decode {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("exchange log line {line} is a {subject:?} event, not exchange_captured")]
    NotAnExchange {
        line: usize,
        subject: crosstalk_spec::events::Subject,
    },
}

/// The log as read.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExchangeLog {
    /// Each exchange once, in log order (a repeated exchange id keeps its
    /// first line).
    pub exchanges: Vec<Exchange>,
    /// Bytes after the last newline, ignored.
    pub torn_tail: usize,
    /// Lines whose exchange id was already in the log.
    pub duplicates: usize,
}

/// Parses the log's bytes.
pub fn parse(bytes: &[u8]) -> Result<ExchangeLog, ExchangeLogError> {
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |last| last + 1);
    let mut seen = BTreeSet::new();
    let mut log = ExchangeLog {
        torn_tail: bytes.len() - complete,
        ..ExchangeLog::default()
    };
    for (at, line) in bytes[..complete].split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let line_no = at + 1;
        let envelope: Envelope =
            serde_json::from_slice(line).map_err(|source| ExchangeLogError::Decode {
                line: line_no,
                source,
            })?;
        let exchange = match envelope.event {
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => *exchange,
            other => {
                return Err(ExchangeLogError::NotAnExchange {
                    line: line_no,
                    subject: other.subject(),
                });
            }
        };
        if seen.insert(exchange.meta.id) {
            log.exchanges.push(exchange);
        } else {
            log.duplicates += 1;
        }
    }
    Ok(log)
}

/// Reads the log at `path`.
pub fn read(path: &Path) -> Result<ExchangeLog, ExchangeLogError> {
    let bytes = std::fs::read(path).map_err(|source| ExchangeLogError::Read {
        path: path.display().to_string(),
        source,
    })?;
    parse(&bytes)
}

/// One session's exchanges in ordinal order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Session {
    pub exchanges: Vec<Exchange>,
    /// Every credential digest the session's exchanges carried; one for an
    /// agent on one key.
    pub credentials: BTreeSet<CredentialHash>,
}

impl Session {
    /// The exchange at generation-request ordinal `turn`.
    pub fn at(&self, turn: u32) -> Option<&Exchange> {
        self.exchanges.get(usize::try_from(turn).ok()?)
    }

    /// The ordinal of exchange `id`.
    pub fn ordinal(&self, id: ExchangeId) -> Option<u32> {
        let at = self.exchanges.iter().position(|ex| ex.meta.id == id)?;
        u32::try_from(at).ok()
    }
}

/// The log's exchanges by harness session.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sessions {
    sessions: BTreeMap<String, Session>,
    /// Exchanges with no session id; nothing in the truth can name them.
    pub without_session: usize,
}

impl Sessions {
    /// Groups `exchanges` by session and orders each group by
    /// (`started_at`, id).
    pub fn index(exchanges: Vec<Exchange>) -> Self {
        let mut out = Self::default();
        for exchange in exchanges {
            let Some(session) = exchange.meta.client.ids.session.clone() else {
                out.without_session += 1;
                continue;
            };
            let entry = out.sessions.entry(session).or_default();
            if let Some(credential) = &exchange.meta.client.credential {
                entry.credentials.insert(credential.hash);
            }
            entry.exchanges.push(exchange);
        }
        for session in out.sessions.values_mut() {
            session
                .exchanges
                .sort_by_key(|exchange| (exchange.meta.started_at, exchange.meta.id));
        }
        out
    }

    pub fn get(&self, session: &str) -> Option<&Session> {
        self.sessions.get(session)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Session)> {
        self.sessions.iter()
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}
