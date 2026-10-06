//! Saving the gateway's detections over its L8 HTTP API, so the benchmark
//! itself runs offline on files:
//!
//! 1. `POST {api}/exports` with an `ExportRequest` for the transmissions
//!    dataset (JSONL, no content columns) over the window asked for, in
//!    [`FETCHED_STATES`]: the confirmed ones and the discarded ones, so
//!    access-only scoring has live input (a settled export never holds a
//!    suspected or awaiting-content transmission; unconfirmed traffic is
//!    `discarded` by then). The body is saved as it came (`export.jsonl`)
//!    and must verify.
//! 2. `GET {api}/transmissions/{id}/evidence?window={"context":0}` for each
//!    exported transmission, discarded ones included; each non-null answer
//!    is one line of `evidence.jsonl`.
//!
//! Plain HTTP/1.1 only (the API on the compose network). A bearer token,
//! when given, goes in `Authorization`.

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::request::{
    ExportDataset, ExportFormat, ExportRequest, ExportScope, ExportStates, TransmissionScope,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::support::TimeWindow;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;

use super::detected::{DetectedError, read_export};
use super::queried::{Queried, batches};
use crosstalk_spec::ids::{ExchangeId, SpanId};
use crosstalk_spec::interfaces::l8_surface::conversation::{ExchangePlacement, SpanPoint};
use std::collections::{BTreeMap, BTreeSet};

/// What to fetch and from where.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// The API's base URL, e.g. `http://crosstalk:8081`.
    pub api: String,
    pub token: Option<String>,
    pub window: TimeWindow,
}

/// What was saved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub export: PathBuf,
    pub evidence: PathBuf,
    pub transmissions: usize,
    /// Exported transmissions whose evidence read answered `null`.
    pub without_evidence: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("starting the HTTP runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("{url} is not a request URL: {reason}")]
    Url { url: String, reason: String },
    #[error("encoding the request: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("the export request is not valid: {0}")]
    Request(String),
    #[error("{method} {url}: {reason}")]
    Http {
        method: &'static str,
        url: String,
        reason: String,
    },
    #[error("{method} {url} answered {status}: {body}")]
    Status {
        method: &'static str,
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error("{url} did not answer a transmission's evidence: {source}")]
    Evidence {
        url: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{url} did not answer the read's shape: {source}")]
    Answer {
        url: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Export(#[from] DetectedError),
    #[error("writing {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// The transmission states the export is asked for: every confirmed state
/// and `Discarded`. A settled export holds no `Suspected` or
/// `AwaitingContent` row, so asking for them would add nothing.
pub const FETCHED_STATES: [TransmissionStateKind; 4] = [
    TransmissionStateKind::Confirmed,
    TransmissionStateKind::Classified,
    TransmissionStateKind::Aggregated,
    TransmissionStateKind::Discarded,
];

/// The export request the fetch makes: the transmissions in
/// [`FETCHED_STATES`] over `window`, JSONL, without content.
pub fn export_request(window: TimeWindow) -> Result<ExportRequest, FetchError> {
    let states = ExportStates::new(FETCHED_STATES.to_vec())
        .map_err(|error| FetchError::Request(format!("{error:?}")))?;
    let scope = TransmissionScope {
        states,
        ..TransmissionScope::confirmed(ExportScope {
            window,
            filter: TopologyFilter::default(),
        })
    };
    ExportRequest::new(
        ExportDataset::Transmissions(scope),
        ExportFormat::Jsonl,
        false,
    )
    .map_err(|error| FetchError::Request(format!("{error:?}")))
}

/// How much of an error body a message quotes.
const QUOTED_BODY: usize = 512;

/// `text` percent-encoded for a query value: every byte but unreserved
/// characters.
fn percent(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 3);
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

struct Api {
    client: Client<HttpConnector, Full<Bytes>>,
    base: String,
    token: Option<String>,
}

impl Api {
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Bytes, FetchError> {
        let url = format!("{}{path}", self.base.trim_end_matches('/'));
        let name = if method == Method::POST {
            "POST"
        } else {
            "GET"
        };
        let mut request = Request::builder().method(method).uri(&url);
        if let Some(token) = &self.token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let request = match body {
            Some(bytes) => request
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(bytes))),
            None => request.body(Full::new(Bytes::new())),
        }
        .map_err(|error| FetchError::Url {
            url: url.clone(),
            reason: error.to_string(),
        })?;
        let response = self
            .client
            .request(request)
            .await
            .map_err(|error| FetchError::Http {
                method: name,
                url: url.clone(),
                reason: error.to_string(),
            })?;
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|error| FetchError::Http {
                method: name,
                url: url.clone(),
                reason: error.to_string(),
            })?
            .to_bytes();
        if status != StatusCode::OK {
            let text = String::from_utf8_lossy(&bytes);
            return Err(FetchError::Status {
                method: name,
                url,
                status,
                body: text.chars().take(QUOTED_BODY).collect(),
            });
        }
        Ok(bytes)
    }
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<(), FetchError> {
    std::fs::write(path, bytes).map_err(|source| FetchError::Write {
        path: path.display().to_string(),
        source,
    })
}

/// Fetches the export and every exported transmission's evidence into
/// `out` (`export.jsonl`, `evidence.jsonl`).
pub fn fetch(config: &FetchConfig, out: &Path) -> Result<Fetched, FetchError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(FetchError::Runtime)?;
    runtime.block_on(fetch_async(config, out))
}

async fn fetch_async(config: &FetchConfig, out: &Path) -> Result<Fetched, FetchError> {
    let api = Api {
        client: Client::builder(TokioExecutor::new()).build_http(),
        base: config.api.clone(),
        token: config.token.clone(),
    };
    let request = export_request(config.window)?;
    let body = serde_json::to_vec(&request).map_err(FetchError::Encode)?;
    let export = api.call(Method::POST, "/exports", Some(body)).await?;
    let export_path = out.join("export.jsonl");
    write_file(&export_path, &export)?;
    let exported = read_export(&export)?;
    tracing::info!(
        transmissions = exported.transmissions.len(),
        path = %export_path.display(),
        "export saved"
    );
    let window = serde_json::to_string(&ExcerptWindow::MATCH_ONLY).map_err(FetchError::Encode)?;
    let window = percent(&window);
    let evidence_path = out.join("evidence.jsonl");
    let file = std::fs::File::create(&evidence_path).map_err(|source| FetchError::Write {
        path: evidence_path.display().to_string(),
        source,
    })?;
    let mut writer = std::io::BufWriter::new(file);
    let mut without_evidence = 0usize;
    for id in &exported.transmissions {
        let path = format!("/transmissions/{}/evidence?window={window}", id.ulid_text());
        let bytes = api.call(Method::GET, &path, None).await?;
        let evidence: Option<TransmissionEvidence> =
            serde_json::from_slice(&bytes).map_err(|source| FetchError::Evidence {
                url: path.clone(),
                source,
            })?;
        let Some(evidence) = evidence else {
            without_evidence += 1;
            tracing::warn!(transmission = %id.ulid_text(), "no evidence for an exported transmission");
            continue;
        };
        let line = serde_json::to_string(&evidence).map_err(FetchError::Encode)?;
        writeln!(writer, "{line}").map_err(|source| FetchError::Write {
            path: evidence_path.display().to_string(),
            source,
        })?;
    }
    writer.flush().map_err(|source| FetchError::Write {
        path: evidence_path.display().to_string(),
        source,
    })?;
    Ok(Fetched {
        export: export_path,
        evidence: evidence_path,
        transmissions: exported.transmissions.len(),
        without_evidence,
    })
}

/// Asks the API's conversation reads for `exchanges` and `spans`
/// (`POST /query/exchange-turns`, `POST /query/span-points`), in batches
/// of `IdBatch::MAX` ids, and merges the answers.
pub fn fetch_queried(
    api: &str,
    token: Option<String>,
    exchanges: &BTreeSet<ExchangeId>,
    spans: &BTreeSet<SpanId>,
) -> Result<Queried, FetchError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(FetchError::Runtime)?;
    let api = Api {
        client: Client::builder(TokioExecutor::new()).build_http(),
        base: api.to_owned(),
        token,
    };
    runtime.block_on(async {
        let mut out = Queried::default();
        for batch in batches(exchanges) {
            let body = serde_json::to_vec(&batch).map_err(FetchError::Encode)?;
            let bytes = api
                .call(Method::POST, "/query/exchange-turns", Some(body))
                .await?;
            let answer: BTreeMap<ExchangeId, ExchangePlacement> = serde_json::from_slice(&bytes)
                .map_err(|source| FetchError::Answer {
                    url: "/query/exchange-turns".to_owned(),
                    source,
                })?;
            out.turns.extend(answer);
        }
        for batch in batches(spans) {
            let body = serde_json::to_vec(&batch).map_err(FetchError::Encode)?;
            let bytes = api
                .call(Method::POST, "/query/span-points", Some(body))
                .await?;
            let answer: BTreeMap<SpanId, SpanPoint> =
                serde_json::from_slice(&bytes).map_err(|source| FetchError::Answer {
                    url: "/query/span-points".to_owned(),
                    source,
                })?;
            out.spans.extend(answer);
        }
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::{export_request, percent};
    use crosstalk_spec::support::{TimeWindow, Timestamp};

    #[test]
    fn the_export_asks_for_confirmed_and_discarded_transmissions() {
        let window = TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(1))
            .unwrap_or_else(|e| panic!("{e:?}"));
        let request = export_request(window).unwrap_or_else(|e| panic!("{e}"));
        let json = serde_json::to_value(&request).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(json["dataset"]["type"], "transmissions");
        assert_eq!(
            json["dataset"]["data"]["states"],
            serde_json::json!(["confirmed", "classified", "aggregated", "discarded"])
        );
        assert_eq!(json["format"], "jsonl");
        assert_eq!(json["include_content"], false);
    }

    #[test]
    fn query_values_are_percent_encoded() {
        assert_eq!(percent(r#"{"context":0}"#), "%7B%22context%22%3A0%7D");
    }
}
