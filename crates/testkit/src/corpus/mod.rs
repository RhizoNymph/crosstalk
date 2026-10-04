//! The recorded-traffic corpus and its loader.
//!
//! Cases live under `crates/testkit/corpus/<protocol>/<route>/<case>/`, one
//! directory each, holding `request.http`, `response.http` and `meta.json`
//! (see `corpus/README.md`). [`anthropic::cases`] loads every Anthropic
//! Messages case, checks each against its own metadata, and returns them
//! typed.

pub mod anthropic;
pub mod http;
pub mod meta;
pub mod sse;

use std::path::{Path, PathBuf};

use bytes::Bytes;
use hyper::header::HeaderValue;

pub use http::{CorpusRequest, CorpusResponse, Difference, Headers, ResponseBody};
pub use meta::{BlockKind, CaseMeta, CredentialMeta, Endpoint, Expect, HarnessMeta, Provenance};
pub use sse::{EventStream, SseEvent};

/// The corpus root: `crates/testkit/corpus`.
pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// One recorded exchange: a request, the response it got, and what it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Case {
    /// The case's directory name.
    pub name: String,
    pub dir: PathBuf,
    pub meta: CaseMeta,
    pub request: CorpusRequest,
    pub response: CorpusResponse,
}

/// A credential that cannot go in a header.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the credential is not a valid header value")]
pub struct InvalidCredential;

impl Case {
    /// The request with its credential placeholder replaced by `secret`, so
    /// tests can send distinct credentials. Unchanged when the case has no
    /// credential.
    pub fn request_with_credential(
        &self,
        secret: &str,
    ) -> Result<CorpusRequest, InvalidCredential> {
        let mut request = self.request.clone();
        let Some(credential) = &self.meta.credential else {
            return Ok(request);
        };
        let mut headers = Headers::new();
        for (name, value) in self.request.headers.iter() {
            let value = if name.as_str() == credential.header {
                let text = String::from_utf8_lossy(value.as_bytes())
                    .replace(&credential.placeholder, secret);
                HeaderValue::from_str(&text).map_err(|_| InvalidCredential)?
            } else {
                value.clone()
            };
            headers.push(name.clone(), value);
        }
        request.headers = headers;
        Ok(request)
    }

    /// The response's body bytes, exactly as recorded.
    pub fn response_bytes(&self) -> &Bytes {
        self.response.body.bytes()
    }
}
