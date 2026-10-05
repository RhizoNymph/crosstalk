//! The HTTP client for the crosstalk L8 surface, used by the operator UI.
//!
//! [`HttpClient`] implements the traits of
//! [`crosstalk_spec::interfaces::l8_surface`] over the HTTP binding
//! ([`crosstalk_spec::interfaces::l8_surface::http`]), so a server that
//! renders pages from a `QueryApi` can talk to a gateway over HTTP with no
//! other change:
//!
//! - [`QueryApi`](crosstalk_spec::interfaces::l8_surface::QueryApi): every
//!   method encodes its call with the binding's `RequestBuilder` for its
//!   `Route`, and decodes the success or the error the route answers with;
//! - [`OperatorActions`](crosstalk_spec::interfaces::l8_surface::OperatorActions):
//!   `act` sends the action's `ActionRequest` to `POST /actions`;
//! - [`LiveFeed`](crosstalk_spec::interfaces::l8_surface::live::LiveFeed):
//!   `GET /live` read as Server-Sent Events, reconnecting from the last
//!   cursor when the connection is cut;
//! - export: `POST /exports`, the JSONL lines read as they arrive and
//!   checked against the header and the trailer as they pass, or the raw
//!   download for a page that hands the file on.
//!
//! ```text
//! QueryApi call ─▶ RequestBuilder (route table) ─▶ EncodedRequest ─▶ hyper request (+ bearer token)
//!   ◀─ success status: the route's body (JSON, frame bytes, SSE, JSONL)
//!   ◀─ any other: 401 AuthError, or the error JSON whose ErrorStatus is that status
//! ```
//!
//! Roadmap: P7.2 (HTTP client). A composition crate: it may depend on layer
//! crates, and needs none.

mod actions;
mod body;
mod client;
mod config;
mod error;
mod export;
mod form;
mod frame;
mod hasher;
mod live;
mod query;

pub use client::HttpClient;
pub use config::{
    BaseUrl, BearerToken, ClientConfig, InvalidBaseUrl, InvalidConfig, InvalidToken,
    ReconnectPolicy,
};
pub use error::{ApiError, ClientError, TransportError};
pub use export::{ExportDownload, HttpExportRows};
pub use hasher::Blake3RowHasher;
pub use live::HttpLiveStream;

#[cfg(test)]
mod tests;
