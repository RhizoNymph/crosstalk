//! L6's computational adapters over HTTP (decision D1,
//! `docs/features/topics_sidecar.md`):
//!
//! - [`SidecarTopicModel`] (`TopicModel`) and [`SidecarLayoutFitter`]
//!   (`LayoutFitter`) over the Python topics sidecar's contract;
//! - [`OpenAiEmbedder`] (`Embedder`) over an OpenAI-compatible embeddings
//!   endpoint;
//! - the shared [`http::HttpClient`] (hyper, rustls) with a deadline on
//!   every call, and the sidecar's [`matrix`] encoding.

pub mod embedder;
pub mod http;
pub mod matrix;
pub mod sidecar;

pub use embedder::{ApiKey, ApiKeyError, EmbedderError, OpenAiEmbedder, OpenAiEmbedderConfig};
pub use http::{BaseUrl, HttpClient, HttpError, InvalidBaseUrl};
pub use sidecar::layout::{SidecarLayoutFitter, TransformError};
pub use sidecar::params::{InvalidTopicFitParams, TopicFitParams};
pub use sidecar::topics::{RestoreError, SidecarTopicModel, TopicModelConfig};
pub use sidecar::{ContractViolation, SidecarClient, SidecarConfig, SidecarError, StatusBody};

#[cfg(test)]
mod tests;
