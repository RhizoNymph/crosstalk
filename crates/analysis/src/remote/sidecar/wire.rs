//! The sidecar's HTTP contract, version 1, as Rust types
//! (`docs/features/topics_sidecar.md`). Requests serialize to exactly the
//! bytes the contract shows (field order included); replies decode
//! strictly, refusing unknown fields and variants.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::projection::ProjectionParams;
use serde::{Deserialize, Serialize};

use super::params::TopicFitParams;
use crate::remote::matrix::Matrix;

/// The contract version these types speak, as `/healthz` reports it.
pub const CONTRACT: &str = "v1";

pub const HEALTH: &str = "/healthz";
pub const TOPICS_FIT: &str = "/v1/topics/fit";
pub const LAYOUT_FIT: &str = "/v1/layout/fit";
pub const LAYOUT_TRANSFORM: &str = "/v1/layout/transform";

/// `GET /healthz`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Health {
    pub status: String,
    pub contract: String,
    pub versions: BTreeMap<String, String>,
}

/// `POST /v1/topics/fit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TopicsFitRequest {
    pub embeddings: Matrix,
    pub texts: Vec<String>,
    pub params: TopicFitParams,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TopicsFitReply {
    /// One per document: `-1` or a cluster index.
    pub labels: Vec<i64>,
    pub topics: Vec<TopicReply>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TopicReply {
    pub label: String,
    /// `[term, weight]` pairs, weight highest first. Checked by the adapter
    /// (finite, positive) before they become `Finite`.
    pub terms: Vec<(String, f64)>,
}

/// `POST /v1/layout/fit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LayoutFitRequest {
    pub embeddings: Matrix,
    pub params: ProjectionParams,
}

/// `POST /v1/layout/transform`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LayoutTransformRequest {
    pub base: Matrix,
    pub params: ProjectionParams,
    pub points: Matrix,
}

/// The reply of both layout routes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LayoutReply {
    pub coordinates: Matrix,
}

/// Every error body, adjacently tagged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ErrorBody {
    InvalidRequest { reason: String },
    NotFound,
    MethodNotAllowed,
    PayloadTooLarge { limit_bytes: u64 },
    TooFewSamples { needed: u32, got: u32 },
    TooFewPoints { needed: u32, got: u64 },
    NonFiniteLayout,
    Internal { reason: String },
}
