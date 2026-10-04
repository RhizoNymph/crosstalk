//! The binary `<ct-projection>` payload.
//!
//! All numbers little-endian; every column starts 4-byte aligned. With `n`
//! points and a header of `h` bytes (a multiple of 4):
//!
//! | Offset | Size | Content |
//! | --- | --- | --- |
//! | 0 | 4 | magic `CTPJ` |
//! | 4 | 4 | `u32` format version, [`VERSION`] |
//! | 8 | 4 | `u32` header length `h` |
//! | 12 | `h` | header JSON ([`ProjectionHeader`]), UTF-8, right-padded with spaces |
//! | | `4n` | `f32` x per point (projection coordinates) |
//! | | `4n` | `f32` y |
//! | | `4n` | `u32` sender, index into `header.agents` |
//! | | `4n` | `u32` reader, index into `header.agents` |
//! | | `n`, padded to 4 | `u8` route kind, index into `header.routeKinds` |
//! | | `4n` | `u32` channel, index into `header.channels`; [`NONE`] unless channel-routed |
//! | | `4n` | `u32` topic, index into `header.topics`; [`NONE`] for outliers |
//! | | `16n` | transmission id, 128-bit big-endian (the ULID's bytes) |
//!
//! The total length is exactly `12 + h + 41n + pad(n)`; decoders reject
//! anything else.

use crosstalk_spec::aggregates::edge::RouteKind;
use serde::{Deserialize, Serialize};

use crate::contract::research::ProjectionPoints;
use crate::data::topology::{RouteKindCode, WindowPayload};
use crate::url::ulid::UlidId;
use crate::url::view_state::format_time;

pub const MAGIC: [u8; 4] = *b"CTPJ";
pub const VERSION: u32 = 1;
/// The index of an absent channel or topic.
pub const NONE: u32 = u32::MAX;
/// Route kinds in the order their `u8` codes index.
pub const ROUTE_KINDS: [RouteKind; 4] = [
    RouteKind::Channel,
    RouteKind::Delegation,
    RouteKind::Direct,
    RouteKind::Unobserved,
];

pub fn route_code(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

/// The JSON header: what the projection is, and the category tables the
/// columns index into.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectionHeader {
    pub id: String,
    pub count: u32,
    pub window: WindowPayload,
    pub topic_version: u32,
    pub fitted_at: String,
    pub embedding_model: EmbeddingModelPayload,
    pub params: ParamsPayload,
    pub route_kinds: Vec<RouteKindCode>,
    pub agents: Vec<NamedEntry>,
    pub channels: Vec<NamedEntry>,
    pub topics: Vec<TopicEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EmbeddingModelPayload {
    pub name: String,
    pub dimension: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParamsPayload {
    pub neighbors: u16,
    pub min_dist: f32,
    /// A string: `u64` does not survive a JavaScript number.
    pub seed: String,
    pub sample_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NamedEntry {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TopicEntry {
    pub id: String,
    /// `None` when the label cannot be shown: no `Content`, or the topic
    /// version is no longer retained.
    pub label: Option<String>,
}

/// Display names for a projection's category tables, one per table entry.
/// Built only through [`ProjectionTables::new`], which checks the lengths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionTables {
    agents: Vec<String>,
    channels: Vec<String>,
    topics: Vec<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TableLengthMismatch {
    #[error("{table}: {got} names for {expected} entries")]
    Table {
        table: &'static str,
        expected: usize,
        got: usize,
    },
}

impl ProjectionTables {
    pub fn new(
        points: &ProjectionPoints,
        agents: Vec<String>,
        channels: Vec<String>,
        topics: Vec<Option<String>>,
    ) -> Result<Self, TableLengthMismatch> {
        for (table, expected, got) in [
            ("agents", points.agents().len(), agents.len()),
            ("channels", points.channels().len(), channels.len()),
            ("topics", points.topics().len(), topics.len()),
        ] {
            if expected != got {
                return Err(TableLengthMismatch::Table {
                    table,
                    expected,
                    got,
                });
            }
        }
        Ok(Self {
            agents,
            channels,
            topics,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("{0} points do not fit a u32 count")]
    TooManyPoints(usize),
    #[error("header: {0}")]
    Header(#[from] serde_json::Error),
}

fn header(points: &ProjectionPoints, tables: &ProjectionTables, count: u32) -> ProjectionHeader {
    let meta = points.meta();
    ProjectionHeader {
        id: meta.id.to_ulid(),
        count,
        window: meta.scope.window.into(),
        topic_version: meta.scope.topic_version.0,
        fitted_at: format_time(meta.fitted_at),
        embedding_model: EmbeddingModelPayload {
            name: meta.embedding_model.name.clone(),
            dimension: meta.embedding_model.dimension.get(),
        },
        params: ParamsPayload {
            neighbors: meta.params.neighbors.get(),
            min_dist: meta.params.min_dist(),
            seed: meta.params.seed.to_string(),
            sample_limit: meta.params.sample_limit.get(),
        },
        route_kinds: ROUTE_KINDS.iter().map(|k| (*k).into()).collect(),
        agents: points
            .agents()
            .iter()
            .zip(&tables.agents)
            .map(|(id, name)| NamedEntry {
                id: id.to_ulid(),
                name: name.clone(),
            })
            .collect(),
        channels: points
            .channels()
            .iter()
            .zip(&tables.channels)
            .map(|(id, name)| NamedEntry {
                id: id.to_ulid(),
                name: name.clone(),
            })
            .collect(),
        topics: points
            .topics()
            .iter()
            .zip(&tables.topics)
            .map(|(id, label)| TopicEntry {
                id: id.to_ulid(),
                label: label.clone(),
            })
            .collect(),
    }
}

fn pad_to_4(buf: &mut Vec<u8>, fill: u8) {
    while !buf.len().is_multiple_of(4) {
        buf.push(fill);
    }
}

/// Encodes a projection in the format described in the module docs.
pub fn encode(
    points: &ProjectionPoints,
    tables: &ProjectionTables,
) -> Result<Vec<u8>, EncodeError> {
    let n = points.len();
    let count = u32::try_from(n).map_err(|_| EncodeError::TooManyPoints(n))?;
    let mut header_json = serde_json::to_vec(&header(points, tables, count))?;
    pad_to_4(&mut header_json, b' ');
    let header_len = u32::try_from(header_json.len()).map_err(|_| EncodeError::TooManyPoints(n))?;

    let mut out = Vec::with_capacity(12 + header_json.len() + 41 * n + 3);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&header_json);
    for x in points.xs() {
        out.extend_from_slice(&x.to_le_bytes());
    }
    for y in points.ys() {
        out.extend_from_slice(&y.to_le_bytes());
    }
    let categories = points.categories();
    for c in categories {
        out.extend_from_slice(&c.sender.to_le_bytes());
    }
    for c in categories {
        out.extend_from_slice(&c.reader.to_le_bytes());
    }
    for c in categories {
        out.push(route_code(c.route));
    }
    pad_to_4(&mut out, 0);
    for c in categories {
        out.extend_from_slice(&c.channel.unwrap_or(NONE).to_le_bytes());
    }
    for c in categories {
        out.extend_from_slice(&c.topic.unwrap_or(NONE).to_le_bytes());
    }
    for id in points.transmissions() {
        out.extend_from_slice(&id.as_ulid().to_be_bytes());
    }
    Ok(out)
}
