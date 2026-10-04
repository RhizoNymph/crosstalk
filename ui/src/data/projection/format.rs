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
//!
//! **From the spec's frame.** [`PayloadPoints::new`] reads a stored
//! `Projection`: its frame's coordinates, transmission ids, sender, reader,
//! route-kind and topic columns, re-indexed into this payload's tables
//! (one `agents` table for senders and readers; each table sorted by id).
//! The frame has no channel column, so the channel of each channel-routed
//! point comes from the caller (the data route reads the transmissions'
//! routes); a point whose channel is unknown is [`NONE`].

use std::collections::{BTreeSet, HashMap};

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::projection::{Projection, ProjectionStatus};
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use serde::{Deserialize, Serialize};

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

/// Category of one point, as indexes into the tables of [`PayloadPoints`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointCategories {
    pub sender: u32,
    pub reader: u32,
    pub route: RouteKind,
    /// Into `channels`; `None` unless channel-routed with a known channel.
    pub channel: Option<u32>,
    /// Into `topics`; `None` for outliers.
    pub topic: Option<u32>,
}

/// A stored projection's points re-indexed into the payload's tables:
/// every table sorted by id, senders and readers sharing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadPoints {
    agents: Vec<AgentId>,
    channels: Vec<ChannelId>,
    topics: Vec<TopicId>,
    categories: Vec<PointCategories>,
}

/// The index of `value` in the sorted `table`.
fn index_of<T: Ord>(table: &[T], value: &T) -> Option<u32> {
    table
        .binary_search(value)
        .ok()
        .and_then(|i| u32::try_from(i).ok())
}

impl PayloadPoints {
    /// `channels` names the channel of channel-routed points by
    /// transmission; others are ignored.
    pub fn new(projection: &Projection, channels: &HashMap<TransmissionId, ChannelId>) -> Self {
        let points: Vec<_> = projection.frame().points().collect();
        let channel_of = |point: &crosstalk_spec::aggregates::projection::ProjectedPoint| {
            (point.route.kind() == RouteKind::Channel)
                .then(|| channels.get(&point.transmission).copied())
                .flatten()
        };
        let agents: Vec<AgentId> = points
            .iter()
            .flat_map(|p| [p.from, p.to])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let channel_table: Vec<ChannelId> = points
            .iter()
            .filter_map(channel_of)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let topics: Vec<TopicId> = points
            .iter()
            .filter_map(|p| p.topic)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let categories = points
            .iter()
            .map(|p| PointCategories {
                sender: index_of(&agents, &p.from).unwrap_or(NONE),
                reader: index_of(&agents, &p.to).unwrap_or(NONE),
                route: p.route.kind(),
                channel: channel_of(p).and_then(|c| index_of(&channel_table, &c)),
                topic: p.topic.and_then(|t| index_of(&topics, &t)),
            })
            .collect();
        Self {
            agents,
            channels: channel_table,
            topics,
            categories,
        }
    }

    pub fn agents(&self) -> &[AgentId] {
        &self.agents
    }

    pub fn channels(&self) -> &[ChannelId] {
        &self.channels
    }

    pub fn topics(&self) -> &[TopicId] {
        &self.topics
    }

    pub fn categories(&self) -> &[PointCategories] {
        &self.categories
    }
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
        points: &PayloadPoints,
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
    #[error("{0} categories for {1} points")]
    Categories(usize, usize),
    #[error("the projection's job is not ready")]
    NotReady,
    #[error("header: {0}")]
    Header(#[from] serde_json::Error),
}

fn header(
    projection: &Projection,
    points: &PayloadPoints,
    tables: &ProjectionTables,
    count: u32,
) -> Result<ProjectionHeader, EncodeError> {
    let info = projection.info();
    let ProjectionStatus::Ready(fitted) = info.status() else {
        return Err(EncodeError::NotReady);
    };
    let spec = info.spec();
    let params = spec.params();
    Ok(ProjectionHeader {
        id: info.id().to_ulid(),
        count,
        window: spec.window().into(),
        topic_version: spec.topic_version().0,
        fitted_at: format_time(fitted.fitted_at),
        embedding_model: EmbeddingModelPayload {
            name: spec.embedding_model().name.clone(),
            dimension: spec.embedding_model().dimension.get(),
        },
        params: ParamsPayload {
            neighbors: params.neighbors(),
            min_dist: params.min_dist(),
            seed: params.seed().to_string(),
            sample_limit: params.limit().get().get(),
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
    })
}

fn pad_to_4(buf: &mut Vec<u8>, fill: u8) {
    while !buf.len().is_multiple_of(4) {
        buf.push(fill);
    }
}

/// Encodes a projection in the format described in the module docs.
pub fn encode(
    projection: &Projection,
    points: &PayloadPoints,
    tables: &ProjectionTables,
) -> Result<Vec<u8>, EncodeError> {
    let columns = projection.frame().columns();
    let n = columns.transmissions.len();
    if points.categories().len() != n {
        return Err(EncodeError::Categories(points.categories().len(), n));
    }
    let count = u32::try_from(n).map_err(|_| EncodeError::TooManyPoints(n))?;
    let mut header_json = serde_json::to_vec(&header(projection, points, tables, count)?)?;
    pad_to_4(&mut header_json, b' ');
    let header_len = u32::try_from(header_json.len()).map_err(|_| EncodeError::TooManyPoints(n))?;

    let mut out = Vec::with_capacity(12 + header_json.len() + 41 * n + 3);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&header_json);
    for [x, _] in &columns.xy {
        out.extend_from_slice(&x.to_le_bytes());
    }
    for [_, y] in &columns.xy {
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
    for id in &columns.transmissions {
        out.extend_from_slice(&id.as_ulid().to_be_bytes());
    }
    Ok(out)
}
