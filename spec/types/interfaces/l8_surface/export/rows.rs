//! The row schema of each dataset, the order rows are sent in, and the
//! reference builders for the two datasets read from stored values
//! (projection points and verdict records).
//!
//! **Resolution.** Every agent and channel a row names is canonical,
//! resolved through `AgentDirectory` and `ChannelDirectory` once, when the
//! export starts, and that resolution (and the copy of current verdicts a
//! `FalseDetections::Exclude` filter reads) holds for the whole export. A
//! projection point keeps what its frame stored at fit time. Every topic a
//! row names belongs to the header's topic version.
//!
//! **Order.** Each dataset's rows are sent in ascending [`RowKey`] order, and
//! no two rows of one export share a key, so the rows of an export are a
//! function of what they are read from, not of how the store happened to
//! return them.
//!
//! | Dataset | Row | Key |
//! | --- | --- | --- |
//! | transmissions | [`TransmissionRow`] | (`confirmed_at`, `id`) |
//! | edges | [`EdgeRow`] | (bucket start, sender, reader, route, topic) |
//! | accesses | [`AccessRow`] | (bucket start, agent, channel, op) |
//! | topics | [`TopicRow`] | `topic` |
//! | projection | [`PointRow`] | `index`, the point's position in the frame |
//! | verdicts | [`VerdictRow`] | (`transmission`, `revision`) |
//!
//! These rows are the export's own. `TransmissionRow` overlaps the
//! surface's transmission summary row and `MatchText` the evidence
//! excerpt; where those exist, the export's rows should be built from them.

use std::collections::HashMap;
use std::num::NonZeroU64;

use crate::aggregates::edge::{EdgeSelector, RouteKind};
use crate::aggregates::projection::{ProjectedPoint, Projection};
use crate::aggregates::quality::{MatchClass, QualityMatch};
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::transmission::{Route, Transmission};
use crate::derived::flow::verdict::{Verdict, VerdictLog, VerdictRevision};
use crate::ids::{AgentId, ChannelId, OperatorId, TopicId, TransmissionId};
use crate::support::{NonEmpty, TimeWindow, Timestamp};

use super::digest::encode_route;
use super::request::ExportDatasetKind;

/// One confirmed transmission. Its sender and reader are canonical as of
/// the export's start; when the two have since been merged they are equal
/// (the topology graph drops such a transmission; the export lists it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionRow {
    pub id: TransmissionId,
    pub from: AgentId,
    pub to: AgentId,
    /// With its channel resolved through supersession.
    pub route: Route,
    pub opened_at: Timestamp,
    /// `Confirmed::at`: what the window is tested against.
    pub confirmed_at: Timestamp,
    pub matched_bytes: NonZeroU64,
    /// [`MatchClass::strongest`] of its content matches.
    pub strongest: MatchClass,
    /// Under the header's topic version; `None` for an outlier.
    pub topic: Option<TopicId>,
    /// The current verdict as of the export's start.
    pub verdict: Option<Verdict>,
    /// Present exactly when the request includes content.
    pub content: Option<TransmissionContent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionContent {
    /// The topic's label; `None` for an outlier.
    pub topic_label: Option<String>,
    /// One per content match, in `Confirmed::content` order.
    pub matches: NonEmpty<MatchText>,
}

/// The text of one content match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchText {
    pub class: MatchClass,
    /// The sender's originated text the match covers.
    pub origin: String,
    /// The reader's input text the match was found in, as received (before
    /// any decoding).
    pub read: String,
}

/// The topic label column of edges and projection points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelContent {
    /// `None` for an outlier.
    pub topic_label: Option<String>,
}

/// One edge bucket: the transmissions from one canonical sender to another
/// canonical reader over one resolved route and topic in one bucket, as
/// `topology` sums them under the export's filter. A pair that resolves to
/// one agent counts nothing, as in the graph, so the edge is an
/// [`EdgeSelector`] and never a self-edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeRow {
    pub edge: EdgeSelector,
    /// Under the header's topic version; `None` for outliers.
    pub topic: Option<TopicId>,
    pub bucket: TimeWindow,
    pub transmissions: NonZeroU64,
    pub matched_bytes: NonZeroU64,
    pub content: Option<LabelContent>,
}

/// One access bucket over a canonical agent and channel, as
/// `channel_topology` sums them under the export's filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessRow {
    pub agent: AgentId,
    pub channel: ChannelId,
    pub op: AccessKind,
    pub bucket: TimeWindow,
    pub accesses: NonZeroU64,
}

/// One topic of the header's version (one the filter's `topics` lists, or
/// every one when it lists none) and the admitted transmissions in the
/// settled window assigned to it, zero included. Outliers are not a topic
/// and have no row.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicRow {
    pub topic: TopicId,
    pub transmissions: u64,
    pub matched_bytes: u64,
    pub content: Option<TopicContent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicContent {
    pub label: String,
    /// Top c-TF-IDF terms, highest weight first, as `Topic::terms`.
    pub terms: Vec<(String, f32)>,
}

/// One point of a stored projection, as its frame holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct PointRow {
    /// The point's position in the frame (sample order).
    pub index: u32,
    pub point: ProjectedPoint,
    pub content: Option<LabelContent>,
}

/// One verdict record, with the detector's call on its transmission as
/// `detection_quality` counts it, so the export reproduces that tally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictRow {
    pub transmission: TransmissionId,
    pub route_kind: RouteKind,
    pub call: QualityMatch,
    pub revision: VerdictRevision,
    /// `None` for a withdrawal.
    pub verdict: Option<Verdict>,
    pub by: OperatorId,
    pub at: Timestamp,
    pub note: Option<String>,
}

/// One row of an export. An export's rows are all of its dataset's kind.
#[derive(Debug, Clone, PartialEq)]
pub enum ExportRow {
    Transmission(TransmissionRow),
    Edge(EdgeRow),
    Access(AccessRow),
    Topic(TopicRow),
    Point(PointRow),
    Verdict(VerdictRow),
}

/// The order of rows within one dataset. Rows of different datasets never
/// meet in one export, so the variants are never compared with each other.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowKey {
    Transmission {
        confirmed_at: Timestamp,
        id: TransmissionId,
    },
    Edge {
        bucket_start: Timestamp,
        from: AgentId,
        to: AgentId,
        /// The route's canonical encoding ([`encode_route`]).
        route: Vec<u8>,
        topic: Option<TopicId>,
    },
    Access {
        bucket_start: Timestamp,
        agent: AgentId,
        channel: ChannelId,
        /// `false` for a write, so writes sort before reads.
        read: bool,
    },
    Topic(TopicId),
    Point(u32),
    Verdict {
        transmission: TransmissionId,
        revision: VerdictRevision,
    },
}

impl ExportRow {
    pub fn kind(&self) -> ExportDatasetKind {
        match self {
            Self::Transmission(_) => ExportDatasetKind::Transmissions,
            Self::Edge(_) => ExportDatasetKind::Edges,
            Self::Access(_) => ExportDatasetKind::Accesses,
            Self::Topic(_) => ExportDatasetKind::Topics,
            Self::Point(_) => ExportDatasetKind::Projection,
            Self::Verdict(_) => ExportDatasetKind::Verdicts,
        }
    }

    /// Whether the row carries its content columns. Rows of datasets
    /// without content columns never do.
    pub fn has_content(&self) -> bool {
        match self {
            Self::Transmission(row) => row.content.is_some(),
            Self::Edge(row) => row.content.is_some(),
            Self::Topic(row) => row.content.is_some(),
            Self::Point(row) => row.content.is_some(),
            Self::Access(_) | Self::Verdict(_) => false,
        }
    }

    pub fn key(&self) -> RowKey {
        match self {
            Self::Transmission(row) => RowKey::Transmission {
                confirmed_at: row.confirmed_at,
                id: row.id,
            },
            Self::Edge(row) => {
                let mut route = Vec::new();
                encode_route(row.edge.route(), &mut route);
                RowKey::Edge {
                    bucket_start: row.bucket.start(),
                    from: row.edge.from(),
                    to: row.edge.to(),
                    route,
                    topic: row.topic,
                }
            }
            Self::Access(row) => RowKey::Access {
                bucket_start: row.bucket.start(),
                agent: row.agent,
                channel: row.channel,
                read: matches!(row.op, AccessKind::Read),
            },
            Self::Topic(row) => RowKey::Topic(row.topic),
            Self::Point(row) => RowKey::Point(row.index),
            Self::Verdict(row) => RowKey::Verdict {
                transmission: row.transmission,
                revision: row.revision,
            },
        }
    }
}

/// The rows of a projection export: the stored frame's points in frame
/// order, each with its index. `labels` is `Some` exactly when the request
/// includes content, and maps the projection version's topic ids to their
/// labels (the catalog keeps every version's topics, so none is missing).
pub fn projection_rows(
    projection: &Projection,
    labels: Option<&HashMap<TopicId, String>>,
) -> Vec<ExportRow> {
    projection
        .frame()
        .points()
        .zip(0_u32..)
        .map(|(point, index)| {
            let content = labels.map(|labels| LabelContent {
                topic_label: point.topic.and_then(|topic| labels.get(&topic).cloned()),
            });
            ExportRow::Point(PointRow {
                index,
                point,
                content,
            })
        })
        .collect()
}

/// Why verdict rows cannot be built from a transmission and a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictRowsError {
    /// The log is another transmission's.
    OtherTransmission,
}

/// The rows of a verdicts export for one transmission: one per record of
/// its log, oldest first, each with the transmission's route kind and the
/// detector's current call. A transmission whose state takes no verdict
/// has an empty log and so no rows.
pub fn verdict_rows(
    transmission: &Transmission,
    log: &VerdictLog,
) -> Result<Vec<ExportRow>, VerdictRowsError> {
    if log.transmission() != transmission.id {
        return Err(VerdictRowsError::OtherTransmission);
    }
    let Ok(judgeable) = transmission.state.judgeable() else {
        return Ok(Vec::new());
    };
    let call = QualityMatch::from(judgeable);
    let route_kind = RouteKind::from(&transmission.route);
    Ok(log
        .records()
        .iter()
        .zip(1_u32..)
        .filter_map(|(record, position)| {
            let revision = VerdictRevision::new(std::num::NonZeroU32::new(position)?);
            Some(ExportRow::Verdict(VerdictRow {
                transmission: transmission.id,
                route_kind,
                call,
                revision,
                verdict: record.verdict(),
                by: record.by(),
                at: record.at(),
                note: record.note().map(str::to_owned),
            }))
        })
        .collect())
}
