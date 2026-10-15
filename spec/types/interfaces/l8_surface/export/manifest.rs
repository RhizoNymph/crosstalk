//! The manifest of an export: a header sent before the rows and a trailer
//! sent after them.
//!
//! The header says what the export is (the request, what its selection
//! resolved to, the watermark, the embedding model, the gateway version and
//! how many rows follow); the trailer says how it ended (the rows sent,
//! their digest, and `Complete` or why it failed). Together they make an
//! export citable (the header reproduces it) and verifiable (the trailer
//! checks it).

use serde::{Deserialize, Serialize};

use crate::aggregates::filter::{TopicVersionSelector, TopologyFilter};
use crate::aggregates::projection::{Fitted, ProjectionSpec};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{ExportId, OperatorId, ProjectionId};
use crate::support::{Blank, NonBlank, TimeWindow, Timestamp, Watermark};
use crate::wire::Rejected;

use super::digest::ExportDigest;
use super::request::{ExportDataset, ExportDatasetKind, ExportRequest};
use super::seal::RowRefused;

/// The gateway build that produced an export (its release version), so a
/// reader knows which row schema and resolution rules it followed. On the
/// wire, a string, decoded as [`GatewayVersion::new`] decodes it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GatewayVersion(NonBlank);

impl GatewayVersion {
    pub fn new(version: &str) -> Result<Self, Blank> {
        NonBlank::new(version).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// The part of `window` before `watermark`: what an export reads. `None`
/// when the whole window is at or after the watermark, and the export holds
/// no rows. The watermark is a bucket boundary, so an aligned window stays
/// aligned.
pub fn settled_window(window: TimeWindow, watermark: Watermark) -> Option<TimeWindow> {
    TimeWindow::new(window.start(), window.end().min(watermark.at())).ok()
}

/// What the request's selection resolved to when the export started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExportBasis {
    /// Transmissions, edges, accesses and topics.
    Scoped {
        /// The version the filter's selector resolved to.
        topic_version: TopicModelVersion,
        /// The request's filter pinned to `topic_version`.
        filter: TopologyFilter,
        /// [`settled_window`] of the request's window.
        settled: Option<TimeWindow>,
    },
    Verdicts {
        settled: Option<TimeWindow>,
    },
    /// The stored projection the rows are read from: its spec (window,
    /// filter pinned to its version, params with the seed, embedding model)
    /// and its fit (when its sample was read, the watermark then, counts).
    Projection {
        projection: ProjectionId,
        spec: ProjectionSpec,
        fitted: Fitted,
    },
}

impl ExportBasis {
    /// The topic-model version every topic in the rows belongs to; `None`
    /// for verdicts, whose rows name no topic.
    pub fn topic_version(&self) -> Option<TopicModelVersion> {
        match self {
            Self::Scoped { topic_version, .. } => Some(*topic_version),
            Self::Verdicts { .. } => None,
            Self::Projection { spec, .. } => Some(spec.topic_version()),
        }
    }
}

/// The fields of an [`ExportHeader`], before checking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ExportHeaderParts {
    pub id: ExportId,
    pub request: ExportRequest,
    /// The caller's operator.
    pub by: OperatorId,
    pub started_at: Timestamp,
    /// L7's watermark (`EdgeStore::watermark`), read before anything else.
    pub watermark: Watermark,
    pub basis: ExportBasis,
    /// The embedding model current when the export started. A projection's
    /// own model is in its spec.
    pub embedding_model: EmbeddingModel,
    pub gateway: GatewayVersion,
    /// How many rows follow, counted before the first was sent.
    pub rows: u64,
}

/// The manifest sent before the rows.
///
/// Built only through [`ExportHeader::new`], which checks that the basis
/// is the request's: the dataset's kind, the request's filter pinned to the
/// resolved version (and to the requested one if it was pinned), the
/// settled window cut at the watermark, the projection named, and for a
/// projection exactly its stored points; and that the watermark is no later
/// than the start.
///
/// A response, never a request. On the wire, its [`ExportHeaderParts`],
/// decoded through [`ExportHeader::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ExportHeaderParts", into = "ExportHeaderParts")]
pub struct ExportHeader {
    parts: ExportHeaderParts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHeader {
    /// The basis is of another dataset than the request's.
    BasisForOtherDataset {
        dataset: ExportDatasetKind,
    },
    /// The request pinned `requested`, and the basis resolved another.
    VersionMismatch {
        requested: TopicModelVersion,
        resolved: TopicModelVersion,
    },
    /// The basis filter is not the request's filter pinned to the resolved
    /// version.
    FilterNotPinned,
    /// The settled window is not the request's window cut at the watermark.
    SettledWindow,
    /// The basis names another projection than the request.
    OtherProjection,
    /// A projection export plans other than its stored point count.
    PlannedRows {
        planned: u64,
        points: u32,
    },
    WatermarkAfterStart,
}

impl TryFrom<ExportHeaderParts> for ExportHeader {
    type Error = Rejected<InvalidHeader>;

    fn try_from(parts: ExportHeaderParts) -> Result<Self, Self::Error> {
        Self::new(parts).map_err(|error| Rejected::new("export header", error))
    }
}

impl From<ExportHeader> for ExportHeaderParts {
    fn from(header: ExportHeader) -> Self {
        header.parts
    }
}

impl ExportHeader {
    pub fn new(parts: ExportHeaderParts) -> Result<Self, InvalidHeader> {
        if parts.watermark.at() > parts.started_at {
            return Err(InvalidHeader::WatermarkAfterStart);
        }
        let dataset = parts.request.dataset();
        let other = InvalidHeader::BasisForOtherDataset {
            dataset: dataset.kind(),
        };
        match (&parts.basis, dataset) {
            (
                ExportBasis::Scoped {
                    topic_version,
                    filter,
                    settled,
                },
                ExportDataset::Transmissions(scope)
                | ExportDataset::Edges(scope)
                | ExportDataset::Accesses(scope)
                | ExportDataset::Topics(scope),
            ) => {
                if let TopicVersionSelector::Pinned(requested) = scope.filter.topic_version
                    && requested != *topic_version
                {
                    return Err(InvalidHeader::VersionMismatch {
                        requested,
                        resolved: *topic_version,
                    });
                }
                if *filter != scope.filter.clone().pinned(*topic_version) {
                    return Err(InvalidHeader::FilterNotPinned);
                }
                if *settled != settled_window(scope.window, parts.watermark) {
                    return Err(InvalidHeader::SettledWindow);
                }
            }
            (ExportBasis::Verdicts { settled }, ExportDataset::Verdicts(window)) => {
                if *settled != settled_window(*window, parts.watermark) {
                    return Err(InvalidHeader::SettledWindow);
                }
            }
            (
                ExportBasis::Projection {
                    projection, fitted, ..
                },
                ExportDataset::Projection(requested),
            ) => {
                if projection != requested {
                    return Err(InvalidHeader::OtherProjection);
                }
                if parts.rows != u64::from(fitted.points) {
                    return Err(InvalidHeader::PlannedRows {
                        planned: parts.rows,
                        points: fitted.points,
                    });
                }
            }
            (ExportBasis::Scoped { .. }, _)
            | (ExportBasis::Verdicts { .. }, _)
            | (ExportBasis::Projection { .. }, _) => return Err(other),
        }
        Ok(Self { parts })
    }

    pub fn id(&self) -> ExportId {
        self.parts.id
    }

    pub fn request(&self) -> &ExportRequest {
        &self.parts.request
    }

    pub fn by(&self) -> OperatorId {
        self.parts.by
    }

    pub fn started_at(&self) -> Timestamp {
        self.parts.started_at
    }

    /// The watermark read before anything else: every row comes from data
    /// settled before it (except a projection's, fixed at its fit, whose
    /// own watermark is in the basis).
    pub fn watermark(&self) -> Watermark {
        self.parts.watermark
    }

    pub fn basis(&self) -> &ExportBasis {
        &self.parts.basis
    }

    pub fn embedding_model(&self) -> &EmbeddingModel {
        &self.parts.embedding_model
    }

    pub fn gateway(&self) -> &GatewayVersion {
        &self.parts.gateway
    }

    /// The number of rows the export holds when it completes.
    pub fn rows(&self) -> u64 {
        self.parts.rows
    }

    pub fn parts(&self) -> &ExportHeaderParts {
        &self.parts
    }
}

/// How an export ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExportEnd {
    /// Every row the header planned was sent.
    Complete,
    /// The export stopped early or went wrong. The rows before the failure
    /// were sent and are counted and digested, but the export is not
    /// complete.
    Failed(ExportFailure),
}

/// Why an export that had started did not complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExportFailure {
    /// A store failed mid-stream. Retrying the export may succeed.
    Store { reason: String },
    /// Retention dropped the export's topic version while it streamed.
    VersionNotRetained { version: TopicModelVersion },
    /// The source ran out after a different number of rows than it planned.
    CountMismatch { planned: u64, produced: u64 },
    /// The source produced a row the sealer refused; `index` counts the rows
    /// accepted before it.
    InvalidRow { index: u64, refused: RowRefused },
}

/// A failure the row source can report. The sealer adds the others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceFailure {
    Store { reason: String },
    VersionNotRetained { version: TopicModelVersion },
}

impl From<SourceFailure> for ExportFailure {
    fn from(failure: SourceFailure) -> Self {
        match failure {
            SourceFailure::Store { reason } => Self::Store { reason },
            SourceFailure::VersionNotRetained { version } => Self::VersionNotRetained { version },
        }
    }
}

/// The manifest sent after the rows. Built only by the sealer
/// ([`super::ExportSealer`]), so `rows` and `digest` are those of the rows
/// it accepted and `Complete` means it accepted every planned row.
///
/// A response, never a request. Decoding cannot rerun the sealer; a reader
/// checks a decoded trailer against the rows it received
/// ([`super::verify_export`]). It does check what the sealer guarantees
/// about the trailer alone ([`InvalidTrailer`]): a count mismatch records
/// the rows counted as produced, and a refused row is the first refusal,
/// after exactly the rows counted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawExportTrailer")]
pub struct ExportTrailer {
    pub(super) export: ExportId,
    pub(super) rows: u64,
    pub(super) digest: ExportDigest,
    pub(super) end: ExportEnd,
}

/// A decoded trailer the sealer could not have built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTrailer {
    /// A `CountMismatch` whose `produced` is not the trailer's `rows`, or
    /// that matches its plan.
    CountMismatch {
        rows: u64,
        planned: u64,
        produced: u64,
    },
    /// An `InvalidRow` whose `index` is not the trailer's `rows`: the sealer
    /// accepts no row after a refusal.
    RefusalIndex { rows: u64, index: u64 },
    /// An `InvalidRow` that is not a first refusal: `AfterRefusal`, or
    /// `BeyondPlan` after other than `planned` rows.
    NotFirstRefusal,
}

/// [`ExportTrailer`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExportTrailer {
    export: ExportId,
    rows: u64,
    digest: ExportDigest,
    end: ExportEnd,
}

impl TryFrom<RawExportTrailer> for ExportTrailer {
    type Error = Rejected<InvalidTrailer>;

    fn try_from(raw: RawExportTrailer) -> Result<Self, Self::Error> {
        let rows = raw.rows;
        let check = match &raw.end {
            ExportEnd::Complete
            | ExportEnd::Failed(
                ExportFailure::Store { .. } | ExportFailure::VersionNotRetained { .. },
            ) => Ok(()),
            &ExportEnd::Failed(ExportFailure::CountMismatch { planned, produced }) => {
                if produced == rows && planned != produced {
                    Ok(())
                } else {
                    Err(InvalidTrailer::CountMismatch {
                        rows,
                        planned,
                        produced,
                    })
                }
            }
            ExportEnd::Failed(ExportFailure::InvalidRow { index, refused }) => match refused {
                _ if *index != rows => Err(InvalidTrailer::RefusalIndex {
                    rows,
                    index: *index,
                }),
                RowRefused::AfterRefusal => Err(InvalidTrailer::NotFirstRefusal),
                RowRefused::BeyondPlan { planned } if *planned != rows => {
                    Err(InvalidTrailer::NotFirstRefusal)
                }
                RowRefused::OtherDataset { .. }
                | RowRefused::ContentMismatch { .. }
                | RowRefused::OutOfOrder
                | RowRefused::BeyondPlan { .. } => Ok(()),
            },
        };
        check.map_err(|error| Rejected::new("export trailer", error))?;
        Ok(Self {
            export: raw.export,
            rows,
            digest: raw.digest,
            end: raw.end,
        })
    }
}

impl ExportTrailer {
    pub fn export(&self) -> ExportId {
        self.export
    }

    /// Rows sent.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn digest(&self) -> ExportDigest {
        self.digest
    }

    pub fn end(&self) -> &ExportEnd {
        &self.end
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.end, ExportEnd::Complete)
    }
}
