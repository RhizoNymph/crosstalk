//! What an export asks for: one dataset, its scope, a format and whether
//! content columns are included; the permission that needs; and how large
//! an export may be.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::aggregates::filter::TopologyFilter;
use crate::ids::ProjectionId;
use crate::interfaces::l8_surface::{ConflictKind, Permission};
use crate::support::TimeWindow;
use crate::wire::{Rejected, WireRequest};

/// The window and the shared filter of a scoped dataset. The filter's
/// [`TopicVersionSelector`](crate::aggregates::filter::TopicVersionSelector)
/// is resolved once, when the export starts, and the header records the
/// filter pinned to that version for the whole export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ExportScope {
    pub window: TimeWindow,
    pub filter: TopologyFilter,
}

/// The one dataset an export holds, with what selects its rows.
///
/// Each dataset carries only the selection it can apply, so a request that
/// would carry an ignored selection cannot be built: a stored projection's
/// points are fixed by its own spec, and verdicts exist on suspected and
/// discarded transmissions, which have no sender or topic for a
/// [`TopologyFilter`] to test, so they are selected by window alone (by
/// `Transmission::opened_at`, as `detection_quality` does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExportDataset {
    /// Confirmed transmissions whose `Confirmed::at` lies in the settled
    /// window and that the filter admits.
    Transmissions(ExportScope),
    /// Edge buckets in the settled window, resolved, filtered and summed as
    /// `topology` counts them, one row per bucket. The window must be
    /// aligned to buckets.
    Edges(ExportScope),
    /// Access buckets in the settled window, resolved and kept by
    /// `TopologyFilter::admits_access`, one row per bucket. The window must
    /// be aligned to buckets.
    Accesses(ExportScope),
    /// The topics of the resolved version with the admitted transmissions
    /// in the settled window counted per topic.
    Topics(ExportScope),
    /// A ready projection's stored frame, point by point.
    Projection(ProjectionId),
    /// Every verdict record of the judgeable transmissions opened in the
    /// settled window.
    Verdicts(TimeWindow),
}

/// Which dataset, without its selection. Rows and headers are checked
/// against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportDatasetKind {
    Transmissions,
    Edges,
    Accesses,
    Topics,
    Projection,
    Verdicts,
}

impl ExportDatasetKind {
    pub const ALL: [Self; 6] = [
        Self::Transmissions,
        Self::Edges,
        Self::Accesses,
        Self::Topics,
        Self::Projection,
        Self::Verdicts,
    ];

    /// Whether the dataset's rows have content columns, which
    /// `include_content` adds: matched text and topic labels for
    /// transmissions, the topic label for edges and projection points, the
    /// label and terms for topics. Accesses and verdicts hold none (a
    /// verdict's note is operator text, which `verdicts` returns with View).
    pub fn has_content_columns(self) -> bool {
        match self {
            Self::Transmissions | Self::Edges | Self::Topics | Self::Projection => true,
            Self::Accesses | Self::Verdicts => false,
        }
    }

    /// Whether every row is derived from message text whatever
    /// `include_content` says: a projection's coordinates are a layout of
    /// embeddings, so reading one needs Content, as `projection` does.
    pub fn is_content_only(self) -> bool {
        match self {
            Self::Projection => true,
            Self::Transmissions | Self::Edges | Self::Accesses | Self::Topics | Self::Verdicts => {
                false
            }
        }
    }

    /// The row tag in the canonical row encoding
    /// ([`super::digest`]): 1 to 6, in [`ExportDatasetKind::ALL`] order.
    pub fn code(self) -> u8 {
        match self {
            Self::Transmissions => 1,
            Self::Edges => 2,
            Self::Accesses => 3,
            Self::Topics => 4,
            Self::Projection => 5,
            Self::Verdicts => 6,
        }
    }
}

impl ExportDataset {
    pub fn kind(&self) -> ExportDatasetKind {
        match self {
            Self::Transmissions(_) => ExportDatasetKind::Transmissions,
            Self::Edges(_) => ExportDatasetKind::Edges,
            Self::Accesses(_) => ExportDatasetKind::Accesses,
            Self::Topics(_) => ExportDatasetKind::Topics,
            Self::Projection(_) => ExportDatasetKind::Projection,
            Self::Verdicts(_) => ExportDatasetKind::Verdicts,
        }
    }

    /// The scope of a scoped dataset; `None` for a projection or verdicts.
    pub fn scope(&self) -> Option<&ExportScope> {
        match self {
            Self::Transmissions(scope)
            | Self::Edges(scope)
            | Self::Accesses(scope)
            | Self::Topics(scope) => Some(scope),
            Self::Projection(_) | Self::Verdicts(_) => None,
        }
    }
}

/// How rows are encoded on the wire. The logical rows, their order, the
/// row count and the digest are the same in either
/// ([`super::digest`]); only the bytes differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormat {
    /// One JSON object per line: the header first, then one per row, then
    /// the trailer.
    Jsonl,
    /// One Parquet file, rows in export order; the header and trailer are
    /// in the footer's key-value metadata.
    Parquet,
}

/// One export request.
///
/// Built only through [`ExportRequest::new`], which refuses
/// `include_content` for a dataset with no content columns, so a request
/// never asks for columns that cannot be delivered. A request, decoded
/// through it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawExportRequest")]
pub struct ExportRequest {
    dataset: ExportDataset,
    format: ExportFormat,
    include_content: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidExportRequest {
    /// `include_content` for accesses or verdicts.
    NoContentColumns { dataset: ExportDatasetKind },
}

/// [`ExportRequest`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExportRequest {
    dataset: ExportDataset,
    format: ExportFormat,
    include_content: bool,
}

impl TryFrom<RawExportRequest> for ExportRequest {
    type Error = Rejected<InvalidExportRequest>;

    fn try_from(raw: RawExportRequest) -> Result<Self, Self::Error> {
        Self::new(raw.dataset, raw.format, raw.include_content)
            .map_err(|error| Rejected::new("export request", error))
    }
}

/// A client chooses every field of an export request.
impl WireRequest for ExportRequest {}

impl ExportRequest {
    pub fn new(
        dataset: ExportDataset,
        format: ExportFormat,
        include_content: bool,
    ) -> Result<Self, InvalidExportRequest> {
        let kind = dataset.kind();
        if include_content && !kind.has_content_columns() {
            return Err(InvalidExportRequest::NoContentColumns { dataset: kind });
        }
        Ok(Self {
            dataset,
            format,
            include_content,
        })
    }

    pub fn dataset(&self) -> &ExportDataset {
        &self.dataset
    }

    pub fn format(&self) -> ExportFormat {
        self.format
    }

    pub fn include_content(&self) -> bool {
        self.include_content
    }

    /// The one permission the caller must hold, checked before anything is
    /// read: Content when the request includes content or names a
    /// projection, View otherwise. A caller without it gets
    /// `Forbidden { missing }` and nothing streams.
    pub fn required_permission(&self) -> Permission {
        if self.include_content || self.dataset.kind().is_content_only() {
            Permission::Content
        } else {
            Permission::View
        }
    }
}

/// How large an export may be (config `export.max_rows`). The surface
/// counts the rows an export will hold before it streams any, so an export
/// over the limit is refused whole rather than cut short.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportLimits {
    max_rows: NonZeroU64,
}

impl ExportLimits {
    pub const DEFAULT_MAX_ROWS: u64 = 10_000_000;

    pub fn new(max_rows: NonZeroU64) -> Self {
        Self { max_rows }
    }

    pub fn max_rows(self) -> NonZeroU64 {
        self.max_rows
    }

    /// `Conflict(ExportTooLarge)` when `rows` exceeds the limit. A smaller
    /// window, a narrower filter or a higher limit makes the same request
    /// pass, so it is a conflict with the current data, not invalid input.
    pub fn check(self, rows: u64) -> Result<(), ConflictKind> {
        let limit = self.max_rows.get();
        if rows > limit {
            return Err(ConflictKind::ExportTooLarge { rows, limit });
        }
        Ok(())
    }
}

impl Default for ExportLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroU64::new(Self::DEFAULT_MAX_ROWS).unwrap_or(NonZeroU64::MIN),
        }
    }
}
