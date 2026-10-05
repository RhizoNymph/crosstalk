//! What an export asks for: one dataset, its scope, a format and whether
//! content columns are included; the permission that needs; and how large
//! an export may be.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::aggregates::filter::TopologyFilter;
use crate::ids::ProjectionId;
use crate::interfaces::l8_surface::summary::TransmissionStateKind;
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

/// The scope of a transmissions export: the shared window and filter, and
/// which transmission states it holds.
///
/// On the wire, `{"window": .., "filter": .., "states": [..]}`, where
/// `states` is left out when it is the default ([`ExportStates::confirmed`]),
/// so a request or header written before `states` existed is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionScope {
    pub window: TimeWindow,
    pub filter: TopologyFilter,
    #[serde(default, skip_serializing_if = "ExportStates::is_confirmed")]
    pub states: ExportStates,
}

impl TransmissionScope {
    /// The confirmed transmissions of `scope`: the default.
    pub fn confirmed(scope: ExportScope) -> Self {
        Self {
            window: scope.window,
            filter: scope.filter,
            states: ExportStates::confirmed(),
        }
    }

    /// The window and the filter, as the other scoped datasets hold them.
    pub fn scope(&self) -> ExportScope {
        ExportScope {
            window: self.window,
            filter: self.filter.clone(),
        }
    }
}

impl From<ExportScope> for TransmissionScope {
    fn from(scope: ExportScope) -> Self {
        Self::confirmed(scope)
    }
}

/// Which transmission states a transmissions export holds: a non-empty set
/// of every [`TransmissionStateKind`] but `Detected` (a detected
/// transmission names no sender and no co-access, so it is not traffic and
/// is never a row).
///
/// The default, [`ExportStates::confirmed`], is `Confirmed`, `Classified`
/// and `Aggregated`. Built only through [`ExportStates::new`]. On the wire,
/// an array of state names in ascending [`TransmissionStateKind`] order,
/// `["suspected", "confirmed", "classified", "aggregated"]`, decoded
/// through the constructor (any order; a repeat, `detected` and an empty
/// array are refused).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<TransmissionStateKind>",
    into = "Vec<TransmissionStateKind>"
)]
pub struct ExportStates(BTreeSet<TransmissionStateKind>);

/// Why a list of states is not an [`ExportStates`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidExportStates {
    Empty,
    Duplicate(TransmissionStateKind),
    /// `Detected` is never exported.
    Detected,
}

impl ExportStates {
    /// The states that carry a confirmation.
    pub const CONFIRMED: [TransmissionStateKind; 3] = [
        TransmissionStateKind::Confirmed,
        TransmissionStateKind::Classified,
        TransmissionStateKind::Aggregated,
    ];

    /// Every state an export can hold.
    pub const ALL: [TransmissionStateKind; 6] = [
        TransmissionStateKind::AwaitingContent,
        TransmissionStateKind::Suspected,
        TransmissionStateKind::Confirmed,
        TransmissionStateKind::Classified,
        TransmissionStateKind::Aggregated,
        TransmissionStateKind::Discarded,
    ];

    pub fn new(states: Vec<TransmissionStateKind>) -> Result<Self, InvalidExportStates> {
        if states.is_empty() {
            return Err(InvalidExportStates::Empty);
        }
        let mut set = BTreeSet::new();
        for state in states {
            if state == TransmissionStateKind::Detected {
                return Err(InvalidExportStates::Detected);
            }
            if !set.insert(state) {
                return Err(InvalidExportStates::Duplicate(state));
            }
        }
        Ok(Self(set))
    }

    /// `Confirmed`, `Classified` and `Aggregated`: the default.
    pub fn confirmed() -> Self {
        Self(Self::CONFIRMED.into_iter().collect())
    }

    /// Every exportable state.
    pub fn all() -> Self {
        Self(Self::ALL.into_iter().collect())
    }

    /// Whether this is the default set.
    pub fn is_confirmed(&self) -> bool {
        *self == Self::confirmed()
    }

    pub fn contains(&self, state: TransmissionStateKind) -> bool {
        self.0.contains(&state)
    }

    /// Whether any state without a confirmation is included.
    pub fn includes_unconfirmed(&self) -> bool {
        self.0.iter().any(|state| !Self::CONFIRMED.contains(state))
    }

    /// In ascending order; never empty.
    pub fn iter(&self) -> impl Iterator<Item = TransmissionStateKind> + '_ {
        self.0.iter().copied()
    }
}

impl Default for ExportStates {
    fn default() -> Self {
        Self::confirmed()
    }
}

impl TryFrom<Vec<TransmissionStateKind>> for ExportStates {
    type Error = Rejected<InvalidExportStates>;

    fn try_from(states: Vec<TransmissionStateKind>) -> Result<Self, Self::Error> {
        Self::new(states).map_err(|error| Rejected::new("export states", error))
    }
}

impl From<ExportStates> for Vec<TransmissionStateKind> {
    fn from(states: ExportStates) -> Self {
        states.0.into_iter().collect()
    }
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
    /// Transmissions in the scope's states (confirmed ones by default) that
    /// the filter admits and whose row time lies in the settled window:
    /// `Confirmed::at` for a confirmed one, `Transmission::opened_at` for an
    /// unconfirmed one ([`super::rows::TransmissionRow::at`]).
    Transmissions(TransmissionScope),
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

    /// The window and filter of a scoped dataset; `None` for a projection
    /// or verdicts.
    pub fn scope(&self) -> Option<ExportScope> {
        match self {
            Self::Transmissions(scope) => Some(scope.scope()),
            Self::Edges(scope) | Self::Accesses(scope) | Self::Topics(scope) => Some(scope.clone()),
            Self::Projection(_) | Self::Verdicts(_) => None,
        }
    }

    /// The states a transmissions export holds; `None` for another
    /// dataset.
    pub fn states(&self) -> Option<&ExportStates> {
        match self {
            Self::Transmissions(scope) => Some(&scope.states),
            Self::Edges(_)
            | Self::Accesses(_)
            | Self::Topics(_)
            | Self::Projection(_)
            | Self::Verdicts(_) => None,
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

/// The formats a gateway writes, in the order its export form offers them
/// (`Present::export_formats`). A format the gateway does not write is
/// refused before anything is read ([`ExportFormats::check`]), so a client
/// that offers only these never meets the refusal.
///
/// Built only through [`ExportFormats::new`]: at least one format, none
/// twice. On the wire, an array of strings, `["jsonl", "parquet"]`, decoded
/// through the constructor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<ExportFormat>", into = "Vec<ExportFormat>")]
pub struct ExportFormats(Vec<ExportFormat>);

/// Why a list of formats is not an [`ExportFormats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidExportFormats {
    Empty,
    Duplicate(ExportFormat),
}

/// An export asked for a format the gateway does not write
/// (`InvalidInput(UnsupportedFormat)` through `QueryError::from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedFormat {
    pub format: ExportFormat,
}

impl TryFrom<Vec<ExportFormat>> for ExportFormats {
    type Error = Rejected<InvalidExportFormats>;

    fn try_from(formats: Vec<ExportFormat>) -> Result<Self, Self::Error> {
        Self::new(formats).map_err(|error| Rejected::new("export formats", error))
    }
}

impl From<ExportFormats> for Vec<ExportFormat> {
    fn from(formats: ExportFormats) -> Self {
        formats.0
    }
}

impl ExportFormats {
    /// The formats in offer order; refuses an empty list and a repeat.
    pub fn new(formats: Vec<ExportFormat>) -> Result<Self, InvalidExportFormats> {
        if formats.is_empty() {
            return Err(InvalidExportFormats::Empty);
        }
        for (index, format) in formats.iter().enumerate() {
            if formats[..index].contains(format) {
                return Err(InvalidExportFormats::Duplicate(*format));
            }
        }
        Ok(Self(formats))
    }

    /// In offer order; never empty.
    pub fn as_slice(&self) -> &[ExportFormat] {
        &self.0
    }

    /// The format a form selects by default: the first offered.
    pub fn first(&self) -> ExportFormat {
        self.0.first().copied().unwrap_or(ExportFormat::Jsonl)
    }

    pub fn offers(&self, format: ExportFormat) -> bool {
        self.0.contains(&format)
    }

    /// `UnsupportedFormat` for a format not offered. `QueryApi::export`
    /// runs it after the permission check and before reading anything.
    pub fn check(&self, format: ExportFormat) -> Result<(), UnsupportedFormat> {
        if self.offers(format) {
            Ok(())
        } else {
            Err(UnsupportedFormat { format })
        }
    }
}

/// One export request.
///
/// Built only through [`ExportRequest::new`], which refuses
/// `include_content` for a dataset with no content columns, and for a
/// transmissions export that includes unconfirmed states (an unconfirmed
/// transmission has no content match to quote), so a request never asks for
/// columns that cannot be delivered. A request, decoded through it.
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
    /// `include_content` for a transmissions export whose states include an
    /// unconfirmed one.
    ContentWithUnconfirmedStates,
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
        if include_content
            && dataset
                .states()
                .is_some_and(ExportStates::includes_unconfirmed)
        {
            return Err(InvalidExportRequest::ContentWithUnconfirmedStates);
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
