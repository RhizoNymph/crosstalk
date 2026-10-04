//! The export stream, and the source the surface reads rows from.
//!
//! [`ExportStream::next`] takes the stream by value and gives it back with
//! each row, and gives back none with the trailer. So after the header, a
//! stream yields rows until it yields exactly one trailer, and then there is
//! nothing left to call: a trailer always follows the rows, and nothing
//! follows the trailer. The only way to stop earlier is to drop the stream
//! (the client went away), and then no trailer is ever sent, which a reader
//! sees as truncation ([`super::verify_export`]).
//!
//! The traits name no runtime; implementations run them on whatever
//! executor serves HTTP. Their futures are `Send`, and so are the rows a
//! source yields from, so the stream can move between worker threads.

use crate::aggregates::filter::VersionUnavailable;
use crate::aggregates::topic::EmbeddingModel;
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::TopicId;
use crate::interfaces::l6_analysis::ProjectionStoreError;
use crate::support::Watermark;

use super::digest::RowHasher;
use super::manifest::{ExportBasis, ExportHeader, ExportTrailer, SourceFailure};
use super::request::ExportRequest;
use super::rows::ExportRow;
use super::seal::ExportSealer;

/// An export as `QueryApi::export` returns it: the header, available before
/// any row, and the stream of rows that ends with the trailer.
#[derive(Debug)]
pub struct Export<S> {
    pub header: ExportHeader,
    pub rows: S,
}

/// One step of an export stream.
#[derive(Debug)]
pub enum ExportStep<S> {
    /// The next row, and the rest of the stream.
    Row(ExportRow, S),
    /// The trailer. The stream is spent.
    End(ExportTrailer),
}

/// The rows of one export, ending with its trailer.
pub trait ExportStream: Sized {
    fn next(self) -> impl Future<Output = ExportStep<Self>> + Send;
}

/// Where the rows of a started export come from: the stores, read under the
/// resolution captured when the export was planned.
pub trait RowSource {
    /// The next row in key order, `None` after the last. A failure ends the
    /// export; the sealer records it in the trailer.
    fn next(&mut self) -> impl Future<Output = Result<Option<ExportRow>, SourceFailure>> + Send;
}

/// A [`RowSource`] whose every row passes through an [`ExportSealer`]: the
/// stream the surface returns.
///
/// - The source yields a row the sealer accepts: the row is sent.
/// - The sealer refuses a row: the row is not sent, and the stream ends
///   with a `Failed(InvalidRow)` trailer.
/// - The source runs out: the trailer is `Complete` if every planned row
///   was sent, `Failed(CountMismatch)` otherwise.
/// - The source fails: the stream ends with a trailer recording the
///   failure, after the rows already sent.
#[derive(Debug)]
pub struct SealedRows<R, H> {
    source: R,
    sealer: ExportSealer<H>,
}

impl<R: RowSource, H: RowHasher> SealedRows<R, H> {
    pub fn new(header: &ExportHeader, source: R, hasher: H) -> Self {
        Self {
            source,
            sealer: ExportSealer::new(header, hasher),
        }
    }
}

impl<R: RowSource + Send, H: RowHasher + Send> ExportStream for SealedRows<R, H> {
    async fn next(mut self) -> ExportStep<Self> {
        match self.source.next().await {
            Ok(Some(row)) => match self.sealer.push(&row) {
                Ok(()) => ExportStep::Row(row, self),
                Err(_) => ExportStep::End(self.sealer.finish()),
            },
            Ok(None) => ExportStep::End(self.sealer.finish()),
            Err(failure) => ExportStep::End(self.sealer.fail(failure)),
        }
    }
}

/// What the stores resolved and counted for a request.
#[derive(Debug)]
pub struct ExportPlan<R> {
    pub basis: ExportBasis,
    pub embedding_model: EmbeddingModel,
    /// The rows `source` will yield.
    pub rows: u64,
    pub source: R,
}

/// The stores behind an export: L5 (transmissions, verdicts), L6 (topics,
/// projections), L7 (edge and access buckets). One implementation reads
/// them all, in one database snapshot.
pub trait ExportSource {
    type Rows: RowSource + Send + 'static;

    /// Resolve the request against `watermark` (read first, from L7):
    /// resolve and pin the filter's topic version as every linked view does
    /// (`TopicVersionSelector::resolve`, `TopologyFilter::topics_outside`),
    /// cut the window at the watermark, capture the agent and channel
    /// resolution and the current verdicts for the whole export, read a
    /// projection's stored frame, and count the rows. Nothing is sent yet.
    fn plan(
        &self,
        request: &ExportRequest,
        watermark: Watermark,
    ) -> impl Future<Output = Result<ExportPlan<Self::Rows>, ExportPlanError>> + Send;
}

/// Why an export could not be planned. Nothing was sent; `QueryApi::export`
/// returns it as a `QueryError` (`query_errors.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportPlanError {
    Store {
        reason: String,
    },
    /// The filter's pinned version is unknown, fitting, never activated or
    /// no longer retained.
    Version(VersionUnavailable),
    /// The filter lists topics outside the resolved version.
    TopicsNotInVersion {
        version: TopicModelVersion,
        topics: Vec<TopicId>,
    },
    /// An edge or access export over a window not aligned to buckets.
    UnalignedWindow,
    /// The projection is unknown, not ready, failed or expired.
    Projection(ProjectionStoreError),
}
