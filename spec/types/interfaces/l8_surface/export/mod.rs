//! Export: one dataset streamed out with a manifest, so it can be cited and
//! checked.
//!
//! ```text
//! export(caller, request)
//!   1. permission (request.required_permission()) ── missing ─▶ Forbidden, nothing read
//!   2. W = EdgeStore::watermark()
//!   3. ExportSource::plan(request, W): resolve and pin the topic version, cut the
//!      window at W, capture agent and channel resolution and verdicts, count rows
//!   4. ExportLimits::check(rows) ── over ─▶ Conflict(ExportTooLarge), nothing sent
//!   5. ExportHeader::new(..) ─▶ audit Started ─▶ return Export { header, rows }
//! wire: header ─▶ row ─▶ row ─▶ … ─▶ trailer { rows, digest, Complete | Failed(why) }
//! ```
//!
//! **Datasets.** Transmissions, edge buckets, access buckets and topics take
//! an [`ExportScope`]: the window and the shared `TopologyFilter`, applied
//! as the linked views apply it, under one topic version resolved when the
//! export starts and pinned for all of it. A projection export reads the
//! stored frame. A verdicts export takes a window over
//! `Transmission::opened_at`, as `detection_quality` does. The rows of each
//! are in [`rows`].
//!
//! **Settled data only.** Every scoped and verdicts export reads only the
//! part of its window before the watermark read at its start
//! ([`settled_window`]), which the header records; buckets, confirmations
//! and detector calls there no longer change. Agent and channel resolution
//! and the copy of current verdicts are captured once at the start too, so
//! the rows are a function of the request, the watermark and that captured
//! state: a re-run with the same request and watermark and no merge,
//! unmerge, promotion or verdict in between yields the same rows in the
//! same order, the same count and the same digest. A projection's rows are
//! its stored frame, the same on every export until the frame expires.
//!
//! **Manifest.** A header before the rows ([`ExportHeader`]: the request,
//! the resolved basis with its topic version, the watermark, the embedding
//! model, the gateway version and the planned row count) and a trailer
//! after them ([`ExportTrailer`]: the rows sent, their digest, and
//! `Complete` or the failure). An export holds one dataset, so its row
//! count is that dataset's. The header is known before any row is read, so
//! it is sent first; the count and digest are known only at the end, so
//! they are in the trailer. A store failure mid-stream still sends a
//! trailer, recording it. Only a stream cut off before its trailer has
//! none, so a reader that finds no trailer, a failed one, or counts or a
//! digest that disagree knows the export is incomplete ([`verify_export`]).
//!
//! **Format.** The format changes the bytes, not the rows, their order,
//! their count or their digest, which is defined over a canonical encoding
//! of the rows ([`digest`]). In JSONL the header is the first line, each
//! row a line, the trailer the last line. In Parquet the rows fill row
//! groups in export order and the header and trailer go in the footer's
//! key-value metadata, which is written on failure too; a file cut off
//! before its footer cannot be read at all.
//!
//! **Permission and audit.** An export needs View, or Content when it
//! includes content or reads a projection
//! ([`ExportRequest::required_permission`]), checked before anything is
//! read. Every export is audited ([`record`]).
//!
//! **Not paged.** An export is a stream, not a list: it has no cursor and
//! cannot be resumed. A client that loses one runs it again.

pub mod digest;
pub mod manifest;
pub mod record;
pub mod request;
pub mod rows;
pub mod seal;
pub mod stream;

pub use digest::{ExportDigest, ROW_DIGEST_CONTEXT, RowHasher};
pub use manifest::{
    ExportBasis, ExportEnd, ExportFailure, ExportHeader, ExportHeaderParts, ExportTrailer,
    GatewayVersion, InvalidHeader, SourceFailure, settled_window,
};
pub use record::{ExportEvent, ExportRecord, InvalidExportRecord};
pub use request::{
    ExportDataset, ExportDatasetKind, ExportFormat, ExportLimits, ExportRequest, ExportScope,
    InvalidExportRequest,
};
pub use rows::{ExportRow, RowKey};
pub use seal::{ExportSealer, Incomplete, RowRefused, verify_export};
pub use stream::{
    Export, ExportPlan, ExportPlanError, ExportSource, ExportStep, ExportStream, RowSource,
    SealedRows,
};
