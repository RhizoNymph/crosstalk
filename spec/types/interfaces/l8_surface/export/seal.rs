//! Sealing an export: every row passes through an [`ExportSealer`], which
//! checks it, counts it and folds it into the digest, and only the sealer
//! builds the trailer. Reading an export back goes through the same checks
//! ([`verify_export`]).
//!
//! The sealer is consumed by [`ExportSealer::finish`] or
//! [`ExportSealer::fail`], so an export has exactly one trailer. Once it
//! refuses a row it stays refused, and its trailer is `Failed` whatever
//! follows, so a malformed export can never be sealed as complete.

use crate::ids::ExportId;

use super::digest::{ExportDigest, RowHasher, hash_row};
use super::manifest::{ExportEnd, ExportFailure, ExportHeader, ExportTrailer, SourceFailure};
use super::request::ExportDatasetKind;
use super::rows::{ExportRow, RowKey};

/// Why the sealer refused a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowRefused {
    /// A row of another dataset than the header's.
    OtherDataset {
        expected: ExportDatasetKind,
        got: ExportDatasetKind,
    },
    /// Content columns present when the request did not include content,
    /// or missing when it did.
    ContentMismatch { requested: bool },
    /// A key not after the previous row's: out of order, or a repeat.
    OutOfOrder,
    /// More rows than the header planned.
    BeyondPlan { planned: u64 },
    /// The sealer already refused a row.
    AfterRefusal,
}

/// Checks, counts and digests the rows of one export, and builds its
/// trailer.
#[derive(Debug)]
pub struct ExportSealer<H> {
    export: ExportId,
    dataset: ExportDatasetKind,
    content: bool,
    planned: u64,
    rows: u64,
    last: Option<RowKey>,
    refused: Option<(u64, RowRefused)>,
    hasher: H,
    scratch: Vec<u8>,
}

impl<H: RowHasher> ExportSealer<H> {
    /// A sealer for the export `header` describes. `hasher` is fresh.
    pub fn new(header: &ExportHeader, hasher: H) -> Self {
        Self {
            export: header.id(),
            dataset: header.request().dataset().kind(),
            content: header.request().include_content(),
            planned: header.rows(),
            rows: 0,
            last: None,
            refused: None,
            hasher,
            scratch: Vec::new(),
        }
    }

    /// Accept `row` as the next row: of the header's dataset, with content
    /// columns exactly when the request includes content, after the
    /// previous row in key order, and within the planned count. An accepted
    /// row is counted and digested; a refused one is neither, and every
    /// later row is refused too.
    pub fn push(&mut self, row: &ExportRow) -> Result<(), RowRefused> {
        if self.refused.is_some() {
            return Err(RowRefused::AfterRefusal);
        }
        let key = row.key();
        let check = if row.kind() != self.dataset {
            Err(RowRefused::OtherDataset {
                expected: self.dataset,
                got: row.kind(),
            })
        } else if row.has_content() != self.content {
            Err(RowRefused::ContentMismatch {
                requested: self.content,
            })
        } else if self.last.as_ref().is_some_and(|last| key <= *last) {
            Err(RowRefused::OutOfOrder)
        } else if self.rows >= self.planned {
            Err(RowRefused::BeyondPlan {
                planned: self.planned,
            })
        } else {
            Ok(())
        };
        if let Err(refused) = check {
            self.refused = Some((self.rows, refused.clone()));
            return Err(refused);
        }
        hash_row(&mut self.hasher, row, &mut self.scratch);
        self.rows += 1;
        self.last = Some(key);
        Ok(())
    }

    /// Rows accepted so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The source has no more rows. `Complete` when every planned row was
    /// accepted and none refused; otherwise `Failed`, with the refusal or
    /// the count mismatch.
    pub fn finish(self) -> ExportTrailer {
        let end = match &self.refused {
            Some((index, refused)) => ExportEnd::Failed(ExportFailure::InvalidRow {
                index: *index,
                refused: refused.clone(),
            }),
            None if self.rows != self.planned => ExportEnd::Failed(ExportFailure::CountMismatch {
                planned: self.planned,
                produced: self.rows,
            }),
            None => ExportEnd::Complete,
        };
        self.seal(end)
    }

    /// The source failed. The trailer records the failure (or an earlier
    /// refusal, which came first) with the rows accepted before it.
    pub fn fail(self, failure: SourceFailure) -> ExportTrailer {
        let failure = match &self.refused {
            Some((index, refused)) => ExportFailure::InvalidRow {
                index: *index,
                refused: refused.clone(),
            },
            None => failure.into(),
        };
        self.seal(ExportEnd::Failed(failure))
    }

    fn seal(self, end: ExportEnd) -> ExportTrailer {
        ExportTrailer {
            export: self.export,
            rows: self.rows,
            digest: ExportDigest::from_digest(self.hasher.finalize()),
            end,
        }
    }
}

/// Why a received export is not a complete copy of what was sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incomplete {
    /// The stream ended without a trailer: it was cut off.
    NoTrailer,
    /// The trailer belongs to another export than the header.
    OtherExport,
    /// The trailer says the export failed.
    Failed(ExportFailure),
    /// A received row fails the sealer's checks.
    InvalidRow { index: u64, refused: RowRefused },
    /// The header planned, the trailer counted and the reader received
    /// different numbers of rows.
    RowCount {
        planned: u64,
        sent: u64,
        received: u64,
    },
    /// The received rows do not hash to the trailer's digest.
    DigestMismatch,
}

/// Whether `rows`, received between `header` and `trailer` (`None` when
/// the stream ended without one), are the complete export: a trailer for
/// the same export that says `Complete`, rows that pass the sealer's checks,
/// the same count in the header, the trailer and the rows received, and
/// the trailer's digest. Anything less is [`Incomplete`], so a truncated
/// export never verifies.
pub fn verify_export<'a, H: RowHasher>(
    header: &ExportHeader,
    rows: impl IntoIterator<Item = &'a ExportRow>,
    trailer: Option<&ExportTrailer>,
    hasher: H,
) -> Result<(), Incomplete> {
    let trailer = trailer.ok_or(Incomplete::NoTrailer)?;
    if trailer.export() != header.id() {
        return Err(Incomplete::OtherExport);
    }
    if let ExportEnd::Failed(failure) = trailer.end() {
        return Err(Incomplete::Failed(failure.clone()));
    }
    let mut sealer = ExportSealer::new(header, hasher);
    let mut received: u64 = 0;
    let mut beyond_plan = false;
    for row in rows {
        received += 1;
        if beyond_plan {
            continue;
        }
        match sealer.push(row) {
            Ok(()) => {}
            Err(RowRefused::BeyondPlan { .. }) => beyond_plan = true,
            Err(refused) => {
                return Err(Incomplete::InvalidRow {
                    index: received - 1,
                    refused,
                });
            }
        }
    }
    if received != header.rows() || trailer.rows() != header.rows() {
        return Err(Incomplete::RowCount {
            planned: header.rows(),
            sent: trailer.rows(),
            received,
        });
    }
    let recomputed = sealer.finish();
    if recomputed.digest() != trailer.digest() {
        return Err(Incomplete::DigestMismatch);
    }
    Ok(())
}
