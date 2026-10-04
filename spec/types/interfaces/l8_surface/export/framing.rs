//! How an export's header, rows and trailer are framed in each format.
//!
//! **JSONL.** The body is UTF-8 lines, each one [`ExportLine`] as compact
//! JSON (`serde_json::to_string`, no whitespace around or inside it)
//! followed by one `\n`:
//!
//! ```text
//! {"type":"header","data":<ExportHeader>}\n                      line 1, exactly once
//! {"type":"row","data":{"type":"<dataset row>","data":<row>}}\n  one per row, in export order
//! {"type":"trailer","data":<ExportTrailer>}\n                    last, at most once
//! ```
//!
//! The header line is first and only first; every line between it and the
//! trailer is a row; nothing follows the trailer line, and the writer ends
//! it with `\n` like every other line. A body whose last line has no `\n`
//! was cut off mid-line: the reader drops that fragment, so an export cut
//! off inside its trailer reads as one with no trailer. Every way a body
//! can end early therefore reads as a missing trailer, never as a
//! different complete export, and [`verify_export`] refuses it.
//! [`read_jsonl`] is the reference reader.
//!
//! **Parquet.** The rows fill row groups in export order, and the footer's
//! key-value metadata holds two entries, written on failure too:
//! [`PARQUET_HEADER_KEY`] with the header's JSON and
//! [`PARQUET_TRAILER_KEY`] with the trailer's JSON, each the compact JSON
//! of the value alone (the `data` of the matching JSONL line, without the
//! line's tag). A file cut off before its footer cannot be opened, so a
//! Parquet export is never read without its trailer.
//!
//! The digest is over the canonical row encoding, not these bytes
//! ([`super::digest`]), so the same rows framed either way carry the same
//! digest.
//!
//! [`verify_export`]: super::verify_export

use serde::{Deserialize, Serialize};

use crate::wire::DecodeError;

use super::digest::RowHasher;
use super::manifest::{ExportHeader, ExportTrailer};
use super::rows::ExportRow;
use super::seal::{Incomplete, verify_export};

/// The Parquet footer key whose value is the [`ExportHeader`]'s JSON.
pub const PARQUET_HEADER_KEY: &str = "crosstalk.export.header";

/// The Parquet footer key whose value is the [`ExportTrailer`]'s JSON.
pub const PARQUET_TRAILER_KEY: &str = "crosstalk.export.trailer";

/// One line of a JSONL export. A response, never a request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExportLine {
    /// Boxed: the header is the largest line.
    Header(Box<ExportHeader>),
    Row(ExportRow),
    Trailer(ExportTrailer),
}

/// A JSONL export as read: the header, the rows in the order received, and
/// the trailer if the body reached it. Check it with [`JsonlExport::verify`].
#[derive(Debug, Clone, PartialEq)]
pub struct JsonlExport {
    pub header: ExportHeader,
    pub rows: Vec<ExportRow>,
    pub trailer: Option<ExportTrailer>,
}

impl JsonlExport {
    /// [`verify_export`] over what was read.
    pub fn verify<H: RowHasher>(&self, hasher: H) -> Result<(), Incomplete> {
        verify_export(&self.header, &self.rows, self.trailer.as_ref(), hasher)
    }
}

/// A body that is not JSONL framing of one export, at a line (from 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonlError {
    pub line: u64,
    pub kind: JsonlErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonlErrorKind {
    /// No complete line: not even a header arrived.
    NoHeader,
    /// The line does not decode as an [`ExportLine`].
    Undecodable(DecodeError),
    /// The first line is a row or a trailer.
    HeaderNotFirst,
    /// A header line after the first.
    SecondHeader,
    /// A line after the trailer.
    AfterTrailer,
}

/// The reference JSONL reader: splits `body` at each `\n`, drops a last
/// fragment that has none (cut off), and checks the framing (module docs).
/// It reads the whole body; an implementation streams the same checks.
pub fn read_jsonl(body: &[u8]) -> Result<JsonlExport, JsonlError> {
    let complete = match body.iter().rposition(|&byte| byte == b'\n') {
        Some(last) => &body[..last],
        None => {
            return Err(JsonlError {
                line: 1,
                kind: JsonlErrorKind::NoHeader,
            });
        }
    };
    let mut lines = complete.split(|&byte| byte == b'\n').zip(1_u64..);
    let fail = |line, kind| Err(JsonlError { line, kind });
    let decode = |text: &[u8], line| {
        serde_json::from_slice::<ExportLine>(text).map_err(|error| JsonlError {
            line,
            kind: JsonlErrorKind::Undecodable(DecodeError::from(error)),
        })
    };
    let header = match lines.next() {
        Some((text, line)) => match decode(text, line)? {
            ExportLine::Header(header) => *header,
            ExportLine::Row(_) | ExportLine::Trailer(_) => {
                return fail(line, JsonlErrorKind::HeaderNotFirst);
            }
        },
        None => return fail(1, JsonlErrorKind::NoHeader),
    };
    let mut rows = Vec::new();
    let mut trailer = None;
    for (text, line) in lines {
        if trailer.is_some() {
            return fail(line, JsonlErrorKind::AfterTrailer);
        }
        match decode(text, line)? {
            ExportLine::Header(_) => return fail(line, JsonlErrorKind::SecondHeader),
            ExportLine::Row(row) => rows.push(row),
            ExportLine::Trailer(last) => trailer = Some(last),
        }
    }
    Ok(JsonlExport {
        header,
        rows,
        trailer,
    })
}
