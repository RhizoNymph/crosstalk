//! Which export formats the backend writes: a capability `crosstalk-spec`'s
//! L8 traits do not expose.
//!
//! `QueryApi::export` takes an `ExportFormat` (JSONL or Parquet), and the
//! export docs define both encodings, but the spec has neither a
//! capability query nor an error for a format the gateway cannot write.
//! The fixture writes JSONL only (a Parquet writer needs a Thrift footer
//! and column encodings the UI crate does not depend on) and refuses a
//! Parquet export with `Store`, before anything is read. The export page
//! asks this trait to show Parquet as unavailable rather than offering a
//! choice that is refused.
//!
//! Proposed for `QueryApi` (or as `InputError::FormatUnsupported`); when it
//! lands there this module is deleted.

use crosstalk_spec::interfaces::l8_surface::export::ExportFormat;

pub trait ExportFormats {
    /// The formats `export` accepts, in the order the form offers them.
    fn export_formats(&self) -> &'static [ExportFormat];
}
