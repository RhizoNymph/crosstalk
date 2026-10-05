//! What the UI needs from L8 that `crosstalk-spec` does not have yet.
//!
//! What is left: the present and the bucket width ([`present`]) and the
//! export formats the backend writes ([`formats`]), both spec gaps.
//! Everything else the UI reads or sends, the export included, is the
//! spec's.

pub mod formats;
pub mod present;
