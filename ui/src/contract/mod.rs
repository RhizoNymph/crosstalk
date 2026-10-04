//! What the UI needs from L8 that `crosstalk-spec` does not have yet.
//!
//! Each item is numbered as in "The L8 contract" in `docs/features/ui.md`.
//! Names and shapes are the ones the UI asks the gateway for. What is left:
//! the present and the bucket width ([`present`], a spec gap), and the
//! export request ([`research`], until the export page moves onto the
//! spec's `QueryApi::export`).

pub mod present;
pub mod research;
