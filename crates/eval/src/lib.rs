//! crosstalk's evaluation harness.
//!
//! Public multi-agent datasets become per-agent sequences of the spec's
//! `NormalizedExchange`s with ground-truth labels ([`corpus`], [`truth`],
//! [`datasets`]). Any detector's transmissions become [`predict`]ions and are
//! scored against the labels ([`score`]), and a deliberately naive
//! [`reference`](mod@reference) matcher validates the labels and sets a baseline;
//! [`gateway`] runs the gateway's own pipeline as a detector, and
//! [`detect::live`] scores a gateway composition's detection through the
//! `LiveBackend` seam. Reports and
//! regression gates are in [`report`]; [`pipeline`] runs the whole thing.

pub mod config;
pub mod corpus;
pub mod datasets;
pub mod detect;
pub mod gateway;
pub mod ids;
pub mod keys;
pub mod location;
pub mod pipeline;
pub mod predict;
pub mod reference;
pub mod report;
pub mod score;
pub mod truth;
