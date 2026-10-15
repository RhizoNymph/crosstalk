//! Analysis (L6) on the wire: alert rules, topics and their history,
//! projections, and the insight bus events. One module per golden area:
//! `rules`, `topics`, `projections` and `insight`.

use std::num::NonZeroU16;

use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crate::ids::{OperatorId, TopicId};
use crate::support::{Similarity, Timestamp};

mod insight;
mod projections;
mod rules;
mod topics;

/// Topic ids of one fit, readable in goldens.
const TOPIC_D: &str = "01J9Z3P5Q6R7S8T9V0W1X2Y3Z4";
const TOPIC_E: &str = "01J9Z3Q6R7S8T9V0W1X2Y3Z4A5";

fn version(n: u32) -> TopicModelVersion {
    TopicModelVersion(n)
}

fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

/// Five distinct topic ids: `topic(0)` to `topic(4)`.
fn topic(n: usize) -> TopicId {
    let text = [ULID_A, ULID_B, ULID_C, TOPIC_D, TOPIC_E][n];
    id(TopicId::from_ulid_text, text)
}

fn sim(value: f32) -> Similarity {
    Similarity::new(value).expect("fixture similarities are in 0..=1")
}

/// A time on the fixture day, `hh:mm:ss`.
fn at(clock: &str) -> Timestamp {
    ts(&format!("2026-10-04T{clock}.000000Z"))
}

/// A four-dimensional model: real models have hundreds of dimensions, which
/// would bury a golden in numbers without changing its shape.
fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "nomic-embed-text-v1.5".into(),
        dimension: NonZeroU16::new(4).unwrap_or(NonZeroU16::MIN),
    }
}

fn other_model() -> EmbeddingModel {
    EmbeddingModel {
        name: "bge-small-en-v1.5".into(),
        dimension: NonZeroU16::new(4).unwrap_or(NonZeroU16::MIN),
    }
}

/// A unit vector of `model`, exactly representable in `f32` and in JSON.
fn embedding(model: EmbeddingModel) -> Embedding {
    Embedding::new(model, vec![0.5, -0.5, 0.5, 0.5]).expect("a unit vector of dimension 4")
}
