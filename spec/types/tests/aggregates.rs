use std::num::NonZeroU16;

use crate::aggregates::edge::{EdgeKey, EdgeSelector, SelfEdge, TopicSlot};
use crate::aggregates::topic::{Embedding, EmbeddingModel, InvalidEmbedding, TopicModelVersion};
use crate::derived::flow::transmission::Route;
use crate::support::TimeWindow;
use crate::tests::fixtures::{agent, at, channel};

fn model(dimension: u16) -> EmbeddingModel {
    EmbeddingModel {
        name: "test".into(),
        dimension: NonZeroU16::new(dimension).expect("non-zero dimension"),
    }
}

#[test]
fn edge_key_rejects_self_edge() {
    let bucket = TimeWindow::new(at(0), at(60)).expect("non-empty");
    let slot = TopicSlot {
        version: TopicModelVersion(1),
        topic: None,
    };
    assert_eq!(
        EdgeKey::new(agent(1), agent(1), Route::Channel(channel(1)), slot, bucket),
        Err(SelfEdge)
    );
    let key = EdgeKey::new(agent(1), agent(2), Route::Channel(channel(1)), slot, bucket)
        .expect("different agents");
    assert_eq!((key.from(), key.to()), (agent(1), agent(2)));
}

#[test]
fn edge_selector_rejects_self_edge() {
    assert_eq!(
        EdgeSelector::new(agent(1), agent(1), Route::Unobserved),
        Err(SelfEdge)
    );
    let edge = EdgeSelector::new(agent(1), agent(2), Route::Channel(channel(1)))
        .expect("different agents");
    assert_eq!((edge.from(), edge.to()), (agent(1), agent(2)));
    assert_eq!(edge.route(), &Route::Channel(channel(1)));
}

#[test]
fn embedding_accepts_unit_vectors_of_the_model_dimension() {
    let embedding = Embedding::new(model(2), vec![0.6, 0.8]).expect("unit vector, dimension 2");
    assert_eq!(embedding.values(), &[0.6, 0.8]);
}

#[test]
fn embedding_rejects_wrong_dimension() {
    assert_eq!(
        Embedding::new(model(3), vec![0.6, 0.8]),
        Err(InvalidEmbedding::WrongDimension {
            expected: 3,
            got: 2
        })
    );
}

#[test]
fn embedding_rejects_unnormalized_and_nan() {
    assert!(matches!(
        Embedding::new(model(2), vec![1.0, 1.0]),
        Err(InvalidEmbedding::NotNormalized { .. })
    ));
    assert!(matches!(
        Embedding::new(model(2), vec![f32::NAN, 0.0]),
        Err(InvalidEmbedding::NotNormalized { .. })
    ));
}
