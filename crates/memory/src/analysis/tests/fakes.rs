//! The computational doubles keep the contracts the stores rely on.

use std::num::NonZeroU16;
use std::sync::Arc;

use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionLimit, ProjectionParams};
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::interfaces::l6_analysis::{
    EmbedError, Embedder, LayoutFitter, RuleContext, TopicError, TopicModel,
};

use super::support::model;
use crate::analysis::fakes::{
    FakeEmbedder, FakeLayoutFitter, FakeRuleContext, FakeTopicModel, fake_model,
};
use crate::analysis::support::similarity as cosine;
use crate::model::build::{channel, similarity, test_model, transmission, ts, unit};
use crate::support::ManualClock;

fn topic_model() -> FakeTopicModel {
    FakeTopicModel::new(
        model(),
        2,
        2,
        similarity(0.5).unwrap(),
        Arc::new(ManualClock::at(ts(9))),
    )
}

#[tokio::test]
async fn embed_preserves_count_and_order() {
    // analysis.embedder.one-per-input, for the double
    let embedder = FakeEmbedder::new(fake_model("fake", NonZeroU16::new(16).unwrap()), 40);
    let texts = ["wiki page", "deploy", "", "wiki"];
    let embeddings = embedder.embed(&texts).await.unwrap();
    assert_eq!(embeddings.len(), texts.len());
    for (text, embedding) in texts.iter().zip(&embeddings) {
        assert_eq!(*embedding, embedder.embed_one(text).unwrap());
    }
    let shared = cosine(&embeddings[0], &embeddings[3]).unwrap();
    let disjoint = cosine(&embeddings[1], &embeddings[3]).unwrap();
    assert!(shared.get() > disjoint.get());
    assert_eq!(
        embedder.embed(&["short", &"x".repeat(41)]).await,
        Err(EmbedError::TooLong { index: 1 })
    );
}

#[test]
fn unfitted_model_has_version_zero_and_assigns_outliers() {
    // analysis.topic.version-zero-outlier, for the double
    let topics = topic_model();
    assert_eq!(topics.version(), TopicModelVersion(0));
    let assignment = topics
        .assign(&unit(&model(), 1.0, 0.0, 0.0).unwrap())
        .unwrap();
    assert_eq!(assignment, Assignment::Outlier);
}

#[test]
fn fit_returns_version_above_current_with_member_mean_centroids() {
    // analysis.topic.refit-new-version and centroid-mean, for the double
    let topics = topic_model();
    let a = unit(&model(), 1.0, 0.0, 0.0).unwrap();
    let b = unit(&model(), 0.0, 1.0, 0.0).unwrap();
    let a2 = unit(&model(), 1.0, 0.2, 0.0).unwrap();
    let (version, fitted) = topics.fit(&[a.clone(), b.clone(), a2.clone()]).unwrap();
    assert!(version > TopicModelVersion(0));
    assert_eq!(topics.version(), version);
    assert_eq!(fitted.len(), 2);
    let mean: Vec<f32> = a
        .values()
        .iter()
        .zip(a2.values())
        .map(|(x, y)| x + y)
        .collect();
    let norm = mean.iter().map(|v| v * v).sum::<f32>().sqrt();
    for (got, want) in fitted[0]
        .centroid
        .values()
        .iter()
        .zip(mean.iter().map(|v| v / norm))
    {
        assert!((got - want).abs() < 1e-6);
    }
    match topics.assign(&a2).unwrap() {
        Assignment::Topic { topic, .. } => assert_eq!(topic, fitted[0].id),
        Assignment::Outlier => panic!("a member is assigned its topic"),
    }
    let (next, _) = topics.fit(&[a, b]).unwrap();
    assert!(next > version);
    assert_eq!(
        topics.fit(&[unit(&model(), 1.0, 0.0, 0.0).unwrap()]),
        Err(TopicError::TooFewSamples { needed: 2, got: 1 })
    );
}

#[test]
fn assign_rejects_embedding_of_other_model() {
    // analysis.embedding.same-model-only, for the double
    let topics = topic_model();
    let other = test_model("other");
    assert_eq!(
        topics.assign(&unit(&other, 1.0, 0.0, 0.0).unwrap()),
        Err(TopicError::WrongModel {
            expected: model(),
            got: other
        })
    );
}

#[test]
fn prop_layout_is_deterministic() {
    // analysis.projection.deterministic-layout, for the double
    let embeddings: Vec<_> = (0..5)
        .map(|n| unit(&model(), 1.0, n as f32, 0.5).unwrap())
        .collect();
    let params = ProjectionParams::new(ProjectionLimit::new(10).unwrap(), 2, 100, 12_345).unwrap();
    let first = FakeLayoutFitter.fit(&embeddings, params).unwrap();
    let second = FakeLayoutFitter.fit(&embeddings, params).unwrap();
    let bits = |layout: &[[f32; 2]]| {
        layout
            .iter()
            .flat_map(|[x, y]| [x.to_bits(), y.to_bits()])
            .collect::<Vec<_>>()
    };
    assert_eq!(bits(&first), bits(&second));
    assert_eq!(first.len(), embeddings.len());
    assert_eq!(
        FakeLayoutFitter.fit(&embeddings[..2], params),
        Err(FitFailure::TooFewPoints { needed: 3, got: 2 })
    );
}

#[tokio::test]
async fn rule_context_reads_what_the_test_set() {
    let mut context = FakeRuleContext::default();
    let embedding = unit(&model(), 1.0, 0.0, 0.0).unwrap();
    context
        .embeddings
        .insert(transmission(1), embedding.clone());
    assert_eq!(
        context.transmission_embedding(transmission(1)).await,
        Some(embedding)
    );
    assert_eq!(context.transmission_embedding(transmission(2)).await, None);
    assert_eq!(context.channel_policy(channel(1)).await, None);
}
