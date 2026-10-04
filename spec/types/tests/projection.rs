use crate::aggregates::edge::{RouteKind, TopicSlot};
use crate::aggregates::projection::{
    InvalidProjection, InvalidProjectionLimit, ProjectedPoint, Projection, ProjectionLimit,
    ProjectionToken,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::TopicId;
use crate::tests::fixtures::{agent, at, transmission};

const TOKEN: ProjectionToken = ProjectionToken::new(TopicModelVersion(3), 0);

fn limit(n: u32) -> ProjectionLimit {
    ProjectionLimit::new(n).expect("fixture limits are in range")
}

fn point(n: u128) -> ProjectedPoint {
    ProjectedPoint {
        transmission: transmission(n),
        from: agent(1),
        to: agent(2),
        route: RouteKind::Unobserved,
        topic: Some(TopicId::from_ulid(7)),
        confirmed_at: at(10),
        x: 0.5,
        y: -1.5,
    }
}

#[test]
fn projection_limit_rejects_zero_and_above_max() {
    assert_eq!(ProjectionLimit::new(0), Err(InvalidProjectionLimit::Zero));
    assert_eq!(
        ProjectionLimit::new(ProjectionLimit::MAX + 1),
        Err(InvalidProjectionLimit::AboveMax {
            max: ProjectionLimit::MAX,
            got: ProjectionLimit::MAX + 1
        })
    );
    assert_eq!(
        limit(ProjectionLimit::MAX).get().get(),
        ProjectionLimit::MAX
    );
}

#[test]
fn projection_holds_every_match_under_the_limit() {
    let projection =
        Projection::new(TOKEN, limit(10), 2, vec![point(1), point(2)]).expect("2 of 2");
    assert_eq!(projection.points().len(), 2);
    assert_eq!(projection.matching(), 2);
    assert!(!projection.is_sampled());
}

#[test]
fn projection_samples_down_to_the_limit() {
    let projection = Projection::new(TOKEN, limit(2), 5, vec![point(1), point(2)]).expect("2 of 5");
    assert!(projection.is_sampled());
    assert_eq!(projection.matching(), 5);
}

#[test]
fn projection_rejects_a_count_other_than_min_of_matching_and_limit() {
    // Too many for the limit.
    assert_eq!(
        Projection::new(TOKEN, limit(1), 2, vec![point(1), point(2)]),
        Err(InvalidProjection::WrongCount {
            expected: 1,
            got: 2
        })
    );
    // Fewer than the limit while more match.
    assert_eq!(
        Projection::new(TOKEN, limit(3), 5, vec![point(1), point(2)]),
        Err(InvalidProjection::WrongCount {
            expected: 3,
            got: 2
        })
    );
    // More than match.
    assert_eq!(
        Projection::new(TOKEN, limit(3), 1, vec![point(1), point(2)]),
        Err(InvalidProjection::WrongCount {
            expected: 1,
            got: 2
        })
    );
}

#[test]
fn projection_rejects_duplicate_transmissions() {
    assert_eq!(
        Projection::new(TOKEN, limit(3), 2, vec![point(1), point(1)]),
        Err(InvalidProjection::Duplicate(transmission(1)))
    );
}

#[test]
fn projection_rejects_non_finite_coordinates() {
    for (x, y) in [
        (f32::NAN, 0.0),
        (0.0, f32::INFINITY),
        (f32::NEG_INFINITY, 0.0),
    ] {
        let bad = ProjectedPoint { x, y, ..point(2) };
        assert_eq!(
            Projection::new(TOKEN, limit(3), 2, vec![point(1), bad]),
            Err(InvalidProjection::NonFinite(transmission(2)))
        );
    }
}

#[test]
fn empty_projection_before_any_match() {
    let projection = Projection::new(TOKEN, limit(1), 0, Vec::new()).expect("nothing matched");
    assert!(projection.points().is_empty());
    assert!(!projection.is_sampled());
}

#[test]
fn point_topic_slot_is_under_the_token_version() {
    let projection = Projection::new(TOKEN, limit(1), 1, vec![point(1)]).expect("1 of 1");
    assert_eq!(projection.topic_version(), TopicModelVersion(3));
    assert_eq!(
        projection.slot(&projection.points()[0]),
        TopicSlot {
            version: TopicModelVersion(3),
            topic: Some(TopicId::from_ulid(7)),
        }
    );
}
