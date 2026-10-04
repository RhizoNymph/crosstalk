//! `GET /projections/{id}/frame`: the ETag is the frame's digest, the frame
//! is cacheable until its retention ends, and a matching `If-None-Match`
//! is a 304.

use std::time::Duration;

use crate::aggregates::projection::ProjectionStatusKind;
use crate::interfaces::l8_surface::http::frame::{FrameCache, MAX_AGE_LIMIT, OCTET_STREAM};
use crate::interfaces::l8_surface::http::{ErrorStatus, ResponseBody, Route, Status};
use crate::interfaces::l8_surface::{ConflictKind, QueryError};
use crate::support::{Blake3, Timestamp};
use crate::tests::export::projection;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn digest() -> Blake3 {
    Blake3::from_bytes([0xab; 32])
}

/// The fixture projection was fitted at 700 µs.
fn at_micros(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

#[test]
fn the_etag_is_the_quoted_digest() {
    let cache = FrameCache::of(&projection(), digest(), DAY, at_micros(700));
    assert_eq!(cache.etag(), format!("\"{}\"", "ab".repeat(32)));
}

#[test]
fn a_ready_frame_is_cached_until_its_retention_ends() {
    let fitted = 700;
    let cache = FrameCache::of(&projection(), digest(), 180 * DAY, at_micros(fitted));
    assert_eq!(
        cache.cache_control(),
        format!("private, max-age={}, immutable", (180 * DAY).as_secs())
    );
    let a_day_later = at_micros(fitted + 1_000_000 * DAY.as_secs());
    let later = FrameCache::of(&projection(), digest(), 180 * DAY, a_day_later);
    assert_eq!(
        later.cache_control(),
        format!("private, max-age={}, immutable", (179 * DAY).as_secs())
    );
    let past = FrameCache::of(
        &projection(),
        digest(),
        DAY,
        at_micros(fitted + 2 * 86_400_000_000),
    );
    assert_eq!(past.cache_control(), "private, max-age=0, immutable");
    let long = FrameCache::of(&projection(), digest(), 1000 * DAY, at_micros(fitted));
    assert_eq!(
        long.cache_control(),
        format!("private, max-age={}, immutable", MAX_AGE_LIMIT.as_secs())
    );
}

#[test]
fn a_matching_if_none_match_is_not_modified() {
    let cache = FrameCache::of(&projection(), digest(), DAY, at_micros(700));
    let etag = cache.etag();
    assert!(!cache.not_modified(None));
    assert!(cache.not_modified(Some(&etag)));
    assert!(cache.not_modified(Some("*")));
    assert!(cache.not_modified(Some(&format!("\"other\", W/{etag}"))));
    assert!(!cache.not_modified(Some("\"other\"")));
    assert!(!cache.not_modified(Some(&etag.replace('"', ""))));
}

/// The frame route serves bytes; a frame that is not ready is a 409 with
/// its conflict, an expired one a 410, so neither is ever cached as a
/// frame.
#[test]
fn a_frame_not_ready_is_a_conflict_and_an_expired_one_gone() {
    let spec = Route::ProjectionFrame.spec();
    assert_eq!(spec.success.body, ResponseBody::Frame);
    assert_eq!(spec.success.body.content_type(), OCTET_STREAM);
    let id = projection().info().id();
    for status in [ProjectionStatusKind::Queued, ProjectionStatusKind::Fitting] {
        let error = QueryError::Conflict(ConflictKind::ProjectionNotReady {
            projection: id,
            status,
        });
        assert_eq!(error.status(), Status::Conflict);
    }
    let gone = QueryError::ProjectionNotRetained { projection: id };
    assert_eq!(gone.status(), Status::Gone);
}
