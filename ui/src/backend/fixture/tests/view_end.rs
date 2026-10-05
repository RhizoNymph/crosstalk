//! Where a default view ends (`FixtureBackend::view_end`,
//! `AppBackend::view_end`): the present's `now`, except in a replay, whose
//! view ends a bucket past the end of its data however far it has got.

use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::present::Present;

use super::super::FixtureBackend;
use super::super::clock::{BUCKET, DAY, HOUR, NOW, ago, plus};
use super::{SEED, researcher, shared};
use crate::backend::AppBackend;

async fn present_of(b: &FixtureBackend) -> Present {
    b.present(&researcher()).await.expect("present")
}

/// One bucket past the end of the generated data.
const REPLAY_END: crosstalk_spec::support::Timestamp = plus(NOW, BUCKET.as_micros().get());

#[tokio::test]
async fn a_fixed_clock_ends_the_view_at_the_present() {
    let b = shared();
    let present = present_of(b).await;
    assert_eq!(present.now, NOW);
    assert_eq!(b.view_end(&present), NOW);
}

#[tokio::test]
async fn a_live_clock_ends_the_view_at_whatever_present_it_is_given() {
    let b = FixtureBackend::try_live(SEED).expect("fixture generates");
    let present = present_of(&b).await;
    assert_eq!(b.view_end(&present), present.now);
    let later = Present {
        now: plus(present.now, HOUR),
        ..present
    };
    assert_eq!(b.view_end(&later), later.now);
}

#[tokio::test]
async fn a_replay_ends_the_view_a_bucket_past_its_data_whatever_its_present() {
    for at in [ago(3 * DAY), ago(HOUR), NOW] {
        let b = FixtureBackend::try_replay_at(SEED, at).expect("fixture generates");
        let present = present_of(&b).await;
        assert!(present.now >= at && present.now <= NOW, "{:?}", present.now);
        assert_eq!(b.view_end(&present), REPLAY_END, "replay at {at:?}");
        b.set_replay_at(ago(DAY)).await;
        assert_eq!(b.view_end(&present_of(&b).await), REPLAY_END);
    }
}

#[tokio::test]
async fn the_app_backend_asks_the_fixture() {
    let fixed = AppBackend::from(FixtureBackend::try_new(SEED).expect("fixture generates"));
    let present = fixed.present(&researcher()).await.expect("present");
    assert_eq!(fixed.view_end(&present), present.now);

    let replay =
        AppBackend::from(FixtureBackend::try_replay_at(SEED, ago(DAY)).expect("fixture generates"));
    let present = replay.present(&researcher()).await.expect("present");
    assert_eq!(replay.view_end(&present), REPLAY_END);
}

#[tokio::test]
async fn every_read_of_the_present_is_counted() {
    let b = FixtureBackend::try_new(SEED).expect("fixture generates");
    let reads = b.present_reads();
    present_of(&b).await;
    present_of(&b).await;
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
}
