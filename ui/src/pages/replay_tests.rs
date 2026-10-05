//! Pages over a replay: early in the replay they show less than at its end.

use topcoat::router::StatusCode;

use crate::backend::fixture::FixtureBackend;
use crate::pages::topology::tests::fixture_state;
use crate::testing::{SEED, get_from, router_over};

const DAY_MICROS: u64 = 86_400_000_000;
const NOW_MICROS: u64 = 1_790_985_600_000_000;

async fn alert_links(at_micros: u64) -> usize {
    let backend = FixtureBackend::try_replay_at(
        SEED,
        crosstalk_spec::support::Timestamp::from_micros(at_micros),
    )
    .expect("fixture generates");
    let router = router_over(backend);
    let reply = get_from(&router, &format!("/alerts?{}", fixture_state().to_query())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body.matches("href=\"/alerts/01").count()
}

#[tokio::test]
async fn the_alert_inbox_fills_in_as_the_replay_runs() {
    let early = alert_links(NOW_MICROS - 3 * DAY_MICROS).await;
    let late = alert_links(NOW_MICROS).await;
    assert!(early < late, "{early} < {late}");
}
