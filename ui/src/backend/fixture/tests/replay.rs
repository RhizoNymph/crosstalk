//! Replay mode: nothing stamped after the replay's present is visible, and
//! it becomes visible as the present moves on.

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, QueryApi};
use crosstalk_spec::support::Timestamp;

use super::super::FixtureBackend;
use super::super::clock::{DAY, HOUR, NOW, ago, replay_watermark};
use super::{SEED, collect, day, researcher};

async fn transmissions(b: &FixtureBackend) -> u64 {
    let scope = day();
    b.overview(&researcher(), scope.window, &scope.topology_filter())
        .await
        .expect("overview")
        .value
        .activity
        .transmissions
}

async fn alerts(b: &FixtureBackend) -> Vec<Alert> {
    let c = researcher();
    collect(50, async |page| {
        b.alerts(&c, &AlertFilter::default(), &page).await
    })
    .await
}

async fn audit_times(b: &FixtureBackend) -> Vec<Timestamp> {
    let c = researcher();
    collect(50, async |page| {
        b.audit(&c, &AuditFilter::default(), &page).await
    })
    .await
    .into_iter()
    .map(|e| e.at)
    .collect()
}

#[tokio::test]
async fn data_after_the_replay_present_is_invisible_until_it_passes() {
    let at = ago(DAY / 2);
    let b = FixtureBackend::try_replay_at(SEED, at).expect("fixture generates");
    let early_tx = transmissions(&b).await;
    let early_alerts = alerts(&b).await;
    let early_audit = audit_times(&b).await;
    // The clock may have moved a few microseconds: allow a second.
    let bound = Timestamp::from_micros(at.as_micros() + 1_000_000);
    assert!(early_alerts.iter().all(|a| a.raised_at <= bound));
    assert!(early_audit.iter().all(|t| *t <= bound));
    let watermark = b.watermark(&researcher()).await.expect("watermark");
    assert_eq!(watermark.0, replay_watermark(at));

    b.set_replay_at(NOW).await;
    let late_tx = transmissions(&b).await;
    let late_alerts = alerts(&b).await;
    assert!(early_tx < late_tx, "{early_tx} < {late_tx}");
    assert!(early_alerts.len() <= late_alerts.len());

    // The same world on a fixed clock sees what the replay sees at its end.
    let fixed = FixtureBackend::try_new(SEED).expect("fixture generates");
    assert_eq!(late_tx, transmissions(&fixed).await);
    assert_eq!(late_alerts.len(), alerts(&fixed).await.len());
}

#[tokio::test]
async fn a_replay_before_the_window_shows_nothing_in_it() {
    let b = FixtureBackend::try_replay_at(SEED, ago(DAY + HOUR)).expect("fixture generates");
    assert_eq!(transmissions(&b).await, 0);
}
