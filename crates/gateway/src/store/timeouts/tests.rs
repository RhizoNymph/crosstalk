//! The pipeline's statement and lock bounds: their defaults, their checks
//! and how the store section reads them.

use std::time::Duration;

use super::*;
use crate::config::StoreSection;

fn section(json: serde_json::Value) -> Result<StoreSection, serde_json::Error> {
    serde_json::from_value(json)
}

#[test]
fn the_defaults_are_a_minute_and_fifteen_seconds() {
    let defaults = PipelineTimeouts::default();
    assert_eq!(defaults.retention(), Duration::from_secs(15 * 60));
    assert_eq!(
        PipelineTimeouts::DEFAULT_RETENTION,
        Duration::from_secs(15 * 60)
    );
    assert_eq!(defaults.statement(), Duration::from_secs(60));
    assert_eq!(defaults.lock(), Duration::from_secs(15));
    assert_eq!(PipelineTimeouts::DEFAULT_STATEMENT, Duration::from_secs(60));
    assert_eq!(PipelineTimeouts::DEFAULT_LOCK, Duration::from_secs(15));
}

/// The lock bound sits above the bus's own tighter ones (a take's 10 s
/// `SET LOCAL lock_timeout`, the reaper's 2 s), which keep theirs, and
/// below the ack timeout, so a stage write blocked on a lock is nacked
/// before its hold times out.
#[test]
fn the_default_lock_bound_sits_between_the_bus_bounds_and_the_ack_timeout() {
    let lock = PipelineTimeouts::default().lock();
    assert!(lock > Duration::from_secs(10));
    assert!(lock < crosstalk_transport::PgBusConfig::DEFAULT_ACK_TIMEOUT);
}

#[test]
fn a_zero_bound_is_refused() {
    assert_eq!(
        PipelineTimeouts::new(
            Duration::ZERO,
            Duration::from_secs(1),
            PipelineTimeouts::DEFAULT_RETENTION
        ),
        Err(InvalidPipelineTimeouts::Zero {
            setting: "statement_timeout"
        })
    );
    assert_eq!(
        PipelineTimeouts::new(
            Duration::from_secs(1),
            Duration::ZERO,
            PipelineTimeouts::DEFAULT_RETENTION
        ),
        Err(InvalidPipelineTimeouts::Zero {
            setting: "lock_timeout"
        })
    );
    // Below a millisecond is zero to the server.
    assert_eq!(
        PipelineTimeouts::new(
            Duration::from_micros(999),
            Duration::from_micros(500),
            PipelineTimeouts::DEFAULT_RETENTION
        ),
        Err(InvalidPipelineTimeouts::Zero {
            setting: "statement_timeout"
        })
    );
}

#[test]
fn a_lock_bound_above_the_statement_bound_is_refused() {
    assert_eq!(
        PipelineTimeouts::new(
            Duration::from_secs(10),
            Duration::from_secs(11),
            PipelineTimeouts::DEFAULT_RETENTION
        ),
        Err(InvalidPipelineTimeouts::LockAboveStatement {
            lock_ms: 11_000,
            statement_ms: 10_000
        })
    );
    let equal = PipelineTimeouts::new(
        Duration::from_secs(10),
        Duration::from_secs(10),
        PipelineTimeouts::DEFAULT_RETENTION,
    )
    .expect("equal bounds are valid");
    assert_eq!(equal.lock(), equal.statement());
}

/// The server keeps both settings as a 32-bit count of milliseconds.
#[test]
fn a_bound_past_the_servers_range_is_refused() {
    let past = Duration::from_millis(u64::try_from(i32::MAX).expect("fits") + 1);
    assert_eq!(
        PipelineTimeouts::new(past, Duration::from_secs(1), past),
        Err(InvalidPipelineTimeouts::TooLarge {
            setting: "statement_timeout",
            ms: u64::try_from(i32::MAX).expect("fits") + 1
        })
    );
    let max = Duration::from_millis(u64::try_from(i32::MAX).expect("fits"));
    assert!(PipelineTimeouts::new(max, max, max).is_ok());
}

/// What every pipeline connection sets, in whole milliseconds.
#[test]
fn the_session_settings_are_whole_milliseconds() {
    assert_eq!(
        PipelineTimeouts::default().session_settings(),
        [
            ("statement_timeout", "60000ms".to_owned()),
            ("lock_timeout", "15000ms".to_owned()),
            ("crosstalk.statement_timeout", "60000ms".to_owned()),
            ("crosstalk.retention_timeout", "900000ms".to_owned()),
        ]
    );
    let short = PipelineTimeouts::new(
        Duration::from_millis(2_500),
        Duration::from_millis(300),
        Duration::from_secs(5),
    )
    .expect("valid");
    assert_eq!(
        short.session_settings(),
        [
            ("statement_timeout", "2500ms".to_owned()),
            ("lock_timeout", "300ms".to_owned()),
            ("crosstalk.statement_timeout", "2500ms".to_owned()),
            ("crosstalk.retention_timeout", "5000ms".to_owned()),
        ]
    );
}

#[test]
fn the_store_section_defaults_them() {
    let store = section(serde_json::json!({"pool": {"max_connections": 4}})).expect("parses");
    assert_eq!(store.pipeline_timeouts, PipelineTimeouts::default());
    let partial = section(serde_json::json!({
        "pool": {"max_connections": 4},
        "pipeline_timeouts": {"lock_ms": 5_000},
    }))
    .expect("parses");
    assert_eq!(
        partial.pipeline_timeouts,
        PipelineTimeouts::new(
            Duration::from_secs(60),
            Duration::from_secs(5),
            PipelineTimeouts::DEFAULT_RETENTION
        )
        .expect("valid")
    );
}

#[test]
fn the_store_section_reads_them_in_milliseconds() {
    let store = section(serde_json::json!({
        "pool": {"max_connections": 4},
        "pipeline_timeouts": {"statement_ms": 30_000, "lock_ms": 2_000, "retention_ms": 600_000},
    }))
    .expect("parses");
    assert_eq!(
        store.pipeline_timeouts,
        PipelineTimeouts::new(
            Duration::from_secs(30),
            Duration::from_secs(2),
            Duration::from_secs(600),
        )
        .expect("valid")
    );
}

#[test]
fn the_store_section_refuses_invalid_bounds_and_unknown_keys() {
    for (bounds, why) in [
        (
            serde_json::json!({"statement_ms": 1_000, "lock_ms": 2_000}),
            "lock_timeout",
        ),
        (
            serde_json::json!({"statement_ms": 0, "lock_ms": 0}),
            "statement_timeout",
        ),
        (
            serde_json::json!({"statement_ms": 1_000, "lock_ms": 0}),
            "lock_timeout",
        ),
        (
            serde_json::json!({"statement_ms": 1_000, "surprise": 1}),
            "surprise",
        ),
        (
            serde_json::json!({"statement_ms": 60_000, "retention_ms": 59_999}),
            "retention_timeout",
        ),
        (
            serde_json::json!({"statement_ms": 1_000_000}),
            "retention_timeout",
        ),
    ] {
        let refused = section(serde_json::json!({
            "pool": {"max_connections": 4},
            "pipeline_timeouts": bounds,
        }))
        .expect_err("refused");
        assert!(refused.to_string().contains(why), "{why}: {refused}");
    }
}

/// Retention deletes get a bound of their own, never below the statement
/// bound every pipeline statement gets.
#[test]
fn a_retention_bound_below_the_statement_bound_is_refused() {
    assert_eq!(
        PipelineTimeouts::new(
            Duration::from_secs(60),
            Duration::from_secs(15),
            Duration::from_secs(59),
        ),
        Err(InvalidPipelineTimeouts::RetentionBelowStatement {
            retention_ms: 59_000,
            statement_ms: 60_000
        })
    );
    assert_eq!(
        PipelineTimeouts::new(
            Duration::from_secs(60),
            Duration::from_secs(15),
            Duration::ZERO
        ),
        Err(InvalidPipelineTimeouts::Zero {
            setting: "retention_timeout"
        })
    );
    let equal = PipelineTimeouts::new(
        Duration::from_secs(60),
        Duration::from_secs(15),
        Duration::from_secs(60),
    )
    .expect("equal bounds are valid");
    assert_eq!(equal.retention(), equal.statement());
}

/// The store section reads `retention_ms`, defaulting it to 15 min.
#[test]
fn the_store_section_reads_the_retention_bound() {
    let store = section(serde_json::json!({
        "pool": {"max_connections": 4},
        "pipeline_timeouts": {"retention_ms": 1_800_000},
    }))
    .expect("parses");
    assert_eq!(
        store.pipeline_timeouts.retention(),
        Duration::from_secs(30 * 60)
    );
    assert_eq!(store.pipeline_timeouts.statement(), Duration::from_secs(60));
}
