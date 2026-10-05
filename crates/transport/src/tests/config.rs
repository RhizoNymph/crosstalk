//! `BusConfig` decoding: defaults, strictness and the checks.

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::{BusConfig, DeliveryOrder};

fn decode(json: &str) -> Result<BusConfig, serde_json::Error> {
    serde_json::from_str(json)
}

#[test]
fn defaults_are_valid() {
    let config = BusConfig::default();
    assert_eq!(config.group_capacity, BusConfig::DEFAULT_GROUP_CAPACITY);
    assert_eq!(config.command_buffer, BusConfig::DEFAULT_COMMAND_BUFFER);
    assert_eq!(config.ack_timeout.get(), BusConfig::DEFAULT_ACK_TIMEOUT);
    assert_eq!(
        config.dead_letter_retry.get(),
        BusConfig::DEFAULT_DEAD_LETTER_RETRY
    );
    assert_eq!(config.order, DeliveryOrder::Fifo);
    assert_eq!(config.retry.max_attempts(), BusConfig::DEFAULT_MAX_ATTEMPTS);
    assert_eq!(
        config.retry.initial_backoff(),
        BusConfig::DEFAULT_INITIAL_BACKOFF
    );
    assert_eq!(config.retry.max_backoff(), BusConfig::DEFAULT_MAX_BACKOFF);
}

#[test]
fn an_empty_object_is_the_defaults() {
    assert_eq!(decode("{}").expect("decodes"), BusConfig::default());
}

#[test]
fn every_field_decodes() {
    let config = decode(
        r#"{
            "group_capacity": 8,
            "command_buffer": 4,
            "ack_timeout_micros": 2000000,
            "dead_letter_retry_micros": 500,
            "order": {"type": "shuffled", "data": {"seed": 42}},
            "retry": {"max_attempts": 7, "initial_backoff_micros": 1000, "max_backoff_micros": 1000}
        }"#,
    )
    .expect("decodes");
    assert_eq!(
        config.group_capacity,
        NonZeroUsize::new(8).expect("non-zero")
    );
    assert_eq!(
        config.command_buffer,
        NonZeroUsize::new(4).expect("non-zero")
    );
    assert_eq!(config.ack_timeout.get(), Duration::from_secs(2));
    assert_eq!(config.dead_letter_retry.get(), Duration::from_micros(500));
    assert_eq!(config.order, DeliveryOrder::Shuffled { seed: 42 });
    assert_eq!(config.retry.max_attempts().get(), 7);
    assert_eq!(config.retry.initial_backoff(), Duration::from_millis(1));
    assert_eq!(config.retry.max_backoff(), Duration::from_millis(1));
}

#[test]
fn a_partial_retry_fills_its_defaults() {
    let config = decode(r#"{"retry": {"max_attempts": 2}}"#).expect("decodes");
    assert_eq!(config.retry.max_attempts().get(), 2);
    assert_eq!(
        config.retry.initial_backoff(),
        BusConfig::DEFAULT_INITIAL_BACKOFF
    );
    assert_eq!(
        decode(r#"{"order": {"type": "fifo"}}"#)
            .expect("decodes")
            .order,
        DeliveryOrder::Fifo
    );
}

#[test]
fn refused_configs() {
    let refused = [
        // Unknown fields, at either level.
        r#"{"group_capacity": 8, "capacity": 8}"#,
        r#"{"retry": {"attempts": 3}}"#,
        // Zero capacities and durations.
        r#"{"group_capacity": 0}"#,
        r#"{"command_buffer": 0}"#,
        r#"{"ack_timeout_micros": 0}"#,
        r#"{"dead_letter_retry_micros": 0}"#,
        // Retry policies `RetryPolicy::new` refuses.
        r#"{"retry": {"max_attempts": 0}}"#,
        r#"{"retry": {"initial_backoff_micros": 0}}"#,
        r#"{"retry": {"initial_backoff_micros": 20, "max_backoff_micros": 10}}"#,
        // Unknown and malformed orders.
        r#"{"order": {"type": "lifo"}}"#,
        r#"{"order": {"type": "shuffled"}}"#,
        r#"{"order": {"type": "shuffled", "data": {"seed": 1, "salt": 2}}}"#,
        r#"{"order": "fifo"}"#,
        // A duration in another unit.
        r#"{"ack_timeout": 30}"#,
    ];
    for json in refused {
        assert!(decode(json).is_err(), "accepted {json}");
    }
}
