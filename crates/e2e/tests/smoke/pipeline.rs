//! The composition ingests the scenario: every body stored where the
//! surface's evidence page reads, every exchange published on the bus the
//! detection consumers subscribe to, in time order.

use std::time::Duration;

use crosstalk_e2e::{compose, feed};
use crosstalk_gateway::pipeline::Settings;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::observed::exchange::ExchangeOutcome;
use crosstalk_spec::support::Clock;

use crate::support::{Failure, normalized, relay, unexpected};

#[tokio::test]
async fn every_exchange_is_stored_and_published_in_order() -> Result<(), Failure> {
    let scenario = relay();
    let composition = compose(scenario.start).await?;
    // Subscribed before anything is published, as a detection consumer is.
    let mut observer = composition
        .stores
        .bus
        .subscribe(
            &[Subject::ExchangeCaptured],
            ConsumerGroup("e2e-observer".to_owned()),
            Settings::default().consumer_retry,
        )
        .await
        .map_err(|error| unexpected(format!("subscribe: {error:?}")))?;

    let clock = composition.clock.clone();
    let fed = feed(&scenario, &composition.pipeline, |at| clock.set(at)).await?;
    let labels: Vec<&str> = fed.iter().map(|fed| fed.label).collect();
    assert_eq!(labels, ["a1-write", "a2-ack", "b1-read", "b2-repeat"]);
    assert_eq!(composition.pipeline.stats().snapshot().published, 4);
    assert_eq!(composition.clock.now(), scenario.ends_at());

    for (wire, expected) in normalized(&scenario)? {
        let delivery = tokio::time::timeout(Duration::from_secs(5), observer.next())
            .await
            .map_err(|_| unexpected(format!("{}: not delivered", wire.label)))?
            .ok_or_else(|| unexpected("the bus shut down"))?
            .map_err(|error| unexpected(format!("delivery: {error:?}")))?;
        let BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) = &delivery.envelope.event
        else {
            return Err(unexpected("not ExchangeCaptured"));
        };
        assert_eq!(**exchange, expected.exchange, "{}", wire.label);
        assert_eq!(delivery.envelope.at, wire.ended_at, "{}", wire.label);
        observer
            .ack(delivery.id)
            .await
            .map_err(|error| unexpected(format!("ack: {error:?}")))?;

        let mut hashes = exchange.request.clone();
        if let ExchangeOutcome::Completed { response, .. } = &exchange.outcome {
            hashes.push(*response);
        }
        for hash in hashes {
            let stored = composition
                .stores
                .blobs
                .get(hash)
                .await
                .map_err(|error| unexpected(format!("blob get: {error:?}")))?;
            assert!(stored.is_some(), "{}: body {hash:?} not stored", wire.label);
        }
    }
    composition.shutdown().await;
    Ok(())
}
