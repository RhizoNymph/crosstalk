//! Under `Bodies::SkipStored` an ingester puts a message body once: a later exchange repeating it
//! (a conversation's history) puts only what is new, and every body it
//! references is still in the store.

use std::sync::Arc;

use crosstalk_canonical::AnthropicMessages;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l1_canonical::{NormalizedExchange, Normalizer};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};

use super::raw;
use super::record::{RecordingStore, drain};
use crate::pipeline::{Bodies, Deps, Pipeline, Settings};

struct Fixed(Timestamp);

impl Clock for Fixed {
    fn now(&self) -> Timestamp {
        self.0
    }
}

pub async fn known_bodies_are_put_once() {
    let mut ids = Ids::seeded(7);
    let exchanges: Vec<NormalizedExchange> = raw::exchanges(&mut ids)
        .iter()
        .map(|raw| AnthropicMessages.normalize(raw).expect("normalize"))
        .collect();
    let first = exchanges.first().expect("a corpus exchange").clone();
    let blobs = MemoryBlobStore::new();
    let (store, mut puts) = RecordingStore::new(blobs.clone());
    let bus = MpscBus::start(BusConfig::default()).expect("bus");
    let at = Timestamp::from_micros(1_000_000);
    let pipeline = Pipeline::build(
        Settings::default(),
        Deps {
            bodies: Bodies::SkipStored,
            ..Deps::stores(store, bus, SeededRandom::new(7))
        },
        Arc::new(Fixed(at)),
    )
    .await
    .expect("pipeline");

    pipeline
        .ingest(first.clone(), at)
        .await
        .expect("first ingest");
    let put = drain(&mut puts);
    assert_eq!(put.len(), first.messages.len() + first.media.len());

    // The same bodies again: only the media blobs are put.
    pipeline
        .ingest(first.clone(), at)
        .await
        .expect("second ingest");
    let again = drain(&mut puts);
    assert_eq!(again.len(), first.media.len());
    for message in &first.messages {
        assert!(
            again
                .iter()
                .all(|put| put.bytes != encoding::encode(&message.body))
        );
        assert_eq!(
            blobs.get(message.hash).await,
            Ok(Some(encoding::encode(&message.body)))
        );
    }

    // Every other exchange puts exactly the bodies not stored before it.
    let mut stored: std::collections::BTreeSet<_> =
        first.messages.iter().map(|message| message.hash).collect();
    for exchange in exchanges.iter().skip(1) {
        pipeline.ingest(exchange.clone(), at).await.expect("ingest");
        let put = drain(&mut puts);
        // Known bodies are skipped; a body new to this exchange is put
        // wherever the request lists it.
        let expected = exchange
            .messages
            .iter()
            .filter(|message| !stored.contains(&message.hash))
            .count();
        assert_eq!(put.len(), expected + exchange.media.len());
        stored.extend(exchange.messages.iter().map(|message| message.hash));
        for message in &exchange.messages {
            assert!(blobs.get(message.hash).await.expect("get").is_some());
        }
    }
}
