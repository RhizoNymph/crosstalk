//! The capture stage counts each refusal by reason and protocol: a request
//! body the normalizer refuses is `request_body`, an exchange of a protocol
//! no normalizer handles is `unsupported_protocol`, and the health total is
//! their sum.

use std::sync::Arc;

use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::{Transport, WireProtocol};
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};

use super::raw;
use crate::capture::{CaptureError, CaptureStage, Refusal};
use crate::normalize_failure::{FailureReason, NormalizeFailure};
use crate::pipeline::{Deps, Pipeline, Settings};

const VALID: &[u8] = br#"{"model":"m","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#;
const UNKNOWN_ROLE: &[u8] =
    br#"{"model":"m","max_tokens":8,"messages":[{"role":"developer","content":"hi"}]}"#;

fn response() -> RawResponse {
    RawResponse::Complete {
        status: 200,
        body: b"{}".to_vec(),
    }
}

pub async fn refusals_are_counted_by_reason_and_protocol() {
    let bus = MpscBus::start(BusConfig::default()).expect("the bus starts");
    let pipeline = Pipeline::build(
        Settings::default(),
        Deps::stores(MemoryBlobStore::new(), bus, SeededRandom::new(1)),
        Arc::new(SystemClock),
    )
    .await
    .expect("the pipeline builds");
    let stats = Arc::clone(pipeline.stats());
    let stage = CaptureStage::new(pipeline.ingester());
    let mut ids = Ids::seeded(7);

    let refused = raw::raw(&mut ids, UNKNOWN_ROLE, Transport::Http, response());
    assert!(matches!(
        stage.capture(&refused).await,
        Err(CaptureError::NotNormalized(Refusal::Refused {
            protocol: WireProtocol::AnthropicMessages,
            ..
        }))
    ));
    let mut chat = raw::raw(&mut ids, VALID, Transport::Http, response());
    chat.meta.protocol = WireProtocol::OpenAiChat;
    for _ in 0..2 {
        assert_eq!(
            stage.capture(&chat).await,
            Err(CaptureError::NotNormalized(Refusal::UnsupportedProtocol(
                WireProtocol::OpenAiChat
            )))
        );
    }

    let failures = stats.normalize_failures();
    let body = NormalizeFailure {
        reason: FailureReason::RequestBody,
        protocol: WireProtocol::AnthropicMessages,
    };
    let unsupported = NormalizeFailure {
        reason: FailureReason::UnsupportedProtocol,
        protocol: WireProtocol::OpenAiChat,
    };
    assert_eq!(failures.get(body), 1);
    assert_eq!(failures.get(unsupported), 2);
    assert_eq!(failures.total(), 3);
    assert_eq!(stats.snapshot().normalize_failed, 3);
}
