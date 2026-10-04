//! The capture stage counts each refusal by reason and protocol: a request
//! body the normalizer refuses is `request_body`, an exchange of a protocol
//! no normalizer handles is `unsupported_protocol`, and the health total is
//! their sum.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::{Transport, WireProtocol};
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};

use super::raw;
use crate::capture::{CaptureStage, Captured, PipelineStats, PutRetry};
use crate::normalize_failure::{FailureReason, NormalizeFailure};

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
    let stats = Arc::new(PipelineStats::new());
    let mut stage = CaptureStage::new(
        MemoryBlobStore::new(),
        bus,
        Arc::new(SystemClock),
        UlidGenerator::new(Arc::new(SystemClock), SeededRandom::new(1)),
        Arc::clone(&stats),
        PutRetry {
            attempts: NonZeroU32::MIN,
            backoff: Duration::ZERO,
        },
    );
    let mut ids = Ids::seeded(7);

    let refused = raw::raw(&mut ids, UNKNOWN_ROLE, Transport::Http, response());
    assert_eq!(stage.capture(&refused).await, Captured::NotNormalized);
    let mut chat = raw::raw(&mut ids, VALID, Transport::Http, response());
    chat.meta.protocol = WireProtocol::OpenAiChat;
    assert_eq!(stage.capture(&chat).await, Captured::NotNormalized);
    assert_eq!(stage.capture(&chat).await, Captured::NotNormalized);

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
