//! Capture's bounds on request bodies: the tee and the decoded size. Past
//! either, the exchange is uncaptured and forwarding is untouched.

use std::io::Write;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::ContentEncoding;
use crosstalk_testkit::corpus::CorpusRequest;
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::header::{HeaderName, HeaderValue};

use super::support::{Options, TestProxy, case, limits_with, non_zero, start};
use crate::encoding::{self, EncodingError};

const MIB: usize = 1024 * 1024;

/// A valid Messages body of about `size` bytes.
fn padded_body(size: usize) -> Vec<u8> {
    let padding = "a".repeat(size);
    serde_json::json!({
        "model": "claude-opus-5-5",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": padding}],
    })
    .to_string()
    .into_bytes()
}

fn compressed(body: &[u8], encoding: ContentEncoding) -> Vec<u8> {
    match encoding {
        ContentEncoding::Identity => body.to_vec(),
        ContentEncoding::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
            let _ = encoder.write_all(body);
            encoder.finish().unwrap_or_default()
        }
        ContentEncoding::Zstd => zstd::stream::encode_all(body, 19).unwrap_or_default(),
    }
}

fn with_body(body: Vec<u8>, encoding: ContentEncoding) -> CorpusRequest {
    let mut request = case("text_turn").request;
    request.body = Bytes::from(body);
    let name = match encoding {
        ContentEncoding::Identity => return request,
        ContentEncoding::Gzip => "gzip",
        ContentEncoding::Zstd => "zstd",
    };
    request.headers.push(
        HeaderName::from_static("content-encoding"),
        HeaderValue::from_static(name),
    );
    request
}

async fn not_captured(proxy: &mut TestProxy) {
    proxy.settle(1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), proxy.captured.recv())
            .await
            .is_err(),
        "captured past the bound"
    );
}

/// A gzip or zstd body that decodes past the bound (a 16 MiB body that
/// compresses to a few kilobytes, against a 1 MiB bound) is forwarded with
/// its compressed bytes unchanged and counted as a decode error; one under
/// the bound is captured.
#[tokio::test]
async fn oversized_decode_aborts_capture() {
    let upstream = FakeUpstream::start(Script::new().case(&case("text_turn")))
        .await
        .expect("upstream");
    let limits = limits_with(|limits| limits.decoded_bytes = non_zero(MIB as u64));
    for encoding in [ContentEncoding::Gzip, ContentEncoding::Zstd] {
        let mut proxy = start(
            &upstream.base_url(),
            Options {
                limits,
                ..Options::default()
            },
        )
        .await;
        let bomb = compressed(&padded_body(16 * MIB), encoding);
        assert!(
            bomb.len() < 64 * 1024,
            "{encoding:?} compressed to {}",
            bomb.len()
        );
        let request = with_body(bomb, encoding);
        let response = proxy.client().send(&request).await.expect("answered");
        assert_eq!(response.status, hyper::StatusCode::OK);
        let received = upstream.received().await.expect("log");
        assert!(
            received
                .last()
                .expect("forwarded")
                .differences_from(&request)
                .is_empty()
        );
        not_captured(&mut proxy).await;
        assert_eq!(proxy.stats.snapshot().decode_error, 1, "{encoding:?}");

        let small = with_body(compressed(&padded_body(MIB / 2), encoding), encoding);
        let _ = proxy.client().send(&small).await.expect("answered");
        let raw = proxy
            .next_capture()
            .await
            .expect("captured under the bound");
        assert_eq!(raw.request.encoding, encoding);
    }
    let direct = encoding::decode(
        &compressed(&padded_body(4 * MIB), ContentEncoding::Zstd),
        ContentEncoding::Zstd,
        MIB as u64,
    );
    assert_eq!(
        direct,
        Err(EncodingError::TooLarge {
            encoding: ContentEncoding::Zstd,
            limit: MIB as u64
        })
    );
}

/// A body larger than the tee bound is forwarded whole, never decoded, and
/// counted uncaptured.
#[tokio::test]
async fn oversized_body_tee_abandons_capture() {
    let upstream = FakeUpstream::start(Script::new().case(&case("text_turn")))
        .await
        .expect("upstream");
    let limits = limits_with(|limits| limits.request_tee_bytes = non_zero(16 * 1024));
    let mut proxy = start(
        &upstream.base_url(),
        Options {
            limits,
            ..Options::default()
        },
    )
    .await;
    let request = with_body(padded_body(256 * 1024), ContentEncoding::Identity);
    let response = proxy.client().send(&request).await.expect("answered");
    assert_eq!(response.status, hyper::StatusCode::OK);
    let received = upstream.received().await.expect("log");
    let last = received.last().expect("forwarded");
    assert_eq!(last.body.len(), request.body.len());
    assert!(last.differences_from(&request).is_empty());
    not_captured(&mut proxy).await;
    assert_eq!(proxy.stats.snapshot().decode_error, 1);
}
