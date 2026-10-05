//! Contract tests against a fake HTTP server (`crosstalk_testkit`'s
//! `FakeUpstream`), the shared fixture files of `sidecar/topics/tests/`,
//! and one ignored test against the real sidecar.

mod contract;
mod embedder;
mod layout;
mod live;
mod matrix;
mod topics;

use std::num::{NonZeroU16, NonZeroU64};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use hyper::StatusCode;

use crate::remote::http::HttpClient;
use crate::remote::sidecar::{SidecarClient, SidecarConfig};

/// A 3-dimensional model, small enough to write vectors by hand.
pub(crate) fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "test-model".to_owned(),
        dimension: NonZeroU16::new(3).unwrap(),
    }
}

pub(crate) fn other_model() -> EmbeddingModel {
    EmbeddingModel {
        name: "other-model".to_owned(),
        dimension: NonZeroU16::new(3).unwrap(),
    }
}

/// `[x, y, z]` normalized, under `model`.
pub(crate) fn unit_of(model: &EmbeddingModel, x: f32, y: f32, z: f32) -> Embedding {
    let norm = (x * x + y * y + z * z).sqrt();
    Embedding::new(model.clone(), vec![x / norm, y / norm, z / norm]).unwrap()
}

pub(crate) fn unit(x: f32, y: f32, z: f32) -> Embedding {
    unit_of(&model(), x, y, z)
}

pub(crate) fn ts(seconds: u64) -> Timestamp {
    Timestamp::from_micros(1_790_000_000_000_000 + seconds * 1_000_000)
}

/// A fake sidecar answering 404 to anything not queued.
pub(crate) async fn fake() -> FakeUpstream {
    FakeUpstream::start(Script::new()).await.unwrap()
}

/// A client for `upstream` with a short deadline.
pub(crate) fn client_for(upstream: &FakeUpstream) -> SidecarClient {
    client_with_timeout(upstream, Duration::from_secs(5))
}

pub(crate) fn client_with_timeout(upstream: &FakeUpstream, timeout: Duration) -> SidecarClient {
    let millis = u64::try_from(timeout.as_millis()).unwrap();
    let config =
        SidecarConfig::new(&upstream.base_url(), NonZeroU64::new(millis).unwrap()).unwrap();
    SidecarClient::new(config, HttpClient::new())
}

/// A reply whose body is `body` exactly.
pub(crate) fn raw(status: StatusCode, body: &str) -> Reply {
    Reply {
        chunks: vec![Bytes::from(body.to_owned())],
        ..Reply::json(status, &serde_json::Value::Null)
    }
}

/// The JSON body of the `index`th request the fake received.
pub(crate) async fn request_json(upstream: &FakeUpstream, index: usize) -> serde_json::Value {
    let received = upstream.received().await.unwrap();
    serde_json::from_slice(&received[index].body).unwrap()
}
