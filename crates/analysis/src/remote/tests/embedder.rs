//! `OpenAiEmbedder` against a fake OpenAI-compatible endpoint.

use std::num::{NonZeroU16, NonZeroUsize};
use std::time::Duration;

use crosstalk_spec::interfaces::l6_analysis::{EmbedError, Embedder};
use crosstalk_testkit::upstream::{FakeUpstream, Fault, Reply};
use hyper::StatusCode;
use proptest::prelude::*;
use serde_json::json;

use super::{fake, raw, request_json};
use crate::remote::embedder::{
    ApiKey, ApiKeyError, EmbedderError, OpenAiEmbedder, OpenAiEmbedderConfig,
};
use crate::remote::http::{BaseUrl, HttpClient};

fn config(upstream: &FakeUpstream, key: Option<&str>, batch: usize) -> OpenAiEmbedderConfig {
    let base = BaseUrl::new(&format!("{}/v1", upstream.base_url())).unwrap();
    let mut config =
        OpenAiEmbedderConfig::new(base, "text-embedding-3-small", key.map(ApiKey::new));
    config.batch_size = NonZeroUsize::new(batch).unwrap();
    config.timeout = Duration::from_secs(5);
    config
}

fn embedder(upstream: &FakeUpstream, key: Option<&str>, batch: usize) -> OpenAiEmbedder {
    OpenAiEmbedder::new(
        config(upstream, key, batch),
        HttpClient::new(),
        NonZeroU16::new(3).unwrap(),
    )
}

/// The vector the fake gives input `i` of the whole call: distinct for
/// every `i`, not normalized (the adapter normalizes).
fn vector(i: usize) -> Vec<f32> {
    let i = f32::from(u16::try_from(i).unwrap());
    vec![1.0 + i, 2.0, 0.5 * i]
}

/// An OpenAI reply for inputs `range` of the call, listed in `order` (a
/// permutation of the batch's positions).
fn reply(offset: usize, order: &[usize]) -> Reply {
    let data: Vec<serde_json::Value> = order
        .iter()
        .map(|&position| {
            json!({"object": "embedding", "index": position, "embedding": vector(offset + position)})
        })
        .collect();
    Reply::json(
        StatusCode::OK,
        &json!({"object": "list", "data": data, "model": "text-embedding-3-small", "usage": {"prompt_tokens": 1, "total_tokens": 1}}),
    )
}

fn normalized(values: &[f32]) -> Vec<f32> {
    let norm = values
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt();
    #[allow(clippy::cast_possible_truncation)]
    values
        .iter()
        .map(|v| (f64::from(*v) / norm) as f32)
        .collect()
}

#[tokio::test]
async fn api_embedder_returns_one_vector_per_text() {
    // analysis.embedder.one-per-input, for the API embedder.
    let upstream = fake().await;
    let embedder = embedder(&upstream, Some("sk-test"), 256);
    upstream.reply_next(reply(0, &[2, 0, 1])).await.unwrap();
    let texts = ["wiki page", "deploy", "lunch"];
    let embeddings = embedder.embed(&texts).await.unwrap();
    assert_eq!(embeddings.len(), texts.len());
    for (i, embedding) in embeddings.iter().enumerate() {
        assert_eq!(embedding.values(), normalized(&vector(i)).as_slice());
        assert_eq!(*embedding.model(), embedder.model());
    }
    let received = upstream.received().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].target, "/v1/embeddings");
    assert_eq!(
        received[0].headers.get_str("authorization"),
        Some("Bearer sk-test")
    );
    assert_eq!(
        request_json(&upstream, 0).await,
        json!({"model": "text-embedding-3-small", "input": texts, "encoding_format": "float"})
    );
}

#[tokio::test]
async fn keyless_endpoint_gets_no_authorization_and_empty_input_no_request() {
    let upstream = fake().await;
    let embedder = embedder(&upstream, None, 256);
    assert_eq!(embedder.embed(&[]).await, Ok(Vec::new()));
    assert!(upstream.received().await.unwrap().is_empty());
    upstream.reply_next(reply(0, &[0])).await.unwrap();
    embedder.embed(&["one"]).await.unwrap();
    let received = upstream.received().await.unwrap();
    assert_eq!(received[0].headers.get_str("authorization"), None);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn embed_preserves_count_and_order(
        count in 1usize..20,
        batch in 1usize..6,
        shuffle in any::<u64>(),
    ) {
        // analysis.embedder.one-per-input: whatever order each batch's
        // reply lists its vectors in, and however the texts are batched,
        // output i is input i's vector.
        let texts: Vec<String> = (0..count).map(|i| format!("text {i}")).collect();
        let texts: Vec<&str> = texts.iter().map(String::as_str).collect();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (embeddings, requests) = runtime.block_on(async {
            let upstream = fake().await;
            let embedder = embedder(&upstream, None, batch);
            for (n, chunk) in texts.chunks(batch).enumerate() {
                let mut order: Vec<usize> = (0..chunk.len()).collect();
                // A seeded rotation and reversal: a permutation per batch.
                let shift = usize::try_from(shuffle.rotate_left(u32::try_from(n).unwrap()) % 7).unwrap();
                order.rotate_left(shift % chunk.len());
                if shuffle & (1 << n) != 0 {
                    order.reverse();
                }
                upstream.reply_next(reply(n * batch, &order)).await.unwrap();
            }
            let embeddings = embedder.embed(&texts).await.unwrap();
            (embeddings, upstream.received().await.unwrap().len())
        });
        prop_assert_eq!(embeddings.len(), count);
        prop_assert_eq!(requests, count.div_ceil(batch));
        for (i, embedding) in embeddings.iter().enumerate() {
            let want = normalized(&vector(i));
            prop_assert_eq!(embedding.values(), want.as_slice());
        }
    }
}

#[tokio::test]
async fn malformed_replies_are_model_errors() {
    let three = |data: serde_json::Value| Reply::json(StatusCode::OK, &json!({"data": data}));
    let broken = [
        // Two vectors for three inputs.
        three(
            json!([{"index": 0, "embedding": [1.0, 0.0, 0.0]}, {"index": 1, "embedding": [1.0, 0.0, 0.0]}]),
        ),
        // A repeated index.
        three(
            json!([{"index": 0, "embedding": [1.0, 0.0, 0.0]}, {"index": 0, "embedding": [1.0, 0.0, 0.0]}, {"index": 2, "embedding": [1.0, 0.0, 0.0]}]),
        ),
        // An index out of range.
        three(
            json!([{"index": 0, "embedding": [1.0, 0.0, 0.0]}, {"index": 1, "embedding": [1.0, 0.0, 0.0]}, {"index": 3, "embedding": [1.0, 0.0, 0.0]}]),
        ),
        // The wrong dimension.
        three(
            json!([{"index": 0, "embedding": [1.0, 0.0]}, {"index": 1, "embedding": [1.0, 0.0, 0.0]}, {"index": 2, "embedding": [1.0, 0.0, 0.0]}]),
        ),
        // A zero vector.
        three(
            json!([{"index": 0, "embedding": [0.0, 0.0, 0.0]}, {"index": 1, "embedding": [1.0, 0.0, 0.0]}, {"index": 2, "embedding": [1.0, 0.0, 0.0]}]),
        ),
        raw(StatusCode::OK, "{\"data\": 7}"),
        Reply::json(
            StatusCode::UNAUTHORIZED,
            &json!({"error": {"message": "Incorrect API key provided: sk-test"}}),
        ),
        Reply::json(
            StatusCode::TOO_MANY_REQUESTS,
            &json!({"error": {"message": "slow down"}}),
        ),
        Reply::json(StatusCode::OK, &json!({"data": []}))
            .with_fault(Fault::Disconnect { after_chunks: 0 }),
    ];
    for reply in broken {
        let upstream = fake().await;
        let embedder = embedder(&upstream, Some("sk-test"), 256);
        upstream.reply_next(reply.clone()).await.unwrap();
        let result = embedder.embed(&["a", "b", "c"]).await;
        match result {
            Err(EmbedError::Model { reason }) => assert!(!reason.contains("sk-test"), "{reason}"),
            other => panic!("{reply:?} gave {other:?}"),
        }
        assert!(!format!("{embedder:?}").contains("sk-test"));
    }
}

#[tokio::test]
async fn timeout_is_a_model_error() {
    let upstream = fake().await;
    let mut config = config(&upstream, None, 256);
    config.timeout = Duration::from_millis(200);
    let embedder = OpenAiEmbedder::new(config, HttpClient::new(), NonZeroU16::new(3).unwrap());
    upstream
        .reply_next(reply(0, &[0]).with_fault(Fault::NoResponse))
        .await
        .unwrap();
    let result = embedder.embed(&["a"]).await;
    assert!(
        matches!(&result, Err(EmbedError::Model { reason }) if reason.contains("within 200 ms")),
        "{result:?}"
    );
}

#[tokio::test]
async fn connect_learns_the_dimension_from_a_probe() {
    let upstream = fake().await;
    upstream
        .reply_next(Reply::json(
            StatusCode::OK,
            &json!({"data": [{"index": 0, "embedding": [0.1, 0.2, 0.3, 0.4, 0.5]}]}),
        ))
        .await
        .unwrap();
    let embedder = OpenAiEmbedder::connect(config(&upstream, None, 256), HttpClient::new())
        .await
        .unwrap();
    assert_eq!(embedder.model().dimension.get(), 5);
    assert_eq!(embedder.model().name, "text-embedding-3-small");
    let probe = request_json(&upstream, 0).await;
    assert_eq!(probe["input"], json!(["crosstalk dimension probe"]));

    upstream
        .reply_next(Reply::json(
            StatusCode::OK,
            &json!({"data": [{"index": 0, "embedding": []}]}),
        ))
        .await
        .unwrap();
    assert!(matches!(
        OpenAiEmbedder::connect(config(&upstream, None, 256), HttpClient::new()).await,
        Err(EmbedderError::ProbeDimension { got: 0 })
    ));
}

#[test]
fn api_key_comes_from_the_environment_and_never_shows() {
    let key = ApiKey::from_lookup("KEY", |_| Ok("sk-secret".to_owned())).unwrap();
    assert_eq!(key, Some(ApiKey::new("sk-secret")));
    assert!(!format!("{key:?}").contains("sk-secret"));
    assert_eq!(ApiKey::from_lookup("KEY", |_| Ok(String::new())), Ok(None));
    assert_eq!(
        ApiKey::from_lookup("KEY", |_| Err(std::env::VarError::NotPresent)),
        Err(ApiKeyError::Unset {
            env: "KEY".to_owned()
        })
    );
}

#[test]
fn base_urls_are_checked_and_joined() {
    assert!(BaseUrl::new("ftp://example.com").is_err());
    assert!(BaseUrl::new("not a url").is_err());
    assert!(BaseUrl::new("http://example.com/v1?x=1").is_err());
    let base = BaseUrl::new("https://api.openai.com/v1/").unwrap();
    assert_eq!(base.as_str(), "https://api.openai.com/v1");
    assert_eq!(
        base.join("/embeddings").unwrap().to_string(),
        "https://api.openai.com/v1/embeddings"
    );
}
