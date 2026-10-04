//! [`OpenAiEmbedder`]: the spec's `Embedder` against an OpenAI-compatible
//! `POST <base_url>/embeddings` (OpenAI itself, or a self-hosted vLLM, TEI
//! or SGLang server), decision D1.
//!
//! Texts go in batches of at most [`OpenAiEmbedderConfig::batch_size`],
//! one batch at a time, in order. Each reply must hold exactly one vector
//! per input, its `index` values a permutation of the batch's positions;
//! vectors are put back in input order by `index`
//! (`analysis.embedder.one-per-input`), must have the model's dimension,
//! and are L2-normalized (in f64) before `Embedding::new` checks them, since
//! not every compatible server normalizes. Any failure is
//! `EmbedError::Model` with the [`EmbedderError`]'s text; the key never
//! appears in it or in a log.

use std::num::{NonZeroU16, NonZeroUsize};
use std::time::Duration;

use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::interfaces::l6_analysis::{EmbedError, Embedder};
use hyper::header::HeaderValue;
use hyper::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::remote::http::{BaseUrl, HttpCall, HttpClient, HttpError};

/// The route under the base URL (`https://api.openai.com/v1` +
/// `/embeddings`).
pub const EMBEDDINGS: &str = "/embeddings";

/// A bearer key. `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

/// Why the key could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiKeyError {
    #[error("the environment variable {env} is not set")]
    Unset { env: String },
    #[error("the environment variable {env} is not valid Unicode")]
    NotUnicode { env: String },
}

impl ApiKey {
    pub fn new(key: &str) -> Self {
        Self(key.to_owned())
    }

    /// The key in the environment variable `env` (config
    /// `embeddings.api_key.env`): `None` when it is set but empty (a keyless
    /// local endpoint), an error when it is unset.
    pub fn from_env(env: &str) -> Result<Option<Self>, ApiKeyError> {
        Self::from_lookup(env, |name| std::env::var(name))
    }

    /// As [`ApiKey::from_env`], reading through `lookup`.
    pub fn from_lookup(
        env: &str,
        lookup: impl FnOnce(&str) -> Result<String, std::env::VarError>,
    ) -> Result<Option<Self>, ApiKeyError> {
        match lookup(env) {
            Ok(key) if key.is_empty() => Ok(None),
            Ok(key) => Ok(Some(Self(key))),
            Err(std::env::VarError::NotPresent) => Err(ApiKeyError::Unset {
                env: env.to_owned(),
            }),
            Err(std::env::VarError::NotUnicode(_)) => Err(ApiKeyError::NotUnicode {
                env: env.to_owned(),
            }),
        }
    }

    fn header(&self) -> Result<HeaderValue, EmbedderError> {
        let mut value = HeaderValue::try_from(format!("Bearer {}", self.0))
            .map_err(|_| EmbedderError::KeyNotAHeader)?;
        value.set_sensitive(true);
        Ok(value)
    }
}

/// The endpoint and how to call it. The gateway builds it from config
/// `embeddings{base_url, model, api_key{env}}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiEmbedderConfig {
    pub base_url: BaseUrl,
    /// The `model` sent with every request, and the name of the
    /// [`EmbeddingModel`].
    pub model: String,
    pub api_key: Option<ApiKey>,
    pub timeout: Duration,
    pub batch_size: NonZeroUsize,
}

impl OpenAiEmbedderConfig {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
    /// OpenAI takes up to 2 048 inputs per request; 256 keeps each request
    /// and reply a few megabytes.
    pub const DEFAULT_BATCH_SIZE: NonZeroUsize = match NonZeroUsize::new(256) {
        Some(size) => size,
        None => NonZeroUsize::MIN,
    };

    pub fn new(base_url: BaseUrl, model: &str, api_key: Option<ApiKey>) -> Self {
        Self {
            base_url,
            model: model.to_owned(),
            api_key,
            timeout: Self::DEFAULT_TIMEOUT,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }
}

/// Why an embeddings call failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EmbedderError {
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error("the embeddings endpoint answered {status}: {body}")]
    Status { status: u16, body: String },
    #[error("the embeddings reply is not the expected JSON: {reason}")]
    Decode { reason: String },
    #[error("{got} embeddings for {expected} inputs")]
    Count { expected: usize, got: usize },
    #[error("embedding index {index} is out of range or repeated")]
    Index { index: usize },
    #[error("embedding {index} has {got} values, not {expected}")]
    Dimension {
        index: usize,
        expected: u16,
        got: usize,
    },
    #[error("embedding {index} is zero or not finite")]
    Degenerate { index: usize },
    #[error("the probe's embedding has {got} values, more than u16::MAX or none")]
    ProbeDimension { got: usize },
    #[error("the API key is not a valid header value")]
    KeyNotAHeader,
    #[error("encoding the request: {reason}")]
    Encode { reason: String },
}

impl From<EmbedderError> for EmbedError {
    fn from(error: EmbedderError) -> Self {
        EmbedError::Model {
            reason: error.to_string(),
        }
    }
}

#[derive(Serialize)]
struct EmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
    encoding_format: &'static str,
}

/// The reply, read leniently: an external API adds fields at will.
#[derive(Deserialize)]
struct EmbeddingsReply {
    data: Vec<EmbeddingItem>,
}

#[derive(Deserialize)]
struct EmbeddingItem {
    index: usize,
    embedding: Vec<f32>,
}

/// The spec's `Embedder` over an OpenAI-compatible endpoint.
#[derive(Debug, Clone)]
pub struct OpenAiEmbedder {
    config: OpenAiEmbedderConfig,
    http: HttpClient,
    model: EmbeddingModel,
}

impl OpenAiEmbedder {
    /// An embedder for a model whose dimension is known.
    pub fn new(config: OpenAiEmbedderConfig, http: HttpClient, dimension: NonZeroU16) -> Self {
        let model = EmbeddingModel {
            name: config.model.clone(),
            dimension,
        };
        Self {
            config,
            http,
            model,
        }
    }

    /// An embedder whose dimension is learned with one request embedding a
    /// fixed probe text (never transmission content).
    pub async fn connect(
        config: OpenAiEmbedderConfig,
        http: HttpClient,
    ) -> Result<Self, EmbedderError> {
        let probe = ["crosstalk dimension probe"];
        let reply = call(&config, &http, &probe).await?;
        let got = reply.first().map_or(0, Vec::len);
        let dimension = u16::try_from(got)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(EmbedderError::ProbeDimension { got })?;
        tracing::info!(model = %config.model, dimension = dimension.get(), "embedding model probed");
        Ok(Self::new(config, http, dimension))
    }

    async fn embed_batch(
        &self,
        texts: &[&str],
        offset: usize,
    ) -> Result<Vec<Embedding>, EmbedderError> {
        let vectors = call(&self.config, &self.http, texts).await?;
        vectors
            .into_iter()
            .enumerate()
            .map(|(position, values)| {
                let index = offset + position;
                if values.len() != usize::from(self.model.dimension.get()) {
                    return Err(EmbedderError::Dimension {
                        index,
                        expected: self.model.dimension.get(),
                        got: values.len(),
                    });
                }
                normalized(&self.model, &values).ok_or(EmbedderError::Degenerate { index })
            })
            .collect()
    }
}

impl Embedder for OpenAiEmbedder {
    fn model(&self) -> EmbeddingModel {
        self.model.clone()
    }

    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        let started = Instant::now();
        let mut embeddings = Vec::with_capacity(texts.len());
        for (batch, chunk) in texts.chunks(self.config.batch_size.get()).enumerate() {
            let offset = batch * self.config.batch_size.get();
            embeddings.extend(self.embed_batch(chunk, offset).await?);
        }
        tracing::debug!(
            model = %self.model.name,
            texts = texts.len(),
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "texts embedded"
        );
        Ok(embeddings)
    }
}

/// One request: the vectors of `texts`, in input order by `index`.
async fn call(
    config: &OpenAiEmbedderConfig,
    http: &HttpClient,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, EmbedderError> {
    let request = EmbeddingsRequest {
        model: &config.model,
        input: texts,
        encoding_format: "float",
    };
    let json = serde_json::to_vec(&request).map_err(|error| EmbedderError::Encode {
        reason: error.to_string(),
    })?;
    let authorization = config.api_key.as_ref().map(ApiKey::header).transpose()?;
    let call = HttpCall {
        method: Method::POST,
        uri: config.base_url.join(EMBEDDINGS)?,
        authorization,
        json: Some(json),
    };
    let reply = http.send(call, config.timeout).await.inspect_err(|error| {
        tracing::warn!(model = %config.model, inputs = texts.len(), %error, "embeddings call failed");
    })?;
    if reply.status != StatusCode::OK {
        let mut body: String = String::from_utf8_lossy(&reply.body)
            .chars()
            .take(512)
            .collect();
        // An endpoint may echo the key it refused; it never leaves here.
        if let Some(key) = config.api_key.as_ref().filter(|key| !key.0.is_empty()) {
            body = body.replace(&key.0, "<redacted>");
        }
        tracing::warn!(model = %config.model, status = reply.status.as_u16(), "embeddings call refused");
        return Err(EmbedderError::Status {
            status: reply.status.as_u16(),
            body,
        });
    }
    let parsed: EmbeddingsReply =
        serde_json::from_slice(&reply.body).map_err(|error| EmbedderError::Decode {
            reason: error.to_string(),
        })?;
    if parsed.data.len() != texts.len() {
        return Err(EmbedderError::Count {
            expected: texts.len(),
            got: parsed.data.len(),
        });
    }
    let mut ordered: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
    for item in parsed.data {
        match ordered.get_mut(item.index) {
            Some(slot @ None) => *slot = Some(item.embedding),
            _ => return Err(EmbedderError::Index { index: item.index }),
        }
    }
    // Every slot is filled: as many distinct in-range indices as slots.
    Ok(ordered.into_iter().flatten().collect())
}

/// `values` scaled to unit length, or `None` when that is impossible.
fn normalized(model: &EmbeddingModel, values: &[f32]) -> Option<Embedding> {
    let norm = values
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt();
    if !norm.is_normal() {
        return None;
    }
    // Rounding each normalized component to the nearest f32 is the intent.
    #[allow(clippy::cast_possible_truncation)]
    let unit = values
        .iter()
        .map(|v| (f64::from(*v) / norm) as f32)
        .collect();
    Embedding::new(model.clone(), unit).ok()
}
