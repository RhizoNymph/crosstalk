//! [`HttpClient`]: one connection pool to one surface, the credential its
//! requests carry, and the exchange every route goes through.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crosstalk_spec::interfaces::l8_surface::http::{
    EncodedRequest, JSON, Method, RequestBuilder, Route,
};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use hyper::{Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;

use crate::body;
use crate::config::{BaseUrl, BearerToken, ClientConfig};
use crate::error::{ApiError, ClientError, TransportError, decode_error};
use crate::form;
use crate::frame::FrameCache;
use crate::hasher::Blake3RowHasher;

/// What every request says it is: the crate and its version, nothing
/// about the operator or the host.
const USER_AGENT_VALUE: &str = concat!("crosstalk-client/", env!("CARGO_PKG_VERSION"));

/// A client of one surface over HTTP: `QueryApi`, `OperatorActions` and
/// `LiveFeed` for the caller its token names.
///
/// Cheap to clone; clones share the connection pool and the frame cache.
/// [`HttpClient::with_token`] gives a client for another credential over
/// the same pool, so a server rendering pages for several operators keeps
/// one pool and a client per session.
///
/// **The caller.** The trait methods take a `&Caller`, which never travels
/// (`surface.api.caller-from-session`): the surface derives the caller
/// from the credential this client presents, and the argument is ignored.
///
/// `H` is the row hasher an export is verified with; the surface's is
/// BLAKE3 ([`Blake3RowHasher`]).
pub struct HttpClient<H = Blake3RowHasher> {
    shared: Arc<Shared>,
    token: Option<BearerToken>,
    hasher: PhantomData<fn() -> H>,
}

pub(crate) struct Shared {
    http: Client<HttpConnector, Full<Bytes>>,
    base: BaseUrl,
    config: ClientConfig,
    /// Ready frames by projection, for `If-None-Match`. Held only between
    /// awaits, never across one.
    pub(crate) frames: Mutex<FrameCache>,
}

impl<H> Clone for HttpClient<H> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            token: self.token.clone(),
            hasher: PhantomData,
        }
    }
}

impl<H> std::fmt::Debug for HttpClient<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("base", &self.shared.base)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl HttpClient<Blake3RowHasher> {
    /// A client of the surface at `base`, with no credential: in trusted
    /// mode that is enough, otherwise give it one with
    /// [`HttpClient::with_token`]. Requests need a Tokio runtime.
    pub fn new(base: BaseUrl, config: ClientConfig) -> Self {
        let http = Client::builder(TokioExecutor::new()).build(HttpConnector::new());
        Self {
            shared: Arc::new(Shared {
                http,
                base,
                frames: Mutex::new(FrameCache::new(config.frame_cache())),
                config,
            }),
            token: None,
            hasher: PhantomData,
        }
    }
}

impl<H> HttpClient<H> {
    /// The same client presenting `token` as `Authorization: Bearer`.
    pub fn with_token(&self, token: BearerToken) -> Self {
        Self {
            token: Some(token),
            ..self.clone()
        }
    }

    /// The same client presenting no credential.
    pub fn without_token(&self) -> Self {
        Self {
            token: None,
            ..self.clone()
        }
    }

    /// The same client verifying exports with the row hasher `G`.
    pub fn with_row_hasher<G>(&self) -> HttpClient<G> {
        HttpClient {
            shared: Arc::clone(&self.shared),
            token: self.token.clone(),
            hasher: PhantomData,
        }
    }

    pub fn base(&self) -> &BaseUrl {
        &self.shared.base
    }

    pub fn config(&self) -> &ClientConfig {
        &self.shared.config
    }

    pub(crate) fn shared(&self) -> &Shared {
        &self.shared
    }

    /// Sends one encoded call: its method, path and form-encoded query
    /// under the base URL, its argument headers, its JSON body, the
    /// credential, `Accept: accept` and `extra` headers. Returns once the
    /// response head is read; the caller bounds the wait.
    pub(crate) async fn send(
        &self,
        route: Route,
        request: EncodedRequest,
        accept: &'static str,
        extra: HeaderMap,
    ) -> Result<Response<Incoming>, TransportError> {
        let target = self
            .shared
            .base
            .join(&request.path, &form::encode(&request.query));
        let method = match request.method {
            Method::Get => hyper::Method::GET,
            Method::Post => hyper::Method::POST,
        };
        let mut builder = Request::builder()
            .method(method)
            .uri(target)
            .header(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE))
            .header(ACCEPT, HeaderValue::from_static(accept));
        if let Some(token) = &self.token {
            builder = builder.header(AUTHORIZATION, token.header().clone());
        }
        for (name, value) in &request.headers {
            builder = builder.header(*name, value.as_str());
        }
        if let Some(headers) = builder.headers_mut() {
            headers.extend(extra);
        }
        let body = match request.body {
            Some(json) => {
                builder = builder.header(CONTENT_TYPE, HeaderValue::from_static(JSON));
                Full::new(Bytes::from(json))
            }
            None => Full::new(Bytes::new()),
        };
        let request = builder.body(body).map_err(TransportError::Build)?;
        tracing::debug!(
            route = ?route,
            method = %request.method(),
            path = %request.uri().path(),
            "sending"
        );
        self.shared
            .http
            .request(request)
            .await
            .map_err(TransportError::Send)
    }

    /// One call of a route that answers with one body, read whole within
    /// the request timeout: the route's success status and `content_type`
    /// give the body, any other status the error it carries.
    pub(crate) async fn exchange<E: ApiError>(
        &self,
        route: Route,
        build: impl FnOnce(RequestBuilder) -> RequestBuilder,
        accept: &'static str,
        extra: HeaderMap,
    ) -> Result<Exchanged, ClientError<E>> {
        let request = build(RequestBuilder::new(route))
            .build()
            .map_err(ClientError::Encode)?;
        let config = self.shared.config;
        let exchange = async {
            let response = self.send(route, request, accept, extra).await?;
            let (parts, body) = response.into_parts();
            let body = body::collect(body, config.max_response_bytes()).await?;
            Ok::<_, TransportError>(Exchanged {
                status: parts.status.as_u16(),
                headers: parts.headers,
                body,
            })
        };
        let exchanged = tokio::time::timeout(config.request_timeout(), exchange)
            .await
            .map_err(|_| TransportError::Timeout {
                millis: config.request_timeout().as_millis(),
            })??;
        tracing::debug!(route = ?route, status = exchanged.status, bytes = exchanged.body.len(), "answered");
        Ok(exchanged)
    }

    /// One JSON call: the route's `Ok` value decoded from its success body.
    pub(crate) async fn call<T: DeserializeOwned, E: ApiError>(
        &self,
        route: Route,
        build: impl FnOnce(RequestBuilder) -> RequestBuilder,
    ) -> Result<T, ClientError<E>> {
        let exchanged = self.exchange(route, build, JSON, HeaderMap::new()).await?;
        exchanged.json(route)
    }
}

/// A response read whole.
#[derive(Debug)]
pub(crate) struct Exchanged {
    pub(crate) status: u16,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl Exchanged {
    /// The route's JSON success, or the error the response carries.
    pub(crate) fn json<T: DeserializeOwned, E: ApiError>(
        &self,
        route: Route,
    ) -> Result<T, ClientError<E>> {
        self.success(route, JSON)?;
        serde_json::from_slice(&self.body).map_err(|error| {
            ClientError::unexpected(
                route,
                self.status,
                format!("the body is not the route's result: {error}"),
            )
        })
    }

    /// `Ok` for the route's success status with `content_type`; the error
    /// the response carries for any other status.
    pub(crate) fn success<E: ApiError>(
        &self,
        route: Route,
        content_type: &str,
    ) -> Result<(), ClientError<E>> {
        if self.status != route.spec().success.status.code() {
            return Err(decode_error(route, self.status, &self.body));
        }
        expect_content_type(route, self.status, &self.headers, content_type)
    }
}

/// The `Content-Type` essence (parameters dropped), lower-cased.
pub(crate) fn content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(|essence| essence.trim().to_ascii_lowercase())
}

/// A success of the wrong content type is not the route's body.
pub(crate) fn expect_content_type<E: std::fmt::Debug>(
    route: Route,
    status: u16,
    headers: &HeaderMap,
    expected: &str,
) -> Result<(), ClientError<E>> {
    match content_type(headers) {
        Some(essence) if essence == expected => Ok(()),
        other => Err(ClientError::unexpected(
            route,
            status,
            format!("content-type {other:?}, expected {expected}"),
        )),
    }
}
