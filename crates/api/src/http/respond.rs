//! Building responses: a success's JSON, an error's status and JSON, the
//! 401, and the `no-store` default every response but a ready frame keeps.

use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::http::auth::AuthError;
use crosstalk_spec::interfaces::l8_surface::http::{ErrorStatus, JSON, Status};
use serde::Serialize;

/// `Cache-Control` of every response but a ready frame.
pub(super) const NO_STORE: &str = "no-store";

/// The status code of a binding status.
pub(super) fn code(status: Status) -> StatusCode {
    match status {
        Status::Ok => StatusCode::OK,
        Status::Accepted => StatusCode::ACCEPTED,
        Status::NotModified => StatusCode::NOT_MODIFIED,
        Status::BadRequest => StatusCode::BAD_REQUEST,
        Status::Unauthorized => StatusCode::UNAUTHORIZED,
        Status::Forbidden => StatusCode::FORBIDDEN,
        Status::NotFound => StatusCode::NOT_FOUND,
        Status::Conflict => StatusCode::CONFLICT,
        Status::Gone => StatusCode::GONE,
        Status::UnprocessableContent => StatusCode::UNPROCESSABLE_ENTITY,
        Status::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
        Status::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// A response with `status`, a body and `Cache-Control: no-store`.
pub(super) fn with_body(status: Status, content_type: &'static str, body: Body) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = code(status);
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    response
}

/// `value` as JSON with `status`. A value with no JSON (a timestamp after
/// year 9999) is a fault of the store that produced it: `Store`.
pub(super) fn json<T: Serialize>(status: Status, value: &T) -> Result<Response, QueryError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        tracing::error!(error = %error, "a response value has no JSON");
        QueryError::Store {
            reason: format!("the response has no JSON: {error}"),
        }
    })?;
    Ok(with_body(status, JSON, Body::from(bytes)))
}

/// An error: its status and its wire JSON.
pub(super) fn error<E: ErrorStatus + Serialize>(error: &E) -> Response {
    let status = error.status();
    match serde_json::to_vec(error) {
        Ok(bytes) => with_body(status, JSON, Body::from(bytes)),
        Err(encode) => {
            // Errors hold ids, enums and strings, all of which encode; this
            // is unreachable in practice, and still answered.
            tracing::error!(error = %encode, "an error has no JSON");
            with_body(Status::ServiceUnavailable, JSON, Body::empty())
        }
    }
}

/// The 401 for a request with no caller: the `AuthError` and its bearer
/// challenge.
pub(super) fn unauthorized(refused: &AuthError) -> Response {
    let mut response = error(refused);
    if let Ok(challenge) = HeaderValue::from_str(&refused.www_authenticate()) {
        response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
    }
    response
}

/// Adds `Cache-Control: no-store` to a response that has no
/// `Cache-Control` (one axum or a layer wrote), so nothing leaves without
/// one (`surface.http.responses-not-shared`).
pub(super) async fn default_no_store(mut response: Response) -> Response {
    if !response.headers().contains_key(CACHE_CONTROL) {
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    }
    response
}

/// A header value for text the server wrote (ids, digests, file names:
/// ASCII by construction); `Store` if it somehow is not one.
pub(super) fn header_value(text: &str) -> Result<HeaderValue, QueryError> {
    HeaderValue::from_str(text).map_err(|_| QueryError::Store {
        reason: format!("`{text}` is not a header value"),
    })
}
