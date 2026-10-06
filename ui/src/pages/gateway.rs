//! The full-page state of a gateway the http backend cannot use: it is
//! unreachable, or it refused the token.
//!
//! ```text
//! page ─▶ view_state ─▶ present fails ─▶ defaults_error(cx, error)
//!   http backend and classify(error) = Some(failure)
//!     ─▶ rewrite("/_gateway") as GET, carrying GatewayDown { failure, url }
//!        ─▶ gateway_page: 503 "unreachable" | 502 "token refused", in the layout
//!   anything else (403 included) ─▶ as before
//! ```
//!
//! The browser's URL stays the page it asked for, so reloading retries.
//! The page shows the gateway's URL without credentials
//! ([`public_url`]) and never the token. `/_gateway` asked for directly,
//! with nothing carried, is a 404. The layout skips its own present read
//! for this page (`root_layout`), so a dead gateway is not asked twice.

use topcoat::Result;
use topcoat::context::{Cx, try_request_context};
use topcoat::router::error::{not_found, rewrite};
use topcoat::router::request::{headers, original_uri};
use topcoat::router::{Body, StatusCode, header, page};
use topcoat::view::{View, view};

use crate::app::{AppBackend, backend};
use crate::backend::http::failure::{GatewayFailure, classify, public_url};
use crate::components::page_header;
use crosstalk_spec::interfaces::l8_surface::QueryError;

/// Where a gateway failure is rendered.
const PATH: &str = "/_gateway";

/// A gateway failure carried into the rewritten request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayDown {
    pub failure: GatewayFailure,
    /// The gateway's URL, without credentials.
    pub url: String,
}

/// The gateway failure `error` is, when the http backend failed with one.
pub fn gateway_down(cx: &Cx, error: &QueryError) -> Option<GatewayDown> {
    let AppBackend::Http(client) = backend(cx) else {
        return None;
    };
    let failure = classify(error)?;
    Some(GatewayDown {
        failure,
        url: public_url(client.base()),
    })
}

/// The router error that renders `down` in place of the page: the request
/// handled again as a `GET` of [`PATH`], without its body.
pub fn render(cx: &Cx, down: GatewayDown) -> topcoat::Error {
    tracing::error!(
        backend = "http",
        failure = ?down.failure,
        url = %down.url,
        path = %original_uri(cx).path(),
        "gateway unavailable; rendering the gateway page"
    );
    let mut headers = headers(cx).clone();
    headers.remove(header::CONTENT_TYPE);
    headers.remove(header::CONTENT_LENGTH);
    rewrite(PATH, Body::empty())
        .method(topcoat::router::Method::GET)
        .headers(headers)
        .with(down)
        .into()
}

/// The gateway failure this request renders, if it was rewritten here.
pub fn carried(cx: &Cx) -> Option<&GatewayDown> {
    try_request_context::<GatewayDown>(cx)
}

#[page("/_gateway")]
async fn gateway_page(cx: &Cx) -> Result<impl View> {
    let Some(down) = carried(cx).cloned() else {
        return Err(not_found().into());
    };
    let retry = original_uri(cx).to_string();
    let (status, title, explanation) = match down.failure {
        GatewayFailure::Unreachable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "The gateway is unreachable",
            "The UI could not connect to the gateway's API, or it stopped answering. \
             Check that the gateway is running and reachable from the UI, then reload.",
        ),
        GatewayFailure::TokenRefused => (
            StatusCode::BAD_GATEWAY,
            "The gateway refused the token",
            "The gateway answered 401: it does not accept the UI's API token. \
             Check that the UI's token variable holds the gateway's current API token, \
             then restart the UI.",
        ),
    };
    Ok(view! {
        (status)
        page_header(title: title, subtitle: "")
        <div class="rounded border border-red-300 bg-red-50 p-4 text-sm text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-200">
            <p>(explanation)</p>
            <p class="mt-2">"Gateway: " <code class="font-mono">(down.url.clone())</code></p>
        </div>
        <p class="mt-4 text-sm"><a class="underline" href=(retry)>"Try again"</a></p>
    })
}
