//! The router, registered from the spec's route table.
//!
//! [`router`] walks `Route::all()` and mounts each row's method and path
//! template (axum's `{param}` syntax is the table's) once, with a handler
//! that knows its [`Target`]: one route, or the actions endpoint, which
//! every `Route::Action(kind)` shares. Nothing here names a route, so a row
//! added to the table is mounted without a change here; what it calls is
//! decided by `dispatch::query`, which does not compile until it serves it.
//!
//! Every handler, the fallback included, authenticates before anything
//! else, so a request with no caller is a 401 whatever its path
//! (`surface.http.unauthenticated-401`). A path no route serves, or a
//! method the path does not answer (anything but its `GET`, `HEAD` for a
//! `GET`, or `POST`), is `404 NotFound`, as `resolve` answers it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::{MethodFilter, MethodRouter};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::http::{Method, Route, Target};

use super::input::{Input, Unread};
use super::{Shared, Surface, actions, dispatch, respond};

type Api<S> = State<Arc<Shared<S>>>;

/// Every route of the table, the not-found fallback, and the `no-store`
/// default.
pub(super) fn router<S: Surface>(shared: Arc<Shared<S>>) -> Router {
    let mut mounted = BTreeSet::new();
    let mut paths: BTreeMap<&'static str, MethodRouter<Arc<Shared<S>>>> = BTreeMap::new();
    for route in Route::all() {
        let spec = route.spec();
        if !mounted.insert((spec.path, spec.method.as_str())) {
            // Another kind of the one actions endpoint.
            continue;
        }
        let target = match route {
            Route::Action(_) => Target::Actions,
            route => Target::Route(route),
        };
        let filter = match spec.method {
            Method::Get => MethodFilter::GET,
            Method::Post => MethodFilter::POST,
        };
        let handler =
            move |State(shared): Api<S>, request: Request| answer(shared, target, request);
        let methods = paths.remove(spec.path).unwrap_or_default();
        paths.insert(spec.path, methods.on(filter, handler));
    }
    let mut router = Router::new();
    for (path, methods) in paths {
        router = router.route(path, methods.fallback(not_found::<S>));
    }
    router
        .fallback(not_found::<S>)
        .with_state(shared)
        .layer(axum::middleware::map_response(respond::default_no_store))
}

/// One request to a mounted route: the caller first, then the route.
async fn answer<S: Surface>(shared: Arc<Shared<S>>, target: Target, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let caller = match shared.auth.caller(&parts.headers) {
        Ok(caller) => caller,
        Err(refused) => {
            tracing::debug!(reason = ?refused.reason, path = %parts.uri.path(), "unauthenticated");
            return respond::unauthorized(&refused);
        }
    };
    let response = match target {
        Target::Actions => actions::serve(shared.surface.as_ref(), &caller, parts, body).await,
        Target::Route(route) => match query(&shared, route, &caller, parts, body).await {
            Ok(response) => response,
            Err(error) => respond::error(&error),
        },
    };
    tracing::debug!(
        route = ?target,
        operator = %caller.operator().ulid_text(),
        status = response.status().as_u16(),
        "answered"
    );
    response
}

async fn query<S: Surface>(
    shared: &Shared<S>,
    route: Route,
    caller: &crosstalk_spec::interfaces::l8_surface::Caller,
    parts: axum::http::request::Parts,
    body: Body,
) -> Result<Response, QueryError> {
    let input = Input::read(route, parts, body)
        .await
        .map_err(|unread| match unread {
            Unread::NoRoute => QueryError::NotFound,
            Unread::Malformed(error) => QueryError::from(error),
        })?;
    dispatch::query(shared, route, caller, &input).await
}

/// No route serves this method and path: 404, after authentication.
async fn not_found<S: Surface>(State(shared): Api<S>, request: Request) -> Response {
    match shared.auth.caller(request.headers()) {
        Ok(_) => respond::error(&QueryError::NotFound),
        Err(refused) => respond::unauthorized(&refused),
    }
}
