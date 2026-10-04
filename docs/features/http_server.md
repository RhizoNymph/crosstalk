# HTTP server

`crosstalk-api` (roadmap P7.1) serves the L8 surface over HTTP with axum.
It implements the [HTTP binding](http_api.md) exactly, over any
implementation of the spec's L8 traits (`QueryApi`, `OperatorActions`,
`LiveFeed`, and export through `QueryApi::export`). The binding is spec
(`spec/types/interfaces/l8_surface/http/`). This crate only wires it to
axum, to a surface and to a credential check.

## Scope

- One axum `Router` for the whole route table. It is registered from
  `Route::all()`, so a row added to the spec is mounted without a change
  here.
- Authentication: the `Authorization` bearer token or the
  `__Host-crosstalk-session` cookie, then the current `OperatorDirectory`.
  The result is a `Caller` or the 401.
- Reading each request against its route: path parameters, the query
  string and the body. Values go through `decode_request`. An id batch, a
  transmission selection, an excerpt window and an `ActionRequest` go
  through their checked constructors.
- Calling the method and answering with the route's success, or with the
  error's status and wire JSON.
- `GET /live` as SSE, `GET /projections/{id}/frame` with its cache
  headers, and `POST /exports` as a streamed JSONL download.
- `serve` and `bind`, which run the router on `api.listen`.

## Non-scope

- What each method does. That is the surface (`crosstalk-surface`, P2.6),
  or the test fake here.
- How tokens and sessions are issued and stored. `CredentialVerifier` is
  the seam. `StaticTokens` verifies the deployment's fixed API tokens and
  knows no sessions.
- Loading the operator directory. The gateway publishes it on a `watch`
  channel (`Auth::new`).
- Wiring into the gateway's `--role api`. The gateway is not edited here.
- Parquet exports. The server writes JSONL only (`written_formats`).
- TLS, CORS, compression and rate limiting, as in the binding.

## Data and control flow

```text
request
  ─▶ axum Router (one route per distinct method + template of Route::all())
       └─ no template, or a method the template does not answer ─▶ not_found
  ─▶ Auth::caller(headers)                              every handler, the fallback too
       Authorization fields, Cookie fields joined "; " ─▶ CredentialHeaders
       ─▶ Verification::of(.., CredentialVerifier::verify) ─▶ authenticate(directory.borrow())
       └─ Err(AuthError) ─▶ 401, WWW-Authenticate, {"reason": ..}           nothing else runs
  ─▶ Target::Route(route)                               routes.rs::answer
       Input::read(route, parts, body)                  input.rs
         match_template(route.path(), raw path) ─▶ PathParams      (mismatch ─▶ 404)
         form_urlencoded pairs ─▶ QueryParams::new(route, ..)
         body read to MAX_BODY_BYTES + 1 ─▶ check_body(route, Content-Type, ..)
         └─ DecodeError ─▶ 400 InvalidInput(MalformedRequest)
       dispatch::query(route)                           one arm per Route, no wildcard
         arguments: Input::path / query / body (decode_request), checked.rs adapters
         └─ checked constructor refuses ─▶ 422 (TooManyIds, EmptySelection, ExcerptContextTooLong)
         QueryApi method(caller, ..) ─▶ Ok(v): route's success status, v's JSON
                                     ─▶ Err(e): e.status(), e's JSON
         ProjectionFrame ─▶ frame.rs · Export ─▶ export.rs · Live ─▶ live.rs
  ─▶ Target::Actions                                    actions.rs
       Input::read(Route::Action(any kind)) ─▶ decode_request::<ActionRequest>
       ─▶ into_action(&caller) (SelfMerge ─▶ 422) ─▶ OperatorActions::act ─▶ 200 ActionOutcome
  ─▶ map_response layer: Cache-Control: no-store on any response that has none
```

**Frame.** `QueryApi::projection` is called first, so its permission and
readiness checks always run. Then the server encodes the frame and takes
the BLAKE3 digest of the bytes. It builds `FrameCache::of(projection,
digest, HttpConfig::frame_retention, HttpConfig::clock.now())`. A
matching `If-None-Match` gets a 304 with `ETag` and `Cache-Control`.
Anything else gets a 200 with the bytes, the strong `ETag` and
`private, max-age=…, immutable`.

**Live.** `sse::resume(Last-Event-ID, cursor)` is passed to
`LiveFeed::subscribe`. The body is a stream that writes `event_frame` per
item and `end_frame` at the end, and then finishes. The headers are
`sse::HEADERS`, and there is no `Content-Length`. An item that cannot be
framed aborts the body.

**Export.** `QueryApi::export` returns the header before any row. The
server then sends 200 with the format's `Content-Type`,
`Content-Disposition`, `no-store` and `nosniff`. The body is a stream of
`ExportLine`s, each compact JSON plus `\n`: the header, each row and the
trailer. A failure the surface records in the trailer still ends the body
normally. A line with no JSON aborts the body without its terminating
chunk, so no trailer follows (tested over a real socket). A Parquet export
the surface accepted is answered `InvalidInput(UnsupportedFormat)` (422)
before any byte is sent.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/api/src/http/mod.rs` | The module and its constructor | `HttpApi` (`new`, `router`), `HttpConfig`, `Surface` |
| `crates/api/src/http/routes.rs` | Table-driven registration, the authenticated fallback, the `no-store` layer | — |
| `crates/api/src/http/auth.rs` | Credentials to `Caller` | `Auth` (`new`, `fixed`), `CredentialVerifier`, `StaticTokens`, `BearerToken` (`new`, `from_env`), `InvalidBearerToken` |
| `crates/api/src/http/input.rs` | Reading path, query and body against a route | `Input`, `Unread` (crate-private) |
| `crates/api/src/http/checked.rs` | Raw shapes, then the checked constructors | — |
| `crates/api/src/http/dispatch.rs` | One arm per `Route`: arguments in, method called | — |
| `crates/api/src/http/actions.rs` | `POST /actions` | — |
| `crates/api/src/http/frame.rs` | `GET /projections/{id}/frame` | — |
| `crates/api/src/http/live.rs` | `GET /live` | — |
| `crates/api/src/http/export.rs` | `POST /exports` | `written_formats` |
| `crates/api/src/http/respond.rs` | Status codes, JSON and error responses, the 401, `no-store` | — |
| `crates/api/src/http/serve.rs` | Listening on `api.listen` | `bind`, `serve`, `ServeError` |
| `crates/api/src/http/integration/` | Tests: a fake surface (`fake.rs`), one case per query route built from the wire goldens (`cases.rs`), and routes, errors, actions, auth, live, frame, export and socket tests | — |

### Wiring (for the gateway's `--role api`)

```rust
let token = BearerToken::from_env(&config.api.token.env)?;          // api.token {env}
let auth = Auth::new(directory_rx, StaticTokens::new([(token, api_operator)]));
let http = HttpConfig { frame_retention, clock: Arc::new(SystemClock) };
let router = HttpApi::new(Arc::new(surface), auth, http).router();
serve(bind(config.api.listen).await?, router, shutdown).await?;    // 8081 in deploy
```

The gateway should give the surface `written_formats()` as
`Present::export_formats`, so no client is offered Parquet.

## Invariants and constraints

- Every request is authenticated before anything else, including on a
  path no route serves. No caller means a 401 that reaches no method
  (`surface.http.unauthenticated-401`). The caller comes only from the
  `Authorization` and `Cookie` fields (`surface.api.caller-from-session`).
- Routes are registered from `Route::all()`. `dispatch::query` matches
  every `Route` with no wildcard, so every route is served
  (`surface.http.route-per-method`). A path no route serves, or a method
  its template does not answer, is `404 NotFound`. `HEAD` is answered for
  every `GET`.
- Request reading follows the spec's readers exactly. Anything they refuse
  is a 400 that reaches no method
  (`surface.http.undecodable-request-bad-request`). A checked constructor's
  refusal is the 422 `InvalidInput` the spec names.
- Errors are answered with `ErrorStatus::status` and their wire JSON
  (`surface.http.status-mapping`). Permissions are the surface's to check.
  The server passes the caller through and answers 403
  (`surface.http.route-permission-matches-method`).
- `GET /live` resumes and frames as the spec says (`surface.http.sse-resume`,
  `surface.live.sse-frame-matches-item`).
- Frame caching: `surface.http.frame-cache`. `no-store` everywhere else:
  `surface.http.responses-not-shared`.
- Export download: `surface.http.export-download`.
- Bearer tokens are kept as BLAKE3 digests and compared in constant time.
  `BearerToken`'s `Debug` is redacted, and a token is at least 16
  `b64token` characters.
- No `unwrap` or `expect` outside tests. The one `unreachable!` is the
  one-format `ExportFormats`.
