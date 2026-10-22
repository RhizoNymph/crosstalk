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
- Loading the operator directory. `Auth::new` reads it from a `watch`
  channel, and `Auth::fixed` takes one that never changes, which is what
  the gateway passes.
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
| `crates/api/src/http/respond.rs` | Status codes, JSON and error responses (each error's `served()` form, never a client-only `Unavailable`), the 401, `no-store` | `Answered` |
| `crates/api/src/http/serve.rs` | Listening on `api.listen` | `bind`, `serve`, `ServeError` |
| `crates/api/src/http/integration/` | Tests: a fake surface (`fake.rs`), one case per query route built from the wire goldens (`cases.rs`), and routes, errors, actions, auth, live, frame, export and socket tests | — |

### Wiring (the gateway's roles `all` and `api`)

The gateway mounts the router over its surface
(`crates/gateway/src/gateway/mod.rs`, [gateway.md](gateway.md)). The token
comes from the variable `api.token` names (`api_token`, through
`BearerToken::new`). `StaticTokens` maps it to `api.operator`
(`ApiOperator::ID`, default name `admin`, every permission). The directory
is fixed (`Auth::fixed`), since config does not change at runtime. The
surface's `export_formats` is `["jsonl"]` (`live/defaults.rs`), the same
as `written_formats()`, so no client is offered Parquet.

- **Memory mode.** The live process starts before the listener serves.
  The router is `HttpApi::new(surface, Auth::fixed(directory, tokens),
  http).router()` over `Live::surface()`, with the directory from the
  memory operator store. `serve(listener, router, shutdown)` runs it on
  `api.listen` (8081 in deploy).
- **Postgres mode** (a `store` section, [postgres_mode.md](postgres_mode.md)).
  The listener binds at start and serves `LateRouter::router()`
  (`crates/gateway/src/gateway/late.rs`). Until a router is set, every
  request is answered `503` with the `QueryError::Store` JSON and
  `no-store`; the reason names the cause (`late::Unserved`: starting, the
  pipeline lock lost, or handed over on a planned shutdown). The pipeline task sets the router
  when an incarnation starts (`postgres/pipeline.rs`). The `api` role sets
  it once its hosted surface is built over the stores (`postgres/api.rs`).
  Either way the directory is the one stored in Postgres. With none stored,
  or when reading it fails, the API stays at `503`. The router is cleared,
  and the API answers `503` again, when the pipeline lock is lost (the
  abandoned incarnation's surface is never served again) and at the start
  of a planned handover (`PgRunning::hand_over`,
  [planned_handover.md](planned_handover.md)). Its surface goes with the
  outgoing incarnation. A new incarnation in this process (after lock loss)
  sets its own.

**Shutdown.** `Running::shutdown` stops the API listener after the proxy's
drain. In Postgres mode with a running incarnation it is also after the
planned handover, which runs beside the drain: once the pipeline task has
stopped, the router is cleared, so the API answers `503` for the rest of
the drain and the handover. A standby already answers `503` (it never
set a router); the `api` role holds no lock, and its router stays set
until its listener stops. The listener's `shutdown` future fires, `serve` lets requests in flight finish,
and the gateway waits up to `shutdown.drain_timeout_ms` for the task.

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
