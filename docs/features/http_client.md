# HTTP client

`crosstalk-client` (roadmap P7.2) implements the L8 traits over the
[HTTP binding](http_api.md):

- `QueryApi`;
- `OperatorActions`;
- `LiveFeed`;
- export.

A server that renders pages from a `QueryApi` (the UI's Topcoat server,
talking to the gateway on `:8081`) can therefore swap the in-process
surface for `HttpClient` and change nothing else. The client is built
over the binding's own checked spec:

- `RequestBuilder` per `Route`;
- `ErrorStatus`;
- `CredentialHeaders`;
- the SSE framing;
- `ExportSealer`.

The client and the server (P7.1) are both checked against the same
definitions.

## Scope

- `HttpClient<H>`: one hyper connection pool to one surface, the bearer
  token its requests carry, and the row hasher `H` exports are verified
  with (BLAKE3 by default).
- `impl QueryApi`: every method encodes its call through
  `RequestBuilder::new(route)`, one argument at a time under the table's
  names, and decodes the route's success or error. `projection` reads the
  frame and then the record. `export` streams JSONL.
- `impl OperatorActions`: `act` sends `ActionRequest::of(&action)` to
  `POST /actions`.
- `impl LiveFeed`: `GET /live` as SSE. The stream checks framing,
  reconnects from the last cursor and gives up per a `ReconnectPolicy`.
- The status mapping in reverse, `401` as `AuthError`, and the typed
  `ClientError` for what the traits' errors cannot express.
- Export verification as the stream passes, and `download_export` for
  passing either format on as bytes.
- The projection frame's ETag check and a small `If-None-Match` cache.
- Configuration: `BaseUrl`, `BearerToken`, `ClientConfig` and
  `ReconnectPolicy`, all checked when built.

## Non-scope

- The server: `crates/api`, P7.1.
- TLS. The base URL is plain `http`; a TLS deployment terminates in front
  of the gateway or beside the UI server.
- The session cookie and the login flow. The client is not a browser; it
  presents a bearer token, which its user (the UI server) obtains and
  holds per operator.
- Reading Parquet. `QueryApi::export` refuses a Parquet request with
  `InvalidInput(UnsupportedFormat)` before sending it; `download_export`
  passes Parquet through as bytes.
- The follow-mode batch (`QueryApi::now`, `bucket_width`, the default
  remap threshold, `DataRevision` ETags), which lands later. Each new
  method or route is one more table-driven call (see "Adding a route").

## Data and control flow

### A JSON call

```text
QueryApi::channels(caller, filter, page)                      caller ignored: the credential names it
  ─▶ HttpClient::call::<T, QueryError>(Route::Channels, |b| b.query("filter", filter).query("page", page))
       RequestBuilder::new(route) … .build()                  Err(EncodeError) ─▶ ClientError::Encode, nothing sent
       ─▶ EncodedRequest { method, path, query, headers, body }
       ─▶ hyper request: <base><path>?<form-encoded query>, User-Agent: crosstalk-client/<version>,
          Accept: application/json, Authorization: Bearer <token>, Content-Type: application/json with a body
       ─▶ response read whole within ClientConfig::request_timeout, at most max_response_bytes
            ├─ the route's success status (200, 202 for fit_projection), Content-Type application/json
            │    ─▶ serde_json::from_slice::<T> (strict, as every wire type)   else UnexpectedResponse
            └─ any other status ─▶ error::decode_error
                  401 ─▶ AuthError ─▶ ClientError::Unauthenticated
                  else ─▶ E with E.status() == status ─▶ ClientError::Api(E)
                          E at another status ─▶ StatusMismatch;  no L8 body ─▶ UnexpectedResponse
  ─▶ QueryError::from(ClientError): Api(e) ─▶ e; Encode ─▶ InvalidInput(MalformedRequest); else ─▶ Store { reason }
```

`form::encode` writes the query string as the WHATWG form serializer
does:

- `*-._`, ASCII letters and digits stay as they are;
- a space becomes `+`;
- every other byte becomes `%XX`.

Each value is the argument's compact JSON, and an `Option`'s `None` is
left out. These are the `RequestBuilder`'s rules.

### Actions

`act(caller, action)` sends `ActionRequest::of(&action)`, which drops
anything the surface stamps, such as a merge's author. It goes to the
action kind's route, `POST /actions`, and comes back as the
`ActionOutcome` or the `ActionError`, through the same reverse mapping.

### Projection

```text
projection(id)
  ─▶ GET /projections/{id}/frame, Accept: application/octet-stream [If-None-Match: cached ETag]
       error ─▶ that error, nothing else read (403, 404, 409 not ready or failed, 410)
       304 ─▶ the cached frame (gone from the cache ─▶ ask again without If-None-Match)
       200 ─▶ ETag must be "<BLAKE3 hex of the bytes>" ─▶ ProjectionFrame::decode ─▶ cached by id
  ─▶ GET /projections/{id} ─▶ ProjectionInfo
  ─▶ Projection::new(info, frame)
       NotReady(Expired) ─▶ ProjectionNotRetained, the cached frame dropped
       any other mismatch ─▶ UnexpectedResponse (Store)
```

### Live feed

```text
subscribe(resume) ─▶ GET /live, Accept: text/event-stream, Last-Event-ID: <cursor> | "unreadable" | none
  ─▶ HttpLiveStream { resume point, connection (ChunkReader + SseParser), attempts }
next():
  event ─▶ live::frame: name == item.event_name() and id == cursor text ─▶ Ok(item), resume point = its cursor
          end (no id, LiveEnd data) ─▶ Err(end), closed for good
          anything else ─▶ cut
  cut (body error, end of body, idle_timeout of silence, misframed event)
  ─▶ reconnect: attempt n waits ReconnectPolicy::delay(n), sends Last-Event-ID: <resume point>
       200 ─▶ carry on      401 / 403 ─▶ Err(SessionEnded)      other ─▶ try again
       attempts exhausted ─▶ Err(ShuttingDown)
```

`SseParser` reads an event stream the way the WHATWG standard does:

- line ends `\r\n`, `\n` or `\r`;
- a leading BOM is dropped;
- comments are skipped;
- one space after the colon is dropped;
- `data` lines join with `\n`;
- an event with no data is not dispatched.

Each event keeps its own `id`, so the client can check that an item's id
is its cursor and that `end` has none.

### Export

```text
export(request)  (JSONL only)
  ─▶ POST /exports, Accept: application/x-ndjson
       error status ─▶ the error, nothing streamed
       200 ─▶ Content-Type must be application/x-ndjson
            line 1 ─▶ ExportLine::Header; header.request() == request; Content-Disposition == content_disposition(header)
  ─▶ Export { header, rows: HttpExportRows<H> }
HttpExportRows::next():
  row ─▶ ExportSealer::push ─▶ yielded (and hashed into the shadow digest) | refused ─▶ End(sealer.finish()) = Failed(InvalidRow)
  trailer ─▶ nothing may follow;  another export's ─▶ Failed(Store)
             Failed ─▶ passed on as the surface's
             Complete ─▶ rows received == planned (else Failed(CountMismatch)),
                         == trailer.rows (else Failed(Store)), digest == shadow digest (else Failed(Store)) ─▶ the trailer
  end of body or error before a trailer, a second header, an undecodable line ─▶ Failed(Store) over the rows yielded
```

A client-made `Failed` trailer comes from the client's own sealer, so it
counts and digests exactly the rows the stream yielded. A stream that
ends `Complete` is therefore one where `verify_export` holds.

`download_export(request)` passes either format through, as
`ExportDownload`:

- the format;
- the `Content-Disposition`;
- the body, chunk by chunk with the idle timeout.

A cut body is an error from `chunk()`.

### Adding a route

When the spec adds a `QueryApi` method, `impl QueryApi for HttpClient`
stops compiling until the method has a body. That body is one line, like
every other: `self.query(Route::New, |b| b.path(..).query(..))`. The
route table supplies the method, the path and where each argument goes.
The `every_query_method_sends_its_route` test then fails until the call
is listed there, and checks the call against the table.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/client/src/lib.rs` | Crate docs, module tree, re-exports | — |
| `crates/client/src/client.rs` | `HttpClient` (pool, token, hasher type), `send` (one encoded call as a hyper request), `exchange` and `call` (a whole-body route), `Exchanged`, content-type checks | `HttpClient` |
| `crates/client/src/config.rs` | Checked configuration | `BaseUrl`, `InvalidBaseUrl`, `BearerToken`, `InvalidToken`, `ClientConfig`, `ReconnectPolicy`, `InvalidConfig` |
| `crates/client/src/error.rs` | The reverse status mapping (`decode_error`) and the conversions into `QueryError` and `ActionError` | `ClientError`, `TransportError`, `ApiError` |
| `crates/client/src/form.rs` | The query string's form encoding | — |
| `crates/client/src/body.rs` | Whole bodies with a limit; chunks with an idle timeout; JSONL lines | — |
| `crates/client/src/query.rs` | `impl QueryApi`, one table-driven call per method | — |
| `crates/client/src/actions.rs` | `impl OperatorActions` | — |
| `crates/client/src/frame.rs` | `projection`: frame, ETag check, `If-None-Match` cache, record | — |
| `crates/client/src/live/mod.rs` | `impl LiveFeed`, `HttpLiveStream` (framing checks, reconnects) | `HttpLiveStream` |
| `crates/client/src/live/sse.rs` | The incremental SSE parser | — |
| `crates/client/src/export/mod.rs` | `POST /exports` up to the response head; `start_export`; `download_export` | `HttpExportRows`, `ExportDownload` |
| `crates/client/src/export/rows.rs` | `HttpExportRows`: rows through the sealer, trailer checks | `HttpExportRows` |
| `crates/client/src/export/download.rs` | The raw download | `ExportDownload` |
| `crates/client/src/hasher.rs` | BLAKE3 under `ROW_DIGEST_CONTEXT` as a `RowHasher` | `Blake3RowHasher` |
| `crates/client/src/tests/` | A stub HTTP server that speaks the binding (`stub.rs`); tests per area, built on the spec's wire goldens | — |

Dependencies, all already pinned in the workspace:

- `crosstalk-spec`;
- `blake3`;
- `bytes`;
- `http-body-util`;
- `hyper`, with `client` and `http1`;
- `hyper-util`, with `client-legacy`, `http1` and `tokio`;
- `serde`;
- `serde_json`;
- `thiserror`;
- `tokio`, with `sync` and `time`;
- `tracing`.

The dev-dependencies add `hyper`'s `server` feature, `http-body-util`'s
`channel`, and `tokio`'s `macros`, `net`, `rt` and `rt-multi-thread`.

## Invariants and constraints

- **`surface.http.client-sends-the-route`.** Every call sends its route's
  `RequestBuilder` request. The surface's `resolve`, `QueryParams` and
  `check_body` read it back as that route with exactly the table's
  arguments. The bodies are the wire goldens.
- **`surface.http.client-error-status-agrees`.** An error is the surface's
  only when its status is the received one. A `401` is the `AuthError`.
  Anything else is never presented as the surface's decision.
- **`surface.http.client-live-resumes`.** The stream delivers only
  correctly framed items, and reconnects from the last cursor it
  delivered. It ends only with the end event's reason, with
  `SessionEnded` on a refused reconnect, or with `ShuttingDown` once its
  attempts run out.
- **`surface.http.client-export-verified`.** An export ends `Complete`
  only when the rows yielded verify against the header and the surface's
  trailer.
- **`surface.http.client-bearer-only`.** The credential is one
  `Authorization: Bearer` header that the binding reads back as the
  token. It is marked sensitive and redacted in `Debug`.
- **The caller never travels.** The `&Caller` argument of every trait
  method is ignored, because the surface derives the caller from the
  credential (`surface.api.caller-from-session`). A server rendering for
  several operators makes one client per session, with
  `HttpClient::with_token`, over one shared pool.
- **Errors the traits cannot express.** These become `Store { reason }`:
  a `401`, a transport failure or timeout, a response the binding does
  not describe, and a status mismatch. The inherent `call` path and
  `download_export` return the typed `ClientError`.
- **No mixing of threads and async.** The only shared state is the frame
  cache, a `std::sync::Mutex` that is never held across an await. The
  test stub records requests over an `mpsc` channel.
- **No `unwrap` or `expect` outside tests.** A `ReconnectPolicy::default`
  constant uses `unwrap_or`.
