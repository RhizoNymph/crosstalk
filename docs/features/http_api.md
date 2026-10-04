# HTTP API

The HTTP binding of the L8 surface: which route serves each `QueryApi`
method, each operator action and the live feed; where each argument
travels; what a success looks like; which status each error gets; how a
request's credential becomes its `Caller`; and how the live feed, the
projection frame and exports use HTTP. The [wire contract](wire_contract.md)
fixes the JSON of every argument and result, and this feature fixes the
HTTP around it.

The binding is checked spec, not only prose. The route table, the request
encoder and decoder, the status mapping, authentication, SSE framing,
frame caching and export headers are Rust in
`spec/types/interfaces/l8_surface/http/`. The server (`crosstalk-api`,
roadmap P7.1) and the client (`crosstalk-client`, P7.2) are both built and
tested against them.

## Scope

- `Route`, one per `QueryApi` method, per `ActionKind` and for the live
  feed. Each has a `RouteSpec`: method, path template, arguments and where
  each travels, success status and content type, permission, and what it
  calls.
- How a client encodes a call (`RequestBuilder`) and how the server reads
  one back (`resolve`, `PathParams`, `QueryParams`, `check_body`). This
  includes the `POST` read bodies in `bodies.rs`.
- The status of every `QueryError`, `ActionError` and `AuthError`
  (`ErrorStatus`).
- Authentication: the credential headers, the session cookie's name,
  `Verification`, `authenticate`, and the 401 body `AuthError`.
- `GET /live`: resuming from `Last-Event-ID` or the `cursor` parameter,
  the exact bytes of each event, and the response headers.
- `GET /projections/{id}/frame`: the ETag, `Cache-Control`, the 304 rule,
  and the status when a frame is not ready.
- `POST /exports`: content types, `Content-Disposition`, and what a
  failure mid-stream looks like.

## Non-scope

- The JSON of arguments and results, strict decoding, and which types a
  client may send: [wire_contract.md](wire_contract.md).
- What each method does: [query_surface.md](query_surface.md),
  [read_models.md](read_models.md) and [export.md](export.md).
- How tokens and sessions are issued, stored, expired and verified, and
  the login flow that sets the cookie. The binding starts from "the
  session store verified this credential for operator X, or did not".
- TLS, CORS (the UI is same-origin), rate limiting, compression and
  request logging. The listener, its limits and its prefix are
  configuration. Templates are relative to the API's root; a deployment
  that shares an origin mounts the API under a prefix, and the client's
  base URL includes it.
- Errors the HTTP stack raises before the API layer reads a request,
  such as a request hyper cannot parse or headers over its buffer. These
  carry no L8 body.
- Versioned paths. There is no `/v1`, because a format change is a
  coordinated upgrade (wire contract).

## Data and control flow

```text
request
  ─▶ authenticate(directory, Verification::of(CredentialHeaders))   only Authorization and Cookie are read
       └─ Err(AuthError) ─▶ 401, WWW-Authenticate, {"reason": ..}   nothing below runs; not audited
  ─▶ resolve(method, path)
       └─ None ─▶ 404 {"type": "not_found"}
  ─▶ decode: PathParams::decode::<T: PathArg>, QueryParams::new + decode::<T: WireRequest>,
             check_body + decode_request::<T>
       └─ Err(DecodeError) ─▶ 400 InvalidInput(MalformedRequest)        not audited
  ─▶ Target::Route(route): the QueryApi method / LiveFeed::subscribe
     Target::Actions: ActionRequest ─into_action(&caller)─▶ OperatorActions::act
                                     └─ SelfMerge ─▶ 422 InvalidInput(SelfMerge), not audited
       ├─ Ok(v) ─▶ route's success status and content type; body: v's JSON (or frame / SSE / export)
       └─ Err(e) ─▶ e.status(), body: e's wire JSON, Cache-Control: no-store
```

A request that has no caller is refused before its route is resolved, so
an anonymous client learns nothing about which routes or ids exist. A
caller without the route's permission gets `403` from the method itself,
which checks the permission first. An undecodable request reaches no
method and no store. An action that does not decode, or is a self-merge,
never becomes an action, so it is not audited.

### Arguments

Every argument travels in exactly one place (`Place`), under its name in
the table:

| Place | Encoding |
| --- | --- |
| `path` | A `{param}` segment holding an entity id's ULID text or a topic-model version in decimal (`PathArg`). It is exactly the JSON without quotes. Anything outside `[0-9A-Za-z]` is refused before decoding, so no escape reaches the decoder, and each value has one path. |
| `query` | A query parameter whose value is the argument's compact JSON (`serde_json::to_string`), form-urlencoded (`application/x-www-form-urlencoded`, WHATWG URL). An absent parameter reads as `null`, so an `Option` is left out for `None` and every other argument is required. Writing `null` explicitly is accepted, which changes no data. |
| `query_text` | The live feed's `cursor`: the SSE id's text, read as `Last-Event-ID` is. |
| `header` | The live feed's `Last-Event-ID`. |
| `body` | The whole JSON body is the argument: an `IdBatch`, an `ExportRequest` or an `ActionRequest`. |
| `body_field` | A member of one JSON body object, named after the method's parameter. The object types are in `http/bodies.rs`. |

Reads are `GET` with query parameters. The exception is an argument that
holds a client-chosen list of ids or free text with no bound below a URL's
practical size:

- the shared `TopologyFilter` (agents, channels, topics), so every linked
  view;
- an `IdBatch` (up to 1,000 ids);
- a `TransmissionSelection` (up to 100,000 ids);
- a search's text.

Those reads are `POST /query/...` with a JSON body. They change nothing.
Keeping them under `/query/` means no literal path ever sits beside an id
template.

The list filters (`ChannelFilter`, `AgentFilter`, `AlertFilter`,
`AlertRuleFilter`, `AuditFilter`) stay in the query string. They are sets
of enum values plus a few ids an operator picks by hand.

Strictness extends to the HTTP layer. Each of these is `MalformedRequest`:

- an unknown or repeated query parameter, or a missing required one;
- a body on a route that takes none;
- a body over `MAX_BODY_BYTES` (8 MiB);
- a body whose `Content-Type` is not `application/json`. Parameters such
  as `charset` are ignored. The forms and text bodies a cross-site page
  can post therefore never reach the decoder.

### Responses

A success carries its route's status and content type. A JSON body is the
method's `Ok` value. An `Option` result's `None` is `200` with `null`;
`404` only ever means `QueryError::NotFound` (or no route). `POST
/projections` answers `202 Accepted` with the `ProjectionId` JSON and
`Location: /projections/{id}`. `POST /actions` answers `200` with the
`ActionOutcome`.

Every response except a ready projection frame carries
`Cache-Control: no-store`. What a response holds depends on the caller's
permissions and on live state, and one URL serves every operator. `HEAD`
is answered for every `GET` route; no method other than `GET`, `HEAD` and
`POST` is.

### Route table

The golden is `spec/types/tests/golden/http/route_table.json`. In the
Arguments column, `?` marks an optional argument, `q` a query parameter,
`f` a body field and `p` a path parameter.

| Method | Path | Arguments | Success | Permission | Calls |
| --- | --- | --- | --- | --- | --- |
| GET | `/channels/{id}` | id p, window q? | 200 JSON | View | `channel` |
| GET | `/channels/{id}/policy-history` | id p | 200 JSON | View | `policy_history` |
| GET | `/channels` | filter q, page q | 200 JSON | View | `channels` |
| POST | `/query/channel-names` | body `IdBatch<ChannelId>` | 200 JSON | View | `channel_names` |
| GET | `/channels/{id}/promotion-preview` | id p, pattern q | 200 JSON | View | `promotion_preview` |
| GET | `/channels/{id}/resources` | id p, window q, page q | 200 JSON | View | `channel_resources` |
| GET | `/agents` | filter q, window q, page q | 200 JSON | View | `agents` |
| GET | `/agents/{id}` | id p, window q | 200 JSON | View | `agent` |
| POST | `/query/agent-names` | body `IdBatch<AgentId>` | 200 JSON | View | `agent_names` |
| GET | `/alert-rules` | filter q, page q | 200 JSON | View | `alert_rules` |
| GET | `/alert-rules/{id}` | id p | 200 JSON | View | `alert_rule` |
| GET | `/sinks` | — | 200 JSON | Govern | `sinks` |
| GET | `/dead-letters` | group q?, page q | 200 JSON | Operate | `dead_letters` |
| GET | `/alerts` | filter q, page q | 200 JSON | View | `alerts` |
| GET | `/alerts/{id}` | id p | 200 JSON | View | `alert` |
| GET | `/watermark` | — | 200 JSON | View | `watermark` |
| POST | `/query/topology` | window f, weighting f, filter f (`GraphBody`) | 200 JSON | View | `topology` |
| POST | `/query/overview` | window f, filter f (`OverviewBody`) | 200 JSON | View | `overview` |
| POST | `/query/channel-topology` | window f, weighting f, filter f (`GraphBody`) | 200 JSON | View | `channel_topology` |
| POST | `/query/edge-transmissions` | edge f, window f, filter f, page f | 200 JSON | View | `edge_transmissions` |
| POST | `/query/transmissions` | selection f, version f, page f | 200 JSON | View | `transmissions_by_id` |
| POST | `/query/series` | grid f, weighting f, grouping f, filter f | 200 JSON | View | `series` |
| GET | `/topic-versions` | — | 200 JSON | View | `topic_versions` |
| GET | `/topic-sizes` | version q?, window q? | 200 JSON | View | `topic_sizes` |
| GET | `/topic-versions/{version}/lineage` | version p | 200 JSON | View | `topic_lineage` |
| POST | `/query/search` | request f, window f, filter f, page f | 200 JSON | Content | `search` |
| GET | `/transmissions/{id}` | id p | 200 JSON | Content | `transmission` |
| GET | `/transmissions/{id}/evidence` | id p, window q | 200 JSON | Content | `transmission_evidence` |
| GET | `/topics` | version q, page q | 200 JSON | Content | `topics` |
| POST | `/projections` | window f, filter f, params f | 202 JSON | Content | `fit_projection` |
| GET | `/projections/{id}` | id p | 200 JSON (`ProjectionInfo`) | Content | `projection_status` |
| GET | `/projections` | page q | 200 JSON | Content | `projections` |
| GET | `/projections/{id}/frame` | id p | 200 `application/octet-stream` | Content | `projection` |
| GET | `/transmissions/{id}/verdicts` | id p | 200 JSON | View | `verdicts` |
| GET | `/detection-quality` | window q | 200 JSON | View | `detection_quality` |
| GET | `/audit` | filter q, page q | 200 JSON | Audit | `audit` |
| GET | `/operators` | — | 200 JSON | View | `operators` |
| POST | `/exports` | body `ExportRequest` | 200 `application/x-ndjson` or `application/vnd.apache.parquet` | View, or Content by the request | `export` |
| GET | `/present` | — | 200 JSON | View | `present` |
| POST | `/actions` | body `ActionRequest` | 200 JSON (`ActionOutcome`) | the kind's: Govern, Triage or Operate | `OperatorActions::act`, one route per `ActionKind` |
| GET | `/live` | `last-event-id` header?, cursor query_text? | 200 `text/event-stream` | View | `LiveFeed::subscribe` |

`POST /actions` is one endpoint with fourteen `Route::Action(kind)` rows.
The kind comes from the body's `type`, and each row's permission is
`ActionKind::required_permission`. `OperatorAction::required_permission`
now returns that same value, so the route and the action agree by
construction.

Path parameters at one position share a name (`/channels/{id}/...`), as
axum's router requires. That is why `policy_history(channel)` reads `id`
from the path.

### Status mapping

`ErrorStatus::status` matches every variant of `QueryError`,
`ActionError`, `ConflictKind` and `InputError` exhaustively. The goldens
are `http/query_error_statuses.json` and `http/action_error_statuses.json`.
An `ActionError` gets the status of `QueryError::from` of it.

| Error | Status | Why |
| --- | --- | --- |
| `InvalidInput(MalformedRequest)` | 400 | The request could not be read as the route's types. |
| `InvalidCursor` | 400 | A cursor the surface did not issue for this request. |
| no credential, an invalid one, or an unknown or former operator | 401 + `WWW-Authenticate` | There is no caller. The body is an `AuthError`. |
| `Forbidden { missing }` | 403 | Authenticated, but without the permission. |
| `NotFound`, and a path no route serves | 404 | |
| `Conflict(_)`, except `ProjectionQueueFull` | 409 | Well-formed, but the state does not allow it now. |
| `VersionNotRetained`, `ProjectionNotRetained` | 410 | The data existed and retention dropped it for good. |
| `InvalidInput(_)`, any other input error | 422 | Read, but invalid whatever the state. This includes `UnsupportedFormat`, an export format outside `Present::export_formats`. |
| `Conflict(ProjectionQueueFull)` | 429 | Capacity: retry later, unchanged. |
| `Store` | 503 | A store or the bus failed; retrying may succeed. |

These start from the UI's two mappings, `ui/src/data/errors.rs` for reads
and `ui/src/pages/common/action.rs` for actions, and settle where they
differed:

- Input errors split 400/422 by whether the request could be read at all.
- A version conflict on a read is a 409, as on an action. The UI's reads
  used 400 because the conflicting value came from the page's URL.
- `ProjectionNotRetained` and `VersionNotRetained` are 410 rather than 404
  and 409.
- `Store` is 503 rather than 500 or 502.

### Authentication

The credential is read from two header fields only (`CredentialHeaders`):

- **Bearer token.** `Authorization: Bearer <b64token>`. The scheme is
  matched ignoring case. This is for clients that are not browsers: the UI
  server and scripts.
- **Session cookie.** `__Host-crosstalk-session=<b64token>` (`SESSION_COOKIE`),
  for browsers. An `EventSource` cannot send `Authorization`. The
  `__Host-` prefix means the browser keeps the cookie only when it was set
  `Secure`, `Path=/` and with no `Domain`. The surface also sets it
  `HttpOnly; SameSite=Strict`.

With an `Authorization` header, that header is the credential and the
cookie is not read. Each of these makes the credential malformed:

- more than one `Authorization` field;
- a scheme other than `Bearer`;
- a token outside `b64token`;
- the session cookie named twice.

`Verification::of` turns the headers and the session store's lookup into
`NoCredential`, `Rejected` or `Verified(operator)`.
`Verification::identity` is the directory's `RequestIdentity`.
`authenticate(directory, verification)` returns
`OperatorDirectory::caller` of it, or the 401 to answer:

| Verification | Directory | 401 `reason` |
| --- | --- | --- |
| `NoCredential` | `NoSession` | `no_credential` (`WWW-Authenticate: Bearer realm="crosstalk"`) |
| `Rejected` | `NoSession` | `invalid_credential` (`…, error="invalid_token"`) |
| `Verified(id)`, not in the directory | `UnknownOperator` | `unknown_operator` |
| `Verified(id)`, former operator | `FormerOperator` | `former_operator` |

In trusted mode every request is the trusted operator's, whatever it
carries, and the surface need not read or verify a credential.

`authenticate` takes only a `Verification`, and only `CredentialHeaders`
produces one. So no path, query parameter, body field or other header can
name or change the caller (`surface.api.caller-from-session`).

A credential's `Debug` prints `Secret(<redacted>)`
(`surface.api.no-raw-credentials`). `AuthError` is
`{"reason": "<failure>"}`, a response decoded strictly, with its golden in
`http/auth_errors.json`.

### Live feed

```text
GET /live[?cursor=7-1042]   (Last-Event-ID: 7-1042 on an automatic reconnect)
  ─▶ resume(last_event_id, cursor) ─▶ LiveFeed::subscribe(caller, resume)
       ├─ Err(e) ─▶ e.status() (403 without View), e's JSON
       └─ Ok(stream) ─▶ 200, text/event-stream, Cache-Control: no-store, X-Accel-Buffering: no
            event_frame(item)…   heartbeats at least every LiveConfig::heartbeat
            end_frame(end)       then the response ends
```

**Resume.** `sse::resume` reads `Last-Event-ID` when present, else the
`cursor` query parameter, else `Fresh`. Both go through
`Resume::from_last_event_id`.

- On an automatic reconnect the header is the newer of the two, so it
  wins.
- A new `EventSource` cannot set headers (after a page load, or after an
  `end` event), so it passes its last id as `cursor`.
- Neither is JSON. Text that is not a cursor is `Unreadable`, which the
  feed answers with a `resync` item rather than an error.

**Framing.** Each item is framed as
`wire/surface_actions.md` says, written by `event_frame`:

```text
event: event
id: 7-1042
data: {"type":"event","data":{"cursor":"7-1042","event":{"type":"alert_changed","data":{"id":"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}}}

```

The last event is written by `end_frame`. It has no `id`:

```text
event: end
data: "lagged"

```

No `retry` field and no comment lines are sent. Heartbeats are items, so
they move the client's cursor.

### Projection

- `GET /projections/{id}` is `projection_status`: the `ProjectionInfo`
  JSON, `no-store`.
- `GET /projections/{id}/frame` calls `QueryApi::projection`. The
  permission and readiness checks therefore always run first. Then:
  - **Ready.** `FrameCache::of(projection, digest, frame_retention, now)`.
    `ETag` is the BLAKE3 digest of `ProjectionFrame::encode`'s bytes in
    quoted lower-case hex, a strong validator; the format version is in
    the bytes. `Cache-Control` is
    `private, max-age=<seconds until fitted_at + frame retention, at most
    a year>, immutable`. A matching `If-None-Match` (`*`, or a list naming
    the tag, weak or strong) is `304` with no body.
  - **Not ready.** Queued or fitting is `409`
    `Conflict(ProjectionNotReady { status })`, `no-store`. The client waits
    for the live feed's `ProjectionReady` (or polls `GET /projections/{id}`)
    and asks again.
  - **Failed** is `409` `ProjectionFailed`, **expired** `410`
    `ProjectionNotRetained`, **unknown** `404`, and **without Content**
    `403`.

A 304 therefore only ever answers a caller with Content, for a frame that
is still ready. The cache ends when retention drops the frame, so a
browser never keeps serving a frame the server has expired. The store may
keep the digest beside the frame rather than hash 5 MB on every read.

### Export

`POST /exports` takes an `ExportRequest` body.

- **Refused before streaming.** A refusal (`Forbidden`, `ExportTooLarge`
  409, `UnsupportedFormat` 422, a version or projection error) is the
  usual status and JSON, and no body byte is sent.
- **Accepted.** The status and headers are sent once `export` returns the
  header, before any row is read:
  - `200`;
  - `Content-Type` is `application/x-ndjson` or
    `application/vnd.apache.parquet`;
  - `Content-Disposition: attachment; filename="crosstalk-<dataset>-<export id>.<jsonl|parquet>"`;
  - `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`;
  - no `Content-Length`. The body is streamed chunked: the JSONL lines,
    or the Parquet file.

**A failure mid-stream** cannot change a status that has already been
sent. The response stays `200`. The export's trailer records the failure
(`ExportEnd::Failed`) as its last JSONL line or in the Parquet footer, and
the body ends normally. A reader trusts the trailer (`verify_export`),
never the status.

When the server cannot write even the trailer, for example because the
process stops, it aborts the response without its terminating chunk (a
`RST_STREAM` in HTTP/2), so the client's HTTP stack reports the cut too.
The body read until then has no trailer, which `read_jsonl` and
`verify_export` refuse (`NoTrailer`), and a Parquet file cut before its
footer does not open. HTTP trailers are not used.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/interfaces/l8_surface/http.rs` | The binding's vocabulary and module docs | `Method`, `Place`, `Arg`, `ResponseBody`, `Success`, `RoutePermission`, `Source`, `RouteSpec`, `JSON` |
| `spec/types/interfaces/l8_surface/http/routes.rs` | The route table | `Route` (`all`, `index`, `spec`, `method`, `path`, `permission`) |
| `spec/types/interfaces/l8_surface/http/path.rs` | Templates, matching, path parameters | `Target`, `PathParams`, `Segment`, `segments`, `match_template`, `resolve`, `PathArg` |
| `spec/types/interfaces/l8_surface/http/request.rs` | Encoding a call, and reading a query string and body back | `RequestBuilder`, `EncodedRequest`, `EncodeError`, `QueryParams`, `check_body`, `MAX_BODY_BYTES` |
| `spec/types/interfaces/l8_surface/http/bodies.rs` | The `POST` read bodies (`WireRequest`s) | `GraphBody`, `OverviewBody`, `EdgeTransmissionsBody`, `TransmissionsBody`, `SeriesBody`, `SearchBody`, `FitProjectionBody` |
| `spec/types/interfaces/l8_surface/http/status.rs` | The status of every error | `Status`, `ErrorStatus`, `conflict_status`, `input_status` |
| `spec/types/interfaces/l8_surface/http/auth.rs` | Credentials, verification, the caller or the 401 | `SESSION_COOKIE`, `REALM`, `Field`, `CredentialHeaders`, `Credential`, `Secret`, `MalformedCredential`, `Verification`, `authenticate`, `AuthError`, `AuthFailure` |
| `spec/types/interfaces/l8_surface/http/sse.rs` | `GET /live` | `EVENT_STREAM`, `LAST_EVENT_ID`, `CURSOR_PARAM`, `HEADERS`, `resume`, `event_frame`, `end_frame`, `Unencodable` |
| `spec/types/interfaces/l8_surface/http/frame.rs` | `GET /projections/{id}/frame` | `OCTET_STREAM`, `MAX_AGE_LIMIT`, `FrameCache` (`of`, `etag`, `cache_control`, `not_modified`) |
| `spec/types/interfaces/l8_surface/http/export.rs` | `POST /exports` | `JSONL`, `PARQUET`, `content_type`, `extension`, `dataset_name`, `file_name`, `content_disposition` |
| `spec/types/interfaces/l8_surface/actions.rs` | `ActionKind::ALL`, `index`, `required_permission`, which `OperatorAction::required_permission` now returns | — |
| `spec/types/tests/wire/http/` | `routes.rs` (golden, completeness, permissions from the methods' docs, ambiguity), `client.rs` (`TableClient`: a `QueryApi` over the table), `request.rs`, `status.rs`, `auth.rs`, `sse.rs`, `frame.rs`, `export.rs`, `bodies.rs` | `TableClient` |
| `spec/types/tests/golden/http/` | `route_table`, `query_error_statuses`, `action_error_statuses`, `auth_errors`, `bodies/*` (seven bodies) | — |

## Invariants and constraints

- Every `QueryApi` method, `ActionKind` and the live feed has exactly one
  route, and every recorded call resolves back to its route with exactly
  the table's arguments (`surface.http.route-per-method`).
- A route needs exactly its method's documented permission
  (`surface.http.route-permission-matches-method`).
- No method and path match two routes; a parameter at one position has one
  name (`surface.http.routes-unambiguous`).
- The status mapping is total, exhaustive over every conflict and input
  error, pinned by goldens, and the same for an action error as for its
  query error (`surface.http.status-mapping`).
- An undecodable request is a 400 `MalformedRequest`. It reaches nothing
  and is not audited (`surface.http.undecodable-request-bad-request`,
  extending `surface.query.undecodable-request-invalid-input`).
- The caller comes only from the credential
  (`surface.api.caller-from-session`, restated for this binding). No
  caller is a 401 before routing; no permission is a 403
  (`surface.http.unauthenticated-401`).
- `GET /live` resumes from `Last-Event-ID`, else `cursor`, else fresh, and
  frames items and the end exactly (`surface.http.sse-resume`, with
  `surface.live.sse-frame-matches-item` and
  `surface.live.heartbeat-interval`).
- A frame is cached by its digest until its retention ends, and a 304 or
  200 follows a successful `projection` call
  (`surface.http.frame-cache`).
- Every other response is `no-store` (`surface.http.responses-not-shared`).
- An export's status is final once sent, and its trailer records any later
  failure (`surface.http.export-download`).
