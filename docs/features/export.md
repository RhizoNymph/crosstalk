# Export

`QueryApi::export` streams one dataset out of the gateway between a
header and a trailer, so that the export can be cited (the header says
exactly what was selected and when) and verified (the trailer says how
many rows were sent and their digest, and whether the export completed).
It is part of the [query surface](query_surface.md). The types are in
`spec/types/interfaces/l8_surface/export/`.

## Scope

- The request: one dataset (transmissions, edge buckets, access buckets,
  topics, a stored projection, verdicts), its selection, a format (JSONL
  or Parquet) and whether content columns are included, with the one
  permission it needs.
- The row schema of each dataset, the order rows are sent in, and the
  reference builders for projection points and verdict records.
- The manifest: the header (request, resolved basis, watermark, embedding
  model, gateway version, planned row count) and the trailer (rows sent,
  digest, `Complete` or the failure).
- The content digest: a canonical binary encoding of the rows and its
  framing, which is the same in every format.
- The stream type, the sealer every row passes through, and verification
  of a received export.
- The row limit, the errors before streaming and their `QueryError`
  mapping, and the failures recorded in the trailer.
- Auditing exports.

## Non-scope

- The wire bytes of Parquet pages, beyond what the manifest needs (where
  the header and trailer go, and the canonical encoding the digest is
  defined over). A JSONL line is a value's JSON under the
  [wire contract](wire_contract.md), as is the `ExportRequest` a client
  sends.
- Resuming an interrupted export; a client runs it again.
- Listing past exports: the audit log lists them, filtered by
  `AuditSubject::Export`.
- How the store holds one snapshot for the whole export (one Postgres
  transaction at `REPEATABLE READ` is the intended implementation).

## Data and control flow

```text
export(caller, request)
  1. request.required_permission() ── missing ─▶ Forbidden { missing }; audit Refused; nothing read
  2. W = EdgeStore::watermark()
  3. ExportSource::plan(request, W)
       resolve the filter's version (TopicVersionSelector::resolve, topics_outside),
       cut the window at W (settled_window), capture agent and channel resolution
       and current verdicts, read a projection's frame, count the rows
       ── Err(ExportPlanError) ─▶ QueryError::from; audit Refused
  4. ExportLimits::check(rows) ── over ─▶ Conflict(ExportTooLarge { rows, limit }); audit Refused
  5. ExportHeader::new(..); audit Started(header) ── append fails ─▶ Store, nothing sent
  6. return Export { header, rows: SealedRows(source, ExportSealer(header)) }
wire: header ─▶ row ─▶ … ─▶ trailer { rows, digest, Complete | Failed(why) }; audit Ended(trailer)
      client gone before the trailer ─▶ audit Abandoned { rows }
```

### Request

`ExportRequest::new(dataset, format, include_content)` (checked) refuses
`include_content` for a dataset with no content columns
(`NoContentColumns`). `ExportDataset` carries each dataset's own
selection:

| Dataset | Selection | Rows | Content columns |
| --- | --- | --- | --- |
| `Transmissions(ExportScope)` | window over `Confirmed::at`, filter as `admits` | one per confirmed transmission (`TransmissionRow`: its `TransmissionSummary` and strongest match class) | topic label and the quoted text of each match (`MatchText`: the evidence's `MatchQuotes`, origin and read) |
| `Edges(ExportScope)` | aligned window, filter as `topology` | one per resolved edge bucket (`EdgeRow`) | topic label |
| `Accesses(ExportScope)` | aligned window, `admits_access` | one per resolved access bucket (`AccessRow`) | none |
| `Topics(ExportScope)` | window and filter count transmissions; `topics` selects rows | one per topic of the version, zero counts included (`TopicRow`) | label and terms |
| `Projection(ProjectionId)` | the projection's own spec | one per stored point, in frame order (`PointRow`) | topic label |
| `Verdicts(TimeWindow)` | window over `Transmission::opened_at` | one per verdict record (`VerdictRow`) | none |

`ExportScope` is the window and the shared `TopologyFilter`. A projection
is fixed by its stored spec, so it takes no scope; verdicts exist on
suspected and discarded transmissions, which have no sender or topic for
the filter to test, so they take a window alone, as `detection_quality`
does. `ExportRequest::required_permission` is Content when the request
includes content or names a projection (a layout of embeddings, Content
for `projection` too), View otherwise.

### Settled data and resolution

Every scoped and verdicts export reads only `settled_window(window, W)`:
the part of its window before the watermark read at its start, which no
late match, upgrade or expiry changes any more (`None` when the window is
wholly after `W`, and the export holds no rows). The watermark is a bucket
boundary, so an aligned window stays aligned. The topic version is
resolved once and pinned in the header's filter. Agent and channel
resolution and the copy of current verdicts (for `FalseDetections::Exclude`
and the transmission row's verdict) are captured once at the start and
held for the whole export, so a merge committed mid-stream cannot split
one export's rows across two resolutions. So the rows are a function of
the request, the watermark and that captured state: a re-run at the same
watermark with no merge, unmerge, promotion or verdict in between (and,
with content, no quoted body dropped by content retention) sends the same
rows, count and digest. A projection export reads the stored
frame (`projection_rows`), the same on every export until the frame
expires; its header's basis carries the fit's own spec and watermark.

### Rows

Each dataset's rows are sent in ascending `RowKey` order, unique per
export: transmissions by (`Confirmed::at`, id); edges by (bucket start,
sender, reader, route encoding, topic); accesses by (bucket start, agent,
channel, write before read); topics by id; points by frame index; verdicts
by (transmission, revision). Every agent and channel a row names is
canonical; an edge row is an `EdgeSelector`, so never a self-edge; a
transmission row keeps a transmission whose two agents have since merged,
with equal ends. `verdict_rows(transmission, log)` builds one row per
record with the transmission's route kind and detector call
(`QualityMatch`), so the export reproduces `DetectionQuality::tally`.

**Transmission rows are the surface's.** A `TransmissionRow` is the
`TransmissionSummary` that `transmissions_by_id` lists for the
transmission ([read_models.md](read_models.md#transmission-rows)), built by
the same `TransmissionSummary::of` under the export's captured aliases,
verdict copy and header version, plus `MatchClass::strongest` of its
matches. `TransmissionRow::new` and `TransmissionRow::of` (checked) take
only a confirmed summary (`Confirmed`, `Classified` or `Aggregated`), since
the dataset is windowed and keyed by `Confirmed::at`; the row keeps the
summary's `Delivery`. Its topic is a `TopicUnder` (`Topic`, `Outlier`, or
`Unassigned` under the header's version), its state kind says whether it
has been classified, and its verdict exists because every confirmed state
is judgeable. With content, `TransmissionContent::of(evidence,
topic_label)` takes the transmission's `TransmissionEvidence` assembled
with `ExcerptWindow::MATCH_ONLY`: one `MatchText { class, quotes }` per
match in stored order, where `quotes` is the evidence page's `MatchQuotes`
(origin and read `Excerpted`), so each side is the matched range alone, at
most `Excerpt::MAX_HIGHLIGHT` (8 KiB) with the bytes cut counted, or
`BodyDropped` when content retention dropped the body. An export with
content therefore completes after retention, marking what is gone, and
quotes exactly what the evidence page shows. The rows keep their own shape
only where the surface has none (edge and access buckets, topic counts,
frame points, verdict records), and the digest's canonical encoding is the
export's own because the surface's types have no byte form
([Digest](#digest-and-verification)).

### Manifest

The header (`ExportHeader::new`, checked) holds the export id, the
request, the requester, the start time, the watermark, the basis
(`ExportBasis::Scoped { topic_version, filter, settled }`, `Verdicts {
settled }` or `Projection { projection, spec, fitted }`), the embedding
model current at the start, the `GatewayVersion` and the planned row
count. It checks that the basis is the request's: the dataset's kind, the
filter pinned to the resolved version (equal to the requested version if
one was pinned), the window cut at the watermark, the projection named and
its stored point count planned, and the watermark no later than the start.
An export holds one dataset, so its row count is that dataset's.

The header is known before any row is read, so it is sent first; the
count and digest are known only at the end, so they are in the trailer.
The trailer (`ExportTrailer`) is built only by the sealer and holds the
export id, the rows sent, their `ExportDigest` and an `ExportEnd`:
`Complete`, or `Failed(ExportFailure)` with `Store`, `VersionNotRetained`
(retention dropped the pinned version mid-stream), `CountMismatch` or
`InvalidRow`.

### Stream and sealer

`ExportStream::next(self)` returns `ExportStep::Row(row, rest)` or
`ExportStep::End(trailer)`: the stream is consumed by each call and given
back only with a row, so a trailer always follows the rows and nothing
follows the trailer. `SealedRows` is the stream the surface returns: it
pulls rows from a `RowSource` and passes each through an `ExportSealer`,
which accepts a row only when it is of the header's dataset, carries
content columns exactly when requested, has a key after the previous
row's and is within the planned count. A refused row is not sent and ends
the stream with `Failed(InvalidRow)`; a source failure ends it with the
failure, after the rows already sent; a source that runs out ends it with
`Complete` if every planned row was sent, `Failed(CountMismatch)`
otherwise. The only way to end without a trailer is a stream dropped
because the client went away, which a reader sees as truncation.

### Digest and verification

The digest is `BLAKE3-derive_key("crosstalk export rows v1", …)` over,
for each row in order, its encoded length as `u64` LE followed by
`ExportRow::encode`, a canonical binary encoding (row tag, then the row's
fields in declaration order: ids as `u128` LE, times and counts as `u64`
LE, options and strings tagged and length-prefixed, floats as their bits;
a transmission row writes its summary's fields, then its class and
content, with each excerpt's text, highlight and elided counts or the
dropped body's hash; `export/digest.rs` has the table). The surface's
row types have no canonical byte form of their own (their serde form is
the implementation's), so the encoding lives here. The wire bytes are the encoder's; the
digest does not depend on them, so a JSONL and a Parquet export of the
same rows carry the same digest. `verify_export(header, rows, trailer,
hasher)` is the reader's check: it accepts only a present, `Complete`
trailer of the same export, rows that pass the sealer's checks, equal
planned, sent and received counts, and the trailer's digest. A cut-off,
failed, shortened, extended, altered or reordered export fails it.

### Formats

JSONL: the header is the first line, each row a line, the trailer the last
line; a body without a trailer line is truncated. Parquet: the rows fill
row groups in export order, and the header and trailer are JSON in the
footer's key-value metadata (`crosstalk.export.header`,
`crosstalk.export.trailer`), written on failure too; a file cut off before
its footer cannot be read at all.

### Errors

Before streaming, export returns a `QueryError`: `Forbidden { missing }`;
`ExportPlanError` through its one `From` impl (`Store`; a version through
`VersionUnavailable` as for a linked view; `TopicsNotInVersion`;
`UnalignedWindow` for edges and accesses; a projection through
`ProjectionStoreError` as for `projection`); and
`Conflict(ExportTooLarge { rows, limit })` from `ExportLimits::check`
(config `export.max_rows`, default 10,000,000), counted before any row is
sent, so an oversized export is refused whole rather than cut short. After
the header, failures are not `QueryError`s; the trailer records them.

### Audit

Every export is audited, View and Content alike: an export moves records
out in bulk. `AuditBody::Export(ExportRecord { caller, request, event })`
with `ExportEvent::Refused(QueryError)`, `Started(header)`,
`Ended(trailer)` or `Abandoned { export, rows }`. One call leaves one
`Refused` entry, or `Started` (appended before the header is returned, so
no row leaves unaudited) and then one `Ended` or `Abandoned`; a `Store`
refusal leaves at most one. `ExportRecord::new` (checked) makes a record
`Refused(Forbidden)` exactly when the caller lacks the request's
permission, naming it, and ties a `Started` header to the record's request
and caller. Its subjects are `AuditSubject::Export(id)` once it has one and
`AuditSubject::Projection(id)` for a projection export; its author is the
caller's operator. An export is a query, not an `OperatorAction`: it needs
a read permission, returns a stream rather than an `ActionOutcome`, and
can end after it started, so it has its own record rather than a place in
`act` and the `AuditOutcome` inverse.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/interfaces/l8_surface/export/mod.rs` | Module docs and re-exports | — |
| `spec/types/interfaces/l8_surface/export/request.rs` | The request and its permission, the row limit | `ExportRequest` (checked), `InvalidExportRequest`, `ExportDataset`, `ExportDatasetKind` (`has_content_columns`, `is_content_only`, `code`), `ExportScope`, `ExportFormat`, `ExportLimits` (`check`) |
| `spec/types/interfaces/l8_surface/export/rows.rs` | Row schema per dataset, row order, reference builders | `ExportRow` (`kind`, `has_content`, `key`), `RowKey`, `TransmissionRow` (checked: `new`, `of`), `InvalidTransmissionRow`, `TransmissionContent` (`of`), `MatchText`, `LabelContent`, `EdgeRow`, `AccessRow`, `TopicRow`, `TopicContent`, `PointRow`, `VerdictRow`, `projection_rows`, `verdict_rows`, `VerdictRowsError` |
| `spec/types/interfaces/l8_surface/export/manifest.rs` | Header and trailer | `ExportHeader` (checked), `ExportHeaderParts`, `InvalidHeader`, `ExportBasis`, `GatewayVersion`, `settled_window`, `ExportTrailer`, `ExportEnd`, `ExportFailure`, `SourceFailure` |
| `spec/types/interfaces/l8_surface/export/digest.rs` | Canonical row encoding and the digest | `ExportRow::encode`, `encode_route`, `hash_row`, `RowHasher`, `ExportDigest`, `ROW_DIGEST_CONTEXT` |
| `spec/types/interfaces/l8_surface/export/seal.rs` | Row checks, the trailer, verification | `ExportSealer` (`push`, `finish`, `fail`), `RowRefused`, `verify_export`, `Incomplete` |
| `spec/types/interfaces/l8_surface/export/stream.rs` | The stream and the stores behind it | `Export`, `ExportStep`, `ExportStream`, `RowSource`, `SealedRows`, `ExportSource` (`plan`), `ExportPlan`, `ExportPlanError` |
| `spec/types/interfaces/l8_surface/export/record.rs` | Exports in the audit log | `ExportRecord` (checked), `ExportEvent`, `InvalidExportRecord` |
| `spec/types/interfaces/l8_surface.rs` | `QueryApi::export` and `QueryApi::ExportRows` | — |
| `spec/types/interfaces/l8_surface/audit.rs` | `AuditBody::Export`, `AuditSubject::Export`, `AuditSubject::Projection` | — |
| `spec/types/interfaces/l8_surface/errors.rs` | `ConflictKind::ExportTooLarge` | — |
| `spec/types/interfaces/l8_surface/summary.rs`, `evidence.rs`, `excerpt.rs` | The transmission row, the match quotes and `ExcerptWindow::MATCH_ONLY` a transmissions export reuses ([read_models.md](read_models.md)) | — |
| `spec/types/interfaces/l8_surface/query_errors.rs` | `From<ExportPlanError> for QueryError` | — |
| `spec/types/ids.rs` | `ExportId` | — |
| `spec/types/tests/export.rs` | Requests, headers, limits, plan errors, transmission, projection and verdict rows, audit records | — |
| `spec/types/tests/export_stream.rs` | Encoding, digest, sealer, sealed stream, verification, a transmission row is a confirmed summary | — |
| `spec/types/tests/evidence.rs` (part) | Export content quotes the evidence | — |

## Invariants and constraints

- A request with content columns for a dataset that has none cannot be
  built. An export needs View, or Content when it includes content or
  reads a projection, checked before anything is read.
- A header's basis is its request's: the dataset's kind, the filter pinned
  to the resolved version, the window cut at the watermark, the projection
  named with its stored point count, and a watermark no later than the
  start.
- Scoped and verdicts exports read only data before the watermark read at
  their start; agent and channel resolution and verdicts are captured once
  at the start; every topic in the rows belongs to the header's version. A
  re-run with the same request at the same watermark, with no merge,
  unmerge, promotion or verdict in between (and, with content, no quoted
  body dropped), sends the same rows, count and digest. A projection export sends its stored frame.
- A transmission row is the `TransmissionSummary` of a confirmed
  transmission under the export's captured resolution and header version,
  with the strongest match class; its content quotes are the evidence's,
  cut with no context.
- Rows are of the header's dataset, carry content exactly when requested,
  are in strictly increasing key order and no more than planned; after one
  refusal every later row is refused.
- Every stream ends with exactly one trailer, built only by the sealer,
  counting and digesting exactly the rows sent, `Complete` only when every
  planned row was sent and none refused. A store failure mid-stream is
  recorded in the trailer.
- The digest is defined over the canonical row encoding, so it does not
  depend on the format. `verify_export` accepts exactly a complete,
  untampered export; a truncated one never verifies.
- An export over `ExportLimits::max_rows` is `Conflict(ExportTooLarge)`
  before anything is sent.
- Every export call is audited: one `Refused` entry, or `Started` before
  the first row and then one `Ended` or `Abandoned`. An export record is
  `Forbidden` exactly when its caller lacks the request's permission.
