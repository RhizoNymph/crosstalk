# Wire contract: surface read models and export

Part of the [wire contract](../wire_contract.md). This page covers the
channel, transmission and evidence read models, the gateway's present and
config, and export
(`interfaces/l8_surface/{channels,channel_traffic,summary,evidence,excerpt,present}.rs`,
`interfaces/l8_surface/export/`). The export's line format is
[export.md](../export.md#formats)'s; this page pins each line's JSON.

## Scope: what each type is on the wire

| Type | Role | Decoded |
| --- | --- | --- |
| `TransmissionSelection` | request: an array of ids | through `new` (distinct, newest first, 1 to 100,000) |
| `ExcerptWindow` | request: `{"context": n}` | through `new` (at most 2,048) |
| `ExportRequest` (with `ExportDataset`, `ExportScope`, `ExportFormat`) | request | through `new` (no content for accesses or verdicts) |
| `Present` (with `ExportFormats`, `FrameRetention`) | response of `present`; the server's clock and config, never a request | plain struct; `ExportFormats` through `new` (non-empty, distinct), `FrameRetention` and `BucketWidth` non-zero, `Similarity` in range |
| `ChannelRow` (with `ChannelStanding`, `ChannelActivity`, `ChannelCounts`) | response of `channel`, `channels` (watermarked); a row in force is `{"type": "in_force", "data": {"traffic": <CrossTraffic>, "activity": ..}}` | through `new`, which also refuses cross-agent traffic on a channel whose stored detection has none (`TrafficWithoutDetection`) |
| `ChannelTransmission`, `ChannelTransmissionPage` | response of `channel_transmissions`: `{"summary": <TransmissionSummary>, "senders": [..]}` per row; the page `{"channel", "topic_version", "page"}` | `ChannelTransmission` checks what it holds: senders ascending and distinct (`SendersUnordered`), none the summary's reader (`SenderIsReader`), and a confirmed summary's senders exactly its delivery's sender (`SendersNotDelivery`); it cannot rerun `ChannelTransmission::of`, which reads the transmission and the aliases |
| `ChannelTransmissionFilter` | request: `{"confirmation": null}`, `"confirmed"` or `"unconfirmed"` | plain |
| `SupersededInto`, `ChannelName` (with `ChannelShape`) | response (inside a row; `channel_names` as an object keyed by id) | field by field: `SupersededInto::of` and `ChannelName::of` read the registry; `ChannelRow::new` checks a row's supersession against its channel |
| `PromotionPreview` | response: `{"type": "promotes", "data": <PromotionCoverage>}` or `{"type": "refused", "data": <ConflictKind>}` | refuses a conflict other than `ChannelSuperseded`, `ChannelNotDiscovered`, `PatternOverlaps` (`NotAPromotionConflict`); the coverage as received |
| `TransmissionSummary`, `SummaryState`, `Delivery`, `TopicUnder`, `TransmissionPage` | response of `transmissions_by_id` | plain: the per-state shape is the enum |
| `TransmissionEvidence` (with `MatchEvidence`, `MatchQuotes`, `AccessDetail`) | response of `transmission_evidence` (`Option`) | through `assemble` over its own transmission, answering each request with the next decoded match or access (`InvalidTransmissionEvidence`); `AccessDetail` through `new` |
| `Excerpt`, `Excerpted` | response (inside evidence and export rows); the highlight as `{"start", "end"}` | `Excerpt` through `new`, which now also refuses counts that fit no part (`CountsOverflow`) |
| `ExportRequest`'s transmissions dataset (`TransmissionScope`, `ExportStates`) | request | `{"window", "filter", "states": ["suspected", ..]}`; `states` left out when it is the default, and decoded through `ExportStates::new` (`[]` is `Empty`, a repeat `Duplicate`, `detected` `Detected`); goldens `export_request_transmissions` (default, unchanged) and `export_request_transmissions_all_states` |
| `ExportHeader` (with `ExportBasis`, `GatewayVersion`) | export line, audit event; stamped, never a request | through `ExportHeader::new` (its `ExportHeaderParts`) |
| `ExportRow` and each dataset's row | export line | `TransmissionRow` through its checks (its delivery is the summary's, not written twice; a delivery whose sender is the summary's reader is `WithinOneAgent`; `strongest` is left out for an unconfirmed row and `state`, the summary's `TransmissionStateKind` as a string, is written only in an export with explicit states, `{"summary", "content", "state": "suspected"}`); a point row's `ProjectedPoint` through `ProjectedPoint::new` (its `PointParts`; a point whose sender is its reader is refused); the rest plain |
| `ExportTrailer` (with `ExportEnd`, `ExportFailure`, `RowRefused`, `ExportDigest`) | export line, audit event | checked for what the sealer guarantees about it alone (`InvalidTrailer`); rows and digest by `verify_export` |
| `ExportLine` | one JSONL line: `header`, `row` or `trailer`, adjacently tagged; never a request (it holds the stamped header) | plain; `read_jsonl` checks the framing ([export.md](../export.md#formats)) |
| `ExportEvent` | inside the audit log's `ExportRecord` | plain |

## Non-scope

No serde, because no wire root reaches them: `TransmissionStateKind`, the
error enums (`InvalidSelection`, `InvalidEvidence`, `EvidenceError`,
`ExcerptError`, `CutError`, `InvalidExcerpt`, `InvalidHeader`,
`InvalidChannelTransmission`, `InvalidTransmissionRow`,
`InvalidTrailer`, `InvalidExportRequest`, `SourceFailure`, `JsonlError`),
`RowKey`, `ExportLimits` (config), `JsonlExport` (what a reader holds), and
the stream and sealer machinery. `ExportRecord` (the audit log's record of
an export, keeping a `CallerSnapshot` of its caller) is goldened with the
audit log ([surface_actions.md](surface_actions.md#audit-log),
`surface_actions/audit/entry_export_*`).

## Channel rows and a channel's transmissions

A row's `Listing` and `Confirmation` are not on the wire: the client
derives them from the stored channel and the row's `CrossTraffic`
(`ChannelRow::listing`, `confirmation`), so they cannot disagree with the
traffic (`surface.channels.listing-from-traffic`). Neither is
`ChannelRow::created_at`, the list's sort key: it is the channel's
declaration time or its seed's `opened_at`, both already in the row's
channel (`surface.channels.rows-newest-created-first`). A superseded row's
standing carries no traffic. Goldens: `channels/channel_row_unconfirmed`
(an unconfirmed channel, traffic `{"confirmed": 0, "unconfirmed": 1}`),
`channels/confirmations` and `channels/cross_traffic` beside the existing
rows.

`channel_transmissions` lists the cross-agent transmissions routed through
a channel, each with the senders its evidence names
(`surface.channels.transmissions-cross-agent`). Goldens under
`channel_traffic/`: `channel_transmission_confirmed`,
`channel_transmission_suspected`, `channel_transmission_page`, and the two
request goldens `channel_transmission_filter_all` and
`channel_transmission_filter_unconfirmed`.

## The present

`QueryApi::present` answers with one object a client reads before it
builds requests:

```json
{"now": "2026-10-04T12:58:30.000000Z", "bucket_width": 300000000,
 "export_formats": ["jsonl"], "current_rule_version": 4,
 "default_remap_threshold": 0.8, "frame_retention_micros": 15552000000000}
```

`bucket_width` is microseconds, like `BucketWidth` everywhere;
`frame_retention_micros` follows the duration convention. The formats are
in offer order: `[]` is `invalid export formats: Empty` and a repeat
`Duplicate`. An export in a format outside the list is answered with
`{"type": "invalid_input", "data": {"type": "unsupported_format", "data":
{"format": "parquet"}}}`. Goldens: `present/present_jsonl_only`,
`present/present_every_format`.

An export's point row carries the point's `PointRoute`
(`{"type": "channel", "data": "<id>"}` for a channel route), its channel
written into the canonical row encoding after the route code. A point's
JSON is unchanged by `ProjectedPoint` becoming checked: it is its
`PointParts`, and a point whose `from` equals its `to` is refused
(`analysis.projection.point-cross-agent`), as `TransmissionRow` refuses a
transmission within one agent (`surface.export.transmission-row-cross-agent`).

## Goldens and digests

Goldens are under
`spec/types/tests/golden/surface_reads/{channels,channel_traffic,transmissions,evidence,export,present}/`;
the one JSONL golden, a complete export line by line, is
`surface_reads/export/export_complete.jsonl`, beside the JSON ones (the
layout check admits `.json` and `.jsonl`). The trailer digests in these
goldens come from the tests' stand-in `RowHasher`, not BLAKE3: the goldens
pin the framing and the canonical row encoding, not real digest values.

## Stage-0 choices changed

- `PromotionPreview` was `transparent` over its outcome; it now decodes
  through `TryFrom<Outcome>`, refusing a conflict no promotion is refused
  with (`surface.channels.preview-refusal-is-a-promotion-conflict`).
- `TransmissionEvidence` decoded field by field; it now decodes through
  `assemble`, so decoded evidence lists exactly its transmission's matches
  and co-access accesses, in order (`surface.evidence.follows-transmission`).
- `ExportTrailer` decoded field by field; it now refuses what no sealer
  builds (`surface.export.trailer-self-consistent`).
- `Excerpt::new` refuses counts that place the matched range past
  `u32::MAX` or overflow the part's length
  (`surface.excerpt.counts-fit-a-part`); `Excerpt::cut` never builds such
  counts.
- `ExportLine` is new: the export group's line format, so a JSONL line
  is one tagged value.
- Channel semantics: `ChannelStanding::InForce` became a struct variant
  carrying the row's `CrossTraffic`; `ChannelTransmission`,
  `ChannelTransmissionFilter` and `ChannelTransmissionPage` are new;
  `TransmissionRow` refuses a transmission within one agent.

## Files

| File | Role |
| --- | --- |
| `spec/types/interfaces/l8_surface/export/framing.rs` | `ExportLine` (one JSONL line), `read_jsonl`, `JsonlExport`, `JsonlError`, the Parquet footer keys |
| `spec/types/interfaces/l8_surface/export/{record,rows,digest}.rs` | `ExportRecord` with its `CallerSnapshot`; rows with `Finite` topic weights and point coordinates; the canonical row encoding over them |
| `spec/types/interfaces/l8_surface/present.rs` | `Present` |
| `spec/types/interfaces/l8_surface/channel_traffic.rs` | `ChannelTransmission` (decoded through `RawChannelTransmission`, `InvalidChannelTransmission`), `ChannelTransmissionFilter` (a request), `ChannelTransmissionPage` |
| `spec/types/interfaces/l8_surface/export/request.rs` | `ExportFormats` (checked), `UnsupportedFormat` |
| `spec/types/tests/wire/surface_reads/` | `fixtures.rs`, `channels.rs`, `channel_traffic.rs`, `transmissions.rs`, `evidence.rs`, `export.rs` (including the JSONL golden and framing errors), `present.rs` |
| `spec/types/tests/golden/surface_reads/` | 70 goldens (69 JSON, 1 JSONL) |

## Invariants

New: `surface.present.export-formats-offered`,
`surface.export.unsupported-format-refused`,
`surface.export.jsonl-framing`, `surface.export.trailer-self-consistent`,
`surface.excerpt.counts-fit-a-part`,
`surface.channels.preview-refusal-is-a-promotion-conflict`; with the
channel semantics, `surface.channels.listing-from-traffic`,
`surface.channels.transmissions-cross-agent`,
`surface.channels.rows-newest-created-first`,
`surface.export.transmission-row-cross-agent` and
`analysis.projection.point-cross-agent`. The decode
tests are also evidence for the area invariants they recheck
(`surface.channels.row-standing-matches-origin`,
`surface.evidence.follows-transmission`, `surface.excerpt.well-formed`,
`surface.query.selection-bounded`, `surface.export.header-matches-request`,
`surface.export.request-content-columns`,
`surface.export.transmission-row-confirmed`,
`surface.export.transmission-states-default-unchanged`,
`surface.export.transmission-row-states`,
`surface.export.truncation-detected`) and for the general wire
invariants (`canonical.wire.checked-decode`, `goldens-pin-format`,
`round-trip`, `strict-decode`, `surface.wire.authority-not-decoded`).
