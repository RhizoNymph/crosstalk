# Wire contract: surface read models and export

Part of the [wire contract](../wire_contract.md). This page covers the
channel, transmission and evidence read models and export
(`interfaces/l8_surface/{channels,summary,evidence,excerpt}.rs`,
`interfaces/l8_surface/export/`). The export's line format is
[export.md](../export.md#formats)'s; this page pins each line's JSON.

## Scope: what each type is on the wire

| Type | Role | Decoded |
| --- | --- | --- |
| `TransmissionSelection` | request: an array of ids | through `new` (distinct, newest first, 1 to 100,000) |
| `ExcerptWindow` | request: `{"context": n}` | through `new` (at most 2,048) |
| `ExportRequest` (with `ExportDataset`, `ExportScope`, `ExportFormat`) | request | through `new` (no content for accesses or verdicts) |
| `ChannelRow` (with `ChannelStanding`, `ChannelActivity`, `ChannelCounts`) | response of `channel`, `channels` (watermarked) | through `new` |
| `SupersededInto`, `ChannelName` (with `ChannelShape`) | response (inside a row; `channel_names` as an object keyed by id) | field by field: `SupersededInto::of` and `ChannelName::of` read the registry; `ChannelRow::new` checks a row's supersession against its channel |
| `PromotionPreview` | response: `{"type": "promotes", "data": <PromotionCoverage>}` or `{"type": "refused", "data": <ConflictKind>}` | refuses a conflict other than `ChannelSuperseded`, `ChannelNotDiscovered`, `PatternOverlaps` (`NotAPromotionConflict`); the coverage as received |
| `TransmissionSummary`, `SummaryState`, `Delivery`, `TopicUnder`, `TransmissionPage` | response of `transmissions_by_id` | plain: the per-state shape is the enum |
| `TransmissionEvidence` (with `MatchEvidence`, `MatchQuotes`, `AccessDetail`) | response of `transmission_evidence` (`Option`) | through `assemble` over its own transmission, answering each request with the next decoded match or access (`InvalidTransmissionEvidence`); `AccessDetail` through `new` |
| `Excerpt`, `Excerpted` | response (inside evidence and export rows); the highlight as `{"start", "end"}` | `Excerpt` through `new`, which now also refuses counts that fit no part (`CountsOverflow`) |
| `ExportHeader` (with `ExportBasis`, `GatewayVersion`) | export line, audit event; stamped, never a request | through `ExportHeader::new` (its `ExportHeaderParts`) |
| `ExportRow` and each dataset's row | export line | `TransmissionRow` through `new` (its delivery is the summary's, not written twice); the rest plain |
| `ExportTrailer` (with `ExportEnd`, `ExportFailure`, `RowRefused`, `ExportDigest`) | export line, audit event | checked for what the sealer guarantees about it alone (`InvalidTrailer`); rows and digest by `verify_export` |
| `ExportLine` | one JSONL line: `header`, `row` or `trailer`, adjacently tagged; never a request (it holds the stamped header) | plain; `read_jsonl` checks the framing ([export.md](../export.md#formats)) |
| `ExportEvent` | inside the audit log's `ExportRecord` | plain |

## Non-scope

No serde, because no wire root reaches them: `TransmissionStateKind`, the
error enums (`InvalidSelection`, `InvalidEvidence`, `EvidenceError`,
`ExcerptError`, `CutError`, `InvalidExcerpt`, `InvalidHeader`,
`InvalidTrailer`, `InvalidExportRequest`, `SourceFailure`, `JsonlError`),
`RowKey`, `ExportLimits` (config), `JsonlExport` (what a reader holds), and
the stream and sealer machinery. `ExportRecord` (the audit log's record of
an export, keeping a `CallerSnapshot` of its caller) is goldened with the
audit log ([surface_actions.md](surface_actions.md#audit-log),
`surface_actions/audit/entry_export_*`).

## Goldens and digests

Goldens are under
`spec/types/tests/golden/surface_reads/{channels,transmissions,evidence,export}/`;
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

## Files

| File | Role |
| --- | --- |
| `spec/types/interfaces/l8_surface/export/framing.rs` | `ExportLine` (one JSONL line), `read_jsonl`, `JsonlExport`, `JsonlError`, the Parquet footer keys |
| `spec/types/interfaces/l8_surface/export/{record,rows,digest}.rs` | `ExportRecord` with its `CallerSnapshot`; rows with `Finite` topic weights and point coordinates; the canonical row encoding over them |
| `spec/types/tests/wire/surface_reads/` | `fixtures.rs`, `channels.rs`, `transmissions.rs`, `evidence.rs`, `export.rs` (including the JSONL golden and framing errors) |
| `spec/types/tests/golden/surface_reads/` | 60 goldens (59 JSON, 1 JSONL) |

## Invariants

New: `surface.export.jsonl-framing`, `surface.export.trailer-self-consistent`,
`surface.excerpt.counts-fit-a-part`,
`surface.channels.preview-refusal-is-a-promotion-conflict`. The decode
tests are also evidence for the area invariants they recheck
(`surface.channels.row-standing-matches-origin`,
`surface.evidence.follows-transmission`, `surface.excerpt.well-formed`,
`surface.query.selection-bounded`, `surface.export.header-matches-request`,
`surface.export.request-content-columns`,
`surface.export.transmission-row-confirmed`,
`surface.export.truncation-detected`) and for the general wire
invariants (`canonical.wire.checked-decode`, `goldens-pin-format`,
`round-trip`, `strict-decode`, `surface.wire.authority-not-decoded`).
