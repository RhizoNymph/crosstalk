# Wire contract: analysis

Part of the [wire contract](../wire_contract.md). This page covers alert
rules, topics and their history, retention, projections, search results
and the insight bus events (L6): `aggregates/alert/rules.rs`,
`aggregates/{topic,topic_history,retention}.rs`,
`aggregates/projection/mod.rs`, `interfaces/l6_analysis.rs`,
`events/insight.rs`. Areas `rules`, `topics`, `projections` and
`insight`.

## Scope

| Type | Wire role |
| --- | --- |
| `UserRule` (with `WatchedTopics`) | request: the rule a client writes in `CreateRule` and `UpdateRule` (inside an `ActionRequest`); its author and time are stamped outside it |
| `TopicModelVersion` | request: the number `topic_sizes` (as `Option`, `null` for the active version) and `topic_lineage` take, and the pin and unpin actions carry; an unknown version is the query's `NotFound`, not a decode error |
| `ProjectionParams` (with `ProjectionLimit`) | request: `fit_projection`'s parameters, seed included |
| `AlertRuleDef`, `AlertRule`, `ContentRule`, `TopicWatch`, `QueryWatch`, `SemanticQuery`, `BuiltinRule`, `AlertRuleKind`, `RuleStatus`, `RuleRevision` | response (`alert_rules`) and bus payload (`AlertRuleChanged`); `AlertRuleDef` is stamped (its creator) |
| `TopicVersionHistory`, `TopicVersionInfo`, `TopicVersionStatus`, `FitRecord`, `CompletedFit`, `Retention`, `Pin` | response (`topic_versions`); `Pin` is stamped |
| `TopicSizes`, `TopicSize`, `TopicLineage`, `LineageEntry`, `LineageLink`, `Topic`, `Embedding`, `EmbeddingModel` | response (`topic_sizes`, `topic_lineage`, `topics`) |
| `ProjectionInfo`, `ProjectionSpec`, `ProjectionStatus`, `Fitted`, `ProjectedPoint`, `PointRoute` | response (`projection_status`, `projections`, an export's point rows); `ProjectionInfo` and `ProjectionSpec` are stamped |
| `RetentionPolicy`, `FrameRetention` | inside an audit entry's `ConfigChange` (`SetTopicRetention`, `SetFrameRetention`); `FrameRetention` also in `Present` ([surface reads](surface_reads.md#the-present)) |
| `SearchResults`, `SearchHit` | response (`search`) |
| `InsightEvent`, `ClassificationCause`, `AlertRevision` | bus payload, inside `Envelope` |

## Non-scope

`Projection` and `ProjectionFrame` have no serde (see
[`QueryApi::projection`](#queryapiprojection)). `AlertRuleSet`,
`AlertRuleConfig`, `RuleDefinition`, `StaleReason`, `AlertDraft`,
`TriageOutcome`, `PinChange`, `TopicVersionStatusKind`,
`TopicAssignment`, `Assignment`, `SearchQuery`, `Sample`, the L6 traits
and store errors are not on the wire: in-memory indexes, config,
in-process values, and errors that reach a client as the `QueryError`
they map to.

## Checked decoding

`AlertRuleDef` decodes through `AlertRuleDef::builtin` for a built-in rule
and `AlertRuleDef::load` for a user rule (`load` accepts every stored
status and staleness), so a built-in rule under any id but its fixed one,
or a user rule under an id in the reserved range, is
`invalid alert rule: BuiltinId` or `Reserved` (`InvalidRuleDef`), in a
response and on the bus alike. `TopicVersionInfo`, `TopicVersionHistory`,
`TopicSizes`, `LineageEntry`, `TopicLineage`, `Embedding`,
`ProjectionLimit`, `ProjectionParams`, `ProjectionInfo` and
`RetentionPolicy` (`{"keep_last": 3}`, at least 2) decode through their
constructors, with a rejection test per constructor error. A
`FrameRetention` is whole microseconds and never zero.

A semantic rule's text (`UserRule::SemanticQuery::text`,
`SemanticQuery::text`) is a `RuleQueryText`: trimmed, non-empty and at most
`RULE_QUERY_MAX_CHARS` (1,000) characters, so `"   "` is
`invalid query text: Blank` and longer text
`invalid query text: TooLong { max: 1000, got: .. }`. A client checks it
with `RuleQueryText::new` before sending.

A `ProjectedPoint`'s `route` is a `PointRoute`, adjacently tagged: a
channel route carries its channel, `{"type": "channel", "data": "<id>"}`,
and the others nothing, `{"type": "direct"}`; a bare kind string
(`"channel"`), a channel route without its channel and another route with
one are decode errors. The channel is the canonical one when the sample
was read.
`TopicVersionHistory` is `{"versions": [..]}`: the active version's index
is found again from the statuses.

`ProjectionSpec` decodes through `ProjectionSpec::new`, which pins the
filter to the resolved version, so a spec written with
`{"type": "current"}` decodes pinned (normalization, as `NonBlank`
trims). That is safe because a spec is server-stamped (the topic version
and embedding model resolved when the fit was accepted) and never a
`WireRequest` (`wire/authority.rs`): a client asks for a fit with
`ProjectionParams`, a window and a `TopologyFilter`, and no request type
holds a `ProjectionSpec`, so no client value is ever normalized.

## Floats are finite

Every float in these types is behind a checked type: similarities,
thresholds and search scores are `Similarity` (`0.0..=1.0`), embeddings
are unit-norm (`Embedding::new`), and a topic term's c-TF-IDF weight
(`Topic::terms`, and an export's `TopicContent::terms`) and a projected
point's `x` and `y` are `support::Finite` (an `f32` never NaN or
infinite, a JSON number; the crate's one such type). JSON has no NaN, but
serde_json narrows a JSON number to `f32` through `f64`, so `1e39` decodes
to infinity; each of these types refuses it
(`canonical.wire.finite-floats`). The UMAP minimum distance is held in
thousandths (`min_dist_milli`), so no float of a spec is on the wire.

**Tuples.** A user rule's `created` is `[operator, time]` and a topic's
`terms` is `[[term, weight], ..]`, per the tuple convention.

## `QueryApi::projection`

A `Projection` (its job record and its frame) has no JSON form, because
the frame is binary with its own layout (`ProjectionFrame::encode` and
`decode`). The answer is split by content type:

- the job record is JSON: the `ProjectionInfo` that `projection_status`
  returns, its `status` saying whether a frame exists (`ready`);
- the frame is `application/octet-stream`: the bytes of
  `ProjectionFrame::encode`, identical on every read until it expires, so
  cacheable for as long as the job is `ready`;
- an error on either (`NotFound`, `Conflict(ProjectionNotReady)`,
  `Conflict(ProjectionFailed)`, `ProjectionNotRetained`) is the usual
  `QueryError` JSON.

The UI reads `projection_status` (or the `projections` list), and once a
job is `ready` fetches its frame bytes, decodes them with
`ProjectionFrame::decode` into the spec type (typed-array views over the
same bytes for the canvas) and joins the two with `Projection::new`, which
refuses a frame whose header disagrees with the job (id, topic version,
watermark, limit, counts). `a_projection_travels_as_info_json_and_frame_bytes`
pins that round trip. The frame is format 2: its channels table and
channel column let the canvas colour points by channel from the bytes
alone, and one `channel_names` batch over the table names them; a decoder
refuses format 1. The routes themselves are HTTP routing, outside
this feature.

## Bus events

Each `InsightEvent` variant has a golden inside a full `Envelope`
(`{"id", "at", "event": {"type": "insight", "data": {"type": .., "data":
..}}}`), so the NATS bytes of every analysis event are pinned; an unknown
insight variant or field, a zero revision and a rule with a refused id are
decode errors there too.

## Files

| File | Role |
| --- | --- |
| `spec/types/aggregates/alert/mod.rs` | Alerts (`Alert`, `AlertState`, `AlertSubject`, `SuppressReason`, `AlertRevision`, …); re-exports every rule type, so `aggregates::alert::<Type>` paths are unchanged |
| `spec/types/aggregates/alert/rules.rs` | Alert rules (`UserRule`, a request; `AlertRuleDef`, decoded through `builtin` or `load`), split out when `alert.rs` reached 1000 lines |
| `spec/types/support.rs` | `Finite`, `NotFinite`, `QueryText` (a semantic rule's text, as `RuleQueryText`) |
| `spec/types/aggregates/projection/mod.rs` | `ProjectionParams` (a request), `ProjectionSpec` (decoded pinned; stamped), `ProjectionInfo`, `ProjectedPoint` (`Finite` coordinates, a `PointRoute`), `FrameRetention` |
| `spec/types/aggregates/retention.rs` | `RetentionPolicy`, decoded through `RetentionPolicy::new` |
| `spec/types/tests/wire/analysis/` | `rules.rs`, `topics.rs`, `projections.rs`, `insight.rs` |
| `spec/types/tests/golden/{rules,topics,projections,insight}/` | 13, 10, 10 and 11 goldens |

## Invariants

`canonical.wire.finite-floats`, `analysis.rule.query-text-bounded`,
`analysis.projection.point-channel-matches-route`; the general wire
invariants take the area's goldens and rejections as evidence.
