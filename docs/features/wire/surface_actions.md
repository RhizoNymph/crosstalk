# Wire contract: surface actions, audit and live feed

Part of the [wire contract](../wire_contract.md). This page covers the
operator surface's own wire types: the action a client sends and the
action it becomes, the audit log, the live feed and its SSE framing, the
operator directory and permissions, alert sinks, the list filters and the
overview (`interfaces/l8_surface/{actions,actions/request,audit,live,operators,permissions,sinks,lists,overview}.rs`).

## Actions: request, action, outcome

```text
client ── ActionRequest JSON ──▶ decode_request::<ActionRequest>
                                   └─ into_action(&caller) ─┬─ Ok(OperatorAction) ─▶ act(caller, action) ─▶ ActionOutcome
                                                            └─ Err(SelfMerge) ─▶ InvalidInput(SelfMerge), never audited
audit log ◀── OperatorRecord { caller: CallerSnapshot, action: OperatorAction, outcome }
```

- `ActionRequest` (a `WireRequest`) has one variant per `OperatorAction`
  variant, same name, same fields, except `MergeAgents { from, into }`,
  which names no author: `{"type": "merge_agents", "data": {"from":
  "01J..", "into": "01J.."}}`. Every other variant carries exactly its
  action's fields, none of which is stamped (where applying an action
  records an operator or a time, the surface stamps it then).
- `ActionRequest::into_action(self, &Caller) -> Result<OperatorAction,
  SelfMerge>` stamps it: a merge is authored by
  `MergeAuthor::Operator(caller.operator())` through
  `OperatorAction::merge_agents`, which refuses one agent named twice with
  the existing `SelfMerge`. It takes no time: no action holds one.
- `ActionRequest::of(&OperatorAction)` is the inverse and
  `ActionRequest::kind` names the action kind; with `into_action` all
  three match exhaustively, so a new action does not compile until it has
  its request form. Each action comes from exactly one request variant
  (`surface.wire.action-request-covers-actions`).
- `OperatorAction` serializes both ways (the audit log returns it) and is
  never a request; a merge carries `"by": {"type": "operator", "data":
  ..}`. Sending that form as a request is refused as an unknown field
  `by`.
- `ActionOutcome` is adjacently tagged; `SupersededChannels` is an array
  of channel ids, sorted and deduplicated on decode as its constructor
  does.

## Audit log

- `AuditEntry { id, at, body }`, `body` one of `operator`, `config`,
  `export`. `AuditFilter` is a request whose `by` is the client's choice
  of authors (its golden allows that key).
- `OperatorRecord` and `ExportRecord` keep a `CallerSnapshot`, not a
  `Caller`: `{"operator": "01J..", "permissions": ["view", "audit"]}`.
  It is public plain data (`of(&Caller)`, `operator`, `permissions`,
  `has`), serde both ways and not a request. Its decode goes through
  `CallerSnapshot::new`, which refuses an empty set (`NoPermissions`):
  `OperatorDirectory::caller` never grants one. Nothing converts a
  snapshot into a `Caller`, so decoding the log yields no authority
  (`surface.wire.authority-not-decoded`; a `compile_fail` doctest in
  `wire/authority.rs`). Records decode through their constructors, so a
  record whose outcome contradicts its snapshot's permissions is a decode
  error. The constructors take `impl Into<CallerSnapshot>`: the surface
  passes the call's `Caller`, decoding the snapshot it read.

## Live feed and SSE framing

Each `LiveItem` is one SSE event:

```text
event: event
id: 7-1042
data: {"type":"event","data":{"cursor":"7-1042","event":{"type":"alert_changed","data":{"id":"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}}}

```

- `event` is `LiveItem::event_name`, the item's JSON `type`: `event`,
  `resync` or `heartbeat`.
- `id` is the cursor's text, `LiveCursor::encode` (`<epoch>-<seq>`, both
  decimal, no leading zeros), the same string as the JSON's `cursor`. The
  browser sends the last one back as `Last-Event-ID`, which
  `Resume::from_last_event_id` reads. Heartbeats carry one too.
- `data` is the whole item as one line of JSON.
- The stream's last event is named `end` (`LiveEnd::EVENT_NAME`), its
  data the `LiveEnd` string (`"lagged"`), with no `id`, so the client's
  resume point stays its last cursor
  (`surface.live.sse-frame-matches-item`).
- `UiEvent` is adjacently tagged with `{"id": ..}` (or `{"at": ..}`,
  `{"version": ..}`) data; `ResyncReason` and `LiveEnd` are strings;
  `Resume` is adjacently tagged, though the server builds it from the
  `Last-Event-ID` header, not from JSON.

## Operators, sinks, lists, overview

- `Operator { id, name, permissions }`; a former operator's permissions
  are `[]`. `OperatorName` is checked text (trimmed; `Blank`, `TooLong`,
  `ControlCharacter` refused). `PermissionSet` is an array in
  `Permission::ALL` order, decoded from any order with repeats counted
  once. `AccessMode` is a string.
- `SinkInfo::last_delivery` is `null`, `{"type": "succeeded", "data":
  "<timestamp>"}` or `{"type": "failed", "data": <SinkError>}`, never
  serde's `{"Ok": ..}` form of a `Result`.
- Requests: `ChannelFilter` (its `OriginFilter` adjacently tagged:
  `in_force`, `with_superseded`, `superseded`), `AlertRuleFilter`,
  `SearchRequest` (`text` is checked non-blank text). `AgentFilter` is
  re-exported from the agents area, which owns its goldens.
- Responses: `TopicPage`, `OverviewCounts`.

## Non-scope

`ActionKind`, `OutcomeKind`, `AuditError`, `FeedWindow`, `ResumePlan`,
`LiveConfig`, `AccessConfig`, `OperatorConfig`, `TrustedOperator`,
`OperatorDirectory`, `RequestIdentity`, `Unauthenticated`,
`InvalidAccessConfig` and the traits (`OperatorActions`, `AuditLog`,
`LiveFeed`, `LiveStream`, `AlertSink`): no wire root reaches them.

## Files

| File | Role |
| --- | --- |
| `spec/types/interfaces/l8_surface/actions/request.rs` | `ActionRequest` (a `WireRequest`): `into_action`, `of`, `kind` |
| `spec/types/interfaces/l8_surface/permissions.rs` | `CallerSnapshot` (checked; `NoPermissions`), `PermissionSet` as an array in `Permission::ALL` order |
| `spec/types/interfaces/l8_surface/live.rs` | `LiveItem::event_name`, `LiveEnd::EVENT_NAME`, `LiveCursor`'s text (no leading zeros) |
| `spec/types/tests/wire/surface_actions/` | `actions.rs`, `audit.rs`, `live.rs`, `lists.rs`, `operators.rs` |
| `spec/types/tests/golden/surface_actions/{actions,audit,live,lists,operators}/` | 59 goldens: one per `ActionRequest` variant, every `OperatorAction`, every `AuditBody` and `UiEvent` |

## Invariants

`surface.wire.action-request-covers-actions` and
`surface.live.sse-frame-matches-item`; `surface.wire.authority-not-decoded`
holds the caller snapshot's rule. The general wire invariants take the
area's goldens and rejections as evidence.
