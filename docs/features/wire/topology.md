# Wire contract: topology, agents and the bus

Part of the [wire contract](../wire_contract.md). This page covers the
topology aggregates and their requests
(`aggregates/{access,edge,filter,node,quality,series,watermark}.rs`), the
agent read models (`aggregates/agents/`), L7's store interface
(`interfaces/l7_topology.rs`), the transport's wire types
(`interfaces/l2_transport.rs`) and the bus framing (`events/mod.rs`,
`events/changed.rs`). Areas `topology`, `agents` and `bus`.

## Scope

- Requests (`WireRequest`, `assert_request_golden`): `TopologyFilter`,
  `TopicVersionSelector`, `Weighting`, `EdgeSelector` (checked),
  `SeriesGrid` (checked; its wire form is `{"window", "step"}`, the point
  count is computed), `SeriesGrouping`, `AgentFilter`, and
  `ConsumerGroup`, which `QueryApi::dead_letters` takes from the client
  (`Option<ConsumerGroup>`; `null` lists every group). Any group name
  decodes; an unknown one lists nothing.
- Responses: `TopologyGraph` with `GraphNode` (`AgentNode`,
  `ChannelNode`), `WeightedEdge`, `EdgeStats`, `EdgeTotals`,
  `EdgeTransmissionPage`, `BipartiteGraph` (checked; on the wire its
  `BipartiteParts`), `WeightedAccess`, `ResourceUsePage` with
  `ResourceUse` (checked), `TopologySeries` (checked) with `SeriesGroups`,
  `Series` and `SeriesEdge`, `DetectionQuality` (checked), `AgentRow`,
  `AgentProfile` (checked), `AgentDetail` with `AgentCluster` (checked),
  `AgentName`, `AgentTraffic`, `DeadLetter`.
- Bus payloads, never requests: `Envelope`, `BusEvent`, `Changed`,
  `EdgeKey` (checked, in `EdgeUpdated`). `Subject` is a string. `BusEvent`
  and `DeadLetter` are in `wire/authority.rs`: a bus event carries the
  operators and times its node stamped, and a dead letter holds an
  envelope.

## Channel semantics in the topology shapes

- `TopologyFilter` gains `unconfirmed_channels`, an `UnconfirmedChannels`
  string: `"include"` (the default) or `"exclude"` ("confirmed only"). It
  narrows the channel-centred view's accesses and channel nodes and the
  overview's channel queues, and changes no transmission view
  (`topology.filter.unconfirmed-changes-no-transmission-view`). An unknown
  value is refused like any unknown variant. Goldens:
  `topology_filter_default` (`include`), `topology_filter_full`
  (`exclude`), and every golden that embeds a filter.
- `ChannelNode` carries `confirmation` (`"confirmed"` or `"unconfirmed"`):
  only channels listed as channels are drawn, and an unconfirmed one is
  drawn marked. Goldens: `graph_nodes`, `bipartite_graph`,
  `bipartite_graph_unread_write`.
- `AccessEdge` and `AccessContribution` are keyed by the access's
  `resource`, resolved to the channel holding it at read time; neither is
  on the wire, so only the drawn `WeightedAccess` (with its canonical
  channel) is.
- `TopologyFilter::admits` never admits a subject whose sender and reader
  are one agent (`topology.filter.cross-agent-only`); `FilterSubject` and
  `AccessSubject` (which gains the channel's `confirmation`) stay off the
  wire.

## Non-scope

No wire root reaches these, so they have no serde: `AccessEdge`, `Edge`,
`NodeId`, `FilterSubject`, `AccessSubject`, `VersionUnavailable`,
`PipelineFrontier`, `EdgeContribution`, `AccessContribution`, `EdgeError`,
`EdgeQueryError`, the `EdgeStore`, `FrontierSource`, `EventBus`,
`Subscription`, `DeadLetterStore` and `BlobStore` traits, `Delivery`,
`DeliveryId`, `RetryPolicy` (config), `BusError`, `BlobError`, and the
constructors' error enums.

## The bus framing

An envelope is `{"id", "at", "event"}`, and the event is tagged twice:
the layer, then the event.

```json
{"id": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "at": "2026-10-04T12:34:56.789012Z",
 "event": {"type": "insight", "data": {"type": "watermark_advanced", "data": "2026-10-04T12:30:00.000000Z"}}}
```

A layer event's inner tag is its `Subject`'s string (`watermark_advanced`
above), so a payload read off the bus names its subject; a change
notification's layer tag is `changed`, its subject, and its inner tag
names the entity (`{"type": "changed", "data": {"type": "channel", "data":
"01J9.."}}`) (`transport.wire.event-tag-is-subject`).

Every `BusEvent` variant is pinned twice. Each layer's area pins its
events with its own fixtures, one golden per variant inside a full
envelope: `observed/envelope_<variant>` (ingest),
`flow/event_<variant>` (detect), `insight/<variant>` (insight). The
goldens `bus/bus_events_{ingest,detect,insight,changed}` are the
exhaustive index: one event of every variant of every layer, built from
`tests::events`'s fixtures behind a match over every variant with no
wildcard, so a new event does not compile until it is listed, and the
test fails until its fixture is in the golden. `bus/changed_every_variant`
does the same for `Changed`, and `bus/subjects` lists every subject
string.

## Graph rules on decode

`BipartiteGraph`, `TopologySeries`, `ResourceUse`, `DetectionQuality`,
`SeriesGrid`, `SeriesStep`, `EdgeKey`, `EdgeSelector`, `AgentProfile` and
`AgentCluster` decode through their constructors, so each constructor
error is a decode error (one rejection test per error variant).
`TopologyGraph` is a checked type like `BipartiteGraph`: private fields
behind accessors, built only by `TopologyGraph::new` from its
`TopologyGraphParts`, which are its wire shape, so decoding runs the same
constructor (`topology.graph.checked-construction`). It checks every rule
`EdgeStore::graph` promises: no self-edge, no (from, to, route) twice,
each share its stat under the weighting over the total, and the node rules
(nodes cover endpoints and ancestors, no duplicates, no channel node,
counts agree). JSON of a graph the server could never build is a decode
error (`topology.wire.graph-decode-checked`). The share helpers are shared
with `BipartiteGraph::new` (one tolerance,
`TopologyGraph::SHARE_TOLERANCE`). The JSON is the same as when the
fields were public.

## Maps keyed by ids

Every map keyed by an id on the wire is a `BTreeMap`: ascending id is
ascending ULID text, so its JSON object has one key order and a golden of
several entries is stable. `EdgeStore::agent_traffic` returns a
`BTreeMap<AgentId, AgentTraffic>` (`topology.agent-traffic.ordered-keys`),
and `QueryApi::agent_names` and `channel_names` return
`BTreeMap<AgentId, AgentName>` and `BTreeMap<ChannelId, ChannelName>`
(`surface.query.name-maps-ordered`); `agents/agent_names_several` and
`surface_reads/channels/channel_names_several` pin several entries each.

## Files

| File | Role |
| --- | --- |
| `spec/types/aggregates/edge.rs` | `TopologyGraph` (checked; `TopologyGraphParts` on the wire, decoded through `new`); `EdgeKey`, `EdgeSelector` (a request), `Weighting` (a request) |
| `spec/types/interfaces/l7_topology.rs` | `EdgeStore::agent_traffic` as a `BTreeMap` |
| `spec/types/interfaces/l2_transport.rs` | `DeadLetter`, `ConsumerGroup` (a request) |
| `spec/types/tests/wire/topology/` | `mod.rs` (fixtures, JSON edit helpers), `filter.rs` (the linked views' requests), `graph.rs` (`TopologyGraph`, nodes, totals, `EdgeKey`, edge drill-down), `access.rs` (`BipartiteGraph`, `ResourceUse`), `series.rs`, `quality.rs` |
| `spec/types/tests/wire/agents.rs` | Agent rows, details, names, traffic, the agents filter |
| `spec/types/tests/wire/bus.rs` | `Envelope`, the exhaustive `BusEvent` and `Changed` index, `Subject`, `DeadLetter`, `ConsumerGroup` |
| `spec/types/tests/events.rs` | The event fixtures (`pub(crate)`) the bus index is built from |
| `spec/types/tests/golden/{topology,agents,bus}/` | 36, 9 and 12 goldens |

## Invariants

`topology.wire.graph-decode-checked`, `topology.agent-traffic.ordered-keys`,
`surface.query.name-maps-ordered` and `transport.wire.event-tag-is-subject`;
the channel-semantics ones above;
the general wire invariants take the area's goldens and rejections as
evidence.
