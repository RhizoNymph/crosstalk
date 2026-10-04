# Channel semantics

What counts as a channel, and what counts as a transmission. A channel is a
medium two agents communicated through; a resource only one agent touches
is not one. This page gathers the rules every layer applies, which the spec
states across `derived/flow`, the L5, L7 and L8 interfaces and the read
models. They come from five decisions:

1. A transmission only exists between different agents. Construction
   enforces it (`ContentMatch::new` refuses a self match, `CoAccess::new`
   a write and read by one agent), and read-time alias resolution never
   lets a transmission between two ids of one merged agent count anywhere.
2. A discovered channel exists only once a transmission between different
   agents goes through its resource. Before that the resource is only a
   resource: its accesses are recorded, but it is in no list or graph and
   raises no `NewChannel`.
3. Suspected transmissions count. A channel whose cross-agent traffic is
   all unconfirmed is listed, counted and drawn, marked `Unconfirmed`, and
   can be filtered out ("confirmed only").
4. A channel declared in config with no cross-agent transmission stays a
   declaration: listed apart, never counted as an active channel or drawn.
5. Merges hide, they don't delete. A discovered channel whose
   transmissions all resolve within one agent after a merge is hidden at
   read time; an unmerge lists it again.

## Scope

- When a discovered channel is created, by whom, and what seeds it.
- Where resources live before and after a channel exists.
- A channel's detection (stored: when traffic flowed), its cross-agent
  traffic and confirmation (read: what evidence backs it), and its listing
  (read: channel, declaration or hidden).
- The one definition of "between different agents" every reader applies
  (`Transmission::crossing`) and every place it is applied: filters,
  graphs, channel rows and counts, the overview's queues, search,
  projections, transmissions by id, exports, detection quality, topic
  sizes, alerts.
- A channel's transmissions (`QueryApi::channel_transmissions`), the
  review list of an unconfirmed channel.
- The channel list's order (`ChannelRow::created_at`).

## Non-scope

- How the correlator pairs a write and a read, and its windows: `l5_flow`
  module docs and `CorrelationTiming`.
- Policy, promotion and supersession, which these rules compose with
  unchanged: [type_spec.md](type_spec.md).
- The wire shapes: [wire/flow.md](wire/flow.md),
  [wire/surface_reads.md](wire/surface_reads.md),
  [wire/surface_actions.md](wire/surface_actions.md),
  [wire/topology.md](wire/topology.md).
- `cascade.yaml` (`design/lifecycles/`, outside the repository) still
  models the old lifecycle (a channel at first access, `observed` and
  `candidate` leaves); `spec/types/README.md`'s mapping table records the
  difference. Updating that model is not part of this feature.

## Data and control flow

```text
tool call / result ─L5 extract─▶ locator
  ChannelRegistry::lookup(locator)            creates nothing
    Known(c)      resource on channel c (canonical)
    Declared(c)   a declared pattern matches (a resource on no channel included)
    NoChannel     on no channel, no pattern
  ChannelTraffic::add_resource(resource)      first sighting: on c, or on no channel
  ChannelTraffic::record_access(access)       on the resource, channel or not
  publish AccessRecorded { access, channel: Option<ChannelId> }    (consumer)
          │
          ▼ L7 EdgeStore::apply_access: bucket (agent, resource, op, bucket)
  correlator: write by A, later read by B on one resource (CoAccess, names A as writer)
    TransmissionUpdate::OpenChannel { on: OpensOn::Channel(c) | OpensOn::Resource(r), .. }
      OpensOn::Resource(r):
        ChannelTraffic::discover(minted id, r, transmission, opened_at)
          Created(id)   seed { r, transmission, opened_at }, Active since opened_at,
                        Unreviewed; registry publishes ChannelDiscovered + Changed::Channel
          Existing(c)   r already on c (a concurrent discovery won), or a declared
                        pattern claims it now (r joins c); nothing published for discovery
      TransmissionStore::save(transmission routed through the channel)
      ChannelTraffic::record_transmission(transmission)
        opened or confirmed: canonical channel's detection Active (since kept)
        Changed::Channel for the canonical channel
  L6 NewChannel ◀── ChannelDiscovered only
```

**Reads.** Nothing about confirmation or listing is stored. At every read:

```text
CrossTraffic = tally(transmissions routed through the channel and every channel it
                     superseded, crossing == Crosses under the read's aliases)
               confirmed:   Confirmed, Classified, Aggregated
               unconfirmed: AwaitingContent, Suspected, Discarded
Listing::of(origin, traffic):
  a confirmed crossing transmission ─▶ Channel(Confirmed)
  only unconfirmed ones             ─▶ Channel(Unconfirmed)
  none, declared (or promoted)      ─▶ Declaration
  none, discovered                  ─▶ Hidden
  superseded                        ─▶ none (its traffic is its superseding channel's)
```

`ChannelReads::channel` and `channels` return each channel with its
traffic (`ChannelWithTraffic`); the surface builds `ChannelRow`s
(`ChannelStanding::InForce { traffic, activity }`) whose `listing` and
`confirmation` derive from it. `ChannelFilter::matches` (rows) and `keeps`
(registry reads) never keep a hidden channel; `listings` selects channels,
unconfirmed channels and declarations ("confirmed only" leaves out
`Unconfirmed`). L7 reads the same listing through `NodeFacts`
(`ChannelFacts::listing`, `NodeFacts::channel_of` for an access bucket's
resource): the channel-centred graph draws accesses only to channels
listed as channels, with their confirmation, and
`TopologyFilter::unconfirmed_channels` (`UnconfirmedChannels::Exclude`)
leaves unconfirmed ones out of the graph, the overview's channel queues
and nothing else (transmission views count confirmed transmissions only,
whose channels are confirmed by them).

**Crossing.** `Transmission::crossing(aliases)`: `Unknown` for `Detected`;
for a confirmed state, whether `Confirmed::from` and the reader resolve to
different agents; for a co-access-only state, whether any co-access's
writer (`CoAccess::writer`) resolves to an agent other than the reader.
Applied by:

| Where | How |
| --- | --- |
| `TopologyFilter::admits` | never admits a subject whose sender and reader are one agent, so graphs, series, search, projection samples, the edge drill-down and exports agree |
| `CrossTraffic::tally` | counts crossing transmissions only, so a merge can hide a channel |
| `ChannelTransmission::of`, `ChannelReads::transmissions` | a channel's list holds crossing transmissions only, each naming its senders |
| `TransmissionSummary::listed` | `transmissions_by_id` leaves out a transmission within one agent |
| `TransmissionRow::new` | refuses `WithinOneAgent` |
| `ProjectedPoint::new`, `ProjectionFrame::new` | refuse a point whose sender is its reader |
| `DetectionQuality::tally`, `verdict_rows` | leave out a transmission within one agent |
| `TopicCatalog::sizes` | counts assignments whose sender and reader resolve apart (`StoredAssignment` carries both) |
| `AlertSubject::shown` | alerts about a hidden channel or a transmission within one agent are not listed or counted |

**The channel list's order.** `ChannelOrigin::created_at` (and
`ChannelRow::created_at`): a channel declared before traffic was created
when it was declared; any other channel when its first cross-agent
transmission opened (`Seed::opened_at`, kept by a promotion and a
supersession). `ChannelReads::channels` and `QueryApi::channels` list
newest first, ties by id descending.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/derived/flow/channel/confirmation.rs` | Confirmation, traffic and listing, read at query time | `Confirmation`, `CrossTraffic` (`tally`, `confirmation`), `Listing` (`of`, `kind`, `confirmation`), `ListingKind` |
| `spec/types/derived/flow/channel/mod.rs` | The seed and the creation time | `Seed` (`resource`, `first_transmission`, `opened_at`), `ChannelOrigin::created_at` |
| `spec/types/derived/flow/channel/detection.rs` | Detection: `Active`, `Dormant` only | `TrafficDetection` (`last_transmission`), `DetectionKind` |
| `spec/types/derived/flow/transmission.rs` | The crossing rule | `Crossing`, `Transmission::crossing` |
| `spec/types/derived/flow/evidence.rs` | A co-access names its writer | `CoAccess::writer` |
| `spec/types/interfaces/l5_flow.rs` | Lookups and discovery | `ChannelLookup::NoChannel`, `OpensOn`, `Discovery`, `TransmissionUpdate::OpenChannel { on }`, `Correlator::on_access(access, Option<ChannelId>)` |
| `spec/types/interfaces/l5_flow/channels.rs` | The registry's writes and reads | `ChannelTraffic` (`add_resource`, `record_access`, `discover`, `record_transmission`, `set_detection`), `ChannelReads` (`channel`, `channels`, `transmissions`), `ChannelWithTraffic`, `TrafficError` |
| `spec/types/aggregates/filter.rs` | The cross-agent rule and the confirmed-only switch | `TopologyFilter::admits`, `UnconfirmedChannels`, `AccessSubject::confirmation` |
| `spec/types/aggregates/access.rs`, `interfaces/l7_topology.rs` | Access buckets by resource; facts L7 reads | `AccessEdge::resource`, `AccessContribution::resource`, `NodeFacts::channel_of`, `ChannelFacts::listing` |
| `spec/types/aggregates/node.rs` | Channel nodes carry their confirmation | `ChannelNode::confirmation` |
| `spec/types/interfaces/l8_surface/channels.rs` | Rows | `ChannelStanding::InForce { traffic, activity }`, `ChannelRow` (`traffic`, `listing`, `confirmation`, `created_at`), `InvalidChannelRow::TrafficWithoutDetection` |
| `spec/types/interfaces/l8_surface/channel_traffic.rs` | A channel's transmissions | `ChannelTransmission` (`of`, `senders`), `ChannelTransmissionFilter`, `ChannelTransmissionPage` |
| `spec/types/interfaces/l8_surface/lists.rs` | The channel list filter | `ChannelFilter::listings`, `matches`, `keeps` |
| `spec/types/interfaces/l8_surface/overview.rs` | Queues | `QueueCounts::unconfirmed_channels`, `QueueCounts::tally` |
| `spec/types/aggregates/alert/mod.rs` | Hidden subjects | `AlertSubject::shown` |
| `spec/types/interfaces/l8_surface/summary.rs`, `export/rows.rs` | Rows by id and export rows | `TransmissionSummary::listed`, `InvalidTransmissionRow::WithinOneAgent`, `verdict_rows` (with aliases) |
| `spec/types/aggregates/quality.rs` | Detection quality | `DetectionQuality::tally` (with aliases) |
| `spec/types/aggregates/projection/mod.rs`, `frame.rs` | Points | `ProjectedPoint` (checked; `PointParts`, `PointWithinOneAgent`), `InvalidFrame::WithinOneAgent` |
| `spec/types/interfaces/l6_analysis/lifecycle.rs` | Topic sizes | `StoredAssignment::from`, `to` |
| `spec/types/interfaces/l8_surface.rs`, `http/routes.rs` | The query and its route | `QueryApi::channel_transmissions`, `Route::ChannelTransmissions` (`GET /channels/{id}/transmissions`) |
| `spec/types/tests/confirmation.rs` | The spec's tests of all of the above | — |
| `crates/memory/src/flow/registry/traffic.rs` | The reference registry's traffic writes and reads | — |
| `crates/memory/src/flow/registry/tests/traffic.rs` | Its reference tests | — |

## Invariants and constraints

- Lookups create nothing (`flow.registry.lookup-never-creates`); a
  discovered channel is created only by `discover`, for a resource on no
  channel, seeded by the cross-agent transmission that opened on it
  (`flow.channel.discovered-by-cross-agent-transmission`); at most one
  channel per resource, and one `ChannelDiscovered` per discovered channel
  (`flow.registry.at-most-one-channel-per-resource`); a resource only one
  agent uses stays a resource (`flow.channel.resource-only-until-cross-agent`);
  `NewChannel` only on discovery (`analysis.rule.new-channel-on-discovery`);
  a correlator shard hands a resource's evidence to the new channel's
  shard (`flow.correlator.resource-shard-handoff`).
- Resources join a channel only through a declared pattern; a discovered
  channel holds its seed (`flow.traffic.resources-placed-by-lookup`).
- Detection: declared channels go `InUse(Active)` on their first
  cross-agent transmission
  (`flow.channel.declared-detection-on-cross-agent-transmission`); traffic
  detection moves only between `Active` and `Dormant`
  (`flow.channel.traffic-detection-transitions`).
- Crossing is one definition (`flow.transmission.crossing-resolved`),
  applied by the filter (`topology.filter.cross-agent-only`), which the
  confirmed-only switch never changes for transmission views
  (`topology.filter.unconfirmed-changes-no-transmission-view`).
- Listing derives from origin and traffic
  (`surface.channels.listing-from-traffic`); the channel filter never
  lists a hidden channel (`surface.channels.filter-listings`); merges hide
  and unmerges restore (`surface.channels.merge-hides-unmerge-restores`);
  the graph draws listed channels only
  (`topology.bipartite.listed-channels-only`,
  `topology.filter.access-admission-confirmation`,
  `topology.node-facts.unknown-channel-defaults`).
- Queues count listed channels and shown alerts
  (`surface.overview.queues-defined`); hidden subjects' alerts are not
  shown (`surface.alerts.hidden-subjects-not-shown`).
- Nothing within one agent is counted or listed: topic sizes
  (`analysis.sizes.match-cross-agent-assignments`), export rows
  (`surface.export.transmission-row-cross-agent`), projections
  (`analysis.projection.point-cross-agent`), rows by id
  (`surface.query.transmissions-by-id-cross-agent`), detection quality and
  verdict rows (`flow.quality.cross-agent-only`), a channel's
  transmissions (`surface.channels.transmissions-cross-agent`, which needs
  View: `surface.query.channel-transmissions-need-view`).
- The channel list is newest created first, by a time derived from the
  stored channel (`flow.channel-reads.list-newest-created`,
  `surface.channels.rows-newest-created-first`).
- Verdicts change neither listing nor confirmation: a channel whose
  suspected transmissions were judged false detections stays listed and
  unconfirmed; views that exclude false detections leave out those
  transmissions, not the channel.
- A discovered channel keeps its existence when its first transmission is
  discarded: existence was decided when the transmission opened, and a
  `NewChannel` already sent cannot be undone.
