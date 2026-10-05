# Handoff: what counts as a channel

For the owner of the spec (`spec/types/`, `spec/invariants/`, working on
`docs/ui-surface`, PR #16 and later). Branch `feat/ui-channel-semantics`
(based on `feat/ui`) changes the spec, the fixture and the UI so that a
channel is only something two agents communicated through. Everything the
spec gained or lost is listed here so it can be merged cleanly; the spec
commit is `docs: create discovered channels only on cross-agent
transmissions` and touches nothing outside `spec/`.

## Why

The channels page listed `kv_put scratch/notes`, a key-value entry only
`cc7` ever wrote and read. The spec created a discovered channel on the
first access to any resource (`ChannelOrigin::Discovered` with
`TrafficDetection::Observed`), moved it to `Candidate` on a cross-agent
co-access and to `Active` on a confirmed transmission, and fired
`NewChannel` at creation. So a resource one agent used was a channel,
alerted on and drawn, and `s3://agent-scratch/handoff` (`Candidate`) was the
next step of the same problem. Transmissions themselves already required
two agents (`ContentMatch::new` refuses `SelfMatch`, `CoAccess::new`
refuses `SameAgent`), but read-time merges could still turn one into a
transmission within one agent that search, projections, exports and topic
sizes kept counting.

## The user's decisions

1. **A transmission only exists between different agents.** The
   construction checks stay, and read-time alias resolution never lets a
   transmission between two ids of one merged agent count anywhere (graphs,
   series, channel status, rows and counts, overview, search, exports,
   alerts).
2. **A discovered channel exists only once a transmission between
   different agents goes through the resource.** Before that a resource is
   only a resource: accesses are recorded, but it is in no channel list or
   graph and raises no `NewChannel`. `Observed` and `Candidate` leave the
   channel lifecycle.
3. **Suspected transmissions count, but are marked, reviewable and
   filterable.** A suspected transmission (cross-agent co-access, no
   content match yet) creates the channel; a channel whose transmissions
   are all suspected is marked unconfirmed (`Unconfirmed` while only
   suspected traffic, `Confirmed` once any confirmed transmission routes
   through it, a type that cannot contradict the traffic). Operators review
   it (its suspected transmissions with evidence and verdict controls; the
   review queue, marked) and filter it out. Default: included and marked,
   with a one-click "confirmed only" filter in the URL view state.
4. **Declared channels stay as declarations.** A channel declared in config
   with no cross-agent transmission is kept and listed separately ("declared,
   no traffic yet"); `SanctionedUnused` depends on it. It does not count as
   an active channel or appear in the graph until a cross-agent transmission
   goes through it.
5. **Merges hide, they don't delete.** If after alias resolution every
   transmission through a channel is between ids of one agent, the channel
   is hidden at read time (not in lists, graph or counts); unmerging brings
   it back; its policy history and record are kept.

## Spec changes

### Types

| Where | Change |
| --- | --- |
| `derived/flow/channel/detection.rs` | `TrafficDetection` is `Active { since, last_transmission }` and `Dormant { since, last_transmission }` only (`Observed`, `Candidate` removed); it advances on a cross-agent transmission opened or confirmed. New `TrafficDetection::last_transmission()`. `DetectionKind` loses `Observed`, `Candidate`. `DeclaredDetection` docs: awaiting/unused means no cross-agent transmission (not no access). |
| `derived/flow/channel/mod.rs` | `Seed { resource, first_access: AccessId }` becomes `Seed { resource, first_transmission: TransmissionId }`. Module docs: a channel exists once a cross-agent transmission goes through it; `pub mod confirmation`. |
| `derived/flow/channel/confirmation.rs` (new) | `Confirmation { Unconfirmed, Confirmed }`; `CrossTraffic { confirmed, unconfirmed }` with `CrossTraffic::tally(transmissions, aliases, writer)` (the reference: crossing transmissions split by confirmed state; `Detected` and within-one-agent ones excluded) and `confirmation()`; `Listing { Channel(Confirmation), Declaration, Hidden }` with `Listing::of(origin, traffic)` (`None` for superseded), `kind()`, `confirmation()`; `ListingKind { Confirmed, Unconfirmed, Declaration }`. Nothing about confirmation is stored: it is read from the traffic, so it cannot drift. |
| `derived/flow/transmission.rs` | `Crossing { Unknown, Crosses, WithinOneAgent }` and `Transmission::crossing(aliases, writer)`: confirmed states compare `Confirmed::from` and the reader; co-access states cross when any co-access writer resolves to another agent than the reader; `Detected` is `Unknown`. Module docs: "Only between different agents". |
| `aggregates/filter.rs` | `TopologyFilter::unconfirmed_channels: UnconfirmedChannels { Include (default), Exclude }` with `keeps(confirmation)`. `TopologyFilter::admits` never admits a subject with `from == to`. `AccessSubject` gains `confirmation: Confirmation`; `admits_access` keeps an unconfirmed channel's access only under `Include`. |
| `aggregates/access.rs` | `AccessEdge` is keyed by `resource: ResourceId` instead of `channel` (buckets exist before a channel does; resolved to the channel holding the resource at read time). |
| `aggregates/node.rs` | `ChannelNode::confirmation: Confirmation`; only channels listed as channels are nodes. |
| `aggregates/alert.rs` | `AlertSubject::shown(aliases, hidden, within_one_agent)`: alerts about a hidden channel or a within-one-agent transmission are not shown (read time). `NewChannel` and `SanctionedUnused` docs. |
| `aggregates/topic_history.rs` | `TopicSizes` docs: count only cross-agent transmissions (merges resolved at the read). |
| `interfaces/l8_surface/channels.rs` | `ChannelStanding::InForce(ChannelActivity)` becomes `InForce { traffic: CrossTraffic, activity: ChannelActivity }`. `ChannelRow::traffic()`, `listing()`, `confirmation()` (derived). `ChannelRow::new` refuses `InvalidChannelRow::TrafficWithoutDetection` (cross-agent traffic on a channel whose stored detection has none). |
| `interfaces/l8_surface/channel_traffic.rs` (new) | `ChannelTransmission::of(transmission, aliases, writer, verdict, topic) -> Option<Self>` (summary plus `senders: NonEmpty<AgentId>`, none for a non-crossing one), `confirmation()`; `ChannelTransmissionFilter { confirmation: Option<Confirmation> }` (`matches`); `ChannelTransmissionPage { channel, topic_version, page }`. |
| `interfaces/l8_surface/lists.rs` | `ChannelFilter::listings: Vec<ListingKind>`; `ChannelFilter::matches(&ChannelRow)` (was `&Channel`): never a hidden channel, listings apply to channels in force, a superseded channel is selected by `origin` alone. |
| `interfaces/l8_surface/overview.rs` | `QueueCounts::unconfirmed_channels: Option<u64>` (`None` under `Exclude`); `QueueCounts::tally(alerts, shown, rows: &ChannelRow, unconfirmed)`: open shown alerts, unreviewed listed channels (unconfirmed ones only under `Include`), unconfirmed channels. |
| `interfaces/l8_surface/export/rows.rs` | `InvalidTransmissionRow::WithinOneAgent(AgentId)`: `TransmissionRow::new` refuses equal ends. |
| `paging.rs` | `ChannelTransmissionList` marker (key `(opened_at, id)`). |

### Events

- `DetectEvent::AccessRecorded { access, channel: Option<ChannelId> }`
  (`None`: a resource on no channel).
- `DetectEvent::ChannelDiscovered { channel, seed: Seed }` (was
  `first_access: AccessId`): published once per discovered channel, after
  `ChannelRegistry::discover` commits; raises `NewChannel`. Since every
  channel transmission is opened by a co-access, a channel is never created
  by a resource only one agent touches.
- `DeclaredChannelUnused`: docs, no cross-agent transmission within the
  idle window.

### Traits

- `ChannelLookup::New` becomes `NoChannel` (record the access on the
  resource alone, create nothing); `Declared` now also covers a resource
  seen before but still on no channel.
- `ChannelRegistry::discover(resource, transmission, at) -> Discovery {
  Created(ChannelId) | Existing(ChannelId) }`: the one place a discovered
  channel is created (seed, `Active` since `at`, `Unreviewed`).
- `ChannelRegistry::channels_of(resources) -> HashMap<ResourceId,
  ChannelId>` (what L7 resolves access buckets through) and
  `ChannelRegistry::cross_traffic(channels) -> HashMap<ChannelId,
  CrossTraffic>` (what rows, nodes and queues read).
- `TransmissionUpdate::OpenChannel { on: OpensOn, .. }` (was `channel`):
  `OpensOn::Channel(id)` or `OpensOn::Resource(id)` (discover first).
- `Correlator::on_access(access, channel: Option<ChannelId>)`; shards are
  keyed by resource for resources on no channel and hand their evidence to
  the new channel's shard on discovery.
- `AccessContribution { resource }` (was `channel`); `EdgeStore` docs:
  buckets by resource, resolved at read time; `channel_topology` draws
  listed channels only, with their confirmation.
- `QueryApi::channel_transmissions(caller, channel, filter, version, page)
  -> ChannelTransmissionPage` (View). `channels` never lists a hidden
  channel; `channel` still returns it. `alerts` leaves out alerts that
  `AlertSubject::shown` hides. `overview` queues honour
  `filter.unconfirmed_channels`.
- `l5_flow` module docs: a "Discovery" section, and `Changed::Agent` is
  what readers re-query channels on after a merge (no `Changed::Channel` is
  published for a read-time listing change).

### Invariants

Validated with `inv_check.py spec/invariants` (395 files, 0 errors).

Retired (deleted; numbers not reused):

| Number | Id | Replaced by |
| --- | --- | --- |
| 240 | `flow.channel.discovered-seed` | 747 |
| 260 | `flow.registry.lookup-precedence` | 746 |
| 261 | `flow.registry.one-channel-per-resource` | 748 |
| 420 | `analysis.sizes.match-assignments` | 761 |
| 677 | `topology.bipartite.unread-writes-drawn` | 757 |
| 678 | `topology.filter.access-admission` | 756 |
| 688 | `surface.channels.default-excludes-superseded` | 754 |
| 701 | `surface.overview.counts-defined` | 760 |

Rewritten in place (rationale only, statement unchanged): 238
`flow.channel.declared-only-states`, 488
`flow.channel.promotion-keeps-detection`, 691
`surface.channels.row-standing-matches-origin`, 743
`surface.overview.active-channels-match-rows`.

Added (746 to 765):

| Number | Id | Kind |
| --- | --- | --- |
| 746 | `flow.registry.lookup-never-creates` | domain, model-based |
| 747 | `flow.channel.discovered-by-cross-agent-transmission` | domain, postcondition |
| 748 | `flow.registry.at-most-one-channel-per-resource` | coordination |
| 749 | `flow.channel.resource-only-until-cross-agent` | domain, invariant |
| 750 | `analysis.rule.new-channel-on-discovery` | domain, postcondition |
| 751 | `flow.correlator.resource-shard-handoff` | coordination |
| 752 | `flow.transmission.crossing-resolved` | domain, postcondition |
| 753 | `surface.channels.listing-from-traffic` | representation |
| 754 | `surface.channels.filter-listings` | domain, postcondition |
| 755 | `surface.channels.merge-hides-unmerge-restores` | domain, metamorphic |
| 756 | `topology.filter.access-admission-confirmation` | domain, postcondition |
| 757 | `topology.bipartite.listed-channels-only` | domain, postcondition |
| 758 | `topology.filter.cross-agent-only` | domain, invariant |
| 759 | `topology.filter.unconfirmed-changes-no-transmission-view` | domain, metamorphic |
| 760 | `surface.overview.queues-defined` | domain, model-based |
| 761 | `analysis.sizes.match-cross-agent-assignments` | domain, model-based |
| 762 | `surface.export.transmission-row-cross-agent` | representation |
| 763 | `surface.alerts.hidden-subjects-not-shown` | domain, postcondition |
| 764 | `surface.channels.transmissions-cross-agent` | domain, postcondition |
| 765 | `surface.query.channel-transmissions-need-view` | confidentiality |

Evidence marked `agent = "true"` points at spec tests in
`spec/types/tests/confirmation.rs` (new) and at the fixture's tests in
`ui/src/backend/fixture/tests/` (`channels.rs`, `graph.rs`); gateway paths
(`crosstalk::…`) are future evidence, marked `false`.

### Tests

`spec/types/tests/confirmation.rs` is new (17 tests). Existing tests were
adapted only where the types they exercise changed: `Seed` fields,
`TrafficDetection::Observed` fixtures became `Active`, `ChannelStanding`
became a struct variant, `ChannelFilter::matches` takes rows (built by the
new `tests::fixtures::channel_row`), `QueueCounts::tally` takes rows and
the unconfirmed choice, `AccessSubject` and `ChannelNode` gained their
confirmation. 589 tests pass; clippy and fmt are clean.

### README

`spec/types/README.md`: layout lines for the new modules and changed
types, a convention ("A channel exists once agents communicate through
it"), and two rows of the cascade mapping table: no channel before a
cross-agent transmission (`cascade.yaml` still creates one at first access
and has `observed` and `candidate` leaves), and declared channels leave
`AwaitingTraffic` on their first cross-agent transmission.

## Decisions beyond the five

- **When a discovered channel is created.** At the co-access that opens
  its first channel transmission (`AwaitingContent`), not when the window
  closes into `Suspected`: the route needs a channel from the moment the
  transmission opens, and a co-access between two agents is exactly the
  suspected evidence the user named. Unconfirmed therefore covers
  `AwaitingContent`, `Suspected` and `Discarded`; a discarded transmission
  still keeps its channel (its existence was decided when it opened; a
  channel disappearing on expiry would undo a `NewChannel` already sent).
- **Confirmation is read, not stored.** `TrafficDetection` keeps only when
  traffic flowed (`Active`, `Dormant`, now on any cross-agent transmission);
  `Confirmation` and `Listing` are computed from `CrossTraffic` at read time
  with merges resolved, which is what makes decision 5 and an unmerge work
  without rewriting anything, and the states impossible to contradict.
- **Access buckets by resource.** Required so the write of a channel's
  first co-access (made before the channel existed) counts on it; resolution
  at read time mirrors supersession and merges and never rewrites a settled
  bucket.
- **Promoted channels are declarations.** A promoted channel whose
  transmissions all merged within one agent is listed as a declaration
  (operator intent), not hidden; only discovered channels hide.
- **Verdicts do not change listing or confirmation.** A channel whose
  suspected transmissions an operator judged false detections stays
  unconfirmed and listed; views that exclude false detections exclude
  those transmissions, not the channel.
- **The "confirmed only" filter lives in `TopologyFilter`** (as
  `unconfirmed_channels`, beside `false_detections`) because the overview
  and the channel-centred graph already take that filter; it is a proven
  no-op for transmission views (INV-759). Channel lists express it through
  `ChannelFilter::listings`.
- **Queues honour it.** `unreviewed_channels` leaves unconfirmed channels
  out under `Exclude`, and `unconfirmed_channels` is `None` (not 0) then.
- **Alerts hide at read time** (`AlertSubject::shown`) rather than being
  suppressed by the merge, so an unmerge restores them exactly; `alert(id)`
  still returns a hidden alert by id.

## Open for the spec owner

- `DetectionQuality` and `verdict_rows` still count transmissions whose
  agents later merged into one: they measure the detector's calls as made.
  Decision 1 could be read to cover them too.
- A dropped topic version's frozen all-time sizes reflect merges up to the
  drop (the spec says so); the fixture applies current merges.
- How a non-seed resource of a discovered channel joins it is as
  unspecified as before (the fixture groups a channel's resources by
  plan); discovery seeds the channel with the resource of its first
  cross-agent transmission.
- `cascade.yaml` still models the old lifecycle (first-access discovery,
  `observed`, `candidate`); the README mapping table records the
  difference.

## Merging

Spec files touched (all in one commit): `README.md`, `aggregates/{access,
alert,filter,node,topic_history}.rs`, `derived/flow/channel/{mod,
detection,promotion,confirmation}.rs`, `derived/flow/transmission.rs`,
`events/detect.rs`, `interfaces/{l5_flow,l7_topology,l8_surface}.rs`,
`interfaces/l8_surface/{channels,channel_traffic,lists,overview,
export/rows}.rs`, `paging.rs`, `tests/{channel_reads,channels,
confirmation,events,filter,fixtures,flow,graph,mod,overview}.rs`, and the
invariant files above. Expect conflicts where PR #16 edits the same
docs or `QueryApi` methods; the new `QueryApi::channel_transmissions` sits
right after `channels`. Any implementor of `ChannelRegistry`, `Correlator`,
`EdgeStore` or `QueryApi` gains the methods and signatures listed under
Traits.
