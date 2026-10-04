# Read models

The rows and pages the UI shows for agents, channels and transmissions,
the names it shows beside them, the evidence behind a transmission, the
overview's counts and one alert by id. Each is a `QueryApi` read in L8
built from the stores below it; the contract they share (callers and
permissions, pagination, linked views, errors, watermarks, the live feed)
is in [query_surface.md](query_surface.md).

## Scope

- Agents: canonical agent rows with windowed traffic, one agent's detail
  following merges, the agents list filter, and refusing a merge of a
  cluster into itself.
- Channels: list rows (seed resource, then activity counted in an optional
  window or the supersession), the channel list filter, and the promotion
  preview computed by the same plan as the promotion.
- Names: batch lookups of agent and channel names over one bounded id
  batch type.
- Transmissions: rows by id with a per-state shape, and the evidence
  behind one transmission with excerpts cut from stored message bodies.
- The overview's counts in one watermarked query, and how they agree with
  the channel and agent rows.
- One alert by id.

## Non-scope

- What the stores hold and how they compute it (identity, detection,
  aggregation): [type_spec.md](type_spec.md).
- The linked views, projections, verdicts, the audit log and the live
  feed: [query_surface.md](query_surface.md).
- Bulk export of transmissions and other datasets, which reuses the
  transmission row and the evidence's quotes: [export.md](export.md).
- The JSON these rows and pages travel as, and which arguments are client
  requests (`IdBatch`, the list filters): [wire_contract.md](wire_contract.md).

## Data and control flow

Every read here takes a `Caller`, checks its permission before reading
anything (View for all of them except `transmission_evidence`, which needs
Content), returns `Result<_, QueryError>`, and resolves merged agents and
superseded channels at read time through `AgentDirectory` and
`ChannelDirectory`. The list reads page with `crate::paging`; the reads
whose data comes from settled buckets are `Watermarked`.

| Read | Permission | Returns | Watermark |
| --- | --- | --- | --- |
| `agents(filter, window, page)` | View | `Watermarked<Page<AgentRow, AgentList>>` | from `EdgeStore::agent_traffic` |
| `agent(id, window)` | View | `Option<Watermarked<AgentDetail>>` | from `EdgeStore::agent_traffic` |
| `agent_names(ids)` | View | `BTreeMap<AgentId, AgentName>` | none (labels change only with `Changed::Agent`) |
| `channels(filter, page)` | View | `Watermarked<Page<ChannelRow, ChannelList>>` | `EdgeStore::watermark`, read first |
| `channel(id, window)` | View | `Option<Watermarked<ChannelRow>>` | `EdgeStore::watermark`, read first |
| `channel_names(ids)` | View | `BTreeMap<ChannelId, ChannelName>` | none |
| `promotion_preview(channel, pattern)` | View | `PromotionPreview` | none (all time, changes nothing) |
| `transmissions_by_id(selection, version, page)` | View | `TransmissionPage` | none (current state, not buckets) |
| `transmission_evidence(id, window)` | Content | `Option<TransmissionEvidence>` | none |
| `overview(window, filter)` | View | `Watermarked<OverviewCounts>` | `EdgeStore::watermark`, read first |
| `alert(id)` | View | `Option<Alert>` | none |

### Agents

The agents list, an agent's page and the names shown beside every row
(`aggregates/agents/`). L3 supplies identity through `AgentReads`
(`l3_reconstruction/agents.rs`), one snapshot per call; L7 supplies traffic
through `EdgeStore::agent_traffic`; the surface joins them.

- **Rows are canonical agents.** `agents` returns a page of `AgentRow {
  profile, traffic }`, newest agent first (`AgentList`, keyed by
  `AgentId`). An `AgentProfile` (checked) holds the id, the canonical
  agent's label, its `ActiveAgentState`, its canonical parent (never itself
  or an alias), its aliases (every agent resolving to it, ascending), the
  `ClaimSet::union` of its and its aliases' harness claims with their
  last-seen times, and `last_seen`, the latest exchange start of any of
  them (`None` only for a registered agent never seen). A merged agent is
  never a row, whatever the filter: its claims, activity and traffic
  already count toward its canonical agent, so a row of its own would count
  them twice and disagree with the graph. `AgentFilter::states` lists
  `CanonicalStateKind`s, so it cannot ask for one.
- **Traffic is windowed, not lifetime.** `AgentTraffic { transmissions_in,
  transmissions_out }` for each row is `EdgeStore::agent_traffic(window,
  ids)`: the agent's node counts in `graph(window, Transmissions,
  TopologyFilter::default())` (merges resolved, self-edges dropped, every
  route and topic, false detections included), zero without a node. So a
  row agrees with the graph the operator came from, its counts are final
  before the watermark like every aggregate, and the cost is bounded by
  the window. The window restricts counts, never rows; an unaligned window
  is `InvalidInput(UnalignedWindow)`. The page's watermark is the one
  `agent_traffic` read before its buckets; labels, claims and last-seen
  times come from L3 and are not settled by it. Activity is not announced
  on the live feed (it changes with every exchange); clients refresh rows
  on each `Watermark` event, as for every `Watermarked` query.
- **The filter.** `AgentFilter { states, claimed, text, parents }`, defined
  in `aggregates/agents/filter.rs` because L3 applies it, and re-exported
  from `l8_surface/lists.rs`. Empty lists and no text do not restrict;
  fields combine with AND:

  | Field | Keeps a canonical agent when |
  | --- | --- |
  | `states` | its `CanonicalStateKind` is listed |
  | `claimed` | a claim in its unioned `ClaimSet`, seen at any time, has a listed `HarnessFamily` (claims, never identity evidence) |
  | `text` (`AgentText`: trimmed, 1 to 64 characters, no controls) | the text is a substring of its label, both lowercased with Unicode's default mapping (`str::to_lowercase`, not full case folding: `ß` ≠ `ss`), or a prefix, ignoring ASCII case, of the ULID text (`ulid_text`, Crockford base32) of the agent or of one of its aliases; Crockford's `I`/`L`/`O` aliases are not applied |
  | `parents` | its canonical parent equals the canonical form of a listed agent: one level of the sub-agent tree |

  Labels match anywhere because they are words; ids match only from their
  start because they are random after the time prefix, so a substring of
  one would match nearly anything. An alias's id finds the agent it was
  merged into; a merged agent's own label is shown nowhere, so it is not
  matched.
- **Detail.** `agent` returns `AgentDetail { cluster, traffic }`, `None`
  for an unknown id. The `AgentCluster` (checked) is the agent `id`
  resolves to: its profile; its `Agent` record (evidence included,
  agreeing with the profile); its aliases' records (each `Merged` into it,
  keeping the `prior` state an unmerge restores); its children (canonical
  agents whose canonical parent it is, ascending; the paged form is
  `agents` with `parents: [id]`); every `MergeRecord` naming it or an alias
  as source, target or repointed agent, oldest first, reverted ones with
  their `Reversal`; every `MergeVeto` with an end in the cluster; and the
  `AgentLookup`: `Canonical`, or `Redirected { from: id }` when `id` is
  merged. The detail is a whole value, like a policy history.
- **Merging a cluster into itself.** `OperatorAction::merge_agents(caller,
  from, into)` builds the action; one id twice is `SelfMerge`, which never
  becomes an action, so it never reaches `act` or the audit log, and the
  surface returns `InvalidInput(SelfMerge)`. Two different ids that the
  merge table resolves to one agent (one merged into the other, or both
  into a third) are refused by L3 with `MergeIntoSelf`, checked before
  `AgentMerged` (`MergeRequest::conflict`): naming the canonical agent
  instead would still merge it into itself. `act` returns
  `Conflict(MergeIntoSelf { from, into, canonical })` and audits it as
  rejected, naming the two agents asked for. The first needs no state, so
  it is an input error; the second needs the merge table, so it is a
  conflict.

### Channels

`l8_surface/channels.rs` holds what the channel list, the channel page and
the promotion page read.

**Rows.** `channels` returns a page of `ChannelRow`s, newest channel first,
and `channel(id, window)` one row as the head of the channel page (a
superseded id answers with its own record and supersession, which the page
shows as a banner to the channel in force). `ChannelRow::new` (checked)
holds:

| Part | Content |
| --- | --- |
| `channel()` | the stored `Channel` under its own id |
| `seed()` | its seed `Resource`, present exactly when the channel has a seed |
| `standing()` | `InForce(ChannelActivity)` when the channel is in force, `Superseded(SupersededInto { into, by, at })` exactly when it is superseded, with its own supersession |

`ChannelActivity` is `Never` (no access and no transmission ever; refused
for a channel whose detection shows traffic) or `Seen { last, counts }`.
`last` is the latest `Access::at` of its resources or `Confirmed::at` of a
transmission routed through it, over all time. `ChannelCounts { writers,
readers, transmissions }` is counted in the filter's window (all time when
`None`; a window not on bucket boundaries is
`InvalidInput(UnalignedWindow)`):

- `writers` and `readers` are `ChannelCounts::tally` of a full
  `channel_resources` traversal of the same channel and window (distinct
  canonical agents);
- `transmissions` is the channel's entry of `ChannelCounts::routed` of the
  topology graph for the window under `TopologyFilter::default()`: the
  transmissions the graph counts on edges routed through the channel
  (active topic version, false detections included, merged self-edges
  dropped), as an agent row's traffic is its node's counts in that graph.

A channel in force counts itself and every channel it superseded. A
superseded row shows **no counts and no last activity**. Accesses to a
superseded channel's resources and transmissions routed through it resolve
to its superseding channel and are counted on that row, so frozen counts on
the superseded row would count them twice whenever a list is summed, and
would look like live activity on a channel that takes none. Its
`SupersededInto` names where they are: `into` (the channel in force), `by`
(the operator, taken by `SupersededInto::of` from the superseding channel's
promotion declaration) and `at`. A late confirmation of a transmission
routed through a superseded channel likewise advances the superseding
channel's detection, never the superseded one's
([query_surface.md](query_surface.md#promotion-and-supersession)).

**Filter.** `ChannelFilter { origin, detections, policies, window }`
(`l8_surface/lists.rs`):

| Field | Keeps a channel when |
| --- | --- |
| `origin: OriginFilter` | `InForce(kinds)` (the default): it is in force and its `CanonicalOriginKind` is listed, or `kinds` is empty. `WithSuperseded(kinds)`: the same, or it is superseded. `Superseded`: it is superseded |
| `detections` | its own `detection_kind()` is listed (a superseded channel's is frozen) |
| `policies` | its own current policy kind is listed (a superseded channel takes no decisions) |
| `window` | always: it changes the counts on each row, never which rows are listed or a row's `last` |

The superseded choice is one `OriginFilter` value rather than a list of
origins plus a flag, so "superseded only, but exclude superseded" cannot
be asked. The page cursor binds the whole filter, window included. The
watermark is read from L7 before the registry and the buckets, and the UI
re-queries rows on `Watermark` as well as `ChannelChanged`.

**Promotion preview.** `promotion_preview(channel, pattern)` shows what
`PromoteChannel { channel, pattern, .. }` would do if sent now:

1. The surface builds the `Declaration` the action would record (the
   caller's operator, the time it accepted this request, the pattern) and
   calls `ChannelRegistry::promotion_coverage(channel, &declaration)`.
2. The registry runs `promotion::coverage` over the channels `promote`
   would plan over, in one snapshot, changing nothing. `coverage` is
   `promotion::plan` (the same function `promote` runs, taking the
   declaration, since it never reads the policy) plus a
   `PromotionCoverage`: the plan's superseded channels, complete, and every
   resource the channel and those channels hold (seed and stored
   resources, all time), each counted once, split into `covered` (the
   pattern matches its locator) and `uncovered`. Each side is a
   `CappedResources` (`Capped<Resource, COVERAGE_CAP>`, 200): its newest
   resources (highest id first), at most 200, and the exact `total()` of
   that side, so the UI can show "and `hidden()` more". `Capped::new`
   refuses more than its cap or a total below what it shows, so a capped
   list cannot pass for a complete one (`is_complete()`). Uncovered
   resources stay with the channel they are stored on, which resolves to
   the promoted one; new resources outside the pattern will not join.
3. `PromotionPreview::from_registry` maps the result through the same
   `ActionError::from` as the action's: a `Conflict` (`ChannelSuperseded`,
   `ChannelNotDiscovered`, `PatternOverlaps`) is an answer, a preview whose
   `conflict()` is that kind and which has no resource samples (`None`, not
   empty samples, which would read as a pattern matching nothing) and no
   superseded channels; `NotFound` (unknown channel),
   `InvalidInput(PatternMissesSeed)` and `Store` are the query's error. A
   pattern that misses the seed is wrong whatever the state, so it stays an
   input error, as for the action.

The preview's accessors are the UI's fields: `covered_resources()` and
`uncovered_resources()` (`Option<&CappedResources>`),
`superseded_channels()` (a complete `SupersededChannels`, exactly what
`ActionOutcome::ChannelPromoted` would report) and `conflict()`. It needs
View, not Govern: it changes nothing and shows only structure View already
shows (ids, locators, declared patterns), so a reviewer without Govern can
prepare a promotion for an operator who has it; the action needs Govern.

### Names

Rows name agents and channels by id; the UI labels a whole page at once
with two batch lookups that take the same checked type, an `IdBatch<T>`
(`batch.rs`): distinct ids, ascending, at most `IdBatch::MAX` (1,000).
`IdBatch::new` drops repeats before counting and refuses more with
`TooManyIds { max, got }`, which the surface returns as
`InvalidInput(TooManyIds)` before calling the lookup.

One cap serves both lookups: twice the largest page (`PageSize::MAX`). A
row names at most two agents (a sender and a reader) and one channel (its
route or subject), so any page's names fit in one call of each lookup; the
rare page that names more (an audit page of promotions, each naming every
channel it superseded) splits its lookup. One cap and one counting rule
(distinct ids) mean a client sizes every batch the same way and
`TooManyIds` from a name lookup always means the same bound.

- `agent_names(ids: &IdBatch<AgentId>)` returns `AgentName { id, label }`
  keyed by the id asked for, in a `BTreeMap`, so the JSON object's keys
  are in ascending id order and one answer has one encoding
  (`surface.query.name-maps-ordered`): the canonical agent and its current label, so
  an alias is named by the agent it was merged into.
- `channel_names(ids: &IdBatch<ChannelId>)` returns `ChannelName { id,
  shape }` keyed by the id asked for, ordered the same way: `id` is the channel in force
  (`ChannelDirectory::canonical`) and `shape` is `ChannelShape::Pattern`
  (declared, before traffic or promoted) or `Seed(Locator)` (discovered).
  `channels::resolve_names` is the reference.

Unknown ids are left out of both maps, not errors, so one stale id never
blanks a page.

### Transmission rows

`transmissions_by_id(selection, version, page)` serves the explore page's
lasso and search selections and the evidence page's header
(`l8_surface/summary.rs`):

1. A `TransmissionSelection` (checked) holds 1 to 100,000 distinct ids
   (`ProjectionLimit::MAX`, so a lasso over a whole projection fits),
   sorted newest first; repeats count once. A request the surface cannot
   build one from is refused before anything is read: no ids is
   `InvalidInput(EmptySelection)`, too many is the same
   `InvalidInput(TooManyIds)` a name lookup reports, with the selection's
   bound (`QueryError::from(InvalidSelection)`).
2. The first page resolves `version` (a `TopicVersionSelector`) as a linked
   view does, with the catalog's retention deciding what is retained;
   errors as for a linked view. The cursor (`TransmissionList`, keyed by
   `TransmissionId`, newest first) binds the selection and pins the
   resolved version, which every `TransmissionPage` reports.
3. Each id of a stored transmission gives one `TransmissionSummary::of`
   row; ids of no stored transmission are left out. No window and no
   filter: the selection came from a view that applied them, and
   re-filtering could drop rows the selection shows.

A `TransmissionSummary { id, to, route, opened_at, state }` names the
canonical reader and the route with its channel resolved. Its
`SummaryState` carries per state exactly what that state knows:

| State | `Delivery` (canonical sender, `Confirmed::at`, matched bytes) | `TopicUnder` | Verdict |
| --- | --- | --- | --- |
| `Detected`, `AwaitingContent` | — | — | — (not judgeable) |
| `Suspected`, `Discarded` | — | — | current |
| `Confirmed` | yes | — (not classified yet) | current |
| `Classified`, `Aggregated` | yes | `Topic`, `Outlier` or `Unassigned` under the page's version | current |

`TransmissionSummary::of` is the definition: it fills each field its
state holds, calling the verdict lookup (`VerdictLog::current`) and the
topic lookup only for states that hold them. The summary is the one
transmission row: an export's transmission row is a summary plus the
strongest match class ([export.md](export.md)). The drill-down behind an
edge keeps its own rows (`EdgeTransmission`): they are what the edge
counted, from L7's stored contributions, so their matched bytes sum to the
edge's, while a summary reads the transmission now, which a later content
match can still extend; and L7 holds no state, opened time or verdict to
build a summary from.

### Evidence and excerpts

`transmission_evidence(id, window)` (Content) returns the text behind one
transmission, `None` for an unknown id. Verdicts stay in `verdicts`
(View).

1. **Assembly** (`l8_surface/evidence.rs`). The surface reads the
   transmission, and `TransmissionEvidence::assemble` lists from it: one
   `MatchEvidence { content_match, origin, read }` per content match of its
   `Confirmed`, in stored order (none before confirmation), and one
   `AccessDetail { access, resource, agent }` per distinct access its
   co-access records name (`TransmissionState::co_accesses`), in order of
   first mention, write before read. The surface supplies each match's two
   excerpts (`MatchQuotes`) and each access's record and resource; a detail
   for another access, or a resource the access did not touch, is refused,
   so the evidence always belongs to its transmission. Records keep their
   stored ids; `AccessDetail::agent` is the access's canonical agent, and
   the canonical sender, reader and route are the transmission's row.
2. **Text** (`observed/message/text.rs`). A `SpanLocation` is a part and a
   byte range into `Message::part_text` of that part: a text part's text,
   visible reasoning, a tool call's argument text, a tool result's text
   contents joined with `"\n"` (`TOOL_RESULT_SEPARATOR`); media, opaque
   reasoning and unknown blocks have none. `origin` is cut around the origin
   span's location, `read` around the match's `read_at`, as the text
   arrived (before decoding).
3. **Excerpt** (`l8_surface/excerpt.rs`). `Excerpted::of(location, body,
   window)` takes what `BlobStore::get` returned for the location's
   message, decoded, and cuts `Excerpt::cut(part_text, range, window)`:
   - `ExcerptWindow` is the context per side, 0 to 2,048 bytes (`DEFAULT`
     256, `MATCH_ONLY` 0, which an export uses). A request over 2,048 is
     refused before anything is read as
     `InvalidInput(ExcerptContextTooLong)`. Each window edge moves inward to
     the nearest character boundary, so an excerpt never shows more than
     the window and never splits a character.
   - The matched range is highlighted whole up to 8,192 bytes
     (`Excerpt::MAX_HIGHLIGHT`); a longer one is cut at the last character
     boundary within that, its remaining bytes counted in `highlight_cut`,
     and no context follows it.
   - `elided_before` and `elided_after` count the part's bytes not shown,
     so `elided_before + text + highlight_cut + elided_after` is the part's
     length.
   - `Excerpt::new` (checked) holds the shape: a non-empty highlight inside
     the text on character boundaries, bounded context and highlight, no
     text after a cut highlight.
4. **Retention.** L1 stores every body before publishing
   `ExchangeCaptured`, so a body the blob store no longer returns was
   dropped by content retention: that side is
   `Excerpted::BodyDropped { message }` and the rest of the evidence is
   still returned. A location that does not fit its stored body (another
   message, no such part, a part with no text, a range outside the text or
   off a character boundary) is an `ExcerptError`, never a panic.
5. **Failure.** Anything that keeps the evidence from being read is an
   `EvidenceError` (a store failure, a `BlobError`, a missing span, access
   or resource, an `ExcerptError`, records that do not belong together),
   which maps to `Store` with a reason naming the cause.

Matches are not paged: a transmission is one reader exchange's matches
from one sender, and each excerpt is at most 12 KiB.

### Overview counts

`overview(window, filter)` (`l8_surface/overview.rs`) returns the
overview's counts without paging any list:

- **Activity** (`EdgeTotals`), scoped by the window and the filter: what
  `topology` counts for them, read from the buckets (`EdgeStore::totals`,
  defined as `EdgeTotals::of` the graph): transmissions counted into the
  graph's edges, their matched bytes, and **active channels**, the
  distinct canonical channels a `Route::Channel` edge names, that is, the
  channels that carried at least one counted transmission. Also the
  resolved topic version. Fails as `topology` does (unaligned window, the
  version's errors, `TopicsNotInVersion`).
- **Queues** (`QueueCounts`), not scoped: **open alerts** are the alerts
  whose `AlertState` is `Open` (not acknowledged, resolved or suppressed),
  as `alerts` with `states: [Open]` lists them; **unreviewed channels**
  are the channels not superseded whose current `Policy` is `Unreviewed`
  (never reviewed or reset), the review queue. A superseded channel is
  reviewed through its superseding channel. `QueueCounts::tally` is the
  definition. A backlog does not depend on a window: an alert raised last
  week still waits.

The watermark is read before anything else and governs the activity; the
queues are as of the read, with no settling point.

**Agreement with the rows.** The overview, the channel rows and the agent
rows all count transmissions from the one topology graph, so they do not
contradict each other:

| Count | Graph | Filter |
| --- | --- | --- |
| overview `active_channels` | `EdgeTotals::of(graph)`: channels some edge routes through | the request's |
| channel row `transmissions` | `ChannelCounts::routed(graph)[channel]`: the sum over those edges | `TopologyFilter::default()` |
| agent row `transmissions_in`, `transmissions_out` | the agent's node counts | `TopologyFilter::default()` |

The keys of `ChannelCounts::routed` are exactly the channels
`EdgeTotals::of` counts as active, so with the default filter and the same
window `active_channels` equals the number of rows of
`channels(ChannelFilter::default())` whose `transmissions` is non-zero.
"Active" means exactly that. It is not a row's `ChannelActivity::Seen`,
which is wider: any access or confirmation ever, so a channel written to
and never read is `Seen` but not active. Under a non-default filter the
overview is narrower than the rows by design: the rows have no topology
filter.

### One alert

`alert(id)` returns the `Alert` that `alerts` lists under `id` (its
subject as raised; matching against channels and agents resolves it with
`AlertSubject::resolved`), or `None`. Alert pages and audit links resolve
through it. Alert ids are never aliased. On the wire it is the alert's
JSON or `null` (`tests/golden/alerts/alert_found.json`,
`alert_unknown.json`); the alert inbox is the wire contract's reference
area ([wire_contract.md](wire_contract.md)).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/batch.rs` | Bounded id batches for name lookups | `IdBatch` (checked: distinct, ascending, at most `MAX` = 1,000), `TooManyIds` |
| `spec/types/aggregates/agents/mod.rs` | Agent read models | `AgentProfile` (checked), `AgentProfileParts`, `InvalidProfile`, `AgentTraffic`, `AgentRow`, `AgentCluster` (checked), `AgentClusterParts`, `InvalidCluster`, `AgentLookup`, `AgentDetail`, `AgentName` (`of`); `CanonicalStateKind: From<ActiveAgentState>` |
| `spec/types/aggregates/agents/filter.rs` | The agents list filter | `AgentFilter` (`matches`, `text_matches`), `AgentText` |
| `spec/types/interfaces/l3_reconstruction/agents.rs` | L3's agent reads | `AgentReads` (`list`, `cluster`, `names`), `ActivityStore` (`record`, `last_seen`), `AgentReadError` |
| `spec/types/aggregates/edge.rs` (part) | What a graph counts in total | `EdgeTotals` (`of`), read by `EdgeStore::totals` |
| `spec/types/derived/flow/channel/promotion.rs` (part) | What a promotion would cover | `coverage`, `PromotionCoverage` (built only by `coverage`), `COVERAGE_CAP`, `CappedResources` |
| `spec/types/support.rs` (part) | A capped list with its exact total | `Capped` (checked: `new`, `first`, `hidden`, `is_complete`), `InvalidCapped` |
| `spec/types/observed/message/text.rs` | The text a span location indexes | `Message::part_text`, `Message::part_count`, `NoPartText`, `TOOL_RESULT_SEPARATOR` |
| `spec/types/interfaces/l8_surface/channels.rs` | Channel read models | `ChannelRow` (checked), `InvalidChannelRow`, `ChannelStanding`, `ChannelActivity`, `ChannelCounts` (`tally`, `routed`), `SupersededInto` (checked: `of`), `InvalidSupersededInto`, `ChannelName` (checked: `of`), `ChannelShape`, `InvalidChannelName`, `resolve_names`, `PromotionPreview` (`from_registry`, `conflict`, `covered_resources`, `uncovered_resources`, `superseded_channels`), `NotAPromotionConflict`. Wire: responses only; `ChannelRow` decodes through `new`, a `PromotionPreview` refuses a conflict no promotion is refused with, `SupersededInto` and `ChannelName` decode field by field ([wire/surface_reads.md](wire/surface_reads.md)) |
| `spec/types/interfaces/l8_surface/lists.rs` (part) | The channel list filter | `ChannelFilter` (origin, detections, policies, counts-only window), `OriginFilter`; re-exports `AgentFilter` and `AgentText` |
| `spec/types/interfaces/l8_surface/summary.rs` | Transmission rows | `TransmissionSummary` (`of`), `SummaryState`, `Delivery`, `TopicUnder`, `TransmissionStateKind`, `TransmissionSelection` (checked), `InvalidSelection`, `TransmissionPage`. Wire: `TransmissionSelection` is a `WireRequest` (an array of ids, decoded through `new`); the rest are responses |
| `spec/types/interfaces/l8_surface/evidence.rs` | The evidence behind a transmission | `TransmissionEvidence` (`assemble`), `MatchEvidence`, `MatchQuotes`, `AccessDetail` (checked), `InvalidEvidence`, `InvalidTransmissionEvidence`, `EvidenceError`, `EvidenceRecord`. Wire: responses; `TransmissionEvidence` decodes through `assemble`, `AccessDetail` through `new`; the error types are not wire data |
| `spec/types/interfaces/l8_surface/excerpt.rs` | Excerpts cut from stored bodies | `ExcerptWindow` (checked; `DEFAULT`, `MATCH_ONLY`), `InvalidWindow`, `Excerpt` (checked; `cut`), `InvalidExcerpt`, `CutError`, `Excerpted` (`of`, `BodyDropped`), `ExcerptError`. Wire: `ExcerptWindow` is a `WireRequest` (`{"context": 256}`); `Excerpt` decodes through `new` |
| `spec/types/interfaces/l8_surface/overview.rs` | The overview's counts | `OverviewCounts`, `QueueCounts` (`tally`) |
| `spec/types/tests/` | `agent_reads.rs` (profiles and clusters, the agents filter, id text, id batches, merging a cluster into itself, agent error mappings); `channel_reads.rs` (channel rows and counts, the channel filter, names, the preview's agreement with promotion); `summary.rs`, `evidence.rs`, `excerpt.rs`, `part_text.rs` (transmission rows, evidence, excerpts, part text, export content from evidence); `overview.rs` (totals, queues, and their agreement with channel rows); `wire/surface_reads/` (the goldens and decode refusals of channel rows, names, previews, transmission rows, selections, evidence and excerpts) | — |

## Invariants and constraints

- Agent rows are canonical agents only, whatever the filter; their counts
  equal their node counts in `topology` for the same window under the
  default filter. `AgentProfile` and `AgentCluster` are checked (no self or
  alias parent, distinct aliases merged into the agent, a redirect only
  from an alias, children outside the cluster, merge records and vetoes
  about the cluster, once each). `AgentFilter::matches` is the list's
  definition. `agent(id)` answers for `canonical(id)` and says when it
  redirected.
- A merge of two ids of one cluster is `Conflict(MergeIntoSelf)`, refused by
  L3 before `AgentMerged` and vetoes; one id twice is
  `InvalidInput(SelfMerge)` and never reaches `act`.
- A `ChannelRow` carries exactly its channel's seed resource, is superseded
  exactly when its channel is (with its own supersession and the promoting
  operator), and then carries no counts or last activity; a channel whose
  detection shows traffic is never shown as never active
  (`ChannelRow::new`, `SupersededInto::of`). A row in force counts writers
  and readers as `ChannelCounts::tally` of a full `channel_resources`
  traversal of the same channel and window, over itself and every channel
  it superseded, and transmissions as `ChannelCounts::routed` of the
  default-filter graph for that window.
- `ChannelFilter::matches` never reads the window, so filters differing
  only in window list the same channels; the default lists every channel
  in force and no superseded one.
- With the default filter and the same window, the overview's
  `active_channels` equals the number of channel rows with non-zero
  `transmissions`; both count from the one topology graph.
- In one registry state, `promotion_preview` and `PromoteChannel` agree:
  both run `promotion::plan` over the same stored channels, the preview's
  `superseded_channels()` equals the action's `ChannelPromoted` superseded
  channels, its `conflict()` is the action's `Conflict`, and its error is
  the action's `NotFound` or `InvalidInput`. Coverage partitions every
  resource the channel and the channels it would supersede hold by the
  pattern, each once; each side shows at most `COVERAGE_CAP` (200) of its
  newest resources with its exact total (`Capped`), while superseded
  channels are always complete. The preview changes nothing. A refused
  preview's conflict is `ChannelSuperseded`, `ChannelNotDiscovered` or
  `PatternOverlaps`, the only ones a promotion is refused with.
- `agent_names` and `channel_names` each take an `IdBatch` of at most
  1,000 distinct ids; they key each known id asked for to the name of what
  it resolves to (never a merged agent or a superseded channel) and leave
  unknown ids out.
- A `TransmissionSummary`'s shape follows its state: a sender, matched
  bytes and confirmation time from `Confirmed` on, a topic from
  `Classified` on, a verdict only in judgeable states. A selection holds 1
  to 100,000 distinct ids; a traversal of `transmissions_by_id` lists one
  row per stored id, none for others, under one resolved version.
- A `TransmissionEvidence` lists exactly its transmission's content matches
  and the distinct accesses its co-access records name, each with its own
  resource; decoded evidence is reassembled from its transmission, so it
  holds the same. An `Excerpt` has a non-empty highlight inside its text on
  character boundaries, at most 2,048 bytes of context per side and 8,192
  highlighted, and accounts for every byte of the part, with counts that
  place the matched range within `u32::MAX` bytes of the part's start and
  sum without overflow. A body retention
  dropped is `BodyDropped`, never an error; a location that does not fit
  its body is an `ExcerptError`, never a panic.
- A selection or an excerpt window the surface cannot build is
  `InvalidInput` (`EmptySelection`, `TooManyIds`, `ExcerptContextTooLong`)
  before anything is read.
- The overview's activity equals `EdgeTotals::of` the topology graph for
  the same window and filter; its queues are the `Open` alerts and the
  unsuperseded `Unreviewed` channels, unscoped. `alert(id)` equals the
  alert `alerts` lists under that id.
- Every read here needs View, reading nothing without it, except
  `transmission_evidence`, which needs Content.
- `agents` and `agent` carry the watermark `EdgeStore::agent_traffic` read
  before their counts' buckets; `channels`, `channel` and `overview` carry
  the one `EdgeStore::watermark` returned before any of their data was
  read.
