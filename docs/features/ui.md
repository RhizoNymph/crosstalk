# User interface

The operator-facing web UI: topology, transmission evidence, content
exploration, topics, channels, agents, alerts, and the research and
pipeline tools around them. It is designed top-down from the operator's
jobs and meets the gateway at the L8 surface (`QueryApi`,
`OperatorActions`). Where L8 does not yet offer what a screen needs, the
UI codes against the shapes in [The L8 contract](#the-l8-contract) and a
fixture backend until the gateway provides them.

## Scope

- Every screen, the data each one reads and the actions it can take.
- How view state lives in URLs so a view can be cited and reproduced.
- The split between server-rendered pages (Topcoat) and the client-side
  WebGL elements (topology graph, UMAP projection, time brush), and the
  contract between them.
- A deterministic fixture backend for development, tests and demos.
- The changes to L8 and the layers below it that the UI needs.

## Non-scope

- The L8 HTTP service and every layer below it.
- Authentication beyond trusted mode (one configured operator holding
  every permission). Organisation-wide auth and roles come later and must
  not need new data paths.
- Native or terminal clients.

## Users and permissions

The first users are a trusted research team. The design keeps every call
scoped by `Caller` so it can later serve many operators across an
organisation's coding and production agents.

| Job | Question | Permissions |
| --- | --- | --- |
| Investigate | What are my agents saying to each other, through what, about what? | View, Content |
| Triage | Something fired. Is it real, and what do I do? | View, Triage |
| Govern | Which channels are allowed? Which agents are the same agent? | View, Govern |
| Operate | Is the pipeline healthy? Replay what failed. | Operate |

Without `Content`, a screen renders its structure (agents, edges, counts,
states) and replaces message text, snippets, topic labels and terms with a
"content hidden" marker. Every screen must render in that mode.

## Architecture

```text
browser ─────────────────────────────────────────────────────────────┐
│ Topcoat runtime (signals, shards)     custom elements (WebGL)       │
│   pages, drawers, tables, forms  ◀──▶  <ct-topology> <ct-projection>│
│                                        <ct-timebrush>               │
└───────────────┬──────────────────────────────┬──────────────────────┘
                │ HTML, shard re-renders        │ JSON / binary
┌───────────────▼──────────────────────────────▼──────────────────────┐
│ crosstalk-ui (Topcoat 0.9 app)                                       │
│   pages/  shards/  data/ (#[route] endpoints for elements)           │
│   view_state (typed URL query)                                       │
│   Backend trait  ── FixtureBackend (seeded synthetic data)           │
│                  └─ L8 client (when the gateway exists)              │
└──────────────────────────────────────────────────────────────────────┘
```

- **Topcoat renders everything except the WebGL views.** Pages are async
  components that read from the `Backend` with the request's `Caller` and
  check permissions before touching data. Shards re-render server-side
  regions (drawers, result lists) when signals change.
- **Custom elements draw the graph, the projection and the time brush.**
  They are TypeScript (`ui/elements/`), bundled with esbuild and served as
  Topcoat assets. They fetch their data from the UI's own `#[route]`
  endpoints under `/data/`, never from L8 directly, so authorization stays
  in one place.
- **The element contract is the form-input contract.** An element's output
  is its string `value` property, announced with a `change` event; a page
  reads it with `@change=$(|e: Event| sig.set(e.target.value))`. Inputs
  arrive as `data-*` attributes bound to signals
  (`:data-highlight=$(sig.get())`) and observed with
  `attributeChangedCallback`. Binding and reading use only Topcoat's typed
  expression vocabulary. The one `raw!` use is keeping the URL current:
  after setting the signal, the handler rewrites one query key with
  `history.replaceState` (or, for the time brush, navigates with
  `location.assign`). Values reach `raw!` as runtime wrappers, so the
  JavaScript converts them with `String(value)` first.
- **Shards take the view state as an argument.** A shard's endpoint does
  not see the page URL, so a page passes `state.to_query()` and the shard
  re-parses it strictly (`pages::view::state_from_query`) together with
  its other arguments, then checks permissions itself.
- **The Backend trait mirrors L8.** Its methods are `QueryApi` and
  `OperatorActions` plus the additions in [The L8 contract](#the-l8-contract).
  Types that exist in `crosstalk-spec` are used directly; the additions are
  declared in `ui/src/contract/` with the same names and shapes, and are
  deleted when the gateway's types land.

## View state and URLs

Everything that changes what data a view shows lives in the URL query:
time window (absolute bounds; "last 24 h" is resolved when chosen),
weighting, filter (agents, channels, route kinds, topics, verdicts), topic
model version, graph mode (agents or bipartite), projection id, and the
selected edge, node, point set or transmission. Hover, open menus and
scroll position do not.

- One typed `ViewState` struct parses and renders the query, so every page
  reads the same keys and links between views carry the filter along.
- A missing window defaults to the 24 hours before the backend's present
  (*`now`*) and a missing topic version to the active version of the
  history `topic_versions` returns (`pages::common::topics::default_version`);
  both are `Backend` calls, so pages never ask the fixture directly.
- Filter controls are a `GET` form, so changing them navigates and the URL
  is always current.
- Selections made inside an element update a signal and the URL
  (`history.replaceState`); a page load starts the signal from the URL.
- Page keys (validated per page, never colliding with the shared
  `from,to,v,w,g,a,c,r,t,x`): topology `sel` (graph selection) and
  `collapse=1`; explore `q`, `m` (`text|semantic|hybrid`), `p` (projection
  id), `cb` (colour by `topic|sender|reader|route|channel`), `ps`
  (projection selection) and `cursor`; topics `ver`; the rule form accepts
  `topic` to preselect a watched topic. The topology filter form submits
  `apply=1` with repeated `fa`/`fc`/`fr`/`ft` and `fx`, and the page
  redirects (303) to the canonical comma-list keys.
- A view whose topic version is no longer retained (retention dropped
  it) shows the typed `VersionNotRetained` error rather than silently
  using another version.
- Aggregate views show the response's watermark ("final up to 14:05"), so
  a cited view says whether its numbers can still change.

## Screens

Each screen lists the calls it makes. Names in *italics* are additions in
[The L8 contract](#the-l8-contract).

### Overview (`/`)

Counts from one `overview` call: the window's activity under the view's
filter (confirmed transmissions and matched bytes, and active channels:
the channels that carried a counted transmission) and the queues as of
the read (open alerts, unreviewed channels in force), each linking to its
section; the five heaviest edges of the view's `topology`, each opening
the topology with that edge selected; the five newest open alerts; and a
link into every section carrying the view state. The header shows the
overview's watermark.

- Calls: `overview`, `topology`, `alerts`, *`agent_names`*,
  *`channel_names`*, *`rules`*, *`operators`*.

### Topology (`/topology`)

The main screen. Nodes are canonical agents sized by volume; sub-agents
collapse into their parent (`collapse=1`). Edge width is the edge's share
of the filtered total; edge colour is the route kind. A mode toggle (`g`)
switches to the bipartite view, where channels are nodes between their
writers and readers, so a hijacked shared resource is visible before
anyone reads it; a weighting toggle (`w`) switches shares between
transmissions and matched bytes. The header shows the window, node, edge
and transmission counts and the watermark.

The filter is a row of dropdowns (agents in the window's unfiltered graph,
every channel, route kinds, topics of the view's version with `Content`,
verdicts) with the active values as removable chips. The time brush shows
the last week (or the view's window if wider), snapped outward to bucket
boundaries, in hourly buckets; every bucket edge is a bucket boundary, so
brushing navigates to the same view with an aligned, canonical window.

Selecting in the graph sets the `sel` signal: the graph highlights it, the
URL's `sel` follows, and the drawer shard re-renders. An edge shows its
route, transmissions, matched bytes and share, a link filtering to its two
agents, and the transmissions counted into it (`edge_transmissions`,
newest confirmation first: confirmation time and matched bytes, the
sender, reader and route being the edge's), twelve per page with a cursor
signal (rows link to the evidence page). An agent or channel shows
a summary card (state, claims, counts; origin, detection, policy) with a
link to its page and its heaviest edges; with nothing selected, the
heaviest edges of the view. Listed edges select themselves on click.

- Calls: `topology` (agents mode; header, drawer and filter choices) or
  `channel_topology` (bipartite), both with their nodes; `series` (twice,
  `Total`) for the time brush;
  `edge_transmissions`, `agent`, `channel`,
  *`channels`* and `topics` (filter choices), *`agent_names`*,
  *`channel_names`*.

### Transmission evidence (`/transmissions/{id}`)

Why the gateway believes this transmission happened. The header is the
transmission's row (`TransmissionSummary`, one `transmissions_by_id` call
under the view's topic version `v`): sender → reader (with a "show edge"
link to the topology), the route in words, the state in a callout styled
by strength (content evidence; access pattern only, dashed amber;
discarded, muted; still gathering), told with its data when the caller has
`Content`, opened and confirmed times, matched bytes, the current verdict
and the topic under `v` (an outlier, not classified yet, or classified
only under later versions). Each match (`transmission_evidence` with the
default 256-byte window) shows its kind (decode chain spelled out, e.g.
"decoded base64 → url"; semantic with its score), carrier and size, then
the sender's originated text and the reader's input side by side with the
matched part highlighted and elided bytes noted (and the part of a very
long match cut); a side whose message body content retention dropped
(`Excerpted::BodyDropped`) says "Body dropped" instead of text. The
co-access timeline shows each write → read with the accesses' canonical
agents, times, resource and lag. The verdict log (`verdicts`, newest
first, the one in force marked) and a form: genuine, false detection or
withdraw, with a note.

Without `Content` the header still renders (state in words without its
data, topic labels hidden), the matches and the co-access timeline say
they need `Content`, and the verdict log is shown: verdicts hold no
message content and need only `View`.

- Calls: `transmissions_by_id` (one id), `transmission_evidence`,
  `verdicts`, `topics`, *`agent_names`*, *`channel_names`*,
  *`operators`*.
- Actions: *`SetVerdict`* (`Triage` and `Content`; offered only on
  judgeable states, a conflict otherwise).

### Explore (`/explore`)

Search and the UMAP projection, linked. The search form (`q`, `m`) runs
the spec's `search` over the view's window and filter (hits on
transmissions confirmed in the window that the filter admits, in rank
order) and lists hits twenty per page with score, route, sender → reader,
time and snippet, each linking to its evidence; the hits' rows come from
one `transmissions_by_id` call under the version the search resolved, and
the page's hits are the projection's `data-highlight`. Without a projection (`p`) the page offers "Fit
projection" (neighbours 15, min distance 0.1, seed 42, sample 5,000,
editable, checked as the spec's `ProjectionParams`: 2 to 200 neighbours,
a minimum distance from 0 to 1 in thousandths, a sample of 1 to 100,000),
which posts `action=fit`, records a job for the view's window and filter
(`fit_projection`, its version pinned) and redirects with `p`. The panel
follows the job's `ProjectionInfo`: queued or fitting (since when),
failed (why, e.g. too few points or a version dropped while queued),
expired (its points dropped after the frame retention; the re-fit form is
filled with its parameters and seed) or ready, showing its model,
version, parameters, points of matching transmissions and fit time; a
projection fitted for another window or filter is flagged with a re-fit
form, and an unknown id says so. Colour-by is a
select bound to the element (and `cb`). A point or lasso sets the `ps`
signal; the results shard shows the point's transmission, or resolves the
lasso against the stored projection (even-odd point-in-polygon in `f64`
over the stored `f32` coordinates with the element's bounding-box test, so
both sides select the same points) and lists those transmissions'
rows (`transmissions_by_id` under the projection's own topic version),
paged. A topic sidebar lists the view
version's topics by size in the window (`topic_sizes`: every assignment
under the version, whatever the view's filter) with a trend sparkline
(`series` grouped by topic, pinned to the version and otherwise
unfiltered) and a "Watch" link to the rule form with that topic
preselected. Everything here reads content, so the page needs `Content`
and says so otherwise.

- Calls: `search`, `transmissions_by_id`, `fit_projection`,
  `projection_status`, `projection(id)`, `topic_sizes`, `series`,
  `topics`.

### Topics (`/topics`)

A version picker (`ver`, default the view's `v`; one tab per version of
the history, pinned, newest and dropped marked, with topic count and fit
time) and a link making the shown version the view's. The topics table
gives label, top terms with weights, transmissions in the window
(`topic_sizes`, with its watermark), a trend sparkline (`series` grouped
by topic, pinned to the version; a version never activated shows none)
and, for the active version (the one the rule form picks topics from), a
"Watch" link. For a version with a successor, the remap table lists each
entry of the lineage (`topic_lineage`): its topic in the next version
where `TopicLineage::remap` carries it at the rule form's default
threshold (0.80), with the similarity, unmapped topics first and
highlighted, with the watched-topic rules the remap leaves stale (each
remapped with its own threshold, or already stale over the topic)
linked. A version retention dropped keeps its topics and lineage, but its
table shows the typed error (409); an unknown version is 404. Needs
`Content`.

- Calls: `topic_versions`, `topics`, `topic_sizes`, `series`,
  `topic_lineage`, *`rules`*.

### Channels (`/channels`, `/channels/{id}`, `/channels/{id}/promote`)

A table of `ChannelRow`s, newest channel first: origin, detection state,
policy, locator or pattern summary, writers, readers and transmissions
counted in the view's window (`ChannelCounts`: the resources' tally and
the default-filter graph's routed count, so the overview's active channels
are the rows with traffic), and last activity over all time. A superseded
row shows no counts of its own (they are on the channel in force).
Filtered by toggle chips (`origin`, `detection`, `policy`, `superseded`
query keys, comma lists like the shared filter) and paged by cursor.
`origin` and `superseded` together are the spec's `OriginFilter`: origin
codes `declared` (before traffic), `promoted` and `discovered`;
`superseded=1` adds superseded channels and `superseded=only` lists only
them (with no origin codes). The "Review queue" tab (`tab=review`) fixes
the policy to unreviewed and hides superseded channels. The detail page
(counts in the view's window) shows origin and detection in words (with a
link to the latest transmission), a banner naming and linking the channel
in force that superseded it, the policy with its decision (author, time,
note) and the set-policy form, a page of the resources accessed in the
view's window with their writers and readers (cursor paged; a superseded
channel's are its channel in force's), the channel's alerts, and its
policy history (`policy_history`: every config and operator decision,
newest first). Traffic over time is not shown yet.

Promotion is its own page. Candidate patterns are derived from the seed
locator, most specific first (exact; URL path prefixes by whole segments,
then host; file path prefixes; MCP server), so every candidate covers the
seed. Picking one (`?pattern=<n>`) asks the backend for a
`promotion_preview` over every resource the channels involved hold (not
the view's window): the newest of the resources the pattern covers and of
those it does not, with exact totals, the other discovered channels it
would supersede (named in one `channel_names` call per `IdBatch`), or the
conflict promotion would hit (shown through `error::describe`). Then a
confirm form with policy and note. Promotion keeps the channel's id, so
the confirm redirects to the same channel page.

- Calls: `channels`, `channel`, `channel_resources`, `policy_history`,
  `alerts` (filtered by channel), *`operators`*, *`rules`* (rule names),
  *`agent_names`* (writer and reader names, one call), `promotion_preview`,
  `channel_names`.
- Actions: `SetPolicy`, *`PromoteChannel`* (`Govern`; hidden on superseded
  channels, promotion only for discovered ones).

### Agents (`/agents`, `/agents/{id}`, `/agents/{id}/merge`)

A paged table, newest agent first (name, state, harness claims, parent
named, transmissions in and out in the view's window, counted as the
agent's topology node under the default filter, last seen or "never")
filtered by toggle chips for state and claimed harness (`state`, `claims`
query keys, comma lists, mapped onto `AgentFilter::{states, claimed}`; the
backend filters), and a detail page (traffic in the view's window):
identity evidence (each variant with
its scope and strength, most specific first, hashes abbreviated to their
key version and first four bytes), harness claims (shown as "claims",
never as identity), the sub-agent tree (one `agents` call per level
filtered by parent, up to four levels and sixty agents), aliases (each
with the state it had before its merge, `MergedInto::prior`), merge
history with revert state (who reverted it and the agents the revert
pointed back), what an unmerge would restore (the source's prior state
and the repointed agents that would point at it again) and an unmerge
button per merge in force, and merge vetoes. A URL naming an alias shows
the canonical agent with a banner (`AgentLookup::Redirected`); actions
always target the canonical agent. Rename validates `AgentLabel` and shows its error inline; clearing
the label is a separate button.

Merge is its own page: `/agents/{id}/merge` offers a target (pick from the
agent list or paste an id), `?into=<id>` shows both agents' evidence side
by side with shared evidence highlighted, warns when they share nothing or
carry different harness ids in the same scope, and confirms a
`MergeRequest` authored by the operator (the first agent becomes an alias
of the second).

- Calls: `agents` (with the spec's `AgentFilter`, over the view's
  window), `agent` (over the view's window), `agent_names` (one call per
  `IdBatch`), *`operators`*.
- Actions: `MergeAgents`, `Unmerge`, `RenameAgent` (`Govern`), still the
  contract's `OperatorAction`. Both agents of a merge must be canonical
  (a merged one is `Conflict(AgentMerged)`; the comparison resolves a
  pasted alias to its canonical agent before confirming), two ids of one
  cluster are `Conflict(MergeIntoSelf)`, one id twice
  `InvalidInput(SelfMerge)`; an unmerge reverts one record
  (`Conflict(MergeAlreadyReverted)` the second time); renaming a merged
  agent is `Conflict(AgentMerged)`.

### Alerts (`/alerts`, `/alerts/{id}`, `/alerts/rules`, `/alerts/rules/new`, `/alerts/rules/{id}`)

An inbox with one tab per state (`tab=open|acknowledged|resolved|
suppressed`), showing rule name, subject link (channel, agent or
transmission page), occurrences, raise time, who moved the alert to its
state (and the resolution note or suppression reason: channel sanctioned,
rule disabled, or rejected as a false detection). Open alerts can be
acknowledged; open and acknowledged ones resolved with a note (`Triage`).
When the shared filter names exactly one channel, the inbox is narrowed to
it. Each alert id links to the alert's page (`/alerts/{id}`): rule (its
edit page, or the rules list for a built-in), subject link, raise time,
occurrences, state with who and when, the acknowledge and resolve forms,
and its history from the audit log (subject `al.<id>`). The rules page
lists built-in rules (enable or disable), operator rules (what they match,
status, stale reason, author, sinks; edit, enable, disable, or "Update"
when stale) and sinks with their last delivery. Watched-topic rules pick
topics of the current topic version (labels need `Content`). Semantic
query rules are text (`QueryText`): the form submits a `UserRuleSpec` and
the backend embeds it, so editing the text re-embeds it.

- Calls: `alerts`, *`alert`*, *`rules`*, *`sinks`*,
  `topic_versions`, `topics`, *`operators`*, *`audit`* (an
  alert's history).
- Actions: `Acknowledge`, `Resolve`, *`CreateRule`*, *`UpdateRule`*,
  *`SetRuleEnabled`*.

### Research and pipeline

- **Export** (`/export`): pick dataset (a projection by id), topic
  version and format over the view's window and filter; including content
  needs `Content` (the checkbox is disabled without it). The backend has no
  *`export`* yet: a valid post answers 501 with the exact `ExportRequest`
  that would be sent, an invalid one 422 with the input kept. Below it,
  `detection_quality` for the window (the spec's `DetectionQuality`: every
  judgeable transmission opened in it) as route × detector call (confirmed
  by its strongest match class, suspected, discarded) with genuine, false
  detection, unlabelled and precision (genuine over labelled, for
  confirmed rows) and a totals row (precision over the confirmed rows).
- **Audit** (`/audit`): operator and config actions with actor, a one-line
  description, note, the entry's own subject (linked to its page; merges
  have none and link to the log filtered to them) and outcome (applied,
  with a link to what the action created, or rejected with the typed
  error), paged. Filters: `op` (operator), `subject`
  (`ch.`/`ag.`/`tx.`/`ru.`/`al.`/`mg.` plus the id; every row has a
  "filter" link; the backend also matches merges an agent took part in
  and what an entry created) and `span=all` (otherwise the shared window).
  Calls *`audit`*, *`operators`*.
- **Pipeline** (`/pipeline`): dead letters (consumer group, event id, kind
  and time, attempts, last error) with replay. Needs `Operate` to view.
  Calls *`dead_letters`*; action `ReplayDeadLetter` (`Operate`).

## Data and control flow

1. A request reaches a page. Trusted mode builds the `Caller` for the
   configured operator. The page parses `ViewState` from the query, with
   defaults read from the backend (*`now`*, `topic_versions`).
2. The page calls the `Backend` with the `Caller` and `ViewState`, and
   renders. Content fields are rendered only if the caller has `Content`.
3. Element tags are rendered with their inputs as `data-*` attributes
   (including the `/data/` URL for their payload, which carries the same
   `ViewState` query).
4. The element fetches its payload from `/data/...`; the route rebuilds
   the `Caller` and `ViewState` and calls the `Backend`.
5. User interaction in an element sets its `value` and fires `change`; a
   page signal takes the value; shards that read the signal re-render on
   the server; the URL is updated.
6. Operator actions are HTML form posts to the page's own path (with the
   view state in the action URL). The handler validates every field into
   typed values (`pages/common/form.rs`), checks the action's permissions
   and calls `Backend::act`. Success redirects (303) back to the page with
   a `flash` code (never text, so a link cannot make a page say something
   else); failure renders the same page with the typed `QueryError` next
   to the form that failed, the submitted input kept, under a status that
   follows the error (422 invalid input, 403, 404, 409 conflicts).
7. Live updates: an SSE stream of `UiEvent`s (ids only) tells open pages
   which regions to re-query.

## Element payloads

Each element reads one route under `/data/`. The Rust types in
`ui/src/data/` define the payloads (each documented in its doc comment);
`ui/elements/src/payloads/` mirrors them with zod schemas and a binary
decoder. `ui/src/data/fixtures/` writes the payloads of hand-built
contract values to `ui/elements/test/fixtures/` and fails when they drift
(`CT_UPDATE_FIXTURES=1 cargo test element_fixtures` regenerates them), so
the TypeScript tests parse exactly what Rust emits.

| Route | Payload | Needs | Backend call |
| --- | --- | --- | --- |
| `GET /data/topology?<view state>` | JSON `TopologyPayload` | `View` | `topology` (`g=agents`) or `channel_topology` plus one `channel_names` (`g=channels`) |
| `GET /data/timeline?<view state>&buckets=<n>` | JSON `TimelinePayload` | `View` | `series` twice (transmissions, matched bytes; `Total`) on one grid, `n` in 1..=1000, default 96 |
| `GET /data/projection/{id}` | binary, `application/octet-stream` | `Content` | `projection`; `transmissions_by_id` for the channels of channel-routed points (the spec's frame has none); `agent_names`, `channel_names` (each one call, chunked at `IdBatch::MAX`) and `topics` for names |

- **No redirects.** A view-state route needs every canonical key (`from`,
  `to`, `v`, `w`, `g`); a missing or invalid one is a 400 naming it.
  Backend errors: Forbidden 403, NotFound 404 (an unknown topic version
  too), VersionNotRetained, InvalidInput and a linked view's version
  conflicts (`TopicVersionFitting`, `TopicVersionNotActivated`,
  `TopicsNotInVersion`) 400 (with the message), anything else 500.
- **Topology** (JSON, camelCase): `mode` (`agents` | `channels`), `window`
  `{from, to}`, `weighting` (`tx` | `bytes`), `topicVersion`, `watermark`,
  `nodes`, `edges`. Nodes are tagged by `kind`:
  `agent {id, name, state, parent, volume, transmissionsIn,
  transmissionsOut, claims[{harness, version, userAgent, lastSeen}]}` and
  (channels mode) `channel {id, name, origin, detection, policy, volume}`.
  Read from the spec's `TopologyGraph` / `BipartiteGraph`: `name` is
  `components::agent_node_name` (as `agent_name`: label, else the id's
  tail) or the channel's pattern / seed locator from one `channel_names`
  call (not the spec's `locator_summary`, which adds a resource count);
  `origin` is `declared` for `DeclaredBeforeTraffic` and `Promoted`,
  `discovered` otherwise; `volume` is transmissions in + out, or accesses.
  A channel node exists for every channel an access touches or a
  transmission is routed through, so an unused channel is not drawn.
  Edges: `transmission {from, to, route, routeKind, share, transmissions,
  matchedBytes}` (`route` is the `url::route` code) and (channels mode)
  `access {agent, channel, op: read | write, accesses, share}`; access
  shares are normalised separately. Channels-mode transmission edges are
  the spec graph's edges not routed through a channel (those are drawn as
  accesses), each keeping its share of the whole filtered total, as in
  agents mode, so they sum to less than 1.
- **Timeline** (JSON): `window`, `bucketMs`, `watermark`,
  `buckets[{from, to, transmissions, matchedBytes, final}]`; `final` is
  `to <= watermark` (the earlier of the two series' watermarks). The grid
  (`data::timeline::timeline_grid`): the step is the smallest multiple of
  the bucket width at least `(to - from) / n`; the grid starts at `from`
  and runs a whole number of steps, ending at the first step boundary at
  or after `to` (so the last bucket may run past `to` by less than a
  step). At most `n` buckets; `bucketMs` is the step; every edge is a
  bucket boundary.
- **Projection** (binary, little-endian, columns 4-byte aligned; `n`
  points, header of `h` bytes):

  | Bytes | Content |
  | --- | --- |
  | 4 | magic `CTPJ` |
  | 4 | `u32` version (1) |
  | 4 | `u32` `h`, a multiple of 4 |
  | `h` | header JSON, space-padded: `id`, `count`, `window`, `topicVersion`, `fittedAt`, `embeddingModel {name, dimension}`, `params {neighbors, minDist, seed (string), sampleLimit}`, `routeKinds` (`["channel","delegation","direct","unobserved"]`), `agents[{id, name}]`, `channels[{id, name}]`, `topics[{id, label}]` (each table sorted by id; `label` null when hidden or the version is unknown) |
  | `4n` each | `f32` xs, `f32` ys, `u32` sender, `u32` reader (into `agents`) |
  | `n` + pad to 4 | `u8` route kind (into `routeKinds`) |
  | `4n` each | `u32` channel (into `channels`), `u32` topic (into `topics`); `0xFFFFFFFF` = none |
  | `16n` | transmission ids, 128-bit big-endian |

  The total is exactly `12 + h + 41n + pad(n)`; decoders reject anything
  else, and any index outside its table. Built from the spec's
  `Projection` (`data::projection::format::PayloadPoints`): the frame's
  coordinates, ids and sender, reader, route-kind and topic columns,
  re-indexed into one agents table for senders and readers. The frame has
  no channel column: a channel-routed point's channel is its
  transmission's route as `transmissions_by_id` reports it now (resolved
  through supersession at read time, not frozen at fit time), `NONE` if
  the transmission is gone. A queued, fitting or failed projection is a
  400 with the reason, an expired one 404.

Element inputs and outputs (`value`, announced with `change`):

| Element | Inputs | `value` |
| --- | --- | --- |
| `<ct-topology>` | `data-src`, `data-highlight` (a topology value), `data-collapse` (`"true"`) | `edge:<fromUlid>:<toUlid>:<routeCode>` \| `agent:<ulid>` \| `channel:<ulid>` \| `` |
| `<ct-projection>` | `data-src`, `data-color-by` (`topic` \| `sender` \| `reader` \| `route` \| `channel`), `data-highlight` (comma-separated transmission ULIDs) | `lasso:<x>,<y>;<x>,<y>;…` \| `point:<ulid>` \| `` |
| `<ct-timebrush>` | `data-src`, `data-from`, `data-to` (RFC 3339) | `<fromRfc3339>/<toRfc3339>` |

- The edge route code is everything after the third colon (tool names may
  contain colons). Clicking empty space clears (`""`). Clicking an access
  edge selects its channel. With `data-collapse`, an edge that merges
  several payload edges selects the heaviest of them.
- A lasso has 3 to 48 vertices in projection (data) coordinates, rounded
  to 4 decimals (trailing zeros dropped); longer lassos are simplified
  (Ramer–Douglas–Peucker) first. The element selects exactly the points
  inside that rounded polygon (even-odd rule), which is what the server
  must resolve.
- The brush snaps to bucket edges and emits on pointer-up; the times are
  the payload's own bucket edge strings. A click selects one bucket.
- Every element validates its payload: a failure shows an error panel,
  never a blank canvas. Empty payloads and HTTP errors show their own
  panels. Changing `data-src` aborts the in-flight request; detaching an
  element releases its WebGL context.
- Colours: route kinds from `--color-route-*`, channel policies from
  `--color-policy-{unreviewed,sanctioned,unsanctioned}` (with built-in
  fallbacks), categories from the eight-slot reference palette (the eight
  most frequent categories keep a slot in table order; the rest are
  "other"). Text and surface colours follow the page, light or dark.
- Layout is deterministic: node ids hash to initial positions, nodes and
  edges are added in sorted order and ForceAtlas2 runs a fixed number of
  iterations for the node count.

## Files

| Path | Role |
| --- | --- |
| `ui/Cargo.toml` | The `crosstalk-ui` package. Pins `topcoat = "=0.9.0"` and every other dependency exactly. |
| `ui/build.rs`, `ui/styles/app.css` | Tailwind 4.3.3 (checksum-pinned on linux-x64) rendered from classes in `src/`. Route-kind colours are theme tokens shared with the elements. |
| `ui/config.json` | Listen address, trusted operator, backend choice (`fixture { seed }`). `CROSSTALK_UI_CONFIG` overrides the path. |
| `ui/src/main.rs` | Loads config, builds the router (pages, app context, assets, runtime) and serves. Raises `recursion_limit` to 256: pages embedding shards nest component futures past the default depth for the `Send` check. |
| `ui/src/app.rs` | `backend(cx)`, `caller(cx)`, `operator(cx)`, `can(caller, permission)`. `AppBackend` is the configured backend type. |
| `ui/src/config.rs` | `Config`, `TrustedOperator` (builds the all-permissions `Caller`), `BackendConfig`. |
| `ui/src/contract/` | The L8 additions, one module per area, numbered as in [The L8 contract](#the-l8-contract). Types that replace a spec type keep its name and say so (`TopologyFilter`, `OperatorAction`, `QueryError`, `RuleStatus`, `RuleDef`; `alerts`: `Alert`, `AlertState`, `SuppressReason` with `OperatorRejected`). Also: `UserRuleSpec` (what an operator submits) beside the stored `UserRule`, `AuditEntry` with `subject` and `AuditOutcome::Applied(ActionOutcome)`. Channels (rows, filter, resources, names, policy history, promotion preview) are the spec's (`contract::channels` and `contract::graph` are gone; `SetPolicy` and `PromoteChannel` are still the contract's `OperatorAction`); graphs, series, the overview, transmission rows, evidence, excerpts, verdicts, search requests and detection quality are the spec's (`OperatorAction::SetVerdict` carries the spec's `Verdict`). Agents, labels, merge records, vetoes, rows, details, names and the agents filter are the spec's (`contract::agents` is gone; `MergeAgents`, `Unmerge` and `RenameAgent` are still the contract's `OperatorAction`, holding the spec's `MergeRequest` and `AgentLabel`, and a merge's outcome is still `ActionOutcome::Merged(MergeId)`). Checked constructors: `RuleName`, `QueryText`. Topics, the topic history, sizes, lineage and projections are the spec's (`contract::topics` is gone; `research` keeps export, audit and operators). Removed when the gateway's types land. |
| `ui/src/backend/mod.rs` | `Backend`: every read and action, returning `Send` futures: the spec's graph reads with `QueryApi`'s exact signatures (`watermark`, `topology`, `overview`, `channel_topology`, `series`, each `Watermarked`), transmissions, evidence, verdicts and search with `QueryApi`'s exact signatures (`edge_transmissions` (`Watermarked`), `transmissions_by_id`, `transmission`, `transmission_evidence`, `verdicts`, `search`, `detection_quality`), topics and projections with `QueryApi`'s exact signatures (`topic_versions` (the `TopicVersionHistory`), `topic_sizes` (`Watermarked`), `topic_lineage`, `topics` (paged `TopicPage`), `fit_projection`, `projection_status`, `projections`, `projection`), channels and promotion with `QueryApi`'s exact signatures (`channels` (a `ChannelFilter`, `Watermarked` page of `ChannelRow`s), `channel` (`Option<TimeWindow>`, `Watermarked`), `policy_history`, `channel_resources` (paged `ResourceUsePage`, `Watermarked`), `promotion_preview`, `channel_names` (an `IdBatch`)), agents with `QueryApi`'s exact signatures (`agents` (an `AgentFilter` and a window, `Watermarked` page of `AgentRow`s), `agent` (a window, `Watermarked` `AgentDetail`), `agent_names` (an `IdBatch`)), alerts (`alerts`, `alert`), rules, research, pipeline and `act`. |
| `ui/src/backend/fixture/` | `FixtureBackend::try_new(seed)` (fails only on a fixture bug; `main` stops then): a deterministic synthetic world (same seed, same world and answers; generated in well under a second), implementing every `Backend` method with real semantics. `identity` is the merge table (spec `Agent`s, `MergeRecord`s, `MergeVeto`s behind private fields, changed only as `IdentityResolver` defines: `merge` refuses unknown agents, then `MergeRequest::conflict` (`MergeIntoSelf` first, then `AgentMerged`), then a resolver merge a veto separates, an operator merge deleting those vetoes, the source merged away with its prior state and the agents merged into it repointed; `unmerge` reverts one record through `Agent::revert` and `Agent::restore`, records its `Reversal` and a `MergeVeto::of` it, `MergeAlreadyReverted` the second time; `rename` through `Agent::rename`; refusals mapped by `ActionError::from(ResolveError)`). `world/` generates it through the spec's and contract's checked constructors: `agents` (cast; the planned merges and the revert replayed through the merge table; claims recorded per agent as `ClaimSet`s; activity seeded with each traffic-created agent's first exchange), `drafts` and `channels` (channel table; the policy decisions table, built into each channel's checked `PolicyHistory`; detection from traffic; the team-notes promotion applied through `promotion::plan`, the superseded channel's detection frozen at the promotion and later confirmations on it advancing the promoted channel's), `traffic` and `states` (transmissions and accesses), `evidence` (matches, with the origin span's and the read's locations into stored bodies), `blobs` (span records and the blob store: compact bodies built into the spec's `Message` on read, so `Message::part_text` indexes the generated text; `drop_body` for retention), `retention` (the dropped-bodies scenario), `topics` (the model, its topics and assignments) and `catalog` (the spec `TopicVersionHistory` with v1 pinned and v0 dropped by a `RetentionPolicy` keeping the last two activated versions, and each `TopicLineage` with its best link and the others above a 0.6 floor), `rules`, `alerts`, `history` (operators, the audit of the policy decisions and the promotion, verdicts, audit, dead letters). `store` holds what actions change behind one `tokio::sync::RwLock` (channels are `ChannelRecord`s: a spec `Channel`, supersession in its origin, with its `PolicyHistory`, the policy always the history's current one; agents, merges and vetoes are the `Identity` merge table, a merged agent keeping its prior state in `MergedInto`, verdicts are one spec `VerdictLog` per judged transmission, the audit log is plain `AuditEntry`s, projection jobs are `Job::Ready(Projection)` or `Job::Record(ProjectionInfo)` for every other status); `queries/` resolves merged agents and superseded channels at read time and pages with keyset cursors (`linked`: the one place a linked view resolves its filter's `TopicVersionSelector`, against the world's versions stated as a spec `TopicVersionHistory` (unknown `NotFound`, never activated `Conflict(TopicVersionNotActivated)`, not retained `VersionNotRetained`, foreign topics `Conflict(TopicsNotInVersion)`), and counts confirmed transmissions by `Confirmed::at` admitted by `TopologyFilter::admits`, accesses by `admits_access`; `graph`: `topology`, `channel_topology` and `overview` (`EdgeTotals::of` the graph, queues as `QueueCounts::tally` defines them) on bucket-aligned windows, checked by `TopologyGraph::check_nodes` / `BipartiteGraph::new`; `nodes`: agent nodes (spec label, canonical state, parent chain and claims as `agents::profile` reads them) and channel nodes (`CanonicalOriginKind`, `DetectionKind`, `PolicyKind`, `locator_summary`); `series`: `EdgeStore::series` on the fixture's bucket width; `topics`: the catalog's reads failing as `CatalogError` maps (the history; `TopicSizes` from the assignments of transmissions confirmed in the window, a dropped version's frozen at its drop and refused with a window; the lineage; a version's topics newest id first, the cursor pinning the version); `projection`: `fit_projection` (version resolved as a linked view, `MAX_PENDING` jobs, a queued `ProjectionInfo` run at once through `start` and `complete` or `fail`), the store's reads (`ProjectionStoreError` mapped by the spec), `sample` (bottom-k of the admitted confirmed transmissions by a seeded SplitMix64 key standing in for the spec's BLAKE3, the frame built with `ProjectionFrame::from_points`, a stand-in layout of theme clusters; too few points fail with `TooFewPoints`) and `seed` (the world's jobs: expired, failed when v0 was dropped while queued, fitting and queued; frames are kept `FRAME_RETENTION`, three days); `transmissions`: `edge_transmissions` (what `linked` counts into the edge, newest confirmation first, `Watermarked`), `transmissions_by_id` (`TransmissionSummary::of` rows, newest id first, unknown ids left out) and `quality` (`DetectionQuality::tally` over every transmission with its current verdict), each traversal pinning its topic version in its cursors (`page::versioned`, `page::pinned`; a pinned version no longer retained is `VersionNotRetained`); `evidence`: `transmission`, `transmission_evidence` (`TransmissionEvidence::assemble`, excerpts by `Excerpted::of` from the span records and the blob store; a missing record is `Store`) and `verdicts`; `search`: a linked view over confirmed transmissions (optional window, filter before ranking, deterministic stand-in scores); `agents`: `list` (canonical `AgentProfile`s admitted by `AgentFilter::matches`, newest agent first, cursors bound to the filter only, unaligned windows refused), `one` (an `AgentCluster`: aliases, children, the merge records and vetoes naming the cluster, `AgentLookup::Redirected` for an alias) and `names` (`AgentName::of` the canonical agent, keyed by the id asked for), `profile` (canonical parent, `ClaimSet::union` over the cluster, last seen over the cluster, shared with graph nodes; `traffic`: the agent nodes' counts of the default-filter topology for the window, `EdgeStore::agent_traffic`); `channels`: the registry as `promotion::Registered` entries, `rows` (`ChannelRow`s newest first, counts as `ChannelCounts::tally` of the resource use and `ChannelCounts::routed` of the default-filter graph for the window, all time as `clock::all_time`, unaligned windows refused, superseded rows with their `SupersededInto`), `resources` (`ResourceUse` pages, newest resource first, through the canonical channel, cursors bound to channel and window), `names` (`channels::resolve_names`), `policy_history` and `coverage` (`promotion::coverage` for `promotion_preview`); `lists`: list filters, and audit subject matching derived from each entry's subject, action, outcome and the merge log); `actions/` applies and audits every action (a policy decision is recorded in the channel's history, a superseded channel refused; a promotion follows `promotion::plan`, keeping the channel's id, recording its decision and superseding the covered channels; sanctioning suppresses the active alerts about the channel and the channels it superseded; subject and outcome recorded on the entry; a verdict is a `TransmissionVerdict::new` appended with `VerdictLog::record`, a repeat appending nothing, and an appended false detection suppresses the transmission's active alerts with `OperatorRejected`); `world/topics` also holds the fixture's text embedder (`embed`: theme vectors weighted by vocabulary hits plus hashed word axes), used for `CreateRule`/`UpdateRule` and the generated semantic rules; `text/` holds the message templates and codecs; `rng` is SplitMix64. The world: 7 days ending at `now()` (2026-10-03T00:00Z, watermark ten minutes earlier), about 5,000 transmissions on a weekday daytime curve in every state (in-flight states sit in the last quarter hour) and every route, match kind (decode chains such as base64 → url) and carrier. 40 canonical agents across Claude Code, Codex, pi, oh-my-pi and self-hosted scripts, with sub-agents, three config-registered agents with no traffic, and labels. Scenarios: pi and oh-my-pi agents labelled `pi-scraper` and `omp-orchestrator` (and two unlabelled ones) also claim Claude Code; `atlas-lead` has a resolver-merged alias whose traffic to it becomes a dropped self-edge; one pi agent holds two aliases, one repointed by a later merge; an operator merge was reverted and left a veto (the oh-my-pi agent with a veto on its page). Channels: declared sanctioned `wiki.corp.internal/eng`, `git.corp.internal/platform/monorepo` and `issues.corp.internal` (active); `docs.corp.internal/design` awaiting traffic; `nfs-01:/mnt/shared/releases` unused, with an open sanctioned-unused alert; the hijacked public wiki is the discovered, unreviewed, active channel seeded at `wiki.example.org/wiki/Agent_Coordination` (injection-style text, the busiest channel), with its talk page as a second discovered channel the same `UrlPrefix` pattern covers; `paste.example.net` unsanctioned; the `memory` MCP server reset to unreviewed; `/tmp/agent-handoff` on `devbox-3` sanctioned; `gist.example.com` dormant; a `kv_put` tool only one agent uses (observed); an `s3://agent-scratch` prefix with only suspected traffic (candidate); `notes.corp.internal/team-a`, discovered at its retro page and promoted by the researcher with the `/team-a` prefix (sanctioned, same id), whose promotion superseded the discovered `notes.corp.internal/team-a/standup` (detection frozen at the promotion). Policy histories: the config declarations, the `memory` server sanctioned then reset, the pastebin unsanctioned, the handoff directory sanctioned, the promotion's decision. Confirmed transmissions not yet classified have no topic. Topics: v0 (unfitted, once active, dropped by retention when v2 was activated), v1 (six topics, fitted six days ago, pinned) and v2 (ten, two days ago, active); v1's "Engineering chatter" has no link at or above the 0.8 remap threshold in v2, so its watched-topic rule is stale. Projections: four seeded jobs (expired, failed, fitting, queued); every fit adds a ready (or failed) job. Rules: the five built-ins, a watched-topic rule on v2 (credentials and agent instructions), a stale v1 rule, a semantic query on paste sites (with an agent-subject alert) and a disabled refund rule; sinks soc-webhook (last delivery failed), #agent-alerts, local-log. About 650 alerts in every state and suppress reason, deduplicated occurrence counts on channel alerts. Two operators: `researcher` (the trusted operator in `config.json`) and `oncall` (view, content, triage); about 60 verdicts (one withdrawn), the oldest six confirmed transmissions with their sender's or reader's message bodies dropped by content retention (alternating; `Scenario::dropped`), a few hundred audit entries including two rejected actions, and four dead letters. |
| `ui/src/url/` | `ulid` (Crockford text for every id), `route` (URL text for `Route` and `RouteKind`), `view_state` (`RawViewState` → `ViewState` and back to the canonical query). |
| `ui/src/pages/` | `mod.rs` (root layout; navigation links carry the current view state when the request has a complete one), `view.rs` (`defaults(cx)`: the default window and topic version from `Backend::now` and the active version of `topic_versions`; async `view_state(cx)`: parse, default, redirect to canonical; async `current_state(cx)` for the layout; async `state_from_query(cx, query)`: a shard's view-state argument, parsed strictly), one module per screen. |
| `ui/src/pages/common/` | Shared by the pages. `action` (`perform`: permission check then `Backend::act`; `done`: 303 with flash; `Failure<F>` and `error_for`/`fields_for` to show an error next to its form; `status_of`), `flash` (`Flash` codes and messages), `form` (`FormFields`: a urlencoded body as pairs, keeping repeated keys; validators `id`, `required`, `note`, `policy`, `similarity`, all failing as `QueryError::InvalidInput`), `paging` (`cursor` key, `page_request`), `links` (entity URLs with the view state), `lookup` (operator and rule names; agent names from one `agent_names` call per `IdBatch`; `id_batches`: distinct ids in `IdBatch`es of at most `IdBatch::MAX`), `topics` (`default_version`; `all_topics`: a version's topics followed to their last page; `topic_trends`: one `series` grouped by topic on a 24-point grid, as `Trends`), `transmissions` (`summaries_by_id`: rows from one `transmissions_by_id` call, an empty or oversized selection refused as the spec's `TransmissionSelection` refuses it; `TransmissionRow` from a spec `TransmissionSummary`/`SummaryState`, `rows`, `transmission_table`; `route_text`, `ChannelNames` from one `channel_names` call per `IdBatch`, `summary_name` of a `ChannelRow`). |
| `ui/src/pages/overview/` | `/`: `model` (`tiles` from one `overview` call; `load`: tiles, the heaviest edges of `topology`, newest open alerts), `mod` (the page). |
| `ui/src/pages/topology/` | `/topology`: `mod` (page, header toggles, `workspace` with the graph, brush and drawer sharing the `sel` signal; `brush_window` (snapped outward to bucket boundaries), `timeline_src`; header counts from the spec graph), `selection` (`Selection`: the element's value grammar, parsed and encoded), `query` (`sel`, `collapse`; `submitted_filter` for the filter form), `filters` (choices, `filter_form`, `filter_chips`), `drawer/` (`mod`: the `topology_drawer` shard; `model`: argument validation and `load`, `edge_items`, `EdgeRow` from the spec's `EdgeTransmission`), `tests`. |
| `ui/src/pages/transmission/` | `/transmissions/{id}` GET and POST `set-verdict`: `mod` (header from the transmission's `TransmissionSummary`, `TopicCell`, load), `model` (state in words, `Strength`, match kind and carrier labels, `ExcerptView` from a spec `Excerpt`, `QuoteView` (shown, or body dropped), co-access views from `AccessDetail`), `sections` (matches side by side, co-access timeline), `verdict` (form with the spec's `Verdict`, parser, rows from a `VerdictLog`), `tests`. |
| `ui/src/pages/explore/` | `/explore` GET and POST `fit`: `mod` (page, `ProjectionPanel`, colour-by and selection signals), `query` (`q`, `m`, `p`, `cb`, `ps`), `lasso` (`Polygon`: parse, even-odd point-in-polygon, `select` over stored points; `ProjectionSelection`), `results` (the `projection_results` shard), `search` (hits, form), `topics` (sidebar, `watch_url`), `fit` (parameters and form), `tests`. |
| `ui/src/pages/topics/` | `/topics`: `mod` (page, version picker, tables), `model` (`version_tabs`, `topic_rows`, `remap_rows` with stale rules). |
| `ui/src/pages/export/` | `/export` GET and POST: `mod` (page, form, the "not available yet" outcome), `request` (form → `ExportRequest`, `describe`), `quality` (`quality_lines`, precision, table). |
| `ui/src/pages/channels/` | `list` (`/channels`; `ListRow` and `Activity`, a row's counts or why it has none), `query` (list keys and toggles onto the spec's `ChannelFilter`/`OriginFilter`), `model` (shape, title, origin and detection in words, decisions), `detail` (`/channels/{id}` GET and POST `set-policy`), `sections` (paged resources, alerts, policy history; the audit history rows the alert page shares), `policy` (set-policy form and parser), `promote/` (`patterns`: candidates from the seed and coverage; `mod`: GET and POST `/channels/{id}/promote`; `screen`: the page). |
| `ui/src/pages/agents/` | `list` (`/agents` with state and claim chips; rows from spec `AgentRow`s over the view's window, parents named in one `agent_names` call per batch, `last_seen_text`), `query` (`state`, `claims` keys into `AgentFilter::{states, claimed}`), `detail` (`/agents/{id}` GET and POST `rename`, `clear-label`, `unmerge`; the page from the `AgentCluster`, the alias banner from `AgentLookup::Redirected`), `actions` (form parsers; `AgentLabel` errors worded by `label_error`; `merge_action`, one id twice `InvalidInput(SelfMerge)`), `evidence` (evidence rows, shared and conflicting evidence), `tree` (bounded sub-agent tree, one `agents` call per level), `sections` (alias rows with their prior state; merge rows say what an unmerge restores, and a reverted one what its `Reversal` pointed back), `merge` (`/agents/{id}/merge` GET and POST; comparing with an id of the same cluster is `Conflict(MergeIntoSelf)`), `tests` (router tests: the alias banner, the reverted merge's veto, unmerging once, refused renames and merges). |
| `ui/src/pages/alerts/` | `inbox` (`/alerts` GET and POST `acknowledge`, `resolve`; `parse_action` shared with the alert page), `detail` (`/alerts/{id}` GET and POST: rule, subject, state, triage forms, audit history), `model` (alert rows), `rules/` (`mod`: `/alerts/rules` GET and POST `set-enabled`; `model`: rule and sink rows; `form`: watched-topic and semantic forms, parsed into `UserRuleSpec`; `edit`: `/alerts/rules/new` (`?topic=<id>` preselects a topic of the current version) and `/alerts/rules/{id}` GET and POST). |
| `ui/src/pages/audit/` | `page` (`/audit`), `query` (`op`, `subject`, `span`), `describe` (an action in words and its note), `subject` (subject codes, `mg.` for merges, and links; alerts link to their page). Rows show the entry's own `subject` and link what an applied action created. |
| `ui/src/pages/pipeline/` | `/pipeline` GET and POST `replay`. |
| `ui/src/components/` | Shared markup: route and claim badges, content-hidden marker, error and empty states, page header, name and time formatting (`agent_name`, `agent_name_of` for an `AgentName`), `abbrev_digest`. `badge` (`Tone`, the `Badge` trait for policy, origin, detection, agent state, alert state, evidence strength, transmission state and verdict; `state_badge`, `kind_badge`), `table` (`data_table` and cell classes), `paging` (`PageLinks`, `pagination`), `nav` (`tabs`, `filter_chip`, `segmented`), `sparkline` (inline SVG trend; `points`), formatting helpers `format_time_short`, `format_bytes`, `format_share`, `format_duration`, `locator` (`locator_text`, `pattern_text`, text forms), `href` (`href`: a path with the view state and page pairs; `state_pairs`), `form` (control classes, `state_inputs` for `GET` forms), `feedback` (`flash_banner`). |
| `ui/src/testing/` | Test-only: a router over the fixture backend with an asset catalog built from the test binary, `get`/`post` returning status, location and body (a fresh world per request), `Session` (one router, so state carries across requests), `world`/`agent_id`/`channel_id` (scenario ids from a same-seed copy), and `cx`/`render` for rendering components. |
| `ui/src/data/mod.rs` | The data routes' module: route table, `require(caller, permission)` (403). |
| `ui/src/data/query.rs` | `view_state(cx)`: the strict view-state parse (every required key or 400, never a redirect; same `ViewState::parse` and `pages::view::defaults`). `buckets(cx)`: `buckets=` in 1..=1000, default 96. |
| `ui/src/data/errors.rs` | `query_error(QueryError)`: Forbidden → 403, NotFound → 404, VersionNotRetained / InvalidInput → 400 with the message, others → 500 (logged). |
| `ui/src/data/names.rs` | Channel display names from a pattern or seed locator (`locator_name`, `pattern_name`, `shape_name` of the spec's `ChannelShape`, `channel_name` of the spec's `ChannelName`). |
| `ui/src/data/topology/` | `GET /data/topology`: `TopologyPayload::{agents, channels}` (from the spec graphs; channel names from one `channel_names` call) and its node, edge and code types; `tests`. |
| `ui/src/data/timeline.rs` | `GET /data/timeline`: `timeline_grid` (the aligned grid for `n` buckets), `TimelinePayload::new` from two `Total` series on that grid (buckets with `final`). |
| `ui/src/data/projection/` | `GET /data/projection/{id}`: `format.rs` (binary layout, `ProjectionHeader`, `PayloadPoints` (the spec frame re-indexed into id-sorted tables, channels supplied), `ProjectionTables`, `encode`), `mod.rs` (route, `point_channels` from `transmissions_by_id`, `tables`: names from one `agent_names` and one `channel_names` call, chunked at `IdBatch::MAX`, and the version's `topics`), `decode.rs` (test-only strict decoder). |
| `ui/src/data/elements.rs` | `TOPOLOGY_JS`, `PROJECTION_JS`, `TIMEBRUSH_JS`: the bundled elements as Topcoat assets. |
| `ui/src/data/fixtures/`, `route_tests.rs` | Tests: hand-built spec graphs and series (`graphs`) and a spec `Projection` with its points' channels (`mod`), the element fixture files written from them, and the routes through the router. |
| `ui/elements/package.json`, `pnpm-workspace.yaml` | pnpm package; exact pins; `minimumReleaseAge` of a week for every transitive dependency. Scripts: `build`, `demo`, `smoke`, `test`, `typecheck`, `lint`. |
| `ui/elements/scripts/build.mjs` | esbuild: `src/ct-*.ts` → `dist/<name>.js` (ESM, minified, external source map); `--serve` rebuilds and serves the package for the demo. |
| `ui/elements/scripts/smoke.mjs` | Headless-Chrome smoke test of the demo over CDP, with screenshots in both colour schemes. |
| `ui/elements/src/ct-*.ts` | Entry points: define `ct-topology`, `ct-projection`, `ct-timebrush` (once). |
| `ui/elements/src/shared/` | `element.ts` (`PayloadElement`: the element contract, fetch/abort, status panels), `fetch.ts` (typed `LoadError`), `selection.ts` (value grammar), `theme.ts` and `color.ts` (tokens, light/dark), `ulid.ts`, `route.ts`, `format.ts`, `hash.ts`, `webgl.ts`, `result.ts`. |
| `ui/elements/src/payloads/` | zod schemas mirroring `ui/src/data/` (`topology.ts`, `timeline.ts`), and the binary projection decoder (`projection.ts`). |
| `ui/elements/src/topology/` | `model.ts` (payload → drawn graph, collapse, selection, highlight), `layout.ts` (seeded ForceAtlas2), `style.ts`, `tooltip.ts`, `diamond-program.ts` (sigma node program), `curvature.ts` (edges sharing a pair of nodes bend apart: reciprocal pairs to opposite sides, same-direction routes fanned out; lone edges stay straight; drawn with `@sigma/edge-curve`), `element.ts`. When sub-agents are collapsed, clicking a merged edge selects the heaviest edge it stands for. |
| `ui/elements/src/projection/` | `transform.ts` (data ↔ normalised), `lasso.ts` (point-in-polygon, simplification, rounding), `colors.ts` (colour-by and legend), `element.ts`. |
| `ui/elements/src/timebrush/` | `model.ts` (axis, snapping, bars, ticks), `element.ts` (SVG). |
| `ui/elements/test/` | vitest suites, and `fixtures/` (written by the Rust tests). |
| `ui/elements/demo/index.html` | Static harness showing all three elements and their states on the fixtures (`pnpm demo`). |

## Invariants and constraints

- A page or route never returns content fields to a caller without
  `Content`, and never performs an action the caller lacks the permission
  for. Shards and procedures check permissions themselves: Topcoat does not
  run page guards for shard and procedure endpoints.
- Every value the browser sends (signals, shard arguments, procedure
  arguments, query strings, form fields) is validated into typed values
  before use. Page-specific query keys live in their own structs; an
  unknown value is a 422 naming the key.
- Action controls are rendered only for callers holding the action's
  permission, and every post checks it again before calling the backend.
- Links between pages carry the view state, so the filter follows the
  user; page-specific keys (tabs, cursors, filters) do not leave the page.
- Every graph, timeline and projection a page shows is fully determined by
  its URL and the data's watermark.
- Elements never call L8 and never hold authorization logic.
- Data routes never redirect, and answer only with payloads the caller's
  permissions allow; payload shapes change in Rust, the zod schemas and
  the fixtures together.
- Harness claims are always presented as claims.
- Pages and routes reach data only through the `Backend` trait, including
  the present and the default topic version; nothing outside `main.rs`,
  `app.rs` and the tests names `FixtureBackend`.
- The UI never computes or handles embeddings: rule forms submit text
  (`UserRuleSpec`) and search sends text (`SearchRequest`).
- Names for many ids are one backend call (`agent_names`,
  `channel_names`), never a read per id.
- An audit entry's subject and what it created come from the backend
  (`AuditEntry::subject`, `AuditOutcome::Applied(ActionOutcome)`); pages
  do not infer them from the action.
- What a promotion would do is computed by the backend over every known
  resource, by the same plan `PromoteChannel` applies.
- Topcoat is a dependency of `crosstalk-ui` only.

## Build and toolchain

- Rust: the repository toolchain (`nightly-2026-10-02`); Topcoat 0.9.0
  needs rustc 1.98 or newer. The build script downloads the Tailwind CLI
  from GitHub on first build.
- Assets: `pnpm --dir ui/elements install && pnpm --dir ui/elements build`
  first, then `topcoat asset bundle` (from `topcoat-cli` 0.9.0, run in
  `ui/`), so the bundle includes the elements and the Tailwind stylesheet.
  The bundle is written next to the binary and must come from the same
  build. Only asset constants a page renders are bundled: an element
  appears in the bundle once a page uses its `data::elements` constant.
- Elements: Node 24, pnpm 11. `pnpm test` (vitest), `pnpm typecheck`,
  `pnpm lint` (biome), `pnpm demo` (serves `demo/` on 127.0.0.1:8737 with
  the fixtures), `pnpm smoke [dir]` (after `pnpm build`; needs
  `google-chrome`).
- Run: `cargo run` from `ui/` serves on the configured address.
- Topcoat releases roughly weekly and expects breaking changes. Upgrades
  are deliberate, one version at a time, and only to releases at least a
  week old.

## The L8 contract

What the UI needs from L8 beyond the current `QueryApi` and
`OperatorActions`. Items are referenced by the screens above.

### Reads

1. **Lists with cursors**: `channels`, `agents`, `agent`, `rules`,
   `sinks`, `operators`, `dead_letters`, `transmissions(filter)` (for an
   edge or a set of ids), `audit`. Search also takes a cursor.
2. **One filter for every view.** `search`, `projection`, export and the
   access aggregate take the same filter as `topology`. The filter gains
   an explicit `topic_version` and a verdict choice (include or exclude
   transmissions marked as false detections).
Items 3, 4 and 6 have landed in the spec (`aggregates::node`,
`aggregates::access`, `aggregates::series`, `QueryApi::{topology,
channel_topology, series, overview}`) and the UI reads them directly; so
have the transmission lists of item 1, items 10, 22 and 23, and the read
side of item 17 (`QueryApi::{edge_transmissions, transmissions_by_id,
transmission, transmission_evidence, verdicts, search,
detection_quality}`, `aggregates::quality`, `derived::flow::verdict`,
`interfaces::l8_surface::{summary, evidence, excerpt}`). `SetVerdict` is
still the contract's `OperatorAction` until the actions land. Items 7, 8
and 9 have landed as the topic history and projections
(`aggregates::{topic_history, retention, projection}`,
`QueryApi::{topic_versions, topic_sizes, topic_lineage, topics,
fit_projection, projection_status, projections, projection}`), with one
gap: a `ProjectionFrame` has no channel column, so the projection
payload reads channel-routed points' channels from `transmissions_by_id`
(see [Element payloads](#element-payloads)). Item 27's
`current_topic_version` is the history's active version. The channel
reads have landed too: the channel list of item 1 and item 28's channel
filter (`ChannelFilter` with `OriginFilter`), item 5 (paged
`ResourceUsePage`), item 24's `channel_names` (over an `IdBatch`), item 26
(`PromotionPreview::from_registry`, a conflict as an answer) and the
policy history (`QueryApi::{channels, channel, channel_names,
channel_resources, promotion_preview, policy_history}`,
`interfaces::l8_surface::channels`, `derived::flow::channel::{policy,
promotion}`). Item 16's semantics are the spec's: promotion keeps the
channel's id (`ChannelPromoted` names it) and supersedes the other
discovered channels whose seed the pattern matches; `SetPolicy` and
`PromoteChannel` are still the contract's actions until the actions land.
Items 14, 15 and 24's agent half, and the agent lists of items 1 and 28,
have landed as the agent read models (`QueryApi::{agents, agent,
agent_names}`, `aggregates::agents` with its `AgentFilter`,
`observed::agent::{merge, claims}`): rows are canonical agents with their
traffic in a window (their topology node's counts), a detail is an
`AgentCluster` (merge records with their `Reversal`, vetoes, aliases whose
`MergedInto::prior` an unmerge restores), and a merge must name two
canonical agents. `MergeAgents`, `Unmerge` and `RenameAgent` are still the
contract's `OperatorAction` (holding the spec's `MergeRequest` and
`AgentLabel`) until the actions land.

3. **Graph nodes.** `TopologyGraph` gains `nodes: Vec<GraphNode>`:
   `Agent { id, label, state_kind, parent, claims, transmissions_in,
   transmissions_out }` or `Channel { id, label, origin_kind,
   detection_kind, policy_kind, locator_summary }`. Every edge endpoint
   has exactly one node; canonical parents are included. `claims` is the
   set of distinct `HarnessClaim`s seen on the agent's exchanges with
   last-seen times.
4. **Bipartite topology.** An access aggregate `AccessEdge { agent,
   channel, op: Read | Write, accesses, bucket }` and
   `channel_topology(...) -> BipartiteGraph { nodes, accesses,
   transmissions }`. Access shares are normalised separately from
   transmission shares.
5. **Channel resources.** `channel_resources(channel, window) ->
   Vec<ResourceUse { resource, writers, readers }>`.
6. **Timeline.** Transmission counts (and matched bytes) per bucket under
   the filter, for the time brush and per-topic trends.
7. **Topic stats and history.** Per-topic counts in a window, the list of
   model versions with fit times, and the remap between consecutive
   versions.
8. **Projection points carry their categories.** Sender, reader, route
   kind and topic per point, in a compact columnar encoding (Arrow IPC or
   packed f32 coordinates with u32 indices into category tables). Still
   missing from the spec's frame: the channel of a channel-routed point.
9. **Reproducibility.** Explicit `topic_version` on topology, search,
   projection and export (typed `VersionNotRetained` when not retained);
   a retention policy for old versions; a `watermark` on every aggregate
   response; projections as stored artifacts: `fit_projection(window,
   filter, topic_version, params) -> ProjectionId` (a job with status) and
   `projection(id)`, recording embedding model, seed, parameters and
   sample.
10. **Detection quality.** `detection_quality(window) -> Vec<QualityRow {
    route_kind, match_kind, genuine, false_detection, unlabeled }>`.
11. **Export.** `export(ExportRequest { dataset: Transmissions | Edges |
    Accesses | Topics | Projection(id) | Verdicts, window, filter,
    topic_version, format: Jsonl | Parquet, include_content })` as a
    stream with a manifest (query, watermark, versions, gateway version).
    `include_content` needs `Content`.
12. **Audit.** `AuditEntry { id, at, by: Config | Operator(id), action,
    subject: Option<AuditSubject>, outcome: Applied(ActionOutcome) |
    Rejected(error) }`, append-only, covering every operator action and
    config change. `subject` is what the entry is about (`Agent`,
    `Channel`, `Transmission`, `Rule`, `Alert` or `Merge(MergeId)`): the
    action's target, or what it created when it names none (a created
    rule); an applied outcome carries what the action created.
    `AuditFilter::subject` keeps entries about an entity or that created
    it, resolving aliases and supersession, and an agent's entries include
    the merges and unmerges it took part in. `operators()` with display
    names.

22. **Evidence content.** `transmission(id)` returns the matched text:
    `TransmissionEvidence { transmission, matches: Vec<MatchEvidence {
    content_match, origin: Excerpt, read: Excerpt }>, accesses:
    Vec<AccessDetail { access, resource }>, verdicts }`. An `Excerpt` is
    text around the matched range with the highlight checked to lie on
    character boundaries. Needs `Content`.
23. **Search by text.** `search` takes `SearchRequest { text, mode: Text |
    Semantic | Hybrid }` and the gateway embeds the text; the UI never
    handles embeddings.
24. **Batch names.** `agent_names(ids) -> HashMap<AgentId, AgentName { id
    (canonical), label }>` and `channel_names(ids) -> HashMap<ChannelId,
    ChannelName { id (in force), shape: Pattern | Seed }>`, keyed by the
    id asked for, unknown ids left out; `View`.
25. **One alert.** `alert(id) -> Option<Alert>`; `View`.
26. **Promotion preview.** `promotion_preview(channel, pattern) ->
    PromotionPreview { covered_resources, uncovered_resources,
    superseded_channels, conflicts: Option<ConflictKind> }` over every
    known resource: what the declared channel would hold, the superseded
    channels' resources outside the pattern, the other channels it would
    supersede, and why `PromoteChannel` would be refused. Unknown channel
    is `NotFound`; `View`.
27. **The present.** `now() -> Timestamp` (the end of a default view's
    window). The default topic version is `topic_versions().active()`.
28. **List filters.** `agents(AgentListFilter { states, harness_claims,
    text, parents }, page)` (`parents`: one level of a sub-agent tree) and
    `channels(ChannelListFilter { origins, detections, policies,
    include_superseded, window: Option<TimeWindow> }, page)`, where
    `window` restricts the counts, never the rows.

### Actions

13. **`act` returns an `ActionOutcome`**: `RuleCreated(id)`,
    `ChannelPromoted(id)`, `Merged(MergeId)` or `Applied`.
14. **Rename**: `Agent.label: Option<AgentLabel>` (checked: trimmed,
    non-empty, bounded) and `RenameAgent { agent, label }`. Renaming a
    merged agent is a typed conflict.
15. **Unmerge**: `Agent.state: AgentState` (replacing the spec's) whose
    `Merged { into, at, by, prior: ActiveAgentState }` keeps the state the
    agent had before the merge (`Registered | Provisional |
    Established`); merges are logged as `MergeRecord { id, from, into, by,
    at, repointed }`; `Unmerge { merge }` reverts exactly that record,
    restoring `prior`; a `MergeVeto { a, b, by, at }` stops the resolver
    from re-merging an operator-split pair, and an operator merge clears
    it. A merge whose target resolves to the source is
    `Conflict(MergeIntoSelf)`.
16. **Promote**: `PromoteChannel { channel, pattern, policy, note }`
    creates a declared channel (detection starts `InUse`) and supersedes
    the discovered channel and every other discovered channel the pattern
    covers. The pattern must cover the seed resource. Channel ids resolve
    through supersession at read time, like agent aliases.
17. **Verdicts**: `Verdict = Genuine | FalseDetection` as an append-only
    `TransmissionVerdict { transmission, verdict, by, at, note }`, a
    separate axis from `TransmissionState`. `SetVerdict { transmission,
    verdict: Option<Verdict>, note }` is allowed on `Suspected`,
    `Discarded` and every state holding a `Confirmed`. A false detection
    suppresses active alerts on that transmission
    (`SuppressReason::OperatorRejected`): `Alert`, `AlertState` and
    `SuppressReason` replace the spec's with that one reason added.
18. **Rules**: built-in rules (one each, enable or disable only) are split
    from operator rules (`WatchedTopic`, `SemanticQuery`: create, edit,
    disable). Rules are never deleted. `AlertRuleDef` gains `name`,
    `created`, `sinks`. Operators submit a `UserRuleSpec`:
    `WatchedTopic { version, topics, remap_threshold }` or
    `SemanticQuery { text: QueryText, threshold }`; the gateway embeds the
    text with its current model and stores a `UserRule` whose
    `SemanticQuery` keeps text, model and embedding. `Stale` carries a
    reason (`TopicsUnmapped` or `EmbeddingModelChanged`). Actions:
    `CreateRule`, `UpdateRule` (re-targets a stale rule and re-embeds a
    query), `SetRuleEnabled`; enabling a stale rule is
    `Conflict(RuleStale)`, a watched-topic rule on a version other than the
    current one `Conflict(TopicVersionNotCurrent)`.

### Wire

19. **JSON** for every request and response, ids as strings.
20. **Live updates**: an SSE stream of `UiEvent` (`AlertChanged`,
    `ChannelChanged`, `AgentChanged`, `RuleChanged`, `Watermark`,
    `TopicVersionReady`, `ProjectionReady`), ids only.
21. **Typed errors**: `QueryError` gains `Forbidden { missing }`,
    `VersionNotRetained { version }`, `Conflict(ConflictKind)` and
    `InvalidInput(InputError)` in place of `BadRequest { reason: String }`.
29. **Conflict kinds**: `ConflictKind` is `AgentMerged`,
    `ChannelSuperseded`, `ChannelNotDiscovered`, `PatternMissesSeed`,
    `MergeReverted`, `NotJudgeable`, `AlertState`, `BuiltinRule`,
    `RuleStale` (enabling a stale rule instead of updating it),
    `MergeIntoSelf` (the target resolves to the source) and
    `TopicVersionNotCurrent` (a watched-topic rule on a non-current
    version). `InvalidInput` is kept for malformed fields.
