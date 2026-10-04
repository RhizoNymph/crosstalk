# User interface

The operator-facing web UI: topology, transmission evidence, content
exploration, topics, channels, agents, alerts, and the research and
pipeline tools around them. It is designed top-down from the operator's
jobs and meets the gateway at the spec's L8 surface: every read goes
through `QueryApi`, every action through `OperatorActions::act`, and live
updates through `LiveFeed`, all from `crosstalk-spec`
(`spec/types/interfaces/l8_surface.rs`). Until the gateway exists, a
deterministic fixture backend implements those traits with the spec's
semantics. Two things the spec does not expose yet (the bucket width and
the present; the export formats a backend writes) are declared as small
gap traits in `ui/src/contract/`; [What the UI uses from
L8](#what-the-ui-uses-from-l8) maps every screen to the spec methods it
calls and lists the [remaining gaps](#remaining-gaps).

## Scope

- Every screen, the data each one reads and the actions it can take.
- How view state lives in URLs so a view can be cited and reproduced.
- The split between server-rendered pages (Topcoat) and the client-side
  WebGL elements (topology graph, UMAP projection, time brush), and the
  contract between them.
- A deterministic fixture backend for development, tests and demos,
  implementing the spec's L8 traits.
- What the UI uses from L8, the spec gaps it still works around, and the
  behaviour the spec's semantics give the screens.

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
| Audit | Who changed what, and what did config change? | Audit |

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
│   pages/  shards/  data/ (#[route] endpoints for elements, SSE)      │
│   url/ (view state, bucket-aligned scope)   config::Access (Caller)  │
│   spec QueryApi + OperatorActions + LiveFeed  ┐                      │
│   contract::{Present, ExportFormats} (gaps)   ┴─ app::AppBackend =   │
│        FixtureBackend (seeded synthetic); an L8 client later         │
└──────────────────────────────────────────────────────────────────────┘
```

- **Topcoat renders everything except the WebGL views.** Pages are async
  components that read through the spec's `QueryApi` with the request's
  `Caller` and check permissions before touching data. Shards re-render
  server-side regions (drawers, result lists) when signals change.
- **There is no UI-side backend trait.** Pages, shards and data routes
  call the spec's traits (`QueryApi`, `OperatorActions`, `LiveFeed`) and
  the two gap traits (`contract::present::Present`,
  `contract::formats::ExportFormats`) directly on `app::AppBackend`, a
  type alias for the concrete `FixtureBackend` (an enum over
  implementations once there is more than one). The spec's methods are
  native `async fn`s with no `Send` bounds; because the backend type is
  concrete, every future a page awaits is concrete too, and its
  `Send`-ness, which Topcoat's multi-threaded runtime needs, is inferred
  and checked where each `#[page]`, shard and `#[route]` is registered.
  Shared helpers that only read (`pages::common::{topics, rules}`,
  `data::projection`) are generic over `B: QueryApi` and are instantiated
  with `AppBackend`.
- **Callers come from the spec's operator directory.** `config::Access`
  loads the spec's `OperatorDirectory` from `AccessConfig::Trusted` with
  the configured `TrustedOperator` and asks it once for the request
  caller (`RequestIdentity::Anonymous`): in trusted mode every request is
  that operator with every permission. `app::caller(cx)` hands it out;
  `app::can` is `Caller::has`.
- **Errors are the spec's.** `error::UiError` is either the spec's
  `QueryError` (an `ActionError` converts through `QueryError::from`) or
  a field the UI refused before calling anything; `error::describe`
  words every `QueryError` once (the spec's errors carry no text), and
  every page, shard and route renders errors through it.
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
- **The fixture implements every trait the UI calls.** `FixtureBackend`
  implements `QueryApi` (`type ExportRows`: the export's row stream),
  `OperatorActions`, `LiveFeed` (`type Stream`) and the two gap traits
  (`backend/fixture/surface.rs`), with the spec's semantics. The gap
  traits are deleted when the spec gains what they declare.

## View state and URLs

Everything that changes what data a view shows lives in the URL query:
time window (absolute bounds; "last 24 h" is resolved when chosen),
weighting, filter (agents, channels, route kinds, topics, verdicts), topic
model version, graph mode (agents or bipartite), projection id, and the
selected edge, node, point set or transmission. Hover, open menus and
scroll position do not.

- One typed `ViewState` struct parses and renders the query, so every page
  reads the same keys and links between views carry the filter along. Its
  `scope` (`url::scope::Scope`) is what every view of the URL is computed
  over: the window, the topic-model version and the filter keys
  (`ViewFilter`).
- **Windows are on bucket boundaries.** The spec refuses any other window
  for graphs, channel and agent counts and series
  (`InvalidInput(UnalignedWindow)`), so the UI snaps to the backend's
  bucket width (`Present::bucket_width`; five minutes in the fixture).
  `ViewState::parse` snaps an unaligned URL window outward to the
  smallest aligned window covering it (`url::scope::{align_down,
  align_up}`) and marks the URL not canonical, so a page redirects to the
  snapped window; a data route or a shard argument refuses it
  instead (400, "the window is not on bucket boundaries"). Every window
  the UI sends is therefore aligned, and the time brush's buckets and the
  timeline grid are multiples of the bucket width.
- **The topic version is pinned.** The URL always names `v`, and
  `Scope::topology_filter` is the one place the spec's `TopologyFilter` is
  built, with `TopicVersionSelector::Pinned(v)`, so every linked view a
  page or route asks for (graphs, series, search, transmission rows,
  projections, export) reads the same version, and a cited view means the
  same thing after a re-fit or a new active version.
- A missing window defaults to the 24 hours before the backend's present
  (`Present::now`), its end snapped up and its start down to bucket
  boundaries, and a missing topic version to the active version of the
  history `topic_versions` returns (`pages::common::topics::default_version`);
  `pages::view::defaults` reads both through the backend's traits, so
  pages never ask the fixture directly.
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
  it), was never activated or is unknown shows the typed error
  (`VersionNotRetained`, `Conflict(TopicVersionNotActivated)`, `NotFound`)
  rather than silently using another version.
- Aggregate views show the response's watermark ("final up to 14:05"), so
  a cited view says whether its numbers can still change.

## Screens

Each screen lists the spec methods it calls: `QueryApi` methods by name,
actions as `OperatorAction` variants sent through `OperatorActions::act`,
and the gap traits' methods as `Present::now` and so on. Name lookups
(`agent_names`, `channel_names`) are one call per `IdBatch` of the ids a
page shows (`pages::common::lookup::id_batches`).

### Overview (`/`)

Counts from one `overview` call: the window's activity under the view's
filter (confirmed transmissions and matched bytes, and active channels:
the channels that carried a counted transmission) and the queues as of
the read (open alerts, unreviewed channels in force), each linking to its
section; the five heaviest edges of the view's `topology`, each opening
the topology with that edge selected; the five newest open alerts; and a
link into every section carrying the view state. The header shows the
overview's watermark.

- Calls: `overview`, `topology`, `alerts`, `agent_names`,
  `channel_names`, `alert_rules` (rule names), `operators`.

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
  `channels` and `topics` (filter choices), `agent_names`,
  `channel_names`, `Present::now` and `Present::bucket_width` (the
  brush's window).

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
withdraw, with a note. Recording the verdict already in force is
`Unchanged` ("nothing to change").

Without `Content` the header still renders (state in words without its
data, topic labels hidden), the matches and the co-access timeline say
they need `Content`, and the verdict log is shown: verdicts hold no
message content and need only `View`. The verdict form needs only
`Triage` (`SetVerdict`'s one permission: a verdict reveals no content);
without `Content` it says the matched text is hidden.

- Calls: `transmissions_by_id` (one id), `transmission_evidence`,
  `verdicts`, `topics`, `agent_names`, `channel_names`, `operators`.
- Actions: `SetVerdict` (`Triage`; offered only on judgeable states,
  `Conflict(TransmissionNotJudgeable)` otherwise).

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
`Content`. For `Govern`, the picker pins the shown version (a version
neither dropped nor fitting) or unpins a pinned one: a small form posting
to `/topics`, redirecting with a flash (`version-pinned`,
`version-unpinned`, `unchanged` when it already was), a refusal
(`Conflict(TopicVersionDropped)`, `Conflict(TopicVersionFitting)`,
`NotFound`) shown next to the picker.

- Calls: `topic_versions`, `topics`, `topic_sizes`, `series`,
  `topic_lineage`, `alert_rules` (a stale rule is one whose
  `StaleReason::TopicsUnmapped` names the lineage's target version and
  the topic).
- Actions: `PinTopicVersion`, `UnpinTopicVersion` (`Govern`).

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
confirm form with policy and note. Promotion keeps the channel's id: the
confirm redirects to the channel `ActionOutcome::ChannelPromoted` names,
with a flash saying how many discovered channels it superseded
(`promoted`, `promoted-<n>`).

- Calls: `channels`, `channel`, `channel_resources`, `policy_history`,
  `alerts` (filtered by channel), `operators`, `alert_rules` (rule names),
  `agent_names` (writer and reader names), `promotion_preview`,
  `channel_names`.
- Actions: `SetPolicy`, `PromoteChannel` (`Govern`; hidden on superseded
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
carry different harness ids in the same scope, and confirms the merge
built by `OperatorAction::merge_agents` for the caller (the first agent
becomes an alias of the second; one id twice is `InvalidInput(SelfMerge)`
before anything is sent or audited).

- Calls: `agents` (with the spec's `AgentFilter`, over the view's
  window), `agent` (over the view's window), `agent_names` (one call per
  `IdBatch`), `operators`.
- Actions: `MergeAgents`, `Unmerge`, `RenameAgent` (`Govern`; renaming to
  the current label is `Unchanged`). Both agents of a merge must be canonical
  (a merged one is `Conflict(AgentMerged)`; the comparison resolves a
  pasted alias to its canonical agent before confirming), two ids of one
  cluster are `Conflict(MergeIntoSelf)`, one id twice
  `InvalidInput(SelfMerge)`; an unmerge reverts one record
  (`Conflict(MergeAlreadyReverted)` the second time); renaming a merged
  agent is `Conflict(AgentMerged)`.

### Alerts (`/alerts`, `/alerts/{id}`, `/alerts/rules`, `/alerts/rules/new`, `/alerts/rules/{id}`)

An inbox with one tab per state (`tab=open|acknowledged|resolved|
suppressed`, the spec's `AlertStateKind`), newest alert first, showing
rule name, subject link (channel, agent or transmission page),
occurrences, raise time, who moved the alert to its state (and the
resolution note or suppression reason: channel sanctioned, rule disabled,
or rejected as a false detection). Open alerts can be acknowledged; open
and acknowledged ones resolved with a note (`Triage`). When the shared
filter names exactly one channel, the inbox is narrowed to it
(`AlertFilter::channel`: the subject's channel or the transmission's
route resolved through supersession). Each alert id links to the alert's
page (`/alerts/{id}`): rule (its edit page, or the rules list for a
built-in), subject link, raise time, occurrences, state with who and
when, the acknowledge and resolve forms, and its history from the audit
log (subject `al.<id>`; reading it needs `Audit`, and without it the
section says so). The rules page lists every rule (`alert_rules`
followed to its last page): built-in rules by their spec names (enable or
disable), operator rules (what they match, status, staleness with its
reason in words, author, sinks; edit, enable, disable, or "Update" when
stale, with "Disable" still offered on a stale rule that is enabled:
status and staleness are separate) and, for `Govern`, sinks with their
last delivery (`sinks` needs `Govern`; others are told so). A rule listing
no sink delivers to none ("inbox only"). Watched-topic rules pick topics
of the current topic version (labels need `Content`; a blank remap
threshold takes the configured default). Semantic query rules are text
(`NonBlank`): the form submits the spec's `UserRule` and the backend
embeds it, so editing the text re-embeds it; text too long for the
embedder is `InvalidInput(QueryTooLong)`. Rule names are the spec's
`RuleName` (`DisplayText<80>`), refused inline.

- Calls: `alerts`, `alert`, `alert_rules`, `sinks` (`Govern`),
  `topic_versions`, `topics`, `operators`, `audit` (an
  alert's history).
- Actions: `Acknowledge`, `Resolve` (`Triage`; a repeat of the
  transition the alert already took, acknowledging an acknowledged alert
  or resolving a resolved one, is `Unchanged` and keeps who did it first;
  acknowledging a resolved or suppressed alert, or resolving a suppressed
  one, is `Conflict(AlertNotActive)`), `CreateRule`, `UpdateRule`,
  `SetRuleEnabled` (`Govern`; an update or switch that changes nothing is
  `Unchanged`).

### Research and pipeline

- **Export** (`/export`): pick dataset (a projection by id), topic
  version and format over the view's window and filter; including content
  needs `Content` (the checkbox is disabled without it). `POST /export`
  validates the form into the spec's `ExportRequest` (transmissions,
  edges, accesses and topics take the view's window and filter pinned to
  the chosen version; verdicts the window alone; a projection its id;
  content refused for accesses and verdicts, which have none
  (`InvalidExportRequest::NoContentColumns`), and a projection or content
  needing `Content`), calls `export` and answers with the JSON Lines
  download (`application/x-ndjson`, `Content-Disposition: attachment;
  filename="crosstalk-<dataset>-<export id>.jsonl"`; see [Export
  downloads](#export-downloads)). The body is read from the stream to its
  trailer before it is sent (a Topcoat 0.9 route answers with a whole
  body; streaming would need `http_body`). A refused post (invalid field,
  a format the backend does not write, a missing permission,
  `Conflict(ExportTooLarge)`, a projection not ready, an unknown one) is
  rewritten to `GET /export` with the error and the submitted fields, and
  the page shows the error with the input kept under the error's status
  (422, 403, 404, 409). Formats the backend does not write
  (`contract::formats::ExportFormats`; the fixture writes JSONL only) are
  listed disabled, "not available on this backend". Every export is
  audited (refused, started, finished or abandoned) and shows on `/audit`.
  Below it,
  `detection_quality` for the window (the spec's `DetectionQuality`: every
  judgeable transmission opened in it) as route × detector call (confirmed
  by its strongest match class, suspected, discarded) with genuine, false
  detection, unlabelled and precision (genuine over labelled, for
  confirmed rows) and a totals row (precision over the confirmed rows).
  Calls `topic_versions`, `export`, `detection_quality`,
  `ExportFormats::export_formats`.
- **Audit** (`/audit`, needs `Audit`; without it the page says so, 403):
  the spec's `AuditEntry`s, newest first and paged: operator calls, config
  changes and exports, each with its actor (the operator's name, or
  "config"), a one-line description (config changes and export events
  described in words: a declared channel, a registered agent, a
  provisioned rule, the access mode, an operator defined or removed; an
  export asked for, started, finished or abandoned), note, every subject
  the entry touched (`AuditEntry::subjects`: what the action named, then
  what its outcome created or superseded; each linked to its page, or to
  the log filtered to it for merge records, operators and exports, and
  with its own "filter" link) and outcome: applied (with what it created:
  a rule, a merge record, a promoted channel and the channels it
  superseded, an export started), unchanged, rejected with the typed
  error, or forbidden with the permission the caller lacked. Filters: `op`
  (an operator, or `config` for config changes: `AuditFilter::by`),
  `subject` (`ch.`/`ag.`/`tx.`/`ru.`/`al.`/`mg.`/`op.`/`ex.`/`pj.` plus the
  id, or `tv.<n>` for a topic-model version) and `span=all` (otherwise the
  shared window). Subjects match as recorded (`AuditFilter::matches`): an
  agent finds the merges it took part in, a superseded channel the
  promotion that superseded it, but ids are not resolved through merges
  or supersession. The author choices list every operator from
  `operators`, former ones marked. Calls `audit`, `operators`.
- **Pipeline** (`/pipeline`): dead letters, newest envelope first
  (consumer group, event id, kind and time, attempts, last error) with
  replay, optionally of one consumer group (`group`: each row's group
  links to it, a chip clears it; a replay returns to the same group).
  Needs `Operate` to view. Calls `dead_letters`; action
  `ReplayDeadLetter` (`Operate`).

## Data and control flow

1. A request reaches a page. `app::caller` gives the `Caller` the spec's
   `OperatorDirectory` gave the configured operator (`config::Access`).
   The page parses `ViewState` from the query, with defaults read from
   the backend (`Present::now`, `Present::bucket_width`,
   `topic_versions`); an incomplete or unaligned URL is redirected to its
   canonical form.
2. The page calls `QueryApi` on `app::AppBackend` with the `Caller`, the
   scope's aligned window and its filter pinned to `v`
   (`Scope::topology_filter`), and renders. Content fields are rendered
   only if the caller has `Content`; a `QueryError` is rendered as a
   `UiError` through `error::describe`.
3. Element tags are rendered with their inputs as `data-*` attributes
   (including the `/data/` URL for their payload, which carries the same
   `ViewState` query).
4. The element fetches its payload from `/data/...`; the route rebuilds
   the `Caller` and parses the `ViewState` strictly (no redirect), calls
   `QueryApi`, and maps errors to statuses (`data::errors::status_of`).
5. User interaction in an element sets its `value` and fires `change`; a
   page signal takes the value; shards that read the signal re-render on
   the server; the URL is updated.
6. Operator actions are HTML form posts to the page's own path (with the
   view state in the action URL). The handler validates every field into
   typed values (`pages/common/form.rs`) and builds the spec's
   `OperatorAction`; `pages::common::action::perform` checks its one
   permission (`OperatorAction::required_permission`) and calls
   `OperatorActions::act`. Success redirects
   (303) back to the page with a `flash` code (never text, so a link
   cannot make a page say something else; `unchanged` when the outcome is
   `ActionOutcome::Unchanged`); failure renders the same page with the
   typed `ActionError` (as a `UiError`) next to the form that failed, the
   submitted input kept, under a status that follows the error (422
   invalid input, 403, 404, 409 conflicts).
7. Live updates ([below](#live-updates)): `<ct-live>` in the layout
   subscribes to `GET /data/live` (`LiveFeed::subscribe`, server-sent
   events of `UiEvent`s, ids only); when an event names something the
   page declared it shows, the page's region is rendered again and
   swapped.

### Live updates

```text
act / fit_projection (write lock held)
  ─▶ actions::changes: the Changed each store publishes ─▶ Feed::publish
       ─▶ FeedLog (epoch, seq; LiveConfig::retention) and broadcast (LiveConfig::buffer)
GET /data/live (View; Last-Event-ID ─▶ Resume::from_last_event_id)
  ─▶ subscribe: FeedWindow::resume ─▶ Resync | replay | live, filtered by UiEvent::visible_to,
     heartbeats every LiveConfig::heartbeat, Lagged when the stream's buffer overflows
  ─▶ SSE: id = LiveCursor::encode, event = kind, data = {"id"} | {"version"} | {"at"}
<ct-live data-src="/data/live"> (root layout, outside the page region)
  ─▶ event matches a [data-live-watch] token of the page, or resync
  ─▶ (after 250 ms of quiet) Topcoat's page runtime renders the page again with
     its current signal values; [data-live-region] elements are swapped and
     the runtime hydrates the document again
  ─▶ otherwise a "this page's data changed" notice with a Reload button
```

- **What is published.** The fixture publishes, after each committed
  change and before releasing its write lock, the spec's `Changed` its
  stores would (`backend::fixture::actions::changes`): a policy decision's
  channel; a promotion's channel and every channel it superseded
  (`Changed::promotion`); a merge's source, target and repointed agents;
  an unmerge's source, former target and restored agents; a renamed
  agent; a judged transmission (`Verdict`); a created, updated, enabled or
  disabled rule; every alert whose state changed (acknowledge, resolve,
  and the suppressions a sanction, a disabled rule or a false detection
  makes); every topic version whose catalog entry changed (pin, unpin and
  the retention drop an unpin runs); a fit's projection job (ready or
  failed). Refused and `Unchanged` calls publish nothing; dead-letter
  replays publish nothing (not a feed entity). The fixture's watermark
  never advances, so it publishes no `Watermark`, and its trusted session
  never ends, so no stream ends `SessionEnded`. Limits: buffer 256,
  heartbeat 15 s, retention 15 minutes. The epoch comes from the clock
  when the backend is built, so a restarted fixture (a new world) makes
  old cursors resync.
- **The wire** (`ui/src/data/live.rs`): one SSE event per `LiveItem`,
  `id:` its cursor (`<epoch>-<seq>`); `event:` `alert`, `channel`,
  `agent`, `rule`, `verdict`, `projection` with `data: {"id": "<ulid>"}`,
  `topic-version` with `{"version": n}`, `watermark` with `{"at": …}`,
  `resync` with `{"reason": "expired" | "other-epoch" | "ahead-of-head" |
  "unreadable"}`, `heartbeat` with `{}`. When the stream ends (`LiveEnd`)
  a last `event: end` with `{"reason": "lagged" | "session-ended" |
  "shutting-down"}` (no id) is sent and the response closes; the browser's
  `EventSource` reconnects with its last id. Built on Topcoat's `sse`
  feature (`Sse`, `Event`, `last_event_id`), which takes a
  `futures_core::Stream` (hence the `futures-core` dependency, the
  version already in the lock file).
- **What pages watch.** The root layout renders `<ct-live>` (for callers
  with View) and wraps the page in `data-live-region="page"`; a page
  declares what it shows with `components::live::live_watch` (a hidden
  `data-live-watch` marker of space-separated tokens: a kind alone, or
  `kind:<id>`). Overview: `alert channel watermark`; alerts inbox: `alert
  rule`; an alert: `alert:<id> rule`; rules: `rule`; channels: `channel`;
  a channel: `channel:<id> alert`; agents and an agent: `agent` (a merge
  elsewhere can re-point the cluster's ids); a transmission: `verdict:<id>
  alert`; topics: `topic-version rule`. Topology, explore, export, audit
  and pipeline declare nothing: the first two hold WebGL elements a swap
  would rebuild, and audit entries and dead letters are not feed entities.
- **The refresh** (`ui/elements/src/live/refresh.ts`) uses the hook
  Topcoat 0.9's page runtime registers on every page for its dev refresh
  (window event `topcoat:dev-runtime:v1`: `request` renders the page again
  with the browser's signal values, `replace` releases bindings, runs the
  update and hydrates again). It is not a documented public API, so the
  element falls back to its notice when the runtime does not answer, the
  response is not the page, a region is missing, or a region holds a form
  control the user is editing (focused, or changed from its default). It
  has unit tests for token and event parsing only; the swap itself has
  not been exercised in a browser.

## Element payloads

Each element reads one route under `/data/`. The Rust types in
`ui/src/data/` define the payloads (each documented in its doc comment);
`ui/elements/src/payloads/` mirrors them with zod schemas and a binary
decoder. `ui/src/data/fixtures/` writes the payloads of hand-built
spec values to `ui/elements/test/fixtures/` and fails when they drift
(`CT_UPDATE_FIXTURES=1 cargo test element_fixtures` regenerates them), so
the TypeScript tests parse exactly what Rust emits.

| Route | Payload | Needs | Spec calls |
| --- | --- | --- | --- |
| `GET /data/topology?<view state>` | JSON `TopologyPayload` | `View` | `topology` (`g=agents`) or `channel_topology` plus one `channel_names` (`g=channels`) |
| `GET /data/timeline?<view state>&buckets=<n>` | JSON `TimelinePayload` | `View` | `series` twice (transmissions, matched bytes; `Total`) on one grid built from `Present::bucket_width`, `n` in 1..=1000, default 96 |
| `GET /data/live` (`Last-Event-ID`) | server-sent events, `text/event-stream` ([Live updates](#live-updates)); read by `<ct-live>` | `View` | `LiveFeed::subscribe` |
| `GET /data/projection/{id}` | binary, `application/octet-stream` | `Content` | `projection`; `transmissions_by_id` for the channels of channel-routed points (the spec's frame has none); `agent_names`, `channel_names` (each one call, chunked at `IdBatch::MAX`) and `topics` for names |

- **No redirects.** A view-state route needs every canonical key (`from`,
  `to`, `v`, `w`, `g`); a missing or invalid one, or a window not on
  bucket boundaries, is a 400 naming it. Backend errors
  (`data::errors::status_of`): `Forbidden` 403; `NotFound` (an unknown
  topic version too) and `ProjectionNotRetained` 404;
  `VersionNotRetained`, `InvalidInput`, `InvalidCursor`, a linked view's
  version conflicts (`TopicVersionFitting`, `TopicVersionNotActivated`,
  `TopicsNotInVersion`) and a projection not ready or failed
  (`ProjectionNotReady`, `ProjectionFailed`) 400 with the
  `error::describe` message; anything else 500 (logged).
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

## Export downloads

`POST /export` answers with one JSON object per line
(`ui/src/pages/export/jsonl/`). Ids are ULID text, times RFC 3339 in UTC,
routes the URL route text (`ch.<ulid>`, `dl.p2c`, `dr.tool.<name>`, …),
route kinds `channel`/`delegation`/`direct`/`unobserved`, windows `{start,
end}`. Content columns are `null` when the request did not include
content.

| Line | Shape |
| --- | --- |
| first | `{"type": "header", "export", "dataset", "selection": {"window", "filter"} \| {"projection"} \| {"window"}, "format": "jsonl", "include_content", "by", "started_at", "watermark", "basis": {"kind": "scoped", "topic_version", "filter", "settled"} \| {"kind": "verdicts", "settled"} \| {"kind": "projection", "projection", "window", "filter", "topic_version", "params": {"limit", "neighbors", "min_dist", "seed"}, "embedding_model", "fitted": {"started_at", "fitted_at", "watermark", "matching", "points"}}, "embedding_model": {"name", "dimension"}, "gateway", "rows"}`. A filter is `{"agents", "channels", "route_kinds", "topics", "topic_version": <n> \| "current", "false_detections": "include" \| "exclude"}`; `settled` is the window cut at the watermark, or `null`. |
| transmissions row | `{"type": "row", "dataset": "transmissions", "id", "from", "to", "route", "route_kind", "opened_at", "confirmed_at", "state": "confirmed" \| "classified" \| "aggregated", "matched_bytes", "topic": {"topic"} \| "outlier" \| "unassigned" \| null, "verdict": "genuine" \| "false_detection" \| null, "strongest": "exact" \| "normalized" \| "decoded" \| "semantic", "content": null \| {"topic_label", "matches": [{"class", "origin": <quote>, "read": <quote>}]}}`; a quote is `{"status": "shown", "text", "highlight": [start, end], "elided_before", "elided_after", "highlight_cut"}` (the matched range alone, at most 8 KiB) or `{"status": "body dropped", "message": <body hash hex>}` |
| edges row | `{"type": "row", "dataset": "edges", "bucket", "from", "to", "route", "route_kind", "topic", "transmissions", "matched_bytes", "topic_label"}` |
| accesses row | `{"type": "row", "dataset": "accesses", "bucket", "agent", "channel", "op": "write" \| "read", "accesses"}` |
| topics row | `{"type": "row", "dataset": "topics", "topic", "transmissions", "matched_bytes", "label", "terms": [[term, weight]]}` |
| projection row | `{"type": "row", "dataset": "projection", "index", "transmission", "from", "to", "route_kind", "topic", "confirmed_at", "x", "y", "topic_label"}` |
| verdicts row | `{"type": "row", "dataset": "verdicts", "transmission", "revision", "route_kind", "call": {"kind": "content", "class"} \| {"kind": "suspected" \| "discarded"}, "verdict": "genuine" \| "false_detection" \| null (a withdrawal), "by", "at", "note"}` |
| last | `{"type": "trailer", "export", "rows", "digest": <64 hex>, "end": {"status": "complete"} \| {"status": "failed", "failure": {"kind": "store" \| "version_not_retained" \| "count_mismatch" \| "invalid_row", …}}}` |

Rows are in the spec's `RowKey` order. The digest is over the spec's
canonical row encoding, not these bytes; the fixture's hash is a stand-in
for BLAKE3 (`backend::fixture::export::digest`), so a fixture digest
checks a fixture export only.

## Files

| Path | Role |
| --- | --- |
| `ui/Cargo.toml` | The `crosstalk-ui` package. Pins `topcoat = "=0.9.0"` and every other dependency exactly. `futures-core` (the version Topcoat already pulls in) names the `Stream` trait Topcoat's `Sse` response takes. |
| `ui/build.rs`, `ui/styles/app.css` | Tailwind 4.3.3 (checksum-pinned on linux-x64) rendered from classes in `src/`. Route-kind colours are theme tokens shared with the elements. |
| `ui/config.json` | Listen address, trusted operator, backend choice (`fixture { seed }`). `CROSSTALK_UI_CONFIG` overrides the path. |
| `ui/src/main.rs` | Loads config, builds the router (pages, app context, assets, runtime) and serves. Raises `recursion_limit` to 256: pages embedding shards nest component futures past the default depth for the `Send` check. |
| `ui/src/app.rs` | `AppBackend` (the configured backend type: `FixtureBackend`, an enum over implementations once there is more than one), `backend(cx)`, `caller(cx)` (the request's `Caller` from `config::Access`; shards and procedures call it themselves), `access(cx)`, `can(caller, permission)` (`Caller::has`). |
| `ui/src/config.rs` | `Config` (listen address, `Access`, `BackendConfig`; `from_env`, `load`), `Access` (built only by `Access::trusted`: the spec's `OperatorDirectory` loaded from `AccessConfig::Trusted` for the configured `TrustedOperator`, the caller it gives a request, the operator's name), `AccessError`, `ConfigError` (typed: read, parse, operator id, operator name, access). |
| `ui/src/error.rs` | `UiError`: `Query(QueryError)` (the spec's error; `From<QueryError>` and `From<ActionError>` through `QueryError::from`) or `Field { field, reason }` (a value the UI refused before calling anything). `describe` words every `QueryError` (the spec's errors carry no text); `permission_name`; `rejection` (a recorded audit `Rejection` as the error it stands for); `export_failure` and `fit_failure` in words. |
| `ui/src/contract/` | The two gap traits, each deleted when the spec gains what it declares ([Remaining gaps](#remaining-gaps)): `present` (`Present::bucket_width`, `Present::now`) and `formats` (`ExportFormats::export_formats`). Everything else the UI reads or sends is the spec's. |
| `ui/src/backend/mod.rs` | The module's docs (pages read through `QueryApi`, act through `OperatorActions`, subscribe through `LiveFeed`, and use the two contract gaps, all implemented by `FixtureBackend`), `Result<T>` (the spec's `QueryError`), `fixture`, and `alert_state`: an `AlertState`'s `AlertStateKind` (`kind`) and whether it is active (`is_active`), which the spec does not say. |
| `ui/src/backend/fixture/` | `FixtureBackend::try_new(seed)` (fails only on a fixture bug; `main` stops then): a deterministic synthetic world (same seed, same world and answers; generated in well under a second), implementing `QueryApi`, `OperatorActions`, `LiveFeed` and the two contract gaps with the spec's semantics. `identity` is the merge table (spec `Agent`s, `MergeRecord`s, `MergeVeto`s behind private fields, changed only as `IdentityResolver` defines: `merge` refuses unknown agents, then `MergeRequest::conflict` (`MergeIntoSelf` first, then `AgentMerged`), then a resolver merge a veto separates, an operator merge deleting those vetoes, the source merged away with its prior state and the agents merged into it repointed; `unmerge` reverts one record through `Agent::revert` and `Agent::restore`, records its `Reversal` and a `MergeVeto::of` it, `MergeAlreadyReverted` the second time; `rename` through `Agent::rename`; refusals mapped by `ActionError::from(ResolveError)`). `world/` generates it through the spec's checked constructors: `agents` (cast; the planned merges and the revert replayed through the merge table; claims recorded per agent as `ClaimSet`s; activity seeded with each traffic-created agent's first exchange), `drafts` and `channels` (channel table; the policy decisions table, built into each channel's checked `PolicyHistory`; detection from traffic; the team-notes promotion applied through `promotion::plan`, the superseded channel's detection frozen at the promotion and later confirmations on it advancing the promoted channel's), `traffic` and `states` (transmissions and accesses), `evidence` (matches, with the origin span's and the read's locations into stored bodies), `blobs` (span records and the blob store: compact bodies built into the spec's `Message` on read, so `Message::part_text` indexes the generated text; `drop_body` for retention), `retention` (the dropped-bodies scenario), `topics` (the model, its topics and assignments) and `catalog` (the spec `TopicVersionHistory` with v1 pinned and v0 dropped by a `RetentionPolicy` keeping the last two activated versions, and each `TopicLineage` with its best link and the others above a 0.6 floor), `rules` (the spec `SinkInfo`s; the `AlertRuleConfig`; an `AlertRuleSet` whose built-ins deliver to every sink, user rules resolved and stored as `CreateRule` stores them as of their creation, the v1 rule carried to v2 by `AlertRuleDef::remap` over the stored lineage, so its `TopicsUnmapped` is exactly `TopicLineage::remap`'s), `alerts` (spec `Alert`s; the refund rule disabled through the same `set_enabled` as the action), `config` (the spec `OperatorDirectory` loaded from an authenticated `AccessConfig` with the researcher (every permission) and the on-call operator; `caller` gives each historical call its `Caller`; the config audit entries: the directory load's `ConfigChange`s, the declared channels, registered agents and built-in rules of the first document, and the design-docs declaration of a later one, each document its own `ConfigHash`), `history` (every past operator call as an `OperatorRecord` with `AuditOutcome::of` its result: the policy decisions and the promotion (with what it superseded), merges and the revert, renames, triage, verdicts (`Unchanged` for a repeat), and the two refused calls (`Forbidden`, `Rejected`)), `letters` (the four dead letters). `store` holds what actions change behind one `tokio::sync::RwLock` (the topic catalog, a spec `TopicVersionHistory` that pins change; channels are `ChannelRecord`s: a spec `Channel`, supersession in its origin, with its `PolicyHistory`, the policy always the history's current one; agents, merges and vetoes are the `Identity` merge table, a merged agent keeping its prior state in `MergedInto`, verdicts are one spec `VerdictLog` per judged transmission, alerts are spec `Alert`s and rules the spec's `AlertRuleSet`, the audit log is `audit::AuditLog`: spec `AuditEntry`s, append-only (no update or delete; `append` idempotent on the id, `IdReused` otherwise), projection jobs are `Job::Ready(Projection)` or `Job::Record(ProjectionInfo)` for every other status); `queries/` resolves merged agents and superseded channels at read time and pages with keyset cursors (`linked`: the one place a linked view resolves its filter's `TopicVersionSelector`, against the store's catalog, a spec `TopicVersionHistory` (unknown `NotFound`, never activated `Conflict(TopicVersionNotActivated)`, not retained `VersionNotRetained`, foreign topics `Conflict(TopicsNotInVersion)`), and counts confirmed transmissions by `Confirmed::at` admitted by `TopologyFilter::admits`, accesses by `admits_access`; `graph`: `topology`, `channel_topology` and `overview` (`EdgeTotals::of` the graph, queues as `QueueCounts::tally` defines them) on bucket-aligned windows, checked by `TopologyGraph::check_nodes` / `BipartiteGraph::new`; `nodes`: agent nodes (spec label, canonical state, parent chain and claims as `agents::profile` reads them) and channel nodes (`CanonicalOriginKind`, `DetectionKind`, `PolicyKind`, `locator_summary`); `series`: `EdgeStore::series` on the fixture's bucket width; `topics`: the catalog's reads failing as `CatalogError` maps (the history; `TopicSizes` from the assignments of transmissions confirmed in the window, a dropped version's frozen at its drop and refused with a window; the lineage; a version's topics newest id first, the cursor pinning the version); `projection`: `fit_projection` (version resolved as a linked view, `MAX_PENDING` jobs, a queued `ProjectionInfo` run at once through `start` and `complete` or `fail`), the store's reads (`ProjectionStoreError` mapped by the spec), `sample` (bottom-k of the admitted confirmed transmissions by a seeded SplitMix64 key standing in for the spec's BLAKE3, the frame built with `ProjectionFrame::from_points`, a stand-in layout of theme clusters; too few points fail with `TooFewPoints`) and `seed` (the world's jobs: expired, failed when v0 was dropped while queued, fitting and queued; frames are kept `FRAME_RETENTION`, three days); `transmissions`: `edge_transmissions` (what `linked` counts into the edge, newest confirmation first, `Watermarked`), `transmissions_by_id` (`TransmissionSummary::of` rows, newest id first, unknown ids left out) and `quality` (`DetectionQuality::tally` over every transmission with its current verdict), each traversal pinning its topic version in its cursors (`page::versioned`, `page::pinned`; a pinned version no longer retained is `VersionNotRetained`); `evidence`: `transmission`, `transmission_evidence` (`TransmissionEvidence::assemble`, excerpts by `Excerpted::of` from the span records and the blob store; a missing record is `Store`) and `verdicts`; `search`: a linked view over confirmed transmissions (optional window, filter before ranking, deterministic stand-in scores); `agents`: `list` (canonical `AgentProfile`s admitted by `AgentFilter::matches`, newest agent first, cursors bound to the filter only, unaligned windows refused), `one` (an `AgentCluster`: aliases, children, the merge records and vetoes naming the cluster, `AgentLookup::Redirected` for an alias) and `names` (`AgentName::of` the canonical agent, keyed by the id asked for), `profile` (canonical parent, `ClaimSet::union` over the cluster, last seen over the cluster, shared with graph nodes; `traffic`: the agent nodes' counts of the default-filter topology for the window, `EdgeStore::agent_traffic`); `channels`: the registry as `promotion::Registered` entries, `rows` (`ChannelRow`s newest first, counts as `ChannelCounts::tally` of the resource use and `ChannelCounts::routed` of the default-filter graph for the window, all time as `clock::all_time`, unaligned windows refused, superseded rows with their `SupersededInto`), `resources` (`ResourceUse` pages, newest resource first, through the canonical channel, cursors bound to channel and window), `names` (`channels::resolve_names`), `policy_history` and `coverage` (`promotion::coverage` for `promotion_preview`); `alerts`: `alerts` (newest id first; the channel filter compares `AlertSubject::resolved` subjects and resolved routes with the channel in force), `alert`, `alert_rules` (built-ins in `BuiltinRule::ALL` order, then user rules newest first, `AlertRuleFilter::matches`); `lists`: `audit` (exactly `AuditFilter::matches`, newest first by `(at, id)`, the cursor bound to the filter) and `dead_letters` (one group or all, newest envelope first, the cursor bound to the group)); `actions/` applies and audits every action as `OperatorActions::act` defines it (the one `required_permission` checked before any effect, `Forbidden` naming it; author and time stamped from the caller and the acceptance time (`Stamp`); every call appended as one `OperatorRecord` (the caller as authenticated, the action, `AuditOutcome::of` the result); refusals the spec's `ActionError`, mapped with the spec's `From` impls where it has them (`ResolveError`, `PromoteError`, `RuleError`, `SelfMerge`) and as each action documents otherwise (`pins::refusal` for `PinError`, `TransmissionNotJudgeable`, `ChannelSuperseded`); `Unchanged` where the state already matched: a store's `Change`, a duplicate policy decision, the verdict in force, an acknowledged alert acknowledged or a resolved one resolved, a pinned version pinned or an unpinned one unpinned; a policy decision is recorded in the channel's history, a superseded channel refused; a promotion follows `promotion::plan`, keeping the channel's id, recording its decision and superseding the covered channels, and returns them as `SupersededChannels`; `pins`: `TopicVersionHistory::pin`/`unpin` on the store's catalog, an unpin followed by retention; sanctioning suppresses the active alerts about the channel and the channels it superseded; subject and outcome recorded on the entry; a verdict is a `TransmissionVerdict::new` appended with `VerdictLog::record`, a repeat appending nothing, and an appended false detection suppresses the transmission's active alerts with `OperatorRejected`; rules follow `AlertRuleStore`: `rules::resolve` checks sinks (`UnknownSink`), a watched-topic rule's version (unknown `UnknownTopics`, not current `TopicVersionNotCurrent`) and topics (`UnknownTopics`), fills a missing remap threshold from the config and embeds a query (`EmbedError` → `QueryTooLong`), a built-in update is `RuleNotEditable`, `AlertRuleDef::update` retargets and enables a stale rule, `set_enabled` refuses enabling a stale rule (`RuleStale`) and disabling suppresses the rule's active alerts, all mapped by `ActionError::from(RuleError)`; acknowledging an acknowledged alert or resolving a resolved one changes nothing (`Unchanged`), acknowledging a resolved or suppressed alert or resolving a suppressed one is `AlertNotActive`); `surface` implements `QueryApi`, `OperatorActions`, `LiveFeed`, `Present` and `ExportFormats` over the modules below (`mod.rs` holds the struct, `try_new` and test handles); `live/` is `LiveFeed` (`Feed`: the log (`log`: entries numbered in one epoch, dropped after the retention) behind a `tokio::sync::RwLock` and a `broadcast` fan-out of `LiveConfig::buffer` entries; `publish` appends and sends without waiting; `subscribe` checks View, plans with `FeedWindow::resume` and joins the fan-out under the log's lock; `stream`: `FeedStream`, a `LiveStream` returning the planned resync or replay, then live entries `visible_to` the caller (passed-over ones still advance its cursor), a heartbeat every `LiveConfig::heartbeat`, and `Lagged` or `ShuttingDown` once and for good), fed from `act` (`actions::changes`: the `Changed` each store publishes, from the action and its outcome plus the alerts and topic versions whose state changed) and `fit_projection`; `export/` is `QueryApi::export` (`mod`: the permission first, a Parquet export refused with `Store` (the fixture writes JSONL only, `FORMATS`), the watermark, the plan, `ExportLimits::check` against `MAX_ROWS` (5,000: a week of transmissions or edge buckets fits, a week of access buckets does not), the checked `ExportHeader`, everything under the write lock; every call audited as an `ExportRecord`: `Refused(error)`, or `Started` before the header is returned; `plan`: `Snapshot` implements the spec's `ExportSource` over one `Ctx` (resolution and verdicts captured), resolving the version as a linked view, cutting the window with `settled_window`, refusing unaligned edge and access windows, reading a projection through `queries::projection::stored`, and reading every row up front into `PlannedRows` (a `RowSource`); `rows`: transmissions (`Linked::admitted`, `TransmissionRow::of`, content from the evidence cut with `ExcerptWindow::MATCH_ONLY`), edge and access buckets, topics (zero counts included), `projection_rows`, `verdict_rows`, each sorted by `RowKey`; `stream`: `ExportRows`, the spec's `SealedRows` with a ledger that appends `Ended(trailer)` when the trailer is yielded and `Abandoned { rows }` when the stream is dropped first; `digest`: `RowDigest`, four FNV-1a lanes keyed by `ROW_DIGEST_CONTEXT`, a stand-in for BLAKE3); `world/topics` also holds the fixture's text embedder (`embed`: theme vectors weighted by vocabulary hits plus hashed word axes, text over `QUERY_CONTEXT_CHARS` refused as `EmbedError::TooLong`), used for `CreateRule`/`UpdateRule` and the generated semantic rules; `text/` holds the message templates and codecs; `rng` is SplitMix64; `tests/` holds the fixture's tests by area (world, graph, series, lists, transmissions, topics, projections, channels, promotion, agents, rules, triage, governance, outcomes, audit, export, live, scenarios). The world: 7 days ending at `now()` (2026-10-03T00:00Z, watermark ten minutes earlier), about 5,000 transmissions on a weekday daytime curve in every state (in-flight states sit in the last quarter hour) and every route, match kind (decode chains such as base64 → url) and carrier. 40 canonical agents across Claude Code, Codex, pi, oh-my-pi and self-hosted scripts, with sub-agents, three config-registered agents with no traffic, and labels. Scenarios: pi and oh-my-pi agents labelled `pi-scraper` and `omp-orchestrator` (and two unlabelled ones) also claim Claude Code; `atlas-lead` has a resolver-merged alias whose traffic to it becomes a dropped self-edge; one pi agent holds two aliases, one repointed by a later merge; an operator merge was reverted and left a veto (the oh-my-pi agent with a veto on its page). Channels: declared sanctioned `wiki.corp.internal/eng`, `git.corp.internal/platform/monorepo` and `issues.corp.internal` (active); `docs.corp.internal/design` awaiting traffic; `nfs-01:/mnt/shared/releases` unused, with an open sanctioned-unused alert; the hijacked public wiki is the discovered, unreviewed, active channel seeded at `wiki.example.org/wiki/Agent_Coordination` (injection-style text, the busiest channel), with its talk page as a second discovered channel the same `UrlPrefix` pattern covers; `paste.example.net` unsanctioned; the `memory` MCP server reset to unreviewed; `/tmp/agent-handoff` on `devbox-3` sanctioned; `gist.example.com` dormant; a `kv_put` tool only one agent uses (observed); an `s3://agent-scratch` prefix with only suspected traffic (candidate); `notes.corp.internal/team-a`, discovered at its retro page and promoted by the researcher with the `/team-a` prefix (sanctioned, same id), whose promotion superseded the discovered `notes.corp.internal/team-a/standup` (detection frozen at the promotion). Policy histories: the config declarations, the `memory` server sanctioned then reset, the pastebin unsanctioned, the handoff directory sanctioned, the promotion's decision. Confirmed transmissions not yet classified have no topic. Topics: v0 (unfitted, once active, dropped by retention when v2 was activated), v1 (six topics, fitted six days ago, pinned) and v2 (ten, two days ago, active); v1's "Engineering chatter" has no link at or above the 0.8 remap threshold in v2, so its watched-topic rule is stale. Projections: four seeded jobs (expired, failed, fitting, queued); every fit adds a ready (or failed) job. Rules: the five built-ins (fixed ids 1 to 5, every sink), a watched-topic rule on v2 (credentials and agent instructions, the configured remap threshold), a v1 rule stale (and still enabled) with `TopicsUnmapped` (no sink), a semantic query on paste sites (with an agent-subject alert) and a disabled refund rule; sinks soc-webhook (last delivery failed), #agent-alerts, local-log. About 650 alerts in every state and suppress reason, deduplicated occurrence counts on channel alerts. Two operators in an authenticated `OperatorDirectory`: `researcher` (the trusted operator in `config.json`, every permission) and `oncall` (view, content, triage), with the config entries that defined them; about 60 verdicts (one withdrawn), the oldest six confirmed transmissions with their sender's or reader's message bodies dropped by content retention (alternating; `Scenario::dropped`), a few hundred audit entries including two refused actions (the on-call operator's forbidden policy change and its acknowledgement of a resolved alert) and the config changes, and four dead letters (one per consumer group). |
| `ui/src/url/` | `ulid` (Crockford text for every id), `route` (URL text for `Route` and `RouteKind`), `scope` (`Scope`: the window, pinned topic version and `ViewFilter` of a view; `Scope::topology_filter`, the one place the spec's `TopologyFilter` is built, pinned to the scope's version; `align_down`, `align_up`, `is_aligned` on a `BucketWidth`), `view_state` (`RawViewState` → `ViewState` and back to the canonical query; `Defaults` with the bucket width; `Parsed` with `complete` and `aligned`, an unaligned window snapped outward). |
| `ui/src/pages/` | `mod.rs` (root layout; navigation links carry the current view state when the request has a complete one; `<ct-live>` and its script for callers with View, and the page inside `data-live-region="page"`), `view.rs` (`defaults(cx)`: the default window, the 24 hours before `Present::now` on bucket boundaries, the bucket width from `Present::bucket_width` and the active version of `topic_versions`; `defaults_error`; async `view_state(cx)`: parse, default, redirect to canonical (an unaligned window to its snapped form); async `current_state(cx)` for the layout; async `state_from_query(cx, query)`: a shard's view-state argument, parsed strictly), one module per screen. |
| `ui/src/pages/common/` | Shared by the pages. `action` (`perform`: `required_permission` checked, then `OperatorActions::act`, its `ActionError` a `UiError`; `settled`: `Flash::Unchanged` for an `Unchanged` outcome; `done`: 303 with flash; `Failure<F>` and `error_for`/`fields_for` to show an error next to its form; `status_of`), `flash` (`Flash` codes and messages; `ChannelPromoted { superseded }` as `promoted` or `promoted-<n>`, `Unchanged` as `unchanged`), `form` (`FormFields`: a urlencoded body as pairs, keeping repeated keys; validators `id`, `required`, `note`, `policy`, `similarity`, all failing as `QueryError::InvalidInput`), `paging` (`cursor` key, `page_request`), `links` (entity URLs with the view state), `lookup` (operator names from the spec `Operator`s, `OperatorNames::of`; agent names from one `agent_names` call per `IdBatch`; `id_batches`: distinct ids in `IdBatch`es of at most `IdBatch::MAX`), `rules` (`all_rules`: `alert_rules` followed to its last page; `rule`; `RuleNames`, `rule_names`), `topics` (`default_version`; `all_topics`: a version's topics followed to their last page; `topic_trends`: one `series` grouped by topic on a 24-point grid, as `Trends`), `transmissions` (`summaries_by_id`: rows from one `transmissions_by_id` call, an empty or oversized selection refused as the spec's `TransmissionSelection` refuses it; `TransmissionRow` from a spec `TransmissionSummary`/`SummaryState`, `rows`, `transmission_table`; `route_text`, `ChannelNames` from one `channel_names` call per `IdBatch`, `summary_name` of a `ChannelRow`). |
| `ui/src/pages/overview/` | `/`: `model` (`tiles` from one `overview` call; `load`: tiles, the heaviest edges of `topology`, newest open alerts), `mod` (the page). |
| `ui/src/pages/topology/` | `/topology`: `mod` (page, header toggles, `workspace` with the graph, brush and drawer sharing the `sel` signal; `brush_window` (snapped outward to bucket boundaries), `timeline_src`; header counts from the spec graph), `selection` (`Selection`: the element's value grammar, parsed and encoded), `query` (`sel`, `collapse`; `submitted_filter` for the filter form), `filters` (choices, `filter_form`, `filter_chips`), `drawer/` (`mod`: the `topology_drawer` shard; `model`: argument validation and `load`, `edge_items`, `EdgeRow` from the spec's `EdgeTransmission`), `tests`. |
| `ui/src/pages/transmission/` | `/transmissions/{id}` GET and POST `set-verdict`: `mod` (header from the transmission's `TransmissionSummary`, `TopicCell`, load), `model` (state in words, `Strength`, match kind and carrier labels, `ExcerptView` from a spec `Excerpt`, `QuoteView` (shown, or body dropped), co-access views from `AccessDetail`), `sections` (matches side by side, co-access timeline), `verdict` (form with the spec's `Verdict`, parser, rows from a `VerdictLog`; offered for `Triage`, saying the text is hidden without `Content`), `tests`. |
| `ui/src/pages/explore/` | `/explore` GET and POST `fit`: `mod` (page, `ProjectionPanel`, colour-by and selection signals), `query` (`q`, `m`, `p`, `cb`, `ps`), `lasso` (`Polygon`: parse, even-odd point-in-polygon, `select` over stored points; `ProjectionSelection`), `results` (the `projection_results` shard), `search` (hits, form), `topics` (sidebar, `watch_url`), `fit` (parameters and form), `tests`. |
| `ui/src/pages/topics/` | `/topics`: `mod` (page, version picker, tables), `model` (`version_tabs`, `topic_rows`, `remap_rows` with stale rules), `pin` (POST `/topics` `pin`/`unpin`: `PinTopicVersion`/`UnpinTopicVersion` for `Govern`, `choice` (unpin a pinned version, pin one neither dropped nor fitting), the picker's `pin_control`). |
| `ui/src/pages/export/` | `/export`: `mod` (`GET` page and form, formats the backend does not write disabled; `POST` route: validate, `export`, download; a refusal rewritten to the page as `Rejected` with the input kept), `request` (form → the spec's `ExportRequest`, `DatasetChoice`, formats), `jsonl` (`Download`: the JSON Lines body and its headers; `header_line`, `trailer_line`; `rows`: one object per `ExportRow`), `quality` (`quality_lines`, precision, table). |
| `ui/src/pages/channels/` | `list` (`/channels`; `ListRow` and `Activity`, a row's counts or why it has none), `query` (list keys and toggles onto the spec's `ChannelFilter`/`OriginFilter`), `model` (shape, title, origin and detection in words, decisions), `detail` (`/channels/{id}` GET and POST `set-policy`), `sections` (paged resources, alerts, policy history), `policy` (set-policy form and parser), `promote/` (`patterns`: candidates from the seed and coverage; `mod`: GET and POST `/channels/{id}/promote`; `screen`: the page). |
| `ui/src/pages/agents/` | `list` (`/agents` with state and claim chips; rows from spec `AgentRow`s over the view's window, parents named in one `agent_names` call per batch, `last_seen_text`), `query` (`state`, `claims` keys into `AgentFilter::{states, claimed}`), `detail` (`/agents/{id}` GET and POST `rename`, `clear-label`, `unmerge`; the page from the `AgentCluster`, the alias banner from `AgentLookup::Redirected`), `actions` (form parsers; `AgentLabel` errors worded by `label_error`; `merge_action`: `OperatorAction::merge_agents` for the caller, one id twice `InvalidInput(SelfMerge)` via `ActionError::from(SelfMerge)`), `evidence` (evidence rows, shared and conflicting evidence), `tree` (bounded sub-agent tree, one `agents` call per level), `sections` (alias rows with their prior state; merge rows say what an unmerge restores, and a reverted one what its `Reversal` pointed back), `merge` (`/agents/{id}/merge` GET and POST; comparing with an id of the same cluster is `Conflict(MergeIntoSelf)`), `tests` (router tests: the alias banner, the reverted merge's veto, unmerging once, refused renames and merges). |
| `ui/src/pages/alerts/` | `inbox` (`/alerts` GET and POST `acknowledge`, `resolve`; `parse_action` shared with the alert page), `detail` (`/alerts/{id}` GET and POST: rule, subject, state, triage forms, audit history as `audit::entry` views, `Audit` checked first), `model` (alert rows from spec `Alert`s), `rules/` (`mod`: `/alerts/rules` GET and POST `set-enabled`, sinks for `Govern` only; `model`: rule rows (status and staleness apart, `StaleReason` in words, `watched_topics`, `semantic_query`) and sink rows; `form`: watched-topic and semantic forms, parsed into the spec's `RuleName` (inline `InvalidText` errors) and `UserRule`; `edit`: `/alerts/rules/new` (`?topic=<id>` preselects a topic of the current version) and `/alerts/rules/{id}` GET and POST). |
| `ui/src/pages/audit/` | `page` (`/audit`, `Audit` first; `AuditRow`: subject cells with page and filter links, `OutcomeCell`), `entry` (`entry_view`: a spec `AuditEntry` as `EntryView` (actor from `AuditEntry::by`, `subjects()`, `OutcomeView`: applied with `Created` subjects, unchanged, rejected in words, forbidden), shared with the alert page), `query` (`op`: an operator or `config` into `AuditFilter::by`; `subject`; `span`), `describe` (operator actions, config changes and export events in words, and notes), `subject` (codes for every spec `AuditSubject`, `tv.<n>` for topic versions, and links). |
| `ui/src/pages/pipeline/` | `/pipeline` GET (`group` key: `parse_group`) and POST `replay` (back to the same group). |
| `ui/src/components/` | Shared markup: `live` (`live_watch`, `watch_one`: a page's `data-live-watch` tokens), route and claim badges, content-hidden marker, error and empty states, page header, name and time formatting (`agent_name`, `agent_name_of` for an `AgentName`), `abbrev_digest`. `badge` (`Tone`, the `Badge` trait for policy, origin, detection, agent state, alert state, evidence strength, transmission state and verdict; `state_badge`, `kind_badge`), `table` (`data_table` and cell classes), `paging` (`PageLinks`, `pagination`), `nav` (`tabs`, `filter_chip`, `segmented`), `sparkline` (inline SVG trend; `points`), formatting helpers `format_time_short`, `format_bytes`, `format_share`, `format_duration`, `locator` (`locator_text`, `pattern_text`, text forms), `href` (`href`: a path with the view state and page pairs; `state_pairs`), `form` (control classes, `state_inputs` for `GET` forms), `feedback` (`flash_banner`). |
| `ui/src/testing/` | Test-only: a router over the fixture backend with an asset catalog built from the test binary, `get`/`post` returning status, location and body (a fresh world per request), `Session` (one router, so state carries across requests), `operator` (the trusted `Access`), `caller_with`/`caller_of` (callers holding given permissions), `world`/`agent_id`/`channel_id` (scenario ids from a same-seed copy), and `cx`/`render` for rendering components. |
| `ui/src/data/mod.rs` | The data routes' module: route table, `require(caller, permission)` (403). |
| `ui/src/data/query.rs` | `parse_strict` (every required key, and a window on bucket boundaries, or an error; never a redirect; same `ViewState::parse` and `pages::view::defaults`), `view_state(cx)` (it, as a 400). `buckets(cx)` and `parse_buckets`: `buckets=` in 1..=1000, default 96. |
| `ui/src/data/errors.rs` | `status_of(UiError)` and `query_error(impl Into<UiError>)`: a refused field and `VersionNotRetained`, `InvalidInput`, `InvalidCursor` and the URL's version and projection conflicts → 400 with the `error::describe` message; `Forbidden` → 403; `NotFound`, `ProjectionNotRetained` → 404; others → 500 (logged). |
| `ui/src/data/names.rs` | Channel display names from a pattern or seed locator (`locator_name`, `pattern_name`, `shape_name` of the spec's `ChannelShape`, `channel_name` of the spec's `ChannelName`). |
| `ui/src/data/topology/` | `GET /data/topology`: `TopologyPayload::{agents, channels}` (from the spec graphs; channel names from one `channel_names` call) and its node, edge and code types; `tests`. |
| `ui/src/data/timeline.rs` | `GET /data/timeline`: `timeline_grid` (the aligned grid for `n` buckets), `TimelinePayload::new` from two `Total` series on that grid (buckets with `final`). |
| `ui/src/data/projection/` | `GET /data/projection/{id}`: `format.rs` (binary layout, `ProjectionHeader`, `PayloadPoints` (the spec frame re-indexed into id-sorted tables, channels supplied), `ProjectionTables`, `encode`), `mod.rs` (route, `point_channels` from `transmissions_by_id`, `tables`: names from one `agent_names` and one `channel_names` call, chunked at `IdBatch::MAX`, and the version's `topics`), `decode.rs` (test-only strict decoder). |
| `ui/src/data/elements.rs` | `TOPOLOGY_JS`, `PROJECTION_JS`, `TIMEBRUSH_JS`, `LIVE_JS`: the bundled elements as Topcoat assets. |
| `ui/src/data/live.rs` | `GET /data/live`: `LiveEvents` (a `Stream` of SSE `Event`s over the backend's `LiveStream`, the subscription moved into each pending `next` and given back with its item), `frame` (one event per `LiveItem`), `ended` (the last event of a `LiveEnd`), `event_fields`; tests through the router (heartbeat, `Last-Event-ID` replay and resync, lag). |
| `ui/src/data/fixtures/`, `route_tests.rs` | Tests: hand-built spec graphs and series (`graphs`) and a spec `Projection` with its points' channels (`mod`), the element fixture files written from them, and the routes through the router. |
| `ui/elements/package.json`, `pnpm-workspace.yaml` | pnpm package; exact pins; `minimumReleaseAge` of a week for every transitive dependency. Scripts: `build`, `demo`, `smoke`, `test`, `typecheck`, `lint`. |
| `ui/elements/scripts/build.mjs` | esbuild: `src/ct-*.ts` → `dist/<name>.js` (ESM, minified, external source map); `--serve` rebuilds and serves the package for the demo. |
| `ui/elements/scripts/smoke.mjs` | Headless-Chrome smoke test of the demo over CDP, with screenshots in both colour schemes. |
| `ui/elements/src/ct-*.ts` | Entry points: define `ct-topology`, `ct-projection`, `ct-timebrush`, `ct-live` (once; `ct-live` without the payload machinery, 6 KB). |
| `ui/elements/src/live/` | `<ct-live>`: `watch` (event kinds, `parseNotice` for each event's data, `parseWatch` tokens, `watches`), `refresh` (`refreshRegions` through the page runtime, `isEditing`), `element` (`LiveElement`: `EventSource`, debounce, notice; `value` is the last event id). |
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
- Pages, shards and routes reach data only through the spec's traits
  (`QueryApi`, `OperatorActions`, `LiveFeed`) and the two gap traits
  (`contract::present::Present`, `contract::formats::ExportFormats`),
  called on `app::AppBackend`, including the present, the bucket width
  and the default topic version. Nothing outside `main.rs`, `app.rs` and
  the tests names `FixtureBackend`, and nothing in `ui/src/contract/`
  restates a spec type.
- Every caller comes from the spec's `OperatorDirectory`
  (`config::Access`); pages never build a `Caller` themselves.
- Every window sent to the backend is on bucket boundaries
  (`url::scope::is_aligned`), and every linked view is sent the scope's
  filter pinned to the URL's topic version (`Scope::topology_filter`).
- Every action goes through `pages::common::action::perform`: the
  action's one `required_permission` checked, then `OperatorActions::act`.
- Errors are the spec's `QueryError` (or a refused field) as `UiError`,
  rendered through `error::describe`; a page never shows a backend error
  as free text it made up, and data routes map errors to statuses in one
  place (`data::errors::status_of`).
- The UI never computes or handles embeddings: rule forms submit text
  (`UserRule::SemanticQuery`) and search sends text (`SearchRequest`).
- Names for many ids are one backend call per `IdBatch` (`agent_names`,
  `channel_names`, chunked at `IdBatch::MAX` by
  `pages::common::lookup::id_batches`), never a read per id; transmission
  rows for many ids are one `transmissions_by_id` call over a checked
  `TransmissionSelection`.
- An audit entry's subjects and what it created come from the spec's
  entry (`AuditEntry::subjects`, `ActionOutcome::subjects`); pages do not
  infer them from the action.
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

## What the UI uses from L8

Every read is a `QueryApi` method, every action an `OperatorAction` sent
through `OperatorActions::act` (its permission is
`OperatorAction::required_permission`), and live updates come from
`LiveFeed::subscribe` (`spec/types/interfaces/l8_surface.rs`,
`l8_surface/`). The two gap traits of `ui/src/contract/` are marked
*(gap)*. Name lookups (`agent_names`, `channel_names`) are View and run
one call per `IdBatch`; operator and rule names come from `operators`
and `alert_rules` (View).

| Screen or route | Spec calls | Main spec types | Permission |
| --- | --- | --- | --- |
| View defaults (every page, shard and data route) | `Present::now`, `Present::bucket_width` *(gap)*, `topic_versions` | `Timestamp`, `BucketWidth`, `TopicVersionHistory` (`active()`), `TimeWindow` | View |
| Overview `/` | `overview`, `topology`, `alerts` (open, first five), `agent_names`, `channel_names`, `alert_rules`, `operators` | `OverviewCounts` (`EdgeTotals`, `QueueCounts`), `Watermarked`, `TopologyGraph`, `AlertFilter`, `Alert` | View |
| Topology `/topology` (header, filter, brush) | `topology` (agents mode) or `channel_topology` (bipartite); `channels` and `topics` (filter choices); `Present::now`, `Present::bucket_width` *(gap)* (brush window) | `TopologyGraph`, `BipartiteGraph`, `TopologyFilter`, `Weighting`, `ChannelRow`, `TopicPage` | View; Content for topic choices |
| Topology drawer (shard) | `topology` (edge stats, heaviest edges), `edge_transmissions`, `agent`, `channel`, `agent_names`, `channel_names` | `EdgeSelector`, `EdgeTransmissionPage` (`EdgeTransmission`), `AgentDetail`, `ChannelRow` | View |
| Evidence `/transmissions/{id}` | `transmissions_by_id` (one id), `transmission_evidence`, `verdicts`, `topics`, `agent_names`, `channel_names`, `operators`; `SetVerdict` | `TransmissionSelection`, `TransmissionSummary`, `TransmissionEvidence`, `ExcerptWindow`, `Excerpted`, `VerdictLog`, `Verdict` | View (header, verdict log); Content (matches, co-access, topic labels); Triage (`SetVerdict`) |
| Explore `/explore` | `search`, `transmissions_by_id`, `fit_projection`, `projection_status`, `projection`, `topic_sizes`, `series` (grouped by topic), `topics` | `SearchRequest`, `SearchMode`, `SearchResults`, `ProjectionParams`, `ProjectionInfo`, `Projection`, `TopicSizes` | Content |
| Topics `/topics` | `topic_versions`, `topics`, `topic_sizes`, `series`, `topic_lineage`, `alert_rules`; `PinTopicVersion`, `UnpinTopicVersion` | `TopicVersionHistory`, `TopicPage`, `TopicSizes`, `TopicLineage`, `AlertRuleDef` (`StaleReason::TopicsUnmapped`) | Content; Govern to pin |
| Channels `/channels`, `/channels/{id}` | `channels`, `channel`, `channel_resources`, `policy_history`, `alerts` (by channel), `agent_names`, `alert_rules`, `operators`; `SetPolicy` | `ChannelFilter` (`OriginFilter`), `ChannelRow` (`ChannelCounts`), `ResourceUsePage`, `PolicyHistory`, `PolicyKind` | View; Govern for `SetPolicy` |
| Promotion `/channels/{id}/promote` | `channel`, `promotion_preview`, `channel_names`; `PromoteChannel` | `ResourcePattern`, `PromotionPreview`, `ActionOutcome::ChannelPromoted` | View for the preview; Govern to promote |
| Agents `/agents`, `/agents/{id}` | `agents` (list; the sub-agent tree, one call per level), `agent`, `agent_names`, `operators`; `RenameAgent`, `Unmerge` | `AgentFilter`, `AgentRow`, `AgentDetail` (`AgentCluster`, `AgentLookup`), `MergeRecord`, `MergeVeto`, `AgentLabel` | View; Govern for actions |
| Merge `/agents/{id}/merge` | `agent` (both agents), `agents` (target choices); `MergeAgents` (`OperatorAction::merge_agents`) | `AgentDetail`, `MergeRequest`, `ActionOutcome::Merged` | View; Govern to merge |
| Alerts `/alerts` | `alerts`, `alert_rules`, `operators`; `Acknowledge`, `Resolve` | `AlertFilter`, `AlertStateKind`, `Alert`, `AlertState`, `SuppressReason` | View; Triage for actions |
| Alert page `/alerts/{id}` | `alert`, `alert_rules`, `operators`, `audit` (subject `al.<id>`); `Acknowledge`, `Resolve` | `Alert`, `AuditFilter`, `AuditEntry` | View; Audit for the history; Triage for actions |
| Rules `/alerts/rules`, `/alerts/rules/new`, `/alerts/rules/{id}` | `alert_rules`, `sinks`, `topic_versions`, `topics`, `operators`; `CreateRule`, `UpdateRule`, `SetRuleEnabled` | `AlertRuleDef`, `RuleStatus`, `StaleReason`, `UserRule`, `RuleName`, `SinkInfo`, `ActionOutcome::RuleCreated` | View; Content for topic labels; Govern for sinks and actions |
| Export `/export` | `topic_versions`, `export`, `detection_quality`, `ExportFormats::export_formats` *(gap)* | `ExportRequest`, `ExportFormat`, `Export` (`ExportHeader`, `ExportRows`: `ExportRow`s, `ExportTrailer`), `DetectionQuality` | View; Content to include content or export a projection (`ExportRequest::required_permission`) |
| Audit `/audit` | `audit`, `operators` | `AuditFilter`, `AuditEntry` (`AuditSubject`, `AuditOutcome`, `ConfigChange`), `Operator` | Audit |
| Pipeline `/pipeline` | `dead_letters`; `ReplayDeadLetter` | `ConsumerGroup`, `DeadLetter` | Operate |
| `/data/topology` | `topology` or `channel_topology`, `channel_names` | `TopologyGraph`, `BipartiteGraph`, `ChannelName` | View |
| `/data/timeline` (the time brush) | `series` twice (transmissions, matched bytes) on a grid built from `Present::bucket_width` *(gap)* | `SeriesGrid`, `SeriesGrouping::Total`, `TopologySeries` | View |
| `/data/projection/{id}` | `projection`, `transmissions_by_id` (channels of channel-routed points), `agent_names`, `channel_names`, `topics` | `Projection` (`ProjectionFrame`), `TransmissionSummary`, `AgentName`, `ChannelName` | Content |
| Live updates `/data/live` | `LiveFeed::subscribe`, `LiveStream::next` | `Resume`, `LiveCursor`, `LiveItem`, `UiEvent`, `LiveEnd` | View (events filtered by `UiEvent::visible_to`) |

Every list is paged with the spec's `PageRequest` and `Page` cursors
(`pages::common::paging`); lists the UI needs whole (`alert_rules`,
`topics`) are followed to their last page with a bound on the number of
pages.

### Behaviour that follows the spec

Moving from the UI's own L8 shapes to the spec's traits changed what
some screens show. These follow the spec's semantics and are intended:

- **Windows and counting.** Windows snap outward to bucket boundaries
  (five minutes in the fixture). Transmissions are counted by
  confirmation time (`Confirmed::at`), not when they opened; search counts
  hits on transmissions confirmed in the window. The overview's active
  channels are the channels that carried a counted transmission, and its
  counts are exact.
- **Topic versions.** v0 was dropped by retention, so `v=0` is the typed
  `VersionNotRetained` error (409 on a page, 400 on a data route); an
  unknown version is 404; filtered topics not in the view's version are
  `Conflict(TopicsNotInVersion)`. Topic sizes count every assignment under
  the version and ignore the view's filter. Every fit records a new
  projection job; a fit needs at least 2 neighbours. The evidence header
  shows the topic under the view's version.
- **Verdicts.** The verdict log needs only View (it holds no content) and
  `SetVerdict` only Triage. Recording the verdict already in force appends
  nothing (`Unchanged`). Detection quality has suspected and discarded
  rows, and precision only for confirmed rows.
- **Topology drawer.** Edge transmission rows show confirmation time and
  matched bytes only (the spec's `EdgeTransmission` has no state or
  verdict). Selecting a self-edge (a merged pair) is refused.
- **Channels.** Promotion keeps the channel's id (`ChannelPromoted` names
  it) and supersedes the discovered channels the pattern covers. Lists
  are newest first. Counts are over the view's window (writers and
  readers from resource use, transmissions as the default-filter graph
  routes them); a superseded row shows "—"; last activity includes
  confirmations. Policy history holds recorded decisions only (refused
  attempts are in the audit log). The origin badge is declared, promoted
  or discovered; `superseded=only` lists superseded channels alone.
- **Agents.** Lists are newest first; transmissions in and out are the
  agent's topology node in the view's window under the default filter; a
  registered agent never seen shows "never". A merge must name two
  canonical agents: merging into an alias is `Conflict(AgentMerged)` (the
  merge page resolves a pasted alias first), one id twice
  `InvalidInput(SelfMerge)`, two ids of one cluster
  `Conflict(MergeIntoSelf)` (409, was 422). The merge author is the
  caller; an alias's merge time and author come from its merge record.
  An unmerge restores only repointed agents nothing has touched since,
  and the veto it leaves names the lower id first.
- **Alerts and rules.** Acknowledging an acknowledged alert or resolving
  a resolved one is accepted as `Unchanged`. A rule with no sinks
  delivers to the inbox only (it used to mean every sink). Built-in rules
  carry the spec's names; staleness is shown apart from enabled or
  disabled. Text too long to embed is the backend's
  `InvalidInput(QueryTooLong)`. The inbox is ordered by alert id. Sinks
  need Govern.
- **Audit and pipeline.** The audit log needs Audit. Its subject filter
  matches ids as recorded (no resolution through merges or supersession;
  an unmerge names only its merge record). Outcomes distinguish forbidden
  from rejected. Config changes are described from the typed
  `ConfigChange`, not free text. Dead letters are newest first.
- **A UI limitation.** The topics page body needs Content, so an operator
  with Govern but not Content cannot reach the pin control.

### Remaining gaps

#### Declared in `ui/src/contract/`

Each is a trait `FixtureBackend` implements, proposed for `QueryApi`, and
deleted when the spec gains it.

| Gap | Trait | Why the UI needs it |
| --- | --- | --- |
| The bucket width | `present::Present::bucket_width` | `QueryApi` refuses unaligned windows (`InvalidInput(UnalignedWindow)`) and series grids for another width (`InvalidInput(BucketWidthMismatch)`), but only L7 knows the width (`EdgeStore::bucket_width`). The UI needs it to snap view windows, draw the time brush and build a `SeriesGrid`. |
| The present | `present::Present::now` (View) | A default view is "the last 24 hours". `QueryApi` offers the watermark, which trails the newest data by the settling delay, but no clock. A gateway would answer with its wall clock; the fixture's data ends at 2026-10-03T00:00Z, and when serving (`FixtureBackend::try_live`) its clock moves on from there in real time, so actions, exports and fits are stamped after the data and show in a freshly loaded default view. Tests use a fixed clock (`try_new`). |
| Export formats | `formats::ExportFormats::export_formats` | `export` takes an `ExportFormat` (JSONL or Parquet), but the spec has neither a capability query nor an `InputError` for a format the gateway cannot write. The fixture writes JSONL only and refuses Parquet with `Store` (audited as refused) before reading anything; the export page lists Parquet as unavailable instead of offering a choice that is refused. |

#### Spec gaps worked around elsewhere

Reads:

- **Projection frames have no channel column** (`ProjectedPoint`,
  `FrameColumns`). `data::projection::point_channels` reads
  `transmissions_by_id` for channel-routed points, so a point's channel
  is resolved now through supersession rather than frozen at fit time,
  at the cost of extra calls on large frames.
- **`ChannelNode::locator_summary` is preformatted text** (it adds a
  resource count). The topology payload names channel nodes from one
  `channel_names` call instead (`data::topology`).
- **`EdgeTransmission` has no state or verdict.** The topology drawer
  drops those columns rather than making a second call per page.
- **`AlertState` has no `kind()`** (the `AlertStateKind` that
  `AlertFilter::states` filters on) **and no `is_active`.**
  `backend::alert_state` defines both.
- **No `QueryApi::alert_rule(id)`.** `pages::common::rules::rule` lists
  every rule (`all_rules`) to find one.
- **`MergedInto` has no time or author.** Alias rows on the agent page
  find their merge record in `AgentCluster::merges()`.
- **`AlertRuleConfig::default_remap_threshold` is not exposed.** The
  topics page's remap table and the rule form use the UI's default 0.80
  (`pages::alerts::rules::form::DEFAULT_REMAP`); a rule submitted with a
  blank threshold takes the configured default on the backend.
- **The rules consumer's current topic version is not exposed.** The rule
  form picks topics of `topic_versions().active()`
  (`pages::common::topics::default_version`).
- **Projection frame retention is not exposed.** The explore panel learns
  that a projection expired only when it has (`ProjectionNotRetained`, an
  `Expired` status); it cannot say when it will.
- **`SearchMode` has no `Default`.** The UI defaults to `Hybrid`
  (`pages::explore::query::DEFAULT_MODE`).
- **Semantic rule text has no length bound** (`UserRule::SemanticQuery`'s
  text is an unbounded `NonBlank`, and `EmbedError::TooLong` names no
  limit), so the rule form cannot check it; the backend's
  `InvalidInput(QueryTooLong)` is shown inline.
- **`TopologyGraph` has public fields and no checked constructor** (unlike
  `BipartiteGraph::new`), and its shares are never validated. The fixture
  checks its graphs with `TopologyGraph::check_nodes`; the topology
  payload trusts the graph it is given.
- **`Watermark`'s field is public,** so a watermark off a bucket boundary
  can be built although the docs promise a boundary. The UI trusts it
  (the timeline's `final` compares bucket ends with it).

Semantics the spec leaves open (the fixture's choice):

- **Self-edges in search** (a merged pair's transmissions) are not
  specified by `SearchIndex::query`; the fixture includes them.
- **`edge_transmissions` alignment.** `EdgeStore::transmissions` says the
  window need not be aligned, while `QueryApi::edge_transmissions` says
  it is exactly what `topology` counts (which refuses an unaligned
  window). The fixture does not refuse; the UI sends only aligned
  windows.
- **`channels` and `channel` "all time"** (`window: None`): transmissions
  are defined through `EdgeStore::graph` for the same window, which needs
  a `TimeWindow`. The fixture uses an aligned all-time window
  (`backend::fixture::clock::all_time`).
- **A watched-topic rule on an unknown version**: `UnknownTopics` or
  `TopicVersionNotCurrent` is unspecified. The fixture answers
  `UnknownTopics` for an unknown version and `TopicVersionNotCurrent` for
  a known one that is not current (`actions::rules`).

Action errors (mapped locally in `backend::fixture::actions`):

- **No `From<PinError>` or `From<CatalogError>` for `ActionError`.**
  `TopicCatalog::pin` documents `CatalogError::VersionNotRetained` for a
  dropped version, which `ActionError` cannot hold, while
  `PinTopicVersion` promises `Conflict(TopicVersionDropped)`.
  `pins::refusal` maps it as the action documents.
- **No `From<VerdictError>` for `ActionError`** (only for `QueryError`).
  `triage::set_verdict` builds the refusals `SetVerdict` documents.
- **No `ActionError` mapping for `RegistryError`** (`SetPolicy` on a
  superseded channel). `channels` checks the supersession and returns
  `Conflict(ChannelSuperseded)`.
- **`ConfigChange` has no variant for sinks or the retention policy,** so
  the fixture's config audit entries cover the access mode, operators,
  declared channels, registered agents and provisioned rules only.

Async:

- **No `Send` bounds.** `QueryApi`, `OperatorActions`, `LiveFeed`,
  `ExportStream::next` and `LiveStream::next` are native `async fn`s
  without `Send` bounds. That is fine for the concrete `AppBackend`,
  whose futures' `Send`-ness is inferred; a client generic over the
  traits would need return-type-notation bounds.

Not spec gaps, but stand-ins only the fixture has:

- The export row digest is four FNV-1a lanes, not BLAKE3
  (`backend::fixture::export::digest`), so a fixture digest checks a
  fixture export only.
- The projection sample key is SplitMix64, not keyed BLAKE3, and the
  layout a set of theme clusters, not UMAP
  (`backend::fixture::queries::projection::sample`); search scores and
  config document hashes are deterministic stand-ins too.
- `futures-core` (`=0.3.34`, the version Topcoat already locks) is a
  direct dependency because Topcoat's `Sse` takes a
  `futures_core::Stream` and does not re-export the trait.
