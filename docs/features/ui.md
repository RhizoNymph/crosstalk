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
  `attributeChangedCallback`. This uses only Topcoat's typed expression
  vocabulary: no `raw!` and no reliance on the runtime's event wrapper.
  Verified in a headless-Chrome spike against Topcoat 0.9.0.
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
- Filter controls are a `GET` form, so changing them navigates and the URL
  is always current.
- Selections made inside an element update a signal and the URL
  (`history.replaceState`).
- A view whose topic version is no longer retained shows the typed
  `VersionNotRetained` error rather than silently using another version.
- Aggregate views show the response's watermark ("final up to 14:05"), so
  a cited view says whether its numbers can still change.

## Screens

Each screen lists the calls it makes. Names in *italics* are additions in
[The L8 contract](#the-l8-contract).

### Topology (`/topology`)

The main screen. Nodes are canonical agents sized by volume; sub-agents
collapse into their parent. Edge width is the edge's share of the
filtered total; edge colour is the route kind. A mode toggle switches to
the bipartite view, where channels are nodes between their writers and
readers, so a hijacked shared resource is visible before anyone reads it.

- Calls: `topology` (agents mode) or *`channel_topology`* (bipartite),
  both with *`nodes`* in the response; *`timeline`* for the time brush.
- Selecting an edge opens a drawer: stats, share, route, and the
  transmissions on it (*`transmissions(filter)`*). Selecting a node opens
  the agent or channel page.

### Transmission evidence (`/transmissions/{id}`)

Why the gateway believes this transmission happened. The sender's
originated text and the reader's input side by side with matched spans
highlighted, each match labelled with its `MatchKind` (with the decode
chain, e.g. "base64 → url") and `Carrier`. A co-access timeline shows the
write, the read and the lag. Suspected transmissions are visibly weaker;
discarded ones say so.

- Calls: `transmission`, message bodies for the matched spans, *verdicts*.
- Actions: *`SetVerdict`* (genuine / false detection, with a note).

### Explore (`/explore`)

Search and the UMAP projection, linked. A query highlights its hits in
the projection; a lasso in the projection fills the result list. Points
colour by topic, sender, reader or channel. A topic sidebar lists topics
with size and trend; "watch" creates a watched-topic rule.

- Calls: `search` (with the shared filter), *`fit_projection`* /
  *`projection(id)`*, `topics` with *topic stats*.
- The lasso is sent as a polygon in projection coordinates and resolved
  server-side against the stored projection, so it is small enough for a
  signal and the URL.

### Topics (`/topics`)

Topics of a model version: label, terms, size, trend, and the mapping to
the previous version after a re-fit. Pick a version; old versions remain
viewable while retained.

- Calls: `topics`, *topic stats*, *topic versions and remaps*.
- Actions: *`CreateRule`* (watch).

### Channels (`/channels`, `/channels/{id}`, `/channels/{id}/promote`)

A table with origin, detection state, policy, locator or pattern summary,
writers, readers, transmissions and last activity, filtered by toggle
chips (`origin`, `detection`, `policy`, `superseded` query keys, comma
lists like the shared filter) and paged by cursor. The "Review queue" tab
(`tab=review`) fixes the policy to unreviewed and hides superseded
channels. The detail page shows origin and detection in words (with a link
to the latest transmission), a banner linking the declared channel that
superseded it, the policy with its decision (author, time, note) and the
set-policy form, the resources accessed in the view's window with their
writers and readers, the channel's alerts, and its policy history from the
audit log. Traffic over time is not shown yet.

Promotion is its own page. Candidate patterns are derived from the seed
locator, most specific first (exact; URL path prefixes by whole segments,
then host; file path prefixes; MCP server), so every candidate covers the
seed. Picking one (`?pattern=<n>`) shows which of the channel's known
resources it covers and which other discovered channels it would
supersede, then a confirm form with policy and note.

- Calls: *`channels`*, `channel`, *`channel_resources`*, `alerts`
  (filtered by channel), *`audit`* (policy history), *`operators`*,
  *`rules`* (rule names), *`agent`* (writer and reader names).
- Actions: `SetPolicy`, *`PromoteChannel`* (`Govern`; hidden on superseded
  channels, promotion only for discovered ones).

### Agents (`/agents`, `/agents/{id}`, `/agents/{id}/merge`)

A paged table (name, state, harness claims, parent, transmissions in and
out, last seen), and a detail page: identity evidence (each variant with
its scope and strength, most specific first, hashes abbreviated to their
key version and first four bytes), harness claims (shown as "claims",
never as identity), the sub-agent tree (read up to four levels and sixty
agents deep), aliases, merge history with revert state and an unmerge
button per merge in force, and merge vetoes. A URL naming an alias shows
the canonical agent with a banner; actions always target the canonical
agent. Rename validates `AgentLabel` and shows its error inline; clearing
the label is a separate button.

Merge is its own page: `/agents/{id}/merge` offers a target (pick from the
agent list or paste an id), `?into=<id>` shows both agents' evidence side
by side with shared evidence highlighted, warns when they share nothing or
carry different harness ids in the same scope, and confirms a
`MergeRequest` authored by the operator (the first agent becomes an alias
of the second).

- Calls: *`agents`*, *`agent`*, *`operators`*.
- Actions: `MergeAgents`, *`Unmerge`*, *`RenameAgent`* (`Govern`).

### Alerts (`/alerts`, `/alerts/rules`, `/alerts/rules/new`, `/alerts/rules/{id}`)

An inbox with one tab per state (`tab=open|acknowledged|resolved|
suppressed`), showing rule name, subject link (channel, agent or
transmission page), occurrences, raise time, who moved the alert to its
state (and the resolution note or suppression reason). Open alerts can be
acknowledged; open and acknowledged ones resolved with a note (`Triage`).
When the shared filter names exactly one channel, the inbox is narrowed to
it. The rules page lists built-in rules (enable or disable), operator
rules (what they match, status, stale reason, author, sinks; edit, enable,
disable, or "Update" when stale) and sinks with their last delivery.
Watched-topic rules pick topics of the newest topic version (labels need
`Content`). Semantic query rules cannot be saved yet: `UserRule::
SemanticQuery` carries an `Embedding` the UI cannot compute, so the form
validates and then reports that.

- Calls: `alerts`, *`rules`*, *`sinks`*, *`topic_versions`*, `topics`,
  *`operators`*.
- Actions: `Acknowledge`, `Resolve`, *`CreateRule`*, *`UpdateRule`*,
  *`SetRuleEnabled`*.

### Research and pipeline

- **Export** (`/export`): pick dataset, window, filter, topic version and
  format; content needs `Content`. Calls *`export`*.
- **Audit** (`/audit`): operator and config actions with actor, a one-line
  description, note, subject link and outcome (applied, or rejected with
  the typed error), paged. Filters: `op` (operator), `subject`
  (`ch.`/`ag.`/`tx.`/`ru.`/`al.` plus the id; every row has a "filter"
  link) and `span=all` (otherwise the shared window). Calls *`audit`*,
  *`operators`*.
- **Pipeline** (`/pipeline`): dead letters (consumer group, event id, kind
  and time, attempts, last error) with replay. Needs `Operate` to view.
  Calls *`dead_letters`*; action `ReplayDeadLetter` (`Operate`).

## Data and control flow

1. A request reaches a page. Trusted mode builds the `Caller` for the
   configured operator. The page parses `ViewState` from the query.
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
decoder. `ui/src/data/fixtures.rs` writes the payloads of hand-built
contract values to `ui/elements/test/fixtures/` and fails when they drift
(`CT_UPDATE_FIXTURES=1 cargo test element_fixtures` regenerates them), so
the TypeScript tests parse exactly what Rust emits.

| Route | Payload | Needs | Backend call |
| --- | --- | --- | --- |
| `GET /data/topology?<view state>` | JSON `TopologyPayload` | `View` | `topology` (`g=agents`) or `channel_topology` (`g=channels`) |
| `GET /data/timeline?<view state>&buckets=<n>` | JSON `TimelinePayload` | `View` | `timeline`, `n` in 1..=1000, default 96 |
| `GET /data/projection/{id}` | binary, `application/octet-stream` | `Content` | `projection`, plus `agent`, `channel`, `topics` for names |

- **No redirects.** A view-state route needs every canonical key (`from`,
  `to`, `v`, `w`, `g`); a missing or invalid one is a 400 naming it.
  Backend errors: Forbidden 403, NotFound 404, VersionNotRetained and
  InvalidInput 400 (with the message), anything else 500.
- **Topology** (JSON, camelCase): `mode` (`agents` | `channels`), `window`
  `{from, to}`, `weighting` (`tx` | `bytes`), `topicVersion`, `watermark`,
  `nodes`, `edges`. Nodes are tagged by `kind`:
  `agent {id, name, state, parent, volume, transmissionsIn,
  transmissionsOut, claims[{harness, version, userAgent, lastSeen}]}` and
  (channels mode) `channel {id, name, origin, detection, policy, volume}`.
  `name` is `components::agent_name` or the channel's pattern / seed
  locator; `volume` is transmissions in + out, or accesses. Edges:
  `transmission {from, to, route, routeKind, share, transmissions,
  matchedBytes}` (`route` is the `url::route` code) and (channels mode)
  `access {agent, channel, op: read | write, accesses, share}`; access
  shares are normalised separately. Channels-mode transmission edges are
  the ones not routed through a channel.
- **Timeline** (JSON): `window`, `bucketMs`, `watermark`,
  `buckets[{from, to, transmissions, matchedBytes, final}]`; `final` is
  `to <= watermark`.
- **Projection** (binary, little-endian, columns 4-byte aligned; `n`
  points, header of `h` bytes):

  | Bytes | Content |
  | --- | --- |
  | 4 | magic `CTPJ` |
  | 4 | `u32` version (1) |
  | 4 | `u32` `h`, a multiple of 4 |
  | `h` | header JSON, space-padded: `id`, `count`, `window`, `topicVersion`, `fittedAt`, `embeddingModel {name, dimension}`, `params {neighbors, minDist, seed (string), sampleLimit}`, `routeKinds` (`["channel","delegation","direct","unobserved"]`), `agents[{id, name}]`, `channels[{id, name}]`, `topics[{id, label}]` (`label` null when hidden or the version is gone) |
  | `4n` each | `f32` xs, `f32` ys, `u32` sender, `u32` reader (into `agents`) |
  | `n` + pad to 4 | `u8` route kind (into `routeKinds`) |
  | `4n` each | `u32` channel (into `channels`), `u32` topic (into `topics`); `0xFFFFFFFF` = none |
  | `16n` | transmission ids, 128-bit big-endian |

  The total is exactly `12 + h + 41n + pad(n)`; decoders reject anything
  else, and any index outside its table.

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
| `ui/src/main.rs` | Loads config, builds the router (pages, app context, assets, runtime) and serves. |
| `ui/src/app.rs` | `backend(cx)`, `caller(cx)`, `operator(cx)`, `can(caller, permission)`. `AppBackend` is the configured backend type. |
| `ui/src/config.rs` | `Config`, `TrustedOperator` (builds the all-permissions `Caller`), `BackendConfig`. |
| `ui/src/contract/` | The L8 additions, one module per area, numbered as in [The L8 contract](#the-l8-contract). Types that replace a spec type keep its name (`TopologyFilter`, `OperatorAction`, `QueryError`, `RuleStatus`). Checked constructors: `TopologyView`, `BipartiteView`, `ProjectionPoints`, `Excerpt`, `AgentLabel`, `RuleName`, `SearchText`, `ProjectionParams`. Removed when the gateway's types land. |
| `ui/src/backend/mod.rs` | `Backend`: every read and action, returning `Send` futures. |
| `ui/src/backend/fixture/` | `FixtureBackend::new(seed)`: a deterministic synthetic world (same seed, same world and answers; generated in well under a second), implementing every `Backend` method with real semantics. `world/` generates it through the spec's and contract's checked constructors: `agents` (cast and merge history), `drafts` and `channels` (channel table, detection from traffic), `traffic` and `states` (transmissions and accesses), `evidence` (matches and excerpts), `topics`, `rules`, `alerts`, `history` (operators, policy decisions, verdicts, audit, dead letters). `store` holds what actions change behind one `tokio::sync::RwLock`; `queries/` resolves merged agents and superseded channels at read time and pages with keyset cursors; `actions/` applies and audits every action; `text/` holds the message templates and codecs; `rng` is SplitMix64. The world: 7 days ending at `now()` (2026-10-03T00:00Z, watermark ten minutes earlier), about 5,000 transmissions on a weekday daytime curve in every state (in-flight states sit in the last quarter hour) and every route, match kind (decode chains such as base64 → url) and carrier. 40 canonical agents across Claude Code, Codex, pi, oh-my-pi and self-hosted scripts, with sub-agents, three config-registered agents with no traffic, and labels. Scenarios: pi and oh-my-pi agents labelled `pi-scraper` and `omp-orchestrator` (and two unlabelled ones) also claim Claude Code; `atlas-lead` has a resolver-merged alias whose traffic to it becomes a dropped self-edge; one pi agent holds two aliases, one repointed by a later merge; an operator merge was reverted and left a veto (the oh-my-pi agent with a veto on its page). Channels: declared sanctioned `wiki.corp.internal/eng`, `git.corp.internal/platform/monorepo` and `issues.corp.internal` (active); `docs.corp.internal/design` awaiting traffic; `nfs-01:/mnt/shared/releases` unused, with an open sanctioned-unused alert; the hijacked public wiki is the discovered, unreviewed, active channel seeded at `wiki.example.org/wiki/Agent_Coordination` (injection-style text, the busiest channel), with its talk page as a second discovered channel the same `UrlPrefix` pattern covers; `paste.example.net` unsanctioned; the `memory` MCP server reset to unreviewed; `/tmp/agent-handoff` on `devbox-3` sanctioned; `gist.example.com` dormant; a `kv_put` tool only one agent uses (observed); an `s3://agent-scratch` prefix with only suspected traffic (candidate); and `notes.corp.internal/team-a/standup`, superseded by the operator-promoted declared channel `notes.corp.internal/team-a`. Topics: v0 (unfitted), v1 (six topics, fitted six days ago, pinned) and v2 (ten, two days ago); v1's "Engineering chatter" maps to nothing in v2. Rules: the five built-ins, a watched-topic rule on v2 (credentials and agent instructions), a stale v1 rule, a semantic query on paste sites (with an agent-subject alert) and a disabled refund rule; sinks soc-webhook (last delivery failed), #agent-alerts, local-log. About 650 alerts in every state and suppress reason, deduplicated occurrence counts on channel alerts. Two operators: `researcher` (the trusted operator in `config.json`) and `oncall` (view, content, triage); about 60 verdicts (one withdrawn), a few hundred audit entries including two rejected actions, and four dead letters. |
| `ui/src/url/` | `ulid` (Crockford text for every id), `route` (URL text for `Route` and `RouteKind`), `view_state` (`RawViewState` → `ViewState` and back to the canonical query). |
| `ui/src/pages/` | `mod.rs` (root layout; navigation links carry the current view state when the request has a complete one), `view.rs` (`view_state(cx)`: parse, default, redirect to canonical; `current_state(cx)` for the layout), one module per screen. |
| `ui/src/pages/common/` | Shared by the governance pages. `action` (`perform`: permission check then `Backend::act`; `done`: 303 with flash; `Failure<F>` and `error_for`/`fields_for` to show an error next to its form; `status_of`), `flash` (`Flash` codes and messages), `form` (`FormFields`: a urlencoded body as pairs, keeping repeated keys; validators `id`, `required`, `note`, `policy`, `similarity`, all failing as `QueryError::InvalidInput`), `paging` (`cursor` key, `page_request`), `links` (entity URLs with the view state), `lookup` (operator, rule and agent names). |
| `ui/src/pages/channels/` | `list` (`/channels`), `query` (list keys and toggles), `model` (shape, title, detection in words, decision), `detail` (`/channels/{id}` GET and POST `set-policy`), `sections` (resources, alerts, policy history), `policy` (set-policy form and parser), `promote/` (`patterns`: candidates from the seed and coverage; `mod`: GET and POST `/channels/{id}/promote`; `screen`: the page). |
| `ui/src/pages/agents/` | `list` (`/agents`), `detail` (`/agents/{id}` GET and POST `rename`, `clear-label`, `unmerge`), `actions` (form parsers, `merge_action`), `evidence` (evidence rows, shared and conflicting evidence), `tree` (bounded sub-agent tree), `sections`, `merge` (`/agents/{id}/merge` GET and POST). |
| `ui/src/pages/alerts/` | `inbox` (`/alerts` GET and POST `acknowledge`, `resolve`), `model` (alert rows), `rules/` (`mod`: `/alerts/rules` GET and POST `set-enabled`; `model`: rule and sink rows; `form`: watched-topic and semantic forms and parsers; `edit`: `/alerts/rules/new` and `/alerts/rules/{id}` GET and POST). |
| `ui/src/pages/audit/` | `page` (`/audit`), `query` (`op`, `subject`, `span`), `describe` (an action in words, its note and subject), `subject` (subject codes and links). |
| `ui/src/pages/pipeline/` | `/pipeline` GET and POST `replay`. |
| `ui/src/components/` | Shared markup: route and claim badges, content-hidden marker, error and empty states, page header, name and time formatting, `abbrev_digest`. `badge` (`Tone`, the `Badge` trait for policy, origin, detection, agent state, alert state and evidence strength; `state_badge`, `kind_badge`), `table` (`data_table` and cell classes), `paging` (`PageLinks`, `pagination`), `nav` (`tabs`, `filter_chip`), `locator` (`locator_text`, `pattern_text`, text forms), `href` (`href`: a path with the view state and page pairs; `state_pairs`), `form` (control classes, `state_inputs` for `GET` forms), `feedback` (`flash_banner`). |
| `ui/src/testing/` | Test-only: a router over the fixture backend with an asset catalog built from the test binary, `get`/`post` returning status, location and body, and `cx`/`render` for rendering components. |
| `ui/src/data/mod.rs` | The data routes' module: route table, `require(caller, permission)` (403). |
| `ui/src/data/query.rs` | `view_state(cx)`: the strict view-state parse (every required key or 400, never a redirect; same `ViewState::parse` and defaults as `pages::view`). `buckets(cx)`: `buckets=` in 1..=1000, default 96. |
| `ui/src/data/errors.rs` | `query_error(QueryError)`: Forbidden → 403, NotFound → 404, VersionNotRetained / InvalidInput → 400 with the message, others → 500 (logged). |
| `ui/src/data/names.rs` | Channel display names from a pattern or seed locator (`locator_name`, `pattern_name`, `channel_node_name`, `channel_summary_name`). |
| `ui/src/data/topology.rs` | `GET /data/topology`: `TopologyPayload::{agents, channels}` and its node, edge and code types. |
| `ui/src/data/timeline.rs` | `GET /data/timeline`: `TimelinePayload` (buckets with `final`). |
| `ui/src/data/projection/` | `GET /data/projection/{id}`: `format.rs` (binary layout, `ProjectionHeader`, `ProjectionTables`, `encode`), `mod.rs` (route, `tables` name lookup), `decode.rs` (test-only strict decoder). |
| `ui/src/data/elements.rs` | `TOPOLOGY_JS`, `PROJECTION_JS`, `TIMEBRUSH_JS`: the bundled elements as Topcoat assets. |
| `ui/src/data/fixtures.rs`, `route_tests.rs` | Tests: hand-built contract values, the element fixture files written from them, and the routes through the router. |
| `ui/elements/package.json`, `pnpm-workspace.yaml` | pnpm package; exact pins; `minimumReleaseAge` of a week for every transitive dependency. Scripts: `build`, `demo`, `smoke`, `test`, `typecheck`, `lint`. |
| `ui/elements/scripts/build.mjs` | esbuild: `src/ct-*.ts` → `dist/<name>.js` (ESM, minified, external source map); `--serve` rebuilds and serves the package for the demo. |
| `ui/elements/scripts/smoke.mjs` | Headless-Chrome smoke test of the demo over CDP, with screenshots in both colour schemes. |
| `ui/elements/src/ct-*.ts` | Entry points: define `ct-topology`, `ct-projection`, `ct-timebrush` (once). |
| `ui/elements/src/shared/` | `element.ts` (`PayloadElement`: the element contract, fetch/abort, status panels), `fetch.ts` (typed `LoadError`), `selection.ts` (value grammar), `theme.ts` and `color.ts` (tokens, light/dark), `ulid.ts`, `route.ts`, `format.ts`, `hash.ts`, `webgl.ts`, `result.ts`. |
| `ui/elements/src/payloads/` | zod schemas mirroring `ui/src/data/` (`topology.ts`, `timeline.ts`), and the binary projection decoder (`projection.ts`). |
| `ui/elements/src/topology/` | `model.ts` (payload → drawn graph, collapse, selection, highlight), `layout.ts` (seeded ForceAtlas2), `style.ts`, `tooltip.ts`, `diamond-program.ts` (sigma node program), `element.ts`. |
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
   packed f32 coordinates with u32 indices into category tables).
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
    outcome: Applied | Rejected(error) }`, append-only, covering every
    operator action and config change; `operators()` with display names.

22. **Evidence content.** `transmission(id)` returns the matched text:
    `TransmissionEvidence { transmission, matches: Vec<MatchEvidence {
    content_match, origin: Excerpt, read: Excerpt }>, accesses:
    Vec<AccessDetail { access, resource }>, verdicts }`. An `Excerpt` is
    text around the matched range with the highlight checked to lie on
    character boundaries. Needs `Content`.
23. **Search by text.** `search` takes `SearchRequest { text, mode: Text |
    Semantic | Hybrid }` and the gateway embeds the text; the UI never
    handles embeddings.

### Actions

13. **`act` returns an `ActionOutcome`**: `RuleCreated(id)`,
    `ChannelPromoted(id)`, `Merged(MergeId)` or `Applied`.
14. **Rename**: `Agent.label: Option<AgentLabel>` (checked: trimmed,
    non-empty, bounded) and `RenameAgent { agent, label }`. Renaming a
    merged agent is a typed conflict.
15. **Unmerge**: `Merged` keeps `prior: ActiveAgentState` (`Registered |
    Provisional | Established`); merges are logged as `MergeRecord { id,
    from, into, by, at, repointed }`; `Unmerge { merge }` reverts exactly
    that record; a `MergeVeto { a, b, by, at }` stops the resolver from
    re-merging an operator-split pair, and an operator merge clears it.
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
    (`SuppressReason::OperatorRejected`).
18. **Rules**: built-in rules (one each, enable or disable only) are split
    from operator rules (`WatchedTopic`, `SemanticQuery`: create, edit,
    disable). Rules are never deleted. `AlertRuleDef` gains `name`,
    `created`, `sinks`. `SemanticQuery` stores its text and embedding
    model with the embedding. `Stale` carries a reason (`TopicsUnmapped`
    or `EmbeddingModelChanged`). Actions: `CreateRule`, `UpdateRule`
    (re-targets a stale rule), `SetRuleEnabled`.

### Wire

19. **JSON** for every request and response, ids as strings.
20. **Live updates**: an SSE stream of `UiEvent` (`AlertChanged`,
    `ChannelChanged`, `AgentChanged`, `RuleChanged`, `Watermark`,
    `TopicVersionReady`, `ProjectionReady`), ids only.
21. **Typed errors**: `QueryError` gains `Forbidden { missing }`,
    `VersionNotRetained { version }`, `Conflict(ConflictKind)` and
    `InvalidInput(InputError)` in place of `BadRequest { reason: String }`.
