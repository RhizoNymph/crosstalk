# Follow mode

Status: **design**, with every decision decided ([Decisions](#decisions)).
Nothing here is implemented yet. This document is the
design and the requirements it puts on the spec (`crosstalk-spec`), the
gateway and the fixture.

Follow mode keeps an operator UI view current while a real gateway keeps
producing data. A followed view's time window ends at the present and slides
forward one bucket at a time. New transmissions, channels, agents and alerts
show up without a reload, and the graph and projection keep their layout,
camera and selection while they do. A followed view can be pinned at any
moment into the citeable, reproducible URL every view has today, and a
pinned view can be followed again.

It builds on the UI described in [ui.md](ui.md), especially [View state and
URLs](ui.md#view-state-and-urls), [Live updates](ui.md#live-updates) and the
[element contract](ui.md#element-payloads), and on the spec's live feed
(`spec/types/interfaces/l8_surface/live.rs`, [query_surface.md](query_surface.md)).

## Scope

- How follow mode appears in the URL and the view state, how a followed
  view resolves to a concrete window on every render, and how it is pinned,
  unpinned, linked and cited.
- Which clock the window follows, and how the not-yet-final part of the
  data (the provisional tail) is shown.
- The refresh mechanism: what tells a page to re-render, how the WebGL
  elements survive the re-render, and how they update incrementally.
- What each screen refreshes, and how: topology (graph, time brush, lists,
  drawer), explore (projection, search, topics), the lists, the overview.
- Load and backpressure: debouncing, refresh intervals, hidden tabs, the
  back/forward cache, connection limits, and cost on the UI server and the
  gateway.
- The precise list of what the spec and the gateway must add
  ([Spec requirements](#spec-requirements)).
- The fixture changes that let follow mode be built and tested before the
  gateway exists: a deterministic trickle of new traffic on a controllable
  clock.
- The testing strategy and an ordered implementation plan split into
  parallel workstreams with file ownership.

## Non-scope

- Changing what a pinned URL means. Pinned views stay exactly as they are
  today: absolute, bucket-aligned windows with a pinned topic version.
- Following the topic-model version. A followed view keeps its `v`. A
  newly active version is announced, never switched to automatically
  ([Topic version](#topic-version-while-following)).
- Live audit log and dead-letter views. Audit entries and dead letters are
  not feed entities. Those pages accept the follow key but do not refresh
  themselves.
- Streaming exports. An export already reads only settled data, so a
  followed view's export is the export of its resolved window.
- Multi-tab sharing of one feed connection (`SharedWorker`,
  `BroadcastChannel`). It is noted as a later option under [Load and
  backpressure](#load-and-backpressure).
- Implementing the spec or gateway changes. They are listed for the
  bottom-up effort that owns `spec/types/` and the gateway.

## Summary of the design

1. **One new page key, `follow=<span>`,** replaces `from`/`to` on a
   followed page's URL (`/topology?follow=24h&v=2&w=tx&g=agents`). On every
   render the page resolves it to a concrete window `[head − span, head)`,
   where `head` is the present rounded up to a bucket boundary. Everything
   below the page (shard arguments, element `data-src`s, form actions,
   exports) gets that resolved, pinned window. Only the page URL follows.
2. **The window follows the present, not the watermark.** The watermark
   trails the newest data by the settling delay (`settle_after`, the
   evidence window plus the suspected TTL) and stands still while any
   consumer lags. The part of the window after the watermark is shown as
   provisional: it is hatched in the time brush, labelled in the header, and
   counted with a "not final" note.
3. **Refresh is a whole-page server re-render that is merged into the
   existing DOM.** A page that watches anything renders `<ct-live>`. When an
   event concerns the page, `<ct-live>` changes its `value`. A signal the
   page reads on the server takes that value, so Topcoat re-renders the page
   with the browser's current signal values and merges the result into the
   existing DOM. A spike verified this:
   - custom elements with a stable `id` keep their DOM node, WebGL context,
     camera and listeners;
   - their changed attributes are applied, so a changed `data-src`
     triggers a refetch;
   - bindings and signals keep working.

   This replaces the undocumented dev-refresh hook `<ct-live>` uses today
   on every page (D6, accepted default), and it lets the topology and
   explore pages refresh at all.
4. **Elements update in place.** A `data-src` change that differs only in
   `from`/`to` (a slide), or a change of the new `data-rev` input (same URL,
   newer data), refetches quietly and merges the payload. There is no
   loading veil, and the element keeps its camera, layout and selection. Any
   other `data-src` change reloads as today.
   - The topology graph keeps every existing node's position and places new
     nodes with ForceAtlas2 while existing nodes are `fixed`.
   - The time brush stays anchored to the right edge.
5. **What triggers a refresh:**
   - entity events a page watches (as today);
   - a new `head` event the UI's `/data/live` route sends at each bucket
     boundary (the slide);
   - `watermark` events for windows that are not yet final;
   - until the spec adds a traffic event (S3), a periodic poll while
     following (default 30 s).

   Refreshes are throttled to one at a time, at most one per page interval
   (5 s, or 10 s for topology and explore; D7, accepted default). They
   pause while the tab is hidden or a form is being edited.
6. **The spec must add** the present and the bucket width to `QueryApi`
   (S1, S2), a coalesced traffic event naming the buckets that changed (S3),
   channel events for listing flips caused by traffic (S4), data revisions
   for conditional refetch (S6), and projection extensions that place new
   points on an existing fit (S8). See [Spec requirements](#spec-requirements).
7. **The fixture gains a controllable clock and a trickle generator**
   that reveal deterministic new traffic, channels, agents, alerts and
   watermark advances as the clock moves. The clock can be real time
   (serving), scaled (demos) or test-driven (`Clock::Manual`).

## Terms

| Term | Meaning |
| --- | --- |
| bucket width | The edge store's bucket width (`EdgeStore::bucket_width`; five minutes in the fixture), today read through the gap trait `Present::bucket_width`. Every window is on its boundaries. |
| present | The gateway's clock (`Present::now` today, `QueryApi::now` after S2). |
| head | `align_up(present, bucket)`: the end of a followed window. The last bucket of a followed window, `[head − bucket, head)`, is in progress. |
| span | How much a followed window covers: a whole number of buckets (`FollowSpan`). |
| watermark | L7's `Watermark`: every bucket ending at or before it is final (`aggregates/watermark.rs`). |
| provisional tail | The part of a window after its watermark: `[max(start, watermark), end)`. Its buckets can still change. |
| slide | The head moving to the next bucket boundary, so a followed window's `from` and `to` both move by one bucket. |
| refresh | One server re-render of a page, merged into the DOM. |
| generation | `<ct-live>`'s `value` in follow mode: a decimal counter that increases by one per refresh it asks for. |

## View state and URLs

### The `follow` key

`follow=<span>` is a shared view-state key, like `from` and `to`. A span is
`<n><unit>` with `n` a positive decimal integer and `unit` one of `m`, `h`,
`d`. The canonical form uses the largest unit that divides the span exactly
(`90m`, `6h`, `1d`, `7d`). A span must be a whole multiple of the bucket
width and at most 31 days: the time brush draws at most 1000 hourly buckets,
and a month of five-minute buckets is the most a graph query should
aggregate on every refresh. The UI offers presets (15m, 1h, 6h, 1d, 7d),
and any other valid span is accepted (D11, accepted default).

- **`follow` and `from`/`to` are exclusive**
  (`ViewStateError::FollowWithWindow`). A followed URL is
  canonical when it carries `follow`, `v`, `w` and `g`, and pages redirect
  an incomplete one, as they do today. Example:
  `?follow=24h` → `?follow=1d&v=2&w=tx&g=agents`.
  A URL carrying both `follow` and a window is a 400 naming `follow`, like
  any other invalid view-state key today.
- **Page links carry `follow`.** Navigation, the shared filter's links,
  mode and weighting toggles, and links between sections keep a followed
  view followed. The layout's `current_state` renders the page query.
- **Data routes and shard arguments never take `follow`.** They take the
  resolved pinned window. `data::query::parse_strict` and
  `pages::view::state_from_query` refuse `follow` with a 400 naming it.
  So every request below the page is reproducible and cacheable, and a page
  and its shards and elements always agree on one window: the page resolved
  it once.
- **The overview and topology default to following** (D2, decided by the
  user). `/` and `/topology` without a window redirect to `follow=1d`
  rather than to a pinned last 24 hours, so an operator who opens the UI
  sees it move.
  - The other pages keep today's default, a pinned last 24 hours.
  - Navigation links carry the current page query, so moving on from a
    followed overview or topology keeps following.

### Types

The parsed state keeps today's `ViewState` with a concrete, aligned window,
so every page, shard, route and helper that reads `state.scope.window` is
unchanged. Following is a property of the page's URL only:

```rust
/// A followed window's length: a whole number of buckets, at most
/// `FollowSpan::MAX` (31 days). Built only by `FollowSpan::new`, which
/// checks both against the bucket width.
pub struct FollowSpan { micros: NonZeroU64 }

pub enum FollowSpanError { Unaligned, TooLong, Unreadable }

/// How the page URL fixes its window.
pub enum Anchor {
    /// `from`/`to` in the URL: exactly today's behaviour.
    Pinned,
    /// `follow=<span>`: resolved against `head` on every render.
    Following { span: FollowSpan, head: Timestamp },
}

/// What a page renders from: the resolved state (window concrete and
/// aligned, as today) and how the URL anchors it.
pub struct PageView {
    pub state: ViewState,
    pub anchor: Anchor,
}

impl PageView {
    /// The page's own URL query: `follow=<span>` when following, else the
    /// state's `from`/`to`. For links between pages.
    pub fn page_query(&self) -> String;
    /// The resolved window as `from`/`to`: for data routes, shard
    /// arguments, form actions, exports and "Pin".
    pub fn pinned_query(&self) -> String; // = self.state.to_query()
    /// This view followed with its current window's span: for "Follow".
    pub fn follow_query(&self) -> Option<String>;
}
```

- `RawViewState` gains `follow: Option<String>`. `ViewState::parse` stays
  pure: it returns `Parsed` with a new `WindowSpec { Pinned(TimeWindow) |
  Following(FollowSpan) }`. `pages::view::view_state` resolves `Following`
  against `defaults.head`, so the present is read once per render through
  the backend's trait, as the default window is today.
- `Defaults` gains `head: Timestamp` (already computed as the default
  window's end) and `watermark: Option<Watermark>`, which is optional
  because it costs a call. Pages that show the provisional tail read it from
  their aggregate's `Watermarked`, not from here.
- `FollowSpan::new(micros, bucket)` is the only constructor, so an
  unaligned or over-long span cannot reach a page. `span_text(span)` gives
  the canonical text.

### Which clock the window follows

The head is `align_up(present, bucket)`, the same end today's default window
uses, so the followed window holds the newest data the gateway has, settled
or not. The user decided (D1) that a followed view follows the present and never
lags behind it by anchoring at the watermark. The reasons the watermark was rejected
as the anchor:

- It trails the present by `settle_after = evidence_window + suspected_ttl`
  (`aggregates/watermark.rs`, `derived/flow/timing.rs`), a configured delay
  that may be tens of minutes. A view anchored there would look frozen,
  since the newest data is exactly what an operator following the gateway
  wants to see.
- It stands still while any consumer lags or a delivery sits
  dead-lettered, then jumps. A watermark-anchored view would freeze during
  incidents, which is when it matters most.
- Both the head and the watermark move in whole buckets, so neither gives
  a smoother slide.

There is no settled-only variant (a window ending at the watermark). Under
D1 a followed window always ends at the head, and the provisional tail is
marked rather than cut off.

### The provisional tail

A followed window always has a provisional tail. A pinned window has one
when its end is after the watermark. Each surface shows it:

- **Page header** (every windowed page). "final up to 14:05 · 14:05–14:25
  provisional". The watermark comes from the page's main `Watermarked` read.
  This is today's "final up to" with the tail made explicit.
- **Time brush.** It already hatches non-final buckets (`final` in the
  timeline payload). In follow mode the brush's right edge is the head, and
  the in-progress bucket is drawn as a partial bar with its hatch.
- **Topology.** Graph aggregates have no per-edge finality. The graph's
  meta line says "includes 20 min of provisional data", and the header
  counts carry the same note.
- **Overview tiles.** The activity tiles carry a small "provisional after
  14:05" caption. The queues have no settling point, as the spec says, and
  carry none.
- **Lists' windowed counts** (channels, agents). The column header says
  "in window, provisional after 14:05".

### Pin, unpin, copy and brush

- **Pin.** A followed page shows a follow bar (`components::follow`):
  "Following the last 1 d · final up to 14:05 · [Pin] [Link]". Pin is a
  plain link to `pinned_query()` (the window as resolved by this render,
  with the page's own keys). Following it stops the view, and the URL is
  then citeable. No script is needed: it is an anchor.
- **Unpin (follow).** A pinned page shows "[Follow]" next to its window, a
  link to `follow_query()` with the span of the current window (clamped to
  `FollowSpan::MAX`). It is offered on every pinned view, not only on views
  whose window reaches the present: "Follow" means "this span, ending now".
- **Copy link.** "Link" on a followed view is the pinned URL, the same
  href as Pin, so a copied link always reproduces what the copier saw,
  subject to the provisional tail settling (D3, decided by the user). A pinned window that
  reaches past the watermark says so in its header, so a citation is honest
  about it. The live URL is the address bar's. Cutting the copied link at
  the watermark was considered: it would make the citation final but drop
  the newest buckets the copier was looking at.
- **History.** A followed page never rewrites its URL when it slides. The
  URL stays `follow=1d`, so back and forward do not fill with one entry per
  bucket. Selections still rewrite `sel` with `history.replaceState` as
  today.
- **Brushing.** The time brush's handler (the topology page's `raw!`)
  compares the brushed end with the brush's last edge, the head
  (`data-head`):
  - If the brushed window ends at the head, it navigates to
    `follow=<to − from>`, so the view stays followed with the new span.
  - Otherwise it navigates to `from`/`to` without `follow`, which pins.

  The handler drops `follow` from the kept keys either way.

### Topic version while following

The URL's `v` stays pinned while following, so every linked view still
reads one version and a re-fit cannot change a followed view's topics under
it. When a `topic-version` event makes another version active, the follow
bar shows "v3 is now active · [Switch]", which links to the same followed
view with `v=3` (D4, accepted default). If a followed view's version is dropped by retention,
the page shows the typed `VersionNotRetained` error, as today, with the same
switch link.

## Refresh mechanism

### Today

`<ct-live>` sits in the root layout, outside `data-live-region="page"`.
When an event matches a page's `data-live-watch` token, it asks Topcoat's
dev-refresh hook (`topcoat:dev-runtime:v1`, not a public API) for the page
and swaps the region with `replaceWith`. Every element inside the region is
rebuilt, so topology and explore declare nothing and never refresh. A pinned
window re-renders the same window, so a `watermark` event shows no new data.

### Spike: a server-read signal re-renders and merges

Topcoat 0.9's documented behaviour (`docs/runtime.md`, "Reading signals on
the server") is that a signal read on the server with `.get()` makes a
browser-side change re-render the page. The page runtime (`PageUnit`)
re-runs the URL with the browser's signal values and merges the new body
into the old one with an idiomorph-style `morph`. Elements are matched by
tag and by persistent `id`, matched elements keep their node, and
attributes are synced.

A throwaway spike on this branch, reverted and not committed, checked it on
`/topology` in headless Chrome over CDP:

- A tracked `u64` signal in the topology page, set by a hidden button,
  re-rendered the page with one `POST` to the page URL.
- `<ct-topology id="graph">` was the same DOM node after the re-render. Its
  `data-src` was rendered from the signal (`w=tx` → `w=bytes`). The morph
  applied the new attribute, the element's `attributeChangedCallback` ran,
  and it fetched the new `/data/topology` and drew again.
- A second tracked signal in the root layout also re-rendered the whole
  page. The graph kept its node and, with an unchanged `data-src`, fetched
  nothing. `<ct-live>` kept its node, and no new `EventSource` was opened.
- Signals kept their values across both re-renders. After both, clicking an
  agent row still set the graph's `data-highlight`, the URL's `sel` and the
  row's `aria-pressed` through the existing bindings. No console errors.

So follow mode needs no region swap and no private hook. `refresh.ts` and
`data-live-region` are deleted.

### The new `<ct-live>` contract

`<ct-live>` moves out of the root layout into the pages that watch
something. It is rendered by one component, which also creates the refresh
signal the page reads on the server:

```rust
/// What a page shows, for live updates.
pub struct LiveSpec {
    /// `data-live-watch` tokens, as today (`alert`, `channel:<ulid>`, …),
    /// plus `traffic` for pages whose windowed counts change with new
    /// transmissions.
    pub watch: Vec<WatchToken>,
    /// The resolved window and how it is anchored.
    pub window: TimeWindow,
    pub anchor: Anchor,
    /// The page's main aggregate's watermark, when it has one.
    pub watermark: Option<Watermark>,
    /// The shortest time between two refreshes of this page.
    pub interval: Duration,
}

/// Renders `<ct-live id="live">` and creates the tracked refresh signal.
/// Returns the generation, for elements' `data-rev`.
pub fn live(cx: &Cx, spec: LiveSpec) -> (Generation, impl View);
```

- **Inputs** (attributes): `data-src` (`/data/live`); `data-watch`
  (tokens); `data-window` (`<from>/<to>`); `data-follow` (span in ms, absent
  when pinned); `data-head`; `data-watermark`; `data-interval` (ms). The
  morph updates them on every re-render, so `<ct-live>` always decides
  against what the page currently shows.
- **Output.** `value` is the generation (decimal text), and it changes,
  with `change`, only when the page should refresh. Today's `value` (the
  last event id) is dropped: nothing reads it, and announcing every
  heartbeat would re-render the page every 15 s.
- **Page side.** `@change=$(|e: Event| refresh.set(e.target.value))`, and
  the page reads `refresh.get()` on the server, which is the tracked read.
  The value is untrusted user input, but it is only a trigger: the server
  never interprets it beyond a length check (`Generation::parse`, at most
  20 digits).
- **Pages that watch nothing render no `<ct-live>`**, so they hold no feed
  connection (export, audit, pipeline). That also saves connections.

### What asks for a refresh

`<ct-live>` turns feed items into decisions with one pure function,
`decide(view, item) → Refresh | Ignore | Notice`, unit-tested over every
case:

| Item | Refresh when |
| --- | --- |
| entity event (`alert`, `channel`, `agent`, `rule`, `verdict`, `projection`, `topic-version`) | a watch token matches, as today. A `topic-version` event on a followed page also updates the follow bar's "now active" notice. |
| `head {head}` (new, UI-side, see below) | following, and `head` is after the page's `data-head` (the slide). |
| `watermark {at}` | the window has a provisional tail (`end > data-watermark`) and `at > data-watermark`, so some shown bucket became final. |
| `traffic {from, to}` (after S3) | the page watches `traffic` and `[from, to)` overlaps the window. This covers late confirmations into closed but not yet final buckets of pinned windows too. |
| `resync` | always (re-query everything shown). |
| poll timer (until S3) | following, and the page watches `traffic`: every 30 s, so new transmissions show within one poll interval rather than one bucket. |
| `pageshow` (restored from bfcache) or `visibilitychange` to visible after the stream was closed | always: events were missed. |

**The `head` event** is sent by the UI's `/data/live` route, not by the
spec. Each connection has a timer for the next bucket boundary of
`Present::now` (`QueryApi::now` after S2). At each boundary the route sends
`event: head` with `data: {"head": "<rfc3339>"}` and no `id:`, so it never
disturbs `Last-Event-ID` resumption. The route knows the gateway's present
and the bucket width, so the slide follows the gateway's clock, not the
browser's, and needs no clock-skew handling.

**Throttling.** Entity events keep today's 250 ms settle window. Then:

- At most one refresh is in flight.
- Refreshes start at least `data-interval` apart (default 5 s; 10 s on
  topology and explore, whose re-render is the heaviest).
- A decision that arrives during the interval or an in-flight refresh sets
  a dirty flag, which gives exactly one trailing refresh. Bursts collapse,
  and nothing is lost.
- Each `<ct-live>` adds a random 0–2 s delay to `head`-triggered refreshes,
  so many followers of the same view do not hit the server in the same
  second.

**Editing pauses refresh.** Topcoat's morph keeps the focused control's
value, but it resets other edited controls that are not signal-bound to the
server's markup. So `<ct-live>` defers a refresh while any form control
inside a `<form>` is focused or differs from its default. It reuses today's
`isEditing`, applied to forms only. The follow bar then says "paused while
you edit", and refresh resumes after `submit`, `reset`, or 10 s without
input once the control no longer differs. Controls outside forms (the
topology lists' filter boxes) must keep their state in a signal, so the
re-render restores it.

### Control flow

```text
gateway store commits ─▶ Changed ─▶ feed log ─▶ /data/live (SSE)  ◀── head timer (UI route)
                                                    │ entity | watermark | traffic | head | resync
<ct-live id="live" data-window data-follow data-head data-watermark data-watch data-interval>
   decide(view, item) ─▶ settle 250 ms ─▶ throttle (interval, one in flight, trailing)
   ─▶ hidden tab or editing? defer : value = generation + 1, fire `change`
page signal `refresh` ◀── @change            (tracked read on the server)
   ─▶ Topcoat PageUnit: POST page URL with signal values ─▶ server render:
        view_state: resolve follow ─▶ head = align_up(now); window = [head − span, head)
        QueryApi reads on the resolved window (pinned `v`, aligned)
        elements rendered with the resolved data-src (+ data-rev = generation)
   ─▶ morph body: id'd elements kept, attributes synced, bindings re-hydrated
        <ct-topology id="graph">: data-src changed only in from/to ⇒ slide ⇒ quiet refetch + merge
        <ct-timebrush id="brush">: slide ⇒ quiet refetch, anchored right
        rows with stable ids keep their DOM; new rows are marked
```

### Element contract additions

Applies to `PayloadElement` (`ui/elements/src/shared/element.ts`) and so to
every element:

- **`data-rev`** (new input). When it changes and `data-src` does not, the
  element refetches the same URL quietly and merges. Pages set it to the
  generation for elements whose data can change without a window change (a
  pinned window with a provisional tail, or new traffic inside a followed
  window between slides).
- **Slide.** When `data-src` changes and the old and new URLs differ only
  in the `from` and `to` query values (same path, same other pairs), the
  change is a slide: quiet refetch and merge.
- **Quiet refetch** shows no loading veil and keeps the drawing until the
  new payload is validated. On failure it keeps the old drawing and shows a
  small stale badge ("not updated: <error>") instead of the error panel. An
  explicit reload still shows the full error panel.
- **`merge(payload)`** is a new optional hook. Its default is `unmount()` +
  `mount()`, today's behaviour. Topology and the time brush implement it;
  the projection implements it with S8.
- **Any other `data-src` change** reloads exactly as today.

The classification is a pure function, `classifySrcChange(old, new) →
'slide' | 'reload' | 'same'`, unit-tested on URL pairs including key order
and percent-encoding.

## Per page

| Page | Watches (in follow mode, added) | Refresh | Elements |
| --- | --- | --- | --- |
| Overview `/` | `alert channel watermark` + `traffic` | whole page | none |
| Topology `/topology` | `channel agent verdict watermark traffic` (today: nothing) | whole page; the graph and brush merge | `<ct-topology id="graph">` slide/rev merge; `<ct-timebrush id="brush">` slide, anchored |
| Explore `/explore` | `projection topic-version watermark traffic` (today: nothing) | whole page; search results only on request | `<ct-projection id="projection">` frozen until S8, then extension merge |
| Topics `/topics` | `topic-version rule` + `traffic` | whole page | none |
| Channels `/channels` | `channel` + `traffic` | whole page; page 1 grows, later pages keep their rows | none |
| A channel `/channels/{id}` | `channel:<id> alert` + `traffic` | whole page | none |
| Agents `/agents`, an agent | `agent` + `traffic` | whole page | none |
| Alerts, an alert, rules | unchanged (not windowed) | whole page (as today) | none |
| Evidence `/transmissions/{id}` | unchanged (not windowed) | whole page (as today) | none |
| Export, audit, pipeline | nothing | none (resolved once on load) | none |

"Whole page" means the merged re-render above. It is the same mechanism
for every page, and with merging there is no need for finer shards.
Shards stay what they are today: regions that re-render on their own
signals, such as the topology drawer on `sel` and `cursor`. A page
re-render renders them again with the resolved state they are passed.

### Overview

Re-rendered in full: one `overview` call, one `topology` call (the five
heaviest edges), one `alerts` call and names. On the fixture this is about
22 KB in 140 ms on a debug build (measured). The tiles carry the
provisional caption. The heaviest-edges list and the newest open alerts
have stable row ids, so they keep their nodes and a new row is marked (see
[Lists](#lists-and-pagination)).

### Topology

The re-render recomputes:

- the header counts;
- the filter choices: the agents in the window's graph and the listed
  channels, which can grow as channels appear;
- the lists;
- the drawer, through the shard and its resolved state argument.

The lists' rows get stable ids (`agent-<ulid>`, `channel-<ulid>`) so they
keep their nodes, their `aria-pressed` and the list's scroll position. The
list filter text moves into a signal, so it survives the re-render and the
`raw!` binding re-applies it. The graph and the brush have stable ids and
merge.

**The graph (`<ct-topology id="graph">`).** `merge(payload)` replaces
`#draw()`'s kill-and-rebuild for slides and revisions:

1. Build the new `GraphModel` (`buildModel(payload, collapse)`), as today.
2. Diff by node id and edge key against the graphology instance sigma
   draws:
   - Removed nodes and edges are dropped (`graph.dropNode`,
     `graph.dropEdge`).
   - Kept ones get their attributes updated (size, label, colour, edge
     width, curvature; `mergeNodeAttributes`), keeping `x` and `y`.
   - Added ones are placed (steps 3 and 4).

   Sigma listens to the graph's `nodeAdded`, `nodeDropped`, `edgeAdded`
   and attribute events and re-indexes on its own. The renderer is not
   killed, so the camera, the WebGL context, hover state and the
   reducers' highlight stay.
3. Seed each added node at the centroid of its already-placed neighbours,
   plus a jitter of 2% of the current extent from `unitHash(id)`. A node
   with no placed neighbour (a new component) is seeded on a ring just
   outside the current extent at an angle from `unitHash(id)`.
4. Run ForceAtlas2 on the merged graph with every kept node marked
   `fixed: true`. `graphology-layout-forceatlas2` 0.10.1 reads the `fixed`
   node attribute into its node matrix (`helpers.js`) and never moves such
   nodes. Use the same settings as `layout.ts` and
   `min(60, iterationsFor(n))` iterations.

   A spike on a 40-node, 80-edge graph (in the scratchpad, not committed)
   checked this. After 800 iterations of the full layout, three new nodes
   were attached and 60 iterations were run:
   - existing nodes moved by exactly 0;
   - the new nodes settled 6–15 units from their neighbours in a graph
     about 53 units wide;
   - the run took 0.7 ms.
5. Freeze sigma's normalisation. Sigma rescales graph coordinates to the
   node extent on every refresh (`autoRescale`), so a node landing outside
   the extent would visually shift every node. At mount, the element calls
   `renderer.setCustomBBox(renderer.getBBox())`.

   When a merge places a node outside the box, it widens the box (with 10%
   padding) and corrects the camera in the same frame. The correction is
   the affine map from old to new normalised coordinates, applied to the
   camera's `x` and `y`, with `ratio` scaled by the new extent over the old.
   Existing nodes then stay on the same pixels.
6. Recompute the highlight from the current selection. A selected node or
   edge that left the window keeps its selection value, so the URL's `sel`
   and the drawer stay. The drawer says "not in this window"; edge stats
   come from `topology`, and an absent edge has none. The graph dims
   everything.
7. Re-render the legend only if the set of route kinds or policies
   changed, because the legend's height moves the stage.

A changed `data-collapse` and an explicit reload still run the full
deterministic layout. Merged layouts are deterministic given the previous
positions and the payload (no randomness), but they depend on the path: a
followed graph can differ from a fresh load of the same pinned URL. The
graph's meta line offers "re-layout", which runs the full layout on the
current payload (D9, decided by the user). The invariant "same payload, same picture" becomes
"same payload, same picture at mount".

New and removed nodes are not animated in v1. If they need emphasis, a
short highlight ring on nodes added by the last merge (cleared on the next
merge) fits the existing reducers.

**The time brush (`<ct-timebrush id="brush">`).** Its payload is small (at
most 1000 buckets; 19 KB for a week of hourly buckets on the fixture), so
`merge` refetches it whole and redraws. In follow mode the page renders
`data-follow` and `data-head`:

- The brush rectangle is drawn from `data-from` and `data-to`. When those
  slide, the brush moves with them, anchored to the right edge.
- During a drag (`#drag` set), a merge stores the new payload and applies
  it on pointer-up, so the axis never moves under the pointer. The pending
  selection is emitted against the axis the user dragged on. Its edges are
  absolute times, so applying it to the new axis is exact.
- The in-progress bucket at the head is drawn as a partial bar, hatched as
  non-final.

The brush's context window (the last week to the head, `brush_window`)
already ends at `max(window end, now)`, so it extends by itself on each
slide.

**The drawer.** It is a shard, re-rendered with the page:

- Edge transmissions (newest confirmation first, twelve per page) grow on
  page 1.
- With the drawer's `cursor` signal set, the page is fixed by its keyset
  cursor, as the spec promises: "concurrent inserts and removals never make
  a traversal skip or repeat an item".
- An agent or channel card refreshes its counts and listing. A channel
  confirmed while followed changes from "unconfirmed" to "confirmed" on the
  next refresh.

**Channels appearing and being confirmed while followed.** These are
consequences of the [channel semantics](../handoff/channel-semantics.md):

- A channel discovered by its first cross-agent transmission appears in
  channels mode as a new diamond, placed by the merge, and in the Channels
  list. Unless `u=confirmed` is set, it appears unconfirmed and marked.
- When its first confirmed transmission arrives, its node label loses
  "(unconfirmed)" and its colour and tooltip update as a kept node's
  attributes, and its list row's badge changes.
- Under `u=confirmed`, the channel appears only then, as an added node.
- A merge that hides a channel (`agent` event) removes its node and row.

Each of these needs the page to learn about the change. A traffic refresh
(the poll, or S3) covers them. S4 makes the confirmation flip explicit.

### Explore

**Projection.** The spec's stored projections are frozen by design
(`aggregates/projection/mod.rs`): "a projection is computed once and stored,
and a cited view is read back exactly", and "Points are frozen at fit
time". There is no transform method, and the frame has no way to add
points. So:

- **v1 (no spec change).** In follow mode the explore page shows its
  projection as today. The panel's existing "fitted for another window"
  flag fires as soon as the window slides, with its re-fit form. The
  element is not refreshed (`data-src` names a projection id, which does
  not change).
  - The frame carries each point's `confirmed_at` (`FrameColumns`), so the
    element can hide points that slid out of the window with
    regl-scatterplot's `filter(indices)`. That needs a `confirmed_at`
    column in the binary payload (format version 2: `8n` bytes of `u64`
    microseconds).
  - The lasso results shard applies the same window, so both sides select
    the same points.
- **With S8 (projection extensions).** L6 keeps each ready projection's
  fitted model and, as the watermark advances, places transmissions
  confirmed after the fit's window and before the watermark onto the
  existing fit with UMAP's `transform` (or a parametric encoder). It
  stores each batch as an immutable, numbered extension.
  - The payload gains an extension section (same columns, plus an
    extension-sequence column). `data-src` names the projection and the
    highest extension sequence the page rendered (`?through=<seq>`).
  - On a `projection` event (`ProjectionExtended`, S8) the page re-renders
    with the new `through`. The element merges: it calls
    `draw(points, { preventFilterReset: true })` with the frame followed
    by every extension, keeping the camera. regl-scatterplot keeps its
    camera across `draw`; the element must not call `reset` or
    `zoomToPoints`.
  - Extension points are drawn with a ring marker or reduced opacity, and
    the legend says "placed onto the fit, not fitted".
  - Transmissions confirmed after the watermark (the provisional tail) are
    not projected. The panel says "N newer transmissions not yet projected
    (settle after 14:05)".
- **Lasso selections survive extensions.** A lasso is a polygon in
  projection coordinates (`ps=lasso:…`), and an extension never moves
  existing points or changes the coordinate space.
  - After a merge the element re-selects with `pointsInPolygon` over frame
    and extensions, so new points inside the polygon join the selection.
  - The results shard resolves the polygon against the frame plus
    extensions up to `through`, the same set the element drew. A pinned
    URL carries `pe=<seq>` (explore page key: extensions included), so a
    cited lasso resolves to the same points forever.
  - A point selection (`point:<ulid>`) is an id and survives anything.
- **Lasso selections do not survive a re-fit.** A new fit is a new
  projection id in a new coordinate space. Switching to it (the panel's
  "Re-fit for this window", or the drift suggestion below) navigates with
  the new `p` and drops `ps`. The previously selected transmission ids are
  offered as a highlight in the new fit while they fit in one
  `TransmissionSelection` (`TransmissionSelection::MAX`).
- **Transform or periodic refits.** Extensions are preferred: they keep
  the picture stable, cost a nearest-neighbour search per new point instead
  of a full UMAP fit, and keep citations exact. They drift, though. Once
  most points were placed rather than fitted, the layout no longer reflects
  the data's structure.
  - S8 carries the share of placed points. The UI suggests a re-fit above
    50% placed (D8, decided by the user), as an operator action, never automatically, because
    a re-fit changes the picture.
  - A gateway-side periodic refit with Procrustes alignment to the
    previous fit (S8b) is the alternative if transforms prove too
    expensive or poor. The UI treats an aligned refit like any new
    projection id.

**Search.** Hits are in rank order (`(score, TransmissionId)` keyset), so
new hits would land in the middle of the list and reorder it under the
reader. In follow mode the hits are not re-run on refresh (D5, accepted default). The results
header says "results for 13:05–14:05 · window has moved · [Update
results]", and the button re-submits the search over the resolved window.
The topic sidebar (`topic_sizes`, sparklines) is re-rendered with the page.

### Lists and pagination

Channels, agents, alerts, a channel's suspected transmissions and the
drawer's edge transmissions are keyset-paginated, newest first by a key that
never changes (`paging.rs`; ids for channels, agents and alerts, which are
time-sortable ULIDs, `ids.rs`). In follow mode:

- **Page 1** (no cursor) is re-rendered and grows at the top. Rows carry
  stable ids (`row-<ulid>`), so the morph keeps existing rows' nodes.
  - `<ct-live>` watches for rows (`[data-live-row]`) the merge adds, with a
    `MutationObserver` armed when it fires `change` and disarmed when the
    page runtime's hydration finishes. It marks them with `data-live-new`,
    which the stylesheet fades out over 3 s.
  - Rows pushed off the bottom move to page 2. The "next" cursor is the new
    last row's key, so going on never skips or repeats.
- **Later pages** (cursor set) keep their rows, because the keyset cursor
  fixes membership, and refresh their counts.
  - When an entity event names an id of the list's kind that is newer than
    the page's first row, a banner says "newer rows · [back to newest]".
    ULIDs compare by time, and the lists are sorted by id, so no query is
    needed.
  - A changed old channel (a policy decision) has an older id and raises
    no banner.
- **Tabs whose membership is read from traffic** move rows between them.
  The Unconfirmed tab loses a channel when it is confirmed, and the
  Declared tab loses a declaration when traffic first crosses it. These
  arrive as traffic refreshes, or explicitly with S4.

### Overview counts and alerts

The overview's activity counts are `topology`'s for the resolved window, so
a refresh moves them with the window. The provisional caption covers the
tail. The queues (open alerts, unreviewed and unconfirmed channels) are "as
of the read" and change on `alert` and `channel` events, as today. A new
alert fired by new traffic (a `NewChannel` on discovery) appears through its
`alert` event in the newest-alerts list, marked as a new row.

## Load and backpressure

### Browser

- **Debounce and throttle** as [above](#what-asks-for-a-refresh): 250 ms
  settle, one refresh in flight, an interval of 5 s (10 s on topology and
  explore), a trailing refresh on a dirty flag, and 0–2 s jitter on
  slides.
- **Hidden tab.** On `visibilitychange` to hidden:
  - `<ct-live>` stops refreshing. Decisions only set the dirty flag.
  - After 30 s hidden it closes its `EventSource`, which frees one of the
    browser's six HTTP/1.1 connections per host.
  - On visible, it reopens the stream (fresh, without a cursor) and
    refreshes once, which re-queries everything, as a `resync` would.
    Replaying from a cursor is not possible here, because a new
    `EventSource` cannot set `Last-Event-ID`, and is not needed.
- **Back/forward cache.** Today's `lifecycle.ts` behaviour stays: the
  stream closes on `pagehide` and reopens on a persisted `pageshow`. This
  was the fix for SSE connections held open in the cache starving the
  foreground page. Follow mode adds:
  - On `pagehide`, the head timer, the poll timer, the throttle timers and
    any in-flight refresh request (its `AbortController`) are cancelled.
  - On a persisted `pageshow`, one refresh runs immediately: the window
    has slid while the page was cached.

  `lifecycleStep` grows to cover `visibilitychange`, with a unit test per
  transition.
- **Connections.** Only pages that watch something hold a stream, one per
  tab. With many tabs over HTTP/1.1 the six-connection limit still bites.
  The hidden-tab close above relieves it, and serving the UI over HTTP/2
  removes it, which is recommended for deployment. A later option is one
  stream per browser through a `SharedWorker` that fans events out to tabs
  over `BroadcastChannel`.

### UI server

Measured on the fixture (debug build, one request each):

| Request | Size | Time |
| --- | --- | --- |
| `/` | 22 KB | 140 ms |
| `/topology` | 88 KB | 157 ms |
| `/channels` | 33 KB | 82 ms |
| `/agents` | 60 KB | 15 ms |
| `/data/topology` | 52 KB | 10 ms |
| `/data/timeline` (week, 169 buckets) | 19 KB | 18 ms |

A topology refresh is one page re-render, about ten spec calls:

- `topology`, plus `channel_topology` in channels mode or when an edge is
  channel-routed;
- `channels`, `topics`, names and the drawer's reads;
- the graph's `/data/topology`, two `series` for the brush, and the
  brush's `/data/timeline`.

At a 10 s interval that is about one spec call per second per followed
topology tab.

- **Coalescing.** Followers of the same view resolve to the same aligned
  window, so their requests are identical. A small cache in front of the
  backend (`app::AppBackend` becomes a caching wrapper that implements the
  spec traits by delegation) would answer them once. It is keyed by method,
  arguments (resolved window, filter with pinned version, weighting, page)
  and the caller's permission set. In-flight requests are shared
  (singleflight). Entries are dropped on any feed item that could change
  them (`traffic`, `watermark`, `head`, entity events by kind, `resync`).
  This is optional in v1 and becomes exact with S6's revisions.
- **Conditional element fetches.** Data routes answer with an `ETag`, a
  digest of the payload in v1 and of the request plus data revision after
  S6. A quiet refetch sends `If-None-Match` and skips the merge on 304.

### Gateway

- **Feed.** The traffic event (S3) is coalesced on the gateway, at most one
  per `LiveConfig::traffic_coalesce` (proposed 2 s), so a busy gateway
  sends one small event per interval per stream rather than one per
  transmission. Heartbeats and the head events are negligible.
- **Sliding-window queries.** A followed graph query re-aggregates the
  whole window every refresh. Every bucket but the provisional tail is
  final and never changes for an activated topic version (the watermark's
  promise), so L7 can cache partial aggregates of settled bucket ranges
  (for example hourly rollups per filter and version) and recompute only
  the tail. A slid query then costs O(new buckets + tail) instead of
  O(window). This is a performance requirement for the gateway (S-perf),
  not a type change.
  - Read-time resolution (merges, verdicts) invalidates such caches. The
    stores already announce those as `agent` and `verdict` changes.
- **Rates to plan for.** With ten operators each following one topology
  view at 10 s, expect about 10 graph queries per second, one per tab per
  interval for each of `topology`, `channel_topology` and the two series.
  The UI server's coalescing reduces that to one set per distinct view
  per interval.

## Spec requirements

For the bottom-up effort that owns `spec/types/` and the gateway. Priority
P0 is needed for follow mode against a real gateway; P1 makes it efficient
or complete; P2 is optional.

| Id | Pri | Requirement | Why |
| --- | --- | --- | --- |
| S1 | P0 | `QueryApi::bucket_width(&self) -> BucketWidth` | Moves `Present::bucket_width` (`ui/src/contract/present.rs`) into the spec. Every window is aligned to it; spans are multiples of it; the head is aligned up to it. |
| S2 | P0 | `QueryApi::now(&self, caller: &Caller) -> Result<Timestamp, QueryError>` (View) | Moves `Present::now` into the spec. The head is `align_up(now)`, and the UI's `head` events tick on its bucket boundaries. |
| S3 | P0 | A traffic change notification: `Changed::Traffic(TimeWindow)` and `UiEvent::TrafficChanged { buckets: TimeWindow }` | The only way a page learns that new transmissions or accesses landed, without polling. |
| S4 | P0 | `Changed::Channel` on every listing change caused by traffic | A channel confirmed, or a declaration first crossed, must move between tabs and show in `u=confirmed` views. |
| S5 | P2 | `LiveItem::Heartbeat { cursor, now: Timestamp }` | Lets any client tick without the UI route's timer. |
| S6 | P1 | Data revisions on `Watermarked` | Conditional refetch (ETag/304) and exact cache invalidation. |
| S7 | P2 | Delta queries for graphs | Deferred; criteria below. |
| S8 | P1 | Projection extensions: place new points on an existing fit | Following the projection without refits, with lasso selections that survive. |
| S8b | P2 | Aligned refits: `ProjectionParams::align_to: Option<ProjectionId>` | Alternative or complement to S8. |
| S9 | P2 | Expose `CorrelationTiming::settle_after` (View) | "Provisional data settles within ~X" in the follow bar. |
| S-perf | P1 | Sliding-window aggregation that reuses settled buckets | Keeps followed graph queries O(new buckets). |

Details:

- **S1.** No caller, no `async`: the bucket width is configuration and is
  fixed for the life of a gateway (L7 already has `EdgeStore::bucket_width`
  as a plain `fn`).
  - State that it never changes while the gateway runs. The UI caches it
    per process.
  - A restart with another width invalidates every cited URL's alignment.
    It is refused as `UnalignedWindow` today, which is correct.
- **S2.** The gateway's wall clock, the one that stamps event times. It is
  non-decreasing per gateway process and never before the exposed
  watermark.
  - The fixture answers its controllable clock's time.
  - Proposed doc: "View. The present: the time the gateway stamps on what
    it receives now. Every exposed watermark is at or before it; a window
    ending at `align_up(now, bucket_width)` holds every bucket that has
    data."
- **S3.**
  - **Publisher.** L7's `EdgeStore` publishes after committing any change
    to an edge or access bucket: `apply`, `apply_access`, a re-key from
    `activate` or classification, and `judge` when it changes a bucket.
    It publishes never before the change is visible to `topology`,
    `channel_topology`, `series`, `overview`, `edge_transmissions`,
    `channel_resources`, `agents`, `agent` and `topic_sizes`.
  - **Payload.** The smallest aligned window covering every bucket changed
    since the store's previous `Traffic`. This can include closed but not
    yet final buckets (late confirmations).
  - **Coalescing.** At most one per `LiveConfig::traffic_coalesce`, a new
    `LiveConfig` field checked to be positive and shorter than the
    heartbeat. A change is announced within one coalescing interval of
    becoming visible.
  - **Permission.** View. `UiEvent::from(Changed::Traffic(w))` is
    `TrafficChanged { buckets: w }`.
  - **SSE wire.** `event: traffic` with
    `data: {"from": "<rfc3339>", "to": "<rfc3339>"}`.
  - **Invariant.** Every committed bucket change is covered by some
    `Traffic` published after it became visible.
  - **Not covered.** Search-index and embedding freshness: a transmission
    becomes searchable when L6 embeds it, which may be later. The UI does
    not auto-refresh search, so this needs no event.
- **S4.**
  - Today `Changed::Channel` is published on "every detection change". The
    handoff notes that "no `Changed::Channel` is published for a read-time
    listing change" caused by merges, which readers learn from
    `Changed::Agent`.
  - Specify that L5 also publishes `Changed::Channel(id)` when
    `Listing::of(origin, traffic)` changes value because of traffic:
    - Unconfirmed → Confirmed (the first confirmed crossing transmission);
    - Declaration → Channel (the first crossing transmission through a
      declared channel);
    - a discovered channel's creation, which is already covered by
      discovery.
  - Also say explicitly that advancing `TrafficDetection::last_transmission`
    alone is not a change to announce, or every transmission would publish
    one.
- **S5.** Optional, P2. A heartbeat carrying `now` lets clients other than
  this UI compute the head and their clock skew without a timer of their
  own. With S2 the UI route does this itself, so it is not needed by this
  UI.
- **S6.** `Watermarked<T>` gains `revision: DataRevision`, an opaque value
  that is equal for two reads of the same request exactly when their values
  are equal.
  - In practice it is a pair: the edge store's committed-change sequence,
    and a resolution sequence advanced by merges, unmerges, verdicts and
    topic activation.
  - Lets data routes answer 304 and the UI cache invalidate exactly.
  - `ExportHeader` could carry it too, to make exports comparable.
- **S7.** Delta queries (`topology_delta(since: DataRevision)`) are
  deferred.
  - Shares are normalised over the window's total, and the window slides,
    so most edges change every slide anyway. For graphs under about 5,000
    edges a full payload (52 KB for the fixture's ~40 agents) is cheap,
    and the client-side diff in `merge` gives the stable picture.
  - Revisit if followed graphs exceed about 10,000 edges or payloads
    exceed about 1 MB. The shape would then be per-bucket edge rows added
    and removed since a revision, with raw counts, and the client computes
    shares.
- **S8.** Proposed shapes:

  ```rust
  /// New points placed onto a ready projection's fit, never moving the
  /// fitted points. Immutable once stored; read back exactly.
  pub struct ProjectionExtension {
      pub projection: ProjectionId,
      pub seq: ExtensionSeq,               // 1, 2, 3, … per projection
      /// Transmissions confirmed in this range that the projection's
      /// filter admits (under its pinned version), placed in sample-key
      /// order. Ranges of successive extensions are adjacent; the first
      /// starts at the fit's window end.
      pub covers: TimeWindow,
      /// The watermark when the batch was read: `covers.end` is at or
      /// before it, so an extension holds only settled transmissions.
      pub watermark: Watermark,
      pub points: ProjectionFrame,          // same columns as the fit
      pub placed: Placement,                // Transform { model: ModelDigest }
  }

  pub struct ExtensionSummary {
      pub through: ExtensionSeq,
      pub fitted_points: u32,
      pub placed_points: u32,               // drift: placed / (fitted + placed)
  }

  // Content.
  async fn projection_extensions(
      &self,
      caller: &Caller,
      id: ProjectionId,
      after: Option<ExtensionSeq>,
      page: &PageRequest<ExtensionList>,
  ) -> Result<Page<ProjectionExtension, ExtensionList>, QueryError>;
  ```

  - `Changed::Projection(id)` (and so `ProjectionReady`) is also published
    when an extension is stored. Renaming the variant to
    `ProjectionChanged` would be clearer.
  - `ProjectionInfo` gains the `ExtensionSummary`.
  - Requirements on L6:
    - keep the fitted model, or a parametric encoder, for as long as the
      frame (`frame_retention_days`);
    - extensions expire with the frame;
    - placement is deterministic given the model and the inputs;
    - extensions are built only up to the watermark, so a cited `(p, pe)`
      is final.
  - Sampling: an extension applies the fit's `ProjectionParams::limit` to
    the total, so a projection never exceeds `ProjectionLimit::MAX` points.
    Once full, it stops extending, and `ProjectionInfo` says so.
- **S8b.** A re-fit seeded with the previous fit's coordinates for shared
  points and aligned by Procrustes, so most points stay close. Useful if
  `transform` quality is poor for a given embedding model.
- **S9.** `QueryApi::settle_after(&self) -> Duration` (no caller;
  configuration).
- **S-perf.** No type change. See [Gateway](#gateway) above.

Already listed in ui.md's remaining gaps, and relevant here: `Watermark`'s
field is public, so a watermark off a bucket boundary can be built. Follow
mode compares watermarks with bucket edges in the browser too, so the spec
should make the boundary a checked property.

## Fixture changes

The fixture must produce a steady, deterministic trickle of new data so
follow mode can be built and tested before the gateway exists. The
conformance effort on `feat/ui-conformance` is extracting the fixture into
its own crate (`fixture/`), so paths below name the behaviour and today's
module. The work lands wherever the fixture lives when it starts.

### Clock

Today `clock::Clock` is `Fixed` (always `NOW`, tests) or `Live { started }`
(`NOW` plus real time since startup, serving). It becomes:

```rust
pub enum Clock {
    /// Always NOW: existing tests, unchanged.
    Fixed,
    /// NOW + (real time since `started`) × `scale`. `scale` 1 for serving;
    /// up to 60 for demos (a five-minute bucket every 5 s at 60×).
    Live { started: Instant, scale: NonZeroU32 },
    /// Driven by tests: `FixtureBackend::advance(by)` / `advance_to(at)`.
    Manual(Arc<ManualClock>),
}
```

`ManualClock` holds the current time in a `tokio::sync::watch` channel, not
shared mutable state, so the trickle task wakes when it moves. Advancing is
the only way time passes under `Manual`, so a test sees exactly the events
of the interval it advanced over.

### The trickle

The generated week stays as it is. It becomes the past, immutable, and
existing tests and scenarios are untouched. New data is revealed from an
append-only tail:

- **Generation in hour chunks.** Each hour after `NOW` is generated on
  demand from `Rng::fork(seed, ("tail", hour_index))` by the same
  generators the world uses (`world::traffic`, `states`, `evidence`,
  `blobs`, `topics`). The result is a deterministic list of pending records
  (transmissions with their accesses and evidence, new agents, new
  channels' first transmissions, verdict-free alerts), each with the time
  it becomes visible:
  - its open or confirmation time;
  - a channel's discovery time, which is its first crossing transmission's
    time;
  - an agent's first exchange.

  The chunk for hour `h` is generated when the clock enters `h`, with the
  current registry and identity table read under the store's lock, so new
  resources resolve through supersession and merges as of then. The tail
  is therefore a function of the seed, the clock and the sequence of
  operator actions, and of nothing else.
- **Revealing.** Pending records are not readable. When the clock passes a
  record's visible time, the record is moved into the tail (in `State`,
  behind the existing `RwLock`). Queries read the world and the tail
  through `Ctx` iterators that chain both, so no query can see a record
  before its time, and future data cannot leak through a forgotten filter.
- **Rates.** About one confirmed transmission a minute on the weekday
  curve (`trickle.per_hour`, default 60), with suspected ones among them
  and some confirmed a few minutes later. That covers late confirmations
  into closed, non-final buckets. On average:
  - a new discovered channel every two hours (`channels_per_day`,
    default 12);
  - a new agent every three hours (`agents_per_day`, default 8).
- **Scripted first hour,** so a demo or test sees every case within
  minutes:
  - at +2 min the in-flight transmissions of the generated world's last
    quarter hour resolve (confirmed or discarded), so nothing stays in
    flight forever;
  - at +5 min an unconfirmed discovered channel appears
    (`Scenario::live_unconfirmed`), raising `NewChannel`;
  - at +12 min it gets its first confirmed transmission (the listing
    flip);
  - at +15 min a new agent appears (`Scenario::live_agent`);
  - at +20 min a transmission crosses `docs.corp.internal/design`, the
    declaration awaiting traffic, which then counts as a channel.
- **Watermark.** `align_down(now − 10 min, BUCKET)`, as the static
  `WATERMARK` is today, so it advances one bucket every five minutes of
  clock time.
- **Alerts.** The built-in rules evaluate revealed records as L6 would:
  `NewChannel` on discovery, `SanctionedUnused` cleared by traffic,
  semantic and watched-topic rules by the fixture's embedder and topics.
  Alerts are created through the same store paths actions use, so
  deduplication and suppression apply.
- **Topics.** Revealed confirmed transmissions get topic assignments under
  the active version after a short classification delay (2 min), so "not
  classified yet" shows briefly.
- **Projections.** New fits sample the tail too. With S8, the fixture's
  stand-in layout (theme clusters) places extension points with the same
  function, which is deterministic per transmission. That is the
  fixture's transform.

### Feed events

The fixture publishes what each store would, after committing and before
releasing the write lock, through today's `live::Feed::publish`:

- `Changed::Channel` on discovery, a confirmation flip (S4 semantics) and
  a declaration's first crossing;
- `Changed::Agent` on a new agent;
- `Changed::Alert` on every alert opened or suppressed;
- `Changed::Watermark` on each advance;
- `Changed::Traffic` (once S3 exists) per coalescing interval, covering the
  revealed buckets;
- `Changed::TopicVersion` and `Changed::Projection` as today.

Under `Live`, a task reveals and publishes on a timer (each second of
clock time). Under `Manual`, `advance_to(at)` reveals and publishes
synchronously, in time order, before returning, so tests are exact.

### Configuration

`ui/config.json`'s `backend.fixture` gains an optional `trickle`
(`{ "per_hour": 60, "channels_per_day": 12, "agents_per_day": 8, "scale":
1 }`; absent means no trickle, exactly today's static world). Tests build
`FixtureBackend::try_manual(seed)`.

A test-only control route, `POST /_test/clock` (advance by a duration),
exists only in builds with the `sim-clock` cargo feature, so a headless
browser test can drive the clock (D10, accepted default). Release builds have no such
route.

## Testing strategy

Tests come first in every workstream. They are the acceptance criteria of
the implementation plan.

### Rust

- **View state** (`url/`): unit tests for `FollowSpan` (unaligned, zero,
  over 31 days, every unit, the canonical text: `90m`, `6h`, `1d`) and
  `ViewState::parse` with `follow` (exclusive with `from`/`to`, incomplete
  redirects, the canonical order of keys).
  - Resolution against a head: `[head − span, head)`.
  - `page_query`, `pinned_query` and `follow_query` round trips.
- **Router tests** (`ui/src/testing/`, `Session` over a manual-clock
  fixture):
  - `/` and `/topology` without a window redirect to `follow=1d`, and
    every other page without a window redirects to a pinned last 24 hours
    (D2);
  - a followed page renders a resolved window;
  - its element `data-src`s, shard arguments and form actions carry
    `from`/`to` and never `follow`;
  - data routes and shard arguments refuse `follow` (400);
  - links between pages carry `follow`;
  - Pin and Link hrefs equal the resolved window;
  - Follow hrefs carry the span;
  - advancing the clock by one bucket changes the resolved window on the
    next request;
  - `/data/live` sends a `head` event without an `id:` at each bucket
    boundary of the manual clock;
  - pages render `<ct-live id="live">` with the right `data-*` and no
    `<ct-live>` on export, audit and pipeline;
  - list rows carry stable ids;
  - the follow bar's provisional text uses the page's watermark;
  - "now active" appears after a `topic-version` activation.
- **Fixture tests** (the fixture's `tests/`):
  - The tail is deterministic: same seed, same clock steps, same records,
    same events.
  - No record is readable before its visible time. A property test over
    random advance steps checks every list and graph.
  - The scripted first hour shows, in order: the channel appears
    unconfirmed (one `Channel` event and one `NewChannel` alert), then is
    confirmed (one `Channel` event, the listing flips), and so on.
  - The watermark advances one bucket per five minutes with one
    `Watermark` event each.
  - Late confirmations land in closed, non-final buckets.
  - A merge after a revealed channel hides it as today.
  - The world's existing tests still pass on `Fixed`.
- **Conformance**: the suite on `feat/ui-conformance` gains live cases
  that a gateway must also pass: S3's coverage invariant, S4's
  announcements, and `now ≥ watermark`.

### TypeScript (vitest, `ui/elements/test/`)

- **`decide`**: a table over every feed item, anchor and window
  combination in [What asks for a refresh](#what-asks-for-a-refresh).
- **Throttle** with fake timers: the settle window, the interval, one in
  flight, the dirty flag giving exactly one trailing refresh, and the
  jitter bounds.
- **Lifecycle**: `pagehide`, persisted `pageshow`, `visibilitychange` (the
  30 s close, reopen and refresh on visible), and timers cancelled on hide.
- **`classifySrcChange`**: slides, reloads, identical URLs, reordered and
  percent-encoded pairs.
- **Topology merge** (pure functions over graphology, no WebGL): kept
  nodes keep `x`/`y` exactly; added nodes are seeded at their neighbours'
  centroid and stay within a bound after placement; removed nodes are
  dropped.
  - The result is deterministic for the same (positions, payload).
  - The bbox widening and camera correction keep a kept node's normalised
    screen position, to 1e-9.
  - Selection survives the selected node leaving the window.
- **Time brush**: anchoring to the right edge on a slide; a merge during a
  drag is deferred to pointer-up; the partial head bucket.
- **Projection** (with S8): the payload v2 decoder (`confirmed_at`, the
  extension section, strict sizes); window filtering; a lasso re-selected
  over frame and extensions; selection by id survives.
- **Lists**: new-row detection from a before/after id set; the "newer
  rows" banner from ULID comparison.

### Headless Chrome

These checks need a real browser for the morph, WebGL and the page
lifecycle. node0 has no Chrome, so they run on a workstation or CI host
with Chrome for Testing: this machine has `~/.local/bin/google-chrome`,
which works headless with `--screenshot` and over CDP. They are not part
of `cargo test` or `pnpm test`.

A new script, `ui/elements/scripts/follow-smoke.mjs`, in the style of
`smoke.mjs` and the spike that informed this design, runs against
`crosstalk-ui` built with `--features sim-clock` and a manual clock. It
advances the clock through `POST /_test/clock` and checks:

- `<ct-topology>` keeps its DOM node, camera state
  (`getCamera().getState()`) and the positions of kept nodes across slides;
- new nodes appear, and the selection and the URL's `sel` stay;
- the brush stays anchored to the right edge;
- a new channel's row appears marked in the lists;
- a hidden tab does not refresh, and refreshes once on visible;
- navigating away and back restores from bfcache, refreshes once, and
  leaves no extra open SSE connections (Chrome's
  `Network.getResponseBody` cannot see SSE, so count through
  `performance.getEntriesByType('resource')` and the server's connection
  log);
- Pin stops refreshing.

It writes screenshots in both colour schemes. It runs on demand
(`pnpm follow-smoke`) and in CI where Chrome exists.

## Implementation plan

Workstreams are split by area of concern, so separate agents can build them
in parallel in separate worktrees. Branch names follow the change's
function. Every workstream writes its tests first.

### Phase 1 (parallel, no dependencies between them)

| Workstream | Branch | Owns | Delivers |
| --- | --- | --- | --- |
| W1 Fixture live world | `feat/fixture-live-world` | the fixture's `clock`, `store` (tail), `queries/` (`Ctx` chaining world and tail), a new `world/tail/` (chunk generator, scripted first hour, reveal), `live/` (ticker publishing), `surface` (`Present::now` from the clock); `ui/src/config.rs` and `ui/config.json` (`trickle`); fixture tests | The manual and scaled clocks, the trickle, feed events (all but `Traffic`), `try_manual`, `advance_to` |
| W2 Follow view state | `feat/follow-view-state` | `ui/src/url/view_state.rs`, new `ui/src/url/follow.rs` (`FollowSpan`, `Anchor`, `WindowSpec`), `ui/src/pages/view.rs` (`PageView`, resolution), `ui/src/data/query.rs` (refuse `follow`), `ui/src/components/href.rs` (page vs pinned queries), new `ui/src/components/follow.rs` (follow bar, Pin, Link, Follow, provisional text), the layout's navigation links in `ui/src/pages/mod.rs` | Parsing, resolution, links and the follow bar, with router tests |
| W3 Live controller | `feat/live-controller` | `ui/elements/src/live/` (new `decide.ts`, `throttle.ts`, `rows.ts`; `element.ts` rewritten; `lifecycle.ts` gains visibility; `refresh.ts` deleted), `ui/elements/test/live.test.ts`; `ui/src/components/live.rs` (`LiveSpec`, `live()`, `Generation`), `ui/src/data/live.rs` (`head` events), the root layout's `<ct-live>` and `data-live-region` removal in `ui/src/pages/mod.rs` | The new `<ct-live>`, the refresh signal, `head` events. It keeps the ten watching pages working by moving their `live_watch` call to `live()` (a mechanical edit per page) |
| W4 Element merge contract | `feat/element-merge` | `ui/elements/src/shared/element.ts` (`data-rev`, `classifySrcChange`, quiet refetch, `merge` hook, stale badge), `ui/elements/src/shared/url.ts` (new), tests | The contract every element builds on |

Expected conflicts: W2 and W3 both edit `ui/src/pages/mod.rs` (navigation
links versus `<ct-live>` and the region), `ui/src/components/mod.rs` (module
lists) and `docs/features/ui.md`. These are acceptable; the merger resolves
them.

### Phase 2 (after their dependencies merge)

| Workstream | Branch | Depends on | Owns | Delivers |
| --- | --- | --- | --- | --- |
| W5 Topology graph merge | `feat/topology-merge` | W4 | `ui/elements/src/topology/` (`element.ts`, new `merge.ts`, `layout.ts` placement and the fixed-node FA2 run, the bbox and camera correction, "re-layout"), tests | In-place graph updates |
| W6 Time brush follow | `feat/timebrush-follow` | W4 | `ui/elements/src/timebrush/`, tests | Right-edge anchoring, deferred merge during drag, partial head bucket |
| W7 Page wiring: topology and explore | `feat/follow-topology-explore` | W2, W3 (W5 and W6 for the full effect, not to start) | `ui/src/pages/topology/` (ids, `data-rev`, the brush handler's follow rule, signal-held list filter, row ids), `ui/src/pages/explore/` (follow bar, search "update results", window filter on lasso results), `ui/src/data/projection/` (payload v2 `confirmed_at`), `ui/elements/src/projection/` and `payloads/projection.ts` (window filter) | Topology and explore followed |
| W8 Page wiring: other pages | `feat/follow-pages` | W2, W3 | `ui/src/pages/{overview,channels,agents,topics}/`, `ui/src/components/table.rs` (row ids), `ui/styles/app.css` (`data-live-new`), the "newer rows" banner | Every other windowed page followed |
| W9 Browser checks | `test/follow-smoke` | W1, W7 | `ui/elements/scripts/follow-smoke.mjs`, `ui/src/data/testing.rs` (`POST /_test/clock` behind `sim-clock`), `ui/Cargo.toml` feature | The headless checks |

Expected conflicts: W7 and W8 both touch `ui/src/components/` (row ids)
and the docs. W7 and W5 meet only at the element's public contract.

### Phase 3 (gated on the spec)

| Workstream | Branch | Gated on | Delivers |
| --- | --- | --- | --- |
| W10 Spec adoption | `feat/follow-spec-adoption` | S1, S2, S3, S4 | Delete `ui/src/contract/present.rs`; `traffic` tokens and events in `<ct-live>` and `/data/live`; drop the poll; the fixture publishes `Traffic` and S4 events |
| W11 Projection extensions | `feat/projection-extensions` | S8 | Fixture extensions, payload extension section, `through`/`pe` keys, element merge, drift suggestion |
| W12 Revisions and caching | `feat/follow-revisions` | S6 (v1 cache possible before) | ETag/304 on data routes, the coalescing backend wrapper |
| S* | the bottom-up effort | none | The [spec requirements](#spec-requirements) |

## Files

The design touches these files (existing unless marked new). Fixture paths
are relative to the fixture's crate root (`ui/src/backend/fixture/` today,
`fixture/src/` after the extraction).

| Path | Role in follow mode | Key interfaces |
| --- | --- | --- |
| `ui/src/url/follow.rs` (new) | The follow span and the anchor | `FollowSpan` (`new`, `MAX`, `text`), `FollowSpanError`, `Anchor`, `WindowSpec` |
| `ui/src/url/view_state.rs` | Parses `follow`, exclusive with `from`/`to` | `RawViewState::follow`, `Parsed::window: WindowSpec`, `ViewStateError::{Follow, FollowWithWindow}` |
| `ui/src/pages/view.rs` | Resolves a followed URL against the head once per render | `Defaults::head`, `PageView`, `view_state(cx) -> PageView` |
| `ui/src/data/query.rs` | Data routes refuse `follow` | `parse_strict` |
| `ui/src/components/href.rs` | Links carry the page query; data and shard URLs the pinned one | `href`, `page_href`, `pinned_href` |
| `ui/src/components/follow.rs` (new) | The follow bar: span, final-up-to, provisional tail, Pin, Link, Follow, "now active" | `follow_bar(view, watermark)` |
| `ui/src/components/live.rs` | Renders `<ct-live>` and the tracked refresh signal | `LiveSpec`, `WatchToken`, `live(cx, spec) -> (Generation, View)`, `Generation` |
| `ui/src/pages/mod.rs` | Root layout: navigation links with the page query; no `<ct-live>`, no `data-live-region` | `root_layout` |
| `ui/src/data/live.rs` | SSE wire: adds `head` events (and `traffic` after S3) | `LiveEvents`, `frame`, the head timer |
| `ui/src/pages/topology/` | Follow wiring: ids, `data-rev`, brush rule, list filter signal, row ids | `workspace`, `brush_window`, `lists::selection_sync` |
| `ui/src/pages/explore/` | Follow bar, search update, lasso window filter | `projection_results`, `search` |
| `ui/src/pages/{overview,channels,agents,topics}/` | Follow wiring, `traffic` tokens, row ids | each page's `live()` call |
| `ui/src/data/projection/` | Payload v2 (`confirmed_at`; extensions after S8) | `format::encode` |
| `ui/src/contract/present.rs` | Kept until S1 and S2; then deleted | `Present::{now, bucket_width}` |
| `ui/elements/src/live/element.ts` | `<ct-live>`: decides, throttles, pauses, fires the generation | `LiveElement` |
| `ui/elements/src/live/decide.ts` (new) | The pure decision function | `decide(view, item)`, `LiveView`, `Decision` |
| `ui/elements/src/live/throttle.ts` (new) | Settle, interval, one in flight, trailing, jitter | `Throttle` |
| `ui/elements/src/live/lifecycle.ts` | `pagehide`, `pageshow`, `visibilitychange` | `lifecycleStep` |
| `ui/elements/src/live/rows.ts` (new) | New-row marking and the newer-rows banner | `markNewRows`, `newerThan` |
| `ui/elements/src/live/refresh.ts` | Deleted (region swap through the dev hook) | none |
| `ui/elements/src/shared/element.ts` | `data-rev`, slide detection, quiet refetch, `merge` hook | `PayloadElement.merge`, `classifySrcChange` |
| `ui/elements/src/topology/merge.ts` (new) | Diff and in-place update of the drawn graph | `mergeGraph(graph, model, positions)` |
| `ui/elements/src/topology/layout.ts` | Seeding and fixed-node placement | `placeAdded`, `iterationsFor` |
| `ui/elements/src/topology/element.ts` | `merge`, frozen bbox, camera correction, re-layout | `TopologyElement` |
| `ui/elements/src/timebrush/element.ts`, `model.ts` | Anchoring, deferred merge, head bucket | `TimebrushElement` |
| `ui/elements/src/projection/element.ts`, `payloads/projection.ts` | Window filter (v1), extensions (S8) | `ProjectionElement`, `decodeProjection` v2 |
| `ui/elements/scripts/follow-smoke.mjs` (new) | Headless-Chrome follow checks | none |
| fixture `clock.rs` | `Fixed`, `Live { scale }`, `Manual` | `Clock`, `ManualClock` |
| fixture `world/tail/` (new) | Hour chunks, scripted first hour, reveal | `Tail`, `Pending`, `reveal_until` |
| fixture `store.rs`, `queries/` | The tail behind the lock; `Ctx` chaining world and tail | `State::tail`, `Ctx::transmissions` |
| fixture `live/` | The ticker publishing revealed changes | `Feed::publish`, `Ticker` |
| fixture `mod.rs` | `try_manual`, `advance_to` | `FixtureBackend` |
| `ui/src/config.rs`, `ui/config.json` | `backend.fixture.trickle` | `BackendConfig::Fixture { seed, trickle }` |
| `ui/src/data/testing.rs` (new, `sim-clock` only) | `POST /_test/clock` | none |
| `spec/types/interfaces/l8_surface.rs`, `l8_surface/live.rs`, `events/changed.rs`, `aggregates/watermark.rs`, `aggregates/projection/` | The spec requirements (owned by the bottom-up effort) | S1–S9 |

## Invariants and constraints

- **Only the page URL follows.** Every request a page makes (`QueryApi`
  calls, shard arguments, element `data-src`s, form actions, exports)
  uses the one window the page resolved, aligned to the bucket width.
  Data routes and shards refuse `follow`.
- **A followed window is `[head − span, head)`** with `head =
  align_up(present, bucket)`, and the span a whole number of buckets of at
  most 31 days. It is resolved once per render, from the backend's
  present, never from the browser's clock.
- **A pinned URL means exactly what it means today.** Pin and Link always
  give the resolved window, so a copied link reproduces what was on
  screen once the watermark passes its end.
- **The topic version never changes while following.** A new active
  version is announced, never applied.
- **A followed page never writes its URL when it slides.**
- **Refresh is a server re-render merged into the DOM.** Elements that
  must survive it have stable ids. Client-only control state that must
  survive it lives in signals.
- **At most one refresh is in flight per tab.** Refreshes start at least
  the page's interval apart, none run while the tab is hidden, and a
  refresh never overwrites a form being edited.
- **A slide or revision never resets an element's camera, layout or
  selection.** A merged graph never moves a node it already drew. A failed
  quiet refetch keeps the last drawing and says it is stale.
- **The provisional tail is always marked** wherever windowed numbers are
  shown.
- **The SSE stream closes on `pagehide` and after 30 s hidden**, and a
  page restored from bfcache refreshes once.
- **Events stay ids only.** The UI's `head` event carries a time, has no
  `id:`, and is not a spec event.
- **The fixture reveals nothing before its time.** Its state is a function
  of the seed, the clock and the operator actions. Under `Manual`, time
  passes only through `advance_to`.
- **Stored projections stay immutable.** Extensions (S8) add points beside
  a fit and never move fitted points. A cited `(p, pe)` resolves to the
  same points forever.
- **No new dependencies.** sigma 3.0.3, graphology 0.26.0,
  graphology-layout-forceatlas2 0.10.1 and regl-scatterplot 1.16.0 already
  provide what the merges need: graph events, `fixed` nodes,
  `setCustomBBox`, camera state, and `draw` that keeps the camera.

## Decisions

Every decision is decided. The user decided D1, D2, D3, D8 and D9. The
user did not comment on D4, D5, D6, D7, D10 and D11, so the
recommendations stand as accepted defaults.

| Id | Decision | Decided by | Not taken |
| --- | --- | --- | --- |
| D1 | Follow the present (head) and mark the provisional tail. A followed view never lags behind the present by anchoring at the watermark, and there is no settled-only variant. | the user | Follow the watermark (always final, but lags `settle_after` and freezes during stalls) |
| D2 | `/` and `/topology` without a window default to `follow=1d`. Other pages keep the pinned last 24 hours. | the user | Keep defaulting to a pinned last 24 h everywhere |
| D3 | Link and Pin give the resolved window, provisional tail included | the user | Cut the copied window at the watermark |
| D4 | Keep `v` pinned while following, and announce a new active version | accepted default | Follow the active version too |
| D5 | Search results refresh only on "Update results" | accepted default | Re-run search on every refresh |
| D6 | Replace the dev-hook region swap with signal-driven page re-renders on every page, and move `<ct-live>` into the pages that watch | accepted default | Keep the region swap for non-WebGL pages |
| D7 | 5 s refresh interval (10 s on topology and explore); close the stream after 30 s hidden | accepted default | Other values, or per-operator settings |
| D8 | v1 does not follow projections: it shows the "fitted for another window" flag and offers a re-fit. With S8, new points are placed onto the existing fit, with a re-fit suggested above 50% placed points. | the user | Periodic gateway refits (S8b) |
| D9 | Merged topology layouts depend on their history, with a "re-layout" control | the user | Always run the full deterministic layout (nodes jump on every slide) |
| D10 | A test-only clock route behind a `sim-clock` cargo feature | accepted default | Drive browser tests through a scaled live clock only |
| D11 | Any bucket-multiple span up to 31 days, with presets offered (15m, 1h, 6h, 1d, 7d) | accepted default | Presets only |
