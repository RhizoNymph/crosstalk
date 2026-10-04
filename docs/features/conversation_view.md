# Conversation view

Status: **design**. Nothing here is implemented. Phase B (implementation)
starts once `feat/ui` has been migrated onto `staging` and the spec has the
conversation reads proposed in `docs/handoff/conversation-view-spec.md`.
Until then no UI code is written against the older spec on this branch.

An operator page showing one agent's conversation turn by turn: what each
exchange read and wrote, and **where text came from and where it went**:
the tool result holding text agent B originated (with a link to that
transmission's evidence), the output of ours that agents C and D later read,
sub-agent delegations inline, harness claims, the conversation's origin,
compaction boundaries and WebSocket continuation increments. Investigators
use it to follow a thread across agents: from an evidence page into the
reader's conversation, back to the sender's turn, and on to whoever read
that.

## Scope

- `/agents/{id}/conversations`: an agent's conversations, newest first.
- `/conversations/{id}`: one conversation's head (origin, successors,
  delegation that started it, claims) and its turns in threading order,
  twenty at a time.
- `/exchanges/{id}` and `/spans/{id}`: citeable redirects to the turn an
  exchange is, or the turn holding a span (with that span highlighted).
- Provenance marks on every turn: inbound text from other agents, output
  spans with their readers, relayed text, delegations.
- Structure-only rendering for View; text for Content.
- Additive links from the agent page, the evidence page and the topology
  drawer.
- Fixture scenarios for conversations (forks, compaction, an increment with
  unseen history, a delegation, a mid-conversation system message, a
  dropped body).

## Non-scope

- Changing any existing page's behaviour beyond adding links.
- Live updates of an open conversation (no `Changed` variant is proposed;
  the head has a manual refresh link). Follow mode can add one later.
- Editing anything: the view has no actions. Verdicts stay on the evidence
  page.
- Search within a conversation, diffing forks, or rendering media bytes
  (media parts show their kind only).
- Reconstructing anything client-side: threading, spans, matches and
  transmissions all come from the gateway's reads.
- Common (boilerplate) spans; they are not shown.

## Routes and URL state

| Path | Page | Page keys |
| --- | --- | --- |
| `/agents/{id}/conversations` | conversations of the canonical agent `id` resolves to (an alias shows a banner, as the agent page does) | `cursor` (the list's `PageRequest<ConversationList>`), `origin` (comma list of `root,fork,compaction`; empty means all) |
| `/conversations/{id}` | the conversation page | `turn` (a `TurnIndex`), `hl` (a `SpanId` to highlight), `rcursor` (the open reader list's `PageRequest<SpanReaderList>`) |
| `/exchanges/{id}` | 303 to `/conversations/{c}?turn={i}` via `exchange_turns`; 404 page "not threaded yet" otherwise | none of its own |
| `/spans/{id}` | 303 to `/conversations/{c}?turn={i}&hl={id}` via `span_points`; a span whose exchange is not threaded shows the span's agent and exchange instead | none of its own |

All four carry the shared view state (`from, to, v, w, g, a, c, r, t, x,
u`) on every link as every page does, so leaving the conversation for
topology or evidence keeps the operator's window and filter. The page keys
above are new to these pages and collide with neither the shared keys nor
the topology keys (`sel`, `collapse`). `cursor` and `origin` are reused
names but page-local, which the UI already allows (page-specific keys do not
leave the page). Unknown or malformed values are a 422 naming the key, as
for every page.

**Citeable.** A turn is addressed by its index, which never changes once
threaded (the spec proposal pages turns by index range, not by cursor, for
this reason). `turn=37` always shows the window containing turn 37: turns
`20..40` (window start = `turn` rounded down to a multiple of 20), scrolled
to `#turn-37`, so two people opening one link see the same page. Without
`turn` the page opens at turn 0. A `turn` past the last turn renders the
last window with a note "this conversation has N turns". `hl` survives
paging links only when the highlighted span lies in the target window.

The exchange and span redirects make links from the evidence page and
elsewhere citeable without those pages knowing conversation ids: the
canonical URL is always the `/conversations/{id}?turn=` form after the
redirect.

## Layout

```text
Agents / cc3 / Conversations / 01J…X          [refresh]
┌ head ──────────────────────────────────────────────────────────────┐
│ agent cc3 (claims: claude-code 2.1.4)   started 12:01  last 12:40   │
│ 84 turns  ·  received 3  ·  sent 5                                  │
│ origin: forked from 01J…A at turn 11 (shared 23 messages)           │
│ spawned by: agent cc0, turn 7 of 01J…P  (delegation, tx 01J…T)      │
│ continued in: compaction 01J…Q (12:41) · fork 01J…R at turn 60      │
└────────────────────────────────────────────────────────────────────┘
« earlier (turns 0–19)                          later (turns 40–59) »
── turn 37 · 12:31:04 · claude-sonnet · sse · completed (tool_use) ──
   claims: claude-code 2.1.4                        provenance: scanned
   ▸ tool result  web_fetch  (4.1 KB)
       ⟵ from agent B via wiki.example.org — 212 B exact — tx 01J…T ↗
   ▸ user  (0.3 KB)
   ◂ assistant output
       text (1.2 KB)
         ⟶ originated · read later by C (turn 4), D (turn 12) +3 more
         ⟳ relayed from agent B (turn 9 of 01J…K)
       tool call  Task  → delegated to sub-agent E (conv 01J…S) ↗
── compaction boundary: 31 messages carried over from 01J…Z ──
── increment on ws connection 01J…W (history before this turn unseen) ──
```

- **Head.** Agent (canonical, named), claims shown with the existing
  `claim_badge` ("claims"), start and last turn, turn count, received/sent
  transmission counts, origin (`Root`; "forked from … at turn k, shared n
  messages"; "compaction of …, n carried over"), "spawned by" from
  `delegated_from`, and successors ("continued in"). Every conversation id
  links to its page, a fork to its parent at `branch_turn`.
- **Turn header.** Index (anchor `#turn-N`), time, model, transport,
  outcome (stop reason or failure), token usage when reported, the
  harness claim as a claim, and provenance status (`pending` greys every
  mark row with "scan in progress").
- **Messages.** A turn lists its new inputs in request order, any role (a
  system message appears wherever the request put it, labelled "system
  prompt changed" after turn 0), then its output. Each part is a row: kind
  (text, reasoning, tool call with name, tool result with its call's tool
  name and outcome, media kind, unknown block type) and size.
- **Inbound marks** (`⟵`) under the part they were read in: "from agent B"
  plus the route (via channel locator, delegation, direct carrier,
  unobserved), match kind and bytes; links: the sender's turn
  (`/spans/{origin}`), the transmission's evidence
  (`/transmissions/{id}`) when one holds the match, the channel. A match no
  transmission holds says so ("content match, no transmission").
- **Output spans** (`⟶`): originated spans with status (pending, indexed,
  propagated with hits, expired: "no later readers can be detected") and
  "read later by" the inline readers, each linking to the reader's turn and
  its transmission; "+N more" opens a reader list (`span_readers`, a
  section on the same page under the span, paged by the `rcursor` key).
  Relayed spans (`⟳`) link to the span they copied or name the input
  message they came from, found in this conversation when it is shown.
- **Delegation.** A reader or inbound mark whose transmission route is
  `Delegation` renders as "delegated to sub-agent E" on the tool-call part
  (parent to child) or "returned by sub-agent E" on the tool result (child
  to parent), linking to the child's conversation turn.
- **Boundaries.** A compaction conversation's turn 0 is preceded by a
  boundary row naming the predecessor and the carried-over count, and its
  carried-over messages are folded under "carried over (n)". An
  `Increment` turn shows its connection; `Unseen` history shows a boundary
  row "history before this turn was not seen by the gateway". A fork's
  first turn is preceded by "branches from turn k of 01J…".
- **Claims are claims.** Harness family and version are only ever shown
  through `claim_badge` or the word "claims", never as identity.

### Permissions

- View is required for every page (`require(caller, View)`), as elsewhere.
- **View alone** shows structure only: every row above with sizes, kinds,
  tool names, ids, times and marks, and `content_hidden()` where text would
  be. Byte ranges of marks are shown as offsets ("bytes 120–332").
- **Content** adds text: the page makes one `conversation_text` call for
  the same window and renders each part's text with marks highlighted
  (`<mark>` over each inbound range and originated span, as the evidence
  page's excerpts do). Parts longer than the limit (8 KiB) end with "show
  more", which loads the next slice through `part_text` in a shard (not a
  URL key: expanding text changes no data the view is about). A dropped
  body says "body dropped by content retention" as on the evidence page.
- Text is fetched only when `can(caller, Content)`; without it the text
  call is never made, so a View-only caller's request never reads message
  text.

### Paging

- Conversation list: spec cursor paging, 50 per page, `cursor` key,
  "« First page / Next page »" as the other lists.
- Turns: windows of 20 by `turn`, with "earlier" and "later" links and a
  turn-number jump box (a GET form submitting `turn`). Long conversations
  (the AI Village stream has hundreds of turns) stay fast because only one
  window's turns, marks and text are read.
- Readers of one span: `span_readers`, 20 per page, `rcursor` key.

## Links from existing pages (additive only)

| Page | Link | Target |
| --- | --- | --- |
| Agent detail header (`pages/agents/detail.rs`) | "Conversations" | `/agents/{id}/conversations` |
| Agents list (`pages/agents/list.rs`) | none in v1 (keeps the table unchanged) | |
| Evidence page, each match (`pages/transmission/sections.rs`) | "in reader's conversation" and "in sender's conversation" | `/exchanges/{reader_exchange}`, `/spans/{origin}` |
| Topology drawer agent panel (`pages/topology/drawer/mod.rs`) | "Conversations" beside "Open agent page" | `/agents/{id}/conversations` |

The evidence-page links need only ids the evidence already holds
(`ContentMatch::reader_exchange`, `origin`), so the evidence page makes no
new read; the redirect pages resolve them.

## Data and control flow

```text
GET /conversations/{id}?turn=37&<view state>
  view_state(cx) ─ canonical URL or redirect
  parse page keys (turn, hl) ─ 422 on bad values
  require View
  QueryApi::conversation(id)                    → head (None → 404)
  QueryApi::conversation_turns(id, {from: 20, size: 20}) → TurnPage
  agent_names(batch of every agent id in head + marks)
  channel_names(batch of channels in mark routes)
  if can(Content): QueryApi::conversation_text(id, same window, TextLimit::DEFAULT)
  model: zip turns with text (aligned by INV-1019) → TurnView[]
  render head, window links, turns

GET /exchanges/{id}   → exchange_turns([id]) → 303 /conversations/{c}?turn={i}
GET /spans/{id}       → span_points([id])    → 303 /conversations/{c}?turn={i}&hl={id}
GET /agents/{id}/conversations → agent(id) for the banner/name,
                                 conversations({agent: id, origins}, page)
shard part_text(part, slice) → PartText (Content)
```

The pages call the spec's `QueryApi` through `app::backend(cx)` like every
other page; the fixture backend implements the new methods over seeded
conversations. If the spec addition lands in a different shape, this
section and the model module change, not the layout.

## Files (planned, Phase B)

| File | Role |
| --- | --- |
| `ui/src/pages/conversation/mod.rs` | `#[page("/conversations/{id}")]`, path param, `load`, head and window rendering |
| `ui/src/pages/conversation/query.rs` | page keys: `turn`, `hl`, `rcursor`; window arithmetic (`window_start(turn) = turn / 20 * 20`) |
| `ui/src/pages/conversation/model.rs` | `TurnView`, `MessageView`, `PartView`, `MarkView` built from `Turn` + optional `TurnText`; route and carrier labels shared with `transmission/model.rs` |
| `ui/src/pages/conversation/sections.rs` | head, turn, boundary, part and mark components |
| `ui/src/pages/conversation/text.rs` | text with highlighted ranges; the `part_text` "show more" shard |
| `ui/src/pages/conversation/list.rs` | `#[page("/agents/{id}/conversations")]` |
| `ui/src/pages/conversation/locate.rs` | `/exchanges/{id}` and `/spans/{id}` redirects |
| `ui/src/pages/conversation/tests.rs` | router tests (below) |
| `ui/src/pages/common/links.rs` | `conversation_url`, `exchange_url`, `span_url` (added) |
| `ui/src/backend/fixture/world/conversations.rs` | seeded conversations and turns derived from the fixture's exchanges, spans and matches |
| `ui/src/backend/fixture/queries/conversations.rs` | the fixture's `QueryApi` conversation methods |
| `ui/src/pages/mod.rs` | module registration only |
| `ui/src/pages/agents/detail.rs`, `transmission/sections.rs`, `topology/drawer/mod.rs` | one link each |

Tests (Phase B): a router test per route (status, canonical redirect,
422 per bad key), View-only rendering has no text and does not call the
text read (`caller_with(&[Permission::View])`), Content rendering
highlights each mark, window arithmetic and `turn` past the end, alias
banner on the list, each fixture scenario (fork link and branch turn,
compaction boundary with carried-over count, unseen increment, delegation
both ways, mid-conversation system message in request order, dropped
body), redirects for threaded and unthreaded exchanges and spans, and a
headless-Chrome check of the page and its links. The existing suite must
pass unchanged.

## Invariants and constraints

- Additive: no existing page changes behaviour; the only edits outside
  `pages/conversation/`, the fixture's conversation modules and link
  helpers are the three links above and module registration.
- No UI code against the old spec; Phase B begins only after the
  migration and the spec addition.
- Turn URLs are citeable: `turn` is a stable index, the window is a pure
  function of it, and redirects end on the canonical
  `/conversations/{id}?turn=` URL.
- Page keys never collide with the shared view-state keys or the topology
  keys, and never leave the page.
- View-only callers never trigger a text read; text appears only with
  Content.
- Every agent shown is canonical and named through `agent_names`; claims
  are only ever shown as claims.
- Marks come only from the gateway's reads; the UI never infers a match,
  span or transmission. A turn whose provenance is pending says so instead
  of implying "no provenance".
- The text and structure of a window are aligned by turn index and part
  index; a misalignment is a backend fault shown with `error_panel`, never
  silently mismatched.
- Spec invariants this view relies on: INV-1000..1029 (proposed in
  `docs/handoff/conversation-view-spec.md`).

## Recommendations taken (for the user to confirm)

1. Turns paged by index window of 20, not cursor, so turn links are
   citeable.
2. No live updates in v1; a refresh link on the head.
3. The agents list gets no new column in v1; the agent page header and
   drawer carry the link.
4. Carried-over messages on a compaction's first turn are shown, folded.
