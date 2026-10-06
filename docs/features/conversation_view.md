# Conversation view

Status: **implemented** on `feat/ui-conversation` (from `staging`), over
the spec's conversation reads (`docs/features/conversation_reads.md`,
INV-1000..1029), which the fixture implements over its own conversations
([Fixture data](#fixture-data)) and the world and http backends serve
through the real surface. The proposal those reads came from is
`docs/handoff/conversation-view-spec.md` (revision 3); where the landed
spec differs (traffic counts on the head, `MessagePlacement`,
`SpanOrigin::Forwarded`, `ScanStatus`, a dropped body's marks,
`ExchangePlacement`), the pages follow the spec. Not built yet: the
"show more" slice of a long part (`part_text`) and the turn-number jump
box; see [Not built](#not-built).

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
  dropped body, a replayed corpus).

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
| `/agents/{id}/conversations` | conversations of the canonical agent `id` resolves to (an alias shows a banner, as the agent page does) | `cursor` (the list's `PageRequest<ConversationList>`), `origin` (comma list of `root,fork,compaction`; empty means all), `replay` (`include` default and omitted, `exclude`, `only`, or `only:<corpus>`) |
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

- **Replayed traffic.** A conversation whose traffic came from a dataset
  replay (`TrafficSource::Replay { corpus }`, from the eval PR's
  `IngressMode::Replay`) carries a "replayed: <corpus>" badge on its list
  row, its head and each turn header, and the list filters on it (`replay`
  key). Live conversations carry no badge.
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
  (`/transmissions/{id}`) when one holds the match, in any state (suspected
  and discarded included; the evidence read covers them), the channel. A match no
  transmission holds says so ("content match, no transmission").
- **Output spans** (`⟶`): originated spans with status (pending, indexed,
  propagated with hits, expired: "no later readers can be detected") and
  "read later by" the inline readers, each linking to the reader's turn and
  its transmission; "+N more" opens a reader list (`span_readers`, a
  section at the top of the page, paged by the `rcursor` key). Spans this
  agent forwarded from an input it read (`SpanOrigin::Forwarded`) render
  like originated ones, labelled "forwarded from an input".
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
  page's excerpts do). Parts longer than the limit (8 KiB) end with a note
  of the bytes not shown. A dropped body says "body dropped by content
  retention" and still lists its marks (`MessageParts::BodyDropped`).
- Text is fetched only when `can(caller, Content)`; without it the text
  call is never made, so a View-only caller's request never reads message
  text.

### Paging

- Conversation list: spec cursor paging, 50 per page, `cursor` key,
  "« First page / Next page »" as the other lists.
- Turns: windows of 20 by `turn`, with "earlier" and "later" links above
  and below the turns. Long conversations
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
GET /agents/{id}/conversations → agent(id) for the name and alias banner,
                                 conversations({agent: canonical, origins, replay}, page)
with hl: span_points([hl]) → the span's turn (hl survives window links only there)
         span_readers(hl, rcursor page of 20) → the "Read later by" section
```

The pages call the spec's `QueryApi` through `app::backend(cx)` like every
other page. `view.rs` maps the spec's read models to the view model
(`model.rs`), which `sections.rs` renders, so a shape change in the spec
touches the mapping, not the layout.

### Backends

- **Fixture**: answers every read from its conversations
  (`backend/fixture/queries/conversations/`): the list (canonical agent,
  origin and replay filters, `ConversationId` descending, keyset cursor
  bound to the filter and the resolved cluster), the head (origin links,
  successors with branch turns, traffic counted over content-holding
  transmissions, the delegation that started it, claims), turn windows
  with part shapes from the bodies and marks from the match and span
  indexes (none while a turn's scan is pending), readers newest first,
  the batch locates, and text cut with `PartText::cut`.
- **World**: the in-process surface starts with `Unrecorded` conversation
  stores (`crosstalk_api::InProcess::start`), so the seeded world has no
  conversations: an agent's list is the "No conversations recorded" empty
  state, a conversation id is 404, and `/exchanges/{id}` says the exchange
  is not threaded. Seeding conversations into the world is
  `crates/world`'s to do.
- **Http**: `crosstalk-client` calls the gateway's routes; a gateway that
  threads exchanges serves real conversations. Router tests run the
  fixture behind the gateway API's binding (`testing::fixture_api`) as the
  on-call operator.

## Fixture data

The fixture's traffic generator knows exchanges only as ids on content
matches and spans only as locations, so `backend/fixture/world/conversations/`
threads that traffic into conversations. They are not part of world
generation, where they cost about 80% of the time (some 800 ms of 960 ms
in a debug build): the backend builds them once, on its first
conversation read (`LazyConversations`, a `OnceLock`, so concurrent first
reads build once), and a replay snapshot cuts them at its present on its
own first conversation read. Pages that read no conversation never build
them. Reads reach them through `Ctx::conversations` and the queries'
`Cv` wrapper:

- a **reader turn** per reader exchange: its inputs are the messages the
  copies arrived in (`ContentMatch::read_at`, bodies in `Blobs`, dropped
  where retention dropped them), its output a generated reply (with a tool
  call when the next turn reads a tool result, and sometimes a quote of what
  it read, recorded as a relayed span), or for a `ReaderOutput` match the
  output the copy was found in;
- a **writer turn** per originated span, a little before its first read:
  its output is the message the span sits in;
- conversations per agent split at a three-hour gap, 24 turns or a
  delegated task (which opens the child's conversation), each opening with
  a generated system prompt and user task; protocol, transport and model
  follow the agent's real harness family, and each turn carries one of the
  agent's recorded harness claims;
- named cases (`Conversations::cases`): a fork sharing its parent's first
  two turns, a compaction carrying two messages over, a Codex conversation
  whose first increment continues a response the gateway never saw, a
  system turn in a second request, a failed exchange with a partial
  output, one self-hosted (or pi) agent's conversations replayed from the
  `agentdojo-workspace` corpus, and one turn still waiting for its scan.

Generated messages are canonical (`Message::new`) and unique (each names
its exchange or conversation). One simplification: the generator stores
each copy found in a reader's output as its own message, but an exchange
has one output, so copies beyond the first are listed among that turn's
inputs as assistant messages.

## Files

| File | Role |
| --- | --- |
| `ui/src/backend/fixture/world/conversations/mod.rs` | `Conversations` (records, exchange → turn index, span records, relayed spans, generated bodies, pending scans, `Cases`), `ConversationRecord`, `TurnRecord`, `Entry`, `Ending`, `SpanRecord`, `RelayedSpan`, `Conversations::at` |
| `ui/src/backend/fixture/world/conversations/build.rs` | `build(&World)`: threading the traffic and making the named cases |
| `ui/src/backend/fixture/world/conversations/messages.rs` | generated system prompts, tasks, replies, summaries, system turns and partial replies |
| `ui/src/backend/fixture/world/conversations/tests.rs` | the overlay's laws: every reader exchange and origin span is a turn of its agent, time order, no message twice, each case, replay cutoff, determinism |
| `ui/src/pages/conversation/query.rs` | page keys `turn`, `hl`, `rcursor`, `origin`, `replay`; `Window` (twenty turns from a multiple of twenty) |
| `ui/src/pages/conversation/model.rs` | the view model (`HeadView`, `TurnView`, `MessageView`, `PartView`, `MarkView`, …) and `segments`, which cuts a part's text at its marks |
| `ui/src/pages/conversation/sections.rs` | head, window links, turn, boundary, part, text and mark components |
| `ui/src/pages/conversation/tests.rs`, `links_tests.rs` | component rendering from view models; the entry links |
| `ui/src/pages/common/links.rs` | `agent_conversations_url`, `conversation_url`, `exchange_url`, `span_url` |
| `ui/src/pages/agents/detail.rs`, `topology/drawer/{mod,model}.rs`, `transmission/{model,sections}.rs` | the entry links: "Conversations" on the agent header and drawer agent panel; "in sender's / reader's conversation" on each evidence match |
| `ui/src/pages/mod.rs`, `backend/fixture/{mod,replay}.rs`, `queries/mod.rs` | module registration; the lazily built conversations (`FixtureBackend::conversations`, `Snapshot::conversations`, `Ctx::with_conversations`) |

Pages and reads:

| File | Role |
| --- | --- |
| `ui/src/pages/conversation/mod.rs` | `#[page("/conversations/{id}")]`: load (head, window, text with Content, the highlighted span's turn and readers), the page and reader section |
| `ui/src/pages/conversation/view.rs` | spec read models → view model: `head_view`, `turn_view`, marks, delegation wording, `referenced` (the agents and channels to name) |
| `ui/src/pages/conversation/list.rs` | `#[page("/agents/{id}/conversations")]`: alias banner, origin and traffic chips, table, paging |
| `ui/src/pages/conversation/locate.rs` | `/exchanges/{id}` and `/spans/{id}`: 303 to the turn, or a page saying it is not threaded or unknown |
| `ui/src/pages/conversation/pages_tests.rs` | router tests over the fixture, over the world backend and over HTTP |
| `ui/src/backend/fixture/queries/conversations/{mod,turns,marks,text}.rs` | the fixture's eight conversation reads |
| `ui/src/backend/fixture/queries/conversations/tests.rs` | the reads' INV-1000..1029 behaviour over the fixture |
| `ui/src/backend/fixture/surface.rs` | the `QueryApi` methods calling those reads |
| `ui/src/backend/fixture/mod.rs` | test handles `conversation_records`, `confirmed_matches`; `Cases` |

Tests cover: each route's status, 303s and 404s, 422 per bad key; the
window arithmetic and a turn past the end; View-only rendering with no
text; marks linking evidence and turns; the fork, compaction, replayed
corpus, unseen increment, failed exchange, pending scan and delegation
cases; the list's filters; the world backend's empty states; the same
pages over HTTP. A headless-Chrome check (light and dark) loaded a fork, a
compaction, an agent's list, a replayed agent's list and a sender's turn
reached from an evidence page through `/spans/{id}`, with no console
errors.

## Not built

- **Show more**: a part longer than 8 KiB shows its first 8 KiB and the
  count of bytes after them; loading the next slice (`part_text`) from the
  page is not wired.
- **Jump to turn**: no turn-number box; `turn=N` in the URL and the window
  links do the paging.

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

## Decisions

Decided by the user:

1. No live updates in v1; a refresh link on the head.
2. The agents list gets no new column in v1; the agent page header and
   drawer carry the link.
3. Carried-over messages on a compaction's first turn are shown, folded.
4. Replayed conversations are shown, labelled "replayed: <corpus>", with a
   `replay` filter that includes them by default.

Design choice, not yet put to the user:

5. Turns paged by index window of 20, not cursor, so turn links are
   citeable.
