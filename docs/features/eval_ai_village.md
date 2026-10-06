# AI Village converter (`crosstalk-eval`, `datasets::ai_village`)

[AI Village](https://theaidigest.org/village) (AI Digest) is a long-running
experiment in which frontier-model agents share a village: each agent has
its own computer, a group chat, memories and goals. The dataset is a
near-verbatim dump of the village database as gzipped JSON Lines under
`~/Data/ai/agents/ai-village` (`ai-village` in `datasets.toml`). The
converter turns it into eval worlds of spec `NormalizedExchange`s with
labels, in two modes, and exposes its streaming passes, resource lookup
and bash access tagger for reuse (topology demos, the M3 UI corpus).

General eval concepts (worlds, labels, alignment, scoring) are in
[eval.md](eval.md).

## Scope

- **Claude Code mode** (`--mode claude-code`): the one agent that ran the
  Claude Agent SDK ("Opus 4.5 (Claude Code)", 2026-01-26..03-31). Its SDK
  entries give exact call boundaries and the tool results it read.
  Construction-tier labels for every chat message the agent read through
  the village MCP server's `get_events` tool, keyed by event id.
- **Window mode** (`--mode window --from DAY --to DAY`, default
  2026-07-13..07-17): every standard agent over a window of village days,
  one world per day, with rebuilt requests, structural chat labels,
  heuristic content labels on repository files, threads and pages, and
  access-only labels on repositories; GUI edits of Google Docs and Gmail
  counted.
  `--hours N` (with `--from` equal to `--to`, `Mode::DaySlice`) keeps
  only the first N hours of the day from its 10:00 UTC start
  (`Window::first_hours`), a bounded window for the live detector.
- **Reusable passes**: `tables` (one streaming pass per table, filtered by
  a window early), `resource` (L5's locator of a URL or remote, and the
  kind of shared resource a locator is), `access` (a bash command → the
  shared-resource reads and writes L5's extractor records, with their
  `WriteOutcome` and the writer's typed text).

## Non-scope

- **Screenshots** (`images/`, ~165 GB) are never read. Image blocks in
  messages are dropped.
- **Exact requests.** `llm_calls` is withheld: standard agents' requests
  are rebuilt from the responses (see below) and the Claude Code agent's
  system prompt and per-query prompts are missing.
- **The Claude Code agent in window mode.** Its calls are not in
  `computer_use_turns`; windows hold the standard agents only.
- **Co-access labels off the repository.** A read of a file, thread or
  page after another agent's write that carried text, whose output does
  not hold that text, is counted, not labelled. Only a push's co-access
  (whose write has no content to find) is an access-only label.
- **Local files.** Every agent has its own computer, so a file outside a
  clone of a forge repository (and a local bare repository) is never a
  shared resource here.
- **GUI transmissions** (Google Docs edits, Gmail) are unobservable from
  tool calls and are only counted.

## Data and control flow

```text
ai-village/*.jsonl.gz ──▶ stream::Table::scan (flate2, line by line; created_at read from the raw line first)
   │
   ├─ claude-code ─▶ ClaudeCodeStream::open
   │                   entries::load (all rows, ordered by session, time, row id)
   │                   entries::contexts (session cut at compact_boundary)
   │                   delivered_events → tables::events_by_id (one events pass, ids only)
   │                   chat_messages pass (the agent's own messages)
   │                 next_world (one per context):
   │                   calls::context → Calls (requests) + ResultRefs (+ which call first carries each)
   │                   exchanges (Reconstructed) ─┐
   │                   get_events results ─▶ events::talks (escaped content located)
   │                     first delivery per event id ─▶ originating exchange (event data.output, Synthetic)
   │                                                 ─▶ label Direct / ToolResult / Construction
   │                   chat_message calls ─▶ matched to chat_messages (stats)
   │
   └─ window ─────▶ WindowStream::open
                       tables::{load_directory, load_sessions, load_goals}
                       tables::scan_turns (raw lines bucketed by village day)
                       tables::scan_events (AGENT_TALK, USER_TALK; RoomTimeline from every event)
                       tables::scan_chat, tables::scan_memories
                       tag: every bash turn ─▶ access::Shell::accesses (crosstalk_flow ToolExtractors,
                            the shell's persistent state) ─▶ repo::AccessLog (pairs: content / access only);
                            GUI turns ─▶ gui::GuiStats
                     next_world (one per village day): day::build
                       calls::turn_call per turn (provider::response), talk_call for unmatched AGENT_TALK
                       requests: system prompt + session history + chat user turn (prompt)
                       chat labels (Structural), repo content labels and access-only labels (Heuristic)
                                     │
                                     ▼
                  World ─▶ Detector ─▶ score ─▶ report (+ ai-village.json: stats, unlabelled predictions)
```

### Tables and time

- Every table is sorted by UUID, not time, so a window needs a full pass.
  `Table::scan` streams one table through `MultiGzDecoder` and a 1 MiB
  `BufReader`; `stream::created_at` takes the row's `created_at` from the
  raw line (the last `"created_at":"` in it: the dump writes it after the
  nested columns), so rows outside the window are dropped before decoding.
- Times are UTC text (`2026-07-10 17:00:16.333633`), parsed by
  `time::parse_timestamp` to microseconds.
- A **village day** runs 10:00–10:00 UTC (`time::village_day`): the village
  works 9am–5pm Pacific, so one day's session never crosses a boundary.

### Claude Code mode

- **Entries.** One `claude_code_messages` row per SDK entry. Assistant
  entries carry one content block each; a call is every entry with one
  `message.id`. User entries carry `tool_result` blocks (or the compaction
  summary text). `compact_boundary` system entries cut contexts.
- **Requests.** A call's request is the context's history when its first
  block arrived: the summary, every earlier response, and the tool results
  after each. With parallel tool use the SDK interleaves a message's blocks
  with results of tools it already called; those results are held and
  placed after the message, where the API saw them. Fidelity
  `Reconstructed` (each message id is one API call). The system prompt and
  per-query prompts are not recorded and are left out: requests start at
  the first assistant message or the compaction summary.
- **Reads.** `get_events` returns pretty-printed JSON whose `AGENT_TALK`
  events (`agentName`, `content`, `id`) are other agents' chat messages.
  Content sits JSON-escaped; `events::talks` locates the escaped bytes
  (the last occurrence before the event's `"id"`).
- **Labels (Construction).** The first `get_events` result that delivers an
  event id to a call (a result the context ended on before any call read it
  does not count) yields: author → Claude Code, `Direct` route,
  `ToolResult` carrier, at the first call carrying the result, located at
  the escaped content. `needs` (`text::need`) is `Exact` when the escaped
  bytes occur verbatim in the author's response (its chat tool's JSON
  arguments escape the same way); `Decoded([Codec::JsonString])` when one
  level of JSON string unescaping gives text that occurs verbatim (the
  author wrote the message as plain text); `Normalized` when case and
  whitespace folding alone make them equal; `Decoded([JsonString])` when
  one string level undone on either side, then folded, does; out of reach
  (`MatchNeed::two_string_levels()`, `Tier::OutOfReach`) when exactly two
  levels undone on one side do, since a decoded chain holds at most one
  string codec (spec #58); `Decoded([JsonString])` when only the matching
  fold does; else `Semantic`. The label's tier is
  `needs.tier(…)`, so the same rule holds for window chat and repository
  labels.
- **Originating exchanges.** The author's response is the `AGENT_TALK`
  event's `data.output` (provider-shaped; for Claude models often a bare
  array of Anthropic content blocks), as an exchange at the event's
  time with an empty request (`Synthetic`). Events without an output are
  counted as `unoriginated` and not labelled.
- **Direct, not Channel.** The spec's `Channel` is a shared resource written
  by the sender and read by the reader through a tool call the gateway
  resolves to that resource. The authors never call `get_events` (or any
  MCP tool): their chat enters through the scaffolding's own action, and
  `get_events` takes no argument naming a resource; it is the agent's
  personal feed of what the scaffolding chose to deliver. That is the
  spec's `Direct` ("a tool whose call touches no extracted resource"),
  `DirectCarrier::ToolResult(mcp__village__get_events)`. A `Direct` label
  also aligns with a detector that routes the hit through an
  `Locator::Mcp { server: "village", tool: "get_events" }` channel.
- **Writes.** The agent's `mcp__village__chat_message` calls are matched to
  `chat_messages` rows by exact content (`chat_writes_matched`). They are
  writes to the other agents, which only a window over its period could
  label; this mode does not rebuild those readers' requests.
- **Coverage** is `Partial` (the agent also read repositories and the web).

### Window mode

- **Calls.** A standard agent's call is a `computer_use_turns` row
  (`session_id` → `computer_use_sessions.agent_id`): the response
  (`agent_messages`), the executed action (`agent_action`) and its output
  (`output`, then `error`). `provider::response` converts the four response
  shapes (Anthropic message, OpenAI Responses items, chat completion,
  Gemini candidates). The raw tool calls are kept as the model made them
  (bash commands, GUI actions); nothing is rewritten into tool calls an
  extractor would like better. An `AGENT_TALK` whose content no
  `send_message_back_to_chat` turn sent becomes a call of its own from the
  event's output (empty request, `Synthetic`).
- **Rebuilt requests** (`Reconstructed`; the placement of chat and memory
  is an assumption, the village's prompts are not public):

  ```text
  system: "You are <name>…" + village goal + the agent's goals + its latest memory   (per session)
  for each earlier call of the same computer-use session:
      user: that call's chat turn (if any)
      assistant: that call's response
      tool: its output, as the result of its first tool call
  user: chat posted in the agent's room since its previous call,
        one "[YYYY-MM-DD HH:MM:SS UTC] #room speaker: content" line each
  ```

- **Rooms.** Before 2026-02-25 everyone is in `#general`; after, an agent is
  in the room of its latest event naming one (`roomId`); `ENTER_ROOM` also
  records `previousRoomId` just before the move (`rooms::RoomTimeline`).
- **Chat labels (Structural).** A chat message (an `AGENT_TALK`, joined to
  `chat_messages` by `messageId`) reaches every other member of its room
  through the chat user turn of their next call; a label sits there when
  that call is within four hours of the message, from the speaker's sending
  call (the turn whose `send_message_back_to_chat` carried exactly the
  content, latest at or before the message), `Direct` / `UserTurn`, at the
  content's bytes.
- **Shared-resource accesses** (`access::Shell`). Every bash command goes
  through `crosstalk_flow::extract::ToolExtractors` as the `bash` call the
  agent made, with its output as a successful result (the village records
  no exit status). The locator, the op and each write's `WriteOutcome`
  are the extractor's; nothing is normalized here:

  | Command | Access | Locator |
  | --- | --- | --- |
  | `git push` | write, no content in the call (`WritePayload::Unseen`) | `Locator::Repository { host, owner, name }` (lower case, no `.git`, GitLab groups joined with `/`) |
  | `git pull`, `git fetch`, `git clone`, `gh repo clone` | read | the repository |
  | `gh`/`glab` `issue`/`pr`/`mr` `create` | write | the collection: `/issues`, `/pulls`, `/-/issues`, `/-/merge_requests` |
  | `comment`, `note`, `edit`, `review` | write | the thread: GitHub `https://<host>/<o>/<n>/issues/<N>` (issues and pulls alike), GitLab `/-/issues/<N>`, `/-/merge_requests/<N>` |
  | `view`, `list` | read | the thread, the collection |
  | `gh api`, `glab api`, `curl`, `wget` | the method's | the site rules: the repository (`api.github.com/repos/o/n`, codeload, web and tree URLs, Pages: `o.github.io/n` is `o/n`, `o.github.io` alone `o/o.github.io`), its file (`raw.githubusercontent.com`, `blob`/`raw`, `contents`, GitLab `repository/files`), a thread or collection; else L5's URL locator (a scheme-less host is `https`) |
  | `cat`, `head`, `tail`, `sed -n '<lines>p'` of a file; `>`, `>>`, `tee` into one | read; write | in a clone whose remote is known, the repository's file `File { host: "<host>/<o>/<n>", path }`; else a local file (not kept) |

  A read whose output reports a failure is no access; a write's outcome is
  read from the output for git, curl, wget and the forge CLIs. Only shared
  resources are kept (`resource::kind`: `Repository`, `RepoFile`, `Url`).

  **The shell's state** is the converter's, because the gateway's context
  cannot know it (see Gaps): the bash tool is one persistent shell per
  agent, so the working directory carries over (the context is moved as
  L5's persistent `Bash` moves it); `~` and `$HOME` are
  `/home/computeruse`, expanded before extraction; clones the extractor
  learns (`git clone`, `gh repo clone`, `git remote add`, `git remote -v`)
  are kept per agent across the window; and a clone made before the
  window is learnt from the remote a push or pull prints (`To <remote>`,
  `From <remote>`): the directory the command ended in is bound to it and
  the command is extracted again.

  A write keeps its **authored text** (`access::payload`): here-document
  bodies, body and title flags, API fields, curl and wget data, `echo`
  and `printf` arguments; a `git push` has none (`Payload::Unseen`).
- **Pairs** (`window::repo`). A pair is a read whose latest earlier write
  to the same locator came from another agent. What it becomes:

  | Write | Pair | Label |
  | --- | --- | --- |
  | `git push` (`Payload::Unseen`) | access only | `Expectation::AccessOnly` (`ExpectedAccess`): `Channel { Repository }`, `ToolResult`, Heuristic, over the read's whole output. The gateway records a push without spans, so co-access alone links it and the spec keeps it Suspected; only a suspected or discarded prediction finds it, under access-only recall |
  | authored text, a line of which (≥ 24 bytes, ≥ 20 letters or digits) is in the read's output | content | `Expectation::Transmission`: `Channel { resource }` (the repository file, thread, collection or page), `ToolResult`, Heuristic, at that line, `needs` from the writer's call |
  | authored text not in the output | co-access only | counted (`repo_co_access`) |

  Content therefore crosses only where it does in the gateway: a file a
  writer wrote in a clone and a reader read from another clone or a raw
  URL of the same repository file, an issue comment read back, a page
  posted and fetched. Both labels sit at the reader's next call in the
  same session (the first whose request carries the output). A pair whose
  write is on an earlier day has its writer's exchange in another world:
  counted, not labelled. Rejected writes never pair.
- **GUI edits** (`gui::GuiStats`): GUI turns whose typed text or the model's
  visible text names Google Docs or Gmail are counted, never labelled.
- **Coverage** is `Partial`.

### The L5 extractor

The converter calls the gateway's own extractor, so a label's resource is
by construction what the gateway records for the same call:
`tests/ai_village/l5.rs` builds a window from representative village
commands (`git clone`, a file written in a clone and pushed, `git pull`,
`sed -n` of the cloned file, `gh issue create`/`list`/`comment`/`view`,
`glab issue note` read back through the GitLab API, a Pages fetch, a raw
file fetch) and runs `ToolExtractors` itself, as the gateway does (the
`bash` tool, default config, one context per agent), asserting that every
label's resource is a write of the writer's call and a read of the
reader's call, that access-only labels are exactly the `Unseen` writes,
and which labels exist. The one difference is the shell state above.

The Claude Code agent's `WebFetch` calls are the only fetch-tool-shaped
calls in the dataset.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/ai_village/mod.rs` | the source and its modes | `AiVillageSource`, `Mode` (`ClaudeCode`, `Window`, `DaySlice`), `Stats`, `AiVillageError`, `DATASET`, `DEFAULT_FROM`, `DEFAULT_TO` |
| `…/stream.rs` | streaming a table | `Table` (`scan`, `load`, `path`, `file_name`), `decode`, `created_at`, `string_field`, `StreamError` |
| `…/time.rs` | times, village days, windows | `parse_timestamp`, `format_seconds`, `Day`, `village_day`, `Window` (`days`, `first_hours`), `TimeError` |
| `…/schema.rs` | the rows read | `AgentRow`, `RoomRow`, `SessionRow`, `TurnRow`, `EventRow`, `ChatRow`, `MemoryRow`, `VillageGoalRow`, `AgentGoalRow`, `ClaudeCodeRow` |
| `…/tables.rs` | reusable passes | `load_directory`/`Directory`, `load_sessions`, `scan_turns`, `scan_events`/`EventScan`, `events_by_id`, `leading_id`, `scan_chat`, `scan_memories`/`Memories`, `load_goals`/`Goals` |
| `…/rooms.rs` | room membership | `RoomTimeline`, `ROOMS_V1_MICROS` |
| `…/resource.rs` | L5's locators and shared kinds | `from_url` (site rules, else the URL), `from_remote` (`RepoId`), `kind`, `ResourceKind` |
| `…/access/mod.rs` | bash accesses through `ToolExtractors` | `Shell` (`accesses`, `cwd`, `unextracted`), `Access` (`payload`, `label`), `Op` (`Read`, `Write { outcome, payload }`), `Payload` (`Unseen`, `Authored`), `expand_home`, `HOME`, `BASH_TOOL` |
| `…/access/payload.rs` | a write's typed text | `authored`, `heredoc_bodies` |
| `…/access/shell.rs` | word splitting (reports, payloads) | `commands`, `SimpleCommand`, `heredoc_argument` |
| `…/provider/{mod,anthropic,openai,gemini}.rs` | provider responses → canonical | `response`, `Response`, `arguments`, `anthropic::{block, tool_result}` |
| `…/text.rs` | locating text, match needs | `json_escape`, `json_unescape`, `escapes`, `find`, `need`, `visible_text` |
| `…/claude_code/mod.rs` | the Claude Code source | `ClaudeCodeStream`, `ClaudeCodeStats`, `after` |
| `…/claude_code/entries.rs` | SDK entries | `Entry`, `EntryKind`, `load`, `contexts` |
| `…/claude_code/calls.rs` | calls and requests | `context`, `Context`, `Call`, `ResultRef`, `ToolUse` |
| `…/claude_code/events.rs` | `get_events` results | `talks`, `Talk`, `chat_content`, `GET_EVENTS`, `CHAT_MESSAGE` |
| `…/window/mod.rs` | the window source | `WindowStream`, `WindowStats`, `Shared` |
| `…/window/calls.rs` | a day's calls | `Call`, `Origin`, `turn_call`, `talk_call`, `results`, `output_text`, `senders`, `by_agent` |
| `…/window/prompt.rs` | synthetic prompts | `system`, `chat`, `ChatLine` |
| `…/window/day.rs` | a day's world | `build`, `CHAT_HORIZON_MICROS` |
| `…/window/repo.rs` | access log and pairs | `AccessLog`, `AccessRecord`, `Pair`, `Link` (`Content`, `AccessOnly`), `AccessStats`, `TurnRef`, `payload_line` |
| `…/window/gui.rs` | GUI edit counts | `GuiStats` |
| `…/report.rs` | unlabelled predictions | `Unlabelled`, `tool_kind` |
| `tests/ai_village/` | synthetic fixtures written to a temp dir; units, Claude Code, window, agreement with the gateway's extractor (`l5.rs`, runs `crosstalk-flow`'s `ToolExtractors` over the window's calls) | |

## Reference baselines

```text
ct-eval run --dataset ai-village --mode claude-code
```

993 contexts (worlds), 65,571 Claude Code calls, 81,369 exchanges (with
15,798 originating exchanges), 15,798 construction labels, about 5.5 min and
0.8 GB. 33,897 `get_events` results delivered 31,541 talks from other
agents (15,737 redeliveries, labelled only at the first); 6 events had no
output to originate from. The agent's 2,627 `chat_message` calls matched
2,623 `chat_messages` rows.

| route | carrier | class | tier | expected | found | recall |
| --- | --- | --- | --- | ---: | ---: | ---: |
| direct | tool_result | exact | construction | 9148 | 9130 | 0.998 |
| direct | tool_result | decoded (json_string) | construction | 6640 | 6640 | 1.000 |
| direct | tool_result | semantic | construction | 10 | 6 | 0.600 |

`get_events` recall is **0.999** (15,776 / 15,798), precision 1.000 on the
labelled hits. Before events' bare Anthropic block arrays were read as
Anthropic (they were taken for OpenAI Responses items, so the authors'
texts were lost), recall was 0.615 with 7,323 labels `semantic`.

Unjudged predictions (partial coverage) by where the reader read them:
198,134 `get_events` (mostly redeliveries), 18,817 bash repository
commands, 15,048 memory edits, 6,315 user turns. **Shared web content**:
2,195 in bash web reads, 87 in `WebSearch`, 17 in `WebFetch`; the examples
are pages several agents fetched (a shared quiz site's "Failed to load
quiz data", news phrases such as "the Department of Defense").

```text
ct-eval run --dataset ai-village --mode window   # 2026-07-13..17
```

5 village days, 26 agents, 116,410 exchanges (one per turn), 98,076
content labels (97,978 structural chat labels and 98 heuristic channel
labels) and 277 access-only labels. About 36 min and 6.7 GB peak with
the bash commands going through L5's extractor (14.7 min before, on a
lightly loaded machine; this run shared 16 cores with two live runs).

| route | carrier | class | tier | expected | found | recall |
| --- | --- | --- | --- | ---: | ---: | ---: |
| direct | user_turn | exact | structural | 61860 | 61788 | 0.999 |
| direct | user_turn | normalized | structural | 48 | 48 | 1.000 |
| direct | user_turn | decoded | structural | 36070 | 36070 | 1.000 |
| channel | tool_result | exact | heuristic | 60 | 0 | 0.000 |
| channel | tool_result | decoded | heuristic | 38 | 0 | 0.000 |
| channel | tool_result | suspected (access-only labels) | heuristic | 277 | 0 | 0.000 |

The 36,070 `decoded` chat labels need one JSON string level undone
(`Decoded([JsonString])`); 48 need whitespace or case only, and none is
two levels deep, so no window label is out of reach.

- **Accesses** (the extractor's, shared resources only): 23,230 (13,997
  reads, 9,233 writes: 9,022 delivered, 83 rejected, 128 unknown) on
  6,691 resources. By kind: repository 4,500 writes (4,493 pushes, the
  rest API writes) and 1,451 reads; repository files 2,928 writes and
  4,892 reads; pages, threads and API URLs 1,805 writes and 7,654 reads.
  72 commands the extractor refused (unterminated quotes and the like).
- **Pairs.** 522 cross-agent read-after-write pairs: 303 on a push
  (access only) and 219 on written text. 277 access-only labels and 98
  content labels (76 on a repository file: written in one clone and read
  from another or from a raw URL; 22 on a thread, collection or page), 68
  content pairs whose output holds none of the writer's text, 61 across
  days, 18 with no next call. Before (the converter's own normalizer,
  every repository form one `Url`): 993 pairs, 10 labels and 870
  co-accesses counted but not scored.
- **Channel recall 0** is the reference matcher's: it routes bash results
  as `Direct` and reports no co-access (see Gaps).
- Unjudged predictions: 578,136 in system prompts (memories quoting other
  agents), 361,968 in user turns (chat quoted again later), 79,730 in bash
  local-file reads, 43,939 in bash repository commands. **Shared web
  content**: 35,914 in bash web reads and 21,579 in scripts fetching a URL.
- GUI: 34,999 GUI turns; 1,347 name Gmail and 141 Google Docs (counted).

### Live (`--detector live`), 2026-10-05

Rescored after the gateway's L5 repository and shell semantics
(`Locator::Repository`, git/gh/glab extraction, shell outcomes read from
output, content confirmed up to `content_retention_ms`). The week window
does not finish live in useful time (about one village hour per wall
hour with full rebuilt session histories, so roughly two days for the
week), so the window is the bounded slice `--from 2026-07-13 --to
2026-07-13 --hours 8` (10:00 to 18:00 UTC, the village's first two
working hours: 6,452 exchanges, 25 agents). The window was never run live
before, so its "before" is the previous converter (base `c9e0465` with
the slice option) under today's gateway. Each run shared 16 cores with two
others.

| run | labels in reach | before: recall / precision | now: recall / precision | repo channel content labels found | access-only recall | time | peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Claude Code, all 993 contexts | 15798 | 0.998 / 1.000 (#83) | 0.998 / 1.000 (15772 / 15798) | - | - | 64 min (52.5 in #83) | 0.8 GB |
| window slice, before (old converter) | 4078 | 0.999 / 1.000 | - | 0 / 4 | - | 3.5 h | 1.2 GB |
| window slice, now | 4089 (+ 15 access-only) | - | 0.996 / 1.000 (4074 / 4089) | 0 / 15 | 0.000 (0 / 15) | 3.6 h | 1.3 GB |

Since `perf/live-profiling` the same slice runs live in about 3 minutes
(2 h 52 min before on the same machine; 168 s of CPU against 7,105 s;
peak RSS 2.1 GB against 1.15 GB, the bounded L4 caches), with
byte-identical predictions and report. The time went to L4's fingerprint
index counting frequencies over every observation, rebuilding the
inputs' coverage and decoding the request history on every exchange, and
re-encoding every history body on ingest (gateway.md, provenance.md,
memory.md). The week window has not been rerun.

Reference on the same slice: 4,074 / 4,078 before and 4,074 / 4,089 now
(chat unchanged; channel rows 0 by construction).

Reading it:

- **Chat is untouched**: 4,074 chat labels found either way; the overall
  recall moves only because 11 more channel labels are in reach (4 → 15).
- **The repository labels are not found live** (0 of 15 content, 0 of 15
  access-only). Most of their accesses are invisible to the gateway's
  context (Gaps: over the week, of the converter's accesses, 3,483
  repository and 4,320 repository-file accesses need a clone the gateway
  never learns, and 442 and 1,054 more a `~` it does not follow), so
  neither side of a pair is a co-access for L5. The short ones (`Wave 2
  Survey: Claude Fable 5`, 29 bytes, six labels) are under L4's 32-byte
  shingle. The access-only pairs also sit minutes apart, beyond the
  agreed 60 s correlation window.
- **A day-long correlation window does not run**: with
  `--correlation-window 86400` the slice's world fails in prediction
  conversion (`PredictError::UnlocatedAccess`: read access
  `01KXE46805TY443EM5HE3VE2YF` names a result part that is not in its
  exchange), so no wider access-only recall is reported.

### Findings for the gateway (2026-10-05)

Found by running the extractor as the gateway runs it (the `bash` tool,
default config, one context per agent) next to the converter over the
week's bash turns. Exchange ids are each turn's own exchange (its call).

1. **The context cannot follow the village's shell** (see Gaps): of the
   converter's accesses, 3,483 repository, 4,320 repository-file and 31
   thread accesses depend on a clone the gateway never learns (made
   before the window, or bound in a directory the gateway's non-persistent
   `bash` context never reaches), and 442, 1,054 and 27 more on `~` or
   `$HOME`, e.g. `01KXE3J8NFPA8TNQ3T8JDP9N5C` (`cd ~/wellbeing-compass &&
   … git push`), `01KXEAYNKYNVQVQBSHD22HR3WW` (`git push` in a clone
   entered in an earlier call), `01KXEB6MG9ZY9FBDHM15SB025E` (`cd
   …/ai-wellbeing/village-ci-tools && git pull` in a clone made before the
   window). Expected: a push or pull whose output prints its remote
   (`To <url>`, `From <url>`) is that remote's, whatever the context
   knows; `bash` tools of harnesses with one persistent shell persist
   their directory.
2. **A wrong binding attributes pushes to the wrong repository.** Seven
   pushes and pulls are recorded on a repository the converter, following
   the shell, does not attribute them to. Confirmed by the output in
   `01KXESRHVM2S1265BP82GN2RCR` (`git push origin main`, output `To
   https://gitlab.com/ai-village-agents/village/constraint-dashboard.git`,
   recorded as a write of `daily-signal-garden-gpt55`); the others
   include `01KXESQPE17KBP4TKRAK0HCY59`, `01KXET8ZTTFVD2MCFCGW3ZH7S6`
   (also recorded on `daily-signal-garden-gpt55`) and
   `01KXHB5HQQHV7CBSMV31T798NT` (a pull recorded on
   `deepseek-pattern-archive`). The non-persistent context resolves the
   remote name through a directory the agent had left. Expected: the
   printed remote wins over a bound one.
3. **A read is recorded for a command that never ran.**
   `01KXED5HV034DD2CSEZCVNJ0FV`: `cd ai-wellbeing && sed -n '1,220p'
   wave2-visualization.html`, output `cd: ai-wellbeing: No such file or
   directory`, is recorded as a read of the repository file. `cat`,
   `head`, `tail` and `sed -n` reads have no command rule, so an output
   that is only a shell error still delivers.
4. **A read access names a result part outside its exchange** under a
   day-long correlation window (`--correlation-window 86400`, slice
   above): access `01KXE46805TY443EM5HE3VE2YF`; the eval's prediction
   conversion fails the world (`PredictError::UnlocatedAccess`).

## Invariants and constraints

- **Streaming.** No table is held whole except the Claude Code stream
  (245k rows) and the small directory tables; window turns are kept as raw
  lines per day and decoded per world. At most one world's exchanges are in
  memory.
- **Determinism.** Every id derives from `(dataset, source ref)`; worlds are
  in day or context order; maps reaching output are ordered.
- **Times.** Each agent's exchanges strictly increase (equal times move
  1 µs later, `claude_code::after`); a sender's exchange precedes the
  reader's: the sending turn is stamped before its `AGENT_TALK` event, and
  the reader call comes after the message (chat) or after the result entry
  (Claude Code).
- **Labels are first deliveries.** Claude Code: the first call carrying an
  event id. Window chat: the first call after the message, within four
  hours. Repository: the first call carrying the read's output.
- **Rejected writes never pair.** A `Rejected` write is counted and kept
  in the log but is never the write of a pair; a read with a failed output
  is no access.
- **Resources are the extractor's locators.** Every channel label's
  resource is a locator `ToolExtractors` gave the writer's and the
  reader's `bash` calls: `Locator::Repository`, a repository's `File`, or
  a `Url`. Nothing in the converter normalizes a resource.
- **Access-only exactly for unseen writes.** A pair is an access-only
  label when, and only when, its write carries no content in the call
  (`WritePayload::Unseen`, a `git push`); content labels need a line of
  the writer's typed text in the reader's output.
- **Only shared resources.** Local files and local bare repositories are
  dropped: each agent has its own computer.
- **The exchanges keep the raw calls.** Bash commands and GUI actions stay
  as the model made them.
- **No dataset bytes in the repository.** Tests write synthetic rows.

## Gaps

- **Eval core: negative controls need a named sender.** "Shared web content"
  traps (two agents fetching one page) cannot be labelled without naming a
  sender, so they surface as unjudged predictions (`ai-village.json`'s
  `unlabelled_predictions`).
- **Eval core: the reference matcher routes bash results as `Direct`.** It
  extracts resources only from URL- or path-valued arguments, so no
  repository-channel label can align with it (channel recall 0 by
  construction).
- **Spec: no replayed-request fidelity marker.** Exchanges with a rebuilt
  request travel as ordinary full-history exchanges; only the eval's
  `Fidelity` says so.
- **Gateway context: the shell's state.** The converter knows what the
  gateway's per-conversation context cannot, and the live detector misses
  accesses for it (counted over the week window, 2026-07-13..17, against
  the extractor run as the gateway runs it; see Reference baselines):
  L5's `bash` tool does not persist its working directory, L5 does not
  follow `~` or `$HOME`, and a clone made before the conversation is never
  learnt from the remote a push or pull prints. A context that is wrong
  rather than unknown is worse: a `git remote add` or clone the gateway
  places in the home directory (the agent had moved elsewhere in an
  earlier call) binds the home directory, and later pushes from other
  clones are attributed to that repository.
- **Spec types not used here.** `IngressMode::Replay { corpus }` is for the
  corpus client, which the eval core has not moved to yet; the eval core
  still has its own `CarrierKind` (TODO in `truth/kinds.rs`), so the
  converter uses that one. `WriteOutcome` and `Codec::JsonString` are the
  spec's.
