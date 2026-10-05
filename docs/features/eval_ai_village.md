# AI Village converter (`crosstalk-eval`, `datasets::ai_village`)

[AI Village](https://theaidigest.org/village) (AI Digest) is a long-running
experiment in which frontier-model agents share a village: each agent has
its own computer, a group chat, memories and goals. The dataset is a
near-verbatim dump of the village database as gzipped JSON Lines under
`~/Data/ai/agents/ai-village` (`ai-village` in `datasets.toml`). The
converter turns it into eval worlds of spec `NormalizedExchange`s with
labels, in two modes, and exposes its streaming passes, resource normalizer
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
  one world per day, with rebuilt requests, structural chat labels and
  heuristic repository-channel labels; GUI edits of Google Docs and Gmail
  counted.
- **Reusable passes**: `tables` (one streaming pass per table, filtered by
  a window early), `resource` (canonical repository and site resources, an
  L5 locator's canonical form), `access` (bash command → resource reads and
  writes with their `WriteOutcome` and `http_request` equivalent).

## Non-scope

- **Screenshots** (`images/`, ~165 GB) are never read. Image blocks in
  messages are dropped.
- **Exact requests.** `llm_calls` is withheld: standard agents' requests
  are rebuilt from the responses (see below) and the Claude Code agent's
  system prompt and per-query prompts are missing.
- **The Claude Code agent in window mode.** Its calls are not in
  `computer_use_turns`; windows hold the standard agents only.
- **Co-access labels.** A repository read after another agent's write whose
  output does not hold the write's text is counted, not labelled: the eval
  has no label kind for a suspected (co-access only) transmission.
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
                       tag: every bash turn ─▶ access::Shell::accesses ─▶ repo::AccessLog (pairs);
                            GUI turns ─▶ gui::GuiStats
                     next_world (one per village day): day::build
                       calls::turn_call per turn (provider::response), talk_call for unmatched AGENT_TALK
                       requests: system prompt + session history + chat user turn (prompt)
                       chat labels (Structural) and repo labels (Heuristic)
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
- **Repository channels (Heuristic).** `access::Shell` tags each bash
  command, per agent in time order (the bash tool is one persistent shell,
  so `cd` and learnt remotes carry over):

  | Command | Access | Resource from |
  | --- | --- | --- |
  | `git push` | write | the output's `To <remote>`, else a URL argument, else the directory's learnt remote |
  | `git clone <url>` | read | the URL (the clone directory learns it) |
  | `git pull` / `git fetch` | read | the output's `From <remote>`, else a URL argument, else the directory's remote |
  | `gh`/`glab` `issue`/`pr`/`mr` create, comment, note, edit, close, reopen, merge, review, approve | write (payload: `--body`, `--title`, `--description`, `--message`, `-m`, glab `-d`, `$(cat <<EOF…)` bodies) | `-R`/`--repo`, else a URL argument, else the directory's remote, else a URL in the output |
  | `gh`/`glab` `issue`/`pr`/`mr` view, list, diff, checks, status | read | the same |
  | `gh api` / `glab api` | write with `-X` other than GET or a field flag, else read | the API path |
  | `curl` | write with `-X` other than GET/HEAD or a data/form/upload flag (unless `-G`), else read (payload: data values) | every URL argument |
  | `wget` | write with `--post-data`/`--post-file`/`--method`, else read | every URL argument |

  A **pair** is a read whose latest earlier write to the same resource came
  from another agent. It is a label (`Channel { resource }`, `ToolResult`,
  Heuristic) when the read's output holds a payload line of the write (≥ 24
  bytes, ≥ 20 letters or digits), located at that line, at the reader's next
  call in the same session (the first whose request carries the output).
  Otherwise it is a co-access only (counted). A pair whose write is on an
  earlier day has its writer's exchange in another world: counted, not
  labelled.
- **Canonical resources** (`resource::from_url`, `from_remote`): every form
  of a repository meets on one canonical URL,
  `https://<forge>/<owner>/<repo>` with a lower-cased path: git remotes
  (https, `git@host:`, `ssh://`), web URLs (`/-/` subpaths, `.git`),
  `api.github.com/repos/o/r/…`, `raw.githubusercontent.com/o/r/…`,
  `gitlab.com/api/v4/projects/<url-encoded path>/…`, Pages
  (`o.github.io/r/…`, `g.gitlab.io/p/…`). A numeric GitLab project id stays
  `https://gitlab.com/api/v4/projects/<id>`, a unique GitLab Pages domain
  (`<name>-<6 hex>.gitlab.io`) stays its site, and any other URL is itself
  without query or fragment. Credentials and `www.` are dropped.
- **GUI edits** (`gui::GuiStats`): GUI turns whose typed text or the model's
  visible text names Google Docs or Gmail are counted, never labelled.
- **Coverage** is `Partial`.

### Write outcomes

Each write carries the spec's `WriteOutcome` (`access::outcome`), judged
from the command's output alone (the village's bash turns record no exit
status, and git and gh write progress to stderr):

- `Rejected`: the output reports a failure: a first line opening with
  `fatal:`, `error:`, `curl: (`, `HTTP 4xx/5xx`, `GraphQL:`, `gh: `,
  `Permission denied`, `could not` and the like; a git push's
  `! [rejected]`, `! [remote rejected]` or `error: failed to push`; or a
  JSON body whose top-level `message`/`error` is a known API failure (`Bad
  credentials`, `Not Found`, `Validation Failed`, `401 Unauthorized`, …).
- `Delivered`: the tool's success shows (a push's `a..b main -> main` or
  `Everything up-to-date`; a forge CLI's printed URL or `✓`; an API body
  with `html_url`, `web_url` or `created_at`).
- `Unknown`: neither (most `curl` writes).

A rejected write is recorded and counted but never pairs, and it does not
hide the latest earlier write to its resource (`WriteOutcome::pairs`). A
read whose output reports a failure is no access (the spec's reads need a
delivered result), so a failed `git clone` or `curl` reads nothing.

### The L5 contract

The agreed L5 `HttpTool` contract: a call of a tool named `http_request`,
`fetch`, `web_fetch` or `curl` with `url` and `method` arguments; `GET` and
`HEAD` read, `POST`, `PUT`, `PATCH` and `DELETE` write (the written spans
from the first of `body`, `content`, `text`, `data`), any other method is
no access; the locator is the canonical URL.

AI Village agents make no such call. Every repository and web access is
inside a `bash` command (or a GUI action), and the exchanges keep those
calls exactly as the model made them. The converter meets the contract in
three ways:

- **HTTP equivalents.** Each `curl`, `wget`, `gh api` and `glab api`
  access keeps the `http_request` call it is equivalent to
  (`Access::http`, `access::HttpRequest`; `HttpRequest::tool_call` builds
  it): the method as the contract reads it (`-X`/`--request`, `-I` HEAD,
  `-T` PUT, data or field flags POST unless `-G`, else GET), the URL as
  written (`gh api <path>` is `https://api.github.com/<path>`, `glab api
  <path>` is `https://gitlab.com/api/v4/<path>`) and the body (curl's data
  values joined with `&` as curl sends them; the API fields as a JSON
  object). A method outside the six is no access, as in L5.
- **Canonical resources.** A URL off the forges is exactly the locator
  L5's extractor gives it (`crosstalk_flow::extract::resource::url_locator`:
  scheme and host lower-cased, default port, user info and fragment
  dropped, dot segments resolved, percent-encoding normalized, query
  parameters sorted). A repository is the locator of its lower-cased web
  URL (`https://github.com/<owner>/<repo>`), which every remote, API, raw,
  blob and Pages form of it maps to. `resource::canonical` maps any L5
  locator (a URL, or L5's GitHub file `File { host: "github.com/o/r" }`) to
  the converter's resource, and a test runs L5's `ToolExtractors` over the
  equivalent calls to check that the two agree.
- **Bash-only accesses.** `git push`, `git clone`, `git pull`/`fetch` and
  the forge CLIs' issue, PR and MR commands speak git or the CLIs' own
  GraphQL: only a Bash extractor could see them (`Access::http` is
  `None`). A label counts as `repo_labels_http_visible` when both its write
  and its read have an HTTP equivalent, else `repo_labels_bash_only`. L5's
  Bash extractor today reads `curl`/`wget` and learns clones from `git
  clone`/`gh repo clone`, but records no access for `git push`,
  `git pull` or any `gh`/`glab` issue command, so it would see the
  `curl`/`wget` side of a label at most.

The Claude Code agent's `WebFetch` calls are the only fetch-tool-shaped
calls in the dataset.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/ai_village/mod.rs` | the source and its modes | `AiVillageSource`, `Mode`, `Stats`, `AiVillageError`, `DATASET`, `DEFAULT_FROM`, `DEFAULT_TO` |
| `…/stream.rs` | streaming a table | `Table` (`scan`, `load`, `path`, `file_name`), `decode`, `created_at`, `string_field`, `StreamError` |
| `…/time.rs` | times, village days, windows | `parse_timestamp`, `format_seconds`, `Day`, `village_day`, `Window`, `TimeError` |
| `…/schema.rs` | the rows read | `AgentRow`, `RoomRow`, `SessionRow`, `TurnRow`, `EventRow`, `ChatRow`, `MemoryRow`, `VillageGoalRow`, `AgentGoalRow`, `ClaudeCodeRow` |
| `…/tables.rs` | reusable passes | `load_directory`/`Directory`, `load_sessions`, `scan_turns`, `scan_events`/`EventScan`, `events_by_id`, `leading_id`, `scan_chat`, `scan_memories`/`Memories`, `load_goals`/`Goals` |
| `…/rooms.rs` | room membership | `RoomTimeline`, `ROOMS_V1_MICROS` |
| `…/resource.rs` | canonical resources | `from_url`, `from_remote`, `repo`, `canonical`, `Forge`, `urls` |
| `…/access/mod.rs` | bash accesses | `Shell` (`accesses`), `Access` (`http_visible`), `Op` (`Read`, `Write(WriteOutcome)`), `Tool`, `HOME` |
| `…/access/http.rs` | the L5 `HttpTool` contract | `HttpRequest` (`tool_call`), `HttpMethod`, `HTTP_TOOL` |
| `…/access/outcome.rs` | judging an output | `failed`, `write_outcome` |
| `…/access/shell.rs` | word splitting | `commands`, `SimpleCommand`, `heredoc_argument` |
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
| `…/window/repo.rs` | access log and pairs | `AccessLog`, `AccessRecord`, `Pair`, `AccessStats`, `TurnRef`, `payload_line` |
| `…/window/gui.rs` | GUI edit counts | `GuiStats` |
| `…/report.rs` | unlabelled predictions | `Unlabelled`, `tool_kind` |
| `tests/ai_village/` | synthetic fixtures written to a temp dir; units, Claude Code, window, the L5 contract (`l5.rs`, runs `crosstalk-flow`'s extractor) | |

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

5 village days, 26 agents, 116,410 exchanges (one per turn), 97,988 labels:
97,978 structural chat labels and 10 heuristic repository labels. About
14.7 min and 6.6 GB peak (the week's raw turn lines are held per day; the
three full passes over the 2.4 GB turns and memories tables dominate).

| route | carrier | class | tier | expected | found | recall |
| --- | --- | --- | --- | ---: | ---: | ---: |
| direct | user_turn | exact | structural | 61860 | 61788 | 0.999 |
| direct | user_turn | normalized | structural | 48 | 48 | 1.000 |
| direct | user_turn | decoded | structural | 36070 | 36070 | 1.000 |
| channel | tool_result | exact / decoded | heuristic | 10 | 0 | 0.000 |

The 36,070 `decoded` labels were `normalized` before the string level
rule (they need one JSON string level undone, `Decoded([JsonString])`);
48 need whitespace or case only, and none is two levels deep, so no
window label is out of reach. Claude Code mode's labels did not move.

- **Accesses.** 21,668 (14,934 reads, 6,734 writes: 4,891 delivered, 48
  rejected, 1,795 unknown) on 2,397 resources; 15,480 have an
  `http_request` equivalent (curl 11,047, `glab api` 4,427, wget 6) and
  6,188 are Bash-only (git push 4,759, pull 718, fetch 449, clone 130,
  `glab` issue/MR commands 132).
- **Pairs.** 993 cross-agent read-after-write pairs: 10 labels, 870
  co-access only (most writes are `git push`, whose payload is code the
  converter does not keep), 76 across days, 37 with no next call. Of the
  10 labels, 5 are `glab api` on both sides (HTTP-visible) and 5 have a
  `glab mr`/`issue` command on one side (Bash only).
- **Channel recall 0** is the reference matcher's: it routes bash results
  as `Direct` (see Gaps).
- Unjudged predictions: 578,674 in system prompts (memories quoting other
  agents), 362,110 in user turns (chat quoted again later), 80,664 in bash
  local-file reads, 44,148 in bash repository commands. **Shared web
  content**: 36,176 in bash web reads and 21,684 in scripts fetching a URL.
- GUI: 34,999 GUI turns; 1,347 name Gmail and 141 Google Docs (counted).

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
- **Resources are canonical URLs.** Every label resource is a
  `Locator::Url`: off the forges, exactly L5's `url_locator`; on them, the
  repository's lower-cased web URL. `resource::canonical` of L5's locator
  for an equivalent `http_request` is the access's resource.
- **The exchanges keep the raw calls.** Bash commands and GUI actions stay
  as the model made them; HTTP equivalents live beside the accesses, never
  in the exchanges.
- **No dataset bytes in the repository.** Tests write synthetic rows.

## Gaps

- **Eval core: no co-access label.** A suspected (co-access-only) transmission
  has no expectation kind; such pairs are counted, not scored.
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
- **Spec: no repository resource.** L5 names a repository's files
  (`File { host: "github.com/o/r", path }`) and URLs, but nothing names the
  repository itself, which is what `git push`, `git pull` and an issue
  comment touch. The converter's repository channel is a `Url` of the
  repository's web URL, coarser than L5's files: a `curl` of a raw file and
  a `git push` meet on it here, and on nothing in L5.
- **Spec: no exit status for shell results.** Village bash turns carry
  stdout and stderr only, and L5 reads no shell result text (`ContentRule`
  none for shell tools), so L5 would mark every shell write `Delivered`
  where this converter judges `Rejected` or `Unknown` from the text.
- **Spec types not used here.** `IngressMode::Replay { corpus }` is for the
  corpus client, which the eval core has not moved to yet; the eval core
  still has its own `CarrierKind` (TODO in `truth/kinds.rs`), so the
  converter uses that one. `WriteOutcome` and `Codec::JsonString` are the
  spec's.
