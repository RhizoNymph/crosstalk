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
  a window early), `resource` (canonical repository and site resources),
  `access` (bash command → resource reads and writes).

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
  the escaped content. `needs` is `Exact` when the escaped bytes occur
  verbatim in the author's response (its chat tool's JSON arguments escape
  the same way), else `Normalized` after folding, else `Semantic`.
- **Originating exchanges.** The author's response is the `AGENT_TALK`
  event's `data.output` (provider-shaped), as an exchange at the event's
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

### What an L5 extractor can see

L5's `HttpTool` extractor reads tool calls named `http_request`, `fetch`,
`web_fetch` or `curl` with `url` and `method` arguments. AI Village agents
make none: every repository and web access is inside a `bash` command (or a
GUI action). So **every access in the table above needs a Bash extractor**:
`git push/clone/pull/fetch` (with the remote from the output's `To`/`From`
lines or the shell's directory), `gh`/`glab` issue, PR, MR and `api`
commands, and `curl`/`wget` inside a command line. For the labels to align,
such an extractor must also canonicalise as `resource` does (one URL per
repository across remote, API, raw and Pages forms). The Claude Code
agent's `WebFetch` calls are the only HTTP-tool-shaped calls in the
dataset.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/datasets/ai_village/mod.rs` | the source and its modes | `AiVillageSource`, `Mode`, `Stats`, `AiVillageError`, `DATASET`, `DEFAULT_FROM`, `DEFAULT_TO` |
| `…/stream.rs` | streaming a table | `Table` (`scan`, `load`, `path`, `file_name`), `decode`, `created_at`, `string_field`, `StreamError` |
| `…/time.rs` | times, village days, windows | `parse_timestamp`, `format_seconds`, `Day`, `village_day`, `Window`, `TimeError` |
| `…/schema.rs` | the rows read | `AgentRow`, `RoomRow`, `SessionRow`, `TurnRow`, `EventRow`, `ChatRow`, `MemoryRow`, `VillageGoalRow`, `AgentGoalRow`, `ClaudeCodeRow` |
| `…/tables.rs` | reusable passes | `load_directory`/`Directory`, `load_sessions`, `scan_turns`, `scan_events`/`EventScan`, `events_by_id`, `leading_id`, `scan_chat`, `scan_memories`/`Memories`, `load_goals`/`Goals` |
| `…/rooms.rs` | room membership | `RoomTimeline`, `ROOMS_V1_MICROS` |
| `…/resource.rs` | canonical resources | `from_url`, `from_remote`, `repo`, `Forge`, `urls` |
| `…/access/mod.rs` | bash accesses | `Shell` (`accesses`), `Access`, `Op`, `Tool`, `HOME` |
| `…/access/shell.rs` | word splitting | `commands`, `SimpleCommand`, `heredoc_argument` |
| `…/provider/{mod,anthropic,openai,gemini}.rs` | provider responses → canonical | `response`, `Response`, `arguments`, `anthropic::{block, tool_result}` |
| `…/text.rs` | locating text, match needs | `json_escape`, `escapes`, `find`, `need`, `visible_text` |
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
| `tests/ai_village/` | synthetic fixtures written to a temp dir; units, Claude Code, window | |

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
