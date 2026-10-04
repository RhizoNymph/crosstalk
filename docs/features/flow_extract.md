# Flow extraction (L5): tool calls to resource accesses

`crosstalk-flow`'s `extract` module (`crates/flow/src/extract/`) implements
the spec's `ResourceExtractor` (`spec/types/interfaces/l5_flow.rs`),
roadmap P5's first item. It turns one assistant tool call, and its result
once that arrives, into the accesses the call implies: which resource,
read or written, how the locator was found (`Extraction`) and, for a write,
what became of it (`WriteOutcome`, from the eval spec PR). Everything here
is pure and synchronous. The flow consumer (feat/flow-correlator) calls it
and turns its output into stored `Access`es.

The key property is **canonical resource identity**: two agents that touch
one thing (a file, a URL, a wiki page, a file of a shared repository, an
MCP resource) through different tools, working directories, clones or
spellings get one `Locator`, and so one `ResourceId`. The registry refuses
a second resource with a stored locator (`TrafficError::DuplicateLocator`),
so locator equality is resource identity.

## Scope

- Claude Code's tools: `Read`, `Write`, `Edit`, `MultiEdit`,
  `NotebookEdit`, `NotebookRead`, `WebFetch`, `Bash`; the same kind of
  tools of OpenCode, pi, Gemini CLI, Codex and the Anthropic text editor
  tool; the `web_fetch` server tool.
- HTTP tools: `http_request {method, url, body?}` (the eval converter's
  shape) and the other configured names (`fetch`, `web_fetch`, `curl` by
  default), with the method deciding the op.
- Shell commands: a conservative lexer and interpreter for redirections,
  file readers (`cat`, `head`, `tail`, `tac`, `bat`), `tee`, `curl`,
  `wget`, `cd`, `git clone`/`remote`/`config`/`show`, `gh repo clone`.
- MCP tools mapped by configuration: tool name and argument paths (JSON
  Pointers) to a resource (a keyed collection, a URL or a file) and an op,
  with server aliases and refusal markers. The M2 wiki server is a
  configuration, not code.
- Canonicalization: lexical file paths, relative paths against the stated
  working directory, URLs, MCP keys, MediaWiki pages, GitHub files, files
  of known clones.
- Write outcomes per known tool (`Delivered`, `Rejected`, `Unknown`), and
  reads kept only for a delivered result.
- The spans a write carries, from L4's spans, and the stored `AccessOp`.
- The conversation context: the working directory stated in a system
  prompt, and what it learns from shell calls (the persistent shell's
  directory, clones and remotes).

## Non-scope

- Building and storing `Access`es (ids, agent, exchange, time), holding a
  write until its result or its settle window, the registry lookups and
  `AccessRecorded`: the flow consumer's (feat/flow-correlator).
- Scanning URLs out of arbitrary tools' arguments (`UrlScanFallback` in the
  spec's list): URLs are scanned only from a fetch tool's prompt argument.
- Code other than shell (Python, JavaScript snippets); shell constructs
  past simple commands (functions, loops, `if`, nested `bash -c`
  strings); globs and expansions (a word the shell would expand names no
  resource); `Grep`/`Glob` results; `git diff`/`log -p` content.
- MediaWiki namespace folding (`talk:` and `Talk:` stay apart), wiki
  writes other than `edit`, `delete`, `protect`, `undelete`, `move` and
  `index.php` `submit`; forges other than GitHub; branch refs with slashes
  in GitHub `blob` URLs.
- Unicode normalization (NFC) of keys and titles.
- Symlinks: path resolution is lexical (`flow.resource.path-normalization`).

## Data and control flow

```text
flow consumer, per conversation, in order
  ConversationContext::from_system_prompt(system)      stated cwd
  for each tool call (and its result once it arrives):
    ToolExtractors::new(&config, &context)
      .extract_classified(call, result)  ─────────────────────────────┐
    context.observe(&config, call, result)   learns cwd, clones        │
                                                                       │
extract_classified                                                     │
  catalog::identify(name, config) ─▶ KnownTool                         │
     File | Fetch | Shell | Mcp (configured) | Http (configured names) │
     none ─▶ Ok([])  (handles() is false)                              │
  result for another call id ─▶ treated as absent                      │
  Args::parse(arguments)          ─▶ ExtractError::Arguments           │
  family ─▶ candidates (kind, locator, via)                            │
     file::candidates      path arg ─▶ resource::file_locator          │
     fetch::candidates     url ─▶ url_locator ─▶ http::candidates (GET)│
     http::tool_candidates method ─▶ op; url ─▶ url_locator ─▶ site?   │
     bash::run             lex::Script::lex ─▶ commands::Shell::run    │
                             redirects, cat/tee, net (curl/wget ─▶     │
                             HttpRequest ─▶ http::candidates),         │
                             git/gh (clones bound in the shell state)  │
     mcp::candidates       ArgPath ─▶ KeyCanon | url | file locator    │
  outcome::write_outcome(tool, result)     one place, per known tool   │
  outcome::read_delivered(tool, result)                                │
  each candidate:                                                      │
     Write ─▶ ExtractedOp::Write(outcome)                              │
     Read  ─▶ ExtractedOp::Read if delivered, else dropped             │
  dedupe ─▶ Vec<Classified> ◀──────────────────────────────────────────┘
  ResourceExtractor::extract ─▶ Classified::into_spec ─▶ ExtractedAccess

consumer, per access:
  spans::write_spans(call part, writer, L4 spans, source agent)  ─▶ Vec<SpanId>
  spans::access_op(op, call part, result part, spans)             ─▶ AccessOp
```

**URL accesses** all go through `http::candidates` (fetch tools, `curl`,
`wget`, MCP URL resources, scanned URLs) or `http::tool_candidates` (HTTP
tools): the URL is normalized, then the site rules (`sites::SitesConfig`)
may name the wiki page or repository file the request reaches.

### Canonical identity

| Thing | Locator | Rule |
| --- | --- | --- |
| an absolute path | `File { host, path }` | `.`/`..`/`//`/trailing `/` resolved lexically; host from the context (`None` locally) |
| a relative path | `File` under the cwd | resolved against the stated cwd, or the shell's tracked one |
| a relative path, no cwd; `~/x`; `C:\x` | `Opaque { tool, key }` | the path as written (`flow.resource.relative-path-opaque`) |
| a file in a known clone | `File { host: Some(<repo id>), path: <path in repo> }` | `RepoBindings::locate`, longest root |
| a URL | `Url { scheme, host, path, query }` | scheme/host lower case, IDNA, default port and fragment and user info dropped, dot segments resolved, percent-encoding normalized, query parameters sorted, empty query none |
| a MediaWiki page | `Url` of the canonical article: `https://en.wikipedia.org/wiki/Dead_drop` | title: `_`/whitespace runs one space, trimmed, `#section` dropped, first letter upper-cased on capital-links sites; mobile host folded |
| a GitHub file | `File { host: Some("github.com/<owner>/<repo>"), path }` | `blob`, `raw`, `raw.githubusercontent.com` and contents API URLs; the ref is dropped |
| an MCP resource | `Mcp { server, tool: <collection>, target }` | configured canonical server (aliases folded), the collection (not the tool) as `tool`, the key folded by `KeyCanon` |

A repository id is `host/owner/name` in lower case, without scheme, user,
port or `.git` (`https://github.com/A/B.git`, `git@github.com:a/b`,
`ssh://git@github.com:22/a/b` are one), or the normalized absolute path of
a local repository (`/srv/shared/atlas.git`, `file:///srv/shared/atlas`).

### Write outcomes

`outcome.rs` is the one place a result is judged:

1. no result: `Unknown` (the consumer extracts a write without one only
   once its settle window closed);
2. `ToolOutcome::Error`: `Rejected`;
3. otherwise the tool's content rule (`content_rule`): file tools read
   Claude Code's refusals (`<tool_use_error>`, "The user doesn't want to
   proceed with this tool use"); a configured MCP tool reads its refusal
   markers; fetch, shell and HTTP tools have none, and keep the wire's
   word (`Success` is `Delivered`; the coming `ToolOutcome::Unknown` will
   be `Unknown`).

A read is kept only with a result the same rule does not judge `Rejected`.
A rejected write is still returned (recorded, never paired:
`WriteOutcome::pairs`).

### HTTP tools

A call of a configured HTTP tool whose arguments carry `url` and `method`:

- `GET`, `HEAD`: Read (the tool result is the read part); `POST`, `PUT`,
  `PATCH`, `DELETE`: Write (the body, the first of `body`, `content`,
  `text`, `data`, is what was written); any other method: no access
  (`flow.extract.http-method-op`). The method's case is ignored.
- The locator is the normalized URL, never the tool name
  (`flow.resource.http-url-tool-independent`). A site rule names the page
  or file instead only when its op agrees with the method's: a MediaWiki
  API `POST` with `action=edit&title=X` in the body (form-encoded string or
  JSON object) writes page X, and a later `GET` of X's article reads it.
- Without `method` (or `url`), a name that is also a fetch tool
  (`web_fetch`) is that fetch tool (a read); any other is
  `ExtractError::Arguments`. `WebFetch` is not an HTTP tool and stays
  read-only.

### The conversation context

`ConversationContext::observe` learns from a shell call whose result
arrived without an error, after it is extracted:

- clones: `git clone <remote> [<dir>]` (default directory: the remote's
  last segment as written), `gh repo clone <owner/repo> [<dir>]`,
  `git remote add|set-url`, and the remote a lone `git remote -v`,
  `git remote get-url` or `git config --get remote.<n>.url` prints, each
  bound to the directory it ran in (`git -C` honoured);
- the directory: for a tool whose shell persists (Claude Code `Bash`), the
  directory the last command left the shell in, unknown after a `cd` it
  cannot follow; Claude Code's "Shell cwd was reset to <dir>" wins.

Within one call the shell interpreter applies its own `cd`s and clones as
it goes (`git clone … && cat repo/README.md` reads the repository file).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `extract/mod.rs` | the composite extractor | `ToolExtractors` (`new`, `identify`, `extract_classified`, `ResourceExtractor`) |
| `extract/op.rs` | outputs; mirrors of the eval PR's types | `WriteOutcome` (`pairs`), `ExtractedOp`, `Classified` (`into_spec`), `Candidate` |
| `extract/outcome.rs` | result judging, per known tool | `ContentRule`, `content_rule`, `write_outcome`, `read_delivered`, `result_text` |
| `extract/catalog.rs` | known tools | `KnownTool`, `FileTool`, `FetchTool`, `ShellTool`, `HttpTool`, `McpTool`, `FILE_TOOLS`, `FETCH_TOOLS`, `SHELL_TOOLS`, `identify`, `mcp_name` |
| `extract/args.rs` | arguments | `Args`, `ArgPath` (JSON Pointer), `ArgError`, `InvalidArgPath` |
| `extract/context.rs` | per-conversation context | `ConversationContext` (`from_system_prompt`, `scope`, `bind_repo`, `observe`), `stated_cwd` |
| `extract/error.rs` | `From` into the spec's `ExtractError` | |
| `extract/file.rs` | file tools | `candidates` |
| `extract/fetch.rs` | fetch tools | `candidates` |
| `extract/http.rs` | HTTP requests and HTTP tools | `Method`, `HttpRequest`, `candidates`, `tool_candidates`, `form_fields`, `BODY_KEYS` |
| `extract/bash/mod.rs` | shell tools | `candidates`, `run` |
| `extract/bash/lex.rs` | shell lexer | `Script` (`lex`, `from_argv`), `Command`, `Word`, `Redirect`, `RedirectOp`, `LexError` |
| `extract/bash/commands.rs` | shell interpreter | `Shell`, `ShellState`, `ShellRun` |
| `extract/bash/net.rs` | `curl`, `wget` | |
| `extract/bash/git.rs` | `git`, `gh` | |
| `extract/bash/options.rs` | getopt-style splitting | `Options`, `OptSpec` |
| `extract/mcp/mod.rs` | configured MCP tools | `candidates` |
| `extract/mcp/config.rs` | the configuration | `ExtractConfig` (`from_json`, `new`, `with_http_tools`, `with_sites`, `rule`), `McpServerConfig`, `McpToolRule`, `McpAccessRule`, `McpResource`, `RuleOp`, `RefusalMarker`, `ConfigError`, `DEFAULT_HTTP_TOOLS` |
| `extract/resource/path.rs` | paths | `AbsolutePath`, `WrittenPath`, `FileScope`, `file_locator`, `absolute_locator`, `PathError` |
| `extract/resource/url.rs` | URLs | `url_locator`, `url_text`, `scan_urls`, `UrlError` |
| `extract/resource/key.rs` | MCP keys | `KeyCanon`, `KeyError` |
| `extract/resource/repo.rs` | repositories | `RepoId` (`parse`, `forge`, `file`), `RepoBindings` |
| `extract/sites/mod.rs` | site rules | `SitesConfig`, `MediaWikiSite`, `HostPattern`, `SitePath`, `SiteAccess` |
| `extract/sites/mediawiki.rs` | MediaWiki | `apply`, `canonical_title`, `page_locator` |
| `extract/sites/github.rs` | GitHub | `apply` |
| `extract/spans.rs` | write spans, `AccessOp` | `write_spans`, `access_op`, `AccessOpError` |
| `extract/fuzz.rs`, `extract/tests/`, `extract/resource/tests.rs` | tests; `tests/wiki.json` is the M2 wiki configuration fixture | |

## Configuration

JSON, in the spec's conventions (snake_case keys, enums tagged
`type`/`data`, unknown fields refused), parsed and checked by
`ExtractConfig::from_json`. Every key is optional:

```json
{
  "mcp_servers": [
    {
      "server": "wiki",
      "aliases": ["team-wiki"],
      "tools": [
        {
          "tool": "write_page",
          "accesses": [
            { "op": "write",
              "resource": { "type": "keyed", "data": {
                "collection": "page", "target": "/title",
                "canon": { "fold_case": true, "fold_separators": true, "path_like": true } } } }
          ],
          "refusal": [{ "type": "prefix", "data": "Error:" }]
        },
        {
          "tool": "read_page",
          "accesses": [
            { "op": "read",
              "resource": { "type": "keyed", "data": {
                "collection": "page", "target": "/title",
                "canon": { "fold_case": true, "fold_separators": true, "path_like": true } } } }
          ]
        }
      ]
    },
    { "server": "fetch",
      "tools": [{ "tool": "fetch", "accesses": [
        { "op": "read", "resource": { "type": "url", "data": { "arg": "/url" } } } ] }] }
  ],
  "http_tools": ["http_request", "fetch", "web_fetch", "curl"],
  "sites": {
    "mediawiki": [
      { "hosts": ["*.wikipedia.org"], "article_path": "/wiki/", "script_path": "/w/",
        "capital_links": true, "fold_mobile_host": true }
    ],
    "github": true
  }
}
```

- `mcp_servers`: default none. A server and its aliases are unique across
  servers; a tool appears once per server and has at least one access;
  names, collections and markers are non-empty. Read and write tools of
  one server name the same `collection`, which is what makes a write and a
  read meet.
- `http_tools`: default `http_request`, `fetch`, `web_fetch`, `curl`.
- `sites`: default the Wikimedia projects (`/wiki/`, `/w/`, capital links,
  mobile hosts folded), Wiktionary (no capital links), Fandom
  (`script_path` `/`), and GitHub on. Giving `mediawiki` replaces the
  built-in list. A host pattern is a lower-case host or `*.suffix`
  (the suffix and its subdomains); paths start and end with `/`.

## Invariants and constraints

- `flow.extract.no-panic` (INV-254): `extract::fuzz::extract_arbitrary_call`
  (proptest over arbitrary names, JSON, invalid argument text, results and
  contexts, including `observe`), plus lexer fuzzing.
- `flow.extract.read-requires-result` (INV-255):
  `extract::tests::no_read_without_result`.
- `flow.resource.path-normalization` (INV-263),
  `flow.resource.relative-path-cwd` (INV-266),
  `flow.resource.relative-path-opaque` (INV-267),
  `flow.resource.url-normalization` (INV-268),
  `flow.resource.pattern-overlap-exact` (INV-660, property side): in
  `extract::resource::tests`.
- New (INV-X): `flow.extract.http-method-op`,
  `flow.resource.http-url-tool-independent`,
  `flow.resource.wiki-page-spelling-independent`,
  `flow.resource.repo-file-clone-independent`.
- Eval spec PR invariants, tested here ahead of the merge:
  `flow.extract.write-outcome-classified`
  (`extract::tests::write_outcome_follows_result`,
  `extract::tests::message_tool_refusal_text_is_rejected`),
  `flow.access.write-spans-include-self-relay`
  (`extract::tests::write_spans_include_self_relayed_sources`; the PR names
  `crosstalk_flow::tests::…`), `flow.access.rejected-write-recorded`
  (`extract::tests::rejected_writes_are_still_extracted`, the extractor's
  half).
- A locator is a function of the canonical thing, never of the tool that
  reached it, except `Opaque`, which is keyed on the tool by design.
- Every access from a known argument is `Structured`, from shell code
  `Parsed`, from a prompt's free text `Scanned`
  (`flow.access.extraction-recorded` is the consumer's to keep).
- Extraction is pure: the same call, result, configuration and context give
  the same accesses. The context changes only through `observe`.
- No access carries credentials: URL user info is dropped (query
  parameters are kept, as the spec requires).

## Binding the eval spec PR

The PR adds `WriteOutcome`, `ExtractedOp`, `ExtractedAccess { op, .. }`,
`AccessOp::Write { outcome, .. }` and `ToolOutcome::Unknown`. At the merge:

1. `op.rs`: delete the local `WriteOutcome` and `ExtractedOp`, import the
   spec's; `Classified::into_spec` builds `ExtractedAccess { op, locator, via }`.
2. `outcome.rs`: add `ToolOutcome::Unknown => content rule, else Unknown` to
   `write_outcome`.
3. `spans.rs`: `access_op` puts the outcome into `AccessOp::Write`.
4. `context.rs`: `observe` keeps learning on `ToolOutcome::Unknown`.
