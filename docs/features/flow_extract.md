# Flow extraction (L5): tool calls to resource accesses

`crosstalk-flow`'s `extract` module (`crates/flow/src/extract/`) implements
the spec's `ResourceExtractor` (`spec/types/interfaces/l5_flow.rs`),
roadmap P5's first item. It turns one assistant tool call, and its result
once that arrives, into the accesses the call implies: which resource,
read or written, how the locator was found (`Extraction`) and, for a write,
what became of it (the spec's `WriteOutcome`). Everything here
is pure and synchronous. The flow consumer (feat/flow-correlator) calls it
and turns its output into stored `Access`es.

The key property is **canonical resource identity**: two agents that touch
one thing (a file, a URL, a wiki page, a shared repository, a file of it,
an issue or pull request, an MCP resource) through different tools, working directories, clones or
spellings get one `Locator`, and so one `ResourceId`. The registry refuses
a second resource with a stored locator (`TrafficError::DuplicateLocator`),
so locator equality is resource identity.

## Scope

- Claude Code's tools: `Read`, `Write`, `Edit`, `MultiEdit`,
  `NotebookEdit`, `NotebookRead`, `WebFetch`, `Bash`; the same kind of
  tools of OpenCode, pi, Gemini CLI, Codex, OpenHands (`execute_bash`,
  `str_replace_editor`) and the Anthropic text editor tool; the
  `web_fetch` server tool.
- HTTP tools: `http_request {method, url, body?}` (the eval converter's
  shape) and the other configured names (`fetch`, `web_fetch`, `curl` by
  default), with the method deciding the op.
- Shell commands: a conservative lexer and interpreter for redirections,
  file readers (`cat`, `head`, `tail`, `tac`, `bat`, `sed -n 'X,Yp'`),
  `tee`, `curl`, `wget`, `cd`, `git clone`/`remote`/`config`/`show`,
  `git push`/`pull`/`fetch`, and the forge CLIs `gh` and `glab` (`repo
  clone`, issue and pull/merge request `create`/`comment`/`note`/`edit`/
  `update`/`review`/`view`/`list`, `api`).
- MCP tools mapped by configuration: tool name and argument paths (JSON
  Pointers) to a resource (a keyed collection, a URL or a file) and an op,
  with server aliases and refusal markers. The M2 wiki server is a
  configuration, not code.
- Canonicalization: lexical file paths, relative paths against the stated
  working directory, URLs, MCP keys, MediaWiki pages, repositories
  (`Locator::Repository`) and their files, issues and pull/merge requests
  on GitHub and `gitlab.com`, files of known clones.
- Write outcomes per known tool (`Delivered`, `Rejected`, `Unknown`), and
  reads kept only for a delivered result.
- Fetch tools configured by name (`fetch_tools`): a tool whose `url`
  argument names the page it reads, such as AgentDojo's `get_webpage`.
- A fetch or HTTP tool's `url` with no scheme: a bare
  `host[:port][/path…]` whose host is a domain (`www.informations.com`)
  reads as `https://`; a lone file name (`README.md`) or words stay an
  `Arguments` error (`flow.extract.bare-host-url-is-https`, INV-1121).
- The spans a write carries, from L4's spans (originated, forwarded from
  an input, and the writer's own relayed sources), and the stored
  `AccessOp`.
  for a known shell command from its output; reads kept only for a
  delivered result.
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
  `index.php` `submit`; forge URLs other than GitHub's and `gitlab.com`'s
  (a self-hosted GitLab's or GitHub Enterprise's web URLs stay URLs; their
  remotes and CLI commands are still repositories and threads); branch
  refs with slashes in `blob` URLs; GitHub Pages user-site subdirectories,
  read as project repositories.
- `gh`/`glab` verbs other than the listed ones (`close`, `reopen`,
  `merge`, `checkout`, `diff`, …); a `create`'s new number, which only the
  result shows (writes are decided from the call,
  `flow.extract.write-locators-from-call`); `gh` and `glab` `graphql`.
- Exit statuses: a shell result's outcome is the wire's flag and, for the
  known commands, their output's text.
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
     Mcp (configured) | Http (configured names) | Fetch (configured    │
     names, CONFIGURED_FETCH) | File | Fetch | Shell                   │
     none ─▶ Ok([])  (handles() is false)                              │
  result for another call id ─▶ treated as absent                      │
  Args::parse(arguments)          ─▶ ExtractError::Arguments           │
  family ─▶ candidates (kind, locator, via)                            │
     file::candidates      path arg ─▶ resource::file_locator          │
     fetch::candidates     url ─▶ tool_url_locator ─▶ http::candidates │
     http::tool_candidates method ─▶ op; url ─▶ tool_url_locator ─▶ site?
     bash::run             lex::Script::lex ─▶ commands::Shell::run    │
                             redirects, cat/tee/sed, net (curl/wget ─▶ │
                             HttpRequest ─▶ http::candidates),         │
                             git (clones bound in the shell state;     │
                             push/pull/fetch/clone ─▶ RepoId::locator),│
                             forge (gh/glab threads, api ─▶ HTTP)      │
                           each shell access tagged with the command   │
                           whose output judges it (CommandRule)        │
     mcp::candidates       ArgPath ─▶ KeyCanon | url | file locator    │
  each candidate:                                                      │
     outcome::judge(tool, candidate.rule, result)  one place           │
     Write ─▶ ExtractedOp::Write { outcome, payload }                  │
     Read  ─▶ ExtractedOp::Read if delivered (not Rejected), else      │
              dropped                                                  │
  dedupe ─▶ Vec<Classified> ◀──────────────────────────────────────────┘
  ResourceExtractor::extract ─▶ Classified::into_spec ─▶ ExtractedAccess

consumer (gateway extract layer), per access:
  spans::write_spans(call part, writer, L4 spans, source agent)  ─▶ Vec<SpanId>
    WritePayload::Unseen (git push) ─▶ no spans
  spans::access_op(op, call part, result part, spans)             ─▶ AccessOp
```

**URL accesses** all go through `http::candidates` (fetch tools, `curl`,
`wget`, MCP URL resources, scanned URLs) or `http::tool_candidates` (HTTP
tools): the URL is normalized, then the site rules (`sites::SitesConfig`)
may name the wiki page or repository file the request reaches.

### Write spans

`spans::write_spans` picks, among L4's spans of the writer's exchange,
the writer's spans located in the call's part:

| Span state | Carried |
| --- | --- |
| originated (`Originated`, `Indexed`, `Propagated`, `Expired`) | the span |
| `Relayed(Input(_))`: text forwarded from an input (a fetched document posted to a channel) | the span: forwarding counts as writing (`flow.access.write-spans-include-forwarded-input`, INV-1112). L4 indexes such a span as authored by the relaying agent, so a reader's match on it names the writer |
| `Relayed(Span(s))`, `s` the writer's own span | `s` (`flow.access.write-spans-include-self-relay`, INV-961) |
| `Relayed(Span(s))`, `s` another agent's (or unknown) | nothing: the match belongs to its originator |
| `Common`, `Extracted` | nothing |

### Canonical identity

| Thing | Locator | Rule |
| --- | --- | --- |
| an absolute path | `File { host, path }` | `.`/`..`/`//`/trailing `/` resolved lexically; host from the context (`None` locally) |
| a relative path | `File` under the cwd | resolved against the stated cwd, or the shell's tracked one |
| a relative path, no cwd; `~/x`; `C:\x` | `Opaque { tool, key }` | the path as written (`flow.resource.relative-path-opaque`) |
| a file in a known clone | `File { host: Some(<repo id>), path: <path in repo> }` | `RepoBindings::locate`, longest root |
| a URL | `Url { scheme, host, path, query }` | scheme/host lower case, IDNA, default port and fragment and user info dropped, dot segments resolved, percent-encoding normalized, query parameters sorted, empty query none |
| a MediaWiki page | `Url` of the canonical article: `https://en.wikipedia.org/wiki/Dead_drop` | title: `_`/whitespace runs one space, trimmed, `#section` dropped, first letter upper-cased on capital-links sites; mobile host folded |
| a forge repository | `Repository { host, owner, name }` | the spec's `Locator::repository`: host lower case without `www.`/port/trailing dot, owner (`/`-joined for nested groups) and name lower case, `.git` dropped. Reached by its remotes (https, `ssh://`, `git@host:o/n`, any case, `.git` or not) as `git clone`/`push`/`pull`/`fetch` operands or a clone's bound remote, and by `github.com/o/n`, `…/tree/<ref>`, `codeload.github.com/o/n/…`, `api.github.com/repos/o/n` and its other subpaths, Pages `o.github.io/n/…` (`o.github.io/` is `o/o.github.io`), `gitlab.com/<path…>/n`, `…/-/<other>`, `gitlab.com/api/v4/projects/<encoded path>`, `g.gitlab.io/p/…` |
| a repository on the local filesystem | `File { host: None, path: <repo dir> }` | `/srv/shared/atlas.git` and `file:///srv/shared/atlas` are `/srv/shared/atlas` |
| a forge file | `File { host: Some("<host>/<owner>/<repo>"), path }` | `blob`, `raw`, `raw.githubusercontent.com`, GitHub contents API, GitLab `/-/blob\|raw/<ref>/…` and `repository/files/<encoded>` URLs; the ref is dropped. The host is the repository's `host/owner/name` (`Locator::repository_file_host`) |
| a GitHub issue or pull request | `Url` `https://<host>/<owner>/<name>/issues/<n>` | for both: they share one number space and one conversation (`/issues/<n>` redirects to `/pull/<n>`; the REST API comments on both at `/issues/<n>/comments`). Reached by `github.com/o/n/issues\|pull/<n>/…`, `api.github.com/repos/o/n/issues\|pulls/<n>/…`, `gh issue\|pr <verb> <n>\|#<n>\|<url>` |
| a GitLab issue or merge request | `Url` `https://<host>/<owner>/<name>/-/issues/<n>`, `…/-/merge_requests/<n>` | web, API v4 (`projects/<p>/issues\|merge_requests/<n>/…`) and `glab issue\|mr <verb> <n>` |
| a repository's issues or pull/merge requests as a whole | `Url` `https://<host>/<owner>/<name>/issues`, `/pulls` (GitHub), `/-/issues`, `/-/merge_requests` (GitLab) | `create` and `list`, and a thread command without a number; the web and API collection URLs |
| an MCP resource | `Mcp { server, tool: <collection>, target }` | configured canonical server (aliases folded), the collection (not the tool) as `tool`, the key folded by `KeyCanon` |

A repository id is `host/owner/name` in lower case, without scheme, user,
port or `.git` (`https://github.com/A/B.git`, `git@github.com:a/b`,
`ssh://git@github.com:22/a/b` are one), or the normalized absolute path of
a local repository (`/srv/shared/atlas.git`, `file:///srv/shared/atlas`).
`RepoId::locator` is the repository's own locator, `RepoId::file` a file
in it, `ForgeRepo::thread`/`collection` its threads.

**Canonical forms, for the eval converter.** Repository:
`Locator::Repository { host: "github.com", owner: "agentvillage", name:
"atlas" }` (wire: `{"type": "repository", "data": {"host": "github.com",
"owner": "agentvillage", "name": "atlas"}}`). File in it: `File { host:
"github.com/agentvillage/atlas", path: "/src/app.py" }`. Thread:
`https://github.com/agentvillage/atlas/issues/13` (an issue or a pull
request), `https://gitlab.com/village/ops/infra/-/merge_requests/3`.
Collection: `https://github.com/agentvillage/atlas/issues` or `/pulls`.

### Git and the forge CLIs

| Command | Access | Locator | Payload | Judged by |
| --- | --- | --- | --- | --- |
| `git push [<remote>]` (not `-n`/`--dry-run`) | write | the remote operand if a URL or path, else the clone's bound repository | `Unseen`: no spans | `GitPush` |
| `git pull`, `git fetch [<remote>]` (`--all`: the bound one) | read | as for push | | `GitTransfer` |
| `git clone <remote>`, `gh repo clone`, `glab repo clone` | read (and the directory is bound) | the remote | | `GitTransfer` |
| `gh issue\|pr`, `glab issue\|mr` `create` | write | the collection | the call's arguments (title, body) | `ForgeCli` |
| … `comment`, `note`, `edit`, `update`, `review` | write | the thread of the number or URL operand, else the collection | the call's arguments | `ForgeCli` |
| … `view` | read | the thread, else the collection | | `ForgeCli` |
| … `list` | read | the collection | | `ForgeCli` |
| `gh api <endpoint>`, `glab api <endpoint>` | the `HttpTool` contract: `-X`, else `POST` with `-f`/`-F`/`--input`, else `GET` | the canonical URL of `https://api.github.com/<endpoint>` (`--hostname h`: `https://h/api/v3/…`) or `https://gitlab.com/api/v4/<endpoint>`, through the site rules; `{owner}`/`{repo}` and `:fullpath`/`:id`/`:namespace`/`:group`/`:repo` filled from the bound repository; `graphql` none | the call's arguments | `ForgeCli` |

The repository of a thread command is `-R`/`--repo` (`o/n`, `host/o/n`
when the first segment has a dot, `group/sub/n`, or a URL), else the URL
operand's, else the clone's bound repository; with none known there is no
access. A write's locator is decided from the call alone: the gateway
holds writes at the call and releases them in order with the result
(`flow.extract.write-locators-from-call`), so `gh issue create` writes the
collection even though its output prints the new issue's URL.

### Write outcomes

`outcome.rs` is the one place a result is judged:

1. no result: `Unknown` (the consumer extracts a write without one only
   once its settle window closed);
2. `ToolOutcome::Error`: `Rejected`;
3. for an access a known shell command made, that command's output
   (`CommandRule`), when it says something:

   | Command | `Rejected` | `Delivered` | neither |
   | --- | --- | --- | --- |
   | `git push` | a line opening with `! [rejected]`, `! [remote rejected]`, `error:`, `fatal:`, `remote: Permission`, `remote: Invalid`, `Permission denied` | a ref update (`a..b  main -> main`, `+ a...b`, `* [new branch]`) or `Everything up-to-date` | `Unknown` |
   | `git pull`/`fetch`/`clone` | a line opening with `fatal:` or `error:` | | the next step |
   | `curl`, `wget` | the last HTTP status shown is 4xx/5xx (`HTTP/x 404` status lines from `-i`/`-I`/`-v`, wget's `awaiting response... 404` and `ERROR 404:`, the trailing code a `-w '%{http_code}'` printed), or `curl: (N)` | the last status shown is 2xx/3xx | the next step |
   | `gh`, `glab` | a line opening with `gh: `, `glab: `, `GraphQL:`, `error:`, `ERROR:`, `HTTP 4`/`HTTP 5`, `could not`, `failed to`, `X `, or ending `(HTTP 4xx/5xx)` | a line opening with `https://` or `✓` | the next step |

   The output is the whole call's (a script's commands share one result);
4. otherwise the tool's content rule (`content_rule`): file tools read
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
- A `url` that does not parse but is a bare `host[:port][/path…]` (no
  scheme or whitespace, a domain host with a dot and an alphabetic
  top-level label of two or more characters, an optional numeric port)
  is read as `https://` (`resource::tool_url_locator`, the same for fetch
  tools). Text that is only a name ending in a common file extension
  (`README.md`, `main.rs`), with no `www.`, port or path, is a file name,
  not a host; the cost is that a bare `docs.rs` is not read
  (`docs.rs/serde` is).
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
- the directory: for a tool whose shell persists (Claude Code `Bash`,
  OpenHands `execute_bash`), the
  directory the last command left the shell in, unknown after a `cd` it
  cannot follow; Claude Code's "Shell cwd was reset to <dir>" wins.

Within one call the shell interpreter applies its own `cd`s and clones as
it goes (`git clone … && cat repo/README.md` reads the repository file).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `extract/mod.rs` | the composite extractor | `ToolExtractors` (`new`, `identify`, `extract_classified`, `ResourceExtractor`) |
| `extract/op.rs` | outputs; re-exports the spec's `WriteOutcome` and `ExtractedOp` | `WriteOutcome` (`pairs`), `ExtractedOp`, `Classified` (`into_spec`), `Candidate` |
| `extract/outcome.rs` | result judging, per known tool | `ContentRule`, `content_rule`, `write_outcome`, `read_delivered`, `result_text` |
| `extract/catalog.rs` | known tools | `KnownTool`, `FileTool`, `FetchTool`, `ShellTool`, `HttpTool`, `McpTool`, `FILE_TOOLS`, `FETCH_TOOLS`, `CONFIGURED_FETCH`, `SHELL_TOOLS`, `identify`, `mcp_name` |
| `extract/op.rs` | outputs; re-exports the spec's `WriteOutcome`, `ExtractedOp` and `WritePayload` | `WriteOutcome` (`pairs`), `ExtractedOp`, `WritePayload`, `Classified` (`into_spec`), `Candidate` (`unseen`, `judged_by`) |
| `extract/outcome.rs` | result judging, per known tool and known shell command | `ContentRule`, `content_rule`, `CommandRule` (`judge`), `judge`, `write_outcome`, `read_delivered`, `result_text` |
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
| `extract/bash/git.rs` | `git`: clones, remotes, `show`, `push`/`pull`/`fetch`/`clone` | |
| `extract/bash/forge.rs` | `gh`, `glab`: `repo clone`, issue and pull/merge request commands, `api` | |
| `extract/bash/options.rs` | getopt-style splitting | `Options`, `OptSpec` |
| `extract/mcp/mod.rs` | configured MCP tools | `candidates` |
| `extract/mcp/config.rs` | the configuration | `ExtractConfig` (`from_json`, `new`, `with_http_tools`, `with_fetch_tools`, `with_sites`, `rule`, `http_tools`, `fetch_tools`), `McpServerConfig`, `McpToolRule`, `McpAccessRule`, `McpResource`, `RuleOp`, `RefusalMarker`, `ConfigError` (`HttpAndFetch` among them), `DEFAULT_HTTP_TOOLS` |
| `extract/resource/path.rs` | paths | `AbsolutePath`, `WrittenPath`, `FileScope`, `file_locator`, `absolute_locator`, `PathError` |
| `extract/resource/url.rs` | URLs | `url_locator`, `tool_url_locator` (a bare host as `https://`), `url_text`, `scan_urls`, `UrlError` |
| `extract/resource/key.rs` | MCP keys | `KeyCanon`, `KeyError` |
| `extract/resource/repo.rs` | repositories and their threads | `RepoId` (`parse`, `forge`, `locator`, `forge_parts`, `file`), `ForgeRepo` (`thread`, `collection`), `ForgeStyle`, `ThreadKind`, `RepoBindings` |
| `extract/sites/mod.rs` | site rules | `SitesConfig`, `MediaWikiSite`, `HostPattern`, `SitePath`, `SiteAccess` |
| `extract/sites/mediawiki.rs` | MediaWiki | `apply`, `canonical_title`, `page_locator` |
| `extract/sites/github.rs` | GitHub: repositories, files, threads, Pages | `apply` |
| `extract/sites/gitlab.rs` | `gitlab.com`: projects, files, threads, Pages | `apply` |
| `extract/spans.rs` | write spans, `AccessOp` | `write_spans`, `access_op`, `AccessOpError` |
| `extract/fuzz.rs`, `extract/tests/`, `extract/resource/tests.rs` | tests (`tests/fetch_config.rs`: configured fetch tools); `tests/wiki.json` is the M2 wiki configuration fixture | |
| `extract/fuzz.rs`, `extract/tests/`, `extract/resource/tests.rs` | tests; `tests/wiki.json` is the M2 wiki configuration fixture; `tests/forges.rs` repositories, git, the forge CLIs, shell outcomes, `sed` and OpenHands | |
| `spec/types/derived/flow/resource.rs` | the spec's repository locator | `Locator::Repository`, `Locator::repository` (`InvalidRepository`), `Locator::repository_file_host` |
| `spec/types/interfaces/l5_flow.rs` | where a write's content is | `ExtractedOp::Write { outcome, payload }`, `ExtractedOp::write`, `WritePayload` (`CallArguments`, `Unseen`) |
| `crates/flow/src/correlate/tests/shared_web.rs` | shared public web content stays suspected | |

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
  "fetch_tools": ["get_webpage"],
  "sites": {
    "mediawiki": [
      { "hosts": ["*.wikipedia.org"], "article_path": "/wiki/", "script_path": "/w/",
        "capital_links": true, "fold_mobile_host": true }
    ],
    "github": true,
    "gitlab": true
  }
}
```

- `mcp_servers`: default none. A server and its aliases are unique across
  servers; a tool appears once per server and has at least one access;
  names, collections and markers are non-empty. Read and write tools of
  one server name the same `collection`, which is what makes a write and a
  read meet.
- `http_tools`: default `http_request`, `fetch`, `web_fetch`, `curl`.
- `fetch_tools`: default none. Names of tools whose `url` argument names
  the page they read and whose result is the page (AgentDojo's
  `get_webpage`): each is extracted as the built-in fetch tools are (a
  `Structured` read of the normalized URL, site rules applied, kept only
  with a delivered result; no `url` is `ExtractError::Arguments`). They
  are identified after MCP and HTTP tools and before the built-in tables,
  so a configured name shadows a built-in one. A name may not be both an
  HTTP and a fetch tool (`ConfigError::HttpAndFetch`); empty names are
  refused. The built-in fetch tools stay known whatever is configured
  (`flow.extract.fetch-tools-configured`, INV-1113). The gateway reads
  this configuration from its config's `extract` section
  (`LiveConfig::extract`); the eval from `ct-eval run --extract-config`.
- `sites`: default the Wikimedia projects (`/wiki/`, `/w/`, capital links,
  mobile hosts folded), Wiktionary (no capital links), Fandom
  (`script_path` `/`), and GitHub on. Giving `mediawiki` replaces the
  built-in list. A host pattern is a lower-case host or `*.suffix`
  (the suffix and its subdomains); paths start and end with `/`.
  `github` and `gitlab` (default on) switch the forge URL rules; the
  `gh`/`glab` CLIs read a URL operand through them either way.

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
- `flow.access.write-spans-include-forwarded-input` (INV-1112):
  `extract::tests::spans::write_spans_include_the_writers_forwarded_input`
  and the property `extract::tests::write_spans_include_self_relayed_sources`
  (whose expectation now carries input relays; INV-961's statement was
  amended to match).
- `flow.extract.fetch-tools-configured` (INV-1113):
  `extract::tests::fetch_config`.
- `flow.extract.bare-host-url-is-https` (INV-1121):
  `extract::tests::bare_url`.
- New (INV-X): `flow.extract.http-method-op`,
  `flow.resource.http-url-tool-independent`,
  `flow.resource.wiki-page-spelling-independent`,
  `flow.resource.repo-file-clone-independent`.
- `flow.extract.http-method-op` (INV-1047),
  `flow.resource.http-url-tool-independent` (INV-1048),
  `flow.resource.repo-file-clone-independent` (INV-1049),
  `flow.resource.wiki-page-spelling-independent` (INV-1050).
- Repositories and forges: `flow.resource.repository-canonical`
  (INV-1080, the spec's constructor), `flow.resource.repository-forms-meet`
  (INV-1081), `flow.extract.git-repository-ops` (INV-1082),
  `flow.extract.forge-cli-threads` (INV-1083),
  `flow.extract.write-locators-from-call` (INV-1084),
  `flow.extract.shell-outcome-from-known-output` (INV-1085),
  `flow.extract.sed-print-read` (INV-1086),
  `flow.route.shared-public-content-stays-suspected` (INV-1087, the
  correlator's), `flow.extract.openhands-tools` (INV-1088).
- A write's locators and payloads are a function of the call alone; only
  its outcome reads the result (INV-1084).
- A write whose content is not in the call (`WritePayload::Unseen`)
  carries no spans: a read of its resource is co-access evidence only.
- Eval spec invariants tested here:
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

## Eval spec types

The extractor uses the eval spec's (#58) `WriteOutcome`, `ExtractedOp`,
`ExtractedAccess { op, .. }`, `AccessOp::Write { outcome, .. }` and
`ToolOutcome::Unknown` directly:

1. `op.rs` re-exports the spec's `WriteOutcome` and `ExtractedOp`;
   `Classified::into_spec` builds `ExtractedAccess { op, locator, via }`.
2. `outcome.rs`: `ToolOutcome::Unknown` goes through the tool's content
   rule like `Success`; a tool with no content rule keeps `Unknown`.
3. `spans.rs`: `access_op` puts the outcome into `AccessOp::Write`.
4. `context.rs`: `observe` keeps learning on `ToolOutcome::Unknown` (it
   skips only `Error`).
