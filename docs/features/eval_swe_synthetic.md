# Eval: SWE background, splices and cipher pairs

Four corpora for `crosstalk-eval` (see [eval.md](eval.md)) that test what
the labelled multi-agent datasets cannot:

- **Background** (`open-swe`, `lmcache`): many independent single-agent
  SWE trajectories mixed into one world. Nothing passed between them, so
  every prediction is a false positive. The report gives the
  false-positive rate per 1k exchanges and the shared texts the false
  positives fell on.
- **Splices** (`swe-splice`): one exact Channel transmission planted in two
  real trajectories. A writes a file, and B later reads it through its
  harness.
- **Cipher pairs** (`cipher`): a payload arrives in another agent's input
  under an encoding. Encodings the spec's `Codec` names are expected
  `Decoded` transmissions. The others are expected but out of reach, and
  reported as missed by design.

```text
ct-eval run --dataset open-swe   [--limit SHARDS] [--count ROWS_PER_SHARD] [--agents-per-world N]
ct-eval run --dataset lmcache    [--limit FILES]  [--count SESSIONS_PER_FILE] [--agents-per-world N]
ct-eval run --dataset swe-splice [--count SPLICES] [--corpus-seed S] [--include TEXT]…
ct-eval run --dataset cipher     [--count PAIRS_PER_CIPHER] [--corpus-seed S] [--include POOL]…
```

## Scope

- Reading Parquet shards row by row with the `parquet` crate. Only the
  needed columns are projected, a row group can be read on its own, and
  rows decode into serde types.
- Converting OpenAI-chat messages, as the trace datasets record them, to
  canonical `MessageBody`s. Tool results without a `tool_call_id` are paired
  with their calls by position.
- The Open-SWE-Traces and LMCache agentic-trace converters, and the
  background world they share.
- The splice generator, with four variants: exact, whitespace-perturbed,
  JSON-string-embedded and base64.
- The cipher pair generator, with encoders reimplemented in Rust. In reach:
  base64, hex, URL and base64 then URL. Out of reach: rot13, rotN, binary8
  and letter substitution.
- `Tier::OutOfReach` and `MatchNeed::Undecodable`, the report's separate
  out-of-reach summary, and the background summary (false positives per 1k
  exchanges, top sources).

## Non-scope

- Positives in the background corpora. They come from the splices.
- Running the steganographic-evals repository's code. Only its payload
  text files are read.
- Teaching the reference matcher new codecs or routes. Its gaps show up as
  numbers below.
- Dataset bytes. Fixtures are synthetic.

## Data and control flow

```text
data/<harness>/<model>/<dataset>/*.parquet ─▶ open_swe::files::discover ─▶ RoundRobin (one row per shard in turn)
                                                │  ParquetRows<OpenSweRow> (projected: instance_id, repo, trajectory_id, messages)
                                                ▼
                                    open_swe::trajectory: chat::convert, call i = messages[..i] → messages[i]
                                                │  clock compose(i, slot, 0)
lmcache/data/*.parquet ─▶ segments (row groups) ─▶ Sessions (interleaved over groups/files)
                                                │  call r = input_r → first assistant message of input_{r+1}
                                                │  (Reconstructed, or Synthetic when history was rewritten)
                                                ▼
                                    background::BackgroundWorld
                                      add(Trajectory) per agent; finish():
                                      NoTransmission per (sender, reader exchange) of distinct trajectories,
                                      SharedSource when same group (repo / task) else Boilerplate,
                                      Coverage::Complete { Construction }

swe_splice::SpliceSource: pool (RoundRobin over Open-SWE shards)
   plan(n): SplitMix64::derived(seed, "splice/n") picks sender A (with a splicable write),
            reader B (another repo, an insertion point), variant = ALL[n mod 4]
   world(): A's messages with A's workdir rewritten to B's, so the written path is in B's tree;
            B gets [read call, read result] spliced before an assistant message
            (ReadForm::of(B): editor view, or shell cat inside {"returncode","output"} JSON);
            clocks put B's read call right after A's write call;
            BackgroundWorld + expect(Transmission: Channel/File(path), ToolResult,
            at the numbered lines, needs/tier from Variant::need(form))

cipher::CipherSource: pools (message_data/*.txt) × CipherKind × pair index
   Pair::plan: SplitMix64::derived(seed, "c/i") draws payload and key
   world(): sender note (plain payload) → receiver exchange carrying the encoding
            in a user turn (even i) or a read_mailbox tool result (odd i);
            one Direct label with Cipher::need(payload)

any source ─▶ pipeline::run ─▶ Scorer (+ sources::SourceTally over every violation)
         ─▶ Report { overall (no out-of-reach rows), out_of_reach, background }
```

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/eval/src/datasets/parquet_rows.rs` | typed, projected, streaming Parquet rows | `ParquetRows` (`open`, `open_group`), `row_groups`, `ParquetError` |
| `crates/eval/src/datasets/chat.rs` | OpenAI-chat messages to canonical bodies | `ChatMessage`, `ChatToolCall`, `ChatFunction`, `bodies`, `convert`, `synthetic_call_id`, `arguments`, `ChatError` |
| `crates/eval/src/datasets/rng.rs` | seeded generator | `SplitMix64` (`new`, `derived`, `next_u64`, `below`, `index`, `pick`, `shuffle`) |
| `crates/eval/src/datasets/background.rs` | worlds of independent agents | `BackgroundWorld` (`add`, `expect`, `finish`), `Trajectory`, `Call`, `stop_for`, `BackgroundError` |
| `crates/eval/src/datasets/open_swe/mod.rs` | Open-SWE-Traces converter | `OpenSweSource`, `Mixing`, `RoundRobin`, `calls`, `trajectory`, `mix`, `agent_name`, `AGENTS_PER_WORLD`, `COLUMNS`, `DATASET`, `OpenSweError` |
| `crates/eval/src/datasets/open_swe/files.rs` | shard discovery | `discover`, `Shard` |
| `crates/eval/src/datasets/open_swe/schema.rs` | projected row | `OpenSweRow` |
| `crates/eval/src/datasets/lmcache/mod.rs` | LMCache converter | `LmcacheSource`, `Sessions`, `Session`, `Segment`, `segments`, `discover`, `calls`, `group`, `trajectory`, `mix`, `LmcacheError` |
| `crates/eval/src/datasets/lmcache/schema.rs` | projected row | `LmcacheRow` |
| `crates/eval/src/datasets/swe_splice/mod.rs` | splice generator | `SpliceSource`, `Pooled`, `Plan`, `plan`, `world`, `workdir`, `rewrite_workdir`, `insertion_points`, `splicable`, `SPLICES`, `MIN_CONTENT`, `MAX_CONTENT`, `SpliceError` |
| `crates/eval/src/datasets/swe_splice/write.rs` | what a sender writes | `writes`, `heredocs`, `resolve`, `FileWrite`, `WriteForm` |
| `crates/eval/src/datasets/swe_splice/read.rs` | the reader's harness read | `ReadForm` (`of`, `call`, `result`), `numbered`, `view_banner`, `OBSERVATION` |
| `crates/eval/src/datasets/swe_splice/variant.rs` | the four variants | `Variant` (`render`, `need`), `perturb` |
| `crates/eval/src/datasets/cipher/mod.rs` | cipher pair generator | `CipherSource`, `Pair`, `Delivery`, `world`, `PAIRS_PER_CIPHER`, `CipherError` |
| `crates/eval/src/datasets/cipher/codec.rs` | encoders and needs | `CipherKind`, `Cipher` (`encode`, `need`), `base64`, `hex`, `url`, `rot`, `binary8`, `substitute` |
| `crates/eval/src/datasets/cipher/pools.rs` | payload pools | `Pool`, `load`, `DEFAULT_POOLS` |
| `crates/eval/src/score/sources.rs` | false-positive sources | `SourceTally`, `SourceCount`, `source_key`, `TOP_SOURCES`, `SOURCE_CHARS` |
| `crates/eval/src/report/mod.rs` | report (shared) | `Report::out_of_reach`, `Report::background`, `Background` |
| `crates/eval/src/truth/kinds.rs`, `truth/mod.rs` | labels (shared) | `Tier::OutOfReach`, `MatchNeed::Undecodable`, `InvalidLabel::Reach`; `CarrierKind` is the spec's |
| `crates/eval/tests/open_swe.rs`, `lmcache.rs`, `swe_splice.rs`, `cipher.rs` | integration tests | |
| `crates/eval/tests/fixtures/open_swe/`, `fixtures/lmcache/` | synthetic rows as JSONL, with the Parquet built from them and the `make.sql` that builds it (`duckdb < make.sql`, a one-off generator; tests read only the committed Parquet) | |
| `crates/eval/tests/fixtures/cipher/` | synthetic payload pools | |

## Invariants and constraints

- **Backgrounds have no positives.** Coverage is
  `Complete { Construction }`. Every ordered pair of distinct trajectories
  gets a negative control at each of the reader's exchanges. In a splice
  world, the planted (sender, reader exchange) gets none, so a mis-routed
  prediction there is a plain false positive and is not charged to
  boilerplate.
- **Out of reach exactly when undecodable.** `ExpectedTransmission::new`
  rejects a label whose need is `Undecodable` unless its tier is
  `OutOfReach`, and the reverse (`InvalidLabel::Reach`). Out-of-reach rows
  are kept out of `Report::overall` and reported in `Report::out_of_reach`.
  Gates select rows themselves, so a gate with no tier counts them too.
- **Needs follow the spec's codecs (#58).** A JSON string level is
  `Decoded([JsonString])`, and base64 inside one is
  `Decoded([JsonString, Base64])`. Two JSON string levels are never undone
  (`provenance.decode.one-string-level`), so a `json_string` file read
  through a shell `cat` is out of reach (`json_string+json_string`). The
  table:

  | Variant | Editor view | Shell cat (JSON output) |
  | --- | --- | --- |
  | exact | `Exact` | `Decoded([JsonString])` |
  | whitespace | `Normalized` | `Decoded([JsonString])` |
  | json_string | `Decoded([JsonString])` | out of reach |
  | base64 | `Decoded([Base64])` | `Decoded([JsonString, Base64])` |

  A cipher's need comes from its actual output. An encoding that changes
  nothing needs `Exact`, and a chain drops any layer that changed nothing.
- **Deterministic.** Splice `n` and cipher pair `c/i` draw from streams
  derived from `--corpus-seed`. Shards are read in sorted order. A rerun
  gives identical worlds and truth (tested).
- **Splice clock.** A's call `i` is at `compose(a0 + i, 1, 0)` and B's call
  `j` at `compose(b0 + j, 0, 0)`. The offsets put B's read call right after
  A's write call, so the write precedes the read.
- **Tool calls carry `signature: None`.** Trace datasets record no
  per-call signatures.
- **Bounded memory.** Parquet pages are read as rows are consumed. LMCache
  sessions are taken from row groups in turn, so a sample spreads over a
  file that is sorted by session.
- **The source tally is complete.** `Score::sources` counts every violation,
  whatever the example cap. It keeps the `TOP_SOURCES` largest.

## Reference baselines (2026-10-05, `ReferenceDetector::default()`: boilerplate cutoff 50, one string level)

Background, with 16 trajectories per world:

| Corpus | Worlds | Exchanges | False positives | Per 1k exchanges | Boilerplate / SharedSource |
| --- | --- | --- | --- | --- | --- |
| open-swe (`--count 16`, 13 shards) | 13 | 14,311 | 4,722 | 330.0 | 4,670 / 52 |
| lmcache (`--count 16`, 5 files) | 5 | 2,513 | 4,742 | 1,887.0 | 336 / 4,406 |

The cutoff (16 before, 50 now, L4's default) changes none of these
numbers. Classifying hits by string level removed 10 open-swe and 4
splice false positives that only two string levels undone explained
(4,732 and 1,632 before).

Top open-swe sources are harness and agent phrasing: `Current working
directory` (484), `the reproduction script` (393), `understand the
codebase` (233), site-packages paths (158), `Backward compatibility`
(142 + 95 + 53 in three spellings) and `All existing tests pass` (59).
LMCache's sessions are almost all Django tasks, so most of its false
positives fall on shared repository paths (`/workspace/django/django/`,
1,249) and `cd /workspace/django && python -c`.

Splices (`--count 80`, seed 0; 59 editor views, 21 shell reads). There are
1,628 false positives over 11,780 exchanges (138.2 per 1k):

| Variant | Editor view | Shell cat | All in reach |
| --- | --- | --- | --- |
| exact | 14 / 14 | 0 / 6 | 14 / 20 |
| whitespace | 15 / 15 | 0 / 5 | 15 / 20 |
| json_string | 14 / 14 | out of reach, 0 / 6 | 14 / 14 |
| base64 | 16 / 16 | 0 / 4 | 16 / 20 |

The reference finds every shell-read splice's content, but routes it
`Direct`: it resolves `str_replace_editor` paths to `Locator::File`, not
`cat -n P` in a bash command. So the labels go unfound, and the
predictions are counted as `direct` false positives. This is the
reference's gap, not the corpus's: crosstalk-flow's real L5 extractors
(`ToolExtractors`, default config, no working directory) resolve both the
spliced `bash {"command": "cat -n P"}` read and the editor's
`{"command": "view", "path": P}` to one `Locator::File { host: None,
path: P }`, as a `Read` (`tests/swe_splice.rs`,
`the_real_extractor_reads_a_shell_cat_as_the_same_file`). A detector built
on L5 routes shell-read splices through the file channel.

Cipher pairs (`--count 50`; 50 per cipher, over the four default pools):

| Cipher | Need | Found |
| --- | --- | --- |
| base64 | `Decoded([Base64])` | 18 / 50 |
| hex | `Decoded([Hex])` | 22 / 50 |
| url | `Decoded([UrlEncoding])` or `Exact` | 26 / 50 |
| base64_url | `Decoded([UrlEncoding, Base64])` or `[Base64]` | 20 / 50 |
| rot13, rot_n, binary8, substitution | out of reach | 0 / 200 (missed by design) |

In-reach misses concentrate in `short_phrases` (12 of 50 misses per
cipher) and `random_strs`. Their decoded text is below the matcher's
`min_span`.
