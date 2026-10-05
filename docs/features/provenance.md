# Provenance (L4)

`crosstalk-provenance` (`crates/provenance`), roadmap item P4.2. It
implements the spec's L4 interfaces (`crosstalk_spec::interfaces::l4_provenance`):

- it cuts each agent's output into spans and classifies them as
  originated, relayed or common;
- it indexes the originated spans' fingerprints;
- it scans every new input for other agents' spans, through decoders
  (base64, hex, URL, Unicode, JSON/YAML string escapes);
- it publishes `SpanOriginated`, `SpanRelayed` and `ContentMatched`.

It is a layer crate. It depends on `crosstalk-spec` and `crosstalk-store`
only. `crosstalk-memory`, `crosstalk-sim`, `crosstalk-testkit` and
`crosstalk-transport` are dev-dependencies.

## Scope

- `Winnowing` (`Fingerprinter`): winnowing over whitespace- and
  case-normalized shingles, with a stable hash and pinned golden vectors.
- Decoders: `Base64Decoder`, `HexDecoder`, `UrlDecoder` and
  `UnicodeNormalizer` (the spec's `Decoder`), plus `JsonStringDecoder` and
  `YamlStringDecoder`. The `DecodePipeline` runs them depth-bounded, and
  every decoded byte keeps a map back to the part text.
- `NovelRunSegmenter` (`Segmenter`).
- The scanner and the engine (`Provenance`): what one delta means, its
  index writes, replay on redelivery, and eviction.
- `PgFingerprintIndex` (`FingerprintIndex`) on Postgres.
- `ProvenanceStore`, a crate-local trait, with two implementations,
  `MemoryProvenanceStore` and `PgProvenanceStore`. It holds the records:
  - exchanges and their scan status;
  - the spans with their states and locations;
  - the matches.
- `DisabledSemanticMatcher` (`SemanticMatcher`): a stub.
- The bus consumer (group `provenance`).
- Typed config (`ProvenanceConfig`).

## Non-scope

- **Embeddings.** The semantic matcher stays a stub until P6.2. The scanner
  already calls `SemanticMatcher::lookup` and filters its hits, but nothing
  inserts embeddings yet.
- **Wiring into the gateway.** `consumer::subscribe` and `consumer::run`
  are shaped like the exchange log's stage (subscribe while building, then
  spawn), but `crosstalk_gateway::pipeline` does not run them yet.
- **Shard routing across nodes.** The index refuses misrouted fingerprints
  (`WrongShard`), and the scanner only sends fingerprints this node owns. A
  multi-node fan-out of lookups is not built.
- **The spec read traits for L4 records on Postgres** (`SpanIndex`, a
  later `ProvenanceReads`). The records and the indexes they need exist
  (see [Tables](#tables-and-indexes)); the memory store implements
  `SpanIndex` (below).
- **Coverage-guided fuzzing** (`fuzz` evidence) and the
  `span-state-written-only-by-advance` lint.

## Data and control flow

### Normalization, k-grams, winnowing

1. `text::normalize` folds every whitespace run to one space and lowercases
   every other character with `char::to_lowercase`, which is context-free.
   It does not trim.
2. Each normalized character keeps its source byte range: a folded run's
   whole extent, or the one source character it lowercased from. This makes
   the normalization of a substring cut on character boundaries a
   substring of the whole text's normalization.
3. `fingerprint::hash::rolling` hashes every window of `k` normalized
   characters. The hash is polynomial over scalar values modulo 2^61 − 1,
   with `k` folded in, and finished by the SplitMix64 mixer. It has no
   per-process seed, and the golden vectors pin it.
4. `Winnowing::select` keeps the rightmost minimum of each window of `w`
   hashes, reporting each selected k-gram once. A text with fewer than `w`
   k-grams reports its minimum.
5. `PositionedFingerprint.offset` is the k-gram's source byte offset.
   `KGram` also carries the end, and its position among the normalized
   characters.

### Decoding

- `TextDecoder::decode_mapped` yields `DecodedText { source, text:
  MappedText }`. The text keeps, for every byte, the offset of the source
  character it came from. A decoder yields only when the decoded bytes are
  valid UTF-8 and differ from the source.
  - Base64 and hex decode each maximal run (at least `min_encoded_run`
    characters; base64 standard or URL-safe, padding optional).
  - URL decoding works per whitespace-delimited token that holds a `%XX`
    escape (`+` becomes a space there). A token that decodes to invalid
    UTF-8 is kept as it is.
  - Unicode normalization is per-segment NFKC, folding of common
    Cyrillic and Greek homoglyphs, and zero-width removal.
  - JSON string unescaping covers JSON escapes plus Python and JavaScript's
    `\'` and `\xNN`; surrogate pairs are joined.
  - YAML string unescaping covers `''`, escaped line breaks and `\ `.
- `Step` is `Codec(Codec)`, `JsonString` or `YamlString`. `Step::codec`
  maps it to the spec `Codec`. Until the spec gains `Codec::JsonString`
  and `Codec::YamlString` (eval PR #58), a match that only needed string
  unescapes is reported as `MatchKind::Normalized` (`scan::kind`). Binding
  the variants is a one-line change in `Step::codec`.
- `DecodePipeline::layers(text)` returns the raw layer, then each decoder
  applied breadth first to every layer, at most `max_depth` steps deep and
  `max_layers` layers in all. A text already produced by a shorter chain is
  dropped. Each layer keeps its chain in decode order, and its map
  composed back to the part text.

### Segmentation

`NovelRunSegmenter::segment_against(output, coverage)`:

1. The coverage holds every k-gram of every layer of every input part (the
   full request history, the new system prompt and the new inputs), with
   its layer and position.
2. Tool-call arguments are canonical JSON, so the output's arguments are
   segmented through their JSON-unescaped view, mapped back to the argument
   bytes.
3. For each text part of the output, copied runs are followed k-gram by
   k-gram. A run continues while the next output k-gram also sits at the
   next position of the same input layer, so the run is one contiguous
   stretch of one input.
4. Runs become `Relayed(Input(message))` spans, cut to be disjoint.
5. Text copied from a server tool's result in the same output gets no
   span. Server tool results are scanned as reads.
6. What is left is trimmed of surrounding whitespace. It becomes an
   `Originated` candidate when it has a k-gram.

### Scanning one delta

`Scanner::scan` reads and decides; it writes nothing. The time every index
call gets is the exchange's `started_at`. It also reads the store's index
watermark.

- **Reads.**
  1. Every text part that carries something is expanded into its layers,
     winnowed and looked up. That covers each `new_inputs` message, the
     `new_system` message, and the output's server tool results.
  2. A hit counts only on a live (`Indexed` or `Propagated`) span that was
     indexed at or before the reader's time and before this scan began
     (index sequence at most the watermark). Hits on the reader's own spans
     are skipped.
  3. For each origin span, one layer wins. A layer whose text holds the
     whole origin text (normalized) beats one that does not; then the one
     covering the most part bytes wins, then the shorter chain. That way
     the kind names the chain that really decodes the text.
  4. `read_at` spans from the first to the last covered byte in the part
     as it arrived, and `matched_bytes` counts the covered bytes.
  5. The carrier depends on the part:

     | Part | Carrier |
     | --- | --- |
     | a tool message's result | `ToolResult(call id)` |
     | a server tool result | `ToolResult(call id)` |
     | a user message | `UserTurn` |
     | a system message, wherever it sits | `SystemPrompt` |

  6. The kind is `Exact` when the read bytes occur verbatim in the origin
     span's text (read from the blob store), `Normalized` otherwise, and
     `Decoded(chain)` through codecs. For `Exact`, `matched_bytes` is
     clamped to the origin span's length.
  7. Semantic hits are kept only at or above the threshold, on live spans
     of other agents that no fingerprint matched in that part.
- **Output.** The output is segmented against the inputs, using a bounded
  per-message k-gram cache (`scan::cache`). Each originated candidate's
  fingerprints are looked up:
  - When there are hits and their spans' bodies are stored, maximal runs
    found in a hit span's text become `Relayed(Span(s))` spans. Without
    bodies, the merged hit extents are used.
  - When `s` is another agent's span, a `ReaderOutput` match covers the
    same bytes.
  - The rest is resolved again.
  - A candidate without hits is `Common` when every fingerprint is above
    the cutoff, else `Originated`.
- **Ids.** Span ids, match ids and envelope ids are ULIDs: the exchange id's
  time plus 80 bits of a BLAKE3 digest of what they describe (`span.rs`).

### The engine

`Provenance<I, S, M, L>` runs over a `FingerprintIndex`, a
`ProvenanceStore`, a `SemanticMatcher` and a `MessageSource` (`BlobMessages`
over any spec `BlobStore`).

**`record_exchange(&Exchange)`.** Records the exchange's start, request
hashes and output, with status `Pending`.

**`process(&ConversationDelta)`** goes by the exchange's status:

| Status | What happens |
| --- | --- |
| not recorded | `EngineError::NotRecorded`, which is transient: the delta is retried |
| `Pending` | see the steps below |
| `Scanned` (a crash after the commit) | redo the index writes from the stored spans, mark indexed, return the stored outcome |
| `Indexed` | return the stored outcome |
| `Failed` | publish nothing |

For a `Pending` exchange:

1. Load the delta's bodies. A missing or undecodable body is `Failed {
   BodyMissing | BodyUndecodable }` and publishes nothing; missing history
   bodies are skipped.
2. Scan.
3. `commit_scan` stores the spans and matches, adds a hit to each origin
   span, and sets the status to `Scanned`.
4. Write the index: postings of the originated spans, then one observation
   per span and per scanned input part.
5. `mark_indexed` advances the originated spans to `Indexed`, assigns
   index sequences, and sets the status to `Indexed`.
6. Return the envelopes, built from the stored records with deterministic
   ids: spans first, then matches, stamped with `started_at`.

Every delivery of a delta returns identical envelopes. The only window
left is a crash between `observe` and `mark_indexed`: a redelivery then
observes again, one extra count per text until retention ends.

**`expire(now)`.**

1. Spans past retention are evicted from the index and the semantic
   matcher, in batches.
2. Only then are they advanced to `Expired`.
3. The index call also ages out old observations, so it runs even when no
   span is due.
4. Request lists of exchanges older than retention are pruned.

### The consumer

`consumer::subscribe(bus, retry)` joins group `provenance` on
`exchange_captured` and `conversation_delta`. `consumer::run(subscription,
engine, bus, clock, settings, stats)` is one task that handles one delivery
at a time:

- `ExchangeCaptured` is recorded, then acked.
- `ConversationDelta` is processed, its envelopes published, then acked.
- A transient failure, a failed publish or a not-yet-recorded exchange is
  nacked. A permanent engine error is acked and logged.
- Every eviction interval (tokio time), it reads the injected clock and
  expires spans.
- `ConsumerStats` counts recorded exchanges, scans, replays, failures,
  retries, publishes and expiries.

## Tables and indexes

Migration `crates/provenance/migrations/0001_provenance.sql`, schema
`provenance`. No message text anywhere.

| Table | Holds | Indexes |
| --- | --- | --- |
| `exchanges` | exchange id, `started_at`, output hash, scan status (`pending`, `scanned`, `indexed`, `failed`) with its time and failure | primary key; `started_at` |
| `exchange_requests` | the request's message hashes (pruned after retention) | primary key |
| `scanned_messages` | (message, exchange, scanned as `input`, `system` or `output`): per-message scan status | primary key (message first); exchange |
| `spans` | span id, agent, exchange, message, part, range, output ordinal, state with relay source, `indexed_at`, first hit, hits, `expired_at`, index sequence | primary key (a span's location by id, ready for `SpanIndex::spans`); (exchange, ordinal); (message, part, start); live spans by `indexed_at` |
| `matches` | match id (= envelope id), reader exchange, ordinal, time, origin span and agent, reader, read message, part and range, carrier and kind as spec JSON, matched bytes | primary key; (read message, part, start): by reader message; (origin, at): by origin span; (reader exchange, ordinal) |
| `postings` | fingerprint, span, offset | primary key; span |
| `observations` | one row per observed text and its time | primary key; `at` |
| `observed` | fingerprint, observation, time | primary key (fingerprint, observation); observation; (fingerprint, at) |

`PgFingerprintIndex` agrees with `crosstalk-memory`'s
`MemoryFingerprintIndex` (the model-based harness runs both). Each write is
one transaction that first deletes observations outside retention. The
boilerplate cutoff is a correlated count, on insert and on lookup.
`PgProvenanceStore` writes each commit in one transaction with row locks,
and changes span states only through `SpanState::advance`.

## Configuration

`ProvenanceConfig` (JSON, unknown fields refused, every field defaulted):

| Field | Default |
| --- | --- |
| `winnow.k`, `winnow.w` | 32, 16: any shared run of 47 normalized characters matches |
| `decode.max_depth` | 3 |
| `decode.max_layers` | 32 |
| `decode.min_encoded_run` | 16 |
| `index.cutoff` | 50 |
| `index.retention_secs` | 30 days |
| `index.shards`, `index.owned` | 1, [0] |
| `eviction_interval_secs` | 3600 |
| `semantic_threshold` | 0.85 |

Every value is checked: `k` at least 4, depth 1 to 8, a non-zero retention,
owned shards that exist.

## Validation against AgentDojo

The run files are read in place, never copied. The ignored test
`tests::agentdojo::agentdojo_gpt4o_pipeline_injections_match` covers all
6775 gpt-4o-2024-05-13 runs:

- 10635 injection slots, of which 9949 were exposed to the agent;
- 9948 of the exposed slots matched (100.0%): 1163 `Exact` and 8785
  `Normalized`. Python-repr `\n` escapes and YAML `''` quotes go through the
  string unescapes, which `Normalized` reports until the spec codecs land.

The single miss, a YAML escaped space (`\ `) after a folded line break, is
fixed. A slot counts as exposed when a crude oracle finds it in the tool
outputs: letters and digits only, backslash escapes dropped.
`agentdojo_injections_match_after_escape_folding` runs the brief's
`slack/user_task_1/important_instructions` slice (5 of 5; `AGENTDOJO_RUNS`
picks another scope). Synthetic regressions in the same shapes run in the
normal suite:

- a Python dict repr;
- a YAML single-quoted scalar with folding;
- a YAML double-quoted scalar with escaped breaks;
- collapsed whitespace;
- verbatim placement.

**Tool-call arguments are cut per string value** (INV-1057,
`provenance.span.tool-arguments-per-value`): when a call's arguments are
JSON, the novel stretches are cut to the string values they cover
(`segment::string_values`, keys and non-string values excluded) before
they become originated spans, so a span's view equals the decoded value
the tool received; a `Write {file_path, content}` yields the page and the
path as two spans. A value with no k-gram (shorter than k) yields none.
Arguments that are not JSON are segmented whole. Relayed runs are not cut.
A string value directly under a locator key (`ProvenanceConfig::locator_keys`,
`LocatorKeys`; default `file_path`, `path`, `notebook_path`, `url`, `uri`,
JSON `locator_keys`) yields no originated span at all (INV-1058,
`provenance.span.locator-arguments-excluded`): it names the resource the
call acts on, not content the tool wrote. A URL inside a content value
still counts. So `Write {file_path, content}` yields one span, the
content.

`MemoryProvenanceStore` also implements the spec's `SpanIndex` over the
spans `commit_scan` wrote: `record` adds nothing, `spans` returns the
originated spans (any state whose origin is `Originated`) as recorded,
leaving out relayed and common spans and unknown ids. `Live`'s evidence
feeder reads through it. `PgProvenanceStore` does not yet.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/lib.rs` | Crate doc, modules | — |
| `src/config.rs` | Typed config | `ProvenanceConfig`, `IndexSettings`, `DecodeLimits`, `winnow_params`, `ConfigError` |
| `src/text/{mod,normalize,mapped}.rs` | Normalization with source ranges; decoded text with byte maps | `normalize`, `NormChar`, `MappedText`, `MappedBuilder`, `trim_range` |
| `src/fingerprint/{mod,hash}.rs` | Winnowing, the stable hash | `Winnowing`, `KGram`, `positioned`, `hash::rolling` |
| `src/decode/{mod,base64,hex,url,unicode,escape}.rs` | Decoders and the pipeline | `Step`, `TextDecoder`, `DecodedText`, `DecodePipeline`, `Layer`, `AnyDecoder`, the six decoders |
| `src/segment/{mod,coverage,view}.rs` | The segmenter, input coverage, part views | `NovelRunSegmenter`, `Coverage`, `message_kgrams`, `runs`, `text_parts`, `view`, `PartKind` |
| `src/scan/{mod,reads,output,hits,kind,cache,messages}.rs` | The scanner | `Scanner`, `Loaded`, `ScanEnv`, `IndexWork`, `ScanError`, `LiveSpans`, `match_kind`, `KGramCache`, `MessageSource`, `BlobMessages`, `MemoryMessages` |
| `src/engine.rs` | Processing, replay, eviction | `Provenance`, `Processed`, `EngineError`, `envelopes`, `exchange_record` |
| `src/consumer.rs` | The bus consumer | `GROUP`, `SUBJECTS`, `subscribe`, `run`, `ConsumerSettings`, `ConsumerStats` |
| `src/span.rs` | Deterministic ids | `span_id`, `span_event_id`, `match_id` |
| `src/store/{mod,memory,pg}.rs` | L4's records | `ProvenanceStore`, `MemoryProvenanceStore`, `PgProvenanceStore`, `ExchangeRecord`, `ScanStatus`, `ScanFailure`, `SpanRecord`, `StoredMatch`, `ScanCommit`, `MessageScan`, `MIGRATIONS`, `migrate` |
| `src/index/{mod,pg}.rs` | The Postgres fingerprint index | `PgFingerprintIndex` |
| `src/semantic.rs` | The semantic stub | `DisabledSemanticMatcher` |
| `src/pg.rs` | Shared Postgres conversions | — |
| `migrations/0001_provenance.sql` | The schema | — |
| `src/tests/` | Unit tests, scenarios, fixtures, AgentDojo | evidence `crosstalk_provenance::tests::*` |
| `src/props/` | Property tests and the scenario generator | evidence `crosstalk_provenance::props::*` |
| `src/dst.rs` | Simulations of the consumer | evidence `crosstalk_provenance::dst::*` |
| `src/integration/` | Postgres tests (gated on `TEST_DATABASE_URL`) | evidence `crosstalk_provenance::integration::*` |

## Invariants and constraints

- Evidence implemented and reviewed: INV-191 to 204, INV-206 to 208,
  INV-211 to 213, INV-215 to 223, INV-225, INV-226, INV-229 to 235, and
  INV-695.
- Not reviewed:
  - `fuzz` for INV-192 and INV-229: no coverage-guided harness exists;
  - `integration` for INV-206 and INV-225: they name a semantic store that
    awaits P6.2;
  - the lint for INV-227.
- New invariants:
  - `provenance.decode.utf8-lossless`;
  - `provenance.scan.status-terminal`.
- The index never holds text. Spans enter it only as `OriginatedSpan`.
- Every location L4 records indexes `Message::part_text` on character
  boundaries. Decoded reads map back to the bytes as they arrived.
- Every time is an argument: the exchange's start for scans, the injected
  clock for eviction.
- Span states change only through `SpanState::advance`.
- No `unwrap` or `expect` outside tests, except one commented infallible
  default.

## Gaps and decisions

- **Increments.** The segmenter's inputs are the exchange's `request` (the
  full history for `FullHistory`; for a WebSocket increment, only the
  increment). The spec publishes no resolved history to L4.
- **Assistant parts in `new_inputs`.** An assistant message's text or tool
  call in `new_inputs` carries nothing (no carrier fits); a server tool
  result there is a `ToolResult`.
- **Server tool results in the output.** They are reads with carrier
  `ToolResult`. INV-212 also reads as `ReaderOutput` for any output part.
- **Observations.** The engine observes each scanned text's winnowed
  fingerprints, not all its k-grams. Windows lying inside a text select the
  same k-grams in any text containing it, so boilerplate spans still read
  as frequent.
