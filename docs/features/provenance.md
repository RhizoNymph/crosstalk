# Provenance (L4)

`crosstalk-provenance` (`crates/provenance`), roadmap item P4.2. It
implements the spec's L4 interfaces (`crosstalk_spec::interfaces::l4_provenance`):

- it cuts each agent's output into spans and classifies them as
  originated, relayed or common;
- it indexes the originated spans' fingerprints, and, when `forwarding`
  is on (off by default), the forwarded spans' (text relayed from the
  agent's own input, indexed under that agent);
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
- The short-span exact path: whole values of 24 to 46 normalized
  characters matched by an exact hash against a read's token runs.
- Forwarded spans indexed under the forwarding agent, and context k-grams
  that keep an originated remainder next to a forward matchable.
- The stricter rules for `ReaderOutput` matches.
- The reader's nearer source: hits explained by the reader's own earlier
  output, or (forwarding on) by its own direct read of a forward's source,
  are not counted.
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
   `Originated` candidate when it has a k-gram, or at least
   `short_spans.min_chars` (24) normalized characters: a short whole value
   is matched by its exact hash, a short remainder next to a forward by
   its context k-grams (below). Shorter text is never matched.

### Scanning one delta

`Scanner::scan` reads and decides; it writes nothing. The time every index
call gets is the exchange's `started_at`. It also reads the store's index
watermark.

- **Reads.**
  1. Every text part that carries something is expanded into its layers,
     winnowed and looked up. That covers each `new_inputs` message, the
     `new_system` message, and the output's server tool results. Each
     layer is also looked up by the short-span hashes of its normalized
     token runs of 24 to 46 characters (`fingerprint::short`): a run starts
     and ends at a boundary (the text's ends, a space, or a change between
     word and non-word characters), so a short value is never found inside
     a longer word. A hit is told apart from a k-gram at the same offset by
     its fingerprint.
  2. A hit counts only on a live span (`Indexed` or `Propagated`, or a
     forwarded span whose forwarding is `Indexed`) that was indexed at or
     before the reader's time and before this scan began
     (index sequence at most the watermark). Hits on the reader's own spans
     are skipped.
  2b. **The spread rule and skeleton matches** (INV-1094, INV-1150,
     `SpreadRule`). For each hit fingerprint, the originating agents are
     the agents of its live postings' spans and of their copies in other
     outputs (spans relayed from them, `ProvenanceStore::relays`), at any
     time within retention; its holders are those originations and
     copies. A fingerprint held by at least `spread.agents` (4) distinct
     agents is boilerplate for short runs unless it is distinctive: one of
     the whole tokens its k-gram covers in the read (`fingerprint::token`:
     a normalized alphanumeric run of 4 or more characters, or holding a
     digit; a word cut by the window's edge does not count) is observed in
     at most `spread.distinctive_ratio` (2) texts per holder plus one. A
     short secret passed around carries such a token (a key, an id, a
     time); template prose is made of words seen throughout the world. A
     candidate match on an origin span in a layer whose merged hit runs are
     all shorter than `spread.distinctive_chars` (64) normalized
     characters, and that holds a boilerplate hit, is a template skeleton
     filled with other slot words (bench transmission
     01M46CB4DFC573NNYA711QRNC2) and is dropped whole. A match with a
     contiguous run of 64 characters or more is kept whatever the spread.
     **Tradeoff:** a template carrying a token seen nowhere else (a header
     such as "ROUTINE-NOTES-v2"), or whose words occur only inside the
     template, reads as distinctive and is matched like a broadcast; and
     distinctiveness is relative to the world, so in a world with little
     other text most words are rare. Every scanned text (each span, each
     scanned input part) observes its distinct token hashes as one
     observation of its own, at most `spread.tokens_per_text` (512) of
     them, within the index's retention; a capped text undercounts, which
     errs toward keeping matches.
  2c. **Inherited fragments** (`provenance.match.inherited-fragment-dropped`,
     `SpreadRule::inherited`, on by default, `scan::inherited`). A
     candidate match on an originated span whose merged hit runs are all
     shorter than `spread.distinctive_chars`, and that covers less than half
     of the origin span's bytes, is dropped whole when every
     run holds at least two whole tokens and each run's token sequence
     occurs consecutively in one carrying part (tool result, user turn,
     system prompt) of the origin span's own exchange's request
     (`inherited::Given`: the stored request list, bodies through the
     `MessageSource`, each message's per-part token sequences cached by
     hash in a bounded `TokenCache`). The origin added nothing of its own:
     on the bench the orchestrator names a page to its writer ("... team
     wiki, page `queue-backpressure-39`.") and to its readers, the writer
     narrates "I'll update the wiki page `queue-backpressure-39` ...", and
     every reader's prompt shares a k-gram with the narration (17
     `UserTurn` false matches, held by one or two writers, so the spread
     rule never applied). Requiring the tokens in order in one part keeps
     replies composed from words seen apart, and judging only partial
     overlaps keeps deliveries of a whole message the writer pasted from
     its own input (SALT's forwarding labels, split into short runs by
     escapes): an earlier version that accepted tokens from anywhere in the
     request cut SALT recall from 0.765 to 0.672, and one without the
     coverage bound lost about 65 SALT labels. A made-up secret holds a token its writer was not
     given and is kept. **Tradeoff:** a short re-punctuated copy of text
     the writer was given in one part is not matched, as a forwarded copy
     is not with forwarding off. Forwarded spans, runs of 64 characters or
     more, and matches whose origin request is no longer recorded are not
     affected.
  2d. **The reader's nearer source** (`scan::nearer`). After the spread
     rule has read the hits, a hit on another agent's span is not counted
     (its extent is left out of the candidate match) when the reader has
     its own path to that text:
     - **Own output** (`provenance.match.own-output-replay`): the hit is
       read in a tool result, or is on a forwarded span (any carrier), and
       the reader's own earlier output holds its k-gram or short-span run. That
       output is every assistant message in the exchange's request (history
       and new inputs, so written before the read): its text, reasoning and
       tool-call argument parts (server tool results are reads and do not
       count), all k-grams and short-span token runs of every decode layer.
       A tool result that replays the reader's call (SALT `get_log`, reader
       `01KDVDP6XWRHKQWFYAC1XBWMJN`; AgentDojo `send_money`, reader
       `01KDVDP3JH5FRHDFQ2CC1ZXZPG`) is the reader's own relay. Whoever wrote
       the text first does not matter: when the peer's span was the actual
       source, the read that first brought it to the reader came before the
       reader's own copy and was matched then; only later replays are
       withheld. A forward is never its forwarder's own text, so a forward
       of what the reader wrote hands the reader's text back. A user turn or
       system prompt holding a peer's originated span stays a delivery even
       where it repeats the reader: applying the rule there too lost 45 SALT
       labels (16 construction, 29 forwarding) for one false positive.
     - **Direct read of a forward's source**
       (`provenance.match.forward-direct-read`, forwarding on only): the hit
       is on a forwarded span (relayed from the forwarder's input message
       `m`) and a decode layer of one of the reader's non-assistant inputs
       in the request (the read itself, or an earlier read: the reader's
       own path to the original) holds the k-gram and at
       least `w` (16) k-grams of `m` that the forwarder's output message does
       not hold. A delivery of the forward carries only what the forwarder
       wrote; a reader holding the source's text around the forward (the
       tool's header, rows not pasted) read the source itself. The reader's
       inputs are indexed by fingerprint once per scan, on the first hit on
       a forward; `m` and the forwarder's output are read from the blob
       store (no rule when either is gone).
     Both are per hit, so a match keeps the runs only its origin explains
     (per run, not per match). Whether a match is a skeleton (2b) or an
     inherited fragment (2c) is still judged on all its hits: what the
     reader already had does not change what the origin shares with the
     read, and judging the remainder alone turned one bench delivery into
     short runs that the skeleton rule dropped. **Tradeoffs:** a forward holding the whole
     source cannot be told from it (a peer's own read of exactly that
     source still matches); text the reader wrote but whose output is not
     in the request (a WebSocket increment, a truncated history) is not
     covered.
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
  per-message k-gram cache (`scan::cache`). The inputs' coverage (where
  every k-gram of every input occurs) is taken from `CoverageCache` when a
  kept coverage covers a prefix of the inputs (the conversation's previous
  exchange, usually) and extended by the remaining messages; a coverage
  only appends, so the extended one equals one built from scratch. The
  cache holds at most 64 coverages and 8 Mi k-gram positions, least
  recently kept evicted first. `BlobMessages` keeps decoded bodies (128 MiB
  of encoded bytes, least recently used evicted first) with the bytes they
  were decoded from, and answers a read from them only when the stored
  bytes are identical, so the request history is not decoded again on
  every scan. Each originated candidate's fingerprints are looked up:
  - When there are hits and their spans' bodies are stored, maximal runs
    found in a hit span's text become `Relayed(Span(s))` spans. Without
    bodies, the merged hit extents are used.
  - When `s` is another agent's span, a `ReaderOutput` match covers the
    same bytes, if the stretch passes the stricter reader-output rules
    (INV-1093): the stretch is one contiguous run, so it needs at least
    `reader_output.min_chars` (64) normalized characters and a hit on `s`;
    and, with `reader_output.rare_token` on (the default,
    `provenance.match.reader-output-rare-token`), a whole token seen in at
    most `SpreadRule::rare_bound(h)` texts, `h` being `s`'s originations,
    copies and matched reads (`Propagated` hits). Two agents filling one
    sentence template with the same words write 64 characters or more
    alike (24 bench false matches, rarest tokens seen in 7 to 74 texts).
    Otherwise the stretch is still `Relayed(Span(s))` and no match is
    made. Other carriers keep no length floor. A broadcast (one writer,
    many later copies) keeps matching that writer however many copies
    there are: the spread rule never applies, and each copy and read
    raises the bound. **Tradeoff (flagged for decision):** an unobserved
    copy of 64 characters or more made only of words common in the world
    yields no match; `"rare_token": false` restores it.
  - The rest is resolved again.
  - A candidate without hits is `Common` when every fingerprint is above
    the cutoff, else `Originated`. A whole short value's short-span hash
    counts as one of its fingerprints; a candidate with no fingerprint at
    all (a short remainder) is `Originated`.
- **Ids.** Span ids, match ids and envelope ids are ULIDs: the exchange id's
  time plus 80 bits of a BLAKE3 digest of what they describe (`span.rs`).

### Index writes

`Scanner::index_work(loaded, spans)` is a function of the committed spans
and the delta's messages, so a replay after a crash redoes the same
writes.

- **Postings.** Every originated span, and, when `forwarding` is on, every
  forwarded span (`Relayed(Input)`, INV-1090), is inserted under its own
  agent with:
  1. its own winnowed fingerprints (through its view);
  2. its context k-grams (`scan::postings`, INV-1091): the originated and
     forwarded spans of one part (forwarded ones even with forwarding off;
     the k-grams they hold are then posted nowhere) that sit next to each other with only whitespace between
     them form a run; the run's view is winnowed as one text, and each
     selected k-gram goes to the span holding more than half of its
     normalized characters. A reader holding the whole run selects the
     same k-grams, so a remainder too short for its own k-gram is still
     found, and a k-gram mostly over a forward never goes to an
     originated span;
  3. for an originated span that is a whole value (its whole text part,
     trimmed, or one whole string value of a tool call's arguments) of 16
     to 46 normalized characters, its short-span hash (INV-1092): one
     hash of its whole normalized text, domain-separated from k-grams.
  The index drops fingerprints above the cutoff, as for any posting.
- **Observations.** One per span (its own winnowed fingerprints, as
  before), one more per whole short value (its short-span hash alone), one
  per scanned input part (its layers' fingerprints), and one more per
  scanned part whose layer is a whole short value. A short hash's
  frequency is therefore the number of texts that were that whole value:
  a stock phrase many agents send whole becomes `Common`.

**Forwarded spans** (only with `forwarding` on; with it off, the default,
they are the pre-forwarding `Relayed` spans: never indexed, not returned
by `SpanIndex::spans`) keep the state `Relayed { source: Input(m) }`; they
are published as `SpanRelayed`. The store records their indexing beside the
state (`store::Forwarding`: `Pending`, then `Indexed { at }` with an index
sequence, then `Expired`), hits on them count no `Propagated` state, and
expiry evicts them like originated spans. A reader that read the same
upstream source as the forwarder itself does not match the forward on the
text its own read holds (`provenance.match.forward-direct-read`, Reads
step 2c); where that cannot be told (a forward of the whole source), L5
keeps the shared-upstream match from confirming a channel (INV-963).

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
5. `mark_indexed` advances the originated spans to `Indexed` and the
   forwarded spans' forwarding to `Indexed`, assigns index sequences in
   output order, and sets the status to `Indexed`.
6. Return the envelopes, built from the stored records with deterministic
   ids: spans first, then matches, stamped with `started_at`.

Every delivery of a delta returns identical envelopes. The only window
left is a crash between `observe` and `mark_indexed`: a redelivery then
observes again, one extra count per text until retention ends.

**`expire(now)`.**

1. Spans past retention (originated, and forwarded ones by their
   forwarding's time) are evicted from the index and the semantic
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

Migrations `crates/provenance/migrations/0001_provenance.sql` and
`0002_forwarded_spans.sql`, schema `provenance`. No message text anywhere.

| Table | Holds | Indexes |
| --- | --- | --- |
| `exchanges` | exchange id, `started_at`, output hash, scan status (`pending`, `scanned`, `indexed`, `failed`) with its time and failure | primary key; `started_at` |
| `exchange_requests` | the request's message hashes (pruned after retention) | primary key |
| `scanned_messages` | (message, exchange, scanned as `input`, `system` or `output`): per-message scan status | primary key (message first); exchange |
| `spans` | span id, agent, exchange, message, part, range, output ordinal, state with relay source, `indexed_at`, first hit, hits, `expired_at`, index sequence; for a forwarded span `forward_indexed_at` and `forward_expired_at` (0002, checked to sit only on `relayed` rows with an input source) | primary key (a span's location by id, ready for `SpanIndex::spans`); (exchange, ordinal); (message, part, start); live spans by `indexed_at` |
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
| `short_spans.min_chars`, `short_spans.max_chars` | 24, 46: whole values of this many normalized characters take the short-span exact path; `min_chars` is also the floor for originated text without a k-gram |
| `reader_output.min_chars` | 64 normalized characters |
| `reader_output.rare_token` | true: a `ReaderOutput` stretch must hold a token rare relative to its source's originations, copies and reads (`RareToken`) |
| `spread.drop_inherited` | true: a short match whose runs each repeat, token for token, one part its origin was given in its own request is dropped (`InheritedFragments`) |
| `spread.agents`, `spread.distinctive_chars`, `spread.distinctive_ratio`, `spread.tokens_per_text` | 4, 64, 2, 512: a fragment four agents originated or copied, at any time, with no token seen in at most 2 texts per holder plus one, is boilerplate for matches whose runs are all under 64 characters; such a match is dropped whole; each text observes at most 512 tokens |
| `forwarding` | false: forwarded spans are not indexed (INV-1090) |

Every value is checked: `k` at least 4, depth 1 to 8, a non-zero retention,
owned shards that exist, `4 <= short_spans.min_chars <= max_chars`.

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
originated spans (any state whose origin is `Originated`) and the
forwarded spans (`Relayed` from an input) as recorded, leaving out spans
relayed from another span, common spans and unknown ids. `Live`'s evidence
feeder reads through it. `PgProvenanceStore` does not yet.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `src/lib.rs` | Crate doc, modules | — |
| `src/config.rs` | Typed config | `ProvenanceConfig`, `IndexSettings`, `DecodeLimits`, `ShortSpans`, `ReaderOutputRules`, `RareToken`, `SpreadRule`, `InheritedFragments`, `winnow_params`, `ConfigError` |
| `src/text/{mod,normalize,mapped}.rs` | Normalization with source ranges; decoded text with byte maps | `normalize`, `trimmed_len`, `NormChar`, `MappedText`, `MappedBuilder`, `trim_range` |
| `src/fingerprint/{mod,hash}.rs` | Winnowing, the stable hash, prefix window hashes | `Winnowing`, `KGram`, `positioned`, `hash::rolling`, `hash::Prefix`, `hash::short`, `hash::token` |
| `src/fingerprint/token.rs` | Tokens for world-wide rarity: what a text observes, the whole tokens in a window | `observed`, `whole_tokens_in`, `MIN_TOKEN_CHARS` |
| `src/fingerprint/short.rs` | The short-span exact path: a whole value's hash, a read's token runs | `whole`, `token_runs` |
| `src/decode/{mod,base64,hex,url,unicode,escape}.rs` | Decoders and the pipeline | `Step`, `TextDecoder`, `DecodedText`, `DecodePipeline`, `Layer`, `AnyDecoder`, the six decoders |
| `src/segment/{mod,coverage,view}.rs` | The segmenter, input coverage, part views | `NovelRunSegmenter`, `Coverage`, `message_kgrams`, `runs`, `text_parts`, `view`, `PartKind` |
| `src/scan/{mod,reads,output,hits,kind,cache,messages}.rs` | The scanner | `Scanner`, `Loaded`, `ScanEnv`, `IndexWork`, `ScanError`, `LiveSpans`, `match_kind`, `KGramCache`, `TokenCache` (`cache::Bounded`), `CoverageCache`, `MessageSource`, `BlobMessages` (`new`, `with_budget`), `MemoryMessages` |
| `src/scan/nearer.rs` | The reader's nearer source: own output and direct reads of a forward's source | `Nearer`, `OwnCache` (`cache::Bounded`), `Scanner::nearer_hits` (crate) |
| `src/scan/postings.rs` | What a span is posted under beyond its own fingerprints: context k-grams, short-span hashes | `Scanner::context_kgrams`, `Scanner::short_fingerprint` (crate) |
| `src/engine.rs` | Processing, replay, eviction | `Provenance`, `Processed`, `EngineError`, `envelopes`, `exchange_record` |
| `src/consumer.rs` | The bus consumer | `GROUP`, `SUBJECTS`, `subscribe`, `run`, `ConsumerSettings`, `ConsumerStats` |
| `src/span.rs` | Deterministic ids | `span_id`, `span_event_id`, `match_id` |
| `src/store/{mod,memory,pg}.rs` | L4's records | `ProvenanceStore`, `MemoryProvenanceStore`, `PgProvenanceStore`, `ExchangeRecord`, `ScanStatus`, `ScanFailure`, `SpanRecord` (`committed`, `indexed_at`), `Forwarding`, `Relay`, `StoredMatch`, `ScanCommit`, `MessageScan`, `MIGRATIONS`, `migrate` |
| `src/index/{mod,pg}.rs` | The Postgres fingerprint index | `PgFingerprintIndex` |
| `src/semantic.rs` | The semantic stub | `DisabledSemanticMatcher` |
| `src/pg.rs` | Shared Postgres conversions | — |
| `migrations/0001_provenance.sql` | The schema | — |
| `migrations/0002_forwarded_spans.sql` | A forwarded span's indexing columns; spans by relay source (the spread rule's copies) | — |
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
- New invariants (also INV-1094 `provenance.match.cross-agent-spread`,
  INV-1150 `provenance.match.skeleton-dropped`, and, numbers pending,
  `provenance.match.inherited-fragment-dropped` and
  `provenance.match.reader-output-rare-token`; INV-1093 and INV-1094
  restated to point at them):
  - `provenance.decode.utf8-lossless`;
  - `provenance.scan.status-terminal`;
  - INV-1090 `provenance.index.forwarded-indexed`;
  - INV-1091 `provenance.index.remainder-around-relay-matchable`;
  - INV-1092 `provenance.match.short-span-exact`;
  - INV-1093 `provenance.match.reader-output-strict`;
  - `provenance.match.own-output-replay` and
    `provenance.match.forward-direct-read` (INV-X, numbers pending).
- Restated for the nearer-source rules: INV-1090 (a peer's own read of a
  forward's source, the forwarding default and its measurements).
- Restated for forwarded spans: INV-205 and INV-224 (what the index and
  the semantic matcher accept), INV-218 (the reader-output rules), INV-1057
  (the short-span floor).
- The index never holds text. Spans enter it only as `OriginatedSpan`:
  originated or forwarded (`OriginatedSpan::new` accepts `Relayed` from
  an input, never from a span).
- Relayed text is never re-originated: a context k-gram is posted under a
  span only when that span holds more than half of its characters.
- Every location L4 records indexes `Message::part_text` on character
  boundaries. Decoded reads map back to the bytes as they arrived.
- Every time is an argument: the exchange's start for scans, the injected
  clock for eviction.
- Span states change only through `SpanState::advance`.
- No `unwrap` or `expect` outside tests, except one commented infallible
  default.

## Gaps and decisions

- **Forwarding is off by default.** Before the nearer-source rules, on
  SALT (`--limit 53`, live) turning it on raised recall from 0.854 to
  0.920 but dropped precision from 0.840 to 0.143 (46,275 `ToolResult`
  false positives): every peer's own read of a schema or log another agent
  forwarded matched the forward. With them (below), forwarding on keeps
  0.894 recall at 0.882 precision, but the SALT forwarding-row gate (0.94)
  fails at 0.885 and splice keeps 77.0 false positives per 1k exchanges
  (3.6 with it off), mostly `UserTurn` reads of text forwarded from a
  shared task prompt. It stays off by default. L5's `write_spans` still
  carries only originated spans and self-relays, so a forward through a
  file write carries no forwarded span (a forward through a message tool's
  result is matched directly); whether writes should carry forwards is
  L5's call.
- **The reader's nearer source, measured** (release, live, seed 0,
  2026-10-05; before = integration/impl 2adbed4, after = this rule set
  merged onto it; recall / precision are `overall`; FP / 1k counts every
  false positive):

  | dataset | forwarding | before: recall / precision, FP / 1k | after: recall / precision, FP / 1k | `ToolResult` FPs before → after |
  | --- | --- | --- | --- | --- |
  | SALT `--limit 53` | off | 0.854 / 0.840, 56.1 | 0.854 / 0.955, 13.9 | 558 → 62 |
  | SALT `--limit 53` | on | 0.920 / 0.143, 4179.1 | 0.894 / 0.882, 72.1 | 46,275 → 478 |
  | AgentDojo (documented selection) | off and on | 1.000 / 0.961, 13.2 | 0.996 / 0.975, 8.2 | 148 → 92 |
  | wiki `--max-agents 100` | off | 0.950 / 1.000 | 0.950 / 1.000 | 0 → 0 |
  | wiki `--max-agents 100` | on | 0.970 / 1.000 | 0.960 / 1.000 | 0 → 0 |
  | swarm-traces | off and on | 1.000 / 1.000 | 1.000 / 1.000 | 0 → 0 |
  | splice `--count 80` | off | 1.000 / 0.875, 7.8 | 1.000 / 0.939, 3.6 | 58 → 8 |
  | splice `--count 80` | on | 1.000 / 0.162, 716.5 | 1.000 / 0.621, 77.0 | 7,764 → 293 |

  SALT's forwarding row (forwarding on) goes from 0.967 to 0.885: a
  delivery of a forward whose content the reader had already read itself
  (the shared `inspect_database` schema in a pasted raw log) is not
  matched on that content. The 9 AgentDojo labels lost are all tool
  results echoing the victim's own call, into whose arguments it had
  pasted the injection (`send_email` bodies, `get_users_in_channel`
  channel names): the rule reads them as the victim's own relay, the
  converter as a delivery. Bench replays: headline 1.000 / 1.000 and
  boilerplate 0.893 (133 correct, 16 false), both unchanged.
- **wiki `--demo` "decoded" misses were not a decode-path bug.** At
  b0bd046 the live detector missed 63 `Decoded([JsonString])` wiki labels
  (`dse/BridgeLAProd1782007689`). Every one was a reread: the reader had
  read the same revision's lines on that page at an earlier edit, and
  that first read was found. L5 folds a reread into the first delivery
  (INV-1122). The labels are `Decoded([JsonString])` only because a run of
  several lines holds a newline, which the writer's `POST` arguments
  escape; the tool-argument view already undoes that. The eval now labels
  rereads as controls (eval.md, collusion-wiki labels). No L4 change.
- **Bench run 20261005T184633Z** (boilerplate scenario, run live on
  staging 02103e9 = c3cd7f2): 41 false matches, 17 `UserTurn` and 24
  `ReaderOutput`. `ct-eval replay` of the same run:

  | build | precision | correct / false | `UserTurn` false | `ReaderOutput` false | headline replay (20261005T184212Z) |
  | --- | ---: | ---: | ---: | ---: | --- |
  | c3cd7f2 (live) | 0.763 | 132 / 41 | 17 | 24 | - |
  | a0f2f3a (skeleton, rarity) | 0.704 | 133 / 56 | 17 | 39 | 1.000 / 1.000 |
  | this branch | 0.893 | 133 / 16 | 0 | 16 | 1.000 / 1.000 |

  - The skeleton and rarity rules removed none of the 41: the 17
    `UserTurn` matches are one k-gram held by one or two writers, under
    the four agents the spread rule needs; the `ReaderOutput` path never
    passes through the spread rule. They added 15 `ReaderOutput` matches:
    c3cd7f2 left spread-boilerplate fingerprints (four agents within 60 s)
    out of a stretch's supporting hits, and withheld a stretch with no
    other support; a0f2f3a dropped that filter so that long broadcasts
    keep matching, which re-admitted stretches supported only by template
    pieces many agents write.
  - The inherited-fragment rule removes the 17; the rare-token requirement
    removes 23 of the 39 `ReaderOutput` matches.
  - **The 16 left** are single generated sentences of 64 to 98 characters
    ("I recommend a short spike on deletion proof before committing to data
    retention (track 3).") that two agents filled alike. In a 253-exchange
    world a topic's terms are seen in only 2 to 16 texts, and a source page
    read several times has a raised bound, so no token-based rule separates
    them from a copy. Raising `reader_output.min_chars` to 100 would remove
    all 16; no evaluated dataset labels a `ReaderOutput` transmission, so
    its measured recall cost is nil, but unobserved copies of 64 to 99
    characters would go unmatched. Not done: flagged for decision.
- **Live evaluation of these rules** (integration b2bd28f against this
  branch, `--detector live`, forwarding off, 2026-10-05):

  | dataset | b2bd28f recall / precision | this branch | `ReaderOutput` false | other |
  | --- | --- | --- | --- | --- |
  | SALT `--limit 53` | 0.856 / 0.810 | 0.854 / 0.840 | 243 to 84 | forwarding row 456 to 453 of 941; 6 construction labels lost; FP/1k 70.6 to 56.1 |
  | wiki `--max-agents 100` | 0.950 / 1.000 | 0.950 / 1.000 | 4 unjudged, unchanged | |
  | swarm-traces | 1.000 / 1.000 | 1.000 / 1.000 | - | |
  | splice `--count 80` | 1.000 / 0.871 | 1.000 / 0.875 | 38 to 34 | the 58 structural tool-result matches ("pkg/credentials/file_aws_config.") unchanged |
  | open-swe `--count 16` | FP/1k 21.5 | FP/1k 18.8 | 126 to 94 | |
  | lmcache `--count 16` | FP/1k 128.5 | FP/1k 98.3 | 120 to 51 | the 203 user-turn Django setup matches: 196 left |

- **Short spans** take only whole values. A short originated piece that is
  not a whole value (a remainder) is matched only through context
  k-grams; one that sits next to no posted span has no posting.
- **Reader-output rules** remove the one `ReaderOutput` match the
  `live_gateway` splice fixture's confirmed transmission rested on (a
  42-character `cd /workspace/... && python` command both agents ran). The
  splice transmission itself needs a day's correlation window (eval
  finding 9); `splice_worlds_are_confirmed_and_scored` and the splice run
  of `two_live_runs_give_byte_identical_reports` fail under the 60 s
  window. They are expected to pass once fix/l5-content-age (content-
  confirmed channel transmissions not bounded by the correlation window)
  is merged; the tests stay unchanged.

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
