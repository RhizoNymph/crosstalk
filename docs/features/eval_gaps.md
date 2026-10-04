# Detection rules from dataset evaluation

Spec changes that close gaps found while evaluating detection on real agent
datasets (SALT, AgentDojo, SWE trajectory corpora, profiling of Anthropic,
OpenAI and Gemini traffic). They were agreed with the implementation track
(`integration/impl`) and are implemented there: this page is the contract,
and its invariants (INV-950..963) are the test plan. It also carries over
the content of the earlier invariants PR (escape-folded matching, opaque
material, rejected writes, the shared upstream question), renumbered into
this range.

## Scope

- **Unknown tool outcome.** `ToolOutcome::Unknown`, for protocols whose tool
  results carry no failure flag (OpenAI Chat).
- **Write outcomes.** `AccessOp::Write { call, spans, outcome: WriteOutcome }`
  with `WriteOutcome { Delivered, Rejected, Unknown }`; `ExtractedAccess`
  carries an `ExtractedOp` (`Write(WriteOutcome)` or `Read`). Rejected
  writes are recorded as accesses and never paired; `Unknown` writes pair at
  lower confidence.
- **Writes whose result never arrives.** A write is held until its result
  arrives or `CorrelationTiming::write_settles_at` passes, then classified
  `Unknown`.
- **Retry after a rejected write.** A write's spans include the writer's own
  earlier spans it relays (`Relayed(RelaySource::Span(s))` with `s` the
  writer's), so a match on `s` links to both writes and the rejected one is
  left out by its outcome.
- **Tool-call signature.** `ToolCall::signature: Option<String>` (Gemini's
  `thoughtSignature`), hashed and kept out of part text like
  `Reasoning::Visible`'s signature (P0.7).
- **Strict UTF-8 decoding** (invariant only): a decoder yields text only
  when the decoded bytes are valid UTF-8.
- **Opaque material is not content**: ids, signatures and opaque reasoning
  stay out of part text, and provenance reads part text only.
- **Escape-folded matching**: `MatchKind::Normalized` also folds one level
  of JSON or YAML string escapes.
- **Shared upstream source** (decided): a `ToolResult` match on a resource
  the sender never wrote confirms nothing; the transmission stays
  `Suspected`.

## Non-scope

- Implementations: the extractors' per-tool content rules, the flow
  consumer's hold, the correlator, the decoders and the fingerprinter are
  `crosstalk-flow` and `crosstalk-provenance` (roadmap P4.2, P5); the
  OpenAI Chat and Gemini normalizers are P8. Every evidence path except the
  spec's own is a future test there.
- A numeric confidence or weighting scheme. The spec has none (see
  "Confidence"), and this change adds none.
- A per-agent-pair minimum of shared spans (a possible later configuration
  knob for the shared upstream rule).
- Nested serialisation (a JSON document inside a JSON string) in matching.
- Text parts that carry a Gemini signature: only tool calls gained one.

## Data and control flow

```text
L1 normalizer
  tool_result is_error / *_error ──▶ ToolOutcome::Error | Success     (Anthropic: always flagged)
  OpenAI Chat tool message ───────▶ ToolOutcome::Unknown              (no flag on the wire)
  functionCall.thoughtSignature ──▶ ToolCall::signature (verbatim; None if empty)
  ids, signatures, opaque reasoning stay out of Text / arguments / result text

L4 provenance (part text only)
  Message::part_text ──▶ Decoder::decode (strict UTF-8) ──▶ Fingerprinter (case, whitespace,
                                                              one level of JSON/YAML escapes)
  retry's arguments relay s ──▶ SpanRelayed { source: Span(s) }

L5 flow consumer
  writing ToolCall ──hold──┬─ result arrives ──▶ extract(call, Some(result))
                           └─ tick ≥ write_settles_at(call time) ──▶ extract(call, None)
  extract ──▶ ExtractedOp::Write(outcome):
                no result ─────────────────▶ Unknown
                ToolOutcome::Error ────────▶ Rejected
                otherwise, known tool ─────▶ its content rule: Delivered | Rejected
                otherwise ─────────────────▶ Success → Delivered, Unknown → Unknown
  record_access(Access { op: Write { call, spans, outcome } }) ──▶ AccessRecorded   (every outcome)
      spans = originated spans in the arguments
            + s for each span relayed from the writer's own span s
  Correlator::on_access(write)
      Rejected ──▶ never paired (CoAccess::new refuses: RejectedWrite)
      Delivered, Unknown ──▶ pairs with later reads (CoAccess) as before
  Correlator::on_match(ToolResult match, reader's call yielded an access on R)
      origin agent has a pairing write on R before the read ──▶ confirm as before
      it has none ──▶ shared upstream: no Confirm / Extend / OpenConfirmed;
                      a transmission it would confirm stays AwaitingContent, then Suspected
  ToolResult match whose call yielded no access ──▶ Direct(ToolResult), unchanged
```

### Tool outcomes (L1)

`ToolOutcome` says what the wire says. Anthropic Messages flags every tool
result: `is_error: true` (or a server tool result whose content type ends
in `_error`) is `Error`, anything else `Success`; the existing mapping in
`crates/canonical/src/anthropic/blocks.rs` is right and stays. An OpenAI
Chat `tool` message has no failure field, so its result is `Unknown`. A
normalizer never reads a refusal out of result text: that is L5's, per
known tool.

### Write outcomes (L5)

A write is the claim that a call's arguments reached a resource. In SALT,
119 of 513 sampled message sends were rejected by the tool (over-length
messages, permission errors); pairing them would open transmissions for
messages never sent, and dropping them (the earlier rule) hid attempted
writes, including attempted exfiltration. So every write is recorded with
its outcome, counted as a write in resource use and access buckets, and
visible to alerting, while only `Delivered` and `Unknown` writes pair.

The outcome is final when the access is recorded. The result of a client
tool call normally arrives in the writer's next request of the
conversation (in the same response for a server tool), and a read by
another agent can be processed before it, so the consumer holds the call,
unpaired and unrecorded, until the result arrives. A write still held when
the settle window closes (`write_settles_at = call time + settle_after`)
is one whose conversation had no later exchange carrying its result (the
agent stopped, or the harness dropped it); it is released as `Unknown`. A
result that arrives later changes nothing. Releasing by `write_settles_at`
keeps L7's watermark bound: a released write's read is after the write,
and the write is after the previous tick minus `settle_after`, so every
confirmation it leads to is no earlier than that.

### Confidence

The only detection-confidence representation in the spec is per access:
`Extraction` (`Scanned` < `Parsed` < `Structured`), "kept but weighted
down in correlation" (`flow.access.extraction-recorded`), with no numeric
weight anywhere in the types. `WriteOutcome` follows the same pattern:
`Unknown` writes are kept and pair exactly as `Delivered` ones do
(`flow.correlator.unknown-write-pairs`), and their lower confidence is the
outcome recorded on the access, for whatever weighs evidence (the
correlator's ranking, L6 triage, the UI) to read. No new weighting scheme
is introduced.

### Retries (contract: self-relay counts)

An agent whose write was rejected usually retries with the same text. The
retry's arguments copy the first attempt's text from the agent's own
context, so the segmenter classifies it `Relayed(RelaySource::Span(s))`,
not `Originated`. `AccessOp::Write::spans` therefore holds, besides the
originated spans, every such `s` whose agent is the writer. A reader's
match on `s` links to both writes; the rejected one is left out by its
outcome and the transmission rests on the retry. Spans relayed from another
agent stay out: the match belongs to its originator.

### Shared upstream source (decided)

In SWE trajectory corpora about 14.5% of an agent's novel shingles reappear
in another task on the same repository because both agents quote the same
repository file, yet only 0.18% of trajectory pairs share 20 or more such
shingles. A writer that reproduces a file it did not read in this
conversation (from a compacted context, or from memory) produces an
`Originated` span, and a reader of the same file gets a `ToolResult` match
from a sender it never heard from.

Rule: a `ToolResult` content match whose reader's call resolved to a
resource, from an origin agent with no write on that resource whose outcome
pairs (made before the read), is evidence of a shared upstream source, not
of a transmission. It confirms nothing; a channel transmission it would
have confirmed (opened by another writer's co-access) stays
`AwaitingContent` and then `Suspected`. A tool result whose call yields no
extracted resource is out of scope and keeps opening `Direct(ToolResult)`
when its window closes (`flow.route.tool-result-without-access`). The
decision is reversible: if evaluation shows it insufficient, a
per-agent-pair minimum of shared spans is a possible later configuration
knob.

### Opaque material, decoding and escapes (L1, L4)

Opaque blobs (redacted thinking, encrypted reasoning, thinking and
tool-call signatures, tool-call ids, Gemini ids that embed a base64
signature after `__thought__`) share structure between agents of one
provider. In text they would be fingerprinted, counted toward frequency and
decoded, manufacturing matches between agents that only share a provider.
`Message::part_text` never reads them, normalizers never fold them into
text, and provenance decodes and fingerprints part text only. Decoding is
strict: a base64, hex or URL-encoded substring whose decoded bytes are not
valid UTF-8 yields nothing, so decoded binary never becomes text.

Text a reader gets through a tool result usually arrives serialised: on
AgentDojo only 9% of injected strings reach tool output byte for byte, 26%
need whitespace folding, and 49% need JSON or YAML string escapes undone.
`MatchKind::Normalized` therefore folds one level of string escapes as
well as whitespace and case, applied alike to both sides before
fingerprinting.

### Carried over from the earlier invariants PR

| Earlier | Now | How |
| --- | --- | --- |
| 806 escape-folded normalization | INV-953 | carried over; the doc comments on `MatchKind::Normalized` and `Fingerprinter::fingerprints` updated as before |
| 807 opaque outside part text | INV-952, INV-694 | narrowed to what P0.7 did not cover (tool-call ids, `Reasoning::Opaque`, tool-call signatures); INV-694 (`canonical.part-text.defined`) now states that ids and signatures are never part text, with a spec test |
| 808 opaque fields inert | INV-954 | folded: with part-text-only input and spans located in part text, inertness follows; stated in INV-954's rationale |
| 809 decoder input is part text | INV-954 | carried over, tool-call signatures added |
| 810 failed write, no access | INV-956, INV-957 | replaced: a rejected write is recorded (957), with its outcome classified (956), never paired (958) |
| 811 co-access never holds a rejected write | INV-958, INV-959 | kept, on `WriteOutcome`: the constructor refuses it (958) and the consumer holds a write until its outcome is final or the settle window closes (959) |
| open question: shared upstream | INV-963 | decided (above) |

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/observed/message.rs` | `ToolOutcome::Unknown`; `ToolCall::signature` | `ToolOutcome`, `ToolCall` |
| `spec/types/observed/message/encoding.rs`, `encoding/mirror.rs` | The encoding of the new field and variant (`"signature"`, `"unknown"`) | `encode`, `decode` |
| `spec/types/observed/message/text.rs` | Part text excludes ids, signatures, opaque reasoning | `Message::part_text` |
| `spec/types/derived/flow/access.rs` | Write outcomes; a write's spans include self-relayed sources | `AccessOp::Write`, `WriteOutcome` (`pairs`) |
| `spec/types/derived/flow/evidence.rs` | A rejected write never pairs | `CoAccess::new`, `InvalidCoAccess::RejectedWrite` |
| `spec/types/derived/flow/timing.rs` | When a write without a result is released | `CorrelationTiming::write_settles_at` |
| `spec/types/derived/flow/transmission.rs` | `Route::Channel` requires a pairing write by the sender | `Route` |
| `spec/types/interfaces/l5_flow.rs` | Extraction with outcomes; module docs on write outcomes, retries and shared upstream | `ExtractedAccess`, `ExtractedOp`, `ResourceExtractor::extract` |
| `spec/types/interfaces/l4_provenance.rs` | Strict, part-text-only decoding; escape folding | `Decoder::decode`, `Fingerprinter::fingerprints` |
| `spec/types/derived/provenance/matching.rs` | Escape folding is `Normalized` | `MatchKind::Normalized` |
| `spec/types/tests/flow.rs` | `co_access_never_holds_a_rejected_write`, `a_held_write_settles_after_the_settle_window` | — |
| `spec/types/tests/part_text.rs` | `ids_signatures_and_opaque_reasoning_are_not_part_text` | — |
| `spec/types/tests/encoding/` | Vectors with a signed tool call and an `Unknown` outcome; generators produce both | — |
| `spec/types/tests/wire/flow/resources.rs`, `golden/flow/write_outcomes.json` | `WriteOutcome` golden; a write without `outcome` is refused | — |
| `crates/canonical/src/anthropic/blocks.rs` | Anthropic tool calls carry no signature; the outcome mapping documented | — |
| `crates/testkit/src/build/flow.rs` | `AccessBuilder::write_outcome` | `AccessBuilder` |
| `crates/memory/src/flow/registry/tests.rs` | `writes_of_every_outcome_are_recorded_and_listed` | — |

## Invariants and constraints

| Id | Statement (short) |
| --- | --- |
| INV-950 `canonical.tool-outcome.unknown-without-flag` | `Error`/`Success` only from a protocol failure marker; `Unknown` without one |
| INV-951 `canonical.tool-call.signature-verbatim` | a tool call's signature verbatim, empty or missing is `None` |
| INV-952 `canonical.opaque.outside-part-text` | no part text holds a tool-call id, opaque reasoning or a tool-call signature |
| INV-953 `provenance.match.escape-folded-normalized` | a span serialised as one JSON or YAML string matches `Exact` or `Normalized` |
| INV-954 `provenance.decode.part-text-input` | decoder and fingerprinter input is part text (or decoded part text) only |
| INV-955 `provenance.decode.strict-utf8` | a decoded substring yields text only when its bytes are valid UTF-8, exactly |
| INV-956 `flow.extract.write-outcome-classified` | each extracted write's outcome from the result's flag, the tool's content rule, or `Unknown` |
| INV-957 `flow.access.rejected-write-recorded` | every extracted write, rejected included, is recorded once with its outcome |
| INV-958 `flow.coaccess.write-not-rejected` | `CoAccess::new` refuses a rejected write |
| INV-959 `flow.correlator.write-held-until-outcome` | a write reaches the correlator only with its final outcome (result, or `Unknown` at `write_settles_at`) |
| INV-960 `flow.correlator.unknown-write-pairs` | `Unknown` pairs as `Delivered` does; `Rejected` pairs never |
| INV-961 `flow.access.write-spans-include-self-relay` | a write's spans include the writer's own relayed source spans |
| INV-962 `flow.correlator.retry-after-rejected-write-confirms` | a retry after a rejected write confirms through the retry only |
| INV-963 `flow.route.shared-upstream-stays-suspected` | a `ToolResult` match from a sender with no pairing write on the resource confirms nothing |

Changed: INV-694 (`canonical.part-text.defined`: never an id or signature;
one more spec test) and INV-255's rationale (a write without a result is
`Unknown`). INV-958's evidence is the spec's and passes; every other new
invariant's evidence is a future test in `crosstalk_canonical`,
`crosstalk_provenance` or `crosstalk_flow`.

Constraints:

- `WriteOutcome::pairs` is the one place that says which writes pair.
- An access is recorded once, with its final outcome; nothing revises it.
- Adding `ToolCall::signature` changed every tool call's encoding
  (`"signature": null`) and so its hash; goldens were re-blessed.
