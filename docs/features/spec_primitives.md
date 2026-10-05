# Spec primitives: the message encoding, canonical JSON, secrets and ids

The computations several layers must perform identically, kept in the spec
because layer crates cannot depend on each other: the canonical encoding of
a message body, its hash and its strict decoder; the exact-number canonical
JSON inside it; the keyed hasher behind credential and account digests and
the deployment secret only it reads; and the ULID generator every minting
layer uses. Roadmap item P0.7. They were written in `crosstalk-canonical`
for the Anthropic normalizer (P2.5) and moved here, unchanged in behaviour,
so that L3, L4 and L8 can decode stored bodies and L0 can hash credentials.

## Scope

- `observed::message::encoding`: `encode` (a body's canonical bytes),
  `decode` (exactly the inverse, on canonical bytes only), `hash` and
  `hash_bytes` (BLAKE3), `message` (a body with its hash), `DecodeError`.
- `observed::message::json`: JSON with exact numbers (`Json`, `Number`), a
  strict parser (`JsonError`), and `CanonicalJson`'s text (`canonicalize`).
- The serde form of message bodies (the encoding's shape), `Message`
  (decoded only under its body's hash), and `MediaBlob` (media bytes and
  their hash, checked).
- `NormalizedExchange`'s media and serde, and its `check`.
- `ids::secret`: `DeploymentSecret`, `KeyedHasher`, `SecretDigests`.
- `ids::mint`: `UlidGenerator`, `RandomSource`, `SeededRandom`, and the
  `EntityId` trait it mints through.
- The spec gaps the normalizer found: `TokenUsage::cache_write`, and a
  signature on `Reasoning::Visible`. Later, from dataset evaluation
  ([eval_gaps](eval_gaps.md)): a signature on `ToolCall` (Gemini's
  `thoughtSignature`) under the same rule, and `ToolOutcome::Unknown` for
  protocols without a failure flag.

## Non-scope

- Normalizers (`crosstalk-canonical`, [canonical](canonical.md)): they
  call these modules.
- Loading secrets from configuration, cross-node agreement on the loaded
  versions (`canonical.ids.secret-versions-agree-across-nodes`, evidence
  in L0), and the lints that confine `from_keyed_digest` and the secret's
  bytes (`lint:` evidence, not written yet).
- The gateway's wiring of a `UlidGenerator` per component or node.
- Fuzzing the decoder.

## Data and control flow

```text
L1 normalizer                              blob store                     L3, L4, L8
  MessageBody ──encoding::encode──▶ bytes ──put──▶ key = BLAKE3(bytes) ──get──▶ bytes
              ──encoding::hash────▶ MessageHash (= that key)                    │
                                                                encoding::decode ▼
                                                                         MessageBody
  media bytes ──MediaBlob::new──▶ { hash: BLAKE3(bytes), bytes } ──put──▶ key = hash

encode(body):  mirror::Body::from(body) ─serde_json─▶ text ─json::canonicalize─▶ canonical bytes
decode(bytes): json::Json::parse_bytes ─▶ canonical text == bytes? (else NotCanonical)
               ─serde_json─▶ mirror::Body (else Shape) ─TryFrom─▶ MessageBody (EmptyTool, NonCanonicalJson)
               ─encode(body) == bytes? (else NotCanonical) ─▶ Ok(body)

L0 proxy: KeyedHasher::credential(raw, started_at) ─▶ SecretDigests { current, previous }
            current  ─▶ ClientContext.credential.hash
            previous ─▶ ClientContext.previous_digests (inside a rotation overlap only)

any minting layer: UlidGenerator::new(Arc<dyn Clock>, RandomSource).mint::<AgentId>()
L0 proxy:          shared Mutex<UlidGenerator>.mint_at::<ExchangeId>(started_at)
```

### The encoding

A body's encoding is the canonical JSON text (members sorted by UTF-16
code units, no whitespace, RFC 8785 escapes, exact numbers) of its JSON
shape, which follows the wire contract's conventions and is also
`MessageBody`'s serde form:

| Body or part | JSON |
| --- | --- |
| `MessageBody` | `{"type": "system" \| "user" \| "assistant" \| "tool", "data": [parts]}` (a tool body's items are its results) |
| `Text` | `{"type": "text", "data": "<text>"}` |
| `Reasoning::Visible` | `{"type": "reasoning", "data": {"type": "visible", "data": {"signature": "<signature>" \| null, "text": "<text>"}}}` |
| `Reasoning::Opaque` | `{"type": "reasoning", "data": {"type": "opaque", "data": {"signature": ".."}}}` |
| `Media` | `{"type": "media", "data": {"blob": "<hex>", "kind": "image" \| "audio" \| "document"}}` |
| `Unknown` | `{"type": "unknown", "data": {"kind": "..", "raw": "<canonical JSON text>"}}` |
| `ToolCall` | `{"type": "tool_call", "data": {"arguments": {"type": "json" \| "invalid", "data": ".."}, "execution": "client" \| "server", "id": "..", "name": "..", "signature": "<signature>"}}`, `signature` omitted when absent |
| `ToolResult` | `{"call_id": "..", "content": [{"type": "text" \| "media" \| "unknown", "data": ..}], "outcome": "success" \| "error" \| "unknown"}` |

Canonical JSON inside a body travels as a string, so its exact numbers
survive. The pinned vectors are `spec/types/tests/golden/encoding/vectors.json`.

`decode` accepts a byte string exactly when it is `encode`'s output for the
body it returns (`canonical.encoding.decode-inverts-encode`): it refuses
non-canonical text (whitespace, member order, escapes, duplicate members),
shapes no body has, a tool body without results, canonical JSON inside that
is not canonical, and, by re-encoding at the end, canonical text that
`encode` would still not write (an optional field left out, which serde
reads as `None`).

### Canonical JSON

`Json` keeps numbers as exact decimals (`Number`: `±digits × 10^exponent`,
no leading or trailing zero), so `1`, `1.0` and `10e-1` are one value and
an id beyond 2^53 keeps every digit. The text is RFC 8785's: members sorted
by UTF-16 code units, `JSON.stringify` escapes, no whitespace; numbers in
ECMAScript's `Number::toString` layout applied to the exact decimal (plain
up to 21 integer digits, `0.000…` down to 10^-7, exponential beyond:
`1e+30`, `1.5e-7`). The parser is strict (RFC 8259, no unpaired surrogate
escapes, at most 256 levels, exponents of at most 30 digits), and a
repeated member keeps its last value as `JSON.parse` does. `serde_json` is
not used for this because exact numbers would need its
`arbitrary_precision` feature, which changes number handling for every
crate in the workspace.

### Secrets

A `DeploymentSecret` is one version's 32-byte BLAKE3 key, built from bytes
or from 64 hex digits (`from_hex`, which ignores surrounding ASCII
whitespace such as an environment variable's trailing newline;
`InvalidSecret` names positions and lengths within the trimmed text,
never the text). It has no key accessor, and implements no
`Serialize`, `Deserialize`, `Clone` or `PartialEq`; `Debug` and `Display`
print the version alone. A `KeyedHasher` holds the current secret and, in
a rotation, the previous one with the instant its overlap ends
(`rotating`, which refuses a previous version not older than the current,
`InvalidRotation`). `credential(raw, at)` and `account(raw, at)` return the
keyed digest under the current version and, while `at` is before the
overlap's end, under the previous one (`SecretDigests`, current first).
The hasher is pure: the caller passes `at`, the time of what it hashes
for (ingress passes the exchange's `started_at`, which the
`ClientIdentifier` derivations take as an argument).

### Minting

`UlidGenerator<R: RandomSource>` reads the time from an `Arc<dyn Clock>`
and the 80 random bits from `R` (two draws). A reading in a later
millisecond than the last id draws fresh randomness; a reading in the
same or an earlier millisecond mints the last id plus one, carrying into
the next millisecond when the random part is full, so a generator's ids
strictly increase whatever its clock does
(`canonical.ids.ulid-monotonic`). Times past 2^48 ms read as the largest;
past the largest ULID, `next_ulid` is `Err(UlidExhausted)`. `mint::<I>()`
returns any `EntityId`. `next_at(at)` and `mint_at::<I>(at)` stamp a time
the caller passes instead of the clock's reading (an exchange id carries
the exchange's start) under the same rule and the same last id: a time in
or before the last id's millisecond mints the last id plus one. `SeededRandom` is SplitMix64: `new(seed)` under
simulation (and `crosstalk_sim::SimRng` is itself a `RandomSource`),
`from_entropy()` in a running gateway, seeded through the standard
library's per-process random hasher keys, which it draws from the
operating system.

## Decisions

- **The thinking signature is hashed.** `Reasoning::Visible { text,
  signature: Option<String> }`. Leaving it out of the encoding would break
  round trips through the blob store (the stored bytes are the hashed
  bytes), so it is in the encoding, and echoes still hash like their
  responses because the provider refuses a thinking block whose signature
  changed: harnesses echo it byte for byte. An empty or missing signature
  is `None`. Shown by `crosstalk_canonical::tests::thinking_signatures_echo_like_their_responses`
  and the echo property, whose generated echoes carry the signature.
- **The tool-call signature is hashed too.** `ToolCall { .., signature:
  Option<String> }` holds Gemini's `thoughtSignature` on a function call
  verbatim (`canonical.tool-call.signature-verbatim`), by the same rule
  and for the same reason as the thinking signature: in the encoding (so
  echoes, which carry it byte for byte, hash like their responses), an
  empty or missing one `None`, and never part text. Anthropic and OpenAI
  calls carry none. An absent signature is omitted from the encoding, not
  written as `null`, so adding the field changed no existing tool call's
  bytes or hash; the canonical decoder refuses an explicit
  `"signature": null`, because `encode` never writes it.
- **Opaque material is never part text.** Tool-call ids, reasoning and
  tool-call signatures and `Reasoning::Opaque` payloads are in the
  encoding but never in `Message::part_text`
  (`canonical.part-text.defined`), normalizers never fold them into text
  (`canonical.opaque.outside-part-text`), and provenance decodes and
  fingerprints part text only (`provenance.decode.part-text-input`).
- **Media bytes ride in `NormalizedExchange`.** It gained `media:
  Vec<MediaBlob>` (ascending hash order, exactly the blobs its `Media`
  parts name), so `Normalizer::normalize` returns everything L1 stores and
  the canonical crate's `Normalization` wrapper is gone.
- **`NormalizedExchange` has serde** in the wire conventions, decoded
  through `check`, for goldens; it still crosses only in process.
- **Cache writes.** `TokenUsage` is checked, with `cache_write:
  Option<u32>`. `input` keeps its meaning (every prompt token); the cache
  counts are parts of it. Anthropic maps `cache_write` to
  `Some(cache_creation_input_tokens)`.
- **One previous secret version.** A client context carries one set of
  previous digests, so a rotation overlaps exactly two versions.
- **Entropy without a dependency.** The spec takes no RNG crate:
  `from_entropy` uses the standard library's OS-seeded hasher keys, which
  is enough for an 80-bit random part that only needs to differ across
  nodes, not to be secret.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `spec/types/observed/message/encoding.rs` | The encoding, its hash, the strict decoder | `encode`, `decode`, `hash`, `hash_bytes`, `message`, `DecodeError` |
| `spec/types/observed/message/encoding/mirror.rs` | The body's JSON shape (private serde mirrors), `MessageBody`'s serde | — |
| `spec/types/observed/message/json.rs` | JSON values with exact numbers; canonical text | `Json` (`parse`, `parse_bytes`, `get`, `kind`, `with`, `canonical`), `canonicalize`, `JsonError`, `MAX_DEPTH` |
| `spec/types/observed/message/json/number.rs` | Exact decimals and their canonical spelling | `Number` (`as_u64`, `canonical`), `MAX_EXPONENT_DIGITS` |
| `spec/types/observed/message/json/parse.rs` | The strict parser | `JsonError` |
| `spec/types/observed/message/json/write.rs` | The canonical writer (UTF-16 member order, escapes) | — |
| `spec/types/observed/message.rs` | `Message` (`new`, decoded under its hash), `Reasoning`, `ToolCall` (its `signature`), `ToolOutcome` (with `Unknown`), `Media`, `MediaBlob` | `MessageHashMismatch`, `InvalidMediaBlob` |
| `spec/types/observed/exchange.rs` | `TokenUsage` (checked) | `TokenCounts`, `InvalidTokenUsage` |
| `spec/types/interfaces/l1_canonical.rs` | `NormalizedExchange` with media, serde and `check` | `InvalidNormalizedExchange` |
| `spec/types/ids/secret.rs` | The deployment secret and the keyed hasher | `DeploymentSecret`, `KeyedHasher`, `SecretDigests`, `InvalidSecret`, `InvalidRotation`, `SECRET_LEN` |
| `spec/types/ids/mint.rs` | The ULID generator and its random source | `UlidGenerator`, `RandomSource`, `SeededRandom`, `UlidExhausted`, `ulid_millis`, `MAX_ULID_MILLIS` |
| `spec/types/ids.rs` | `EntityId`, re-exports of the above | `EntityId` |
| `spec/types/support.rs` | `Blake3::of`, hex of raw bytes | `hex`, `from_hex` |
| `spec/types/wire/confidential.rs` | Compile-time checks that secrets never serialize, clone or compare | — |
| `spec/types/tests/encoding/` | Vectors, round trips, the exact-inverse decode (unit and edit-based property), RFC 8785 and exact-number vectors, formatting independence | — |
| `spec/types/tests/secrets.rs`, `tests/minting.rs` | Keyed digests (BLAKE3's keyed vector), overlaps, formatting; ULID monotonicity and uniqueness | — |
| `spec/types/tests/wire/observed/normalized.rs` | `NormalizedExchange` goldens and decode refusals | — |
| `spec/types/tests/golden/encoding/vectors.json` | The pinned encodings | — |
| `crates/sim/src/tests/ids.rs` | Concurrent generators on skewed, stepping node clocks under simulation | — |

## Invariants and constraints

- `encode` is deterministic and platform-independent; the vectors pin it
  (`canonical.encoding.golden-vectors`); `decode` inverts it and accepts
  nothing else (`canonical.encoding.round-trips`,
  `canonical.encoding.decode-inverts-encode`).
- Canonical JSON keeps exact numbers and is RFC 8785 but for number
  formatting (`canonical.json.large-integers-exact`,
  `canonical.json.rfc8785-form`).
- A secret digest is the keyed BLAKE3 under its recorded version
  (`canonical.ids.secret-digest-keyed`, `canonical.ids.secret-version-recorded`);
  inside an overlap both versions' digests, current first, after it the
  current only (`canonical.ids.rotation-overlap-digests`).
- A secret is never formatted or serialized
  (`canonical.ids.secret-never-emitted`, `canonical.ids.secret-never-serialized`).
- ULIDs increase per generator and are distinct across generators
  (`canonical.ids.ulid-monotonic`, `canonical.ids.ulid-unique`).
- `TokenUsage`'s parts never exceed their wholes
  (`canonical.usage.cache-within-input`).
- Signatures (visible reasoning's and a tool call's) are kept verbatim,
  hashed, and never part text (`canonical.reasoning.signature-verbatim`,
  `canonical.tool-call.signature-verbatim`,
  `canonical.opaque.outside-part-text`).
- A decoded `NormalizedExchange` passes `check`
  (`canonical.normalized.decode-checked`).
- Everything here is pure or takes its clock and randomness as arguments;
  no `unwrap` or `expect` outside tests except the two in
  `encoding::encode`, each with its reason.
