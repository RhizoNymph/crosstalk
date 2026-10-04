# The synthetic world (`crosstalk-world`)

`crates/world` holds one deterministic synthetic world that the operator UI
and the gateway's tests share: a week of agent traffic generated from a
seed, written through the spec's write traits (P0.6) into any stores that
implement them. It is the UI fixture's world (`fixture/` on
`feat/ui-conformance`) ported onto `integration/impl`: data plus a clock.
Reads go through the stores' read traits, and later through
`crosstalk-surface`; the fixture's own `QueryApi` implementation is not
ported.

It is a TestSupport crate (the architecture test's rule 2): other crates
take it as a dev-dependency only. It depends on `crosstalk-spec`,
`thiserror` and `tracing`; its tests also use `crosstalk-memory`,
`crosstalk-transport` and `tokio`.

## Scope

- **Generation** (`generate/`): 44 agent ids (40 canonical after merges)
  across Claude Code, Codex, pi, oh-my-pi and self-hosted scripts, with
  sub-agents, labels, three config-registered agents, five merges (one
  repointing, one reverted with a veto) and impersonation claims; 16
  channels (declared, discovered, promoted, superseded, sanctioned,
  unsanctioned, reset, dormant, unused, awaiting traffic); about 5,000
  transmissions on a weekday daytime curve in every state, route, match
  kind, codec chain and carrier, with their accesses, co-access records,
  content matches and message bodies; three topic versions; user rules;
  content retention's dropped bodies.
- **Assembly** (`assemble/`): every write the pipeline, the surface and
  config would have made over that week, as timed steps.
- **Seeding** (`World::seed`, `run/`): the steps in time order, each a
  spec write-trait call (or the short sequence one component makes for one
  event), with operator actions audited as `OperatorActions::act` records
  them.
- **Configuration** (`WorldConfig`): what a host builds its stores with
  (operators, sinks, built-in rules, rule config, embedding model, catalog
  retention and lineage floor, frame retention, bucket width, correlator
  timing), and the world's embedder (`WorldEmbedder`, a spec `Embedder`).
- **Handles** (`Scenario`): the ids every role got: agents by fixture key
  (`cc0`, `cc0.a`, `pi1`, `al0`), `ChannelKey`, `MergeKey`, `RuleKey`,
  `JobKey`, sinks, topics per version, the unmapped v1 topic, the lone
  resource, dropped bodies, impersonators, registered agents.
- **Clock** (`WorldClock`): the spec `Clock`, fixed at the anchor for tests
  or moving on from it in real time for serving.

## Non-scope

- Reads. The world writes; it reads back only what writes return. The
  stores' read traits, and `crosstalk-surface` over them, answer queries.
- The UI fixture's `QueryApi`, `OperatorActions`, `LiveFeed` and export
  implementations, its conformance binder and its stand-in digests.
- The conformance suite's named scenarios (`crosstalk-conformance` is not
  on `integration/impl`). The world does not compose them yet; once the
  suite lands, its seeder and this world should share the scenario
  vocabulary.
- Wire-level traffic (exchanges through ingress and canonicalization).
- Computing what the pipeline computes (correlation, fingerprints, topic
  fits, layouts): the world states the results and writes them.

## Data and control flow

```text
World::new(seed, at)
  anchor = at rounded down to 5 minutes; WorldConfig::new(seed, anchor)
        │
host builds empty stores from world.config() (+ world.embedder() for the alert store)
        │
World::seed(&mut stores)
  1. declare        ChannelRegistry::declare for config's five declared channels
                    (the registry assigns their ids; traffic is routed by them)
  2. generate       Mint (one-shot seeded UlidGenerator per id, at the entity's time)
                    cast → channel plan → topic model → traffic (states, matches,
                    bodies encoded with the spec's encoding) → dropped bodies
  3. assemble       Script of Step { at, Op }:
                    config (directory load, config entries, sink deliveries, model)
                    agents (create, establish, merge/unmerge, rename, claims, activity)
                    channels (discover/add resource at first access, accesses,
                              Candidate/Dormant/Unused, policies, promotion, refusal)
                    transmissions (saves per state, confirm, assign, index, edge;
                              re-fits v1/v2, pin, verdicts, watermark)
                    alerts (rules, triage drafts, acknowledgements, resolutions)
                    bodies, projection jobs, dead letters
                    → sorted by time, stable among equal times
  4. run            Runner: one trait call per op; Ledger keeps store-assigned ids
                    (merges, rules, alerts), lineages, and an alert book
  → Scenario
```

**Ordering.** Steps sort by time; among equal times, the order assembly
added them in. Parts are added in dependency order (a fit's activation
before a job that fails on it; a channel's discovery before the access
that discovered it).

**Store-assigned ids.** Declared channels (before generation), merge
records, user rules and alerts (during the run, behind `MergeKey`,
`RuleKey` and `AlertKey`) take the ids the stores return. The world is
therefore deterministic per store implementation: the same seed, anchor
and stores give the same world.

**The alert book.** The runner mirrors which planned alerts are active.
Suppressions are never planned: they follow from the store when a channel
is sanctioned or promoted sanctioned (`AlertTriage::channel_sanctioned`),
a rule is disabled, or a transmission is judged a false detection. A
planned acknowledgement or resolution of an alert already suppressed is
skipped; a triage that does not open or deduplicate as planned is
`WorldError::Diverged`.

**Time.** Every instant is the anchor minus an offset (`generate::times`);
every write is given its time; nothing reads a clock while seeding.

### What each op writes

| Op | Writes |
| --- | --- |
| `LoadAccess` | `OperatorStore::load` (access mode and operators, audited by the store) |
| `ConfigEntry` | `AuditLog::append` of an applied `ConfigRecord` |
| `Delivery` | `SinkRegistry::record_delivery` |
| `SetModel` | `SearchCorpus::set_model` |
| `CreateAgent`, `Advance` | `AgentLifecycle::create`, `advance` |
| `Claim`, `Activity` | `ClaimStore::record`, `ActivityStore::record` |
| `Merge`, `Unmerge`, `Rename` | `IdentityResolver::merge`, `unmerge`, `rename` (operator ones audited) |
| `Discover`, `AddResource`, `Detection`, `Confirm` | `ChannelTraffic::discover`, `add_resource`, `set_detection`, `confirm` |
| `Access` | `ChannelTraffic::record_access`, then `EdgeStore::apply_access` |
| `Save` | `TransmissionStore::save` |
| `Policy`, `Promote` | `ChannelRegistry::set_policy`, `promote` (audited); a sanction then `AlertTriage::channel_sanctioned` |
| `ForbiddenPolicy` | the audit entry of a refused call (`Forbidden`) |
| `Verdict` | `TransmissionVerdicts::set` (audited); a new revision then `EdgeStore::judge`, `SearchCorpus::judge`, `AlertTriage::transmission_judged` |
| `BeginFit`, `CompleteFit`, `Assign` | `TopicLifecycle::begin_fit`, `complete_fit`, `assign` |
| `Ready` | `TopicLifecycle::mark_ready`, `EdgeStore::version_ready`, `AlertRuleMaintenance::topic_version_ready` |
| `Activate` | `EdgeStore::activate`, `TopicLifecycle::mark_active`, `EdgeStore::drop_version` per dropped version |
| `Pin` | `TopicCatalog::pin` (audited) |
| `Index`, `Edge`, `Watermark` | `SearchCorpus::index`, `EdgeStore::apply` (a self-edge is accepted), `EdgeStore::advance_watermark` |
| `CreateRule`, `SetRuleEnabled` | `AlertRuleStore::create`, `set_enabled` (audited) |
| `Triage`, `Acknowledge`, `Resolve`, `RefusedAcknowledge` | `AlertTriage::triage`, `AlertActions::acknowledge`, `resolve` (audited; a refusal audited as rejected) |
| `Enqueue`, `StartFit`, `CompleteJob`, `FailFit`, `ExpireFrames` | `ProjectionStore::enqueue`, `claim`, `complete`, `fail`, `expire` |
| `Body`, `DeadLetter` | `BlobStore::put` (checked against the planned hash), `DeadLetterStore::put` |

## The scenarios

| Scenario | Where | Handles |
| --- | --- | --- |
| Hijacked public wiki (busiest channel, injection text) and its talk page | `generate/drafts.rs` | `ChannelKey::HijackedWiki`, `WikiTalk` |
| pi and oh-my-pi agents claiming Claude Code | `generate/agents.rs` | `pi0`, `pi1`, `omp0`, `omp2`, `omp0.b`; `Scenario::impersonators` |
| Resolver merge with a self-edge, a repointing chain, an operator merge, a reverted merge with a veto | `generate/agents.rs` | `MergeKey::*`; `al0`..`al3`, `omp3` |
| Promotion with supersession | `generate/drafts.rs` | `TeamNotes`, `OldTeamNotes` |
| Policies: unsanctioned, sanctioned, sanctioned then reset | `generate/drafts.rs` | `Pastebin`, `SharedFile`, `McpMemory` |
| Declared: active, awaiting traffic, unused (with its alert) | `generate/drafts.rs` | `InternalWiki`, `Monorepo`, `IssueTracker`, `DesignDocs`, `ReleaseBucket` |
| Three topic versions: v0 dropped by retention, v1 pinned, v2 active; stored fits and lineage | `generate/topics.rs`, `assemble/transmissions.rs` | `Scenario::topics`, `unmapped_topic` |
| Stale rule after the re-fit (still enabled), a disabled rule, a semantic rule with an agent alert | `generate/rules.rs`, `assemble/alerts.rs` | `RuleKey::*` |
| Every transmission state, route kind, match kind, codec chain, carrier | `generate/traffic.rs`, `states.rs`, `evidence.rs` | — |
| Alerts in every state and suppress reason, with deduplicated occurrences (616 for the default seed) | `assemble/alerts.rs` | — |
| Verdicts (one withdrawn), sinks, audit history with two refused calls, four dead letters, four projection jobs | `assemble/transmissions.rs`, `config.rs`, `surface.rs` | `JobKey::*` |
| Dropped bodies, sender side and reader side | `generate/retention.rs` | `Scenario::dropped` |
| Channel semantics: a single-agent scratch resource, an unconfirmed S3 handoff, a channel hidden by a merge | `generate/traffic.rs`, `drafts.rs` | `Scenario::lone_resource`, `ChannelKey::Scratch`, `S3Handoff`, `SelfNotes` |

## Divergences from the UI fixture

Where `integration/impl`'s spec differs from the UI's, the world follows
`integration/impl`.

1. **Discovery.** A discovered channel is created at the first access to
   any of its resources, which becomes its seed (`Observed`); its other
   resources join at their first access. The fixture created it at its
   first cross-agent transmission, seeded at its first planned locator
   (the channel-semantics rule, INV-850..869 once ported).
2. **The scratch entry is a channel.** `cc7`'s key-value entry gets a
   discovered channel (`ChannelKey::Scratch`, `Observed`); the fixture
   kept it a resource on no channel. No `NewChannel` alert is raised for
   it (the fixture's alert set is kept).
3. **Detection.** `Observed` at discovery, `Candidate` at the first
   co-access when it precedes the first confirmation, `Active` on each
   confirmation; the S3 handoff, with suspected traffic only, stays
   `Candidate` (the fixture: active, unconfirmed). The self-notes channel
   is listed (the fixture hid it while the merge stands).
4. **Remap threshold 0.65, not 0.8.** The memory catalog's lineage
   similarity is the clamped cosine; the fixture's was `(cos + 1) / 2`.
   0.65 keeps the outcome: only v1's "Engineering chatter" is unmapped.
5. **Watermark.** The frontier is ticked through the anchor with nothing
   pending, so the watermark is the anchor minus the correlator's
   `settle_after` (15 minutes plus the 2-day suspected TTL), rounded down
   to a bucket; the fixture's was ten minutes before the present.
6. **Re-fits.** Each re-fit assigns (and hands L7, with cause `Refit`)
   only the transmissions classified before it started; transmissions
   confirmed during a fit are classified under the version active then.
   The fixture assigned every transmission under all three versions, so
   its v1 views of the last two days held more.
7. **Alerts follow the store.** Drafts are triaged when the event behind
   them happened (a confirmation, a suspicion, a discovery), not at the
   opening time; suppressions come from the store (the memory server's
   acknowledged new-channel alert is suppressed by its sanction, where
   the fixture kept it acknowledged); a draft confirmed just after a
   resolution opens a fresh alert. 616 alerts for the default seed
   (the fixture: about 650).
8. **Ids.** ULIDs from the spec's `UlidGenerator`, one seeded generator per
   id so each carries its entity's time; store-assigned ids come from the
   stores. Message hashes are real: bodies are spec `MessageBody`s in the
   spec's canonical encoding (the fixture made hashes up).
9. **Config.** Registered agents are registered at the first config load
   (the fixture: at a random earlier time). The first document's entries
   also set the sinks, topic retention and frame retention
   (`ConfigChange::SetSink`, `SetTopicRetention`, `SetFrameRetention`,
   which the UI's spec lacked). Pinning v1 is audited
   (`PinTopicVersion`).
10. **Dropped bodies** are never stored (`BlobStore` has no delete).
11. **Search corpus.** Each classified transmission is indexed with its
    senders' text, embedded by `WorldEmbedder` from its first 1,000
    characters.
12. **Dead letters.** The `EdgeUpdated` letter's bucket is five minutes
    (the fixture used an hour); the `AccessRecorded` letter names the
    channel the lookup named at the time.

## Gap list: fixture reads with no store or spec trait

The fixture answered these from its own state. On `integration/impl` no
store read trait (or no spec trait at all) answers them, so neither the
seed nor `crosstalk-surface` can serve them through the spec alone. The
seed does not work around any of them.

| # | Fixture read | Needed for | Missing | Proposed shape |
| --- | --- | --- | --- | --- |
| 1 | A transmission's topic under a version (`TxRecord::assignment`) | `transmissions_by_id` (`TopicUnder`), `edge_transmissions` rows, the transmissions export's topic column | No read of a stored assignment. `TopicLifecycle::assign` writes it; `TopicCatalog` reads only sizes. The memory search index and edge store reach it through `InMemoryTopicCatalog::assignment`, an inherent method behind the memory-only `TopicVersions` trait. | `TopicCatalog::assignments(version, ids: &IdBatch<TransmissionId>) -> BTreeMap<TransmissionId, StoredAssignment>` (`VersionNotRetained` for a dropped version) |
| 2 | A span's location (`Blobs::span`) | `transmission_evidence`: the sender-side excerpt of every content match | No span store. `FingerprintIndex` keeps fingerprints only; nothing records an `OriginatedSpan` (its location) for reading back. | L4 `SpanStore`: `record(span: OriginatedSpan)` (the provenance consumer's write) and `spans(ids: &IdBatch<SpanId>) -> BTreeMap<SpanId, OriginatedSpan>` |
| 3 | An access and its resource by id (`World::access`, `World::resource`) | `transmission_evidence`'s `AccessDetail` for co-access evidence; a discovered channel's seed access and when it happened | `ChannelTraffic::record_access` stores accesses, but `ChannelReads` reads only channels; a resource is readable only inside a windowed `resource_use` page. | `ChannelReads::accesses(ids: &IdBatch<AccessId>) -> BTreeMap<AccessId, (Access, Resource)>` |
| 4 | Transmissions by state, channel and window (`World::transmissions`) | Verdicts and quality rows over unconfirmed transmissions (`verdict_rows` export of a verdict on a suspected one), a review queue of suspected transmissions, the channel-semantics port's `channel_transmissions` and `CrossTraffic` | `TransmissionStore` reads one id. `EdgeStore::transmissions` drills into an edge, which holds aggregated confirmed transmissions only. | `TransmissionStore::list(query: TransmissionQuery { window, states, channel }, page: &PageRequest<TransmissionList>) -> Page<Transmission, TransmissionList>` (channel resolved through `ChannelDirectory`) |
| 5 | Content retention dropping a body (`Blobs::drop_body`) | Seeding the dropped-bodies scenario; retention itself | `BlobStore` has `put` and `get`, no delete, and no content retention is specified. The seed never stores those bodies (a `get` of `None` is "dropped by retention" by the spec's definition). | `BlobStore::drop(hash)`, or an L2 `ContentRetention::enforce(now) -> Vec<MessageHash>` |
| 6 | The configured sinks, built-in rules and retention as config loads (`config::record`) | Seeding config through writes; config reloads | Only operators load through a trait (`OperatorStore::load`, audited in one transaction). Sinks, built-in rule status and sinks, topic retention and frame retention are store constructor config; their `ConfigChange` entries are appended to the audit log separately, so a load and its entries are not one transaction. | A config-load trait per area mirroring `OperatorStore::load`: `SinkRegistry::load(sinks, hash, at)`, `AlertRuleStore::provision(builtins, hash, at)`, `TopicCatalog::set_retention(policy, hash, at)`, each returning its `ConfigChange`s and appending them in its transaction |

Related, not a spec gap: the memory crate implements `NodeFacts` only as
`StaticNodes` (set by hand), so graphs over the seeded memory stores draw
nodes with the defaults (no labels, claims or policies). A `NodeFacts`
cache fed by L3's and L5's events (as the spec describes it) is wiring
work for `crosstalk-surface` or the gateway.

## Tests waiting for the channel-semantics port

In `tests/channels.rs`, each `#[ignore = "waits for the channel-semantics port"]`;
the facts they read are seeded today:

- `the_scratch_entry_one_agent_uses_is_no_channel`
- `the_unconfirmed_s3_handoff_is_active`
- `the_channel_hidden_by_a_merge_is_not_listed`
- `discovered_channels_are_seeded_by_a_cross_agent_transmission`

When the port lands, discovery moves to the first cross-agent
transmission (`ChannelRegistry::discover(resource, transmission, at)`), the
scratch entry's channel and `ChannelKey::Scratch` go, and these tests are
updated to the port's reads (`Listing`, `CrossTraffic`).

## Invariants and constraints

- **Writes only through spec traits.** No store hook, no inherent method,
  no reach-around: a gap is listed above instead.
- **Time is an argument.** Every instant is the anchor minus an offset;
  the anchor is on a bucket boundary; no store and no generator reads a
  clock.
- **Deterministic.** The same seed, anchor and store implementation give
  the same `Scenario` and the same reads (`tests/agents.rs` compares two
  seedings list by list); another seed gives another world.
- **Ids carry their time.** Every minted id is a ULID whose time is its
  entity's (`Mint`, one seeded `UlidGenerator` per id).
- **Every value through the spec's checked constructors**; a refusal is
  `WorldError::Invalid`. A store's refusal is `WorldError::Store` with the
  trait's typed error (`StoreError`); a store answering differently from
  the plan is `WorldError::Diverged`. No `unwrap` or `expect` outside
  tests.
- **Empty stores.** Seeding assumes fresh stores configured from
  `World::config` (topic versions are expected to be numbered 1 and 2, the
  first projection claim to hand out the planned job).
- **Operator actions are audited** as `OperatorActions::act` records them:
  the directory's `Caller`, the action, `AuditOutcome::of` its result.
- **Test support only.** A dev-dependency of other crates (architecture
  test, `TestSupport::World`).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/world/src/lib.rs` | Crate docs and re-exports | `World`, `WorldConfig`, `WorldEmbedder`, `WorldStores`, `Scenario`, `ChannelKey`, `MergeKey`, `RuleKey`, `JobKey`, `BodySide`, `Anchor`, `WorldClock`, `UI_ANCHOR`, `WorldError`, `StoreError` |
| `crates/world/src/seed.rs` | `World`: new, config, clock, embedder, seed (declare, generate, assemble, run) | `World` |
| `crates/world/src/stores.rs` | The stores the seed writes, as spec traits | `WorldStores` |
| `crates/world/src/config.rs` | Store configuration as spec values; operators; constants | `WorldConfig`, `SinkDef`, `BuiltinDef`, `OPERATOR_RESEARCHER`, `OPERATOR_ONCALL`, `REMAP_THRESHOLD`, `document` |
| `crates/world/src/clock.rs` | Anchor, offsets, the world clock | `Anchor`, `WorldClock`, `BUCKET`, `UI_ANCHOR`, `plus`, `minus` |
| `crates/world/src/mint.rs` | Ids from one-shot seeded `UlidGenerator`s | `Mint` |
| `crates/world/src/embed.rs` | The theme-vector embedder (spec `Embedder`), topic centroids | `WorldEmbedder`, `mix`, `similarity` |
| `crates/world/src/error.rs` | Typed errors | `WorldError`, `StoreError` |
| `crates/world/src/scenario.rs` | Role handles | `Scenario`, `ChannelKey`, `MergeKey`, `RuleKey`, `JobKey`, `BodySide` |
| `crates/world/src/rng.rs`, `text/` | SplitMix64; message templates and codecs (ported verbatim) | `Rng`, `Theme`, `paragraph`, `sentence` |
| `crates/world/src/generate/` | Generation: `times`, `agents`, `drafts`, `channels`, `topics`, `traffic`, `states`, `evidence`, `bodies`, `retention`, `rules` | `generate`, `Generated`, `Cast`, `ChannelPlan`, `TopicModel`, `Traffic`, `TxRecord`, `Blobs` |
| `crates/world/src/script/` | The script's ops and ordering | `Script`, `Step`, `Op`, `AlertKey`, `RuleRef` |
| `crates/world/src/assemble/` | Generated data to steps: `config`, `agents`, `channels`, `transmissions`, `alerts`, `surface` | `assemble`, `Assembled` |
| `crates/world/src/run/` | The runner: one module per area of writes; the ledger and alert book | (crate) `Runner`, `Ledger` |
| `crates/world/tests/support/` | The memory stores as `WorldStores` (with transport's blob store and the bus's dead letters), one shared seeded world per binary, whole-list reads | `MemoryWorld`, `seed`, `shared`, `run`, `read::*` |
| `crates/world/tests/*.rs` | Scenario tests through the read traits: `agents`, `channels`, `insight`, `history`; `generation` (the generated data, no stores) | — |
