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
  repointing, one reverted with a veto) and impersonation claims; 15
  channels (declared, discovered, promoted, superseded, sanctioned,
  unsanctioned, reset, dormant, unused, awaiting traffic, listed
  unconfirmed, hidden by a merge) and a resource only one agent uses,
  which is no channel; about 5,000
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
  resource (and `ChannelKey::Scratch`, the id of the channel the world
  never creates for it), dropped bodies, impersonators, registered agents.
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
                    channels (resource at first sighting: on a declared channel
                              or on none; accesses; discovery at the first
                              cross-agent transmission through a seed;
                              Dormant/Unused; policies, promotion, refusal)
                    transmissions (saves per state, each channel state recorded
                              as traffic, assign, index, edge; re-fits v1/v2,
                              pin, verdicts, watermark)
                    alerts (rules, triage drafts, acknowledgements, resolutions)
                    bodies, projection jobs, dead letters
                    → sorted by time, stable among equal times
  4. run            Runner: one trait call per op; Ledger keeps store-assigned ids
                    (merges, rules, alerts), lineages, and an alert book
  → Scenario
```

**Ordering.** Steps sort by time; among equal times, the order assembly
added them in. Parts are added in dependency order (a fit's activation
before a job that fails on it; the read that opened a channel's first
cross-agent transmission, then the discovery, then that transmission's
save).

**Placement** (`assemble::Placement`). Where each resource is over the
week: a declared channel's resources are on it from their first sighting;
a discovered channel's seed is on no channel until its first cross-agent
transmission opens (the oldest channel transmission routed through it
with a co-access), which discovers it; after the promotion, the standup
page's lookups name the promoted channel; the scratch entry is never on
one. `AddResource` is checked against it, `Discover` is planned from it,
and the `AccessRecorded` dead letter names the channel it gives.

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
| `Span` | `SpanIndex::record` of each content match's origin span (`OriginatedSpan`, written by the match's sender; its exchange id is the span's own ULID, since the sender's exchange is not modelled), when its exchange was captured, in span id order |
| `AddResource` | `ChannelTraffic::add_resource`, which must place the resource where `Placement` does (`Diverged` otherwise) |
| `Discover` | `ChannelTraffic::discover` under the plan's minted id, which must answer `Created` |
| `Detection` | `ChannelTraffic::set_detection` (`Dormant`, `Unused`) |
| `Access` | `ChannelTraffic::record_access`, then `EdgeStore::apply_access` (bucketed by resource) |
| `Save` | `TransmissionStore::save`; for a channel transmission past `Detected`, then `ChannelTraffic::record_transmission` (opened or confirmed keeps its canonical channel `Active`; a declared channel goes `InUse`) |
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
| Channel semantics: a single-agent scratch resource on no channel, an S3 handoff listed unconfirmed (suspected traffic only), a channel hidden by a merge (its traffic all within `cx1` once `al1` merged into it) | `generate/traffic.rs`, `drafts.rs`, `assemble/channels.rs` | `Scenario::lone_resource`, `ChannelKey::Scratch` (never stored), `S3Handoff`, `SelfNotes` |

## Divergences from the UI fixture

Where `integration/impl`'s spec differs from the UI's, the world follows
`integration/impl`.

1. **Discovery** follows the channel-semantics rule, as the fixture's
   did: a discovered channel is created by the first cross-agent
   transmission through its seed, when it opens, and seeded by it. Its id
   is minted before traffic is generated (traffic is routed by it), at the
   start of the channel's planned traffic window (`Draft::created`), so
   the id's time is that, not the discovery's.
2. **A discovered channel holds one resource.** The fixture's discovered
   channels held several (the hijacked wiki three pages, the pastebin
   three pastes, the memory server three tools, team notes and the handoff
   directory two each). On `integration/impl` a discovered channel holds
   exactly its seed and a resource joins a channel only through a declared
   pattern, so each other resource would discover a channel of its own.
   The world keeps each one's first locator as its seed and drops the
   rest. Declared channels keep all their resources.
3. **Detection.** `Active` from discovery (a declared channel `InUse` at
   its first cross-agent transmission), kept active by every transmission
   that opens or is confirmed; `Dormant` a day after a dormant channel's
   last one. As in the fixture, the S3 handoff is active and listed
   unconfirmed, and the self-notes channel is hidden while the merge
   stands; the scratch entry is a resource on no channel.
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
    channel the lookup named at the time (none for a resource on no
    channel).

## Gap list: fixture reads with no store or spec trait

The fixture answered these from its own state. On `integration/impl` no
store read trait (or no spec trait at all) answers them, so neither the
seed nor `crosstalk-surface` can serve them through the spec alone. The
seed does not work around any of them.

| # | Fixture read | Needed for | Missing | Proposed shape |
| --- | --- | --- | --- | --- |
| 1 | A transmission's topic under a version (`TxRecord::assignment`) | `transmissions_by_id` (`TopicUnder`), `edge_transmissions` rows, the transmissions export's topic column | Resolved: `TopicCatalog::assignments(version, ids)` (INV-1071); rows by id and a channel's transmissions read it (INV-1072). The transmissions export still reads the stored classification. | — |
| 2 | A span's location (`Blobs::span`) | `transmission_evidence`: the sender-side excerpt of every content match | Resolved: the seed records every origin span through L4's `SpanIndex::record` (`WorldStores::Spans`, op `Span`); the in-process `MemoryEvidence` keeps them (INV-1076). | — |
| 3 | An access and its resource by id (`World::access`, `World::resource`) | `transmission_evidence`'s `AccessDetail` for co-access evidence | Resolved: `AccessStore::accesses` reads accesses with their resources; the in-process `MemoryEvidence` reads them, and a resource by id (`MemoryChannels::resource`), from the registry. | — |
| 4 | Transmissions by state, channel and window (`World::transmissions`) | Verdicts and quality rows over unconfirmed transmissions (`verdict_rows` export of a verdict on a suspected one), a review queue of suspected transmissions across channels (one channel's is `ChannelReads::transmissions`) | `TransmissionStore` reads one id. `EdgeStore::transmissions` drills into an edge, which holds aggregated confirmed transmissions only. | `TransmissionStore::list(query: TransmissionQuery { window, states, channel }, page: &PageRequest<TransmissionList>) -> Page<Transmission, TransmissionList>` (channel resolved through `ChannelDirectory`) |
| 5 | Content retention dropping a body (`Blobs::drop_body`) | Seeding the dropped-bodies scenario; retention itself | `BlobStore` has `put` and `get`, no delete, and no content retention is specified. The seed never stores those bodies (a `get` of `None` is "dropped by retention" by the spec's definition). | `BlobStore::drop(hash)`, or an L2 `ContentRetention::enforce(now) -> Vec<MessageHash>` |
| 6 | The configured sinks, built-in rules and retention as config loads (`config::record`) | Seeding config through writes; config reloads | Only operators load through a trait (`OperatorStore::load`, audited in one transaction). Sinks, built-in rule status and sinks, topic retention and frame retention are store constructor config; their `ConfigChange` entries are appended to the audit log separately, so a load and its entries are not one transaction. | A config-load trait per area mirroring `OperatorStore::load`: `SinkRegistry::load(sinks, hash, at)`, `AlertRuleStore::provision(builtins, hash, at)`, `TopicCatalog::set_retention(policy, hash, at)`, each returning its `ConfigChange`s and appending them in its transaction |

Related, not a spec gap: the memory crate implements `NodeFacts` only as
`StaticNodes` (set by hand). Over the in-process surface the graphs read
`crosstalk-surface`'s `NodeCache`, fed by the relay; `seed_world` (and the
UI's world backend) wait for `InProcess::settle` so it holds every seeded
event before the world is read.

## Channel-semantics tests

In `tests/channels.rs`, once marked to wait for the channel-semantics port
and now run as written:

- `the_scratch_entry_one_agent_uses_is_no_channel`
- `the_unconfirmed_s3_handoff_is_active`
- `the_channel_hidden_by_a_merge_is_not_listed`
- `discovered_channels_are_seeded_by_a_cross_agent_transmission`

Added with the port: the hijacked wiki discovered from its one page, the
scratch entry's lookup (`NoChannel`), every channel's `Listing`, the S3
handoff's review list (`ChannelReads::transmissions`), and each discovered
channel created when its seed transmission opened.

`the_hijacked_wiki_is_discovered_and_holds_its_three_pages` is ignored: it
expects a discovered channel with two resources besides its seed, which
the semantics rule out (see divergence 2).

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
- **Channels follow cross-agent traffic.** A discovered channel is
  created only by `ChannelTraffic::discover`, at its first cross-agent
  transmission, and holds exactly its seed; a resource only one agent uses
  is never a channel; projection frames hold no transmission within one
  agent.
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
| `crates/world/src/stores.rs` | The stores the seed writes, as spec traits (L4's `SpanIndex` among them) | `WorldStores` |
| `crates/world/src/config.rs` | Store configuration as spec values; operators; constants | `WorldConfig`, `SinkDef`, `BuiltinDef`, `OPERATOR_RESEARCHER`, `OPERATOR_ONCALL`, `REMAP_THRESHOLD`, `document` |
| `crates/world/src/clock.rs` | Anchor, offsets, the world clock | `Anchor`, `WorldClock`, `BUCKET`, `UI_ANCHOR`, `plus`, `minus` |
| `crates/world/src/mint.rs` | Ids from one-shot seeded `UlidGenerator`s | `Mint` |
| `crates/world/src/embed.rs` | The theme-vector embedder (spec `Embedder`), topic centroids | `WorldEmbedder`, `mix`, `similarity` |
| `crates/world/src/error.rs` | Typed errors | `WorldError`, `StoreError` |
| `crates/world/src/scenario.rs` | Role handles | `Scenario`, `ChannelKey`, `MergeKey`, `RuleKey`, `JobKey`, `BodySide` |
| `crates/world/src/rng.rs`, `text/` | SplitMix64; message templates and codecs (ported verbatim) | `Rng`, `Theme`, `paragraph`, `sentence` |
| `crates/world/src/generate/` | Generation: `times`, `agents`, `drafts`, `channels`, `topics`, `traffic`, `states`, `evidence`, `bodies`, `retention`, `rules` | `generate`, `Generated`, `Cast`, `ChannelPlan`, `TopicModel`, `Traffic`, `TxRecord`, `Blobs` |
| `crates/world/src/script/` | The script's ops and ordering | `Script`, `Step`, `Op`, `AlertKey`, `RuleRef` |
| `crates/world/src/assemble/` | Generated data to steps: `config`, `agents`, `channels` (placement and discovery), `transmissions`, `alerts`, `surface` | `assemble`, `Assembled`, `Placement`, `Seeding`, `promotion` |
| `crates/world/src/run/` | The runner: one module per area of writes; the ledger and alert book | (crate) `Runner`, `Ledger` |
| `crates/world/tests/support/` | The memory stores as `WorldStores` (with transport's blob store and the bus's dead letters), one shared seeded world per binary, whole-list reads | `MemoryWorld`, `seed`, `shared`, `run`, `read::*` |
| `crates/world/tests/*.rs` | Scenario tests through the read traits: `agents`, `channels`, `insight`, `history`; `generation` (the generated data, no stores) | — |
