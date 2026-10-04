# L8 conformance suite

`crosstalk-conformance` (`conformance/`) is a test suite that any
implementation of the spec's L8 traits (`QueryApi`, `OperatorActions`,
`LiveFeed` and the export stream) must pass. It holds the behaviour the UI
relies on (alias resolution, counting by confirmation time, bucket-aligned
windows and the watermark, shares, filters and paging, permissions, the
channel rules, action outcomes and audit) as `async` tests generic over the
implementation. Today it runs against the fixture backend. The gateway's
surface (`crates/surface` on `integration/impl`) will run the same tests.

This page has two parts:

- **What exists on this branch.** The suite as built on the `#16`-era spec,
  instantiated for `crosstalk-fixture`.
- **The redesign onto `integration/impl`.** The suite seeded through the
  spec's write traits, run against the memory stores, then Postgres and the
  HTTP client. The coordinator and the gateway team decided this after the
  first part was built. Nothing on this branch implements it yet: the
  branch cannot compile against `integration/impl` until `feat/ui` is
  migrated.

## Scope

- **The harness contract.** What an implementation provides so the suite
  can run against it (`Harness`).
- **The scenario vocabulary.** Worlds described as facts over typed roles,
  and the named scenarios built from the fixture world's cases.
- **The scenario self-check.** Every fact of a provisioned scenario is
  observed through L8 reads before any test relies on it.
- **The tests.** One `async fn` per test, generic over the harness,
  grouped by area, each citing the `spec/invariants` id it checks.
- **`suite!`.** The macro that instantiates every test for one harness.
- **The fixture's harness** (`crosstalk-fixture`, `src/conformance/`).

## Non-scope

- **Fixture-specific behaviour.** World generation and determinism, the
  scenario contents of the generated week, the Parquet refusal, the live
  clock, the fixture's export limit, and its seeded projection jobs stay as
  fixture tests (`fixture/src/tests/`).
- **Wire-level, end-to-end scenarios.** Harness traffic replayed through
  ingress, canonicalization and the stores is a later layer. See
  "End-to-end layer" below.
- **Performance invariants.**
- **New invariants.** The suite cites existing ones. If the UI ever adds
  any, its block is INV-900..949.

## What exists on this branch

### Crates and dependencies

| Crate | Role | Depends on |
| --- | --- | --- |
| `conformance/` `crosstalk-conformance` | The suite: harness trait, scenarios, self-check, tests, `suite!` | `crosstalk-spec`, `thiserror = "=2.0.21"`, `tokio = "=1.53.1"` (`rt`, `time`, `macros`, `sync`). No new third-party dependency. |
| `fixture/` `crosstalk-fixture` | The synthetic backend. Under `cfg(test)` or the `testing` feature it also holds `conformance::FixtureHarness` and runs the suite (`src/conformance/suite.rs`). | `crosstalk-conformance`: optional under `testing`, and a dev-dependency |
| `ui/` `crosstalk-ui` | Unchanged. Its dev-dependency on `crosstalk-fixture` with `testing` now pulls the suite crate in for tests only. | |

Each crate keeps its own lock, as before (there is no workspace on this
branch).

### Data and control flow

```text
suite!(harness) ── one #[test] per suite test ──▶ run(harness, test)
                                                   │ current-thread runtime, block_on
                                                   ▼
test(&harness) ── World::open(harness, scenario) ──▶ Harness::provision(Provision { scenario, knobs })
                     │                                  └─▶ Provisioned { backend, bindings }
                     │   + Harness::{operators, bucket_width, extent, now, row_hasher}
                     ▼
            reads and actions through QueryApi / OperatorActions / LiveFeed only
            roles → ids through Bindings; expectations from the scenario's facts
            and from relations between reads
```

1. A test opens a world: `World::open(harness, scenario)` (or
   `World::everything`, `World::of`).
2. The harness returns a fresh backend whose world contains every fact of
   the scenario, plus `Bindings` from each role to an id.
3. The test reads and acts only through the L8 traits. It names things by
   role (`hijacked_wiki::WIKI`), and `World::id` looks up the bound id.
4. Assertions are either facts the scenario implies (a discovered channel
   lists as confirmed once a confirmed cross-agent transmission crosses
   it), or relations between reads:
   - an edge's transmissions page holds exactly the edge's count;
   - halves of a window add up to the whole;
   - a merge, an unmerge and a re-merge restore the channel list.

   They never assert totals of one particular world.

### The harness (`conformance/src/harness/`)

```rust
pub trait Harness {
    type Backend: QueryApi + OperatorActions + LiveFeed;
    type Hasher: RowHasher;
    async fn provision(&self, request: Provision<'_>) -> Result<Provisioned<Self::Backend>, ProvisionError>;
    fn operators(&self) -> Operators;          // two operators the backend's directory defines
    fn bucket_width(&self) -> BucketWidth;     // QueryApi does not expose it
    async fn now(&self, backend: &Self::Backend) -> Timestamp;
    async fn extent(&self, backend: &Self::Backend) -> TimeWindow; // aligned, covers every fact
    fn row_hasher(&self) -> Self::Hasher;      // verify_export's digest
}
```

**`provision`.**
- Takes a `Scenario` and `Knobs`. The knobs are the spec's own config types:
  `LiveConfig` for the feed, and `ExportLimits` for `export.max_rows`.
- After it returns, nothing changes the world but the test's own calls.
- Every fact is settled before the watermark, except in-flight evidence.

**`ProvisionError`.** Either `Unsupported { scenario }` (the fixture
provisions only the named scenarios) or `Failed { scenario, reason }`.

**`Callers`.** Builds callers the only way the spec builds one: an
authenticated `OperatorDirectory` for one of the harness's operators,
holding exactly the permissions a test asks for.

### The scenario vocabulary (`conformance/src/scenario/`)

**Roles** (`roles.rs`):
- A `Role<K>` is `(scenario, name)` with its kind in the type: `AgentRole`,
  `ResourceRole`, `ChannelRole`, `TransmissionRole`, `MergeRole`,
  `RuleRole`.
- An agent role can never be looked up as a channel.
- Roles composed from different scenarios never collide.

**Facts** (`facts.rs`) are stated at the level of what the gateway derives,
never how it came to be:

| Fact | Says |
| --- | --- |
| `AgentFact` | An agent: its harness (or any), extra claimed families (impersonation), label, parent, `Seen` or `RegisteredOnly` |
| `ResourceFact` | A resource and its locator |
| `ChannelFact` | `Declared { pattern }` or `Discovered { seed }` |
| `AccessFact` | An access that belongs to no transmission (`Op::Write` / `Op::Read`) |
| `TransmissionFact` | `reader`, an optional route (`Via::Resource`, `Delegation`, `Direct`, `Unobserved`), and its `Evidence` |
| `MergeFact` | `alias` into `into`, by `Resolver` or `Operator`, optionally reverted (leaving a veto) |
| `PromotionFact` | A past promotion: channel, pattern, policy, the channels it superseded |
| `PolicyFact` | Operator policy decisions, oldest first |
| `VerdictFact` | Operator verdicts, oldest first (`None` withdraws) |
| `BodyDroppedFact` | Content retention dropped the sender's or reader's body |
| `TopicHistoryFact` | A dropped version, then an older retained version with a lineage to its successor, then the active version |
| `StaleRuleFact` | An enabled watched-topic rule left stale by the re-fit |
| `DeadLetterFact` | Dead letters in at least *n* consumer groups |

`Evidence` names a writer exactly when its evidence does:

```text
Confirmed { writer, timing } | Suspected { writer } | AwaitingContent { writer }
  | Discarded { writer } | Detected
```

`Timing::ConfirmedInLaterBucket` makes counting by confirmation time
observable.

**Validation.** `Scenario::build(name)...done()` checks:
- roles are declared once, by their own scenario;
- every reference is declared;
- a transmission never names one agent twice;
- co-access evidence travels through a resource;
- a resource carrying transmissions is on a channel;
- a promotion supersedes only discovered channels.

**Composition.** `Scenario::compose` unions scenarios and records their
parts.

**Named scenarios** (`scenario/named/`). Each is a module of role
constants and a `scenario()` function:

| Scenario | The case |
| --- | --- |
| `hijacked_wiki` | A public wiki page and its talk page, discovered, with confirmed cross-agent traffic |
| `late_confirmation` | A transmission confirmed a bucket after it opened |
| `impersonation` | A pi agent whose traffic also claims Claude Code, labelled `pi-scraper` |
| `merges` | A resolver alias with a self-edge, a repointed chain, a reverted merge with a veto, canonical agents for the merge actions |
| `hidden_channel` | A discovered channel whose only traffic was between two ids an operator later merged |
| `suspected` | A channel whose only traffic is suspected (unconfirmed) |
| `declared` | A declared channel with traffic, and one never used |
| `lone_resource` | A resource only one agent writes and reads: no channel |
| `promotion` | A promoted notes page that superseded a sibling's channel |
| `policies` | A paste site unsanctioned; an MCP server sanctioned, then reset |
| `topics` | The topic history after two re-fits, and a stale rule |
| `verdicts` | Transmissions in every evidence state; a false detection, a withdrawn verdict |
| `dropped_bodies` | Sender-side and reader-side dropped bodies, and a kept one |
| `pipeline` | Dead letters in two groups |
| `routes` | Confirmed delegation, direct and unobserved transmissions |
| `registered` | A config-registered agent never seen |
| `everything` | All of them composed |

**Closed-world rule.** A channel's traffic is exactly what the scenario
routes through it. A harness must not route other transmissions through a
scenario's channels, because the listing the self-check expects follows
from that traffic.

### The self-check (`support/check.rs`)

`check::observable(&world)` verifies every fact through L8 reads. Each
check below cites the invariant it exercises.

- **Agents** resolve to the canonical agent of their scenario (INV-714).
  - Claims cover the fact's families over the cluster (INV-663).
  - Labels and parents match.
  - A registered-only agent has no claims and no last-seen time.
- **Channels** have their origin.
  - A declared channel has its pattern.
  - A discovered one has its seed and seed locator (INV-747).
  - A promoted one has `Promoted { from }` and the promotion's pattern and
    policy.
  - A superseded one has its supersession.
  - Its listing follows the scenario's cross-agent traffic: confirmed,
    unconfirmed, a declaration, hidden, or none when superseded (INV-753).
- **Transmissions** have a row by id (a non-crossing one may not).
  - The reader and sender are canonical.
  - The state matches the evidence, and a confirmed one settled before the
    watermark.
  - A late one was confirmed in a later bucket.
  - The route resolves through supersession (INV-682).
- **Merges** have their record (source, target, author) in the alias's
  cluster. A reverted merge carries its reversal and its veto (INV-617).
- **Policy histories and verdict logs** end with the fact's decisions.
- **A dropped body** shows `BodyDropped` on its side only (INV-698).
- **A lone resource** seeds and belongs to no channel (INV-749).
- **The topic history and stale rules** exist; a stale rule is still
  enabled (INV-309).
- **Dead letters** span enough groups.

### The tests (`conformance/src/tests/`)

64 tests, all passing against the fixture:

| Area | Tests | What they hold (invariants) |
| --- | --- | --- |
| `scenarios` | 18 | Every named scenario validates, and each one and their composition is observable fact by fact |
| `graph` | 12 | Canonical nodes covering edges, no self-edges, shares sum to one (INV-680, 681, 758); counting by `Confirmed::at` (INV-589); windows add up (INV-354); an edge's transmissions are exactly its count and bytes (INV-409); the channel-centred view shares `topology`'s edges and draws listed channels only (INV-676, 675, 757); agent and channel filters resolve aliases and supersession (INV-679); route, topic and conjunction filters (INV-345, 637); false detections subtracted (INV-534); confirmed only (INV-756, 759); nothing within one agent counts (INV-758); claims shown as claims |
| `series` | 8 | Series totals and groupings equal the graph (INV-446, 433, 437, 438); a coarser step sums finer points (INV-445); a grid for another bucket width is refused (INV-443); the overview is `EdgeTotals::of` the graph (INV-710, 743); queues are the lists and honour confirmed only (INV-760); every watermarked read carries the watermark (INV-579, 594, 588) |
| `refusals` | 5 | Unaligned windows (INV-351); unknown topic versions `NotFound` (INV-645); dropped versions `VersionNotRetained`, with topics, lineage and frozen sizes still readable (INV-572, 563); cursors bound to their request (INV-402); unknown ids |
| `projections` | 5 | Each fit is a new job with a reproducible frame, pinned spec and watermark (INV-639, 623, 632, 634, 393); samples honour window and filter (INV-394, 381, 395); narrowing keeps the sample (INV-396); too few points fail (INV-629); jobs newest first |
| `channels` | 16 | The default list and origin filters (INV-754, 691); listings follow cross-agent traffic (INV-753) and partition the list; declarations without traffic (INV-238); row counts are the resources' tally and the graph's (INV-687, 743); the window counts but never filters (INV-692); resources through the channel in force (INV-668); names (INV-689); policy histories; an unconfirmed channel's suspected transmissions (INV-764); channel transmissions are cross-agent only (INV-764) and need View (INV-765); a merge hides, an unmerge restores and a re-merge hides again (INV-755); alerts on a hidden channel (INV-763); discovery raises `NewChannel` and resources sit on one channel (INV-750, 748) |

**Not ported yet.** These fixture tests stay in `fixture/src/tests/` until
their suite versions exist, so nothing is lost meanwhile:
- agents and governance (merge, unmerge, rename);
- action outcomes and permissions;
- audit;
- triage and alerts;
- promotion and policy actions;
- rules;
- topics;
- transmissions, evidence and search;
- lists;
- live feed;
- export.

The redesign below is where they land: the coordinator stopped this branch
before they were ported.

### `suite!` and running

```rust
crosstalk_conformance::suite!(crate::conformance::FixtureHarness::new().expect("clock"));
```

- **Expansion.** The macro expands to one module per area, and one
  `#[test]` per suite test.
- **Each test** evaluates the harness expression, which sees the caller's
  names through `use super::*`. It then runs the test on a fresh
  current-thread runtime with time enabled (`run`), and returns
  `Result<(), RunError>`.
- **The fixture's suite** runs under `cargo test` in `fixture/`:
  `cargo test conformance::suite`.

### Async and `Send`

On this branch the spec's traits are native `async fn` without `Send`
bounds:

- The suite runs every test on a current-thread runtime and never spawns,
  so no future needs to be `Send`.
- `Harness` follows the spec (`#[allow(async_fn_in_trait)]`).
- A generic client, or a test that spawned (an HTTP client polled from a
  multi-threaded server test), would need return-type-notation bounds:
  `H::Backend: QueryApi<topology(..): Send>`.

On `integration/impl` every spec trait returns `impl Future + Send`, so
that need disappears. The redesign keeps the current-thread runner anyway,
for determinism.

### The fixture's harness (`fixture/src/conformance/`)

**What it is.** The fixture cannot build arbitrary worlds: it generates one
week from a seed. It provisions a named scenario by binding.

**How it binds.**
- `bind::scenario` dispatches each part to a binder.
- The binder finds things in the generated world that satisfy the facts,
  using `find::Find`:
  - the agents and channels by their fixture keys;
  - seeds from origins;
  - merges from the merge log;
  - the newest settled, cross-agent, confirmed transmission between
    canonical agents that a predicate admits.
- The suite's scenario tests then check every bound fact, so a wrong binder
  fails there.
- Any other scenario is `Unsupported`.

`FixtureHarness` itself:
- operators: the researcher (lead) and on-call;
- bucket width: five minutes;
- `now`: the fixture clock;
- extent: `[START, NOW + BUCKET)`;
- hasher: the fixture's `RowDigest` stand-in.

### Invariants and constraints

- Tests reach the implementation only through the L8 traits and the
  harness: no store, world or fixture internals.
- Expectations come from the scenario's facts and from relations the spec
  defines, never from totals of one generated world.
- Every test that acts opens its own fresh world.
- A role's id comes from `Bindings` only.
- Every assertion that has an invariant cites it in the test's doc
  comment.
- Ordering the spec leaves unobservable is not asserted. For example,
  "newest channel first": a `ChannelRow` carries no creation time.

### Files

| File | Role | Key exports |
| --- | --- | --- |
| `conformance/src/lib.rs` | Crate doc, re-exports | `Harness`, `Scenario`, `Bindings`, `run`, `suite!` |
| `conformance/src/harness/mod.rs` | The harness contract | `Harness`, `Provision`, `Knobs`, `Provisioned`, `ProvisionError`, `Operators` |
| `conformance/src/harness/callers.rs` | Callers from an authenticated directory | `Callers`, `CallerError` |
| `conformance/src/scenario/{mod,roles,facts,bindings}.rs` | The vocabulary and validation | `Scenario`, `ScenarioBuilder`, `ScenarioError`, `Role<K>`, the `*Fact` types, `Evidence`, `Via`, `Timing`, `Bindings`, `Unbound` |
| `conformance/src/scenario/named/*.rs` | The named scenarios | one module per scenario, `all`, `everything` |
| `conformance/src/support/world.rs` | A provisioned world for a test | `World` |
| `conformance/src/support/{paging,reads,windows}.rs` | Traversal, shared reads, aligned windows | `collect`, `first`, `counted`, `edge_rows`, `grid`, `halves`, `split_at`, `quiet`, `unaligned` |
| `conformance/src/support/check.rs` | The scenario self-check | `observable` |
| `conformance/src/tests/*.rs` | The tests by area | one `pub async fn` per test |
| `conformance/src/suite.rs` | Runner and macro | `run`, `RunError`, `suite!` |
| `fixture/src/conformance/{mod,bind,find,suite}.rs` | The fixture's harness | `FixtureHarness`, `SEED` |

## The redesign onto `integration/impl`

### What `integration/impl` has (as read at `75573db`)

- **A workspace.** `spec/` plus `crates/*`, with one lock, and roles
  enforced by `crates/gateway/tests/architecture.rs`:
  - layer crates never depend on each other;
  - test-support crates (`memory`, `sim`, `testkit`) may only be
    dev-dependencies of layer crates.
- **Send futures.** Every spec trait method returns `impl Future + Send`.
- **Store writes the spec already defines:**
  - L3: `IdentityResolver::{merge, unmerge, rename}`, `ClaimStore::record`,
    `ActivityStore`;
  - L5: `ChannelRegistry::{declare, set_policy, promote}`,
    `TransmissionVerdicts::set`;
  - L6: `TopicCatalog::{pin, unpin, enforce_retention}`,
    `ProjectionStore::*`, `AlertRuleStore`, `AlertTriage::triage`;
  - L7: `EdgeStore::{apply, apply_access, judge, activate, drop_version,
    advance_watermark}`;
  - L2: `DeadLetterStore`, `BlobStore`.
- **Writes still outside the spec.** The writes the spec gives no method
  are the memory crate's seeding traits:
  - `SeedAgents::{create, advance, attach}`;
  - `SeedChannels::{discover, add_resource, record_access, set_detection,
    confirm}`;
  - `SeedTransmissions::put`;
  - the catalog's fit lifecycle and the alert store's listing, which are
    inherent methods.

  The coordinator describes these as becoming the P0.6 spec write traits,
  with time always passed in (`at`, `now`). At `75573db` there is no "P0.6"
  in the tree, and the channel-semantics change (INV-850..869 there) has
  not been ported: `SeedChannels::discover` still takes `first_access`.
  The design below names the operations, not their final signatures.
- **The surface.** `crates/surface` (P2.6) is still an empty library. It
  will implement `QueryApi`, `OperatorActions`, `LiveFeed` and export over
  the L3–L7 store traits, with an in-process constructor in `crosstalk-api`
  over the memory stores.
- **`crosstalk-sim`.** Virtual clock, seeded RNG, faults, a deterministic
  simulation driver.
- **`crosstalk-testkit`.** Builders, a recorded harness corpus, fake
  upstreams.

### Decisions (from the coordinator and the gateway team)

- **Seed through the write traits.** Scenarios are seeded through the
  spec's write traits, not through a fixture or store backdoor. One suite,
  generic over reads and writes, then runs unchanged against the memory
  stores, Postgres and the HTTP client.
- **The fixture becomes data plus a clock.** It becomes `crates/world`
  (`crosstalk-world`), a TestSupport crate holding a seed script. The
  script writes the synthetic world through the same write traits into the
  memory stores.
  - Its own `QueryApi`, `OperatorActions` and `LiveFeed` implementations
    retire.
  - Reads go through `crates/surface`.
  - That port is under way on `feat/world-seed`.
- **End-to-end comes later.** Wire traffic replayed through `sim` and
  `testkit` (ingress → canonical → stores) is a later layer.

### The harness, redesigned

```rust
/// Writes a world through the spec's write traits.
pub trait Stores: Send + Sync {
    type Agents: IdentityResolver + AgentDirectory + ClaimStore + ActivityStore + AgentWrites;
    type Channels: ChannelRegistry + ChannelDirectory + ChannelWrites;
    type Transmissions: TransmissionVerdicts + TransmissionWrites;
    type Edges: EdgeStore;
    type Catalog: TopicCatalog + CatalogWrites;
    type Rules: AlertRuleStore + AlertTriage;
    type Letters: DeadLetterStore;
    type Blobs: BlobStore;
    // one accessor per store
}

pub trait Harness: Send + Sync {
    type Stores: Stores;
    type Surface: QueryApi + OperatorActions + LiveFeed + Send + Sync;
    type Hasher: RowHasher;
    /// Fresh, empty stores (a memory set, or a fresh Postgres schema).
    fn stores(&self, knobs: &Knobs) -> impl Future<Output = Result<Self::Stores, ProvisionError>> + Send;
    /// The surface over those stores: crosstalk-surface in process, or an
    /// HTTP client in front of crosstalk-api serving them.
    fn surface(&self, stores: &Self::Stores) -> impl Future<Output = Result<Self::Surface, ProvisionError>> + Send;
    fn operators(&self) -> Operators;
    fn row_hasher(&self) -> Self::Hasher;
}
```

Here `*Writes` stands for the P0.6 traits (today's `Seed*`), whatever their
final names.

Several things change against the current harness:

- **`provision` moves into the suite.** It becomes generic: a `Seeder`
  writes a `Scenario` through `Stores` and returns the `Bindings` from the
  ids it created. The harness only supplies empty stores and a surface over
  them, so no implementation ever writes scenario code.
- **`bucket_width` and `now` leave the harness.** The width comes from
  `EdgeStore::bucket_width`. Time comes from the seeder's own clock, since
  stores never read one.
- **The extent is computed, not reported.** It follows from the scenario's
  timeline and the bucket width.
- **The watermark is set explicitly.** After seeding, the seeder calls
  `EdgeStore::advance_watermark` with a frontier past every settled fact.
  "Settled before the watermark" then holds by construction.
- **`Knobs` stays.** `LiveConfig` and `ExportLimits` are surface config.
  The harness builds the surface with them.

### Scenarios as write-trait calls

The vocabulary is unchanged; each fact gains a seeding rule. Time enters
through a `Timeline`:

- Each scenario places its facts at offsets from an anchor, by default in
  whole buckets before the watermark.
- `Timing::ConfirmedInLaterBucket` puts the opening and the confirmation in
  adjacent buckets.
- The seeder turns offsets into the `at` and `now` every write takes.

| Fact | Writes, in order |
| --- | --- |
| `AgentFact` | `AgentWrites::create` (registered or provisional), `advance` to established; for a seen agent, `ClaimStore::record` for each claimed family and an activity record at its first fact |
| `ResourceFact` | Nothing on its own: stored by the first access, discovery or declaration that needs it |
| `ChannelFact::Declared` | `ChannelRegistry::declare(pattern, …, at)` |
| `ChannelFact::Discovered` | Nothing directly: the channel is created by its first cross-agent transmission (`ChannelRegistry::discover(resource, transmission, at)` after the INV-850..869 port). The binding is the id `discover` returns. |
| `AccessFact` | `ChannelWrites::record_access` and `EdgeStore::apply_access` (the access lands on no channel when its resource has none) |
| `TransmissionFact` | The write and read accesses (for a route through a resource), `TransmissionWrites::put` with the state the evidence names. For a confirmed one: `EdgeStore::apply` with its contribution under the active version, and the channel's confirmation (`confirm`, or what the port makes of it). For co-access evidence: the transmission put in `AwaitingContent` / `Suspected` / `Discarded`. |
| `MergeFact` | `IdentityResolver::merge` (author `Resolver` or `Operator`); when reverted, `unmerge` |
| `PromotionFact` | `ChannelRegistry::promote` |
| `PolicyFact` | `ChannelRegistry::set_policy` per decision |
| `VerdictFact` | `TransmissionVerdicts::set` per verdict, then `EdgeStore::judge` with the revision it returned |
| `BodyDroppedFact` | `BlobStore` deletes the body after the transmission's evidence is stored |
| `TopicHistoryFact` | The catalog's fit lifecycle for three versions (fit, ready, activated), assignments, the lineage, `EdgeStore::activate` and `drop_version` for the dropped one, `enforce_retention` |
| `StaleRuleFact` | `AlertRuleStore` create on the older version before the re-fit; the re-fit's `topic_version_ready` remap leaves it stale |
| `DeadLetterFact` | `DeadLetterStore` writes in *n* groups |

**What follows from seeding through writes.**

- **The self-check becomes stronger.** The listings, redirects and counts
  it checks are now computed by the implementation from the writes, rather
  than chosen by a binder.
- **Alerts come from triage.** Any alert a test needs (`NewChannel` on
  discovery, a transmission alert) comes from `AlertTriage::triage`, called
  by the seeder exactly as the alerts consumer would call it.
- **Named scenarios are shared.** They stay in `crosstalk-conformance`, and
  `crosstalk-world` uses them: its seed script composes them with its bulk
  generated week, so the UI's world contains every conformance case.

### What carries over, and what changes

**Carries over unchanged, or nearly:**
- Roles, the facts and their validation, `Scenario::compose`, and the
  named scenarios.
- `Bindings`; the seeder fills them with the ids it created.
- The self-check (`check::observable`).
- The 64 tests: they use only L8 reads and actions, roles, and the support
  layer.
- `World` (its fields come from the seeder instead of the harness),
  `collect`, `reads`, `windows`.
- `suite!`, `run`, `Callers`, `Knobs`.

**Changes:**
- **The harness.** `Harness::provision` is replaced by the
  `stores` + `surface` pair and the suite's `Seeder`; `bucket_width`,
  `now` and `extent` go.
- **The fixture's binder retires.** `fixture/src/conformance/` goes with
  `FixtureBackend`'s trait implementations.
- **Invariant citations are renumbered** INV-746..765 → INV-850..869,
  following crosstalk-impl's map.
- **The crate joins the workspace** as `crates/conformance`, and
  `crosstalk-world` as `crates/world`. Both are registered as TestSupport
  in `Role::of` of the architecture test.
  - They are dev-dependencies of `surface`, `api` and `client`.
  - The suite's own run (memory stores and `crosstalk-surface`) lives in
    `crates/surface`'s tests, where the composition is legal.
  - `crates/conformance` depends on `memory` only as a dev-dependency.
- **The unported areas** are written against the new base: agents and
  governance, actions and permissions, audit, triage and alerts, promotion,
  rules, topics, transmissions and search, lists, live, export.

### How the fixture fits

**Options.**
1. The fixture implements the write traits itself.
2. The fixture becomes a seeded wrapper over the memory stores.

**Recommendation: (2), which is what the coordinator and crosstalk-impl
decided.** `crosstalk-world` is data plus a clock:
- a seed script writing the generated week through the write traits into
  the memory stores;
- reads and actions served by `crosstalk-surface` over them.

**Reasons:**
- **One implementation of the semantics.** With (1), the fixture's 10k
  lines of hand-written read semantics stay a second implementation that
  can drift. With (2) the UI, the suite and the gateway read through the
  same surface code.
- **The seed script is a test of its own.** Every row of the world goes
  through the stores' checks.
- **The world shares the conformance cases.** The UI's world contains every
  named scenario, because the script composes them.

**What the fixture keeps.**
- Its deterministic generator: names, themes, text, the weekly curve.
- Its clock: a fixed one for tests, a live one for serving.
- Its fixture-specific tests: world shape, determinism, generation speed.

The UI's two gap traits (`Present`, `ExportFormats`) are answered by the
surface service or its config once the spec adds them.

### End-to-end layer (later)

Scenarios will also be expressible as wire traffic, compiled from the same
facts into `testkit` exchanges:
- an assistant turn with a tool call that writes a resource;
- a later turn of another agent whose tool result reads it, carrying the
  text.

`sim` then drives that traffic through ingress → canonical → the pipeline
into the stores, and the same suite runs against the surface over them. The
facts and the self-check are shared, so the same test proves the whole
pipeline derives what the write-level seeding asserted directly.

## How to run it against a new implementation

For the gateway's developers.

**Today, on this branch's API.**

1. Depend on `crosstalk-conformance` as a dev-dependency.
2. Implement `Harness` in your test support:
   - `provision` builds a fresh backend and writes each fact of the
     scenario. Use your write path for each fact, as in the table above.
     Return `Bindings`, with `bindings.bind(role, id)` for every role,
     using the ids your stores assigned.
   - `operators` names two operators your directory defines.
   - `bucket_width`, `now` and `extent` answer from your edge store and
     clock.
   - `row_hasher` returns your BLAKE3 row hasher.
3. In a test module, call
   `crosstalk_conformance::suite!(path::to::YourHarness::new())`.
4. Run `cargo test`. The `scenarios::*` tests fail first if a fact is not
   observable as the spec says. Fix those before reading other failures.

**After the redesign.**

1. Implement `Stores`: hand the suite your empty stores.
2. Implement `Harness::surface`: hand it your surface over them.
3. Call `suite!`. The suite seeds every scenario itself, through the write
   traits.

To add a case:
1. Add a named scenario as facts.
2. Add a self-check rule if the case introduces a new kind of fact.
3. Write tests against roles, citing the invariant each assertion checks.
