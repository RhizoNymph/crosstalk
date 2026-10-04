# Workspace

The Cargo workspace that holds the spec and every implementation crate, the
rule for which crate may depend on which, the checks every change runs, and
the convention that ties invariant evidence to crates. Roadmap items P1.1,
P1.2 and P0.5.

## Scope

- The root virtual workspace: members, shared package fields, lints and
  dependency pins, and the one lockfile.
- The crate layout under `crates/`, one library per roadmap layout entry.
- The dependency rule between crates, and the architecture test that
  enforces it.
- `scripts/check.sh` (every check, in order) and `scripts/inv_check.py` (the
  invariant validator).
- The evidence path convention for invariants.

## Non-scope

- Any implementation. Every crate under `crates/` started as an empty
  library whose crate doc says what it will implement; the per-layer
  roadmap items fill them (`crosstalk-transport`: [transport](transport.md);
  `crosstalk-canonical`: [canonical](canonical.md); `crosstalk-gateway`:
  [gateway](gateway.md)).
- The UI (`ui/`), which is excluded from the workspace and keeps its own
  package and lock.
- CI configuration. `scripts/check.sh` is what CI runs once it exists.

## Layout

```text
Cargo.toml                 virtual workspace: members spec + crates/*, excludes ui, resolver 3
Cargo.lock                 the one lock for every member
rust-toolchain.toml        nightly-2026-10-02 with rustfmt, clippy, miri
spec/                      crosstalk-spec: types, invariants, wire goldens (the boundary crate)
crates/
  sim/                     crosstalk-sim          P1.3   virtual clock, seeded RNG, faults, DST driver
  testkit/                 crosstalk-testkit      P1.4   builders, recorded corpus, fake upstreams
  memory/                  crosstalk-memory       P2.3   in-memory reference store implementations
  store/                   crosstalk-store        P1.5   Postgres pool, per-layer migrations, test DB
  transport/               crosstalk-transport    P2.1/2 L2: bus, dead letters, blob store
  ingress/                 crosstalk-ingress      P2.4   L0: proxy, routing, framers
  canonical/               crosstalk-canonical    P2.5   L1: normalizers
  reconstruct/             crosstalk-reconstruct  P4.1   L3: identity, merges, threading
  provenance/              crosstalk-provenance   P4.2   L4: segmenter, decoders, fingerprints
  flow/                    crosstalk-flow         P5     L5: resources, channels, correlator, verdicts
  analysis/                crosstalk-analysis     P6.2/3 L6: embeddings, topics, search, alerts
  topology/                crosstalk-topology     P6.1   L7: edges, graphs, series, watermark
  surface/                 crosstalk-surface      P2.6   L8: QueryApi, actions, live feed, export
  api/                     crosstalk-api          P7.1   HTTP + SSE server for the L8 surface
  client/                  crosstalk-client       P7.2   L8 traits over HTTP, for the UI
  gateway/                 crosstalk-gateway      P3     the `crosstalk` binary; wiring
scripts/
  check.sh                 every workspace check
  inv_check.py             the invariant validator
```

Every crate is `crosstalk-<dir>` (library `crosstalk_<dir>`), inherits
`version`, `edition` (2024) and `publish = false` from `[workspace.package]`
and the lints from `[workspace.lints]`, and depends on `crosstalk-spec` by
path. Each `lib.rs` holds the crate doc (what it implements, the spec
interface module, the roadmap item), `use crosstalk_spec as _;` so the
declared dependency is not reported unused before code uses it, and an
empty `#[cfg(test)] mod tests {}`. `crosstalk-gateway` also has the binary
target `crosstalk` (`crates/gateway/src/main.rs`).

## Manifests

| File | Role |
| --- | --- |
| `Cargo.toml` | `[workspace]`: `members = ["spec", "crates/*"]`, `exclude = ["ui"]`, `resolver = "3"`. `[workspace.package]`: version, edition 2024, `publish = false`. `[workspace.lints]`: `unsafe_code = "forbid"`, clippy at its defaults. `[workspace.dependencies]`: the shared pins in use (`serde = "=1.0.229"` with `derive`, `serde_json = "=1.0.151"`; for `crosstalk-transport` (the bus and the blob store) `blake3 = "=1.8.7"`, `thiserror = "=2.0.21"`, `tokio = "=1.53.1"`, `tracing = "=0.1.44"`, and the test pins `proptest = "=1.11.0"` (std only) and `tempfile = "=3.27.0"`; for the store `sqlx = "=0.9.0"` without default features, with `postgres`, `runtime-tokio`, `migrate` and `macros`, see [store](store.md) for decision D2; for `crosstalk-canonical`, `base64 = "=0.22.1"` plus the shared `blake3`, `serde`, `serde_json`, `thiserror` and `tracing`; for `crosstalk-testkit`, `bytes = "=1.12.1"`, `http-body-util = "=0.1.5"`, `hyper = "=1.11.1"` and `hyper-util = "=0.1.21"`, plus the shared `thiserror`, `tokio` and `tracing`, each the UI's resolved version) |
| `Cargo.lock` | The workspace lock. It replaced `spec/Cargo.lock` and resolves the identical third-party versions (serde 1.0.229, serde_core, serde_derive, serde_json 1.0.151, syn 3.0.6, quote, proc-macro2, unicode-ident, itoa, memchr, zmij), with the same checksums |
| `spec/Cargo.toml` | `crosstalk-spec`; takes serde and serde_json from the workspace pins |
| `crates/<dir>/Cargo.toml` | `crosstalk-<dir>`; `crosstalk-spec` by path, plus the workspace pins its implementation uses (`crates/store`: serde, sqlx, thiserror, tokio, tracing; `crates/testkit/Cargo.toml`: see `docs/features/testkit.md`; `crates/memory/Cargo.toml`: below) |
| `crates/memory/Cargo.toml` | Adds `blake3`, `proptest`, `serde_json`, `thiserror`, `tokio` (`sync`, `rt`, `macros`) and `tracing` from the workspace pins: the reference stores and their exported property harnesses |
| `crates/gateway/Cargo.toml` | The `crosstalk` binary; depends on the layer crates `canonical`, `ingress`, `transport` and on `store`, plus `bytes`, `http-body-util`, `hyper` (client, http1, server), `hyper-util` (tokio), `serde`, `serde_json`, `thiserror`, `tokio` (fs, io-util, macros, net, rt, rt-multi-thread, signal, sync, time), `tracing` and `tracing-subscriber` (env-filter, fmt, json, std, which bring in `matchers`, `regex-automata`, `aho-corasick`, `tracing-serde` and `valuable`); dev: `crosstalk-sim`, `crosstalk-testkit`, `tempfile`; see [gateway.md](gateway.md) |
| `crates/sim/Cargo.toml` | Adds `thiserror`, `tracing` and `tokio` with `macros`, `rt`, `sync`, `time` and `test-util` (paused time); see [sim.md](sim.md) |
| `crates/ingress/Cargo.toml` | The proxy's pins: `blake3`, `bytes`, `flate2 = "=1.1.10"`, `http-body-util`, `hyper` (client, http1, server), `hyper-rustls = "=0.27.10"` (no default features; http1, ring, tls12, webpki-tokio), `hyper-util` (client-legacy, http1, tokio), `serde`, `serde_json`, `thiserror`, `tokio`, `tracing`, `zstd = "=0.13.3"` (no default features); dev: `crosstalk-sim`, `crosstalk-testkit`, `proptest`, `tower-service = "=0.3.3"`, `tracing-subscriber = "=0.3.23"` (fmt, std); see [ingress.md](ingress.md) |

A crate that needs a new third-party dependency adds its exact pin to
`[workspace.dependencies]` (matching the UI's pin where they overlap) and
refers to it with `<name>.workspace = true`. Pins in the workspace table
list only what some member uses.

## Dependency rule

Layer crates talk to each other only through the spec (bus events and spec
traits handed to them at wiring time), and composition happens in the
gateway. The roles:

| Role | Crates |
| --- | --- |
| layer | `ingress`, `canonical`, `transport`, `reconstruct`, `provenance`, `flow`, `analysis`, `topology`, `surface` |
| composition | `api`, `client`, `gateway` |
| test support | `memory`, `sim`, `testkit` |
| open | `spec`, `store`, and every third-party crate |

The rules, applied to every declared dependency (normal, dev and build,
including optional and target-specific ones) of every workspace member:

1. A layer crate never depends on another layer crate, nor on `api`,
   `client` or `gateway`, under any kind, except that it may depend on
   `transport` as a dev-dependency. `transport` is infrastructure as well as
   L2: layers publish through the spec's `EventBus` trait, and use the
   in-process bus only in their tests.
2. `memory`, `sim` and `testkit` are never a normal or build dependency of a
   layer crate; they may be dev-dependencies.

Layer crates may depend freely on `spec`, `store` and third-party crates.
Composition and support crates are unrestricted.

### The architecture test

`crates/gateway/tests/architecture.rs` runs
`cargo metadata --format-version 1 --no-deps --offline` on the workspace
manifest (through `$CARGO`, so it uses the same toolchain as the test) and
parses the JSON with `serde_json`.

- `Role::of` classifies a package name; `DepKind` is `Normal`, `Dev` or
  `Build` (an unknown `kind` is an error, not a default).
- `check(&Edge) -> Option<Violation>` is the rule for one edge, and
  `violations` collects every broken edge. `Violation` is `LayerOnLayer`,
  `LayerOnComposer` or `TestSupportNotDev`, each carrying the edge.
- `workspace_obeys_the_dependency_rule` fails with every violation listed.
- `workspace_has_every_crate_the_rule_names` fails if a crate the rule
  names is missing, so the rule can never pass vacuously after a rename.
- `every_crate_depends_on_the_spec` fails if a member other than the spec
  lacks a normal dependency on `crosstalk-spec`.
- The other tests exercise `check` on hand-built edges: every layer pair
  under every kind, the `transport` dev exception, composers, support
  crates, open crates, and the metadata parser.

## Checks

```sh
scripts/check.sh
CARGO_TARGET_DIR=/tmp/target scripts/check.sh
```

`scripts/check.sh` runs, from the repository root, in order, and stops at
the first failure, printing `check failed at step: <name>`:

| Step | Command |
| --- | --- |
| `fmt` | `cargo fmt --all --check` |
| `clippy` | `cargo clippy --workspace --all-targets -- -D warnings` |
| `test` | `cargo test --workspace` |
| `doc` | `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` |
| `invariants` | `python3 scripts/inv_check.py spec/invariants` |

It respects `CARGO_TARGET_DIR`. It selects the toolchain by exporting
`RUSTUP_TOOLCHAIN` as `rust-toolchain.toml`'s channel unless the caller set
it, so the checks need only that channel with rustfmt and clippy, not every
component the file lists (miri, which no check uses). The invariant step
runs without `--allow-pending`: an unnumbered `INV-X-` file fails it.

## Testing

Tests sit beside the code they test (`#[cfg(test)] mod tests` in each
crate, with `spec/types/tests/` for the spec), and the invariant evidence
paths name them (below). Each evidence kind of
`spec/invariants/README.md` has its tool:

| Kind | How it runs |
| --- | --- |
| `unit`, `property` | plain `#[test]`s (`cargo test --workspace`) |
| `dst` | a scenario under `crosstalk-sim` ([sim.md](sim.md)), declared with `crosstalk_sim::sim_test!` or `crosstalk_sim::sim_test(..)`, in the layer crate's tests with `crosstalk-sim` as a dev-dependency |
| `integration` | against a real Postgres from `crosstalk-store`'s harness (P1.5) |

A `dst` test is an ordinary `#[test]`, so `scripts/check.sh` runs it. It
sweeps a fixed set of seeds (16 by default, `SimConfig::default_seeds`),
so CI is reproducible. Two environment variables change that:

```sh
CROSSTALK_SIM_SEED=1234 cargo test -p crosstalk-flow dst::   # rerun the one failing seed
CROSSTALK_SIM_SEEDS=1000 cargo test -p crosstalk-transport   # sweep seeds 0..1000
```

A failure panics with the seed, the step it reached, the last step label,
the simulated time, the cause, the last trace records and the
`CROSSTALK_SIM_SEED=<n>` command that reproduces it.

Layer code reads time only through the spec's `Clock`
(`crosstalk_spec::support::Clock`) and `tokio::time::Instant`
(`canonical.clock.injected`), so a simulation can drive it: the gateway
wires `SystemClock`, and a test wires `SimClock`.

## Evidence paths

Each invariant's evidence names where its proof lives, by the library name
of the crate that holds it:

- `crosstalk_spec::...` for the spec crate (types, checked constructors and
  `crosstalk_spec::tests::<module>::<fn>`);
- `crosstalk_<crate>::...` for an implementation crate under `crates/`. A
  layer's invariants point at the layer's crate, for example
  `crosstalk_flow::tests::...` or `crosstalk_surface::tests::...`.
- `lint` evidence is a rule name (`lint:<name>`), not a path.

The former single-crate form `crosstalk::<layer>::` was rewritten to
`crosstalk_<layer>::` (805 paths over the 9 layer prefixes) and is refused.

### The validator

`scripts/inv_check.py DIR [--layer PREFIX] [--allow-pending] [--root REPO]`
checks every `INV-*.toml` in `DIR` against `spec/invariants/README.md`, and
additionally:

- every non-`lint` evidence path matches `crosstalk_<crate>::<segment>...`,
  where `<crate>` is `spec` or a directory under `crates/` with a
  `Cargo.toml`;
- every evidence path under `crosstalk_spec::tests::` whose review is
  `agent = "true"` names a test that exists: `fn <name>` in the module file
  the path names (`spec/types/tests/<mods>.rs` or `<mods>/mod.rs`, or the
  nearest enclosing module file for an inline module).

`--root` defaults to two directories above `DIR`. The second-to-last line
reports `<missing> of <checked> reviewed spec test paths name no test fn`,
and the last line `<files> files, <errors> errors`; the exit code is 1 on
any error.

## Invariants and constraints

- One lock for the workspace; `spec/` has no lock of its own.
- Every member inherits `[workspace.lints]`, so `unsafe` is forbidden
  everywhere, and clippy warnings fail `scripts/check.sh`.
- Pins are exact (`=`), shared through `[workspace.dependencies]`, and match
  the UI's where they overlap.
- The dependency rule holds for every declared dependency of every member,
  and the crates the rule names all exist.
- Every member other than the spec depends on `crosstalk-spec`.
- `tokio`'s `test-util` feature (paused time) is enabled only by
  `crosstalk-sim`, which layer crates take only as a dev-dependency, and by
  `crosstalk-transport`'s own dev-dependencies (its `dst` tests), so it
  never reaches a production build.
- No evidence path uses the `crosstalk::<layer>::` form, and every path's
  crate exists.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `Cargo.toml` | The virtual workspace | `[workspace.package]`, `[workspace.lints]`, `[workspace.dependencies]` |
| `Cargo.lock` | The workspace lock | — |
| `rust-toolchain.toml` | Pinned nightly and its components | — |
| `crates/*/Cargo.toml`, `crates/*/src/lib.rs` | One library per layout entry, with its crate doc; empty until its roadmap item fills it (`sim`: [sim.md](sim.md)) | crates `crosstalk_<dir>` |
| `crates/gateway/src/main.rs` | The `crosstalk` binary ([gateway](gateway.md)) | `main` |
| `crates/gateway/tests/architecture.rs` | The dependency rule over `cargo metadata` | `Role`, `DepKind`, `Edge`, `Violation`, `check`, `violations` (test-local) |
| `scripts/check.sh` | Every workspace check, stopping at the first failure | — |
| `scripts/inv_check.py` | The invariant validator | `Repo`, `TestTally`, `check`, `check_paths`, `main` |
