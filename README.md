# crosstalk

A gateway that sits between AI agent harnesses (Claude Code, Codex and others) and the inference APIs they call. It forwards every request unchanged and watches for one agent's output showing up in another agent's input: in a tool result, a user turn or a system prompt. From that it aims to:

- detect agent-to-agent communication;
- discover the channels agents use, including undeclared ones, such as a public wiki that agents start writing to and reading from;
- record who talked to whom, through what, and what was said.

An operator UI turns those records into a topology graph, a channel-centred view of who reads and writes each channel, evidence pages, topics and alerts.

## Status

This is an early-stage project. The design is fully specified, detection runs end to end, and it is measured against a ground-truth benchmark on synthetic agent traffic. Its accuracy on real agent traffic has not yet been measured.

| Area | State |
|---|---|
| Specification (`spec/`) | Done. Types, per-layer traits and invariants, plus the JSON wire contract with golden files. `scripts/inv_check.py` validates the invariants. |
| Capture and serving (`crosstalk` binary) | Working. Reverse proxy for Anthropic Messages over HTTP/SSE; Claude Code pointed at it through `ANTHROPIC_BASE_URL` works unchanged, including with Claude Code's OAuth login. `serve --role all` runs the proxy, the detection pipeline and the HTTP API together. |
| Transport, store, blob store | Working. In-process bus with retries and dead letters. Every detection layer has a Postgres store with its own migrations; wiring them into the gateway, with restart recovery and an on-disk capture spool for Postgres outages, is in progress. |
| Detection (L3–L7: reconstruct, provenance, flow, analysis, topology) | Wired end to end through `Live`. On the demo swarm benchmark, the headline scenario scores precision 1.000 and recall 1.000, and the template-heavy boilerplate scenario recall 1.000 and precision 0.927 (68.5 false positives per 1,000 exchanges). |
| Surface service (L8) | Working, in process and over HTTP (`api.listen`, bearer token). It covers queries, operator actions, the live feed and export, and passes all 65 tests of the L8 conformance suite. |
| Evaluation | Moving to the standalone benchmark [a2a-transmission-bench](#benchmark-a2a-transmission-bench). `crates/eval` keeps `ct-bench-detect`, the adapter that runs crosstalk as a detector for it; `ct-eval`'s own scoring is being retired. |
| Operator UI (`ui/`) | Runs against a live gateway over HTTP, or on a synthetic fixture world (optionally in replay mode). Includes follow mode for the overview and topology, and a per-agent conversation view. |
| Deployment (`deploy/`) | Docker Compose stack with the gateway, the UI, Postgres, Grafana, Prometheus, Loki and Alloy. A demo swarm sends synthetic agent traffic with known ground truth, and `run.sh bench` scores the gateway's detections against it with regression gates. |

### Since the hackathon demo

The hackathon submission and demo were made from `staging` at [`79dd38b`](https://github.com/RhizoNymph/crosstalk/commit/79dd38b), pushed on 2026-10-04 at 17:52 PDT. At that point the capture path, flow detection (L5) and L3 reconstruct were on `staging`. The demo was the operator UI replaying a scripted fixture world, so it showed what an operator would see but ran no detection code.

Since then (321 commits, PRs #23 to #118):

- **Detection wired end to end.** L4 provenance, L6 analysis and L7 topology landed, composed with L3 and L5 in `Live`. The e2e tests confirm that one agent's output read by another becomes a transmission and a channel edge.
- **Detection measured.** `run.sh bench` drives the demo swarm through the real gateway and scores the export against the swarm's ground truth, with regression gates and a held-out split. Fixes to L4 match quality (template and boilerplate handling, short broadcasts) took the headline precision of the first live run from 0.175 to 1.000.
- **Operator UI.** Merged into `staging` (#23), connected to the gateway over HTTP, with a gateway-unreachable page, follow mode, the conversation view and an export of unconfirmed transmissions.
- **Surface over HTTP.** The real L8 surface passes the full conformance suite, in process and over HTTP.
- **Postgres stores.** A Postgres store for every layer (transport, reconstruct, provenance, flow, analysis, topology, surface), with durability tests.
- **Claude Code OAuth.** Claude Code logged in with OAuth works through the proxy.
- **Operations.** Grafana panels and alerts for the gateway's capture spool.
- **The benchmark split out** into its own repository, described next.

### Benchmark: a2a-transmission-bench

[a2a-transmission-bench](https://github.com/RhizoNymph/a2a-transmission-bench) is part of this work: a detector-neutral benchmark for agent-to-agent transmission detection, built after the demo so that crosstalk is measured by code it does not share. It converts public multi-agent datasets (SALT-NLP, AgentDojo, τ²-bench, AI Village, collusion-wiki and swarm traces, among others) and crosstalk's demo swarm runs into labelled corpora, scores any detector's predictions against them with a naive reference matcher as the baseline, and keeps a held-out split. Crosstalk depends on its format crate through the pinned tag `a2a-bench-format-v1.0.0`.

The step-by-step plan and current progress are in [`docs/roadmap.md`](docs/roadmap.md).

## Layout

```
spec/          crosstalk-spec: the shared types, traits, invariants and wire goldens
crates/        one crate per layer or tool (crosstalk-<dir>)
  ingress, canonical          L0 proxy and L1 normalizer
  transport, store            bus, blob store, Postgres
  reconstruct, provenance,    L3–L7 detection layers
  flow, topology, analysis
  surface, api, client        L8 surface service and its bindings
  gateway                     the `crosstalk` binary and pipeline composition
  memory, sim, testkit, world reference stores, simulation, fixtures, seeded world
  eval, e2e, demo             bench adapter (ct-bench-detect), end-to-end smoke, demo swarm
sidecar/topics  Python sidecar for topic modelling and layout (UMAP, HDBSCAN)
deploy/        compose stack, Dockerfiles, observability config, run.sh
docs/          overview, roadmap, per-feature docs
```

## Running it

Run all checks (fmt, clippy, tests, docs, invariant validation):

```sh
scripts/check.sh
```

The store tests need `TEST_DATABASE_URL` and skip without it.

Start the full stack with Docker Compose:

```sh
bash deploy/run.sh init   # writes deploy/.env with generated secrets
bash deploy/run.sh up
bash deploy/run.sh urls   # where the proxy, API, UI and Grafana are listening
```

Then point an agent at the proxy, for example `ANTHROPIC_BASE_URL=http://<host>:<proxy port>/anthropic`.

To drive the stack with the synthetic demo swarm:

```sh
bash deploy/run.sh demo up
bash deploy/run.sh demo run
```

The fake upstream, the shared wiki channel and the swarm options are described in [`docs/features/demo.md`](docs/features/demo.md).

## Documentation

- [`docs/OVERVIEW.md`](docs/OVERVIEW.md): what the system does, its subsystems, the data flow between them, and an index of every feature.
- [`docs/roadmap.md`](docs/roadmap.md): the phased plan, milestones and status.
- `docs/features/*.md`: one document per feature, covering scope, data flow, files and invariants.
