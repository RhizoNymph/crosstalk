# crosstalk

A gateway that sits between AI agent harnesses (Claude Code, Codex and others) and the inference APIs they call. It forwards every request unchanged and watches for one agent's output showing up in another agent's input: in a tool result, a user turn or a system prompt. From that it aims to:

- detect agent-to-agent communication;
- discover the channels agents use, including undeclared ones, such as a public wiki that agents start writing to and reading from;
- record who talked to whom, through what, and what was said.

An operator UI turns those records into a topology graph, a channel-centred view of who reads and writes each channel, evidence pages, topics and alerts.

## Status

This is a hackathon-stage project. The design is fully specified; the implementation is partway through a phased roadmap.

| Area | State |
|---|---|
| Specification (`spec/`) | Done. Types, per-layer traits and invariants, plus the JSON wire contract with golden files. `scripts/inv_check.py` validates the invariants. |
| Capture (`crosstalk` binary) | Working. Reverse proxy for Anthropic Messages over HTTP/SSE. Claude Code pointed at it through `ANTHROPIC_BASE_URL` works unchanged, and every exchange is normalized, stored and logged. |
| Transport, store, blob store | Working. In-process bus with retries and dead letters; Postgres harness with per-layer migrations. |
| Surface service (L8) | Working, in process, over reference stores. It covers queries, operator actions, the live feed and export. |
| Detection (L3–L7: reconstruct, provenance, flow, topology) | Specified, with reference in-memory stores. The pipeline stages are being integrated and are **not yet wired end to end** on this branch. |
| Evaluation (`crates/eval`, `ct-eval`) | Converts public multi-agent datasets (SALT-NLP first) into labelled corpora and scores detectors against them. Ships a naive reference matcher as the baseline the pipeline has to beat. |
| Operator UI (`ui/`) | Builds as part of the workspace. It runs on a synthetic fixture world, optionally in replay mode, or on the seeded world through the in-process surface service. It is not yet connected to live detection output. |
| Deployment (`deploy/`) | Docker Compose stack with the gateway, Postgres, Grafana, Prometheus, Loki and Alloy. A demo swarm sends synthetic agent traffic through the proxy without spending real tokens. |

The demo shows what an operator sees. Its data is scripted fixture data, not the output of live detection. The step-by-step plan and current progress are in [`docs/roadmap.md`](docs/roadmap.md).

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
  eval, e2e, demo             evaluation harness, end-to-end smoke, demo swarm
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
