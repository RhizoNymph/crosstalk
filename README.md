# crosstalk

A gateway that sits between AI agent harnesses (Claude Code, Codex and others) and the inference APIs they call. It forwards every request unchanged and watches for one agent's output showing up in another agent's input: in a tool result, a user turn or a system prompt. From that it aims to:

- detect agent-to-agent communication;
- discover the channels agents use, including undeclared ones, such as a public wiki that agents start writing to and reading from;
- record who talked to whom, through what, and what was said.

An operator UI turns those records into a topology graph, a channel-centred view of who reads and writes each channel, evidence pages, topics and alerts.

## Status

This is an early-stage project. The design is fully specified, and detection now runs end to end; what remains is measuring how well it works.

| Area | State |
|---|---|
| Specification (`spec/`) | Done. Types, per-layer traits and invariants, plus the JSON wire contract with golden files. `scripts/inv_check.py` validates the invariants. |
| Capture and serving (`crosstalk` binary) | Working. Reverse proxy for Anthropic Messages over HTTP/SSE; Claude Code pointed at it through `ANTHROPIC_BASE_URL` works unchanged. `serve --role all` runs the proxy, the detection pipeline and the HTTP API together. |
| Transport, store, blob store | Working. In-process bus with retries and dead letters; Postgres harness with per-layer migrations. |
| Detection (L3–L7: reconstruct, provenance, flow, analysis, topology) | Wired end to end through `Live`. In the e2e smoke test, one agent's output read by another becomes a confirmed transmission and an A→B channel edge. Its accuracy on real traffic has not yet been measured. |
| Surface service (L8) | Working, in process and over HTTP (`api.listen`, bearer token). It covers queries, operator actions, the live feed and export. |
| Evaluation (`crates/bench-adapter`, `ct-bench-detect`) | The datasets, labels, scoring and gates live in the [a2a-transmission-bench](https://github.com/RhizoNymph/a2a-transmission-bench) (SALT-NLP, AgentDojo, τ²-bench, AI Village, collusion-wiki, the demo swarm). `ct-bench-detect` is crosstalk's detector for it: the live composition over a bench input directory, and saved node0 bench runs as bench inputs and predictions. |
| Operator UI (`ui/`) | Runs on a synthetic fixture world (optionally in replay mode) or on the seeded world. Connecting it to a running gateway over HTTP is in progress. |
| Deployment (`deploy/`) | Docker Compose stack with the gateway, Postgres, Grafana, Prometheus, Loki and Alloy. A demo swarm sends synthetic agent traffic with known ground truth, and `run.sh bench` scores the gateway's detections against it. |

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
