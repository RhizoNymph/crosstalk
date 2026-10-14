# crosstalk

A gateway that sits between AI agent harnesses (Claude Code, Codex and others) and the inference APIs they call. It forwards every request unchanged and watches for one agent's output showing up in another agent's input: in a tool result, a user turn or a system prompt. From that it aims to:

- detect agent-to-agent communication;
- discover the channels agents use, including undeclared ones, such as a public wiki that agents start writing to and reading from;
- record who talked to whom, through what, and what was said.

An operator UI turns those records into a topology graph, a channel-centred view of who reads and writes each channel, evidence pages, topics and alerts.

## Status

This is an early-stage project. The design is fully specified and detection runs end to end. Detection is measured against a ground-truth benchmark on synthetic agent traffic, including held-out runs it was never tuned on, and the gateway keeps capturing through database outages and network partitions ([Results](#results)). Its accuracy on real agent traffic has not yet been measured ([Known limitations](#known-limitations)).

| Area | State |
|---|---|
| Specification (`spec/`) | Done. Types, per-layer traits and invariants, plus the JSON wire contract with golden files. `scripts/inv_check.py` validates the invariants. |
| Capture and serving (`crosstalk` binary) | Working. Reverse proxy for Anthropic Messages over HTTP/SSE; Claude Code pointed at it through `ANTHROPIC_BASE_URL` works unchanged, including with Claude Code's OAuth login. `serve --role all` runs the proxy, the detection pipeline and the HTTP API together. |
| Transport, store, blob store | Working. The gateway runs every detection layer on its own Postgres store (the default in the compose stack) over a durable Postgres bus. Capture goes through an on-disk spool, so it keeps going while the database is down. Restarts recover in place, and a lost pipeline lock is resumed inside the process. An in-memory mode remains for tests and the benchmark adapter. |
| Detection (L3–L7: reconstruct, provenance, flow, analysis, topology) | Wired end to end through `Live`. On held-out demo swarm runs, the headline scenario scores recall 1.000 and precision 1.000, and the template-heavy boilerplate scenario recall 1.000 and precision 0.890 ([Results](#results)). |
| Surface service (L8) | Working, in process and over HTTP (`api.listen`, bearer token). It covers queries, operator actions, the live feed and export, and passes all 65 tests of the L8 conformance suite. |
| Evaluation | Moving to the standalone benchmark [a2a-transmission-bench](#benchmark-a2a-transmission-bench). `crates/eval` keeps `ct-bench-detect`, the adapter that runs crosstalk as a detector for it; `ct-eval`'s own scoring is being retired. |
| Operator UI (`ui/`) | Runs against a live gateway over HTTP, or on a synthetic fixture world (optionally in replay mode). Includes follow mode for the overview and topology, and a per-agent conversation view. |
| Deployment (`deploy/`) | Docker Compose stack with the gateway, the UI, Postgres, Grafana, Prometheus, Loki and Alloy. A demo swarm sends synthetic agent traffic with known ground truth, and `run.sh bench` scores the gateway's detections against it with regression gates. |

### Since the hackathon demo

The hackathon submission and demo were made from `staging` at [`79dd38b`](https://github.com/RhizoNymph/crosstalk/commit/79dd38b), pushed on 2026-10-04 at 17:52 PDT. At that point the capture path, flow detection (L5) and L3 reconstruct were on `staging`. The demo was the operator UI replaying a scripted fixture world, so it showed what an operator would see but ran no detection code.

Since then (586 commits, PRs #23 to #129):

- **Detection wired end to end.** L4 provenance, L6 analysis and L7 topology landed, composed with L3 and L5 in `Live`. The e2e tests confirm that one agent's output read by another becomes a transmission and a channel edge.
- **Detection measured.** `run.sh bench` drives the demo swarm through the real gateway and scores the export against the swarm's ground truth, with regression gates and a held-out split. Fixes to L4 match quality (template and boilerplate handling, short broadcasts) took the headline precision of the first live run from 0.175 to 1.000.
- **Operator UI.** Merged into `staging` (#23), connected to the gateway over HTTP, with a gateway-unreachable page, follow mode, the conversation view and an export of unconfirmed transmissions.
- **Surface over HTTP.** The real L8 surface passes the full conformance suite, in process and over HTTP.
- **Postgres mode.** A Postgres store for every layer (transport, reconstruct, provenance, flow, analysis, topology, surface), wired into the gateway. It has restart recovery, a capture spool for database outages, and a single-writer pipeline lock that is resumed in process when lost. Restart, outage, partition and conformance suites cover it, and fault runs on the deployed stack confirm it.
- **Claude Code OAuth.** Claude Code logged in with OAuth works through the proxy.
- **Operations.** Grafana panels and alerts for the gateway's capture spool.
- **The benchmark split out** into its own repository, described next, and a held-out release scored by it.

### Benchmark: a2a-transmission-bench

[a2a-transmission-bench](https://github.com/RhizoNymph/a2a-transmission-bench) is part of this work: a detector-neutral benchmark for agent-to-agent transmission detection, built after the demo so that crosstalk is measured by code it does not share. It converts public multi-agent datasets (SALT-NLP, AgentDojo, τ²-bench, AI Village, collusion-wiki and swarm traces, among others) and crosstalk's demo swarm runs into labelled corpora, scores any detector's predictions against them with a naive reference matcher as the baseline, and keeps a held-out split. Crosstalk depends on its format crate through the pinned tag `a2a-bench-format-v1.0.0`.

The step-by-step plan and current progress are in [`docs/roadmap.md`](docs/roadmap.md).

## Results

All results below are on synthetic traffic: crosstalk's demo swarm, whose agents read and write a shared wiki through the real gateway, with known ground truth. They are scored by [a2a-transmission-bench](#benchmark-a2a-transmission-bench), not by crosstalk's own code.

The numbers describe the build tagged [`post-2026-10`](https://github.com/RhizoNymph/crosstalk/tree/post-2026-10) (staging `5295fdd`). Changes merged after that tag are not covered by them until the next measurement.

### Held-out detection

Six demo swarm runs on seeds no development run used (1000001–1000006). They were saved without scoring and never inspected on crosstalk's side, then scored once by the bench, against its naive reference matcher. The full report, with provenance and the commands to reproduce it, is the bench's [release results](https://github.com/RhizoNymph/a2a-transmission-bench/blob/main/docs/results/release-2026-10.md).

| Scenario (3 runs each) | Detector | Recall | Precision | False positives per 1,000 exchanges | Runs passing every gate |
|---|---|---:|---:|---:|---|
| headline | crosstalk | 1.000 (153/153) | 1.000 | 0.0 | 3/3 |
| headline | reference | 1.000 (153/153) | 0.927 | 15.9 | (no gates) |
| boilerplate | crosstalk | 1.000 (139/139) | 0.890 | 96.1 | 2/3 |
| boilerplate | reference | 1.000 (139/139) | 0.044 | 19,997.3 | (no gates) |

`headline` traffic writes wiki pages with distinctive text. `boilerplate` fills them with shared template sentences, the hard case for content matching. One boilerplate run failed the false-positive gate (115 per 1,000), which was set from development runs before these were made.

These runs used staging [`7d678e2`](https://github.com/RhizoNymph/crosstalk/commit/7d678e2) with the gateway in memory mode, scored by bench tag `a2a-bench-v1.0.1`. The tagged build has the same detection, plus one fix: it closes a rare race in which a match and a classification landing on one transmission at the same moment could lose one of them. The fix can only keep matched content that the held-out build might have dropped. It never changes which transmissions are found or how they are attributed.

The same release also scores crosstalk on public multi-agent datasets (SALT, AgentDojo, τ², AI Village and others). crosstalk was developed and tuned on those, so they are development numbers, not held-out ones.

### Durability in Postgres mode

Fault runs on 2026-10-14 on the deployed compose stack in Postgres mode, at the same content as staging `d6d69bc`. Each is one boilerplate demo swarm run (one headline run for the restart), with a fresh database and no manual intervention, scored by `a2a-bench-v1.1.1`. Faults were injected mid-run: a 30 s silent network partition between the gateway and Postgres (`docker network disconnect`), or Postgres stopped for 30 s.

| Fault | Requests refused by the proxy | Gateway exits | Pipeline running again | Dead letters | Detection gates |
|---|---|---|---|---|---|
| Partition, 30 s | 0 of 245 | 0 | 46 s after the fault | 0 | pass |
| Partition, 30 s | 0 of 238 | 0 | 46 s | 0 | pass |
| Partition, 30 s | 0 of 256 | 0 | 45 s | 0 | false positives 121.1 against 115 |
| Postgres stopped, 30 s | 0 of 252 | 0 | 39 s | 0 | pass |
| Gateway restarted (`docker restart`) | 66 of 310, over 9.8 s | 1 (the restart) | 12 s | 0 | pass (headline 1.000 / 1.000) |

During a fault the proxy keeps forwarding and the capture spool holds what it captured (up to about 90 exchanges here). The spool drains within a few seconds of the database answering. In every partition the gateway noticed it had lost its pipeline lock before Postgres released it, so no second gateway could have taken the lock while this one still ran its pipeline. Most of the third partition's false positives came from reads before the fault, so its gate miss reflects the boilerplate scenario's run-to-run variance, not the partition.

## Known limitations

- **Real traffic is unmeasured.** Every number above comes from synthetic or dataset traffic. How crosstalk does on real agents' traffic has not been measured.
- **Template-heavy pages.** In the boilerplate scenario crosstalk reports roughly 60 to 140 false positives per 1,000 exchanges, varying from run to run, so its gates (115 per 1,000, precision 0.86) fail on some runs. One known source: when a page is rewritten from shared template text, a read of the new version can also be credited to the writer of an older version, through text both versions kept.
- **An occasional dropped read.** In about 2 of 12 development runs, a live read of a wiki page did not reach flow detection, so a channel transmission was reported as direct messages instead. Replaying the same capture offline detects it correctly.
- **Planned restarts.** A restart (SIGTERM) closes the proxy while the old process drains its open streams and the new one starts: about 10 s in the run above. Outages and partitions do not close it.
- **One gateway per database.** The pipeline lock lets a second gateway wait as a standby. If the lock's session is reset while the database stays reachable, two gateways could overlap for up to 5 s. The compose stack runs one gateway.

Fixes to detection will be judged on fresh development runs and then on a new held-out release, not on the runs above.

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
