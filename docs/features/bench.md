# Live detection benchmark (`run.sh bench`)

`bash deploy/run.sh bench` runs one scored benchmark of the real gateway on
the single-machine compose deployment (node0). The demo swarm
([demo.md](demo.md)) sends its traffic through the gateway to the fake
upstream and writes ground truth v2. The gateway's detections are then
exported over the L8 API, and `ct-eval swarm` ([eval.md](eval.md#swarm-benchmark))
scores them against the truth, giving precision and recall. The host has no
Rust toolchain, so `ct-eval` ships in the `crosstalk-demo` image and runs as
the compose service `bench`.

**Status:** the running gateway cannot serve detections yet. `serve` neither
binds the L8 API (`api.listen`, port 8081) nor runs the `Live` detection
pipeline. Until it does, `run.sh bench` stops at step 3 with
`the gateway does not expose detections yet (api.listen not bound); see docs/features/bench.md`
and produces no score. See [Pending](#pending) for what is still needed.

## Scope

- `run.sh bench`, implemented in `deploy/bench.sh`: one run from a fresh
  world to a printed score, with the swarm's knobs passed through.
- Two scenarios ([below](#scenarios)): `headline` and `boilerplate`.
- The `bench` compose service (profile `bench`): `ct-eval` with the run
  directory bind-mounted and the gateway's `data` volume mounted read-only.
- `ct-eval` and its gates file in the demo image.
- The run directory `deploy/bench/<run>/` (gitignored).

## Non-scope

- Detection itself, the L8 API and the flow config. Those are the gateway's
  (see [Pending](#pending)); the bench only consumes them.
- Scoring logic. It is `ct-eval swarm`, unchanged.
- Gates for `demo-swarm/<scenario>`. They live in `crates/eval/gates.toml`
  (`detector = "gateway-export"`; see eval.md, Swarm benchmark).
- Comparing runs, trends and dashboards.
- Pruning the exchange log and blobs. They accumulate across runs (below).
- Multi-machine deployments. The bench assumes the gateway, the swarm and
  the scorer share one compose project.

## Running it

On node0, from the repository root, with `deploy/.env` in place
(`bash deploy/run.sh init`):

```sh
bash deploy/run.sh bench                                  # 20 agents, 2m, seed 42; asks before restarting
bash deploy/run.sh bench --yes --agents 50 --duration 5m --seed 7
bash deploy/run.sh bench --yes --claude-code-shape -- --agents-per-key 2 --write-fraction 0.3
bash deploy/run.sh bench --yes --scenario boilerplate     # the shared-boilerplate regression scenario
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--agents N` | 20 | Swarm agents |
| `--duration D` | 2m | Swarm run time (`crosstalk-demo` duration syntax) |
| `--seed N` | 42 | Swarm seed |
| `--scenario S` | headline | `headline` or `boilerplate`; passed to the swarm and recorded in `bench.env` |
| `--claude-code-shape` | off | The swarm's Claude Code request shape |
| `--settle-timeout SECS` | 900 | Give up waiting for Live's watermark to pass the swarm's end after this long |
| `--yes`, `-y` | off | Restart `wiki` and `crosstalk` without asking |
| `-- ...` | | Any further `crosstalk-demo swarm` options, as is |

The exit code is ct-eval's: 0 when every gate passes (or none applies), 2
when a gate fails, 1 for any failure before or during scoring.

### Scenarios

`--scenario` is passed to the swarm (`crosstalk-demo swarm --scenario`),
which ends every agent's system prompt with a style marker; the fake
upstream picks its prose generator from it per request, so one running
upstream serves both and nothing restarts between scenarios
([demo.md](demo.md#scenarios-and-prose-generators)). The truth header
records it (`"scenario"`), as do `bench.env` and the printed headline.

- **`headline`** (default): high-entropy model prose. Unrelated agents'
  outputs share no 32-byte run, so a detection is either a real copy
  through the wiki or a gateway error. Its precision and recall are the
  benchmark's headline numbers.
- **`boilerplate`**: the templated prose earlier runs used. Unrelated
  outputs share template fragments of 30–64 bytes (`Open question: does
  consumer lag interact with …`), as real agents share boilerplate. It is a
  regression scenario for false positives on shared text: the first live
  run on node0 scored precision 0.175 overall (0.951 on the wiki-channel
  path) here, almost every false positive a 34–46 byte exact match between
  different agents' outputs. Compare boilerplate runs with boilerplate
  runs only.

### Holdout runs

The bench (a2a-transmission-bench) keeps a holdout split of demo-swarm runs
that nobody tunes against: they are scored only in the bench's release
runs, which report aggregates. A run scored here (with `report/`) is
development data and can never become a holdout, so holdout runs are made
with `--holdout`:

```sh
bash deploy/run.sh bench --yes --holdout --seed 1000001 --scenario headline
bash deploy/run.sh bench --yes --holdout --seed 1000002 --scenario boilerplate
```

- **Seeds.** Holdout runs need a reserved seed, `1_000_000 + n` for the n-th
  holdout run; every other run must use a seed below 1,000,000. The bench
  refuses either the other way, so a development run cannot spend a holdout
  seed. The seed is recorded in `bench.env` (`seed=`, `holdout=1`).
- **Stops after fetching.** Steps 1 to 7 run as usual (fresh world, swarm,
  watermark, export and evidence, snapshot of the exchange log and blobs);
  step 8 (scoring) does not. There is no `report/`, no `score.txt`, and the
  swarm's report and the fetch output go to `swarm.txt` and `fetch.log`
  only, never the terminal. The bench prints the run id and its file list.
- **Where it goes.** On node0 the run is under `deploy/bench/holdout/<run>/`.
  Copy it to the bench machine's dataset root, outside every git repo,
  never under `bench-runs/`:
  `rsync -a node0:<deploy dir>/bench/holdout/<run>/ ~/Data/ai/agents/demo-swarm-holdout/<run>/`.
- **Same scenarios** as development runs (headline and boilerplate), so the
  holdout measures the same thing.
- Once `ct-bench-detect fetch` (crosstalk #112) lands, a holdout run also
  saves its outputs, as development runs will.

## Data and control flow

```text
run.sh bench
 1 confirm_restart ── prints what restarts; asks on a TTY unless --yes
 2 start_stack ────── compose up -d --build   (base + compose.demo.yaml)
 3 require_detection ── host curl /readyz: tasks `live` and `api` running;
                        host curl GET /operators with the API token: 200
 4 fresh_world ────── compose restart wiki crosstalk; wait until both are healthy; step 3 again
 5 run_swarm ──────── compose run --no-deps swarm --agents … --ground-truth /bench/<run>/truth.jsonl
                        swarm ─▶ crosstalk:8080 ─▶ fake-upstream:8070; tool calls ─▶ wiki:8090
                        crosstalk ─▶ data volume: exchanges/exchange-log.jsonl, blobs/
 6 wait_caught_up ─── host curl <CROSSTALK_BIND>:<CROSSTALK_OPS_PORT>/healthz until
                        live.watermark_micros ≥ the swarm's end
 7 fetch_detections ─ compose run --no-deps bench swarm-fetch --api http://crosstalk:8081
                        --token-env CROSSTALK_API_TOKEN --truth … --out /bench/<run>
                        (POST /exports, GET /transmissions/{id}/evidence)
 8 score ──────────── compose run --no-deps bench swarm --truth … --exchanges /var/lib/crosstalk/exchanges/exchange-log.jsonl
                        --blobs /var/lib/crosstalk/blobs --export … --evidence … --gates … --out /bench/<run>/report
                      prints the `overall:` line, the gates, the result; exits with ct-eval's code
```

Step by step:

1. **Confirm.** The bench prints that it will start the demo stack
   (recreating services whose image or config changed) and restart `wiki`
   and `crosstalk`, losing the wiki's pages and the gateway's detection
   state, while the `data` volume is kept. Without `--yes` it asks; when
   stdin is not a terminal it refuses rather than guess.
2. **Start.** `up -d --build` with the demo override, as `demo up` does. The
   build makes sure the demo image carries `ct-eval`. It then waits for
   `crosstalk` to be healthy.
3. **Fail fast.** From the host, the bench reads the ops listener's
   `/readyz` and requires two running tasks: `live` (every layer stage of
   the Live detection pipeline is running) and `api` (the operator API's
   listener is bound). It then calls `GET /operators` on the published API
   port with `CROSSTALK_API_TOKEN` from `deploy/.env` and requires a 200.
   Without `live` the export would be empty and the score a real but
   meaningless zero; without `api` there is no export. Either stops the run
   with exit 1, before any traffic is sent and before step 4's restarts.
4. **A fresh world.** The wiki lives in memory, so restarting it empties it
   and the run has no `unattributed_read` rows (no page version from before
   the run). Restarting `crosstalk` clears its in-memory detection state, so
   the export holds only this run. Both restarts happen here, at the start,
   and never between the swarm and the export. The bench waits for both
   containers to report healthy (Docker resets health to `starting` on a
   restart), then repeats step 3's checks against the restarted gateway.
5. **Swarm.** The run id is the UTC start time (`20261005T141500Z`). The
   bench creates `deploy/bench/<run>/`, records the scenario, the
   parameters and the image ids in `bench.env`, and runs the `swarm` service once with the run
   directory mounted at `/bench` and `--ground-truth /bench/<run>/truth.jsonl`.
   It runs as the invoking user (`--user $(id -u):$(id -g)`), so the files
   are theirs. The swarm's report is kept in `swarm.txt`. A missing or empty
   `truth.jsonl` stops the run.
6. **Caught up.** The bench records the moment the swarm exits
   (`swarm_end_unix_ms` in `bench.env`) and polls `/healthz` every 10 s
   until Live's `live.watermark_micros` has passed it. Exports are cut at
   the watermark, so before that the export would miss the run's tail. The
   watermark advances only when every layer group is empty, trails the
   clock by `evidence_window_ms + suspected_ttl_ms` and is aligned down to
   the 5-minute bucket: with the demo flow config (10 s + 60 s) it is
   reached about 70 s after the swarm stops, and at worst about 6 minutes
   after. More than `--settle-timeout` seconds stops the run (the
   detections stay in `crosstalk` until it restarts). The last body is kept
   as `healthz.json`.
7. **Fetch.** `ct-eval swarm-fetch` exports the transmissions from the
   truth header's `started_at_unix_ms` onward and fetches each one's
   evidence, with the deployment's API token. It writes `export.jsonl` and
   `evidence.jsonl`. This must happen before anything restarts `crosstalk`.
   The transmissions export holds **confirmed** transmissions (with their
   classifications and aggregates), as the spec defines it; suspected ones
   are not in it. A transmission is confirmed when its read's evidence
   window closes, well before the watermark passes, so a short run loses
   nothing to this.
8. **Score.** `ct-eval swarm` reads the run's truth, export and evidence,
   and the gateway's exchange log and blobs in place on the `data` volume.
   It writes `report/` (`report.json`, `report.txt`, `diagnostics.json`);
   its stdout goes to `score.txt`. The bench prints the headline, for
   example:

   ```text
   bench 20261005T141500Z (headline)
   overall: recall 0.912 (52 / 57), precision 0.963 (52 correct, 2 false, 1 unjudged)
   gates: none apply to demo-swarm
   result: pass
   report: deploy/bench/20261005T141500Z/report/report.txt
   ```

   and exits with ct-eval's code (2 = a gate failed).

### The run directory

```text
deploy/bench/<run>/
  bench.env          run id, scenario, swarm arguments, settle window, image ids
  truth.jsonl        ground truth v2 (the swarm)
  swarm.txt          the swarm's report
  healthz.json       the last /healthz body read while settling
  export.jsonl       the transmissions export (ct-eval swarm-fetch)
  evidence.jsonl     one TransmissionEvidence per exported transmission
  exchange-log.jsonl the gateway's exchange log, copied after the export
  blobs/             the gateway's blob store, copied after the export
  score.txt          ct-eval swarm's stdout
  report/            report.json, report.txt, diagnostics.json
```

The exchange log and blobs make the directory self-contained: it can be
copied elsewhere and re-scored with `ct-eval swarm --truth truth.jsonl
--exchanges exchange-log.jsonl --blobs blobs --export export.jsonl
--evidence evidence.jsonl`. Both accumulate across runs on the `data`
volume, so each copy holds everything so far (blobs are content-addressed
and small); the importer only reads the run's sessions.

### Replaying a run offline

`ct-eval replay --run <dir>` re-runs a saved run through the same `Live`
composition in memory, with the run's `evidence_window_ms` and
`suspected_ttl_ms` from `bench.env`, and scores it like step 8 (see
eval.md, "Replay"). It needs the run's exchange log and blobs: either a
copy beside the run (`exchange-log.jsonl`, `blobs/`) or `--exchanges` and
`--blobs` pointing at the data volume. Only exchanges captured since the
truth header's `started_at_unix_ms` are replayed, which is this run's
traffic since step 4's restart. The replay of a run reproduces its
`score.txt` when built from the commit the gateway ran.

### Why the data volume is mounted, not copied

The `bench` service mounts the `data` volume read-only at
`/var/lib/crosstalk`, the gateway's own path, and `ct-eval swarm` reads
`exchanges/exchange-log.jsonl` and `blobs/` there. Copying them out with
`docker cp` would also work with the distroless gateway, but:

- the exchange log and blobs **accumulate across runs** (a restart does not
  clear them, and nothing prunes them), so every run would copy the whole
  history, most of it irrelevant. The importer joins on the run's own
  sessions (from the truth file), so the older entries do no harm in place;
- a read-only mount cannot disturb the running gateway, and the blob store
  opens a read-only root without writing (the directory already exists);
- the score can be recomputed later from `deploy/bench/<run>/` plus the
  volume, as long as the volume is not removed (`down -v`).

The files on the volume are created `0644` (directories `0755`), so the
bench container can read them as the invoking user. A gateway still
writing the log while it is read is fine: the importer ignores a torn last
line.

### Why `/healthz` is read from the host

The images are distroless (no shell, no curl), and both healthcheck
binaries (`crosstalk healthcheck`, `crosstalk-demo healthcheck`) report only
the status, not the body. Reading the counters from inside the network
would need a new image or a Rust change, so the bench reads the published
ops port from the host with `curl`, at `CROSSTALK_BIND` (or 127.0.0.1 when
that is unset or `0.0.0.0`) and `CROSSTALK_OPS_PORT`. The token check
uses the published API port (`CROSSTALK_API_PORT`) the same way.

## Flow config for the bench

The gateway's Live pipeline reads an optional top-level `flow` section:
`correlation_window_ms`, `evidence_window_ms`, `suspected_ttl_ms`,
`content_retention_ms`, `shards` and `tick_ms`. `deploy/config/crosstalk.json`
spells out the defaults (600 s, 120 s, 1800 s, 30 days, 1, 1 s); `deploy/demo/crosstalk.demo.json` shortens two for
the bench, and `crates/demo`'s `tests::deploy` allows exactly those two (and
the upstream URL) to differ:

| Key | Demo value | Why |
| --- | --- | --- |
| `evidence_window_ms` | 10000 | A transmission confirms 10 s after its read |
| `suspected_ttl_ms` | 60000 | Keeps the watermark (which trails by both) about a minute behind, not half an hour |

`bench.env` records both values for each run.

## Files

| File | Role |
| --- | --- |
| `deploy/run.sh` | Usage text and the `bench` subcommand; sources `deploy/bench.sh` |
| `deploy/bench.sh` | The bench: one function per step (`confirm_restart`, `start_stack`, `require_detection`, `fresh_world`, `run_swarm`, `wait_caught_up`, `fetch_detections`, `score`), plus `bench` (options), `ops_url`, `api_url`, `env_value`, `ready_task`, `healthz_watermark`, `demo_flow_ms`, `wait_healthy` |
| `deploy/compose.demo.yaml` | The `bench` service: the demo image with entrypoint `ct-eval`, profile `bench`, `restart: "no"`, `user: ${BENCH_UID:-65532}:${BENCH_GID:-65532}`, `CROSSTALK_API_TOKEN`, `./bench:/bench` and `data:/var/lib/crosstalk:ro` |
| `deploy/demo.Dockerfile` | Builds `crosstalk-demo` and `ct-eval` in one cargo invocation; ships `ct-eval` at `/usr/local/bin/ct-eval` and `crates/eval/gates.toml` at `/usr/local/share/crosstalk-eval/gates.toml`. The `.dockerignore` already admits `crates/` and `spec/` and excludes only `deploy`, `target`, VCS and editor files, which the build does not need |
| `.gitignore` | `/deploy/bench/` |
| `crates/eval/src/bin/ct-eval/swarm.rs` | `ct-eval swarm` and `swarm-fetch`, used unchanged |
| `crates/eval/src/bin/ct-eval/replay.rs` | `ct-eval replay`: a saved run re-scored offline through `Live` (not run by the bench) |

`ct-eval`'s default gates path is compiled in from `CARGO_MANIFEST_DIR`
(`/src/crates/eval/gates.toml` in the build stage), which the runtime image
lacks, so the bench always passes `--gates`.

`BENCH_UID` and `BENCH_GID` are set by `run.sh bench` to the invoking user;
like the `DEMO_*` variables they are not in `.env.example`.

## Invariants and constraints

- **Nothing restarts `crosstalk` between the swarm and the export.** The
  only restart is step 4. Every later `compose run` passes `--no-deps`, so
  compose never recreates a dependency, and the bench service declares
  none.
- **No fake score.** Without Live or the API, or with a token the API
  refuses, the run stops at step 3 with exit 1, before the restarts and
  before any run directory exists.
- **No truncated export.** The export is fetched only once Live's watermark
  has passed the swarm's end.
- **A holdout run is never scored here.** `--holdout` stops before
  scoring and prints no metrics; holdout seeds (>= 1,000,000) are refused
  without `--holdout`, and `--holdout` refuses any other seed.
- **One run, one directory.** The run id is the UTC second the swarm
  starts; an existing directory stops the run.
- **The world is fresh:** an empty wiki and empty detection state at the
  start of every run. The exchange log and blobs are not fresh, by design.
- **The scenario is recorded** in `bench.env` and the truth header, so a
  score is never read against the other scenario's baseline.
- **ct-eval's exit code is the bench's** once scoring runs (0 pass, 2 gate
  failed); any earlier failure is 1 with a message naming the step.
- `deploy/bench/` is created by the invoking user before any bind mount of
  it, so Docker never creates it as root.
- `run.sh`'s existing subcommands are unchanged. Both scripts are bash with
  `set -euo pipefail` (inherited by `bench.sh`) and pass `bash -n`.

## Pending

- **Suspected transmissions.** The export holds confirmed transmissions
  only; an all-states export (a `states` scope with a state column) is on
  the gateway's queue. Until then a detection still suspected at export
  time is not scored.
- **Lag gauge.** Waiting on the watermark is exact for exports but coarse
  (5-minute buckets); a per-consumer lag gauge is on the gateway's
  observability list.
- **Gates.** `crates/eval/gates.toml` gates `demo-swarm/headline` from the
  first real run on node0; the `demo-swarm/boilerplate` false-positive
  ceiling is a placeholder until a measured run.
