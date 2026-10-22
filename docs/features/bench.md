# Live detection benchmark (`run.sh bench`)

`bash deploy/run.sh bench` runs one scored benchmark of the real gateway on
the single-machine compose deployment (node0). The demo swarm
([demo.md](demo.md)) sends its traffic through the gateway to the fake
upstream and writes ground truth v2. The gateway's detections are then
exported over the L8 API, turned into a2a-transmission-bench input by
`ct-bench-detect`, and scored against the truth by `a2a-bench`, the bench
repo's scorer, at a pinned release tag (`bench_ref`), giving precision and
recall. The host has no Rust toolchain, so the bench tools ship in the
`crosstalk-demo` image and run as the compose service `bench`.

The scorer used to be crosstalk's own `ct-eval swarm` with
`crates/eval/gates.toml`; it was retired in favour of the bench (its code
stays in history at `7f8a2fb` and `4d3d2c3`), and the fetch step moved from
`ct-eval swarm-fetch` to `ct-bench-detect swarm-fetch` with the same flags
and output ([bench_adapter.md](bench_adapter.md)).

**Status:** working. The gateway runs `Live` and the L8 API in Postgres
mode (the compose stack's default), each run gets its own database, and
runs are scored by `a2a-bench` at `A2A_BENCH_REF=a2a-bench-v1.1.1`
(`deploy/demo.Dockerfile`). Held-out runs are made with `--holdout` and
scored only by the bench's releases, not here (README "Results").

## Scope

- `run.sh bench`, implemented in `deploy/bench.sh`: one run from a fresh
  database and a fresh world to a printed score, with the swarm's knobs
  passed through.
- Two scenarios ([below](#scenarios)): `headline` and `boilerplate`.
- The `bench` compose service (profile `bench`): the bench tools with the
  run directory bind-mounted (and no gateway volume).
- `ct-bench-detect` (fetching and bench input) and `a2a-bench` at
  `A2A_BENCH_REF` with the bench repo's gates, in the demo image.
- The run directory `deploy/bench/<run>/` (gitignored).

## Non-scope

- Detection itself, the L8 API and the flow config. Those are the gateway's
  (see [Pending](#pending)); the bench only consumes them.
- Scoring logic and the demo-swarm label join. They are the bench repo's
  (`a2a-bench export --dataset demo-swarm` and `a2a-bench score`; since
  `a2a-bench-v1.1.0` labels join captured exchanges by session and tool-use
  id, and turns are diagnostics only; since `a2a-bench-v1.1.1` exchanges that
  start in the same microsecond keep the capture's `(at_us, id)` order, so
  such a capture is no longer refused as "not exactly the labelled run").
- Gates for `demo-swarm/<scenario>`. They live in the bench repo's
  `gates/crosstalk-gateway-export.toml`, shipped in the image at
  `/usr/local/share/a2a-bench/gates/`.
- Comparing runs, trends and dashboards.
- Pruning the exchange log and blobs. They accumulate across runs (below).
- Dropping old runs' databases (`crosstalk_bench_<run>`). They are kept;
  the bench prints how many there are and their total size after each
  fetch (`bench_database_tally`), and nothing drops them.
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

The exit code is `a2a-bench score`'s: 0 when every gate passes (or none applies), 2
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
  step 8 (scoring) does not. There is no `report/` and no `score.txt`.
  `swarm.txt` holds the swarm's own report (generator traffic: sessions,
  writes, reads, planted transmissions; nothing the gateway detected) and is
  never echoed to the terminal. `swarm-fetch`'s output, which counts the
  export's rows per state and so summarises the detector, is not kept:
  `fetch.log` says only whether the fetch succeeded and the export and
  evidence files' byte sizes. A failed fetch keeps its output in
  `fetch.err` and the run is not a usable holdout. The bench prints the run
  id and its file list.
- **Where it goes.** On node0 the run is under `deploy/bench/holdout/<run>/`.
  Copy it to the bench machine's dataset root, outside every git repo,
  never under `bench-runs/`:
  `rsync -a node0:<deploy dir>/bench/holdout/<run>/ ~/Data/ai/agents/demo-swarm-holdout/<run>/`.
- **Same scenarios** as development runs (headline and boilerplate), so the
  holdout measures the same thing.
- **Saves the bench inputs too.** After the fetch and snapshot, a holdout
  run runs `ct-bench-detect fetch` and `from-export` like a development run
  (`exchange-turns.json`, `span-points.json`, `bench-input/`), with their
  output kept only in `detect-fetch.log` and `from-export.log`, never echoed.

## Data and control flow

```text
run.sh bench
 1 confirm_restart ── prints what restarts; asks on a TTY unless --yes
   fresh_database ─── compose up -d postgres; CREATE DATABASE crosstalk_bench_<run>;
                        export CROSSTALK_DB_NAME (compose.yaml's DATABASE_URL)
 2 start_stack ────── compose up -d --build   (base + compose.demo.yaml): migrate,
                        crosstalk recreated on the run's database
 3 require_detection ── host curl /readyz: Postgres mode waits (≤180 s) for
                        recovery done, pipeline_lock held, capture durable;
                        then tasks `live` and `api` running;
                        host curl GET /operators with the API token: 200
 4 fresh_world ────── compose restart wiki crosstalk; wait until both are healthy; step 3 again
 5 run_swarm ──────── compose run --no-deps swarm --agents … --ground-truth /bench/<run>/truth.jsonl
                        swarm ─▶ crosstalk:8080 ─▶ fake-upstream:8070; tool calls ─▶ wiki:8090
                        crosstalk ─▶ data volume: exchanges/exchange-log.jsonl, blobs/
 6 wait_caught_up ─── host curl <CROSSTALK_BIND>:<CROSSTALK_OPS_PORT>/healthz until
                        live.watermark_micros ≥ the swarm's end
 7 fetch_detections ─ compose run --no-deps bench (ct-bench-detect) swarm-fetch --api http://crosstalk:8081
                        --token-env CROSSTALK_API_TOKEN --truth … --out /bench/<run>
                        (POST /exports, GET /transmissions/{id}/evidence)
   snapshot_inputs ── docker cp the exchange log and blobs from crosstalk into the run
   bench_detect_inputs  ct-bench-detect fetch (/query answers), then from-export → bench-input/
 8 score ──────────── compose run --no-deps --entrypoint a2a-bench bench export --dataset demo-swarm …
                        then score --predictions bench-input/predictions.jsonl --gates … --out /bench/<run>/report
                      prints the `overall:` line, the gates, the result; exits with a2a-bench score's code
```

Step by step:

1. **Confirm.** The bench prints that it will start the demo stack
   (recreating services whose image or config changed), create a fresh
   database for the run and point `crosstalk` at it, and restart `wiki`,
   losing the wiki's pages, while the `data` volume is kept. Without
   `--yes` it asks; when stdin is not a terminal it refuses rather than
   guess.

   **Fresh database** (`fresh_database`). The run id is stamped here, and
   the bench starts `postgres`, creates `crosstalk_bench_<run>` (the run id
   lowercased) and exports `CROSSTALK_DB_NAME`, which `compose.yaml`'s
   `DATABASE_URL` reads (default `crosstalk`). Nothing earlier runs left in
   Postgres (fingerprints, token observations, transmissions) can reach
   this one; earlier runs' databases are kept, never dropped. A later plain
   `run.sh up` points `crosstalk` back at the `crosstalk` database.
2. **Start.** `up -d --build` with the demo override, as `demo up` does,
   which migrates the run's database and recreates `crosstalk` on it. The
   build makes sure the demo image carries the bench tools (`ct-bench-detect`,
   `a2a-bench`). It then waits for `crosstalk` to be healthy.
3. **Fail fast.** From the host, the bench reads the ops listener's
   `/readyz`. In Postgres mode (a `store` section; `/readyz` then carries
   `recovery`) `/readyz` is already 200 (degraded) while the pipeline
   recovers, so the bench first polls it for up to 180 s until `recovery`
   is `done`, `pipeline_lock` is `held` and `capture` is `durable`, and
   stops with exit 1 if not (`wait_pipeline_recovered`); memory mode skips
   the wait. It then requires two running tasks: `live` (every layer stage of
   the Live detection pipeline is running) and `api` (the operator API's
   listener is bound). It then calls `GET /operators` on the published API
   port with `CROSSTALK_API_TOKEN` from `deploy/.env` and requires a 200.
   Without `live` the export would be empty and the score a real but
   meaningless zero; without `api` there is no export. Either stops the run
   with exit 1, before any traffic is sent and before step 4's restarts.
4. **A fresh world.** The wiki lives in memory, so restarting it empties it
   and the run has no `unattributed_read` rows (no page version from before
   the run). Restarting `crosstalk` starts its pipeline over on the run's
   fresh database, so the export holds only this run. Both restarts happen here, at the start,
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
7. **Fetch.** `ct-bench-detect swarm-fetch` exports the transmissions from the
   truth header's `started_at_unix_ms` onward and fetches each one's
   evidence, with the deployment's API token. It writes `export.jsonl` and
   `evidence.jsonl`. This must happen before anything restarts `crosstalk`.
   The export asks for the states in `fetch::FETCHED_STATES`: confirmed,
   classified, aggregated and discarded (so access-only transmissions have
   live input). A settled export holds no suspected or awaiting-content
   row: by the time the watermark passes, unconfirmed traffic is
   discarded. Then, after the snapshot of the exchange log and blobs,
   `bench_detect_inputs` runs `ct-bench-detect fetch` (the gateway's
   `/query` answers, `exchange-turns.json` and `span-points.json`, which
   only a live gateway can give) and `ct-bench-detect from-export` (the run
   directory as a2a-transmission-bench input in `bench-input/`), with
   their output in `detect-fetch.log` and `from-export.log`. Neither
   scores anything.
8. **Score.** `a2a-bench export --dataset demo-swarm` reads the truth and
   `bench-input/` (step 7's `ct-bench-detect from-export`) and writes the
   bench's export to `bench-export/` (its log: `bench-export.log`). Then
   `a2a-bench score` scores `bench-input/predictions.jsonl` against it with
   `/usr/local/share/a2a-bench/gates/crosstalk-gateway-export.toml`, writing
   `report/` (`report.json`, `report.txt`); its output goes to `score.txt`.
   The bench prints the headline, for example:

   ```text
   bench 20261007T152245Z (headline, a2a-bench-v1.1.0)
   overall: recall 1.000 (40 / 40), precision 1.000 (40 correct, 0 false, 0 unjudged)
   gates:
     pass  1.000                    demo-swarm/headline: every wiki transmission is found
     ...
   result: pass
   report: deploy/bench/20261007T152245Z/report/report.txt
   ```

   and exits with `a2a-bench score`'s code (2 = a gate failed).

### The run directory

```text
deploy/bench/<run>/
  bench.env          run id, scenario, seed, holdout, crosstalk_commit (deploy/SOURCE_COMMIT),
                     database, swarm arguments, settle window, image ids, bench_ref and
                     bench_commit (the a2a-bench that scored it), swarm_end_unix_ms
  truth.jsonl        ground truth v2 (the swarm)
  swarm.txt          the swarm's report
  healthz.json       the last /healthz body read while settling
  export.jsonl       the transmissions export (ct-bench-detect swarm-fetch)
  evidence.jsonl     one TransmissionEvidence per exported transmission
  fetch.log          holdout only: whether swarm-fetch succeeded and the two files' sizes
  fetch.err          holdout only, on a failed fetch: swarm-fetch's output
  exchange-log.jsonl the gateway's exchange log, copied after the export
  blobs/             the gateway's blob store, copied after the export
  exchange-turns.json, span-points.json
                     the gateway's /query answers (ct-bench-detect fetch)
  detect-fetch.log   ct-bench-detect fetch's output
  bench-input/       a2a-bench input (ct-bench-detect from-export)
  from-export.log    ct-bench-detect from-export's output
  bench-export/      the bench's export of this run (a2a-bench export); not in a holdout run
  bench-export.log   a2a-bench export's output; not in a holdout run
  score.txt          a2a-bench score's output; not in a holdout run
  report/            report.json, report.txt; not in a holdout run
```

The exchange log and blobs make the directory self-contained: it can be
copied elsewhere and re-scored with any bench release, `a2a-bench export
--dataset demo-swarm --inputs bench-input --truth truth.jsonl --out <dir>`
then `a2a-bench score --export <dir> --predictions
bench-input/predictions.jsonl --gates <bench repo>/gates/crosstalk-gateway-export.toml
--out <report dir>` (`bench-input/` itself is rebuilt from the run by
`ct-bench-detect from-export --run <dir>`). Both accumulate across runs on the `data`
volume, so each copy holds everything so far (blobs are content-addressed
and small); the importer only reads the run's sessions.

### Replaying a run offline

`ct-bench-detect replay --run <dir> --out <out>` re-runs a saved run
through the same `Live` composition in memory, with the run's
`evidence_window_ms` and `suspected_ttl_ms` from `bench.env`, and writes a
bench input directory with `crosstalk-live` predictions, which `a2a-bench`
scores like step 8 ([bench_adapter.md](bench_adapter.md)). It reads the
run's own copy of the exchange log and blobs (`exchange-log.jsonl`,
`blobs/`, from `snapshot_inputs`). Only exchanges captured since the truth
header's `started_at_unix_ms` are replayed, which is this run's traffic
since step 4's restart. (This was `ct-eval replay`, which scored the
replay itself.)

### The data volume

Nothing after the export reads the gateway's `data` volume in place:
`snapshot_inputs` copies the exchange log and blobs into the run directory
(`docker cp` from the `crosstalk` container), and `ct-bench-detect
from-export` and `a2a-bench` read only the run directory. The copy holds
everything so far (both accumulate across runs, and nothing prunes them);
the bench input joins on the run's own sessions, so the older entries do
no harm. The `bench` service does not mount the volume: no step reads it
in place, so it gets no access to the gateway's data.

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
| `deploy/bench.sh` | The bench: one function per step (`confirm_restart`, `fresh_database`, `start_stack`, `require_detection`, `fresh_world`, `run_swarm`, `wait_caught_up`, `fetch_detections`, `snapshot_inputs`, `bench_detect_inputs`, `score`), plus `bench` (options), `bench_usage`, `bench_fail`, `ops_url`, `api_url`, `env_value`, `positive_int`, `ready_task`, `readyz_field`, `wait_pipeline_recovered`, `healthz_watermark`, `demo_flow_ms`, `wait_healthy`, `bench_database_tally`, `bench_swarm_out`, `bench_detect_step`, `bench_a2a`, `bench_ref_lines` |
| `deploy/compose.yaml` | `DATABASE_URL` ends in `${CROSSTALK_DB_NAME:-crosstalk}`, which `fresh_database` sets per run |
| `deploy/compose.demo.yaml` | The `bench` service: the demo image with entrypoint `ct-bench-detect` (the `a2a-bench` steps pass `--entrypoint`), profile `bench`, `restart: "no"`, `user: ${BENCH_UID:-65532}:${BENCH_GID:-65532}`, `CROSSTALK_API_TOKEN`, `./bench:/bench` (no gateway volume) |
| `deploy/demo.Dockerfile` | Builds `crosstalk-demo` and `ct-bench-detect` (`-p crosstalk-bench-adapter`) in one cargo invocation, and `a2a-bench` from the bench mirror (the `a2a` build context) at `ARG A2A_BENCH_REF`; ships `a2a-bench` at `/usr/local/bin/a2a-bench`, the bench repo's `gates/*.toml` at `/usr/local/share/a2a-bench/gates/` and `/usr/local/share/a2a-bench/REF` (`bench_ref`, `bench_commit`), which `bench_ref_lines` copies into `bench.env`. The `.dockerignore` already admits `crates/` and `spec/` and excludes only `deploy`, `target`, VCS and editor files, which the build does not need |
| `.gitignore` | `/deploy/bench/` |
| `crates/bench-adapter/src/bin/ct-bench-detect/main.rs` | `ct-bench-detect`: `swarm-fetch` (step 7, the flags and output line `ct-eval swarm-fetch` had), `fetch` and `from-export` (`bench_detect_inputs`), and `replay` (offline, not run by the bench) |
| `crates/bench-adapter/src/swarm/fetch.rs` | The L8 HTTP reads behind `swarm-fetch` and `fetch`; `FETCHED_STATES` |

**Moving to a newer bench release:** fetch its tag into the mirror
(`git --git-dir=<A2A_BENCH_GIT> fetch --tags`), bump `A2A_BENCH_REF` in
`deploy/demo.Dockerfile`, and rebuild the demo image (the next `run.sh
bench` does). Every run's `bench.env` names the release that scored it.

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
- **One run, one directory.** The run id is the UTC second the bench
  stamps the run, just after the confirmation and before the stack starts
  (the run's database is named from it); an existing directory stops the
  run. Scoring windows come from the truth header, not the run id.
- **The world is fresh:** an empty wiki and a new database
  (`crosstalk_bench_<run>`, recorded as `database=` in `bench.env`) at the
  start of every run. The exchange log and blobs are not fresh, by design.
- **The scenario is recorded** in `bench.env` and the truth header, so a
  score is never read against the other scenario's baseline.
- **a2a-bench score's exit code is the bench's** once scoring runs (0
  pass, 2 gate failed); any earlier failure is 1 with a message naming the
  step.
- **crosstalk never scores.** The demo image carries no crosstalk scorer
  or gates file: crosstalk's side is `ct-bench-detect` (fetch and bench
  input), and the score and gates are `a2a-bench`'s at `A2A_BENCH_REF`.
- **Every scored run names its scorer:** `bench_ref` and `bench_commit` in
  `bench.env`; the run stops before the swarm if the image lacks them.
- `deploy/bench/` is created by the invoking user before any bind mount of
  it, so Docker never creates it as root.
- `run.sh`'s existing subcommands are unchanged. Both scripts are bash with
  `set -euo pipefail` (inherited by `bench.sh`) and pass `bash -n`.

## Pending

- **Lag gauge.** Waiting on the watermark is exact for exports but coarse
  (5-minute buckets); a per-consumer lag gauge is on the gateway's
  observability list.
