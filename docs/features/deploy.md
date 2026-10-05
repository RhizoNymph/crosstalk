# Deploy: single-machine docker compose

## Scope

- Container images for the `crosstalk` binary and the operator UI.
- `deploy/compose.yaml`: Postgres, migrations, the gateway (`--role all`),
  the UI, and the infrastructure observability stack (Prometheus, Grafana,
  Loki, Alloy, node-exporter, cAdvisor, postgres-exporter).
- The gateway's deployment config (`deploy/config/crosstalk.json`) and its
  secrets (`deploy/.env`, generated).
- Infrastructure dashboards and alert rules.
- The contract the `crosstalk` binary must implement for this deployment
  (below). The gateway crate implements it; this feature only depends on it.

## Non-scope

- Application metrics and tracing inside the gateway (the ops port and the
  scrape job are ready for them).
- Multi-machine deployment, NATS JetStream, object storage, backups and
  retention jobs (roadmap P9; see `docs/infrastructure.md`).
- TLS termination. The proxy speaks plain HTTP to agents; put it on a
  trusted network or behind a TLS terminator.

## The binary contract

`deploy/` assumes the `crosstalk` binary (`crates/gateway`) provides:

| Command | Behaviour |
|---|---|
| `crosstalk serve --role <all\|proxy\|pipeline\|api\|analysis> --config <path>` | Runs the role(s). `all` is the only role compose uses today. Exits non-zero on bad config. Handles SIGTERM by refusing new exchanges and letting in-flight streams finish (compose waits 60 s). |
| `crosstalk migrate --config <path>` | Runs every layer's migrations (`crates/store`), then exits 0. Idempotent. |
| `crosstalk healthcheck --url <url>` | GETs the URL; exits 0 on 2xx, 1 otherwise. The runtime image is distroless (no shell, no curl), so the container healthcheck needs this. |

Listeners:

| Container port | Config key | Serves |
|---|---|---|
| 8080 | `ingress.listen` | The reverse proxy. Agents set `ANTHROPIC_BASE_URL=http://<host>:<CROSSTALK_PROXY_PORT>/anthropic` (host port, default 8080). |
| 8081 | `api.listen` | The L8 HTTP binding over the live process's surface (roles `all` and `api`). Every request carries `Authorization: Bearer <value of the variable api.token names>` and is made as `api.operator` (default `{"name": "admin"}`, every permission); without it, `401`. |
| 9464 | `ops.listen` | `GET /metrics` (Prometheus text format), `GET /healthz` (process serving, with the capture, pipeline and exchange-log counters, and a `live` section: each layer stage's handled count and the L7 watermark), `GET /readyz` (database reachable, migrations at head, every role's tasks running: `exchange_log`, `capture`, `live` (every layer stage), `proxy`, `api`). |

Environment:

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | Postgres URL (`crates/store::config::DATABASE_URL_VAR`). |
| `CROSSTALK_SECRET_V1` | 64 hex digits, named by `ingress.secrets.current.env`. |
| `CROSSTALK_API_TOKEN` | Named by `api.token.env`. |
| `CROSSTALK_EMBEDDINGS_API_KEY` | Named by `embeddings.api_key.env`; may be empty for a keyless local endpoint. |
| `RUST_LOG` | `tracing` filter. Logs are JSON on stdout, one object per line, with a top-level `level` field (Alloy lifts it into a Loki label). |

Config (`deploy/config/crosstalk.json`): JSON in the spec's wire
conventions (snake_case, unknown fields refused), secrets only by `{"env":
...}` reference. Top-level keys:

| Key | Shape |
|---|---|
| `ingress` | `crosstalk_ingress::config::IngressConfig`, unchanged. |
| `api` | `{"listen": SocketAddr, "token": {"env": String}, "operator": {"name": String}}`; `operator` defaults to `{"name": "admin"}` |
| `ops` | `{"listen": SocketAddr}` |
| `store` | `{"pool": crosstalk_store::config::PoolSettings}` |
| `blobs` | `{"root": path}` for `FsBlobStore::open`. The `data` volume is mounted at `/var/lib/crosstalk` (owned by the runtime user), so the blob root and anything the gateway keeps beside it (P3's exchange log in the blob root's parent) persist. |
| `embeddings` | `{"base_url": String, "model": String, "api_key": {"env": String}}`, an OpenAI-compatible endpoint. |

Optional keys the gateway also accepts, all defaulted when absent: `bus`,
`pipeline` (blob put retries), `shutdown` (`drain_timeout_ms` 45 000 +
`flush_timeout_ms` 10 000, chosen to fit compose's 60 s
`stop_grace_period`), `extract` (L5's extractor configuration,
crosstalk-flow's `ExtractConfig`: `mcp_servers`, `http_tools`,
`fetch_tools`, `sites`; see `docs/features/flow_extract.md`) and `flow`,
L5's correlation timing, every key defaulted:

| `flow` key | Default | Meaning |
|---|---|---|
| `correlation_window_ms` | 600 000 | The longest write-to-read lag that pairs on access alone (no content) |
| `evidence_window_ms` | 120 000 | How long after a read a channel transmission waits for its content match |
| `suspected_ttl_ms` | 1 800 000 | How long a suspected transmission waits for a late match |
| `content_retention_ms` | 2 592 000 000 | The longest write-to-read lag a content match still confirms (L4's 30-day span index retention); at least `correlation_window_ms` |
| `shards` | 1 | Correlator shards |
| `tick_ms` | 1 000 | How often the live process ticks (windows close, the watermark advances) |

`deploy/config/crosstalk.json` spells the defaults out; the demo config
(`deploy/demo/crosstalk.demo.json`) differs only in its upstream URL and
`evidence_window_ms` 10 000 / `suspected_ttl_ms` 60 000, so the demo's
transmissions confirm and its watermark (the tick's time minus
`evidence_window_ms + suspected_ttl_ms`, aligned to a 5-minute bucket)
moves within minutes. `docs/features/gateway.md` is the authoritative
description of the binary's side of this contract; keep the two in step.

### Rotating the deployment secret

`CROSSTALK_SECRET_V1` keys the credential and account digests. To rotate
without breaking the linking of exchanges across the change:

1. Add `CROSSTALK_SECRET_V2=$(openssl rand -hex 32)` to `deploy/.env` and
   pass it to the gateway (`x-crosstalk-env` in `compose.yaml`).
2. In `deploy/config/crosstalk.json`, make version 2 current and keep
   version 1 as `previous` until a chosen instant (RFC 3339, UTC,
   microseconds):

   ```json
   "secrets": {
     "current": {"version": 2, "env": "CROSSTALK_SECRET_V2"},
     "previous": {"version": 1, "env": "CROSSTALK_SECRET_V1",
                  "overlap_ends": "2026-11-01T00:00:00.000000Z"}
   }
   ```

   `previous.version` must be strictly older than `current.version`, or
   the gateway refuses to start.
3. `bash deploy/run.sh up`. Until `overlap_ends`, exchanges also record
   version-1 digests; from then on only version 2 is used.
4. After `overlap_ends`, remove `previous`, drop `CROSSTALK_SECRET_V1`
   from `deploy/.env` and `compose.yaml`, and restart.

## Data and control flow

1. `run.sh init` copies `.env.example` to `.env`, filling each secret with
   `openssl rand -hex`.
2. `run.sh up` runs `docker compose up -d --build` (adding the `ui` profile
   when `ui/` exists).
   1. `postgres` initialises its volume on first start. The init scripts
      create `vector`, `pg_trgm` and `pg_stat_statements`, and the
      `crosstalk_monitor` role (`pg_monitor`).
   2. Once `pg_isready` passes, `migrate` runs `crosstalk migrate` and
      exits.
   3. `crosstalk` starts only after `migrate` has succeeded.
3. At runtime:
   - Agents send requests to the proxy's host port (`CROSSTALK_PROXY_PORT`,
     default 8080), and the proxy forwards them upstream.
   - Capture feeds the in-process pipeline, which writes to Postgres and
     the `data` volume (`/var/lib/crosstalk`).
   - The UI and operators read through the API (`CROSSTALK_API_PORT`,
     default 8081).
4. Observability:
   - Prometheus scrapes node-exporter (on the host network at port 19100,
     via `host.docker.internal`), cAdvisor, postgres-exporter, Loki, Alloy,
     Grafana and `crosstalk:9464`.
   - Alloy tails every container in the `crosstalk` project through the
     Docker socket and pushes its logs to Loki.
   - Grafana is provisioned with both datasources and the dashboard
     "crosstalk / infrastructure".

## Files

| File | Role |
|---|---|
| `deploy/compose.yaml` | Every service, volume, port bind and healthcheck. The proxy publishes on `CROSSTALK_PROXY_BIND` (0.0.0.0); every other published port on `CROSSTALK_BIND` (127.0.0.1 unless set); node-exporter listens on the host network. |
| `deploy/crosstalk.Dockerfile` (+ `.dockerignore`) | Builds the gateway from the repository root on the nightly in `rust-toolchain.toml` (minimal profile). The runtime image is distroless `cc-debian13:nonroot`. |
| `deploy/ui.Dockerfile` (+ `.dockerignore`) | Builds the elements bundle (pnpm 11.27.1, frozen lockfile), then the Topcoat binary. The context is the repository root, because `ui/` depends on `spec/` by path and `spec/` inherits from the root workspace. |
| `deploy/config/crosstalk.json` | Gateway config (contract above). |
| `deploy/config/ui.json` | UI config: the HTTP backend, reading the gateway's operator API at `http://crosstalk:8081` with `CROSSTALK_API_TOKEN` (no operator section: the gateway's API names the operator). |
| `deploy/.env.example` | Every variable compose reads, with the defaults: secrets, host ports and binds, `DOCKER_ROOT_DIR` (Docker's data root, for cAdvisor) and Postgres/Prometheus tuning. |
| `deploy/run.sh` | `init`, `up`, `infra`, `down`, `logs`, `ps`, `psql`, `urls` (plus `demo ...` and `bench`, see below). Sets `DOCKER_ROOT_DIR` from `docker info` unless the environment or `deploy/.env` does. |
| `deploy/postgres/init/` | First-start SQL: extensions and the monitor role. |
| `deploy/prometheus/prometheus.yml` | Scrape jobs: `prometheus`, `node`, `cadvisor`, `postgres`, `loki`, `alloy`, `grafana`, `crosstalk`. |
| `deploy/prometheus/rules/infrastructure.yml` | Infra alert rules (group `crosstalk-infra`). |
| `deploy/prometheus/rules/gateway.yml` | Gateway health alert rules (group `crosstalk-gateway`) on the gateway's own `/metrics`: normalize, store and publish failures, capture drops and decode errors, exchange log write failures, nothing published while capturing, stuck draining. |
| `deploy/grafana/provisioning/` | Datasources (uids `prometheus`, `loki`) and the dashboard provider. |
| `deploy/grafana/dashboards/infrastructure.json` | Host, containers, Postgres, logs and monitoring-stack panels, plus a crosstalk-process row. |
| `deploy/grafana/dashboards/gateway.json` | "crosstalk / gateway" (uid `crosstalk-gateway`): health, capture, pipeline, exchange log, the captured → published → written funnel, and the gateway's WARN/ERROR logs. |
| `deploy/loki/loki.yaml` | Single-binary Loki on the filesystem, 7-day retention. |
| `deploy/alloy/config.alloy` | Docker log discovery, `service`/`container`/`stream` labels, JSON `level` label for crosstalk, migrate and ui. |
| `deploy/compose.demo.yaml`, `deploy/demo.Dockerfile`, `deploy/demo/` | The token-free demo (fake upstream, wiki, agent swarm; `run.sh demo ...`): see [demo.md](demo.md). The image also carries `ct-eval`, and the override adds the `bench` service (profile `bench`). |
| `deploy/bench.sh`, `deploy/bench/` (gitignored) | `run.sh bench`: one scored detection benchmark on the demo stack, its runs under `deploy/bench/<run>/`; see [bench.md](bench.md). |

## Store tests against the compose Postgres

Until there is a live deployment, the store tests (`crates/store`'s
`TestDb`) may run against the compose `postgres` service instead of
`scripts/test-db.sh`. Reach it through an SSH tunnel to node0's
`127.0.0.1:5432` as role `crosstalk`; the URL lives in the gitignored
`.env.test` the harness reads.

- Each test creates and drops its own `crosstalk_test_*` database, which
  shows in the dashboard's per-database Postgres panels while it exists.
- Each test holds a pool of 4 connections plus one admin connection, and
  libtest runs one test per core. That stays under the default
  `max_connections=200`. If `PostgresConnectionsNearMax` fires or tests
  fail with "too many clients", set `PG_MAX_CONNECTIONS=500` in
  `deploy/.env` and run `bash deploy/run.sh up`, which recreates Postgres.
- The server keeps durable settings (fsync on), so tests run slower here
  than on `scripts/test-db.sh`.

Before a real deployment, or after a killed test run, drop the leftovers
from `bash deploy/run.sh psql`:

```sql
SELECT format('DROP DATABASE %I WITH (FORCE)', datname)
FROM pg_database WHERE datname LIKE 'crosstalk\_test\_%' \gexec
```

`WITH (FORCE)` ends any sessions still connected; `\gexec` runs each
generated statement.

## Running on a shared host

What it took to bring the stack up on a homelab box that other projects
also use (node0, at 10.1.1.69, in the examples). None of it needs a code
change; the knobs are `deploy/.env` and, for node-exporter, two config
lines.

### Before you start

On the host, before the stack's first start (once it runs, its own ports
show as taken):

```sh
docker info --format '{{.SecurityOptions}} {{.DockerRootDir}}'
for p in 8080 8081 9464 3000 3001 9090 12345 5432 19100; do
  ss -Hltn "sport = :$p" | grep -q . && echo "port $p is taken"
done
```

- A data root under `/var/snap/docker/` means snap Docker (below);
  `name=rootless` among the security options means rootless Docker, which
  has the same no-`rslave` limit.
- Every port the loop prints needs another host port (below) before
  `run.sh up`. A listener on any address counts: publishing
  `127.0.0.1:5432` fails while another process holds `0.0.0.0:5432`.

To find who holds a port:

```sh
sudo ss -ltnp 'sport = :9100'                              # docker-proxy: a published container port
docker ps --format '{{.Names}}\t{{.Ports}}' | grep 9100    # names that container
```

### Snap Docker

- **`/` is not a shared mount**, so a bind with `rslave` fails ("path / is
  mounted on / but it is not a shared or slave mount"). node-exporter binds
  `/:/host:ro` without it; a filesystem mounted after node-exporter starts
  shows up once it restarts.
- **The data root is `/var/snap/docker/common/var-lib-docker`**, not
  `/var/lib/docker`. Mounting the latter into cAdvisor failed with "mkdir
  /var/lib/docker: read-only file system". Compose mounts
  `${DOCKER_ROOT_DIR}` instead, and `run.sh` fills it from `docker info
  --format '{{.DockerRootDir}}'` unless `deploy/.env` sets it. Set it in
  `deploy/.env` when running `docker compose` without `run.sh`.
- **Confinement may refuse bind mounts outside `$HOME` and `/media`.**
  Compose binds `deploy/config/`, `deploy/prometheus/` and the other
  config from the checkout, so keep the checkout under `$HOME`.

### Port clashes

Every host port but node-exporter's is a variable in `deploy/.env`:

| Listener | Variable | Default | On node0 |
|---|---|---|---|
| Proxy (agents) | `CROSSTALK_PROXY_PORT` | 8080 | 18080 (8080 was taken) |
| Operator API | `CROSSTALK_API_PORT` | 8081 | 18081 |
| Ops (`/healthz`, `/readyz`, `/metrics`) | `CROSSTALK_OPS_PORT` | 9464 | 19464 |
| UI | `CROSSTALK_UI_PORT` | 3000 | default |
| Grafana | `GRAFANA_PORT` | 3001 | default |
| Prometheus | `PROMETHEUS_PORT` | 9090 | default |
| Alloy | `ALLOY_PORT` | 12345 | default |
| Postgres | `POSTGRES_PORT` | 5432 | default |
| node-exporter | none: compose.yaml and prometheus.yml | 19100 | default |

- **The variables move only the host side.** Container ports stay
  8080/8081/9464, so `deploy/config/crosstalk.json`, the container
  healthcheck and Prometheus's scrape of `crosstalk:9464` do not change.
  After editing `deploy/.env`, `bash deploy/run.sh up` recreates the
  affected containers and `bash deploy/run.sh urls` prints the new
  addresses. Anything outside the stack follows by hand: the agents' base
  URL, and the SSH tunnel for the store tests.
- **node-exporter has no port mapping.** It runs on the host network, so
  its listen address is a host port. It left 9100 because another
  project's MinIO publishes 9100 and 9101 (`rbs-minio`, 9100→9000 and
  9101→9001). Prometheus scraping someone else's port shows as the `node`
  target down with `received unsupported Content-Type "text/html"` (that
  was MinIO's console on 9101). To move it, change both together:
  - `--web.listen-address` on `node-exporter` in `deploy/compose.yaml`;
  - the `node` job's target in `deploy/prometheus/prometheus.yml`.

  Then `bash deploy/run.sh up` and `docker restart crosstalk-prometheus-1`
  (see the single-file mounts under syncing).

### Reaching the stack from a tailnet or the LAN

By default only the proxy is reachable from other machines; the API, ops
port, UI, Grafana, Prometheus, Alloy and Postgres bind 127.0.0.1. One
variable in `deploy/.env` moves all of them:

| `CROSSTALK_BIND` | Reachable from |
|---|---|
| `127.0.0.1` (default) | this machine only (SSH tunnels from elsewhere) |
| `0.0.0.0` | this machine, the LAN and the tailnet |
| the host's tailnet IP (`tailscale ip -4`, e.g. `100.x.y.z`) | the tailnet only, and **not** this machine's 127.0.0.1 |

`0.0.0.0` is the simple choice on a trusted LAN, and what node0 uses:

```
echo "CROSSTALK_BIND=0.0.0.0" >> deploy/.env
bash deploy/run.sh up && bash deploy/run.sh urls
```

`up` recreates the containers whose ports changed. A tailnet IP keeps the
services off the LAN, but they then stop answering on the host's
127.0.0.1: the smoke test's `curl 127.0.0.1:<ops port>` and any SSH tunnel
to `127.0.0.1:5432` (the store tests') must use the tailnet IP instead, and
Tailscale must be up before the stack starts (after a reboot Docker may
start first; `bash deploy/run.sh up` again fixes it).
Grafana keeps its admin password, Postgres its role password and the
operator API its bearer token; Prometheus, Alloy and the ops endpoints
(`/metrics`, `/healthz`) have no authentication, so anyone who can reach
the bind address can read them.

### Reaching the proxy from agent machines

Agents point at the host's IP and the proxy's host port:

```sh
export ANTHROPIC_BASE_URL=http://10.1.1.69:18080/anthropic
```

A name that works for `ssh` may resolve nowhere else. `node0` resolved
only through the SSH config, so `ssh` and `rsync` reached it but Claude
Code with `ANTHROPIC_BASE_URL=http://node0:18080/anthropic` failed with
`EAI_AGAIN`. Use the IP, or add the host to `/etc/hosts` on each agent
machine; `getent hosts node0` shows whether a name resolves outside SSH.
The proxy speaks plain HTTP, so keep it on a trusted network (accepted
for now; see `docs/infrastructure.md`).

### Syncing a checkout to the host

The checkout reaches the host by rsync rather than git, so any worktree
can be deployed (a worktree's `.git` is only a pointer file):

```sh
rsync -a --exclude deploy/.env --exclude target --exclude .claude <checkout>/ node0:<dir>/
```

- **No `--delete`, and `deploy/.env` excluded**, so the host's generated
  secrets survive. Files deleted from the checkout stay on the host until
  removed by hand.
- **Data survives a re-sync.** Volumes are named after the compose project
  (`name: crosstalk`), not the directory: `crosstalk_pgdata`,
  `crosstalk_data` and the rest.
- **Single-file bind mounts keep the old file.** rsync replaces a changed
  file with a new inode, and a running container still sees the old one.
  `run.sh up` does not recreate a container whose compose definition is
  unchanged, so restart the one that mounts the file, e.g. `docker restart
  crosstalk-prometheus-1` after changing `prometheus.yml`. The same goes
  for `crosstalk.json`, `ui.json`, `loki.yaml` and `config.alloy`.
  Directory mounts (`rules/`, Grafana's provisioning and dashboards) see
  new files.

### Smoke test

The end-to-end check that passed on node0. On the host:

```sh
bash deploy/run.sh up
docker inspect -f '{{.State.ExitCode}}' crosstalk-migrate-1   # 0
curl -s 127.0.0.1:<ops port>/readyz                            # "ready": true
curl -s 127.0.0.1:<ops port>/healthz                           # note capture.captured
```

From an agent machine:

```sh
ANTHROPIC_BASE_URL=http://<host-ip>:<proxy port>/anthropic claude -p "say hi"
```

Then `/healthz` again: `capture.captured` has gone up (one `claude -p`
may make more than one generation request), and `pipeline.published` and
`log.written` with it, and `live.stages` counts the layers' work. The API:

```sh
curl -s -H "Authorization: Bearer $CROSSTALK_API_TOKEN" 127.0.0.1:<api port>/operators
```

### Inspecting the distroless gateway

The runtime image has no shell, `ls` or `curl`: `docker exec
crosstalk-crosstalk-1 ls` fails with "executable file not found". Work
from the host instead.

- **Health.** `curl -s 127.0.0.1:<ops port>/healthz` returns the counters
  (`docs/features/gateway.md` has the shape); `/readyz` returns each
  check. An exchange that went all the way counts in `capture.captured`,
  `pipeline.published` and `log.written`. Any other non-zero counter names
  where exchanges stopped: `capture`'s others before the pipeline,
  `pipeline.normalize_failed` when L1 refused the request,
  `store_failed` and `publish_failed` after it, `log.write_failed` at the
  exchange log.
- **Logs.** One JSON object per line, with a top-level `level`:

  ```sh
  docker logs crosstalk-crosstalk-1 2>&1 | grep -iE '"level":"(WARN|ERROR)"'
  ```

  Each warning carries the `exchange` id and an `error` field. A rising
  `normalize_failed` pairs with `exchange not normalized; dropped`, whose
  `error` says what L1 refused; on node0 the first real request counted
  `captured` 1 and `normalize_failed` 1, and that line named the cause.
  The same lines are in Grafana Explore as `{service="crosstalk",
  level=~"WARN|ERROR"}`.
- **Files.** The `data` volume on the host, under Docker's data root:

  ```sh
  sudo ls "$(docker info --format '{{.DockerRootDir}}')/volumes/crosstalk_data/_data"
  ```

  `docker cp crosstalk-crosstalk-1:/var/lib/crosstalk/<path> .` copies
  a file out without a shell.
- **The binary.** `docker exec` can still run it by path:
  `docker exec crosstalk-crosstalk-1 /usr/local/bin/crosstalk inspect
  --config /etc/crosstalk/crosstalk.json [<exchange-id>]`.

## Invariants and constraints

- **Secrets never live in a tracked file.**
  - `deploy/.env` is git-ignored and written with mode 600.
  - Config files only name environment variables.
  - Each container receives only the secrets it uses.
- **Only the proxy is published off-host by default.** Every other
  published port binds `CROSSTALK_BIND`, 127.0.0.1 unless `deploy/.env`
  sets the host's tailnet IP or 0.0.0.0 (see "Reaching the stack from a
  tailnet or the LAN"); none of those services has TLS, and Prometheus,
  Alloy and the ops port have no authentication. node-exporter is the exception that is not published:
  it runs on the host network and listens on `0.0.0.0:19100`, because
  Prometheus reaches it through the Docker bridge (`host-gateway`), so
  host metrics are readable from the LAN unless a firewall blocks the port.
- **Every image is pinned to a release at least a week old:**

  | Image | Version |
  |---|---|
  | pgvector | 0.8.6-pg18-trixie |
  | Prometheus | v3.15.0 |
  | Grafana | 13.2.2 |
  | Loki | 3.7.8 |
  | Alloy | v1.20.0 |
  | node-exporter | v1.12.1 |
  | cAdvisor | v0.60.6 |
  | postgres-exporter | v0.20.1 |
  | rust | 1.98.1-slim-trixie |
  | node | 24.21.0-trixie-slim |

  The distroless base images track the `nonroot` tag.
- **The compiler is the nightly in `rust-toolchain.toml`.** The Dockerfiles'
  `RUST_TOOLCHAIN` argument must change together with that file.
- **`crosstalk` never starts against an unmigrated database.** Compose
  orders it after `migrate` has completed successfully.
- **The data volume is writable by uid 65532.** The image creates
  `/var/lib/crosstalk` and `/var/lib/crosstalk/blobs` with that owner, and
  a fresh named volume copies the ownership. A bind mount in its place must
  be chowned by hand. Anything the gateway writes to disk must stay under
  `/var/lib/crosstalk`; the rest of the filesystem is lost when the
  container is recreated.
- **Log volume is bounded.** Docker's `json-file` driver keeps at most
  5 × 50 MB per container. Loki keeps 7 days. Prometheus keeps 15 days,
  capped at 10 GB.
