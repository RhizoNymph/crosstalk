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

| Port | Config key | Serves |
|---|---|---|
| 8080 | `ingress.listen` | The reverse proxy. Agents set `ANTHROPIC_BASE_URL=http://<host>:8080/anthropic`. |
| 8081 | `api.listen` | The L8 HTTP binding (bearer token from `api.token`). |
| 9464 | `ops.listen` | `GET /metrics` (Prometheus text format), `GET /healthz` (process alive), `GET /readyz` (database reachable, migrations at head, every role's tasks running). |

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
| `api` | `{"listen": SocketAddr, "token": {"env": String}}` |
| `ops` | `{"listen": SocketAddr}` |
| `store` | `{"pool": crosstalk_store::config::PoolSettings}` |
| `blobs` | `{"root": path}` for `FsBlobStore::open`. The `data` volume is `/var/lib/crosstalk` (owned by the runtime user), so the blob root and anything the gateway keeps beside it (P3's exchange log in the blob root's parent) persist. |
| `embeddings` | `{"base_url": String, "model": String, "api_key": {"env": String}}`, an OpenAI-compatible endpoint. |

Optional keys the gateway also accepts, all defaulted when absent: `bus`,
`pipeline` (blob put retries) and `shutdown` (`drain_timeout_ms` 45 000 +
`flush_timeout_ms` 10 000, chosen to fit compose's 60 s
`stop_grace_period`). `docs/features/gateway.md` is the authoritative
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
   - Agents send requests to `:8080`, and the proxy forwards them upstream.
   - Capture feeds the in-process pipeline, which writes to Postgres and
     the `data` volume (`/var/lib/crosstalk`).
   - The UI and operators read through `:8081`.
4. Observability:
   - Prometheus scrapes node-exporter (over host networking, via
     `host.docker.internal`), cAdvisor, postgres-exporter, Loki, Alloy,
     Grafana and `crosstalk:9464`.
   - Alloy tails every container in the `crosstalk` project through the
     Docker socket and pushes its logs to Loki.
   - Grafana is provisioned with both datasources and the dashboard
     "crosstalk / infrastructure".

## Files

| File | Role |
|---|---|
| `deploy/compose.yaml` | Every service, volume, port bind and healthcheck. Only the proxy binds beyond 127.0.0.1. |
| `deploy/crosstalk.Dockerfile` (+ `.dockerignore`) | Builds the gateway from the repository root on the nightly in `rust-toolchain.toml` (minimal profile). The runtime image is distroless `cc-debian13:nonroot`. |
| `deploy/ui.Dockerfile` (+ `.dockerignore`) | Builds the elements bundle (pnpm 11.27.1, frozen lockfile), then the Topcoat binary. The context is the repository root, because `ui/` depends on `spec/` by path and `spec/` inherits from the root workspace. |
| `deploy/config/crosstalk.json` | Gateway config (contract above). |
| `deploy/config/ui.json` | UI config (fixture backend until the HTTP backend lands). |
| `deploy/.env.example` | Every variable compose reads, with the defaults. |
| `deploy/run.sh` | `init`, `up`, `infra`, `down`, `logs`, `ps`, `psql`, `urls`. |
| `deploy/postgres/init/` | First-start SQL: extensions and the monitor role. |
| `deploy/prometheus/prometheus.yml` | Scrape jobs: `prometheus`, `node`, `cadvisor`, `postgres`, `loki`, `alloy`, `grafana`, `crosstalk`. |
| `deploy/prometheus/rules/infrastructure.yml` | Infra alert rules (group `crosstalk-infra`). |
| `deploy/grafana/provisioning/` | Datasources (uids `prometheus`, `loki`) and the dashboard provider. |
| `deploy/grafana/dashboards/infrastructure.json` | Host, containers, Postgres, logs and monitoring-stack panels, plus a crosstalk-process row. |
| `deploy/loki/loki.yaml` | Single-binary Loki on the filesystem, 7-day retention. |
| `deploy/alloy/config.alloy` | Docker log discovery, `service`/`container`/`stream` labels, JSON `level` label for crosstalk, migrate and ui. |

## Invariants and constraints

- **Secrets never live in a tracked file.**
  - `deploy/.env` is git-ignored and written with mode 600.
  - Config files only name environment variables.
  - Each container receives only the secrets it uses.
- **Only the proxy listens off-host.** Every other port binds 127.0.0.1.
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
