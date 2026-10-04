#!/usr/bin/env bash
# Single-machine deployment helper around deploy/compose.yaml.
#
#   bash deploy/run.sh init     write deploy/.env with generated secrets (once)
#   bash deploy/run.sh up       build and start everything (UI too if ui/ exists)
#   bash deploy/run.sh infra    start only Postgres and the observability stack
#   bash deploy/run.sh down     stop (volumes are kept)
#   bash deploy/run.sh logs [service...]
#   bash deploy/run.sh ps
#   bash deploy/run.sh psql     open psql against the crosstalk database
#   bash deploy/run.sh urls     print where everything listens
#
#   bash deploy/run.sh demo up            the stack against a fake upstream, plus the wiki
#   bash deploy/run.sh demo run [opts]    one swarm run (crosstalk-demo swarm options)
#   bash deploy/run.sh demo down          stop the demo stack (volumes are kept)
#   bash deploy/run.sh demo logs [service...]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
env_file="${here}/.env"
# compose.yaml, plus compose.demo.yaml for the demo subcommands.
compose_files=(-f "${here}/compose.yaml")

compose() {
    local profiles=()
    [[ -d "${here}/../ui" ]] && profiles=(--profile ui)
    # cAdvisor mounts Docker's data root; ask the daemon where it is (snap
    # Docker keeps it under /var/snap) unless deploy/.env pins it.
    if [[ -z "${DOCKER_ROOT_DIR:-}" ]] && ! grep -q '^DOCKER_ROOT_DIR=' "$env_file" 2>/dev/null; then
        DOCKER_ROOT_DIR="$(docker info --format '{{.DockerRootDir}}')"
        export DOCKER_ROOT_DIR
    fi
    docker compose --project-directory "$here" "${compose_files[@]}" \
        --env-file "$env_file" "${profiles[@]}" "$@"
}

need_env() {
    if [[ ! -f "$env_file" ]]; then
        echo "run.sh: ${env_file} missing; run: bash deploy/run.sh init" >&2
        exit 1
    fi
}

hex() { openssl rand -hex "$1"; }

init() {
    if [[ -f "$env_file" ]]; then
        echo "run.sh: ${env_file} already exists; not overwriting" >&2
        exit 1
    fi
    umask 077
    sed \
        -e "s/^POSTGRES_PASSWORD=$/POSTGRES_PASSWORD=$(hex 24)/" \
        -e "s/^POSTGRES_MONITOR_PASSWORD=$/POSTGRES_MONITOR_PASSWORD=$(hex 24)/" \
        -e "s/^CROSSTALK_SECRET_V1=$/CROSSTALK_SECRET_V1=$(hex 32)/" \
        -e "s/^CROSSTALK_API_TOKEN=$/CROSSTALK_API_TOKEN=$(hex 32)/" \
        -e "s/^GRAFANA_ADMIN_PASSWORD=$/GRAFANA_ADMIN_PASSWORD=$(hex 16)/" \
        "${here}/.env.example" >"$env_file"
    echo "run.sh: wrote ${env_file}; set CROSSTALK_EMBEDDINGS_API_KEY in it if embeddings are used" >&2
}

infra_services=(postgres prometheus grafana loki alloy node-exporter cadvisor postgres-exporter)

# The gateway's capture counters, from the host (the ops port binds
# 127.0.0.1 and may be overridden in deploy/.env).
capture_counters() {
    local port
    port="$(sed -n 's/^CROSSTALK_OPS_PORT=//p' "$env_file" | tail -n 1)"
    if command -v curl >/dev/null 2>&1; then
        echo "crosstalk /healthz:"
        curl -fsS "http://127.0.0.1:${port:-9464}/healthz" || echo "(ops port not reachable)"
        echo
    fi
}

demo() {
    need_env
    compose_files+=(-f "${here}/compose.demo.yaml")
    case "${1:-}" in
        up) compose up -d --build ;;
        # `up -d` first, so a stack started with plain `up` is switched to
        # the demo config instead of sending fake keys to the real API.
        run) shift; compose up -d; compose --profile swarm run --rm swarm "$@"; capture_counters ;;
        down) compose --profile swarm down ;;
        logs) shift; compose logs -f --tail=200 "$@" ;;
        *)
            sed -n '13,16p' "$0" >&2
            exit 2
            ;;
    esac
}

case "${1:-}" in
    init) init ;;
    demo) shift; demo "$@" ;;
    up) need_env; compose up -d --build ;;
    infra) need_env; compose up -d "${infra_services[@]}" ;;
    down) need_env; compose down ;;
    logs) need_env; shift; compose logs -f --tail=200 "$@" ;;
    ps) need_env; compose ps ;;
    psql) need_env; compose exec postgres psql -U crosstalk -d crosstalk ;;
    urls)
        need_env
        set -a
        # shellcheck source=/dev/null
        source "$env_file"
        set +a
        cat <<URLS
proxy (agents)   http://<this-host>:${CROSSTALK_PROXY_PORT:-8080}/anthropic   (ANTHROPIC_BASE_URL)
operator API     http://127.0.0.1:${CROSSTALK_API_PORT:-8081}
operator UI      http://127.0.0.1:${CROSSTALK_UI_PORT:-3000}
grafana          http://127.0.0.1:${GRAFANA_PORT:-3001}   (admin / GRAFANA_ADMIN_PASSWORD)
prometheus       http://127.0.0.1:${PROMETHEUS_PORT:-9090}
alloy            http://127.0.0.1:${ALLOY_PORT:-12345}
postgres         postgres://crosstalk@127.0.0.1:${POSTGRES_PORT:-5432}/crosstalk
URLS
        ;;
    *)
        sed -n '2,16p' "$0" >&2
        exit 2
        ;;
esac
