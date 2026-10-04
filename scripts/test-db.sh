#!/usr/bin/env bash
# Starts a disposable Postgres 18 with pgvector and pg_trgm in Docker for the
# crosstalk-store database tests, waits until it accepts connections and both
# extensions are available, and prints the TEST_DATABASE_URL to export.
#
#   bash scripts/test-db.sh       start (or reuse) the container, print the URL
#   bash scripts/test-db.sh url   print the URL of the running container
#   bash scripts/test-db.sh stop  remove the container (its data goes with it)
#
#   eval "$(bash scripts/test-db.sh)"  start and export in one step
#
# The data directory is a tmpfs and durability is off (fsync, synchronous
# commit, full-page writes): the server is for tests only and loses
# everything when stopped. It listens on 127.0.0.1 only.
#
# Overrides (environment):
#   CROSSTALK_TEST_DB_IMAGE      image          (default pgvector/pgvector:pg18)
#   CROSSTALK_TEST_DB_CONTAINER  container name (default crosstalk-test-db)
#   CROSSTALK_TEST_DB_PORT       host port      (default 55432)
#   CROSSTALK_TEST_DB_PASSWORD   superuser password of the throwaway server
#                                (default crosstalk)
set -euo pipefail

image="${CROSSTALK_TEST_DB_IMAGE:-pgvector/pgvector:pg18}"
container="${CROSSTALK_TEST_DB_CONTAINER:-crosstalk-test-db}"
port="${CROSSTALK_TEST_DB_PORT:-55432}"
password="${CROSSTALK_TEST_DB_PASSWORD:-crosstalk}"
timeout_s=60

url="postgres://postgres:${password}@127.0.0.1:${port}/postgres"

log() { echo "test-db: $*" >&2; }

running() {
    [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" == "true" ]]
}

start() {
    if running; then
        log "container ${container} already running"
    else
        # A stopped container with the same name would block `docker run`.
        docker rm -f "$container" >/dev/null 2>&1 || true
        log "starting ${image} as ${container} on 127.0.0.1:${port}"
        docker run --detach --rm \
            --name "$container" \
            --env POSTGRES_PASSWORD="$password" \
            --publish "127.0.0.1:${port}:5432" \
            --tmpfs /var/lib/postgresql:rw \
            "$image" \
            -c max_connections=500 \
            -c fsync=off \
            -c synchronous_commit=off \
            -c full_page_writes=off >/dev/null
    fi
    wait_ready
    check_extensions
    echo "export TEST_DATABASE_URL=${url}"
}

# The image's entrypoint runs a temporary socket-only server during initdb,
# so readiness is checked over TCP, which only the final server accepts.
wait_ready() {
    local waited=0
    until docker exec "$container" pg_isready -q -h 127.0.0.1 -p 5432 -U postgres; do
        if ((waited >= timeout_s)); then
            log "server not ready after ${timeout_s}s; logs follow"
            docker logs "$container" >&2 || true
            exit 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    log "server ready after ${waited}s"
}

check_extensions() {
    local found
    found="$(docker exec "$container" psql -h 127.0.0.1 -U postgres -d postgres -tA \
        -c "SELECT count(*) FROM pg_available_extensions WHERE name IN ('vector', 'pg_trgm')")"
    if [[ "$found" != "2" ]]; then
        log "image ${image} lacks vector or pg_trgm (found ${found} of 2)"
        exit 1
    fi
    log "extensions vector and pg_trgm available"
}

case "${1:-start}" in
    start) start ;;
    url)
        if ! running; then
            log "container ${container} is not running; run scripts/test-db.sh"
            exit 1
        fi
        echo "export TEST_DATABASE_URL=${url}"
        ;;
    stop)
        docker rm -f "$container" >/dev/null 2>&1 || true
        log "removed ${container}"
        ;;
    *)
        echo "usage: scripts/test-db.sh [start|url|stop]" >&2
        exit 2
        ;;
esac
