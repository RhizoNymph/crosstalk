# shellcheck shell=bash disable=SC2154
# `bash deploy/run.sh bench ...`: one scored detection benchmark on the demo
# stack. Sourced by run.sh, which provides `here`, `env_file`,
# `compose_files`, `compose` and `need_env` (hence SC2154 off). See
# docs/features/bench.md.
#
# Steps, in order (each is one function below):
#   1. confirm_restart          say what restarts; ask unless --yes
#   2. start_stack              `up -d --build` with the demo override
#   3. require_detection_api    fail fast unless http://crosstalk:8081 answers
#   4. fresh_world              restart wiki and crosstalk, wait for health
#   5. require_live_pipeline    (pending) serve runs the Live detection pipeline
#   6. run_swarm                the swarm, writing ground truth v2
#   7. wait_caught_up           /healthz counters stable over one evidence window
#   8. fetch_detections         ct-eval swarm-fetch (export + evidence)
#   9. score                    ct-eval swarm, headline, ct-eval's exit code
# Nothing after step 4 may restart or recreate crosstalk: its detection
# state is in memory. Every `compose run` here passes --no-deps for that.

# Inside the compose network.
bench_api_url="http://crosstalk:8081"
# Where the `data` volume is mounted (read-only) in the bench container;
# the gateway's data directory (blobs.root's parent).
bench_data_dir="/var/lib/crosstalk"
bench_gates="/usr/local/share/crosstalk-eval/gates.toml"

bench_usage() {
    sed -n '18,23p' "${here}/run.sh" >&2
    cat >&2 <<'USAGE'

  --agents N             swarm agents (default 20)
  --duration D           swarm run time, e.g. 90s, 2m (default 2m)
  --seed N               swarm seed (default 42)
  --claude-code-shape    Claude Code request shape (system turns, headers)
  --settle SECS          the evidence window the counters must hold still for (default 10)
  --settle-timeout SECS  give up settling after this long (default 300)
  --yes                  do not ask before restarting wiki and crosstalk
  -- ...                 further crosstalk-demo swarm options, passed as is
USAGE
}

bench_fail() {
    echo "run.sh bench: $*" >&2
    exit 1
}

# A deploy/.env setting as compose sees it: the environment wins over the
# file, as in compose's own interpolation.
env_value() {
    local key="$1"
    if [[ -n "${!key-}" ]]; then
        printf '%s\n' "${!key}"
        return
    fi
    sed -n "s/^${key}=//p" "$env_file" | tail -n 1
}

# The ops listener as published on this host. It binds CROSSTALK_BIND, which
# may be the tailnet IP rather than 127.0.0.1.
ops_url() {
    local bind port
    bind="$(env_value CROSSTALK_BIND)"
    port="$(env_value CROSSTALK_OPS_PORT)"
    [[ -z "$bind" || "$bind" == "0.0.0.0" ]] && bind="127.0.0.1"
    printf 'http://%s:%s\n' "$bind" "${port:-9464}"
}

positive_int() {
    [[ "$2" =~ ^[1-9][0-9]*$ ]] || bench_fail "$1 needs a positive integer, got '$2'"
}

# The default for --settle: the gateway's evidence window, in seconds.
default_settle_secs() {
    # TODO(live-flow-config): read flow `evidence_window_ms` from deploy/demo/crosstalk.demo.json once the gateway accepts the flow keys and their place in the config is decided; until then this is the recommended bench value (10000 ms).
    echo 10
}

# 1. Say what is about to restart; ask unless --yes.
confirm_restart() {
    local yes="$1" answer
    cat >&2 <<'EOF'
run.sh bench is about to:
  - start the demo stack (`up -d --build` with compose.demo.yaml); services
    whose image or config changed are recreated;
  - restart `wiki`: its pages live in memory and are lost (a fresh world);
  - restart `crosstalk`: its detection state lives in memory and is lost.
The exchange log and blobs on the `data` volume are kept.
EOF
    [[ "$yes" == 1 ]] && return 0
    [[ -t 0 ]] || bench_fail "stdin is not a terminal; pass --yes to restart without asking"
    read -r -p "Continue? [y/N] " answer
    [[ "$answer" =~ ^([yY]|[yY][eE][sS])$ ]] || bench_fail "not confirmed; nothing was restarted"
}

# 2. The demo stack, built (the image must carry ct-eval) and running.
start_stack() {
    compose up -d --build
}

# Wait for a compose service's container to report healthy.
wait_healthy() {
    local service="$1" timeout="$2" id status deadline
    deadline=$((SECONDS + timeout))
    while true; do
        id="$(compose ps -q "$service")"
        status="missing"
        if [[ -n "$id" ]]; then
            status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id")"
        fi
        [[ "$status" == "healthy" ]] && return 0
        ((SECONDS >= deadline)) && bench_fail "${service} is ${status} after ${timeout}s; see: bash deploy/run.sh demo logs ${service}"
        sleep 2
    done
}

# 3. Fail fast, before a run's worth of traffic, when nothing answers on the
# API port. The demo image's healthcheck says "answered <status>" when an
# HTTP server is there but the root is not a 2xx: that is still bound.
require_detection_api() {
    local out rc=0
    out="$(compose --profile bench run --rm --no-deps -T \
        --entrypoint /usr/local/bin/crosstalk-demo bench \
        healthcheck --url "$bench_api_url" 2>&1)" || rc=$?
    if ((rc == 0)) || grep -Eq 'answered [0-9]{3}' <<<"$out"; then
        return 0
    fi
    echo "run.sh bench: the gateway does not expose detections yet (api.listen not bound); see docs/features/bench.md" >&2
    echo "  probe of ${bench_api_url}: $(tail -n 1 <<<"$out")" >&2
    exit 1
}

# 4. A fresh world: an empty wiki (so the run has no unattributed reads) and
# empty detection state. Done once, at the start of the run.
fresh_world() {
    compose restart wiki crosstalk
    wait_healthy wiki 120
    wait_healthy crosstalk 180
}

# 5. The API answering is not enough: serve must also run the detection
# pipeline, or the export is empty and the score a real but meaningless zero.
require_live_pipeline() {
    # TODO(live-serve): fail here unless serve runs the Live detection pipeline; needs the gateway to report it (e.g. a `live` task in /readyz `tasks`, or a /healthz section) so this can check it the way require_detection_api checks the port.
    echo "run.sh bench: warning: cannot yet check that serve runs the Live pipeline; an empty export scores zero" >&2
}

# 6. The swarm against the real gateway (through the fake upstream), writing
# ground truth v2 into the run directory. Runs as the invoking user so the
# files are theirs.
run_swarm() {
    local run="$1"
    shift
    compose --profile swarm run --rm --no-deps -T \
        --user "${BENCH_UID}:${BENCH_GID}" \
        -v "${here}/bench:/bench" \
        swarm "$@" --ground-truth "/bench/${run}/truth.jsonl" \
        | tee "${here}/bench/${run}/swarm.txt" \
        || bench_fail "the swarm failed; see ${here}/bench/${run}/swarm.txt"
    [[ -s "${here}/bench/${run}/truth.jsonl" ]] \
        || bench_fail "the swarm wrote no ground truth at deploy/bench/${run}/truth.jsonl"
}

# One /healthz counter: the number under "<section>": {... "<key>": N ...}.
healthz_counter() {
    sed -n "s/.*\"$1\":[[:space:]]*{[^}]*\"$2\":[[:space:]]*\([0-9][0-9]*\).*/\1/p" <<<"$3"
}

# "<captured> <published>" from /healthz, saved as the run's last snapshot.
healthz_sample() {
    local out="$1" body captured published
    body="$(curl -fsS --max-time 5 "$(ops_url)/healthz")" \
        || bench_fail "$(ops_url)/healthz did not answer"
    captured="$(healthz_counter capture captured "$body")"
    published="$(healthz_counter pipeline published "$body")"
    [[ -n "$captured" && -n "$published" ]] \
        || bench_fail "/healthz has no capture.captured or pipeline.published: ${body}"
    printf '%s\n' "$body" >"$out"
    echo "${captured} ${published}"
}

# 7. Caught up: the capture count and pipeline.published unchanged across
# one evidence window after the swarm stops.
wait_caught_up() {
    local run="$1" settle="$2" timeout="$3" previous current deadline
    # TODO(live-lag): replace this counter-stability heuristic with the gateway's detection lag gauge (Live's watermark caught up to the last published exchange) once it is exposed on /healthz or /metrics.
    deadline=$((SECONDS + timeout))
    previous="$(healthz_sample "${here}/bench/${run}/healthz.json")"
    echo "run.sh bench: settling: captured/published ${previous}, window ${settle}s" >&2
    while true; do
        sleep "$settle"
        current="$(healthz_sample "${here}/bench/${run}/healthz.json")"
        if [[ "$current" == "$previous" ]]; then
            echo "run.sh bench: settled at captured/published ${current}" >&2
            return 0
        fi
        ((SECONDS >= deadline)) && bench_fail "not settled after ${timeout}s (captured/published ${previous} -> ${current})"
        previous="$current"
    done
}

# 8. The gateway's detections: the transmissions export and each one's
# evidence, saved as export.jsonl and evidence.jsonl in the run directory.
fetch_detections() {
    local run="$1"
    # TODO(live-http): needs serve to mount the L8 HTTP API on api.listen (8081) with POST /exports and GET /transmissions/{id}/evidence behind the api.token bearer (CROSSTALK_API_TOKEN); this call is already the agreed one and needs no change once that lands.
    compose --profile bench run --rm --no-deps -T bench \
        swarm-fetch --api "$bench_api_url" --token-env CROSSTALK_API_TOKEN \
        --truth "/bench/${run}/truth.jsonl" --out "/bench/${run}" \
        || bench_fail "ct-eval swarm-fetch failed; the detection state is still in crosstalk until it restarts"
}

# 9. Score offline against the exchange log and blobs read in place on the
# data volume; print the headline and return ct-eval's exit code (2: a gate
# failed).
score() {
    local run="$1" rc=0 dir="${here}/bench/${run}"
    compose --profile bench run --rm --no-deps -T bench \
        swarm --truth "/bench/${run}/truth.jsonl" \
        --exchanges "${bench_data_dir}/exchanges/exchange-log.jsonl" \
        --blobs "${bench_data_dir}/blobs" \
        --export "/bench/${run}/export.jsonl" \
        --evidence "/bench/${run}/evidence.jsonl" \
        --gates "$bench_gates" \
        --out "/bench/${run}/report" >"${dir}/score.txt" || rc=$?
    if ((rc != 0 && rc != 2)); then
        echo "run.sh bench: ct-eval swarm failed (exit ${rc}); output in ${dir}/score.txt" >&2
        return "$rc"
    fi
    echo
    echo "bench ${run}"
    grep -m 1 '^overall:' "${dir}/report/report.txt" || true
    if grep -q '^gates:' "${dir}/report/report.txt"; then
        sed -n '/^gates:/,/^$/p' "${dir}/report/report.txt"
    else
        echo "gates: none apply to demo-swarm"
    fi
    if ((rc == 2)); then echo "result: GATE FAILED"; else echo "result: pass"; fi
    echo "report: ${dir}/report/report.txt"
    return "$rc"
}

bench() {
    local agents=20 duration=2m seed=42 shape=0 yes=0 timeout=300 settle run rc=0
    local extra=()
    settle="$(default_settle_secs)"
    while (($# > 0)); do
        case "$1" in
            --agents) positive_int "$1" "${2-}"; agents="$2"; shift 2 ;;
            --duration) [[ -n "${2-}" ]] || bench_fail "--duration needs a value"; duration="$2"; shift 2 ;;
            --seed) [[ "${2-}" =~ ^[0-9]+$ ]] || bench_fail "--seed needs an integer"; seed="$2"; shift 2 ;;
            --claude-code-shape) shape=1; shift ;;
            --settle) positive_int "$1" "${2-}"; settle="$2"; shift 2 ;;
            --settle-timeout) positive_int "$1" "${2-}"; timeout="$2"; shift 2 ;;
            --yes | -y) yes=1; shift ;;
            --) shift; extra=("$@"); break ;;
            -h | --help) bench_usage; exit 0 ;;
            *) bench_usage; exit 2 ;;
        esac
    done
    need_env
    compose_files+=(-f "${here}/compose.demo.yaml")
    command -v curl >/dev/null 2>&1 || bench_fail "needs curl on this host (to read /healthz)"
    BENCH_UID="$(id -u)"
    BENCH_GID="$(id -g)"
    export BENCH_UID BENCH_GID

    # Created by this user before any bind mount of it, or docker creates it
    # as root.
    mkdir -p "${here}/bench"

    confirm_restart "$yes"
    start_stack
    wait_healthy crosstalk 180
    require_detection_api
    fresh_world
    require_live_pipeline

    run="$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir "${here}/bench/${run}" || bench_fail "deploy/bench/${run} already exists"
    local swarm_args=(--agents "$agents" --duration "$duration" --seed "$seed")
    ((shape == 1)) && swarm_args+=(--claude-code-shape)
    {
        echo "run=${run}"
        echo "swarm=${swarm_args[*]} ${extra[*]}"
        echo "settle_secs=${settle}"
        echo "crosstalk_image=$(docker image inspect --format '{{.Id}}' crosstalk:dev)"
        echo "demo_image=$(docker image inspect --format '{{.Id}}' crosstalk-demo:dev)"
    } >"${here}/bench/${run}/bench.env"
    echo "run.sh bench: run ${run} -> deploy/bench/${run}/" >&2

    run_swarm "$run" "${swarm_args[@]}" "${extra[@]}"
    wait_caught_up "$run" "$settle" "$timeout"
    fetch_detections "$run"
    score "$run" || rc=$?
    exit "$rc"
}
