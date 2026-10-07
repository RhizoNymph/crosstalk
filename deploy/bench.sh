# shellcheck shell=bash disable=SC2154
# `bash deploy/run.sh bench ...`: one scored detection benchmark on the demo
# stack. Sourced by run.sh, which provides `here`, `env_file`,
# `compose_files`, `compose` and `need_env` (hence SC2154 off). See
# docs/features/bench.md.
#
# Steps, in order (each is one function below):
#   1. confirm_restart          say what restarts; ask unless --yes
#      fresh_database           a new database for the run (crosstalk_bench_<run>)
#   2. start_stack              `up -d --build` with the demo override
#   3. require_detection        fail fast unless /readyz has `live` and `api`
#                               running and the API takes the token
#   4. fresh_world              restart wiki and crosstalk, wait for health,
#                               check step 3 again
#   5. run_swarm                the swarm, writing ground truth v2
#   6. wait_caught_up           /healthz `live.watermark_micros` past the
#                               swarm's end (exports are cut at the watermark)
#   7. fetch_detections         ct-eval swarm-fetch (export + evidence)
#      snapshot_inputs          copy the exchange log and blobs into the run dir
#      bench_detect_inputs      ct-bench-detect fetch (/query answers) and from-export
#   8. score                    ct-eval swarm, headline, ct-eval's exit code
# Nothing after step 4 may restart or recreate crosstalk: its detection
# state is in memory. Every `compose run` here passes --no-deps for that.

# Inside the compose network.
bench_api_url="http://crosstalk:8081"
# Where the `data` volume is mounted (read-only) in the bench container;
# the gateway's data directory (blobs.root's parent).
bench_data_dir="/var/lib/crosstalk"
bench_gates="/usr/local/share/crosstalk-eval/gates.toml"
# Seeds at or above this are reserved for holdout runs (the n-th holdout run
# uses 1_000_000 + n); development runs stay below it, so a dev run can never
# spend a holdout seed.
bench_holdout_seed=1000000

bench_usage() {
    sed -n '18,23p' "${here}/run.sh" >&2
    cat >&2 <<'USAGE'

  --agents N             swarm agents (default 20)
  --duration D           swarm run time, e.g. 90s, 2m (default 2m)
  --seed N               swarm seed (default 42)
  --scenario S           headline (default): high-entropy model prose, the
                         headline precision/recall; boilerplate: templated
                         prose unrelated agents share, a regression scenario
                         for false positives on shared text
  --claude-code-shape    Claude Code request shape (system turns, headers)
  --settle-timeout SECS  give up waiting for the watermark after this long
                         (default 900; with the demo flow config it is
                         reached about 70 s to 6 min after the swarm ends)
  --holdout              a holdout run for the bench's release scoring: needs
                         a seed >= 1000000, writes deploy/bench/holdout/<run>/,
                         stops after fetching (no scoring, no report/, no
                         metrics printed). See docs/features/bench.md.
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

# The operator API as published on this host (CROSSTALK_BIND, as ops_url).
api_url() {
    local bind port
    bind="$(env_value CROSSTALK_BIND)"
    port="$(env_value CROSSTALK_API_PORT)"
    [[ -z "$bind" || "$bind" == "0.0.0.0" ]] && bind="127.0.0.1"
    printf 'http://%s:%s\n' "$bind" "${port:-8081}"
}

positive_int() {
    [[ "$2" =~ ^[1-9][0-9]*$ ]] || bench_fail "$1 needs a positive integer, got '$2'"
}

# A flow setting from the demo gateway config, for the run record.
demo_flow_ms() {
    sed -n "s/.*\"$1\":[[:space:]]*\([0-9][0-9]*\).*/\1/p" "${here}/demo/crosstalk.demo.json" | head -n 1
}

# 1. Say what is about to restart; ask unless --yes.
confirm_restart() {
    local yes="$1" answer
    cat >&2 <<'EOF'
run.sh bench is about to:
  - start the demo stack (`up -d --build` with compose.demo.yaml); services
    whose image or config changed are recreated;
  - create a fresh database for the run (crosstalk_bench_<run>), migrate it
    and point `crosstalk` at it, so earlier runs' detection state (kept in
    their own databases, never deleted) cannot affect this one;
  - restart `wiki`: its pages live in memory and are lost (a fresh world).
The exchange log and blobs on the `data` volume are kept. A later plain
`run.sh up` points crosstalk back at the `crosstalk` database.
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

# How many per-run bench databases exist and their total size, so whoever
# asks the user about dropping old ones knows when it matters. Nothing here
# drops anything.
bench_database_tally() {
    compose exec -T postgres psql -At -U crosstalk -d crosstalk -c \
        "SELECT count(*) || ' bench databases, ' || pg_size_pretty(coalesce(sum(pg_database_size(datname)), 0)) FROM pg_database WHERE datname LIKE 'crosstalk\_bench\_%'" \
        2>/dev/null || echo "bench databases: unknown (postgres not reachable)"
}

# A fresh database for run `stamp`, so nothing earlier runs left in Postgres
# (fingerprints, token observations, transmissions) reaches this one. The
# gateway and `migrate` read its name from CROSSTALK_DB_NAME (compose.yaml's
# DATABASE_URL); start_stack then migrates it and recreates crosstalk on it.
fresh_database() {
    local stamp="$1" name
    name="crosstalk_bench_${stamp,,}"
    compose up -d postgres
    wait_healthy postgres 120
    compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U crosstalk -d crosstalk \
        -c "CREATE DATABASE \"${name}\"" \
        || bench_fail "could not create the run's database ${name}"
    CROSSTALK_DB_NAME="$name"
    export CROSSTALK_DB_NAME
    echo "run.sh bench: database ${name}" >&2
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

# Whether /readyz lists task `$1` as running.
ready_task() {
    grep -Eq "\"name\":[[:space:]]*\"$1\",[[:space:]]*\"running\":[[:space:]]*true" <<<"$2"
}

# 3. Fail fast, before a run's worth of traffic: the gateway must run the
# Live detection pipeline (`live`: every layer stage task running) and serve
# the operator API (`api`: listener bound), and the API must take our token.
# Without `live` the export is empty and the score a real but meaningless
# zero; without `api` there is no export at all.
require_detection() {
    local ready code
    ready="$(curl -sS --max-time 5 "$(ops_url)/readyz")" \
        || bench_fail "$(ops_url)/readyz did not answer"
    if ! ready_task api "$ready"; then
        echo "run.sh bench: the gateway does not expose detections (no running \`api\` task in /readyz); see docs/features/bench.md" >&2
        echo "  /readyz: ${ready}" >&2
        exit 1
    fi
    if ! ready_task live "$ready"; then
        echo "run.sh bench: the gateway is not running the Live detection pipeline (no running \`live\` task in /readyz); see docs/features/bench.md" >&2
        echo "  /readyz: ${ready}" >&2
        exit 1
    fi
    code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
        -H "Authorization: Bearer $(env_value CROSSTALK_API_TOKEN)" \
        "$(api_url)/operators")" || bench_fail "$(api_url)/operators did not answer"
    [[ "$code" == 200 ]] \
        || bench_fail "the operator API refused CROSSTALK_API_TOKEN from deploy/.env (GET /operators: HTTP ${code})"
}

# 4. A fresh world: an empty wiki (so the run has no unattributed reads) and
# empty detection state. Done once, at the start of the run.
fresh_world() {
    compose restart wiki crosstalk
    wait_healthy wiki 120
    wait_healthy crosstalk 180
    require_detection
}

# 5. The swarm against the real gateway (through the fake upstream), writing
# ground truth v2 into the run directory. Runs as the invoking user so the
# files are theirs.
# The swarm's report: to the run directory, and to the terminal unless this
# is a holdout run (whose numbers nobody should see before release scoring).
bench_swarm_out() {
    if [[ "$1" == 1 ]]; then cat >"$2"; else tee "$2"; fi
}

run_swarm() {
    local run="$1" quiet="$2"
    shift 2
    compose --profile swarm run --rm --no-deps -T \
        --user "${BENCH_UID}:${BENCH_GID}" \
        -v "${here}/bench:/bench" \
        swarm "$@" --ground-truth "/bench/${run}/truth.jsonl" \
        | bench_swarm_out "$quiet" "${here}/bench/${run}/swarm.txt" \
        || bench_fail "the swarm failed; see ${here}/bench/${run}/swarm.txt"
    [[ -s "${here}/bench/${run}/truth.jsonl" ]] \
        || bench_fail "the swarm wrote no ground truth at deploy/bench/${run}/truth.jsonl"
}

# /healthz's `live.watermark_micros`, saving the body as the run's last
# snapshot; empty when the gateway has no `live` section.
healthz_watermark() {
    local out="$1" body
    body="$(curl -fsS --max-time 5 "$(ops_url)/healthz")" \
        || bench_fail "$(ops_url)/healthz did not answer"
    printf '%s\n' "$body" >"$out"
    sed -n 's/.*"watermark_micros":[[:space:]]*\([0-9][0-9]*\).*/\1/p' <<<"$body"
}

# 6. Caught up: Live's watermark has passed the moment the swarm stopped.
# The watermark advances only when every layer group is empty, trails the
# clock by evidence_window_ms + suspected_ttl_ms, and is aligned down to the
# 5-minute bucket; exports are cut at it, so before this the export would
# miss the run's tail.
wait_caught_up() {
    local run="$1" end_ms="$2" timeout="$3" watermark deadline
    deadline=$((SECONDS + timeout))
    echo "run.sh bench: waiting for the watermark to pass the swarm's end ($(date -u -d "@$((end_ms / 1000))" +%H:%M:%SZ))" >&2
    while true; do
        watermark="$(healthz_watermark "${here}/bench/${run}/healthz.json")"
        [[ -n "$watermark" ]] \
            || bench_fail "/healthz has no live.watermark_micros; is serve running Live? (see ${here}/bench/${run}/healthz.json)"
        if ((watermark / 1000 >= end_ms)); then
            echo "run.sh bench: caught up (watermark $(date -u -d "@$((watermark / 1000000))" +%H:%M:%SZ))" >&2
            return 0
        fi
        ((SECONDS >= deadline)) \
            && bench_fail "the watermark is still at $(date -u -d "@$((watermark / 1000000))" +%H:%M:%SZ) after ${timeout}s; the detection state is still in crosstalk until it restarts"
        sleep 10
    done
}

# 7. The gateway's detections: the transmissions export and each one's
# evidence, saved as export.jsonl and evidence.jsonl in the run directory.
fetch_detections() {
    local run="$1" quiet="${2:-0}" dir rc=0 out
    dir="${here}/bench/${run}"
    if [[ "$quiet" != 1 ]]; then
        compose --profile bench run --rm --no-deps -T bench \
            swarm-fetch --api "$bench_api_url" --token-env CROSSTALK_API_TOKEN \
            --truth "/bench/${run}/truth.jsonl" --out "/bench/${run}" \
            || bench_fail "ct-eval swarm-fetch failed; the detection state is still in crosstalk until it restarts"
        return
    fi
    # Holdout: swarm-fetch's own output summarises the detector (rows per
    # state), so it is not kept. fetch.log records only success, file names
    # and byte sizes. On failure the output is kept in fetch.err for
    # debugging: a failed fetch is not a usable holdout anyway.
    out="$(compose --profile bench run --rm --no-deps -T bench \
        swarm-fetch --api "$bench_api_url" --token-env CROSSTALK_API_TOKEN \
        --truth "/bench/${run}/truth.jsonl" --out "/bench/${run}" 2>&1)" || rc=$?
    if ((rc != 0)); then
        printf '%s\n' "$out" >"${dir}/fetch.err"
        echo "swarm-fetch failed (exit ${rc}); output in fetch.err" >"${dir}/fetch.log"
        bench_fail "ct-eval swarm-fetch failed (exit ${rc}); see ${dir}/fetch.err (not a usable holdout run)"
    fi
    {
        echo "swarm-fetch ok"
        for f in export.jsonl evidence.jsonl; do
            [[ -f "${dir}/${f}" ]] && echo "${f} $(wc -c <"${dir}/${f}") bytes"
        done
    } >"${dir}/fetch.log"
}

# After the export: copy the gateway's exchange log and blob store into the
# run directory, so the run can be re-scored anywhere (ct-eval swarm needs
# the truth, the exchange log, the blobs and the export). Both accumulate
# across runs (blobs are content-addressed), so this copies everything so
# far; the importer joins on the run's sessions.
snapshot_inputs() {
    local run="$1" id dir="${here}/bench/${run}"
    id="$(compose ps -q crosstalk)"
    [[ -n "$id" ]] || bench_fail "no crosstalk container to copy the exchange log and blobs from"
    docker cp "${id}:${bench_data_dir}/exchanges/exchange-log.jsonl" "${dir}/exchange-log.jsonl" \
        && docker cp "${id}:${bench_data_dir}/blobs" "${dir}/blobs" \
        || bench_fail "could not copy the exchange log and blobs into ${dir}"
}

# One ct-bench-detect step in the bench container; its output goes to
# <run>/<name>.log, and to the terminal too unless this is a holdout run.
bench_detect_step() {
    local run="$1" quiet="$2" name="$3" dir rc=0 out
    dir="${here}/bench/${run}"
    shift 3
    out="$(compose --profile bench run --rm --no-deps -T \
        --entrypoint /usr/local/bin/ct-bench-detect bench "$@" 2>&1)" || rc=$?
    if [[ "$quiet" == 1 ]]; then
        printf '%s\n' "$out" >"${dir}/${name}.log"
    else
        printf '%s\n' "$out" | tee "${dir}/${name}.log"
    fi
    ((rc == 0)) || bench_fail "ct-bench-detect ${name} failed (exit ${rc}); see ${dir}/${name}.log"
}

# After the snapshot, while the gateway still holds the run's detections:
# `ct-bench-detect fetch` saves the gateway's /query answers
# (exchange-turns.json, span-points.json) beside the export, which only a
# live gateway can give; `from-export` turns the run directory into
# a2a-transmission-bench input (bench-input/: manifest.json, messages.jsonl,
# exchanges.jsonl, predictions.jsonl). Neither scores anything.
bench_detect_inputs() {
    local run="$1" quiet="$2"
    bench_detect_step "$run" "$quiet" detect-fetch \
        fetch --api "$bench_api_url" --token-env CROSSTALK_API_TOKEN \
        --truth "/bench/${run}/truth.jsonl" --out "/bench/${run}"
    bench_detect_step "$run" "$quiet" from-export \
        from-export --run "/bench/${run}" --out "/bench/${run}/bench-input"
}

# 8. Score offline against the exchange log and blobs read in place on the
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
    echo "bench ${run} ($(sed -n 's/^scenario=//p' "${dir}/bench.env"))"
    grep -m 1 '^overall:' "${dir}/report/report.txt" || true
    if grep -q '^gates:' "${dir}/report/report.txt"; then
        sed -n '/^gates:/,/^$/p' "${dir}/report/report.txt"
    else
        echo "gates: none defined for this dataset in ${bench_gates}"
    fi
    if ((rc == 2)); then echo "result: GATE FAILED"; else echo "result: pass"; fi
    echo "report: ${dir}/report/report.txt"
    return "$rc"
}

bench() {
    local agents=20 duration=2m seed=42 scenario=headline shape=0 yes=0 timeout=900 run rc=0 end_ms
    local holdout=0
    local extra=()
    while (($# > 0)); do
        case "$1" in
            --agents) positive_int "$1" "${2-}"; agents="$2"; shift 2 ;;
            --duration) [[ -n "${2-}" ]] || bench_fail "--duration needs a value"; duration="$2"; shift 2 ;;
            --seed) [[ "${2-}" =~ ^[0-9]+$ ]] || bench_fail "--seed needs an integer"; seed="$2"; shift 2 ;;
            --scenario)
                [[ "${2-}" == headline || "${2-}" == boilerplate ]] \
                    || bench_fail "--scenario needs headline or boilerplate, got '${2-}'"
                scenario="$2"; shift 2 ;;
            --claude-code-shape) shape=1; shift ;;
            --settle-timeout) positive_int "$1" "${2-}"; timeout="$2"; shift 2 ;;
            --yes | -y) yes=1; shift ;;
            --holdout) holdout=1; shift ;;
            --) shift; extra=("$@"); break ;;
            -h | --help) bench_usage; exit 0 ;;
            *) bench_usage; exit 2 ;;
        esac
    done
    if ((holdout == 1)); then
        ((seed >= bench_holdout_seed)) \
            || bench_fail "--holdout needs a reserved seed (>= ${bench_holdout_seed}: 1000000 + n for the n-th holdout run), got ${seed}"
    else
        ((seed < bench_holdout_seed)) \
            || bench_fail "seed ${seed} is reserved for holdout runs (>= ${bench_holdout_seed}); pass --holdout or a seed below it"
    fi
    need_env
    compose_files+=(-f "${here}/compose.demo.yaml")
    command -v curl >/dev/null 2>&1 || bench_fail "needs curl on this host (to read /readyz and /healthz)"
    BENCH_UID="$(id -u)"
    BENCH_GID="$(id -g)"
    export BENCH_UID BENCH_GID

    # Created by this user before any bind mount of it, or docker creates it
    # as root.
    mkdir -p "${here}/bench"

    confirm_restart "$yes"
    local stamp
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    fresh_database "$stamp"
    start_stack
    wait_healthy crosstalk 180
    require_detection
    fresh_world

    run="$stamp"
    if ((holdout == 1)); then
        mkdir -p "${here}/bench/holdout"
        run="holdout/${run}"
    fi
    mkdir "${here}/bench/${run}" || bench_fail "deploy/bench/${run} already exists"
    local swarm_args=(--agents "$agents" --duration "$duration" --seed "$seed" --scenario "$scenario")
    ((shape == 1)) && swarm_args+=(--claude-code-shape)
    {
        echo "run=${run}"
        echo "scenario=${scenario}"
        echo "seed=${seed}"
        echo "holdout=${holdout}"
        # The crosstalk commit this checkout was synced from (the sync
        # writes deploy/SOURCE_COMMIT; a rsynced copy has no .git).
        echo "crosstalk_commit=$(cat "${here}/SOURCE_COMMIT" 2>/dev/null || echo unknown)"
        echo "database=${CROSSTALK_DB_NAME}"
        echo "swarm=${swarm_args[*]} ${extra[*]}"
        echo "evidence_window_ms=$(demo_flow_ms evidence_window_ms)"
        echo "suspected_ttl_ms=$(demo_flow_ms suspected_ttl_ms)"
        echo "crosstalk_image=$(docker image inspect --format '{{.Id}}' crosstalk:dev)"
        echo "demo_image=$(docker image inspect --format '{{.Id}}' crosstalk-demo:dev)"
    } >"${here}/bench/${run}/bench.env"
    echo "run.sh bench: run ${run} (scenario ${scenario}) -> deploy/bench/${run}/" >&2

    run_swarm "$run" "$holdout" "${swarm_args[@]}" "${extra[@]}"
    # Whole seconds: `date +%3N` is not portable (uutils prints nanoseconds),
    # and the watermark moves in 5-minute buckets.
    end_ms="$(($(date +%s) * 1000))"
    echo "swarm_end_unix_ms=${end_ms}" >>"${here}/bench/${run}/bench.env"
    wait_caught_up "$run" "$end_ms" "$timeout"
    fetch_detections "$run" "$holdout"
    snapshot_inputs "$run"
    echo "run.sh bench: $(bench_database_tally)" >&2
    bench_detect_inputs "$run" "$holdout"
    if ((holdout == 1)); then
        # Holdout: no scoring, no report/, no metrics; just say what was saved.
        echo "holdout run ${run#holdout/} (scenario ${scenario}, seed ${seed}) saved, unscored:"
        ls -1 "${here}/bench/${run}" | sed 's/^/  /'
        echo "copy it to the dataset root on the bench machine:"
        echo "  rsync -a <this host>:${here}/bench/${run}/ ~/Data/ai/agents/demo-swarm-holdout/${run#holdout/}/"
        exit 0
    fi
    score "$run" || rc=$?
    exit "$rc"
}
