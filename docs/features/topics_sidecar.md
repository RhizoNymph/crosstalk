# Topics sidecar

Topic modeling (UMAP plus HDBSCAN plus c-TF-IDF labels) and 2-D layouts
(UMAP) run in a small Python HTTP service, `sidecar/topics/`, behind the
spec's `TopicModel` and `LayoutFitter` traits (roadmap D1, P6.3). The Rust
side is three adapters in `crosstalk-analysis` (`crates/analysis`):
`SidecarTopicModel`, `SidecarLayoutFitter` and `OpenAiEmbedder` (the
`Embedder` against an OpenAI-compatible `/embeddings` endpoint, roadmap
P6.2).

## Scope

- The sidecar: its HTTP contract (versioned, below), its computations, its
  determinism guarantee, its configuration, logging, tests and container
  image (`deploy/topics.Dockerfile`).
- The Rust adapters that implement `TopicModel`, `LayoutFitter` and
  `Embedder` over HTTP, with timeouts and typed errors.
- The spec changes the adapters need: `TopicModel::fit` and
  `LayoutFitter::fit` are async, `fit` takes the version the catalog began,
  the documents' texts (c-TF-IDF needs them) and the fit time, and both
  traits gained a backend-failure error that is not a deterministic
  `FitFailure`.

## Non-scope

- The topic catalog and its Postgres storage (`TopicCatalog`,
  `TopicLifecycle`), and the projection store (P6.2/P6.3 storage).
- The `analyze` consumer that triggers re-fits and re-classification, and
  the projection fitter loop.
- Gateway wiring (config keys, constructing the adapters) and
  `deploy/compose.yaml` (owned by the infrastructure track; what compose
  needs is listed under [Running](#running)).

## HTTP contract, version 1

The sidecar listens on port 8090 (`CROSSTALK_TOPICS_PORT`). Every route
other than `/healthz` is under `/v1/`. A breaking change adds `/v2/` routes
and a new section here; `/v1/` keeps its meaning until every client has
moved. `GET /healthz` reports the contract version the service speaks.

Every body is JSON (UTF-8) in the spec's wire conventions
(`docs/features/wire_contract.md`): snake_case keys, every field present,
unknown fields refused, enums adjacently tagged (`{"type": ..., "data":
...}`, a unit variant `{"type": ...}`), raw bytes as lower-case hex. Times
and ids do not occur in version 1: the sidecar is stateless and the Rust
adapter stamps times and mints topic ids.

### Matrices

Embeddings and coordinates travel as a matrix of IEEE-754 binary32 values,
because a JSON number array of 100 000 × 1 536 floats would cost gigabytes
of Python objects:

```json
{"rows": 3, "columns": 2, "data": "0000803f000000000000000000000040cdcccc3d9a99993e"}
```

- `rows`: u32, `columns`: u16 (at least 1).
- `data`: lower-case hex of `rows × columns` little-endian binary32 values,
  row-major, 8 hex digits per value. Its length is exactly
  `rows × columns × 8`. Upper-case hex, a wrong length and any NaN or
  infinity are `invalid_request`.

The encoding is exact: the Rust `f32` bits are the bits UMAP reads, and the
coordinates UMAP writes are the bits the adapter returns.

### `GET /healthz`

`200`, always, while the process serves:

```json
{"status":"ok","contract":"v1","versions":{"numba":"0.67.0","numpy":"2.5.3","python":"3.14.4","scikit_learn":"1.9.1","umap_learn":"0.5.12"}}
```

`versions` (keys ascending) names the libraries whose versions the
determinism guarantee is relative to.

### `POST /v1/topics/fit`

Clusters documents and labels the clusters.

```json
{
  "embeddings": {"rows": 30, "columns": 8, "data": "..."},
  "texts": ["deploy the wiki page", "..."],
  "params": {
    "seed": 7,
    "min_cluster_size": 5,
    "min_samples": null,
    "umap_neighbors": 15,
    "umap_components": 5,
    "top_terms": 10
  }
}
```

| Field | Meaning |
| --- | --- |
| `embeddings` | One row per document (unit vectors from one embedding model). |
| `texts` | One text per row, same order. `len(texts) == rows`, else `invalid_request`. |
| `params.seed` | u64; keys UMAP's random state. |
| `params.min_cluster_size` | u32, at least 2: HDBSCAN's `min_cluster_size`. |
| `params.min_samples` | u32 at least 1, or `null` for HDBSCAN's default (`min_cluster_size`). |
| `params.umap_neighbors` | u16 in `2..=200`: UMAP's `n_neighbors` for the reduction. |
| `params.umap_components` | u16 in `1..=100`: dimensions HDBSCAN clusters in. |
| `params.top_terms` | u16 in `1..=50`: c-TF-IDF terms kept per topic. |

The fit needs `needed = max(min_cluster_size, umap_neighbors + 1,
umap_components + 2)` documents; fewer is `too_few_samples`.

Computation:

1. UMAP (`n_neighbors = umap_neighbors`, `n_components = umap_components`,
   `min_dist = 0.0`, `metric = "cosine"`, seeded as in
   [Determinism](#determinism)) reduces the embeddings.
2. HDBSCAN (scikit-learn's, `metric = "euclidean"`, `cluster_selection_method
   = "eom"`) clusters the reduced points; label `-1` is an outlier.
3. Clusters are renumbered `0..k` by size, largest first, ties to the
   cluster whose first member comes first in the input.
4. c-TF-IDF over the texts, one class per cluster (outliers excluded):
   scikit-learn's `CountVectorizer(lowercase=True, stop_words="english")`
   counts terms per class; each class's counts are L1-normalized; a term's
   weight is `tf × ln(1 + A / f)`, where `A` is the mean number of counted
   terms per class and `f` the term's total count over all classes.
5. A topic's `terms` are its terms with a positive weight, highest first
   (ties to the lexicographically smaller term), at most `top_terms`. Its
   `label` is its first three terms joined by `", "`, or `"topic <k>"`
   when it has none.

Response `200`:

```json
{"labels":[0,0,1,-1,1],"topics":[{"label":"wiki, page, deploy","terms":[["wiki",0.41],["page",0.27],["deploy",0.2]]},{"label":"topic 1","terms":[]}]}
```

- `labels`: one per document, `-1` or a cluster index in `0..len(topics)`.
  Every cluster has at least one member.
- `topics[k]`: cluster `k`. Weights are finite positive JSON numbers.

The adapter, not the sidecar, computes each topic's centroid (the
normalized mean of its members' embeddings), so `analysis.topic.centroid-mean`
holds by construction in Rust.

### `POST /v1/layout/fit`

Lays embeddings out in two dimensions.

```json
{"embeddings": {"rows": 40, "columns": 8, "data": "..."}, "params": {"limit": 100, "neighbors": 15, "min_dist_milli": 100, "seed": 42}}
```

`params` is the spec's `ProjectionParams` JSON, checked as its constructor
checks it (`neighbors` in `2..=200`, `min_dist_milli` at most 1 000,
`limit` in `1..=100000`), plus `rows <= limit`; a violation is
`invalid_request`. Fewer than `neighbors + 1` rows is `too_few_points`.

UMAP runs with `n_neighbors = neighbors`, `n_components = 2`, `min_dist =
min_dist_milli / 1000`, `metric = "cosine"`, seeded as below. A NaN or
infinite coordinate is `non_finite_layout`.

Response `200`: `{"coordinates": {"rows": 40, "columns": 2, "data": "..."}}`,
row `i` the `[x, y]` of embedding `i`.

### `POST /v1/layout/transform`

Places new points onto an existing layout without moving it.

```json
{"base": {"rows": 40, "columns": 8, "data": "..."}, "params": {"limit": 100, "neighbors": 15, "min_dist_milli": 100, "seed": 42}, "points": {"rows": 3, "columns": 8, "data": "..."}}
```

`base` and `params` name the layout exactly as `/v1/layout/fit` was asked
for it (same checks and errors); `points.columns` must equal
`base.columns`. The sidecar fits the base (deterministically, so it is the
layout `/v1/layout/fit` returned) and runs UMAP's `transform` on the
points. Response `200`: `{"coordinates": {...}}` with one row per point.
Fitted bases are kept in a small in-process LRU cache keyed by a SHA-256
of the base and the params (`CROSSTALK_TOPICS_LAYOUT_CACHE` entries); a hit
and a miss return the same bytes.

### Errors

Every error is a JSON body, adjacently tagged, with its HTTP status:

| `type` | Status | `data` |
| --- | --- | --- |
| `invalid_request` | 400 | `{"reason": string}`: malformed JSON, unknown or missing field, bad matrix, out-of-range parameter, mismatched counts |
| `not_found` | 404 | none |
| `method_not_allowed` | 405 | none |
| `payload_too_large` | 413 | `{"limit_bytes": u64}` |
| `too_few_samples` | 422 | `{"needed": u32, "got": u32}` (topics) |
| `too_few_points` | 422 | `{"needed": u32, "got": u64}` (layouts) |
| `non_finite_layout` | 422 | none |
| `internal` | 500 | `{"reason": string}` |

```json
{"type":"too_few_points","data":{"needed":16,"got":9}}
```

422s are deterministic outcomes of the input: asking again gives the same
answer. Everything else is a caller bug (400, 404, 405, 413) or a service
fault (500).

## Determinism

The same request bytes give the same response bytes: compact JSON, fields
in the order above, floats written by pydantic's serializer (shortest
round-trip form), coordinates as exact binary32 hex. The sidecar ensures it
by:

- **Seeding.** UMAP's `random_state` is
  `numpy.random.RandomState(numpy.random.MT19937(numpy.random.SeedSequence(seed)))`,
  built fresh per request, so the whole u64 seed counts (a plain
  `RandomState(seed)` only takes 32 bits). `transform_seed` is the first
  word of `SeedSequence(seed).generate_state(1)`. A seeded UMAP runs its
  optimisation single-threaded.
- **One thread everywhere.** The package forces `OMP_NUM_THREADS`,
  `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS` and `NUMBA_NUM_THREADS` to `1`
  before NumPy or Numba is imported, and refuses to start if Numba reports
  more than one thread. Requests are computed one at a time on a single
  worker thread, so the event loop stays free for `/healthz`.
- **A fixed JIT target.** `NUMBA_CPU_NAME=generic` makes Numba compile the
  same machine code on every x86-64 host instead of tuning for the host
  CPU.
- **HDBSCAN and c-TF-IDF have no randomness**; ties are broken by the rules
  above, never by hash or dict order.

**Limit.** Bit-identity is guaranteed for the same image (Python and
library versions, as `/healthz` reports them) on the same CPU architecture.
NumPy and OpenBLAS still pick SIMD kernels by CPU at run time, so two
x86-64 hosts with different vector extensions may differ in the last bits
of a layout; the goldens in `sidecar/topics/tests/fixtures/` were blessed
on x86-64. A projection's frame is stored once and read back exactly, so
this matters only when an expired projection is re-fitted on other
hardware.

The Rust side adds no nondeterminism: matrices are exact; topic ids are
derived (below), not random; centroids are summed in input order.

## Rust adapters (`crates/analysis/src/remote/`)

See [analysis](analysis.md) for the crate's files and exports. The
contract tests (`remote::tests::contract`) send the adapter's requests for
the inputs of `sidecar/topics/tests/fixtures/*.request.json` to a fake
server and require the bytes to equal those files exactly, then decode the
Python goldens (`*.response.json`) through the adapters; the Python golden
tests serve the same request files. Both halves are pinned by one set of
files.

- **`SidecarTopicModel`** implements `TopicModel`. `fit(version, documents,
  at)` refuses a version not above `version()`, checks every embedding is
  from its model, sends `/v1/topics/fit`, validates the reply, computes
  each topic's centroid and returns the topics, stamped `version` and
  `fitted_at = at`; it then makes them current. Topic `k` of a fit gets the
  id whose ULID time is `at` in milliseconds and whose 80 random bits are
  the first ten bytes of `BLAKE3::derive_key("crosstalk topic id v1",
  version as u32 LE ‖ k as u32 LE)`, so a re-run of the same fit names the
  same topics and two versions share an id only if 80 bits of BLAKE3
  collide (`analysis.topic.ids-derived`). `assign` is local: the
  most similar centroid (cosine, ties to the lower index) if its
  similarity is at least `outlier_below`, else `Outlier`; version 0 (no
  topics) assigns `Outlier`. `restore(version, topics)` rebuilds the
  state from the catalog after a restart.
- **`SidecarLayoutFitter`** implements `LayoutFitter` with
  `/v1/layout/fit`, and has `transform(base, params, points)` for
  `/v1/layout/transform`. Fewer than `neighbors + 1` points is
  `FitFailure::TooFewPoints` without a request.
- **`OpenAiEmbedder`** implements `Embedder` against `<base_url>/embeddings`
  (config `embeddings{base_url, model, api_key{env}}` from
  `deploy/config/crosstalk.json`). The key is read from the named env var;
  an empty value means a keyless endpoint and an unset one is a setup
  error. `connect` learns the model's dimension with one probe request.
  Inputs go in batches of at most 256, outputs are ordered by the
  response's `index`, and each vector is L2-normalized before
  `Embedding::new` checks it.

### Error mapping

| Sidecar outcome | `TopicModel::fit` | `LayoutFitter::fit` |
| --- | --- | --- |
| 422 `too_few_samples` | `TopicError::TooFewSamples` | — |
| 422 `too_few_points` | — | `LayoutError::Failed(FitFailure::TooFewPoints)` |
| 422 `non_finite_layout`, or a non-finite coordinate | — | `LayoutError::Failed(FitFailure::NonFiniteLayout)` |
| timeout, connection failure, any other status, a reply that breaks the contract | `TopicError::Backend` | `LayoutError::Backend` |

A backend failure is never a `FitFailure`: a projection job whose fit hit
one is left to be requeued when its lease lapses, and a topic fit is
failed with `fail_fit` and retried by the next re-fit. The adapters' own
error is `SidecarError` (timeout, transport, status with the decoded error
body, decode, contract violation); it reaches the spec error as its
`Display` text.

## Running

Locally, from `sidecar/topics/`:

```sh
uv sync                                   # Python 3.14, pinned deps from uv.lock
uv run pytest                             # unit, determinism and golden tests
uv run crosstalk-topics                   # serves 0.0.0.0:8090
CROSSTALK_BLESS=1 uv run pytest tests/test_golden.py   # rewrite the goldens
```

The Rust integration tests against the real sidecar
(`remote::tests::live`) are ignored by default:

```sh
(cd sidecar/topics && uv run crosstalk-topics) &
CROSSTALK_TOPICS_URL=http://127.0.0.1:8090 cargo test -p crosstalk-analysis -- --ignored
```

Environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `CROSSTALK_TOPICS_HOST` | `0.0.0.0` | Bind address. |
| `CROSSTALK_TOPICS_PORT` | `8090` | Bind port. |
| `CROSSTALK_TOPICS_LOG_LEVEL` | `info` | `debug`, `info`, `warning` or `error`. |
| `CROSSTALK_TOPICS_MAX_BODY_BYTES` | `1073741824` | Larger request bodies are `payload_too_large`. |
| `CROSSTALK_TOPICS_LAYOUT_CACHE` | `4` | Fitted layouts kept for `transform`. |

Logs are JSON, one object per line on stdout, with `ts` (RFC 3339 UTC,
microseconds), `level`, `msg` and key-value fields (for example `route`,
`status`, `rows`, `duration_ms`), so Alloy lifts `level` like the
gateway's.

The image: `docker build -f deploy/topics.Dockerfile -t crosstalk-topics:dev .`
(context is the repository root). For compose (owned by the
infrastructure track), a `topics` service:

- `image: crosstalk-topics:dev`, built from `deploy/topics.Dockerfile`;
- no published port needed (the gateway reaches `http://topics:8090`); for
  debugging, `127.0.0.1:${CROSSTALK_TOPICS_PORT:-8090}:8090`;
- `healthcheck: ["CMD", "crosstalk-topics", "healthcheck"]`, interval 10s,
  timeout 3s, retries 3, start period 30s (Numba compiles on first use);
- environment: `CROSSTALK_TOPICS_LOG_LEVEL: ${CROSSTALK_TOPICS_LOG:-info}`;
  the thread pins are baked into the image;
- `crosstalk` `depends_on: topics: condition: service_healthy` once the
  analysis role uses it, and the gateway config gains
  `"topics": {"base_url": "http://topics:8090", "timeout_ms": 600000}`.
