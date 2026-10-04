# Analysis (`crosstalk-analysis`)

The L6 layer crate (`crates/analysis`, library `crosstalk_analysis`),
roadmap P6.2 and P6.3. So far it holds the three computational adapters of
decision D1, all under `src/remote/`: the topic model and the layout fitter over the Python topics
sidecar ([topics_sidecar](topics_sidecar.md)), and the embedder over an
OpenAI-compatible endpoint. A layer crate: it depends on `crosstalk-spec`
and third-party crates only (`crosstalk-testkit` as a dev-dependency).

## Scope

- `SidecarTopicModel` (`TopicModel`): fits over `/v1/topics/fit`, centroids
  and topic ids computed here, local nearest-centroid assignment, restore
  from the catalog after a restart.
- `SidecarLayoutFitter` (`LayoutFitter`): `/v1/layout/fit`, plus
  `transform` over `/v1/layout/transform`.
- `OpenAiEmbedder` (`Embedder`): batched `POST <base_url>/embeddings`,
  ordered by `index`, normalized, dimension probed by `connect`.
- The shared HTTP client (hyper, rustls with ring and the Mozilla roots,
  HTTP/1.1), with a deadline on every call and a response size cap, and
  the typed errors of each adapter.
- The sidecar contract's Rust types and the matrix encoding.

## Non-scope

- The topic catalog, search index, alert rules and triage, projection
  store and source (the Postgres implementations of P6.2/P6.3).
- The `analyze` and `alerts` consumers, the re-fit trigger and the
  projection fitter loop, which call these adapters.
- Gateway wiring: parsing `embeddings` and a future `topics` config
  section, reading the key's env var, and building the adapters.

## Data and control flow

**Topic fit.** The `analyze` consumer (not yet written) begins a version
in the catalog, then calls `SidecarTopicModel::fit(version, documents, at)`:

1. `version` must be above `version()` (`TopicError::VersionNotNewer`);
   every document's embedding must be from the configured model
   (`WrongModel`); there must be `TopicFitParams::needed` documents
   (`TooFewSamples`). Each check is local, before any request.
2. `Matrix::encode` packs the embeddings; the texts and the
   `TopicFitParams` go beside them in a `TopicsFitRequest`.
   `SidecarClient::post` sends it with the configured deadline and logs
   `route`, `rows`, `status` and `duration_ms`.
3. A 200 decodes strictly as `TopicsFitReply`; `build_topics` checks it
   (one label per document, each in range, no empty topic, finite positive
   term weights) and builds each `Topic`: the sidecar's label and terms,
   `centroid` (normalized f64 mean of the members), `topic_id(at, version,
   k)`, `version`, `fitted_at = at`. A 422 `too_few_samples` is
   `TopicError::TooFewSamples`; everything else is `TopicError::Backend`
   with the `SidecarError`'s text.
4. The topics replace the current fit with a compare-and-set on a
   `tokio::sync::watch` channel: only if `version` is still above the
   current one (a concurrent newer fit wins and this one is
   `VersionNotNewer`).

`assign(embedding)` reads the current fit from the channel and returns
the most similar centroid at or above `outlier_below`, ties to the lower
index, else `Outlier`.

**Layout.** `SidecarLayoutFitter::fit(embeddings, params)` checks there are
more than `neighbors` points (`FitFailure::TooFewPoints`) of one model,
sends a `LayoutFitRequest` (the spec's `ProjectionParams` JSON as is), and
decodes the coordinates exactly. The sidecar's `too_few_points` and
`non_finite_layout`, and a non-finite value in the reply, are
`LayoutError::Failed`; everything else is `LayoutError::Backend`.
`transform(base, params, points)` does the same over
`/v1/layout/transform`, with its own `TransformError`.

**Embeddings.** `OpenAiEmbedder::embed(texts)` sends batches of at most
`batch_size` in order; each reply must hold one vector per input with
distinct in-range `index` values, each of the model's dimension and
non-zero; vectors are placed by `index`, normalized in f64 and checked by
`Embedding::new`. Any failure is `EmbedError::Model` (which the surface maps
to `QueryError::Store`), with an echoed key redacted. An empty input sends
nothing.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/analysis/Cargo.toml` | Manifest: `crosstalk-spec`, `blake3`, `bytes`, `http-body-util`, `hyper` (client, http1), `hyper-rustls` (http1, ring, tls12, webpki-tokio), `hyper-util` (client-legacy, http1, tokio), `serde`, `serde_json`, `thiserror`, `tokio` (sync, time), `tracing`; dev: `crosstalk-testkit`, `proptest`, `tokio` (macros, net, rt, rt-multi-thread) | — |
| `src/lib.rs` | Crate doc; `pub mod remote` | — |
| `src/remote/mod.rs` | The HTTP adapters, self-contained so the P6.2 stores sit beside them | re-exports everything below |
| `src/remote/http.rs` | The shared client | `HttpClient` (`send`, `DEFAULT_MAX_RESPONSE_BYTES` 64 MiB), `HttpCall`, `HttpReply`, `HttpError` (`Url`, `Request`, `Timeout`, `Transport`, `TooLarge`), `BaseUrl`, `InvalidBaseUrl` |
| `src/remote/matrix.rs` | binary32 matrices as lower-case hex | `Matrix` (`encode`, `decode`, `rows`, `columns`), `MatrixError` |
| `src/remote/sidecar/mod.rs` | Sidecar client, config, errors | `SidecarConfig` (`new(base_url, timeout_ms)`, `DEFAULT_TIMEOUT` 600 s), `SidecarClient` (`health`), `SidecarError` (`Http`, `Status`, `Decode`, `Contract`, `Encode`), `StatusBody`, `ContractViolation` |
| `src/remote/sidecar/wire.rs` | Contract v1 types and routes | `CONTRACT`, `HEALTH`, `TOPICS_FIT`, `LAYOUT_FIT`, `LAYOUT_TRANSFORM`, `Health`, `TopicsFitRequest`, `TopicsFitReply`, `TopicReply`, `LayoutFitRequest`, `LayoutTransformRequest`, `LayoutReply`, `ErrorBody` |
| `src/remote/sidecar/params.rs` | Checked topic fit parameters | `TopicFitParams` (`new`, `needed`, `Default`), `InvalidTopicFitParams` |
| `src/remote/sidecar/topics.rs` | The topic model | `SidecarTopicModel` (`new`, `restore`, `topics`), `TopicModelConfig`, `RestoreError`, `topic_id`, `centroid`, `cosine` |
| `src/remote/sidecar/layout.rs` | The layout fitter | `SidecarLayoutFitter` (`new`, `transform`), `TransformError` |
| `src/remote/embedder.rs` | The embedder | `OpenAiEmbedder` (`new`, `connect`), `OpenAiEmbedderConfig` (`DEFAULT_TIMEOUT` 60 s, `DEFAULT_BATCH_SIZE` 256), `ApiKey` (`from_env`, `from_lookup`; redacted `Debug`), `ApiKeyError`, `EmbedderError`, `EMBEDDINGS` |
| `src/remote/tests/mod.rs` | Test helpers: a 3-dimensional model, unit vectors, the fake sidecar (`FakeUpstream`) and clients | — |
| `src/remote/tests/{topics,layout,embedder,matrix}.rs` | Contract tests against the fake server | — |
| `src/remote/tests/contract.rs` | The shared fixture files of `sidecar/topics/tests/fixtures/`: requests re-encode byte for byte, golden replies decode into topics and layouts | — |
| `src/remote/tests/live.rs` | `#[ignore]`d tests against a running sidecar (`CROSSTALK_TOPICS_URL`) | — |

## Invariants and constraints

- `analysis.topic.version-zero-outlier` (INV-323),
  `analysis.topic.refit-new-version` (INV-320, restated for a passed-in
  version), `analysis.topic.centroid-mean` (INV-314): evidence in
  `remote::tests::topics`, reviewed.
- `analysis.embedder.one-per-input` (INV-294): the property
  `remote::tests::embedder::embed_preserves_count_and_order` (reviewed) and the
  API unit test; the local-embedder unit test it also names has no
  implementation under D1.
- `analysis.embedding.same-model-only` (INV-296): the `assign` half
  (`remote::tests::topics::assign_rejects_embedding_of_other_model`).
- `analysis.projection.deterministic-layout` (INV-623): the adapter's half
  (`remote::tests::layout::prop_layout_is_deterministic`: the bits the sidecar
  returns reach the caller unchanged); the computation's half is the
  sidecar's determinism suite, which no Rust evidence path can name.
- New: `analysis.fit.backend-failure-distinct` (only deterministic refusals
  become `TooFewSamples` or a `FitFailure`; a failed fit changes nothing),
  `analysis.topic.ids-derived`, `analysis.embedder.key-never-disclosed`.
- Every call has a deadline and a response cap; no call retries (the
  callers decide: a projection job is requeued, a topic fit failed and
  re-run).
- No clock is read: fit times are arguments, durations come from
  `tokio::time::Instant`.
- Errors are typed (`thiserror`); the spec errors carry their `Display`
  text. No `unwrap` outside tests.
