# Crosstalk overview

```yaml
Overview:
  description: >
    A gateway that sits between agent harnesses and inference APIs. It
    proxies every request unchanged and watches for one agent's output
    showing up in another agent's input: in a tool result, a user turn or a
    system prompt. From that it classifies agent-to-agent communication,
    discovers the channels agents use (including ones nobody declared, such
    as a public wiki agents start writing to), and records the content,
    topology and location of the communication. The records power a
    topology view with edge weights and per-node metadata, a channel-centred
    view of who reads and writes each channel, search, topic modeling,
    clustering, a UMAP view of message content, and alerts.

    It works with Claude Code, Codex, pi and oh-my-pi, against vendor APIs,
    subscription backends reached with OAuth (Claude Pro/Max, ChatGPT/Codex,
    GitHub Copilot, Gemini Code Assist) and self-hosted vLLM or SGLang, over
    HTTP, SSE and WebSocket.

    Status: milestone M1 (capture) is reachable: the crosstalk binary
    (crosstalk-gateway, roadmap P3) runs the capture slice in single-node
    mode, so Claude Code pointed at it through ANTHROPIC_BASE_URL works
    unchanged and every generation exchange is normalized, its bodies
    stored, ExchangeCaptured published and the exchange persisted
    (gateway). L2's in-process bus and blob store (transport) are
    implemented; L3 to L8 are not started. The data
    model is specified in spec/types (crate crosstalk-spec), and the spec
    types are also the JSON wire format between the gateway, the operator
    UI and other gateway nodes. The root Cargo.toml is a virtual workspace
    of spec plus one library per implementation crate under crates/
    (crosstalk-<dir>), with the dependency rule between them enforced by an
    architecture test and every check run by scripts/check.sh (workspace).
    crosstalk-store (Postgres pool, per-layer migrations, extensions, typed
    errors, serializable retries, test databases) is implemented (store).
    crosstalk-sim is filled in: the deterministic simulation kit the dst
    invariants are tested with (sim). crosstalk-testkit holds the builders,
    the synthetic Anthropic corpus and the fake upstream and client
    (testkit). crosstalk-transport has the in-process bus (MpscBus) with
    consumer groups, retries, dead letters and envelope dedup, and the
    content-addressed blob store (FsBlobStore, MemoryBlobStore)
    (transport). crosstalk-canonical has the Anthropic Messages
    normalizer (L1): pure functions from a RawExchange to a
    NormalizedExchange (media bytes included), with streaming reassembly,
    and a step that stores the bodies through BlobStore (canonical).
    crosstalk-ingress has the L0 reverse proxy for Anthropic
    Messages over HTTP and SSE, handing each generation exchange to a
    bounded capture channel as a RawExchange (ingress). The
    primitives several layers share are in the spec (spec_primitives):
    the canonical message encoding, its BLAKE3 hash and strict decoder,
    exact-number canonical JSON, the keyed hasher and deployment secret,
    and the ULID generator.
    crosstalk-gateway
    is the crosstalk binary of the deployment contract (serve --role,
    migrate, healthcheck, inspect; JSON config; JSON logs; an ops listener
    with /metrics, /healthz and /readyz): it wires the proxy to a capture
    stage that normalizes with L1, stores bodies in FsBlobStore and
    publishes ExchangeCaptured on MpscBus, and persists each captured
    exchange through a bus consumer to an append-only exchange log, a P3
    stopgap because the spec has no exchange store (gateway). That
    composition is a library entry point, crosstalk_gateway::pipeline::
    Pipeline (build over any blob store, bus and injected clock), and a
    pre-normalized exchange enters it through Pipeline::ingest, the same
    path the capture stage takes after L1 (P3.1), so the eval harness can
    drive the real layers under simulated time. The other
    crates are still empty. The phased implementation plan, with its
    dependencies, milestones and current status, is docs/roadmap.md.

  subsystems:
    spec: >
      crosstalk-spec (spec/): the shared boundary crate. Types, per-layer
      traits, invariants and wire goldens. Every implementation crate
      depends on it, and layer crates reach each other only through it.
    ingest: >
      Crates crosstalk-ingress, crosstalk-canonical and
      crosstalk-reconstruct. L0 ingress (reverse and forward proxy, upstream routing, credential
      hashing, provider adapters, SSE framing and WebSocket taps), L1
      canonicalization (wire format and dialect to canonical Exchange and
      Message), L3 reconstruction (agent identity, the merge log with exact
      unmerge and vetoes, renames, harness claims seen per agent,
      conversation threading, WebSocket increment resolution).
    transport: >
      Crate crosstalk-transport. L2: the event bus (in-process channels on one node, NATS JetStream
      across nodes) and the content-addressed blob store. The only path
      between components. The in-process bus is one tokio task owning
      every consumer group, delivery and dead letter; envelopes cross it
      as their wire JSON and are decoded strictly on delivery.
    detect: >
      Crates crosstalk-provenance and crosstalk-flow. L4 provenance (span extraction, novelty classification, fingerprint
      index, content matching) and L5 flow detection (resource extraction,
      channel registry with promotion and supersession, in which a channel
      exists only once a transmission between different agents goes
      through it, write/read correlation into transmissions, and the
      operator verdict log kept beside each transmission).
    insight: >
      Crates crosstalk-analysis, crosstalk-topology and crosstalk-surface.
      L6 analysis (embeddings, topics, the topic-model version history with
      sizes, lineage, pins and retention, paged search, stored projection
      jobs fitted in the background, built-in and user alert rules with
      their sinks, triage), L7 topology (edge and access buckets per time
      window, graphs with node metadata, the channel-centred graph, time
      series, and the watermark before which every bucket is final), L8
      surface (the query API with cursor-paginated lists and linked views
      sharing one filter and one resolved topic-model version; read models
      for agents, channels and transmissions: canonical agent rows and
      details that follow merges, channel rows carrying activity or their
      supersession, a promotion preview computed by the promotion's own
      plan, batch names over one bounded id batch, transmission rows by id
      with a per-state shape, the evidence behind a transmission with
      excerpts cut from stored bodies, the overview's counts, one alert
      and one alert rule by id; the present (the gateway's clock, L7's
      bucket width, the export formats it writes, the topic version rules
      are written against, the default remap threshold, the frame
      retention); typed query and action errors, with one From impl per
      store error behind every query and every action; the operator
      directory with a
      trusted single-user mode; operator actions with one permission each;
      the append-only audit log of operator actions, config changes and
      exports; the id-only SSE live feed; streamed exports with a header
      and trailer manifest; alert sinks; and the HTTP binding of all of it:
      one route per query, action kind and the live feed, a status for
      every error, and the caller taken from a bearer token or session
      cookie only).
    serve: >
      Crates crosstalk-api (the HTTP and SSE server for the L8 surface),
      crosstalk-client (the L8 traits over HTTP, for the UI) and
      crosstalk-gateway (the crosstalk binary: config, wiring, process
      roles, the ops listener, graceful shutdown; today the single-node
      capture slice). The only crates allowed to depend on layer crates.
    support: >
      Crates crosstalk-store (Postgres through sqlx: the pool configured
      from DATABASE_URL, one schema and one migrations table per layer,
      the vector and pg_trgm extensions, classified errors, serializable
      transaction retries, and a database-per-test harness gated on
      TEST_DATABASE_URL), crosstalk-memory (in-memory reference stores and
      the model-based harnesses the Postgres stores reuse; L3 to L8 done),
      crosstalk-sim (deterministic simulation), crosstalk-testkit
      (builders, recorded corpus, fake upstreams) and crosstalk-world (the
      synthetic week the UI and the tests share, seeded through the write
      traits). Layer crates may depend on store; memory, sim, testkit and
      world are their dev-dependencies only.
    deploy: >
      deploy/ (outside the workspace): docker compose on one machine with
      Postgres, a migrate step, the crosstalk binary as --role all, the UI,
      and the infrastructure observability stack (Prometheus, Grafana, Loki,
      Alloy, node-exporter, cAdvisor, postgres-exporter). Where things are
      stored, how they run and how they scale is in docs/infrastructure.md.

  data_flow: >
    Each layer below runs in its own crate (crosstalk-<layer>); layer crates
    exchange data only as spec bus events or through spec traits that
    crosstalk-gateway hands them at wiring time, never by calling each
    other's code. Harness request (via its base URL, or via the gateway as HTTPS proxy) →
    L0 routes it to its upstream, hashes the credential, forwards it
    unchanged without waiting for its body to decode, decodes the body
    concurrently off the hot path, and tees the response (or each WebSocket
    turn) → RawExchange (in-process) → L1 normalizes, writes message bodies
    to the blob store, publishes ExchangeCaptured → L3 resolves the agent,
    records its harness claim and threads the conversation, publishes
    ConversationDelta → L4 indexes the agent's originated spans and matches
    new inputs against other agents' spans (ContentMatched); L5 turns tool
    calls into accesses on resources, on a canonical channel or on none
    (AccessRecorded), correlates cross-agent accesses and content matches
    into transmissions (TransmissionConfirmed / Suspected), and discovers
    a channel from a resource only when the first transmission between
    two different agents goes through it (ChannelDiscovered, from the
    registry; a resource only one agent touches is never a channel) → L6 embeds and
    classifies transmissions, records topic-model versions and their
    lineage, and evaluates alert rules → L7 aggregates edges and access
    buckets, advances the watermark from the correlator's ticks and the
    oldest unprocessed input, and announces topic-version activation back
    to L6, whose topic catalog then drops the versions its retention
    policy no longer keeps and publishes TopicVersionDropped from that
    transaction (L7 deletes their buckets) → L8 serves
    topology, the channel-centred graph, a channel's resources, series,
    topic history, search, projections, verdicts, detection quality,
    lists, alerts, and the read models: agent rows (L3's profiles joined
    with L7's traffic in the window) and details following merged ids;
    channel rows (newest created first, each with its cross-agent traffic
    tallied at the read and the listing that follows from it: a confirmed
    or unconfirmed channel, a declaration without traffic, or hidden once
    every transmission through it is within one merged agent; writers and
    readers from L5's resource use, transmissions from the same graph the
    overview counts, over the channel and every channel it superseded, in
    an optional window that never changes which rows are listed); a
    channel's cross-agent transmissions (the review list of an
    unconfirmed channel); promotion previews (the registry's promotion plan run
    without effect, so a preview and the promotion agree); agent and
    channel names; transmission rows by id; the evidence page, which cuts
    excerpts of both sides of each content match from the blob store's
    bodies through the spans' and matches' locations (a body content
    retention dropped is reported, not an error); and the overview's
    counts. Exports stream one dataset between a header naming the
    request, resolved version, watermark, embedding model and gateway
    version and a trailer with the row count, a digest and whether it
    completed, reading only data settled before the watermark; a
    transmissions export's rows are the surface's transmission rows and
    its quoted text the evidence page's. Every aggregate comes back with
    the watermark read before it; every linked view applies one
    TopologyFilter under one resolved (or pinned) topic-model version,
    with merged agents and superseded channels resolved at read time (a
    late confirmation on a superseded channel advances the superseding
    channel's detection); projection fits run as background jobs whose
    stored frames read back exactly, each point carrying its route's
    channel as it was when the sample was read. A client reads the present
    first (clock, bucket width, export formats, rule version, remap
    threshold, frame retention) to build valid requests; an export in a
    format the gateway does not write is refused before anything is read. Every store publishes an id-only
    Changed after each committed change (agents, channels, verdicts,
    alerts, rules, topic versions, projection jobs, the watermark), which
    the live feed streams to the UI over SSE so it re-queries (resumable by
    cursor, with a resync marker when a cursor is too old). Each request
    becomes a Caller through the operator directory config defines (in
    trusted mode, the one operator with every permission). Operator actions
    flow back down: policy changes and promotions to L5, which records every
    policy decision in the channel's policy history (a promotion keeps the
    channel id, records the operator's policy and supersedes the
    discovered channels its pattern covers); verdicts to L5's verdict log,
    beside the detector's state (L6 suppresses a false detection's alerts;
    L6 and L7 views can exclude false detections at query time); merges,
    unmerges of one merge record and renames to L3 (two ids of one cluster
    are refused as MergeIntoSelf, one id twice never becomes an action);
    rule management (a stale rule is updated, never re-enabled) and
    topic-version pins to L6. Every action call is recorded in the audit
    log with its outcome, and so is every change a config load makes and
    every export (refused, or started and then ended or abandoned).
    Over HTTP each request is authenticated from its Authorization bearer
    token or session cookie alone (401 without a caller), resolved to its
    one route of the route table (404 without one), its path, query and
    body decoded strictly as the route's types (400 MalformedRequest
    otherwise), and answered with the method's JSON or the error's JSON
    under the error's status; the live feed is GET /live (resumed from
    Last-Event-ID or a cursor parameter), a projection frame is cached by
    its digest until its retention ends, and an export streams with its
    status sent before the first row and any later failure in its trailer.
    Across process boundaries every value travels as the JSON of its spec
    type (the wire contract): the UI's requests are decoded only as
    WireRequest types (an action as an ActionRequest, which the surface
    stamps with the Caller into an OperatorAction), with the Caller taken
    from the verified session and never from the body, and authors and
    acceptance times stamped by the surface; audit and export records keep
    a CallerSnapshot of the caller, which never becomes a Caller again;
    responses, errors, live-feed items and bus events between
    nodes are decoded strictly, so a node that does not know a field or
    variant refuses the delivery rather than dropping data.

Features Index:
  type_spec:
    description: >
      The gateway's data model as type-checked Rust: observed facts
      (including clients, upstreams, credentials, the merge log and harness
      claims), derived inferences (including channels that exist only
      once agents communicate through them, channel promotion with
      supersession and operator verdicts beside the detector's state),
      aggregates (including edge and access buckets, time series, topic
      history with retention, and the watermark that marks buckets final),
      bus events and per-layer interfaces whose async methods return Send
      futures and whose associated streams are Send + 'static, so they
      can be hosted on tokio and used generically. Every stateful store
      has a spec write side (P0.6: agent lifecycle, channel traffic and
      reads, transmissions, topic fits and assignments, search indexing,
      alert rule upkeep, actions and reads, edge activation, the operator
      store, the sink registry), so in-memory and Postgres stores
      implement the same traits; a store publishes the events of the
      decisions it takes from the transaction that makes them (the topic
      catalog owns TopicVersionDropped, the channel registry
      ChannelDiscovered), and every store method that
      depends on the time takes it as an argument (the types are also the
      JSON wire format: wire_contract), with tests for the invariants
      checked at runtime and one TOML file per invariant in
      spec/invariants. Harness and server wire behavior it is based on is
      in docs/research/harness-wire-protocols.md.
    entry_points: [spec/types/mod.rs, spec/Cargo.toml]
    depends_on: []
    doc: docs/features/type_spec.md
  query_surface:
    description: >
      The L8 contract the UI reads and acts through: callers from the
      operator directory (with a trusted single-user mode) and one
      permission per query and action; paginated lists; linked views
      sharing one TopologyFilter and one resolved topic-model version, with
      merged agents and superseded channels resolved at read time; the
      channel-centred graph and graph nodes; promotion with supersession;
      stored projections and their columnar frame; verdicts and detection
      quality; watermarked aggregates and retention; the present (clock
      and the config a request is built with) and one rule by id; typed
      query and action errors with one From impl per store error, every
      action's refusals included; operator actions; the
      append-only audit log of actions, config changes and exports; and the
      id-only SSE live feed fed by every store's Changed. Its traits'
      futures are Send, so a UI or client may be generic over QueryApi.
    entry_points:
      - spec/types/interfaces/l8_surface.rs
      - spec/types/interfaces/l8_surface/permissions.rs
      - spec/types/interfaces/l8_surface/actions.rs
      - spec/types/interfaces/l8_surface/errors.rs
      - spec/types/interfaces/l8_surface/query_errors.rs
      - spec/types/interfaces/l8_surface/audit.rs
      - spec/types/interfaces/l8_surface/live.rs
      - spec/types/interfaces/l8_surface/present.rs
    depends_on: [type_spec]
    doc: docs/features/query_surface.md
  read_models:
    description: >
      The rows and pages the UI shows on the query surface: canonical agent
      rows with claims, last seen and windowed traffic, and a detail with
      aliases, children, merges and vetoes that follows merged ids; channel
      rows with their cross-agent traffic, listing and activity or their
      supersession, newest created first, the channel list filter (listings
      included), a channel's cross-agent transmissions, and the promotion
      preview computed by the promotion's own plan; agent and channel names
      over one bounded IdBatch; transmission rows by id with a per-state
      shape, never a transmission within one agent; the evidence behind a transmission with
      bounded excerpts; the overview's counts, which agree with the channel
      and agent rows; and one alert by id. An agent's detail finds each
      alias's merge record (when and by whom it was merged).
    entry_points:
      - spec/types/aggregates/agents/mod.rs
      - spec/types/interfaces/l8_surface/channels.rs
      - spec/types/batch.rs
      - spec/types/interfaces/l8_surface/summary.rs
      - spec/types/interfaces/l8_surface/evidence.rs
      - spec/types/interfaces/l8_surface/excerpt.rs
      - spec/types/interfaces/l8_surface/overview.rs
      - spec/types/interfaces/l8_surface/channel_traffic.rs
    depends_on: [query_surface, type_spec, channel_semantics]
    doc: docs/features/read_models.md
  channel_semantics:
    description: >
      What counts as a channel and as a transmission, applied by every
      layer. A transmission exists only between different agents
      (Transmission::crossing, with merges resolved at the read), so one
      between two ids of a merged agent counts in no filter, graph, row,
      count, projection, topic size, quality figure, export or alert. A
      discovered channel exists only once such a transmission goes through
      a resource on no channel (ChannelTraffic::discover, seeded by the
      resource and that transmission; the registry publishes
      ChannelDiscovered, which raises NewChannel); before that a resource
      is only a resource. A channel's cross-agent traffic, confirmation
      (Confirmed, or Unconfirmed while all its traffic is suspected) and
      listing (a channel, a declaration without traffic, or hidden after a
      merge, which an unmerge undoes) are read, never stored. Unconfirmed
      channels are listed and drawn marked and can be filtered out
      (ChannelFilter::listings, TopologyFilter::unconfirmed_channels).
      Access buckets are kept by resource and resolved to the channel
      holding it at read time. Channel lists are newest created first.
    entry_points:
      - spec/types/derived/flow/channel/confirmation.rs
      - spec/types/derived/flow/transmission.rs
      - spec/types/interfaces/l5_flow/channels.rs
      - spec/types/interfaces/l8_surface/channel_traffic.rs
      - crates/memory/src/flow/registry/traffic.rs
    depends_on: [type_spec, query_surface]
    doc: docs/features/channel_semantics.md
  export:
    description: >
      QueryApi::export: one dataset (transmissions, edge or access buckets,
      topics, a stored projection, verdicts) streamed in JSONL or Parquet
      between a header (request, resolved topic version, watermark,
      embedding model, gateway version, planned rows) and a trailer (rows
      sent, a format-independent digest over a canonical row encoding,
      Complete or the failure). A format the gateway does not write (outside
      the present's export formats) is refused as UnsupportedFormat before
      anything is read. Reads only data settled before the
      watermark, under resolution captured at the start, so a re-run
      reproduces it; transmission rows are the surface's transmission
      summaries and their quoted text the evidence page's; content needs
      Content; oversized exports are refused before streaming; every export
      is audited. In JSONL each line is a tagged header, row or trailer
      (ExportLine, read_jsonl), so any truncation reads as a missing
      trailer; Parquet keeps the header and trailer JSON in its footer.
    entry_points:
      - spec/types/interfaces/l8_surface/export/mod.rs
      - spec/types/interfaces/l8_surface/export/stream.rs
      - spec/types/interfaces/l8_surface/export/framing.rs
    depends_on: [query_surface, read_models, type_spec]
    doc: docs/features/export.md
  wire_contract:
    description: >
      The JSON wire format, which is the spec types themselves: snake_case
      objects, adjacently tagged enums ({"type", "data"}) and all-unit
      enums as strings, entity ids (and ConnectionId) as ULID text, digests
      as lower-case hex, timestamps as RFC 3339 UTC at microsecond
      precision, durations as whole microseconds in _micros fields, floats
      only behind checked finite types, id-keyed maps as BTreeMaps in id
      order; strict decoding (unknown fields and variants refused, three
      documented leniencies that change no data); checked types decoded
      only through their constructors; WireRequest and decode_request for
      what a client may send (ActionRequest for actions, stamped with the
      Caller into an OperatorAction), with Caller never serialized, audit
      and export records keeping a CallerSnapshot, and server-stamped
      records never requests; an undecodable request as
      InvalidInput(MalformedRequest); the live feed's SSE framing; the
      projection as ProjectionInfo JSON plus octet-stream frame bytes; an
      export as JSONL lines; golden files pinning every shape of every
      area (observed, provenance, flow, topology, agents, bus, analysis,
      surface actions, surface reads, and the HTTP binding's route table
      and status tables), rewritten with CROSSTALK_BLESS=1.
      One page for the conventions and harness, one per area under
      docs/features/wire/.
    entry_points:
      - spec/types/wire/mod.rs
      - spec/types/wire/time.rs
      - spec/types/wire/duration.rs
      - spec/types/wire/authority.rs
      - spec/types/interfaces/l8_surface/actions/request.rs
      - spec/types/interfaces/l8_surface/export/framing.rs
      - spec/types/tests/wire/harness.rs
      - spec/types/tests/golden/
    depends_on: [type_spec, query_surface, read_models, export]
    doc: docs/features/wire_contract.md
    area_docs:
      - docs/features/wire/observed.md
      - docs/features/wire/flow.md
      - docs/features/wire/topology.md
      - docs/features/wire/analysis.md
      - docs/features/wire/surface_actions.md
      - docs/features/wire/surface_reads.md
  workspace:
    description: >
      The virtual Cargo workspace (members spec and crates/*, ui excluded,
      edition 2024, unsafe forbidden, shared exact pins, one lock), one
      empty library per implementation crate, the dependency rule (layer
      crates never depend on each other or on the composers api, client,
      eval or gateway, take
      transport only as a dev-dependency, and take memory, sim and testkit
      only as dev-dependencies) checked by an architecture test over cargo
      metadata, scripts/check.sh (fmt, clippy, test, doc, invariant
      validator), and the invariant evidence path convention
      (crosstalk_spec:: or crosstalk_<crate>::, checked by
      scripts/inv_check.py together with the existence of reviewed spec
      tests).
    entry_points:
      - Cargo.toml
      - crates/gateway/tests/architecture.rs
      - scripts/check.sh
      - scripts/inv_check.py
    depends_on: [type_spec]
    doc: docs/features/workspace.md
  http_api:
    description: >
      The HTTP binding of the L8 surface, as checked spec: the route table
      (one Route per QueryApi method, per ActionKind on POST /actions, and
      GET /live; method, path template, where each argument travels,
      success status and content type, permission), reads as GET with JSON
      query parameters except the ones whose filter, id batch, selection
      or search text needs a body (POST /query/...), the client's
      RequestBuilder and the server's resolve, PathParams and QueryParams,
      the status of every QueryError, ActionError and AuthError, the
      caller from a bearer token or the __Host-crosstalk-session cookie
      only (401 AuthError otherwise), SSE resume and framing, projection
      frame caching by digest, and export downloads whose trailer records
      a failure after the status.
    entry_points:
      - spec/types/interfaces/l8_surface/http.rs
      - spec/types/interfaces/l8_surface/http/routes.rs
      - spec/types/interfaces/l8_surface/http/status.rs
      - spec/types/interfaces/l8_surface/http/auth.rs
    depends_on: [query_surface, read_models, export, wire_contract]
    doc: docs/features/http_api.md
  store:
    description: >
      crosstalk-store, the Postgres infrastructure layer crates build on
      (roadmap P1.5, decision D2: sqlx 0.9 with runtime-checked queries,
      no TLS compiled in). StoreConfig reads DATABASE_URL from the
      environment and pool sizing from structured config; Store::connect
      opens a PgPool. Each layer owns a schema named after it and embeds
      crates/<layer>/migrations with sqlx::migrate!; migrate() runs them in
      that schema with its own "<layer>"._sqlx_migrations table, so layers
      never collide on versions. ensure_extensions creates vector and
      pg_trgm (TimescaleDB stays out pending D3). classify maps sqlx errors
      to DbFailure (unique, foreign-key and check violations, retryable
      serialization failures and deadlocks, connection loss, pool timeout)
      for layers to map into spec errors; retry_serializable runs a
      SERIALIZABLE transaction with bounded, backed-off retries. TestDb
      creates a fresh database per test from TEST_DATABASE_URL and drops it
      on close or drop; tests skip with a printed reason when the variable
      is unset, and scripts/test-db.sh starts a disposable Postgres 18 with
      pgvector.
    entry_points:
      - crates/store/src/lib.rs
      - crates/store/src/migrate.rs
      - crates/store/src/test_db.rs
      - scripts/test-db.sh
    depends_on: [workspace]
    doc: docs/features/store.md
  sim:
    description: >
      crosstalk-sim, the deterministic simulation kit for every dst
      invariant (decision D4: tokio paused time plus an in-crate fault
      layer, not turmoil). Sim::run drives an async scenario from one seed
      on a current-thread runtime with paused time; every random choice
      comes from a SplitMix64 stream forked from that seed; SimClock is the
      spec's Clock on paused time, with steps and per-node skew; FaultPlan
      describes the faults; FaultyBus wraps any spec EventBus with delay,
      reorder, duplicate, drop-and-redeliver, crash on publish and crash
      before ack, per subject; FaultyStore wraps any store call with
      latency, failure before or after commit and crash after commit;
      UpstreamFaultInjector picks a fault per upstream exchange; Node
      supervises crashed nodes. The run's trace is hashed for determinism,
      and a failure reports its seed and step (CROSSTALK_SIM_SEED reruns
      it, CROSSTALK_SIM_SEEDS sweeps). The spec gained the Clock trait and
      SystemClock it builds on; SimRng is the spec's RandomSource, so ULID
      generators draw from the run's seed.
    entry_points:
      - crates/sim/src/lib.rs
      - crates/sim/src/driver.rs
      - crates/sim/src/bus.rs
      - crates/sim/src/store.rs
      - spec/types/support.rs
    depends_on: [type_spec, workspace]
    doc: docs/features/sim.md
  testkit:
    description: >
      Test support (crosstalk-testkit, roadmap P1.4, a dev-dependency only):
      deterministic seeded ids and a fixed epoch; builders for agents,
      resources, accesses, channels, exchanges, normalized exchanges,
      content matches, co-accesses, transmissions in every state, alerts,
      rules, topic version histories and bus envelopes, each building
      through the spec's checked constructors (message hashes are the
      spec's real encoding hash); a synthetic Anthropic
      Messages corpus (request.http, response.http, meta.json per case,
      Claude Code's headers and body shape, streaming and not, tool use,
      thinking, cache control, mid-stream and HTTP errors, non-generation
      routes) with a loader that checks each case against its metadata; a
      hyper fake upstream that replays cases with paced event streams and
      stalls, disconnects, withholds or fails on command and records what
      it received; and a hyper fake harness client that collects responses
      chunk by chunk.
    entry_points:
      - crates/testkit/src/lib.rs
      - crates/testkit/src/corpus/anthropic.rs
      - crates/testkit/src/upstream/mod.rs
      - crates/testkit/src/client.rs
      - crates/testkit/corpus/README.md
    depends_on: [type_spec, workspace]
    doc: docs/features/testkit.md
  transport:
    description: >
      crosstalk-transport, L2 (P2.1 and P2.2). The in-process bus
      (MpscBus): consumer groups that each get every envelope and share it
      among their subscriptions, at-least-once delivery with ack, nack, ack
      timeouts and redelivery after a consumer crash, backoff from the
      group's RetryPolicy, dead letters stored before release after the
      retry budget, listed by cursor and replayed to one group, bounded
      per-group capacity with waiting publishers, envelopes encoded as wire
      JSON and decoded strictly (an undecodable payload is reported once
      and terminated), a seeded shuffled delivery order for simulation,
      structured config, and the Dedup wrapper over a per-group handled-id
      record. Simulation tests on tokio's paused clock with a seeded fault
      scenario. The content-addressed blob store: BlobStore on the
      filesystem (FsBlobStore: bodies at <root>/<2 hex>/<62 hex> keyed by
      BLAKE3, written atomically through a synced temporary file, rename
      and directory sync, rehashed on every read with Corrupt on a
      mismatch, every operation one spawn_blocking task) and in memory
      (MemoryBlobStore, for tests and the simulation). Puts are idempotent
      and safe to race; a missing body is None. No deletion hook: the spec
      defines no content retention for bodies.
    entry_points:
      - crates/transport/src/lib.rs
      - crates/transport/src/bus/mod.rs
      - crates/transport/src/bus/actor.rs
      - crates/transport/src/config.rs
      - crates/transport/src/dedup.rs
      - crates/transport/src/blob/mod.rs
      - crates/transport/src/blob/fs/mod.rs
      - crates/transport/src/blob/memory.rs
    depends_on: [type_spec, wire_contract, workspace]
    doc: docs/features/transport.md
  memory:
    description: >
      crosstalk-memory, the in-memory reference implementation of every
      stateful spec store (roadmap P2.3), each with a model-based proptest
      harness that runs random operation sequences on a store under test
      and on the reference and requires equal results, events and
      observations; the harnesses drive every store through spec traits
      only (write side included, P0.6), with what a store reads from other
      layers' caches handed in as spec read traits, so the Postgres stores
      reuse them with no crate-specific hooks. The pipeline half (L3 to
      L5): MemoryAgents (AgentDirectory, the merge log with exact unmerges
      and vetoes through IdentityResolver's merge, unmerge and rename, the
      evidence lookup behind resolve, AgentLifecycle, ClaimStore,
      ActivityStore, AgentReads), MemoryFingerprintIndex (FingerprintIndex
      with cutoff, retention measured from the now each call is given, and
      shards), MemoryChannels (ChannelRegistry, ChannelTraffic,
      ChannelReads and ChannelDirectory: lookups that create nothing,
      resources on a channel or on none, discovery by a cross-agent
      transmission, the recorded state of every channel transmission and
      the cross-agent traffic, listing and order read from it,
      declarations, policy history, promotion by promotion::plan and its
      coverage, supersession, resource use) and MemoryVerdicts
      (TransmissionStore and TransmissionVerdicts, its quality leaving out
      transmissions within one merged agent); state sits behind a std
      RwLock per store. The insight and surface half (L6 to L8): the topic
      catalog (TopicCatalog and TopicLifecycle: fit lifecycle, lineage,
      assignments, sizes of cross-agent assignments, pins and retention,
      publishing its drops), exact
      search and projection sampling (SearchIndex, SearchCorpus,
      ProjectionSource), projection jobs with leases and frame retention,
      the alert store (AlertRuleStore, AlertTriage, AlertRuleMaintenance,
      AlertActions, AlertReads) in one transaction scope, the edge store
      computed from stored contributions (activation, watermark, drops,
      graph, totals, channel-centred graph drawing listed channels only
      from access buckets kept by resource, drill-down, agent traffic,
      series), the append-only audit log, the OperatorStore and the
      SinkRegistry, and Fake* doubles of the computational traits. Every
      store shares one id sequence, one mpsc outbox, one cursor book and
      pager (crates/memory/src/support) and one harness runner
      (model::run, HarnessConfig, ModelMismatch), with planted mutants and
      broken stores proving each harness catches a bug.
    entry_points:
      - crates/memory/src/reconstruct/mod.rs
      - crates/memory/src/provenance/mod.rs
      - crates/memory/src/flow/mod.rs
      - crates/memory/src/reconstruct/model.rs
      - crates/memory/src/provenance/model.rs
      - crates/memory/src/flow/registry/model.rs
      - crates/memory/src/flow/verdicts/model.rs
      - crates/memory/src/support/mod.rs
      - crates/memory/src/analysis/mod.rs
      - crates/memory/src/topology/mod.rs
      - crates/memory/src/surface/mod.rs
      - crates/memory/src/model/mod.rs
    depends_on: [type_spec, query_surface, workspace]
    doc: docs/features/memory.md
  canonical:
    description: >
      crosstalk-canonical, L1 (P2.5). AnthropicMessages, the spec's
      Normalizer for Anthropic Messages in every dialect, as pure
      functions: the request body (system prompt as a string or blocks,
      user turns split into maximal runs of one role so tool results
      become Tool messages, tool calls with canonical JSON arguments,
      thinking and redacted thinking, base64 media as their own blobs,
      cache_control markers dropped, unknown blocks kept and warned) and
      the response, whole or reassembled from SSE events (interleaved
      deltas, pings, message_delta usage, partial input_json, an error
      event mid-stream giving the partial response plus the failure), to
      the canonical Exchange, its messages and warnings (unknown blocks,
      orphan tool results in a full history) and the media bytes they
      name. Bodies are hashed and provider JSON read with the spec's
      encoding and exact-number JSON (spec_primitives); thinking keeps its
      signature; token usage reports cache reads and cache writes as parts
      of input; store() writes every body and media blob through
      BlobStore. Goldens (the spec's NormalizedExchange JSON) over the
      testkit corpus, properties over generated requests and streams.
    entry_points:
      - crates/canonical/src/lib.rs
      - crates/canonical/src/anthropic/mod.rs
      - crates/canonical/src/capture.rs
    depends_on: [type_spec, spec_primitives, testkit, transport, workspace]
    doc: docs/features/canonical.md
  ingress:
    description: >
      crosstalk-ingress, L0 (P2.4): a hyper 1 reverse proxy for Anthropic
      Messages over HTTP and SSE. Structured routes map a path prefix (the
      harness's base URL path) to an upstream by the head alone; no route
      is a local 421. The credential and account are hashed at once with
      the spec's KeyedHasher (the deployment secret from an environment
      variable; the previous version too for exchanges that start before
      the rotation overlap's configured end) and the scheme
      follows the documented rule; harness claims are recorded as sent.
      Only Generation is captured: the request is forwarded as soon as it
      is routed, its body teed (bounded) and decoded concurrently (gzip and
      zstd within a decoded-size bound) by a RequestDecoder; the response
      is relayed frame by frame through a tee that feeds a framer chosen
      from the response head (SSE, one JSON body, or an error document) and
      keeps the bytes (bounded; past the bound the exchange is counted, not
      captured with a cut body). Stages move only along the legal
      transitions, the first failure cause wins, times come from one wall
      reading plus monotonic time, and each exchange whose request decoded
      is handed off once, after its stream ended, with try_send on the
      caller's bounded channel; every loss is counted by reason. Hop-by-hop
      headers are dropped; everything else is forwarded byte for byte.
      Exchange ids come from the spec's UlidGenerator, shared by every
      connection behind a std Mutex and stamped with each exchange's
      start. Tests run over sockets
      against testkit's fake upstream and harness, and as crosstalk-sim
      simulations over in-memory pipes; two ignored timing tests hold the
      latency budgets.
    entry_points:
      - crates/ingress/src/lib.rs
      - crates/ingress/src/proxy/mod.rs
      - crates/ingress/src/proxy/relay.rs
      - crates/ingress/src/adapter/anthropic.rs
      - crates/ingress/src/framer/mod.rs
      - crates/ingress/src/config.rs
    depends_on: [type_spec, workspace, sim, testkit]
    doc: docs/features/ingress.md
  spec_primitives:
    description: >
      Roadmap P0.7: what several layers must compute identically, moved
      from crosstalk-canonical into the spec because layer crates cannot
      depend on each other. The canonical encoding of a message body
      (canonical JSON of its wire-convention shape, also MessageBody's
      serde form), its BLAKE3 MessageHash, and a decoder that accepts
      exactly the bytes encode writes; JSON with exact numbers and RFC 8785
      text; MediaBlob (media bytes under their hash) and the
      NormalizedExchange that now carries them, with serde and a check
      applied on decode; the DeploymentSecret (never serialized, cloned or
      shown) and the KeyedHasher that alone reads it, with rotation
      overlaps; the ULID generator over the injected Clock and a
      RandomSource, monotonic per generator (stamping the clock's reading
      or a given time). TokenUsage gained cache_write
      (checked: cache counts within input) and Reasoning::Visible a hashed
      signature.
    entry_points:
      - spec/types/observed/message/encoding.rs
      - spec/types/observed/message/json.rs
      - spec/types/ids/secret.rs
      - spec/types/ids/mint.rs
      - spec/types/interfaces/l1_canonical.rs
    depends_on: [type_spec, wire_contract, sim]
    doc: docs/features/spec_primitives.md
  gateway:
    description: >
      crosstalk-gateway, the crosstalk binary (P3, milestone M1), on the
      deployment contract (docs/features/deploy.md): serve --role
      all|proxy|pipeline|api|analysis, migrate (extensions; no layer
      migrations yet), healthcheck (a hyper GET for the distroless image)
      and inspect (lists logged exchanges and decodes one with its bodies).
      One JSON config refusing unknown fields (ingress's config unchanged,
      api, ops, store, blobs, embeddings, and optional bus, pipeline and
      shutdown tuning), secrets only by environment variable name. In role
      all the L0 proxy hands each RawExchange over a bounded channel to a
      capture stage that normalizes it with L1, stores every body and media
      blob in FsBlobStore (retrying idempotent puts) and only then
      publishes ExchangeCaptured on MpscBus; a bus consumer appends each
      envelope, synced before its ack, to
      <parent of blobs.root>/exchanges/exchange-log.jsonl (a P3 stopgap:
      the spec has no exchange store). The ops listener serves /metrics
      (Prometheus text), /healthz (counters) and /readyz (database when
      configured, migrations, role tasks). SIGINT and SIGTERM stop
      accepting, drain in-flight streams up to a deadline (cutting the
      rest, which are still captured as client_disconnected), drain the
      capture stage and the log's consumer group, and sync the log. Logs
      are JSON lines on stdout filtered by RUST_LOG. Tested end to end over
      sockets with testkit's fake upstream, harness and corpus (against
      L1's goldens), with a simulation of the capture stage under blob
      store faults (INV-48), and by hand with scripts/try-claude-code.sh.
      The composition behind the proxy is the library entry point
      pipeline::Pipeline (P3.1): Pipeline::build(Settings, Deps, clock)
      over any spec BlobStore and EventBus subscribes and spawns the
      role's stages (capture stage, exchange log), and
      Pipeline::ingest(NormalizedExchange, at) stores the blobs (same
      retry), mints the envelope id at at and publishes ExchangeCaptured;
      the capture stage calls it after normalizing, so there is one path
      after L1. Envelope ids reach the bus in strictly increasing order
      under concurrent ingests. Every serve role builds one; the eval
      harness (crosstalk-eval, a composer) builds one over simulated
      stores and time.
    entry_points:
      - crates/gateway/src/main.rs
      - crates/gateway/src/gateway.rs
      - crates/gateway/src/pipeline/mod.rs
      - crates/gateway/src/pipeline/ingest.rs
      - crates/gateway/src/capture.rs
      - crates/gateway/src/config/mod.rs
      - crates/gateway/src/log/mod.rs
      - crates/gateway/src/ops/mod.rs
      - scripts/try-claude-code.sh
    depends_on: [ingress, canonical, transport, store, workspace, sim, testkit]
    doc: docs/features/gateway.md
  deploy:
    description: >
      Single-machine deployment: images for the gateway and the UI, a docker
      compose stack (Postgres 18 with pgvector, pg_trgm and
      pg_stat_statements; a migrate step; crosstalk serve --role all; the
      UI), generated secrets in deploy/.env, and infrastructure
      observability (host, container, Postgres and log metrics, dashboards,
      alert rules). Defines the contract the crosstalk binary implements:
      serve/migrate/healthcheck commands, ports 8080/8081/9464, the ops
      endpoints and the config file's top-level keys.
    entry_points:
      - deploy/compose.yaml
      - deploy/run.sh
      - deploy/crosstalk.Dockerfile
      - deploy/config/crosstalk.json
      - docs/infrastructure.md
    depends_on: [workspace, store, ingress, transport]
    doc: docs/features/deploy.md
  world:
    description: >
      crosstalk-world (crates/world, TestSupport): the UI fixture's
      synthetic week ported onto the spec's write traits. World::new(seed,
      at) gives the config a host builds its stores with (operators, sinks,
      built-in rules, embedding model and embedder, catalog retention,
      bucket width, correlator timing) and the world's clock;
      World::seed(&mut stores) declares config's channels, generates the
      cast, channels, topics and about 5,000 transmissions with their
      accesses, matches and encoded bodies, assembles every write the
      pipeline, surface and config would have made as timed steps, and
      runs them in time order through the write traits (operator actions
      audited), returning the Scenario handles (agents by fixture key,
      ChannelKey, MergeKey, RuleKey, JobKey). Deterministic per seed,
      anchor and store implementation; ids are ULIDs minted at their
      entity's time. Tests seed the memory stores and assert every scenario
      through the read traits, the channel semantics included (discovery
      at the first cross-agent transmission, the scratch entry on no
      channel, an unconfirmed and a hidden channel). The feature doc lists the divergences from the UI fixture and
      the gap list: fixture reads no store or spec trait answers.
    entry_points:
      - crates/world/src/lib.rs
      - crates/world/src/seed.rs
      - crates/world/src/stores.rs
      - crates/world/src/generate/mod.rs
      - crates/world/src/assemble/mod.rs
      - crates/world/src/run/mod.rs
      - crates/world/tests/support/mod.rs
    depends_on: [type_spec, memory, transport, workspace]
    doc: docs/features/world.md
```
