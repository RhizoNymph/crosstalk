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
    implemented. L3 (crosstalk-reconstruct) attributes and threads
    exchanges: evidence derivation, the agent store on Postgres, the
    threader over in-memory or Postgres conversations, and a bus consumer
    of ExchangeCaptured not yet wired into the pipeline (reconstruct). L4
    provenance (crosstalk-provenance: winnowing, decoders with escape
    folding, the segmenter, the scanner and its bus consumer publishing
    ContentMatched, the fingerprint index and L4's records on Postgres) is
    implemented but not yet wired into the gateway (provenance). L5 flow
    has its extractors, correlator and consumer (flow_extract,
    flow_correlator) and its Postgres stores (flow_store); L7 has its
    Postgres edge store and bus consumer (topology_store); L6 has its
    remote adapters (analysis) and Postgres search and alerts
    (search_alerts).
    The L8 surface service
    (crosstalk-surface: QueryApi, OperatorActions, LiveFeed, export and
    the graphs' node facts, generic over the spec's store traits) is
    implemented and runs in process over the reference stores through
    crosstalk-api's InProcess (surface_service). The data
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
    drive the real layers under simulated time. crosstalk_gateway::live::
    Live composes every layer in one process over the memory stores: L3
    reconstruct, L4 provenance with L5's extraction step, L5 flow, a
    minimal L6 classifier and L7 topology as bus consumer slots, the store
    outboxes forwarded onto the bus, and crosstalk-api's InProcess surface
    over the same stores; Live::settle drives it deterministically to a
    fixed point (gateway). crosstalk-eval is that
    harness (eval): it converts public multi-agent datasets (SALT-NLP
    first) into labelled corpora of spec NormalizedExchanges, scores a
    detector against the labels, and runs a naive reference matcher,
    Pipeline::ingest (unscored: it has no detection consumers) and a
    LiveBackend seam that scores the gateway's live composition once
    crosstalk_gateway::live::Live merges. crosstalk-analysis has
    L6's HTTP adapters (analysis, topics_sidecar): SidecarTopicModel and
    SidecarLayoutFitter over a Python sidecar (sidecar/topics: UMAP,
    HDBSCAN and c-TF-IDF behind a versioned JSON contract, deterministic
    for a seed) and OpenAiEmbedder over an OpenAI-compatible endpoint.
    The operator UI (crosstalk-ui, ui/) runs on the spec's L8 traits,
    implemented by a fixture backend by default, by the in-process
    surface seeded with the synthetic world (dev and demos), or by the
    gateway's L8 HTTP API through crosstalk-client, the only path for real
    data (ui). The other crates are
    still empty. The phased implementation plan, with its
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
      index, content matching over part text only, strict decoding,
      escape-folded normalization, boilerplate rules: template skeletons,
      fragments the origin was given by its own upstream, and unobserved
      copies without a rare token are not matched) and L5 flow detection (resource
      extraction with write outcomes, channel registry with promotion and
      supersession, in which a channel exists only once a transmission
      between different agents goes through it, write/read correlation
      into transmissions, in which rejected writes are recorded but never
      paired and a match on a resource its sender never wrote stays
      suspected, and the operator verdict log kept beside each
      transmission).
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
      trusted single-user mode, and `me`, the caller's own operator, which
      any caller may read; operator actions with one permission each;
      the append-only audit log of operator actions, config changes and
      exports; the id-only SSE live feed; streamed exports with a header
      and trailer manifest; alert sinks; and the HTTP binding of all of it:
      one route per query, action kind and the live feed, a status for
      every error, and the caller taken from a bearer token or session
      cookie only). crosstalk-surface implements the L8 service over the
      spec's store traits (surface_service).
    serve: >
      Crates crosstalk-api (the HTTP and SSE server for the L8 surface;
      today InProcess, the surface over the reference stores in one
      process),
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
    eval: >
      Crate crosstalk-eval (a composer, beside the gateway rather than in
      it) and its ct-eval binary: dataset converters stream worlds of spec
      NormalizedExchanges (replayed: IngressMode::Replay) with ground-truth
      labels; a detector (the naive reference matcher, the gateway's own
      Pipeline::ingest, which is unscored, or a LiveBackend composition)
      produces spec Transmissions, whose spans, accesses and channel
      resources are read back through the spec's read traits (SpanIndex,
      AccessStore, ChannelReads); the scorer aligns them with the labels
      and reports per dataset, route, carrier, match or access class and
      tier against regression gates. Out-of-reach and forwarding labels
      (SALT deliveries pasting the sender's own tool output) are reported
      apart from overall; ct-eval run --detector live --forwarding on|off
      sets L4's ProvenanceConfig::forwarding, and gates select by detector
      and forwarding setting.
      The swarm benchmark (ct-eval swarm) instead scores the live gateway:
      it joins the demo swarm's ground truth to the gateway's exchange log
      and blobs, and scores a saved L8 transmissions export and its evidence,
      suspected and discarded transmissions as access-only predictions
      from their evidence's accesses (reported as access-only recall,
      apart from overall). The truth's session rows map every gateway
      session to its swarm agent; it scores under
      demo-swarm/<scenario> (headline or boilerplate), and its gates are
      those named detector "gateway-export". A discarded co-access that
      aligns with no label is dismissed (the detector's own "no"), never a
      false positive or a control violation. ct-eval replay --run <dir>
      replays a saved bench run's exchange log and blobs through
      crosstalk_gateway::live::Live in memory with the run's flow windows
      (bench.env), reads the export and evidence back through the same L8
      surface, and scores them as ct-eval swarm does, offline and
      deterministically.
    e2e: >
      Crate crosstalk-e2e (a composer): the end-to-end smoke harness. A
      scripted two-agent Claude Code scenario as wire traffic, captured
      through L0 and L1, fed through a gateway Live process (every layer
      consuming the bus), and asserted through the L8 surface; two settled
      runs give identical transmissions. The scenario is reusable for
      demos.
    deploy: >
      deploy/ (outside the workspace): docker compose on one machine with
      Postgres, a migrate step, the crosstalk binary as --role all, the UI,
      and the infrastructure observability stack (Prometheus, Grafana, Loki,
      Alloy, node-exporter, cAdvisor, postgres-exporter). Where things are
      stored, how they run and how they scale is in docs/infrastructure.md.
      run.sh bench drives one scored detection benchmark on that stack: the
      demo swarm through the real gateway, its detections exported over the
      L8 API, and ct-eval (shipped in the demo image) scoring them.

    ui: >
      Crate crosstalk-ui (ui/, a workspace member; Topcoat): the operator
      web UI. Server-rendered pages plus custom elements for the topology
      graph and UMAP projection (WebGL), the time brush (SVG) and the
      live-update listener, fed by the UI's own /data/ routes. Reads, acts
      and subscribes only through the spec's L8 traits (QueryApi,
      OperatorActions, LiveFeed), called on one concrete backend type, an
      enum over the deterministic fixture, the world (crosstalk-api's
      InProcess surface seeded by crosstalk-world) and crosstalk-client's
      HttpClient over the gateway's API (bearer token from the
      environment). The clock, bucket
      width, export formats and rule defaults are QueryApi::present, read
      once per request; the one backend question outside the spec is
      where a default view ends (AppBackend::view_end: the present's now,
      unless the fixture replays up to a fixed end). Callers come from the spec's
      operator directory: trusted mode for the local backends, and over
      HTTP the server's operator for the token (QueryApi::me,
      refreshed every 30 s); view windows are bucket-aligned
      and every linked view pins the URL's topic version.
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
    ConversationDelta (new inputs exclude what the agent already saw in
    another conversation) → L4 indexes the agent's originated spans and matches
    new inputs against other agents' spans (ContentMatched); L5 turns tool
    calls into accesses on resources, on a canonical channel or on none
    (AccessRecorded; a write once its result settles its outcome),
    correlates cross-agent accesses and content matches
    into transmissions (TransmissionConfirmed / Suspected), and discovers
    a channel from a resource only when the first transmission between
    two different agents goes through it (ChannelDiscovered, from the
    registry; a resource only one agent touches is never a channel) → L6
    embeds (an OpenAI-compatible endpoint) and classifies transmissions
    (topic fits and projection layouts are computed by the Python topics
    sidecar over HTTP; assignment to the current topics is local), records
    topic-model versions and their
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
    channel names; transmission rows by id; the evidence page, which reads
    span records (L4's SpanIndex::spans) and accesses with their resources
    (L5's AccessStore::accesses) in batches, in every transmission state,
    and cuts excerpts of both sides of each content match from the blob
    store's bodies through the spans' and matches' locations (a body
    content retention dropped is reported, not an error); and the overview's
    counts. Exports stream one dataset between a header naming the
    request, resolved version, watermark, embedding model and gateway
    version and a trailer with the row count, a digest and whether it
    completed, reading only data settled before the watermark; a
    transmissions export's rows are the surface's transmission rows (their
    topics read from the catalog's assignments under the export's version,
    as rows by id read them) and its quoted text the evidence page's. Every aggregate comes back with
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
    The operator UI renders L8's reads (QueryApi), with view state in the
    URL (a bucket-aligned window and a pinned topic version), sends every
    operator action through OperatorActions::act, downloads exports as
    JSON Lines, and re-renders a page's region when the live feed (SSE from
    LiveFeed) names something the page shows. With real data it does all of
    this over HTTP: crosstalk-client sends each call to the gateway's API
    with the bearer token, the gateway derives the caller from the token,
    and the UI's own caller (its label and permission gating) is the
    server's operator for that token.

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
  follow_mode:
    status: design
    description: >
      Keeping UI views live as a gateway produces data. A follow=<span>
      page key resolves on every render to the window ending at the
      present (rounded up to a bucket) and is pinned into today's citeable
      from/to URLs for everything below the page; the provisional tail
      after the watermark is marked. Pages refresh by a server re-render
      that Topcoat merges into the DOM, triggered by a tracked signal that
      <ct-live> sets (replacing the dev-hook region swap), so the WebGL
      elements keep their nodes and update in place (topology keeps node
      positions and places new nodes with fixed-node ForceAtlas2; the time
      brush stays anchored right). Includes the spec gap list (the present,
      the bucket width, a coalesced traffic event, channel events for
      traffic-driven listing changes, data revisions, projection
      extensions), the fixture's controllable clock and deterministic
      trickle, the testing strategy and a parallel implementation plan.
    entry_points:
      - ui/src/url/view_state.rs
      - ui/src/pages/view.rs
      - ui/src/components/live.rs
      - ui/src/data/live.rs
      - ui/elements/src/live/element.ts
      - ui/elements/src/shared/element.ts
      - ui/elements/src/topology/element.ts
    depends_on: [ui, query_surface, type_spec]
    doc: docs/features/follow_mode.md
  conversation_view:
    status: design
    description: >
      Operator page for one agent's conversation, turn by turn: inputs of
      any role in request order and outputs, with provenance marks (text
      other agents originated, output spans and who later read them,
      relayed text, sub-agent delegations), harness claims, origin (fork,
      compaction), compaction boundaries, WebSocket increments and replayed traffic
      (labelled, filterable). Structure
      with View, text with Content; turns paged by citeable index windows.
      Waits on proposed L8 conversation reads
      (docs/handoff/conversation-view-spec.md, INV-1000..1029).
    entry_points: []
    depends_on: [ui, query_surface, type_spec]
    doc: docs/features/conversation_view.md
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
      summaries (topics as rows by id read them under the export's version)
      and their quoted text the evidence page's; content needs
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
      eval or gateway, take transport only as a dev-dependency, take
      memory, sim and testkit only as dev-dependencies, and never depend
      on the tool crate demo) checked by an architecture test over cargo
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
      the status of every QueryError, ActionError and AuthError (the
      client-only Unavailable, with its UnavailableKind, and
      LiveEnd::Unreachable are never served: a server answers their
      served() form, Store and ShuttingDown), the
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
  surface_service:
    description: >
      crosstalk-surface, L8 (P2.6). Surface<S: SurfaceStores>, generic over
      the spec's L3 to L8 store traits (one associated type per store
      group): every QueryApi method with its permission checked first,
      watermark-first reads, paging and a keyed-MAC cursor for
      transmission rows by id, typed errors through the spec's From
      impls; OperatorActions::act (one store write stamped with the
      caller and the accept time, then exactly one OperatorRecord whose
      AuditOutcome inverts to the returned result) and Surface::request;
      the live feed (a writer task owning the epoch's log, bounded
      per-stream buffers that end lagging streams, resume and resync,
      heartbeats, session ends, a bus consumer that appends before it
      acks); export (refusals, plan, limits, header, sealed BLAKE3 rows,
      trailer, Started/Ended/Abandoned audit) with SpecExportSource
      planning rows from the spec's read traits; channel_transmissions
      (a channel's crossing transmissions under a surface cursor wrapping
      the registry's); cross-agent semantics at every read (listings from
      each channel's traffic, hidden channels and transmissions within one
      agent left out of lists, counts, alerts and rows by id); and
      NodeCache, the spec's NodeFacts (listings and the channel holding
      each resource included), kept by NodeFeeder from L3's and L5's events
      and rebuilt from the stores on start. crosstalk-api's InProcess builds it over
      the reference stores with a relay from their outbox to the node
      facts and the feed; InProcess::start_with takes a composer's own
      Backbone (bus, blob store, outbox, relay input), which gateway's
      Live uses to feed the relay from the bus. The relay applies each backlog
      at once (NodeFeeder::apply_all) and InProcess::settle waits until it has
      applied everything published before the call; MemoryEvidence reads
      accesses and resources from the registry and keeps spans as recorded
      (SpanIndex); an opt-in in-process projection fitter
      (ProjectionFitting::Deterministic, FakeLayoutFitter) fits queued jobs.
      Rows by id and a channel's transmissions read topics from
      TopicCatalog::assignments under the page's version. The HTTP server (P7.1) is http_server.
    entry_points:
      - crates/surface/src/lib.rs
      - crates/surface/src/service.rs
      - crates/surface/src/stores.rs
      - crates/surface/src/query/mod.rs
      - crates/surface/src/actions/mod.rs
      - crates/surface/src/live/mod.rs
      - crates/surface/src/export/mod.rs
      - crates/surface/src/nodes/mod.rs
      - crates/api/src/in_process/mod.rs
      - crates/api/src/in_process/fitting.rs
    depends_on: [query_surface, read_models, export, channel_semantics, memory, transport, sim, testkit, workspace]
    doc: docs/features/surface_service.md
  http_server:
    description: >
      crosstalk-api's HTTP server (roadmap P7.1): the http_api binding
      served with axum 0.8 over any QueryApi + OperatorActions + LiveFeed.
      The router is registered from Route::all(), and every request is
      authenticated first (bearer token or session cookie, then the
      operator directory on a watch channel), so even an unserved path
      without a caller is a 401. Paths, queries and bodies are read with
      the spec's readers and decode_request; id batches, selections,
      excerpt windows and action requests go through their checked
      constructors, with 422 for a refusal. Errors are answered with
      their status and wire JSON, in their served() form (never a
      client-only Unavailable). GET /live is SSE resumed from
      Last-Event-ID or cursor, the projection frame is cached by its
      BLAKE3 ETag, and POST /exports streams JSONL with the trailer last.
      Every other response is no-store. HttpApi::new(surface, Auth,
      HttpConfig).router() plus serve(bind(api.listen)) is what the
      gateway's roles all and api mount over their Live process's surface,
      the bearer token from api.token mapped to api.operator. Tested against a fake surface with
      requests and responses from the wire goldens.
    entry_points: [crates/api/src/http/mod.rs, crates/api/src/http/routes.rs]
    depends_on: [http_api, query_surface, wire_contract]
    doc: docs/features/http_server.md
  http_client:
    description: >
      crosstalk-client (P7.2): HttpClient implements QueryApi,
      OperatorActions and LiveFeed over the HTTP binding, so the UI's
      server can use it in place of the in-process surface. Every call is
      encoded with the binding's RequestBuilder for its Route (query
      parameters form-encoded compact JSON, bodies the wire goldens) and
      carries one Authorization: Bearer token; an error response is
      decoded as the route's error and accepted only at the status the
      binding gives it (401 is AuthError); a call that never reached a
      surface (401, transport, cut body, timeout) is the client-only
      Unavailable { kind, reason } through the traits, the reason keeping
      its old text; the live feed is parsed as SSE,
      checked frame by frame and reconnected with Last-Event-ID from the
      last cursor delivered, ending with the client-only Unreachable when
      the attempts run out; a JSONL export is checked row by row with the
      binding's ExportSealer and ends Complete only when the surface's
      trailer verifies (download_export passes either format on as bytes);
      a projection frame is checked against its BLAKE3 ETag and revalidated
      with If-None-Match. Tested against a stub server speaking the binding.
    entry_points:
      - crates/client/src/client.rs
      - crates/client/src/query.rs
      - crates/client/src/live/mod.rs
      - crates/client/src/export/mod.rs
    depends_on: [http_api, wire_contract, export]
    doc: docs/features/http_client.md
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
  flow_store:
    description: >
      The L5 stores on Postgres in crosstalk-flow (crates/flow/src/store,
      roadmap P5) with the flow layer's migrations (schema flow).
      PgChannelRegistry implements ChannelRegistry, ChannelTraffic,
      ChannelReads and ChannelDirectory (lookups, declarations, policy
      history, promotion and supersession, resources, accesses, discovery,
      recorded channel traffic and the reads that tally it at query time);
      PgTransmissionStore implements TransmissionStore and
      TransmissionVerdicts. Every write is one SERIALIZABLE transaction under
      the store harness's retry, so concurrent discoveries from one resource
      commit one channel. Stores publish what they decide through a
      transactional outbox relayed to an EventSink after commit (at least
      once). The directory caches supersessions for the synchronous
      canonical(); ShardKey keys correlator shards by canonical channel and
      PgShardTicks keeps the shards' tick checkpoints. Model-tested against
      crosstalk-memory's reference stores with the reference harnesses'
      proptest strategies.
    entry_points:
      - crates/flow/src/store/mod.rs
      - crates/flow/src/store/registry/mod.rs
      - crates/flow/src/store/transmissions.rs
      - crates/flow/migrations/0001_flow_store.sql
    depends_on: [store, memory, channel_semantics]
    doc: docs/features/flow_store.md
  topology_store:
    description: >
      crosstalk-topology (roadmap P6.1): the spec's EdgeStore on plain
      Postgres (decision D3) as PgEdgeStore, and the topology bus consumer.
      Edge buckets per topic version and access buckets per resource live in
      range-partitioned tables (partitions made on demand); one row per
      applied contribution and access makes applies idempotent and backs
      the drill-down and FalseDetections::Exclude. Writes lock one state
      row (FOR SHARE for applies, FOR UPDATE for activation, retention and
      the watermark), so no bucket the exposed watermark finalizes changes;
      reads run in one REPEATABLE READ snapshot and resolve agents,
      channels, node facts and the topic version (spec TopicCatalog) at
      query time, folding in Rust. Events the store decides go to a
      transactional outbox, relayed after commit; traffic rows coalesce
      into one window per drain for Changed::Traffic (a marked hook until
      the follow-mode spec lands). consumer::run (group "topology") applies
      TransmissionClassified, AccessRecorded, VerdictSet and the version
      events to any EdgeStore, publishes EdgeUpdated before acking, and
      recomputes the watermark every bucket width. Model-tested against
      crosstalk-memory's InMemoryEdgeStore.
    entry_points:
      - crates/topology/src/lib.rs
      - crates/topology/src/store/mod.rs
      - crates/topology/src/consumer.rs
      - crates/topology/src/outbox.rs
      - crates/topology/migrations/0001_topology.sql
    depends_on: [type_spec, store, memory, transport, channel_semantics]
    doc: docs/features/topology_store.md
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
      shards; SpanIndex, the span records read in batches), MemoryChannels
      (ChannelRegistry, ChannelTraffic, ChannelReads, AccessStore and
      ChannelDirectory: lookups that create nothing, resources on a channel
      or on none, discovery by a cross-agent transmission, the recorded
      state of every channel transmission and the cross-agent traffic,
      listing and order read from it, declarations, policy history,
      promotion by promotion::plan and its coverage, supersession, resource
      use, accesses read back in batches) and MemoryVerdicts
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
      functions: the request body (system prompt as a string or blocks
      first, a role "system" turn inside messages as a System message in
      place, user turns split into maximal runs of one role so tool results
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
      BlobStore. A refused body's top-level shape (keys, roles, content
      kinds, never values) for the gateway's debug log. Goldens (the
      spec's NormalizedExchange JSON) over the testkit corpus, properties
      over generated requests and streams.
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
      (checked: cache counts within input), Reasoning::Visible a hashed
      signature, and later ToolCall one too (Gemini's thoughtSignature).
    entry_points:
      - spec/types/observed/message/encoding.rs
      - spec/types/observed/message/json.rs
      - spec/types/ids/secret.rs
      - spec/types/ids/mint.rs
      - spec/types/interfaces/l1_canonical.rs
    depends_on: [type_spec, wire_contract, sim]
    doc: docs/features/spec_primitives.md
  eval_gaps:
    description: >
      Detection rules from evaluating on real agent datasets (INV-950..973):
      ToolOutcome::Unknown for protocols without a failure flag; write
      outcomes (AccessOp::Write carries WriteOutcome Delivered, Rejected or
      Unknown, classified per known tool), with a writing call held until
      its result arrives or CorrelationTiming::write_settles_at passes
      (then Unknown), every write recorded and only Delivered and Unknown
      writes paired (CoAccess::new refuses a rejected one); a write's spans
      include the writer's own relayed spans, so a retry after a rejected
      write confirms; a ToolResult match on a resource its sender never
      wrote is a shared upstream source and keeps the transmission
      Suspected; ToolCall::signature hashed and never part text; decoders
      and the fingerprinter read part text only and decode strictly as
      UTF-8, and string serialisation is undone by two codecs
      (Codec::JsonString, Codec::YamlString, one level per chain). Batch
      reads shared by evaluation, the evidence page, the conversation view
      and the UI's world seed: SpanIndex::spans (span records with their
      author as recorded) and AccessStore::accesses (accesses with their
      resources), so evidence exists in every transmission state;
      CarrierKind splits quality rows by carrier; IngressMode::Replay {
      corpus: CorpusId } marks replayed datasets, which L3 keeps apart.
    entry_points:
      - spec/types/derived/flow/access.rs
      - spec/types/derived/flow/evidence.rs
      - spec/types/derived/flow/timing.rs
      - spec/types/interfaces/l5_flow.rs
      - spec/types/interfaces/l4_provenance.rs
      - spec/types/interfaces/l5_flow/channels.rs
      - spec/types/derived/provenance/matching.rs
      - spec/types/observed/message.rs
      - spec/types/observed/client.rs
    depends_on: [type_spec, spec_primitives, wire_contract, read_models]
    doc: docs/features/eval_gaps.md
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
      (Prometheus text; refusals as normalize_failed by fixed reason and
      protocol codes), /healthz (counters) and /readyz (database when
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
      stores and time. live::Live is the whole detection path and the L8
      surface in one process over the memory stores (what the UI hosts,
      the e2e smoke drives and eval builds against): Live::start(LiveConfig
      { surface, clock: LiveClock, blobs (memory or fs), bus, pipeline,
      flow: FlowConfig (correlation_window_ms, evidence_window_ms,
      suspected_ttl_ms, content_retention_ms, shards, tick_ms), provenance, extract:
      ExtractConfig (the gateway config's extract section), ticking, seed,
      capture }) fills one consumer slot per layer (L3
      ReconstructConsumer; L4 Provenance then the extraction step feeding
      L5 its Extracted inputs, pairing a tool result with its call
      wherever the request or the conversation's history carries it and
      reading a replayed result once; L5 FlowConsumer on its own task; the
      gateway's minimal L6 classifier; L7 topology::consumer::handle; an
      evidence feeder; a surface relay), forwards the stores' outbox onto
      the bus, and builds the surface with InProcess::start_with over the
      same stores. Live::settle(until) moves a manual clock, ticks every
      stage and drains every group until a pass changes nothing;
      Live::stores and Live::layers expose TransmissionStore::list and
      ExchangePlacements::placement for eval. Live advances the L7
      watermark on each tick when the layer groups are empty (the spec's
      Watermark::settled rule) and reports per-stage counts. serve runs a
      Live process in every role but analysis (memory stores, wall clock,
      periodic ticks every flow.tick_ms), feeds it from the proxy, and in
      roles all and api mounts crosstalk-api's HttpApi on api.listen (bearer
      token from api.token, mapped to api.operator, default admin, every
      permission); the optional flow section configures L5's windows;
      /readyz lists exchange_log, capture, live, proxy and api, and
      /healthz has a live section (stage counts, watermark_micros).
    entry_points:
      - crates/gateway/src/main.rs
      - crates/gateway/src/gateway.rs
      - crates/gateway/src/pipeline/mod.rs
      - crates/gateway/src/pipeline/ingest.rs
      - crates/gateway/src/live/mod.rs
      - crates/gateway/src/live/settle.rs
      - crates/gateway/src/live/wiring.rs
      - crates/gateway/src/live/layers/l7.rs
      - crates/gateway/src/capture.rs
      - crates/gateway/src/config/mod.rs
      - crates/gateway/src/log/mod.rs
      - crates/gateway/src/ops/mod.rs
      - scripts/try-claude-code.sh
    depends_on: [ingress, canonical, transport, store, workspace, sim, testkit, memory, surface_service, http_server, reconstruct, provenance, flow_extract, flow_correlator, topology_store]
    doc: docs/features/gateway.md
  analysis:
    description: >
      crosstalk-analysis, the L6 layer crate (P6.2/P6.3). Its Postgres
      search and alert stores are search_alerts. Its remote module: SidecarTopicModel (TopicModel: fits the catalog's
      version over documents at a given time through the sidecar, computes
      centroids as normalized member means and derives topic ids from the
      fit time, version and cluster; assigns locally to the nearest
      centroid above a threshold), SidecarLayoutFitter (LayoutFitter, plus
      transform onto an existing layout), OpenAiEmbedder (Embedder:
      batched, ordered by index, normalized, dimension probed, key from an
      env var and never disclosed), a shared hyper/rustls client with a
      deadline per call, and typed errors in which only the sidecar's
      deterministic refusals become TooFewSamples or a FitFailure.
      Contract-tested against testkit's fake server and the sidecar's own
      fixture files; ignored live tests run against the real sidecar.
    entry_points:
      - crates/analysis/src/remote/mod.rs
      - crates/analysis/src/remote/sidecar/topics.rs
      - crates/analysis/src/remote/sidecar/layout.rs
      - crates/analysis/src/remote/embedder.rs
    depends_on: [type_spec, topics_sidecar, testkit, workspace]
    doc: docs/features/analysis.md
  topics_sidecar:
    description: >
      The Python topics sidecar (sidecar/topics, roadmap D1/P6.3): a
      FastAPI service on port 8090 with /healthz, /v1/topics/fit (UMAP
      reduction, HDBSCAN clusters renumbered by size, c-TF-IDF terms and
      labels), /v1/layout/fit and /v1/layout/transform (seeded UMAP to two
      dimensions, fitted bases in an LRU cache). Contract v1: JSON in the
      spec's wire conventions, embeddings and coordinates as exact
      little-endian binary32 hex matrices, adjacently tagged errors mapped
      to HTTP statuses. Same request bytes give the same response bytes
      (SeedSequence-seeded UMAP, one thread for BLAS and Numba, a generic
      Numba target; bit-identity per image and CPU architecture). Pinned
      with uv; pytest with determinism and golden tests; image
      deploy/topics.Dockerfile (python slim, non-root).
    entry_points:
      - sidecar/topics/src/crosstalk_topics/app.py
      - sidecar/topics/src/crosstalk_topics/topics.py
      - sidecar/topics/src/crosstalk_topics/layout.py
      - sidecar/topics/pyproject.toml
      - deploy/topics.Dockerfile
    depends_on: [wire_contract]
    doc: docs/features/topics_sidecar.md
  reconstruct:
    description: >
      crosstalk-reconstruct, L3 (P4.1). Evidence derivers (credential by
      stability, account, scoped harness ids, prompt fingerprint, their
      chain) with the identity scope decided in one place
      (evidence::scope, ready for per-corpus replay scoping); PgAgents,
      every L3 agent store trait on Postgres (merge log with exact unmerges,
      vetoes, renames, resolve, lifecycle, claims, activity, reads; an
      outbox; an in-process directory cache), model-tested against
      crosstalk-memory; ConversationThreader over MemoryConversations or
      PgConversations (prefix chains, forks, compaction from summary
      turns, WebSocket increment resolution scoped by upstream and identity
      scope, system turns anywhere, every message kept in order under an
      ordinal, a per-agent seen-message set within a configured retention
      that keeps history replayed from another conversation out of a
      delta's new inputs, INV-1100); and the reconstruct consumer (ExchangeCaptured in;
      AgentSeen and ConversationDelta out under envelope ids derived from
      the exchange). Replays AI Village's Claude Code stream and lmcache's
      interleaved re-runs as ignored fixture tests.
    entry_points:
      - crates/reconstruct/src/lib.rs
      - crates/reconstruct/src/agents/mod.rs
      - crates/reconstruct/src/thread/mod.rs
      - crates/reconstruct/src/consumer/mod.rs
      - crates/reconstruct/src/evidence/mod.rs
    depends_on: [type_spec, store, memory, transport, sim, testkit]
    doc: docs/features/reconstruct.md
  provenance:
    description: >
      crosstalk-provenance, L4 (P4.2). Winnowing (the spec's
      Fingerprinter) over whitespace- and case-normalized shingles with a
      stable rolling hash pinned by golden vectors; base64, hex, URL and
      Unicode decoders (the spec's Decoders) plus JSON and YAML string
      unescapes, run depth-bounded with byte maps back to the part text;
      the NovelRunSegmenter, which follows copied runs through every
      decode layer of the exchange's inputs (relayed) and leaves the rest
      originated; the scanner, which looks up new inputs, the new system
      prompt and the output (k-grams, plus exact hashes of short token
      runs for whole values of 24 to 46 characters), resolves originated
      text against the index (hidden relays become ReaderOutput matches
      under stricter length and rare-token rules, boilerplate Common),
      drops short matches that are template skeletons or that the origin
      was given token for token in its own request, and
      picks carrier, kind, read range and matched bytes; the index holds
      originated spans and, with forwarding on (off by default), forwarded
      ones (relayed from the agent's own input, indexed under the
      forwarder, state left Relayed), with the
      k-grams an originated remainder mostly covers next to a forward
      posted under it; the engine, which
      records exchanges from ExchangeCaptured and scans each
      ConversationDelta, writes the index, republishes the stored outcome
      with deterministic ids on redelivery and evicts after retention;
      the bus consumer (group provenance) publishing SpanOriginated,
      SpanRelayed and ContentMatched; PgFingerprintIndex model-tested
      against crosstalk-memory's reference; L4's records (exchanges with
      per-exchange and per-message scan status, spans by id, matches by
      reader message and by origin span) in memory and on Postgres; a
      semantic matcher stub until P6.2. Validated on AgentDojo: 9948 of
      9949 exposed injection slots matched.
    entry_points:
      - crates/provenance/src/lib.rs
      - crates/provenance/src/engine.rs
      - crates/provenance/src/consumer.rs
      - crates/provenance/src/scan/mod.rs
      - crates/provenance/src/segment/mod.rs
      - crates/provenance/src/decode/mod.rs
      - crates/provenance/src/fingerprint/mod.rs
      - crates/provenance/src/index/pg.rs
      - crates/provenance/src/store/mod.rs
      - crates/provenance/migrations/0001_provenance.sql
      - crates/provenance/migrations/0002_forwarded_spans.sql
    depends_on: [type_spec, spec_primitives, store, memory, transport, sim, testkit, workspace]
    doc: docs/features/provenance.md
  deploy:
    description: >
      Single-machine deployment: images for the gateway and the UI, a docker
      compose stack (Postgres 18 with pgvector, pg_trgm and
      pg_stat_statements; a migrate step; crosstalk serve --role all; the
      UI), generated secrets in deploy/.env, and infrastructure
      observability (host, container, Postgres and log metrics, dashboards,
      alert rules). Defines the contract the crosstalk binary implements:
      serve/migrate/healthcheck commands, ports 8080/8081/9464, the ops
      endpoints and the config file's top-level keys. Also notes for
      running on a shared host (snap Docker, host port clashes, syncing a
      checkout, a smoke test, inspecting the distroless gateway).
    entry_points:
      - deploy/compose.yaml
      - deploy/run.sh
      - deploy/crosstalk.Dockerfile
      - deploy/config/crosstalk.json
      - docs/infrastructure.md
    depends_on: [workspace, store, ingress, transport]
    doc: docs/features/deploy.md
  demo:
    description: >
      crosstalk-demo (crates/demo, a tool crate no layer depends on), one
      binary with four subcommands. upstream is a fake Anthropic upstream:
      POST /v1/messages, streaming SSE or JSON, in the real wire format,
      answered with text and tool_use deterministically from a seed and the
      request body, with a configurable first-byte wait and stream pacing.
      Its prose is high-entropy (unrelated outputs share no 32-byte run) or
      templated boilerplate, picked per request by a [style:headline] /
      [style:boilerplate] marker the swarm puts in each agent's system
      prompt from its --scenario headline|boilerplate (default headline),
      so one stateless upstream serves both scenarios.
      wiki is an in-memory HTTP page store with versions and authors, the
      shared channel. swarm runs N agents through the crosstalk proxy. Each
      keeps a growing conversation, resent whole every turn, with fake
      x-api-keys per agent or group. The one declared tool is http_request
      (L5's HTTP tool contract); the model's GET and PUT calls of
      <wiki>/pages/<page> run against the wiki, and their results go back as
      tool_result, so one agent's model output reaches another's input and
      L5 can discover the wiki as a channel. swarm reports throughput,
      p50/p95/p99 time to first byte and total time, and the expected
      transmissions, self-reads, rereads and misses, optionally as a
      ground-truth JSONL file (schema v2: header with the scenario, agent clusters, and per
      read the writer's and reader's session, turn and tool_use id, the
      content's hashes and its exact message/block in the reader's request),
      which ct-eval scores against.
      healthcheck serves the distroless image. deploy/compose.demo.yaml,
      deploy/demo.Dockerfile (which also ships ct-eval for run.sh bench),
      deploy/demo/crosstalk.demo.json and run.sh demo up|run|down|logs run
      the demo on the compose stack. It reuses
      testkit's harness client and SSE parser and the spec's seeded random
      source.
    entry_points:
      - crates/demo/src/main.rs
      - crates/demo/src/upstream/mod.rs
      - crates/demo/src/wiki/mod.rs
      - crates/demo/src/swarm/mod.rs
      - crates/demo/src/swarm/truth.rs
      - deploy/compose.demo.yaml
      - deploy/run.sh
    depends_on: [testkit, deploy, gateway, workspace]
    doc: docs/features/demo.md
  bench:
    description: >
      The live detection benchmark on the single-machine compose
      deployment (bash deploy/run.sh bench, implemented in deploy/bench.sh).
      One run: restart wiki and crosstalk at the start (fresh world, empty
      in-memory detection state), run the demo swarm through the real
      gateway writing ground truth v2 to deploy/bench/<run>/ (gitignored,
      run id a UTC timestamp), wait until /healthz live.watermark_micros
      passes the swarm's end (exports are cut at the watermark), export the
      gateway's detections with ct-eval swarm-fetch over the L8 API, and
      score them with ct-eval swarm against the exchange log and blobs read
      in place from the data volume (mounted read-only into the bench
      service). Prints precision, recall and the gate result and passes
      ct-eval's exit code through (2 = a gate failed). Fails fast unless
      /readyz has the `live` and `api` tasks running and the API takes the
      token. ct-eval ships in the crosstalk-demo image. --scenario
      headline (default, high-entropy prose: the headline precision and
      recall) or boilerplate (templated prose unrelated agents share: a
      regression scenario for false positives on shared text), recorded in
      bench.env.
    entry_points:
      - deploy/run.sh
      - deploy/bench.sh
      - deploy/compose.demo.yaml
      - deploy/demo.Dockerfile
    depends_on: [demo, eval, deploy, gateway, http_api]
    doc: docs/features/bench.md
  conformance:
    description: >
      crosstalk-conformance (crates/conformance, TestSupport): the L8
      conformance suite, tests generic over any implementation of the L8
      traits (QueryApi, OperatorActions, LiveFeed, export). A harness
      provisions worlds from scenarios (facts over typed roles: agents,
      resources, channels, transmissions with their evidence, merges,
      promotions, policies, verdicts, topic history, dead letters), the
      suite checks every fact of a scenario through L8 before relying on
      it, and assertions are relations the spec defines and what the facts
      imply, citing spec/invariants ids, never totals of one world.
      suite!(harness) instantiates every test; a harness lists what its
      implementation is known to fail, and a listed test that passes fails.
      It runs against the UI fixture (ui/src/backend/fixture/conformance),
      the in-process crosstalk-surface over memory stores seeded by
      crosstalk-world (crates/api/tests/conformance.rs) and the same surface
      over HTTP through crosstalk-client (crates/client/tests/conformance.rs),
      the last two through crosstalk_api::world (feature world: seed_world,
      serve_world) and the world binder over the store read traits. The
      real surface passes all 65 in process and over HTTP (SURFACE_FAILURES
      is empty). Next: Postgres, and scenarios seeded through the write
      traits.
    entry_points:
      - crates/conformance/src/lib.rs
      - crates/conformance/src/harness/mod.rs
      - crates/conformance/src/scenario/mod.rs
      - crates/conformance/src/suite.rs
      - ui/src/backend/fixture/conformance/mod.rs
      - crates/api/src/world.rs
      - crates/api/tests/conformance.rs
      - crates/client/tests/conformance.rs
    depends_on: [query_surface, read_models, export, channel_semantics, type_spec, world, http_api, surface_service]
    doc: docs/features/conformance.md
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
      the gap list: fixture reads no store or spec trait answers. Origin
      spans are recorded through L4's SpanIndex (WorldStores::Spans).
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
  ui:
    description: >
      Operator web UI (crosstalk-ui, a workspace member): an overview,
      topology (agents or bipartite with channels) with an edge drawer,
      transmission evidence with verdicts, search and UMAP exploration,
      topics (with version pins), channels (active, unconfirmed, declared
      with no traffic yet, and the review queue; a channel's suspected
      transmissions with verdicts; a shared confirmed-only filter) with
      promotion, agents with merges, alerts and rules, export (JSON Lines
      downloads), audit and pipeline, kept current by the SSE live feed.
      Reads and acts through the spec's L8 traits (QueryApi,
      OperatorActions, LiveFeed), the channel-semantics port's shapes
      included, on one of three backends (backend::AppBackend): the
      deterministic fixture (default; optionally replaying its last hours
      for demos), the world backend (crosstalk_api::InProcess over the
      memory stores, seeded with crosstalk-world; dev and demos), or the
      http backend (crosstalk_client::HttpClient over the gateway's L8 API,
      `"backend": {"http": {"url", "token": {"env"}}}`; the operator and
      its permissions are the server's for the token; transport and auth
      failures render as the UI's error states and are logged, and a page
      that cannot read the present because the gateway is unreachable or
      refused the token is a full-page gateway state, 503 or 502, showing
      the gateway's URL). The UI declares no traits of its own: the
      clock, bucket width, export formats and rule version come from
      QueryApi::present (app::present, once per request), and where a
      default view ends from AppBackend::view_end.
      Built from the workspace root into deploy/ui.Dockerfile and
      deploy/ui.demo.Dockerfile with its Topcoat asset bundle.
    entry_points:
      - ui/src/main.rs
      - ui/src/app.rs
      - ui/src/backend/mod.rs
      - ui/src/backend/dispatch.rs
      - ui/src/backend/fixture/surface.rs
      - ui/src/backend/world/mod.rs
      - ui/src/backend/http/mod.rs
      - ui/src/config/mod.rs
      - ui/src/identity.rs
      - ui/src/pages/mod.rs
      - ui/src/pages/view.rs
      - ui/src/data/mod.rs
      - ui/src/data/live.rs
      - ui/src/pages/topology/mod.rs
      - ui/src/pages/explore/mod.rs
      - ui/src/pages/export/mod.rs
      - ui/elements/src/topology/element.ts
      - ui/elements/src/live/element.ts
      - deploy/ui.Dockerfile
    depends_on: [query_surface, read_models, export, type_spec, workspace, deploy, memory, world, surface_service, http_client, http_server]
    doc: docs/features/ui.md
  eval:
    description: >
      crosstalk-eval and the ct-eval CLI (a composer): dataset converters
      (SALT-NLP, AgentDojo, tau2-bench, AI Village, collusion-wiki (synthesised
      http_request reads and writes of public wiki pages) and swarm-traces (a
      decode-chain corpus reported by chain, count and length only))
      streaming worlds of checked
      spec NormalizedExchanges on a deterministic virtual clock (datasets
      without times pace calls 1 to 5 s apart, seeded and configurable;
      synthetic tool calls are answered in the agent's next request of one
      growing conversation), with typed,
      JSONL-serialisable ground truth (expected transmissions, out of reach
      when undecodable or read from a medium the sender never wrote
      (INV-963), negative controls, exemptions, agent clusters, with tiers); predictions converted from
      spec Transmissions (one per ContentMatch, and one per CoAccess of a
      suspected or discarded transmission) through a read seam over the
      spec's SpanIndex, AccessStore and channel reads; one documented
      alignment rule and a scorer with TP/FP/FN by dataset, route, carrier
      kind, match or access class and tier, negative-control violations
      and a DetectionQuality bridge keyed by QualityMatch; a Detector seam
      with the naive reference matcher (escape-aware matching classed as
      Exact, Normalized or Decoded([JsonString | YamlString]) through one
      classifier, with hits only two string levels explain out of reach
      and unreported, decoding, opaque-blob exclusion, a boilerplate cutoff
      on shingle postings at L4's default of 50), the gateway pipeline (Pipeline::ingest under
      the corpus clock or a sim clock, reported as unscored), and
      LiveDetector over the LiveBackend seam (a fresh composition per
      world: ingest, settle, list transmissions, read spans, accesses,
      channel resources and L3 attribution), whose adapter
      (detect::live::gateway) drives the merged crosstalk_gateway::live::Live
      (ct-eval run --detector live, with overridable correlation windows and
      a --predictions JSONL dump), and ct-eval replay, which re-runs a
      saved node0 bench run (exchange log, blobs, truth, bench.env)
      through Live offline and scores it like ct-eval swarm, discarded
      co-access aligned with no label counted dismissed rather than false;
      reports (overall, out of reach,
      access-only recall, background) and regression gates per detector
      (reference or live), found through --gates, CT_EVAL_GATES, the bench
      image's installed file or the crate's own, else none. Every converter labels escaped text with the spec's
      string codecs, never Normalized.
    entry_points:
      - crates/eval/src/lib.rs
      - crates/eval/src/pipeline.rs
      - crates/eval/src/gateway.rs
      - crates/eval/src/score/align.rs
      - crates/eval/src/datasets/salt/mod.rs
      - crates/eval/src/datasets/wiki/mod.rs
      - crates/eval/src/datasets/swarm/mod.rs
      - crates/eval/src/bin/ct-eval/main.rs
      - crates/eval/src/datasets/agentdojo/mod.rs
      - crates/eval/src/datasets/tau2/mod.rs
      - crates/eval/src/datasets/swarm_truth/mod.rs
      - crates/eval/src/datasets/swarm_truth/replay.rs
      - crates/eval/src/predict/reads.rs
      - crates/eval/src/detect/live/mod.rs
      - crates/eval/src/detect/live/gateway.rs
      - crates/eval/src/reference/classify.rs
      - crates/eval/src/report/gates.rs
    depends_on: [type_spec, gateway, transport, flow_extract, export, http_api, eval_gaps, sim, testkit, memory]
    doc: docs/features/eval.md
  e2e_smoke:
    description: >
      crosstalk-e2e (a composer): the end-to-end smoke. A deterministic
      wiki relay scenario as Claude Code HTTP traffic (agent A writes a
      shared wiki page with a distinctive sentence through Write, agent B
      reads it through Read and repeats it; two sessions, two API keys),
      captured through L0's route table, identifier and adapter and L1's
      normalizer into NormalizedExchanges, fed through Pipeline::ingest in
      time order, and read back only through QueryApi (agents by session,
      the A to B channel edge, the confirmed transmission, its evidence,
      and the channel the cross-agent transmission created, dated by its
      opening, listed and confirmed). The composition is shaped like
      crosstalk_gateway::live::Live and is wired today from InProcess plus
      a pipeline over its blob store and bus; the assertions needing L3 to
      L7 are ignored until Live composes them. The scenario and readers are
      a library, so a UI demo can feed the same traffic into a running
      Live.
    entry_points:
      - crates/e2e/src/lib.rs
      - crates/e2e/src/scenario/mod.rs
      - crates/e2e/src/capture.rs
      - crates/e2e/src/compose.rs
      - crates/e2e/src/read.rs
      - crates/e2e/tests/smoke/main.rs
    depends_on: [gateway, ingress, canonical, surface_service, memory, workspace]
    doc: docs/features/e2e_smoke.md
  flow_extract:
    description: >
      crosstalk-flow's extract module, L5 (P5): the spec's
      ResourceExtractor over every known tool (Claude Code's file, fetch
      and Bash tools and their OpenCode, pi, Gemini CLI, Codex and text
      editor equivalents; HTTP tools such as http_request {method, url,
      body?}, the method deciding the op; fetch tools configured by name,
      fetch_tools, such as AgentDojo's get_webpage, a scheme-less
      host[:port][/path] url read as https://; MCP tools mapped by
      typed JSON configuration of tool name and argument paths to a
      resource and an op). Every locator is canonical, so agents touching one thing meet
      on one resource: lexical paths, relative paths against the stated
      or tracked working directory (Opaque without one), normalized URLs
      (Opaque, keyed by the normalized raw text, when IDNA refuses the host),
      folded MCP keys, MediaWiki pages as their canonical article URL
      whatever URL or API reaches them, a forge repository as the spec's
      canonical Locator::Repository from every remote, web, API and Pages
      form, its files (forge URLs and files of known clones) as the
      repository's file, GitHub/GitLab issues and pull/merge requests as
      their canonical web URL. A conservative shell lexer and interpreter
      reads redirections, file readers (cat, head, tail, sed -n), tee,
      curl, wget, cd, git (clone, remote, show, push as an Unseen-payload
      write, pull/fetch/clone as reads) and the gh/glab CLIs (issue and
      PR/MR threads, api as the HttpTool contract); OpenHands'
      execute_bash and str_replace_editor are known tools. Each write
      carries its outcome (Delivered, Rejected, Unknown), judged in one
      place per tool and, for git, curl/wget and gh/glab, from the
      command's output; reads need a delivered result; a write's locators
      never depend on its result. ConversationContext learns the
      persistent shell's directory and clones from shell calls. Builds the
      stored AccessOp with the write's spans (originated, forwarded from an
      input, plus self-relayed sources; none for an Unseen payload).
    entry_points:
      - crates/flow/src/extract/mod.rs
      - crates/flow/src/extract/context.rs
      - crates/flow/src/extract/outcome.rs
      - crates/flow/src/extract/mcp/config.rs
      - crates/flow/src/extract/spans.rs
      - crates/flow/src/extract/bash/forge.rs
      - crates/flow/src/extract/resource/repo.rs
    depends_on: [type_spec, channel_semantics, workspace]
    doc: docs/features/flow_extract.md
  flow_correlator:
    description: >
      The L5 correlator and flow consumer in crosstalk-flow (P5, the M2
      path). WindowedCorrelator (the spec's Correlator, one per shard)
      pairs a write and a later read of one resource by another agent into
      a CoAccess (within the correlation window on access alone, within
      FlowConfig::content_retention_ms, default L4's 30-day index
      retention, when a held match the write explains backs it, so a
      dead drop read days later confirms; a reread of a span already
      delivered to the same reader on the medium refreshes that delivery
      and confirms nothing new), opens the channel
      transmission, and at the close of the
      read's evidence window confirms it with every held tool-result match
      a write of the sender explains (origin span among the write's
      spans), else suspects it; late matches confirm a suspicion, later
      ones extend, expiry discards, content after a discard opens a new
      transmission; a match no sender write explains (shared upstream)
      confirms nothing. Delegation (parent links read through AgentReads),
      Direct and Unobserved matches open confirmed at their exchange's
      window close. Every pairing rule lives in correlate/pairing.rs, ready
      for the eval PR's WriteOutcome. Shards are keyed by medium (canonical
      channel or resource); a medium's evidence moves on discovery,
      ChannelDiscovered, ChannelPromoted and any access resolved to a
      channel. The flow consumer (group flow) records accesses
      (add_resource, record_access, AccessRecorded), holds writes until
      their outcome or settle time, feeds the shards, and applies each
      decision through discover, TransmissionStore::save and
      record_transmission before publishing ChannelCrossAccessed,
      TransmissionConfirmed and TransmissionSuspected, one ordered step
      queue with retries. Time is only input event times and ticks of the
      injected clock, so replayed corpora settle on the replay clock. Its
      input from extraction is a local type until the spec has an event
      for it.
    entry_points:
      - crates/flow/src/correlate/windowed.rs
      - crates/flow/src/correlate/pairing.rs
      - crates/flow/src/consumer/mod.rs
      - crates/flow/src/consumer/shards.rs
      - crates/flow/src/consumer/apply.rs
    depends_on: [type_spec, channel_semantics, memory, sim, testkit, transport]
    doc: docs/features/flow_correlator.md
  search_alerts:
    description: >
      crosstalk-analysis (crates/analysis, L6, P6.2) on Postgres. PgSearchIndex
      implements SearchIndex and SearchCorpus (full-text tsvector terms plus
      pgvector embeddings, scores computed in SQL exactly as the reference,
      the filter applied in Rust before the page is cut, keyed cursors), and
      PgProjectionSource samples the same documents. PgAlertStore implements
      AlertRuleStore, AlertTriage, AlertRuleMaintenance, AlertActions and
      AlertReads in SERIALIZABLE transactions with an outbox published after
      commit; RuleEvaluator is AlertRuleEval for every rule kind; AlertsStage
      is the alerts consumer group, built like the gateway pipeline's
      stages. Model-tested against crosstalk-memory's harnesses.
    entry_points:
      - crates/analysis/src/search/mod.rs
      - crates/analysis/src/alerts/mod.rs
      - crates/analysis/src/alerts/eval/mod.rs
      - crates/analysis/src/alerts/consumer.rs
      - crates/analysis/src/pg/mod.rs
    depends_on: [type_spec, store, memory, gateway]
    doc: docs/features/search_alerts.md
  eval_swe_synthetic:
    description: >
      Background and synthetic corpora for crosstalk-eval (ct-eval --dataset
      open-swe | lmcache | swe-splice | cipher). Open-SWE-Traces and LMCache
      trajectories (Parquet, read with the parquet crate; Open-SWE tool
      results paired with calls by position, LMCache responses taken from
      the next request) are mixed into background worlds with only
      Boilerplate/SharedSource negative controls. Seeded splices plant one
      Channel/File transmission from A's file write to B's harness-format
      read (exact, whitespace, JSON-string, base64). Seeded cipher pairs
      deliver steganographic-evals payloads encoded: base64, hex, URL and
      chains are in reach, while rotN, binary8 and substitution are
      Tier::OutOfReach and reported as missed by design; a JSON-string
      splice read through a shell is in reach (L4 decodes the writer's
      argument values, INV-1057, so the reader needs one level). crosstalk-flow's
      real extractors resolve the shell cat -n read and the editor view to
      the same Locator::File read. Reports gain out_of_reach and background
      (false positives per 1k exchanges, top sources) summaries.
    entry_points:
      - crates/eval/src/datasets/background.rs
      - crates/eval/src/datasets/open_swe/mod.rs
      - crates/eval/src/datasets/lmcache/mod.rs
      - crates/eval/src/datasets/swe_splice/mod.rs
      - crates/eval/src/datasets/cipher/mod.rs
      - crates/eval/src/score/sources.rs
    depends_on: [eval, type_spec, flow_extract]
    doc: docs/features/eval_swe_synthetic.md
  eval_ai_village:
    description: >
      The AI Village converter (crosstalk-eval datasets::ai_village, ct-eval
      --dataset ai-village). Claude Code mode: the Claude Agent SDK agent's
      exact calls, one world per context, with construction-tier labels for
      every chat message it read through the village MCP server's
      get_events (keyed by event id; Direct/ToolResult). Window mode
      (default 2026-07-13..17): every standard agent, one world per village
      day, requests rebuilt from responses (system prompt from goals and
      memory, session history, chat since the previous call), structural
      chat labels, heuristic repository-channel labels from bash accesses
      on canonical repository URLs, GUI edits counted. Bash accesses follow
      the agreed L5 HttpTool contract: curl, wget and gh/glab api keep
      their equivalent http_request {method, url, body} call, git and the
      forge CLIs' issue commands are marked Bash-only, every resource is a
      canonical URL (L5's url_locator off the forges, the repository's web
      URL on them), and each write carries the spec's WriteOutcome
      (rejected writes never pair). Its streaming table passes, resource
      normalizer and bash access tagger are reusable.
    entry_points:
      - crates/eval/src/datasets/ai_village/mod.rs
      - crates/eval/src/datasets/ai_village/tables.rs
      - crates/eval/src/datasets/ai_village/resource.rs
      - crates/eval/src/datasets/ai_village/access/mod.rs
      - crates/eval/src/datasets/ai_village/access/http.rs
    depends_on: [eval, flow_extract]
    doc: docs/features/eval_ai_village.md
```
