# Crosstalk overview

```yaml
Overview:
  description: >
    A gateway that sits between agent harnesses and inference APIs. It
    proxies every request unchanged and watches for one agent's output
    showing up in another agent's input: in a tool result, a user turn or a
    system prompt. From that it classifies agent-to-agent communication,
    discovers the channels agents use (including ones nobody declared, such
    as a public wiki agents start writing to; a resource becomes a channel
    once a transmission between two different agents goes through it, and
    a channel whose transmissions are all suspected is marked unconfirmed),
    and records the content,
    topology and location of the communication. The records power a
    topology view with edge weights and per-node metadata, a channel-centred
    view of who reads and writes each channel, search, topic modeling,
    clustering, a UMAP view of message content, and alerts.

    It works with Claude Code, Codex, pi and oh-my-pi, against vendor APIs,
    subscription backends reached with OAuth (Claude Pro/Max, ChatGPT/Codex,
    GitHub Copilot, Gemini Code Assist) and self-hosted vLLM or SGLang, over
    HTTP, SSE and WebSocket.

    Status: design. The data model is specified in spec/types; the operator
    UI (ui/) runs on the spec's L8 traits, implemented by a fixture backend
    until the gateway exists.

  subsystems:
    ingest: >
      L0 ingress (reverse and forward proxy, upstream routing, credential
      hashing, provider adapters, SSE framing and WebSocket taps), L1
      canonicalization (wire format and dialect to canonical Exchange and
      Message), L3 reconstruction (agent identity, the merge log with exact
      unmerge and vetoes, renames, harness claims seen per agent,
      conversation threading, WebSocket increment resolution).
    transport: >
      L2: the event bus (in-process channels on one node, NATS JetStream
      across nodes) and the content-addressed blob store. The only path
      between components.
    detect: >
      L4 provenance (span extraction, novelty classification, fingerprint
      index, content matching) and L5 flow detection (resource extraction,
      channel registry with discovery on the first cross-agent transmission
      through a resource, promotion and supersession, write/read
      correlation into transmissions, each channel's cross-agent traffic
      read at query time with merges resolved (confirmed, unconfirmed, or
      none: a declaration without traffic, or a hidden channel), and the
      operator verdict log kept beside each transmission).
    insight: >
      L6 analysis (embeddings, topics, the topic-model version history with
      sizes, lineage, pins and retention, paged search, stored projection
      jobs fitted in the background, built-in and user alert rules with
      their sinks, triage), L7 topology (edge and access buckets per time
      window, graphs with node metadata, the channel-centred graph, time
      series, and the watermark before which every bucket is final), L8
      surface (the query API with cursor-paginated lists and linked views
      sharing one filter and one resolved topic-model version; read models
      for agents, channels and transmissions: canonical agent rows and
      details that follow merges, channel rows carrying their cross-agent
      traffic, listing and activity or their supersession, a channel's
      cross-agent transmissions for review, a promotion preview computed by
      the promotion's own
      plan, batch names over one bounded id batch, transmission rows by id
      with a per-state shape, the evidence behind a transmission with
      excerpts cut from stored bodies, the overview's counts and one alert
      by id; typed query and action errors; the operator directory with a
      trusted single-user mode; operator actions with one permission each;
      the append-only audit log of operator actions, config changes and
      exports; the id-only SSE live feed; streamed exports with a header
      and trailer manifest; alert sinks).
    ui: >
      The operator web UI (crosstalk-ui, Topcoat): server-rendered pages
      plus custom elements for the topology graph and UMAP projection
      (WebGL), the time brush (SVG) and the live-update listener, fed by
      the UI's own /data/ routes. Reads, acts and subscribes only through
      the spec's L8 traits (QueryApi, OperatorActions, LiveFeed), called on
      one concrete backend type (a deterministic fixture implementing them
      until the gateway exists), plus two documented gap traits for what
      the spec does not expose yet (the bucket width and the present; the
      export formats a backend writes). Callers come from the spec's
      operator directory in trusted mode; view windows are bucket-aligned
      and every linked view pins the URL's topic version.

  data_flow: >
    Harness request (via its base URL, or via the gateway as HTTPS proxy) →
    L0 routes it to its upstream, hashes the credential, forwards it
    unchanged without waiting for its body to decode, decodes the body
    concurrently off the hot path, and tees the response (or each WebSocket
    turn) → RawExchange (in-process) → L1 normalizes, writes message bodies
    to the blob store, publishes ExchangeCaptured → L3 resolves the agent,
    records its harness claim and threads the conversation, publishes
    ConversationDelta → L4 indexes the agent's originated spans and matches
    new inputs against other agents' spans (ContentMatched); L5 turns tool
    calls into accesses on resources and their canonical channels, if any
    (AccessRecorded), correlates cross-agent accesses and content matches
    into transmissions (TransmissionConfirmed / Suspected), and discovers a
    channel from a resource on no channel when the first transmission
    between two different agents goes through it (ChannelDiscovered,
    raising NewChannel) → L6 embeds and
    classifies transmissions, records topic-model versions and their
    lineage, and evaluates alert rules → L7 aggregates edges and access
    buckets, advances the watermark from the correlator's ticks and the
    oldest unprocessed input, and announces topic-version activation back
    to L6, which then drops the versions its retention policy no longer
    keeps (TopicVersionDropped; L7 deletes their buckets) → L8 serves
    topology, the channel-centred graph, a channel's resources, series,
    topic history, search, projections, verdicts, detection quality,
    lists, alerts, and the read models: agent rows (L3's profiles joined
    with L7's traffic in the window) and details following merged ids;
    channel rows (cross-agent traffic over all time from L5's
    transmissions with merges resolved, from which each row's listing
    follows: a confirmed or unconfirmed channel, a declaration without
    traffic, or hidden; writers and readers from L5's resource use,
    transmissions from the same graph the overview counts, over the channel
    and every channel it superseded, in an optional window that never
    changes which rows are listed); a channel's cross-agent transmissions; promotion previews (the registry's promotion plan run
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
    channel's detection; a transmission whose two agents merged into one
    counts nowhere, and a discovered channel left with no cross-agent
    transmission is hidden until an unmerge; the filter's
    unconfirmed_channels leaves out channels whose traffic is all
    suspected); projection fits run as background jobs whose
    stored frames read back exactly. Every store publishes an id-only
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
    The operator UI renders L8's reads (QueryApi), with view state in the
    URL (a bucket-aligned window and a pinned topic version), sends every
    operator action through OperatorActions::act, downloads exports as
    JSON Lines, and re-renders a page's region when the live feed (SSE from
    LiveFeed) names something the page shows.

Features Index:
  type_spec:
    description: >
      The gateway's data model as type-checked Rust: observed facts
      (including clients, upstreams, credentials, the merge log and harness
      claims), derived inferences (including channels discovered by their
      first cross-agent transmission, their confirmation and listing read
      from that traffic, channel promotion with supersession, and operator
      verdicts beside the detector's state),
      aggregates (including edge and access buckets, time series, topic
      history with retention, and the watermark that marks buckets final),
      bus events and per-layer interfaces, with tests for the invariants
      checked at runtime and one TOML file per invariant in
      spec/invariants. Harness and server wire behavior it is based on is
      in docs/research/harness-wire-protocols.md.
    entry_points: [spec/types/mod.rs, spec/Cargo.toml]
    depends_on: []
    doc: docs/features/type_spec.md
  ui:
    description: >
      Operator web UI: an overview, topology (agents or bipartite with
      channels) with an edge drawer, transmission evidence with verdicts,
      search and UMAP exploration, topics (with version pins), channels
      (active, unconfirmed, declared with no traffic yet, and the review
      queue; a channel's suspected transmissions with verdicts; a shared
      confirmed-only filter) with promotion, agents with merges, alerts and rules, export (JSON
      Lines downloads), audit and pipeline, kept current by the SSE live
      feed. Reads and acts through the spec's L8 traits (QueryApi,
      OperatorActions, LiveFeed) with a deterministic fixture
      implementation; what the spec lacks is two documented gap traits
      (ui/src/contract: bucket width and present, export formats).
    entry_points:
      - ui/src/main.rs
      - ui/src/app.rs
      - ui/src/backend/fixture/surface.rs
      - ui/src/contract/mod.rs
      - ui/src/pages/mod.rs
      - ui/src/pages/view.rs
      - ui/src/data/mod.rs
      - ui/src/data/live.rs
      - ui/src/pages/topology/mod.rs
      - ui/src/pages/explore/mod.rs
      - ui/src/pages/export/mod.rs
      - ui/elements/src/ct-topology.ts
      - ui/elements/src/ct-projection.ts
      - ui/elements/src/ct-timebrush.ts
      - ui/elements/src/ct-live.ts
    depends_on: [query_surface, read_models, export, type_spec]
    doc: docs/features/ui.md
  conversation_view:
    status: design
    description: >
      Operator page for one agent's conversation, turn by turn: inputs of
      any role in request order and outputs, with provenance marks (text
      other agents originated, output spans and who later read them,
      relayed text, sub-agent delegations), harness claims, origin (fork,
      compaction), compaction boundaries and WebSocket increments. Structure
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
      merged agents and superseded channels resolved at read time (and
      transmissions between ids of one merged agent counted nowhere); the
      channel-centred graph of channels with cross-agent traffic and graph
      nodes with their confirmation; promotion with supersession;
      stored projections and their columnar frame; verdicts and detection
      quality; watermarked aggregates and retention; typed query and action
      errors with one From impl per store error; operator actions; the
      append-only audit log of actions, config changes and exports; and the
      id-only SSE live feed fed by every store's Changed.
    entry_points:
      - spec/types/interfaces/l8_surface.rs
      - spec/types/interfaces/l8_surface/permissions.rs
      - spec/types/interfaces/l8_surface/actions.rs
      - spec/types/interfaces/l8_surface/errors.rs
      - spec/types/interfaces/l8_surface/query_errors.rs
      - spec/types/interfaces/l8_surface/audit.rs
      - spec/types/interfaces/l8_surface/live.rs
    depends_on: [type_spec]
    doc: docs/features/query_surface.md
  read_models:
    description: >
      The rows and pages the UI shows on the query surface: canonical agent
      rows with claims, last seen and windowed traffic, and a detail with
      aliases, children, merges and vetoes that follows merged ids; channel
      rows with their cross-agent traffic, listing (confirmed, unconfirmed,
      declaration, hidden) and activity or their supersession, the channel
      list filter with listings, a channel's cross-agent transmissions,
      and the promotion preview computed by the promotion's own plan; agent
      and channel names over one bounded IdBatch; transmission rows by id
      with a per-state shape; the evidence behind a transmission with
      bounded excerpts; the overview's counts, which agree with the channel
      and agent rows; and one alert by id.
    entry_points:
      - spec/types/aggregates/agents/mod.rs
      - spec/types/interfaces/l8_surface/channels.rs
      - spec/types/interfaces/l8_surface/channel_traffic.rs
      - spec/types/derived/flow/channel/confirmation.rs
      - spec/types/batch.rs
      - spec/types/interfaces/l8_surface/summary.rs
      - spec/types/interfaces/l8_surface/evidence.rs
      - spec/types/interfaces/l8_surface/excerpt.rs
      - spec/types/interfaces/l8_surface/overview.rs
    depends_on: [query_surface, type_spec]
    doc: docs/features/read_models.md
  export:
    description: >
      QueryApi::export: one dataset (transmissions, edge or access buckets,
      topics, a stored projection, verdicts) streamed in JSONL or Parquet
      between a header (request, resolved topic version, watermark,
      embedding model, gateway version, planned rows) and a trailer (rows
      sent, a format-independent digest over a canonical row encoding,
      Complete or the failure). Reads only data settled before the
      watermark, under resolution captured at the start, so a re-run
      reproduces it; transmission rows are the surface's transmission
      summaries and their quoted text the evidence page's; content needs
      Content; oversized exports are refused before streaming; every export
      is audited.
    entry_points:
      - spec/types/interfaces/l8_surface/export/mod.rs
      - spec/types/interfaces/l8_surface/export/stream.rs
    depends_on: [query_surface, read_models, type_spec]
    doc: docs/features/export.md
```
