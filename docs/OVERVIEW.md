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

    Status: design. The data model is specified in spec/types; there is no
    implementation yet. The spec types are also the JSON wire format
    between the gateway, the operator UI and other gateway nodes.

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
      channel registry with promotion and supersession, write/read
      correlation into transmissions, and the operator verdict log kept
      beside each transmission).
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
      and trailer manifest; alert sinks).

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
    calls into accesses on canonical channels (AccessRecorded), resolves
    channels and correlates cross-agent accesses and content matches into
    transmissions (TransmissionConfirmed / Suspected) → L6 embeds and
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
    channel rows (writers and readers from L5's resource use, transmissions
    from the same graph the overview counts, over the channel and every
    channel it superseded, in an optional window that never changes which
    rows are listed); promotion previews (the registry's promotion plan run
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
      claims), derived inferences (including channel promotion with
      supersession and operator verdicts beside the detector's state),
      aggregates (including edge and access buckets, time series, topic
      history with retention, and the watermark that marks buckets final),
      bus events and per-layer interfaces (the types are also the JSON
      wire format: wire_contract), with tests for the invariants
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
      id-only SSE live feed fed by every store's Changed.
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
      rows with activity or their supersession, the channel list filter,
      and the promotion preview computed by the promotion's own plan; agent
      and channel names over one bounded IdBatch; transmission rows by id
      with a per-state shape; the evidence behind a transmission with
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
    depends_on: [query_surface, type_spec]
    doc: docs/features/read_models.md
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
      surface actions, surface reads), rewritten with CROSSTALK_BLESS=1.
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
```
