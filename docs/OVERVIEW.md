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
    implementation yet.

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
      surface (the query API with cursor-paginated lists, linked views
      sharing one filter and one resolved topic-model version, transmission
      rows by id with a per-state shape, the evidence behind a transmission
      with excerpts cut from stored bodies, one alert by id, the overview's
      counts in one watermarked query, typed query
      and action errors, the operator directory with a trusted single-user
      mode, operator actions with one permission each, the append-only
      audit log of operator actions and config changes, the id-only SSE
      live feed, alert sinks).

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
    topic history, search, projections, verdicts, detection quality, lists
    and alerts. Every aggregate comes back with the watermark read before
    it; every linked view applies one TopologyFilter under one resolved (or
    pinned) topic-model version, with merged agents and superseded
    channels resolved at read time; the evidence page cuts excerpts of
    both sides of each content match from the blob store's bodies through
    the spans' and matches' locations (a body content retention dropped is
    reported, not an error); projection fits run as background jobs
    whose stored frames read back exactly. Every store publishes an id-only
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
    unmerges of one merge record and renames to L3; rule management and
    topic-version pins to L6. Every action call is recorded in the audit
    log with its outcome, and so is every change a config load makes.

Features Index:
  type_spec:
    description: >
      The gateway's data model as type-checked Rust: observed facts
      (including clients, upstreams, credentials, the merge log and harness
      claims), derived inferences (including channel promotion with
      supersession and operator verdicts beside the detector's state),
      aggregates (including edge and access buckets, time series, topic
      history with retention, and the watermark that marks buckets final),
      bus events and per-layer interfaces, with tests for the invariants
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
      permission per query and action; paginated lists; transmission rows
      by id, transmission evidence with bounded excerpts, one alert by id
      and the overview's counts; linked views
      sharing one TopologyFilter and one resolved topic-model version, with
      merged agents and superseded channels resolved at read time; the
      channel-centred graph and graph nodes; stored projections and their
      columnar frame; verdicts and detection quality; watermarked
      aggregates and retention; typed query and action errors with one
      From impl per store error; operator actions; the append-only audit
      log of actions and config changes; and the id-only SSE live feed fed
      by every store's Changed.
    entry_points:
      - spec/types/interfaces/l8_surface.rs
      - spec/types/interfaces/l8_surface/actions.rs
      - spec/types/interfaces/l8_surface/live.rs
      - spec/types/interfaces/l8_surface/summary.rs
      - spec/types/interfaces/l8_surface/evidence.rs
      - spec/types/interfaces/l8_surface/excerpt.rs
      - spec/types/interfaces/l8_surface/overview.rs
    depends_on: [type_spec]
    doc: docs/features/query_surface.md
```
