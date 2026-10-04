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
    topology view with edge weights, search, topic modeling, clustering, a
    UMAP view of message content, and alerts.

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
      Message), L3 reconstruction (agent identity, merges, conversation
      threading, WebSocket increment resolution).
    transport: >
      L2: the event bus (in-process channels on one node, NATS JetStream
      across nodes) and the content-addressed blob store. The only path
      between components.
    detect: >
      L4 provenance (span extraction, novelty classification, fingerprint
      index, content matching) and L5 flow detection (resource extraction,
      channel registry, write/read correlation into transmissions).
    insight: >
      L6 analysis (embeddings, topics, search, alert rules), L7 topology
      (edge aggregation per time window), L8 surface (query API with
      cursor-paginated lists and linked views sharing one filter, UI,
      operator actions, alert sinks).

  data_flow: >
    Harness request (via its base URL, or via the gateway as HTTPS proxy) →
    L0 routes it to its upstream, hashes the credential, forwards it unchanged
    without waiting for its body to decode, decodes the body concurrently
    off the hot path, and tees the response (or each WebSocket turn) →
    RawExchange (in-process) → L1 normalizes, writes message bodies to the
    blob store, publishes ExchangeCaptured → L3 resolves the agent and
    threads the conversation, publishes ConversationDelta → L4 indexes the
    agent's originated spans and matches new inputs against other agents'
    spans (ContentMatched); L5 turns tool calls into accesses, resolves
    channels and correlates cross-agent accesses and content matches into
    transmissions (TransmissionConfirmed / Suspected) → L6 embeds and
    classifies transmissions and evaluates alert rules → L7 aggregates
    edges → L8 serves topology, search, projections, lists and alerts, with
    the graph, search, projection and edge drill-down all filtered by one
    TopologyFilter. Operator actions flow
    back down: policy changes to L5, agent merges to L3.

Features Index:
  type_spec:
    description: >
      The gateway's data model as type-checked Rust: observed facts
      (including clients, upstreams and credentials), derived inferences,
      aggregates, bus events and per-layer interfaces (including the query
      surface's paginated lists, shared view filter and projection), with
      tests for the invariants checked at runtime. Harness and server wire behavior it is
      based on is in docs/research/harness-wire-protocols.md.
    entry_points: [spec/types/mod.rs, spec/Cargo.toml]
    depends_on: []
    doc: docs/features/type_spec.md
```
