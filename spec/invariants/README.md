# Invariants

One TOML file per invariant, named `INV-<N>-<id>.toml`. `N` is one more than
the highest number already assigned and is never reused; `id` is the
invariant's `id` field.

```toml
[invariant]
id = "flow.transmission.single-sender"
statement = "Every content match in a confirmed transmission has the same origin agent."
kind = "representation"
requires = ["type", "unit"]
rationale = """
Edges are keyed by sender. A transmission mixing two senders would credit
one sender with the other's text.
"""

[evidence]
type = "crosstalk_spec::derived::flow::transmission::Confirmed::new"
unit = ["crosstalk_spec::tests::flow::confirmed_requires_one_origin"]

[evidence-review]
type = { agent = "true", human = "false" }
unit = { agent = "true", human = "false" }
```

## Fields

### `[invariant]`

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | yes | `<layer>.<subject>.<rule>`, lowercase kebab-case segments. Unique. |
| `statement` | yes | One testable sentence. |
| `kind` | yes | What the invariant constrains. See below. |
| `property` | when `kind = "domain"`, and only then | Hughes' class of the property. See below. |
| `requires` | yes | Non-empty list of evidence kinds that must exist, no duplicates. |
| `rationale` | yes | Why it matters: what breaks without it, and any design choice it pins down. |

Layer prefixes, one per layer of the abstraction stack (`spec/types/interfaces/`):

| Prefix | Layer | Owns |
| --- | --- | --- |
| `ingress` | L0 | proxy hot path, provider adapters, response framing |
| `canonical` | L1 | normalization, canonical messages, content addressing, ids and shared value types |
| `transport` | L2 | event bus, deliveries, blob store |
| `reconstruct` | L3 | agent identity, conversation threading |
| `provenance` | L4 | spans, fingerprints, decoding, content matching |
| `flow` | L5 | resources, accesses, channels, correlation, transmissions, policy routing |
| `analysis` | L6 | embeddings, topics, search, alert rules and triage |
| `topology` | L7 | edge aggregation and topology graphs |
| `surface` | L8 | query API, operator actions, alert delivery |

An invariant belongs to the layer whose component must uphold it.

### `kind`

Exactly one value. Apply the tests in order and take the first that
matches, so every invariant has exactly one kind:

1. `confidentiality`: restricts which data a component, store or person
   may observe or retain.
2. `performance`: a quantitative bound on time, space or throughput.
3. `coordination`: relates two or more operations, events or components
   across time: ordering, delivery counts, idempotency under redelivery,
   concurrency.
4. `representation`: holds for every value of a type at every instant, and
   the type or its checked constructor makes a violating value impossible
   to build.
5. `domain`: what a single operation computes from its inputs: the
   functional correctness of the gateway's logic.

### `property` (Hughes' classes, `domain` only)

From Hughes, *How to Specify It!*. Pick the class that matches how the
statement is phrased. If a statement fits two classes, split it.

| Value | The statement says |
| --- | --- |
| `invariant` | Every output of an operation satisfies a validity predicate that its types alone do not guarantee. |
| `postcondition` | After one call, a predicate relating that call's inputs to its output holds. |
| `metamorphic` | Transforming an input in a stated way changes the output in a stated way (relates two calls). |
| `inductive` | The result on a composite input is determined by the results on its parts (base cases plus a composition law). |
| `model-based` | The operation agrees with a simpler reference model under an abstraction function. |

### `requires`: evidence kinds

Evidence is something that exercises or constrains the code itself. Design
models (such as the stateviz lifecycle definition) check the design, not
the code, so they are not evidence. Each kind is separated from the others
by a single question, so a given test is exactly one kind:

| Value | Evidence is | Distinguished by |
| --- | --- | --- |
| `type` | A type or checked constructor that makes a violation unrepresentable or rejected at construction. | Enforced by the compiler or a constructor, not by running a test. |
| `unit` | A test on fixed, hand-picked inputs. | Inputs are chosen by hand; no simulated time, scheduling or faults; no external processes. |
| `property` | A test on generated inputs, checked by an explicit oracle (proptest). | Inputs are generated and the oracle checks a stated property. |
| `fuzz` | Coverage-guided input generation whose only oracle is no panic, crash or hang. | The oracle is crash-freedom only. |
| `dst` | A deterministic simulation test: seeded control of scheduling, time, delivery order and injected faults. | Concurrency, time or faults are simulated and replayable. |
| `integration` | A test against a real external dependency (Postgres, NATS, an upstream provider). | A real external process is involved. |
| `bench` | A benchmark that asserts a threshold. | Measures time or space against a bound. |
| `lint` | A static rule over source code (clippy lint, deny rule, custom check). | Does not execute the code. |

### `[evidence]`

One key per entry in `requires`, and no others. Each value is a string or a
list of strings: a Rust path for `type`, `unit`, `property`, `fuzz`, `dst`,
`integration` and `bench` (for example
`crosstalk_spec::tests::flow::confirmed_requires_one_origin`), and the rule's
name for `lint`. Paths name
where the evidence lives, or will live once the implementation exists.

#### Evidence paths

A path starts with the library name of the crate that holds the evidence:

- `crosstalk_spec::...` for evidence in the spec crate: a type or checked
  constructor (`crosstalk_spec::derived::...`), or a test under
  `spec/types/tests/` (`crosstalk_spec::tests::<module>::<fn>`);
- `crosstalk_<crate>::...` for evidence in an implementation crate, where
  `<crate>` is a directory under `crates/` (package `crosstalk-<crate>`,
  library `crosstalk_<crate>`). For the layer prefixes above that is the
  layer's own crate: `crosstalk_ingress`, `crosstalk_canonical`,
  `crosstalk_transport`, `crosstalk_reconstruct`, `crosstalk_provenance`,
  `crosstalk_flow`, `crosstalk_analysis`, `crosstalk_topology` and
  `crosstalk_surface`, for example `crosstalk_surface::tests::foo`.

The old single-crate form `crosstalk::<layer>::...` is not used.

`scripts/inv_check.py` checks every non-`lint` path against this
convention (the crate must exist under `crates/`), and checks that every
`crosstalk_spec::tests::<module>::<fn>` path whose review is
`agent = "true"` names a `fn <fn>` in that module's file
(`spec/types/tests/<module>.rs` or `<module>/mod.rs`).

### `[evidence-review]`

One key per entry in `requires`, and no others. Each value is
`{ agent = "true" | "false", human = "true" | "false" }`:

- `agent = "true"`: the evidence exists and an agent has read it and
  confirmed that it demonstrates the statement.
- `human = "true"`: a person has done the same.

Evidence that does not exist yet is `{ agent = "false", human = "false" }`.

## Validation

```sh
python3 scripts/inv_check.py spec/invariants                  # what scripts/check.sh runs
python3 scripts/inv_check.py spec/invariants --allow-pending  # also accept INV-X-<id>.toml
python3 scripts/inv_check.py spec/invariants --layer flow     # one layer's prefix only
```

It checks every file against this page (names, fields, kinds, evidence keys
and reviews, unique ids and numbers) and the evidence path rules above. Its
last line is `<files> files, <errors> errors`, and it exits non-zero on any
error.
