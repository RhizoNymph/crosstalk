//! The workspace dependency rule, checked against `cargo metadata`.
//!
//! Layer crates (`ingress`, `canonical`, `transport`, `reconstruct`,
//! `provenance`, `flow`, `analysis`, `topology`, `surface`) talk to each
//! other only through the spec, so:
//!
//! 1. a layer crate never depends on another layer crate, nor on `api`,
//!    `client` or `gateway`, under any dependency kind, with one exception:
//!    it may use `transport` as a dev-dependency (an in-process bus for its
//!    tests);
//! 2. `memory`, `sim` and `testkit` are only ever dev-dependencies of a
//!    layer crate, never normal or build dependencies.
//!
//! `store` and `spec` are open to every crate. Only `gateway`, `api`,
//! `client` and `eval` (the evaluation harness, `crates/eval`) compose
//! layer crates.
//!
//! The rule is a pure function over a typed dependency graph, tested on
//! hand-built graphs, and then applied to the real workspace.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;
use std::process::Command;

/// The nine layer crates, one per layer of the abstraction stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Layer {
    Ingress,
    Canonical,
    Transport,
    Reconstruct,
    Provenance,
    Flow,
    Analysis,
    Topology,
    Surface,
}

impl Layer {
    const ALL: [Layer; 9] = [
        Layer::Ingress,
        Layer::Canonical,
        Layer::Transport,
        Layer::Reconstruct,
        Layer::Provenance,
        Layer::Flow,
        Layer::Analysis,
        Layer::Topology,
        Layer::Surface,
    ];

    fn dir(self) -> &'static str {
        match self {
            Layer::Ingress => "ingress",
            Layer::Canonical => "canonical",
            Layer::Transport => "transport",
            Layer::Reconstruct => "reconstruct",
            Layer::Provenance => "provenance",
            Layer::Flow => "flow",
            Layer::Analysis => "analysis",
            Layer::Topology => "topology",
            Layer::Surface => "surface",
        }
    }
}

/// The crates allowed to wire layer crates together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Composer {
    Api,
    Client,
    Eval,
    Gateway,
}

impl Composer {
    const ALL: [Composer; 4] = [
        Composer::Api,
        Composer::Client,
        Composer::Eval,
        Composer::Gateway,
    ];

    fn dir(self) -> &'static str {
        match self {
            Composer::Api => "api",
            Composer::Client => "client",
            Composer::Eval => "eval",
            Composer::Gateway => "gateway",
        }
    }
}

/// Test-only support crates: dev-dependencies of layer crates, never more.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TestSupport {
    Memory,
    Sim,
    Testkit,
}

impl TestSupport {
    const ALL: [TestSupport; 3] = [TestSupport::Memory, TestSupport::Sim, TestSupport::Testkit];

    fn dir(self) -> &'static str {
        match self {
            TestSupport::Memory => "memory",
            TestSupport::Sim => "sim",
            TestSupport::Testkit => "testkit",
        }
    }
}

/// What a crate is, as far as the dependency rule cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    Layer(Layer),
    Composer(Composer),
    TestSupport(TestSupport),
    /// `crosstalk-spec`, `crosstalk-store`, or a crate outside the workspace:
    /// anyone may depend on it.
    Open,
}

impl Role {
    /// Classifies a package by name. Workspace crates are `crosstalk-<dir>`.
    fn of(package: &str) -> Role {
        let Some(dir) = package.strip_prefix("crosstalk-") else {
            return Role::Open;
        };
        if let Some(layer) = Layer::ALL.into_iter().find(|l| l.dir() == dir) {
            return Role::Layer(layer);
        }
        if let Some(c) = Composer::ALL.into_iter().find(|c| c.dir() == dir) {
            return Role::Composer(c);
        }
        if let Some(t) = TestSupport::ALL.into_iter().find(|t| t.dir() == dir) {
            return Role::TestSupport(t);
        }
        Role::Open
    }
}

/// The kind of a declared dependency (`cargo metadata`'s `kind` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DepKind {
    Normal,
    Dev,
    Build,
}

/// One declared dependency edge between two packages.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Edge {
    from: String,
    to: String,
    kind: DepKind,
}

/// Why an edge breaks the rule.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Violation {
    /// A layer crate depends on another layer crate.
    LayerOnLayer { edge: Edge },
    /// A layer crate depends on `api`, `client` or `gateway`.
    LayerOnComposer { edge: Edge },
    /// `memory`, `sim` or `testkit` is a non-dev dependency of a layer crate.
    TestSupportNotDev { edge: Edge },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (what, e) = match self {
            Violation::LayerOnLayer { edge } => {
                ("layer crate depends on another layer crate", edge)
            }
            Violation::LayerOnComposer { edge } => {
                ("layer crate depends on a composition crate", edge)
            }
            Violation::TestSupportNotDev { edge } => (
                "test-support crate is a non-dev dependency of a layer crate",
                edge,
            ),
        };
        write!(f, "{what}: {} -> {} ({:?})", e.from, e.to, e.kind)
    }
}

/// Checks one edge against the rule.
fn check(edge: &Edge) -> Option<Violation> {
    let Role::Layer(_) = Role::of(&edge.from) else {
        return None;
    };
    match Role::of(&edge.to) {
        Role::Layer(Layer::Transport) if edge.kind == DepKind::Dev => None,
        Role::Layer(_) => Some(Violation::LayerOnLayer { edge: edge.clone() }),
        Role::Composer(_) => Some(Violation::LayerOnComposer { edge: edge.clone() }),
        Role::TestSupport(_) if edge.kind != DepKind::Dev => {
            Some(Violation::TestSupportNotDev { edge: edge.clone() })
        }
        Role::TestSupport(_) | Role::Open => None,
    }
}

/// Every violation in a graph, in a stable order.
fn violations(edges: &[Edge]) -> BTreeSet<Violation> {
    edges.iter().filter_map(check).collect()
}

/// Failures reading the workspace's dependency graph.
#[derive(Debug)]
enum MetadataError {
    Spawn(std::io::Error),
    Exit {
        status: std::process::ExitStatus,
        stderr: String,
    },
    Json(serde_json::Error),
    Shape(&'static str),
    UnknownKind(String),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataError::Spawn(e) => write!(f, "could not run cargo metadata: {e}"),
            MetadataError::Exit { status, stderr } => {
                write!(f, "cargo metadata exited with {status}: {stderr}")
            }
            MetadataError::Json(e) => write!(f, "cargo metadata output is not JSON: {e}"),
            MetadataError::Shape(what) => write!(f, "unexpected cargo metadata shape: {what}"),
            MetadataError::UnknownKind(k) => write!(f, "unknown dependency kind {k:?}"),
        }
    }
}

impl std::error::Error for MetadataError {}

/// The workspace members and every dependency they declare.
struct Workspace {
    members: BTreeSet<String>,
    edges: Vec<Edge>,
}

fn workspace_manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml")
}

fn parse_metadata(json: &serde_json::Value) -> Result<Workspace, MetadataError> {
    let packages = json
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or(MetadataError::Shape("no packages array"))?;
    let mut members = BTreeSet::new();
    let mut edges = Vec::new();
    for package in packages {
        let from = package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or(MetadataError::Shape("package without a name"))?;
        members.insert(from.to_owned());
        let deps = package
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .ok_or(MetadataError::Shape("package without dependencies"))?;
        for dep in deps {
            let to = dep
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or(MetadataError::Shape("dependency without a name"))?;
            let kind = match dep.get("kind") {
                None | Some(serde_json::Value::Null) => DepKind::Normal,
                Some(serde_json::Value::String(k)) if k == "dev" => DepKind::Dev,
                Some(serde_json::Value::String(k)) if k == "build" => DepKind::Build,
                Some(other) => return Err(MetadataError::UnknownKind(other.to_string())),
            };
            edges.push(Edge {
                from: from.to_owned(),
                to: to.to_owned(),
                kind,
            });
        }
    }
    Ok(Workspace { members, edges })
}

/// Reads the declared dependencies of every workspace member. `--no-deps`
/// keeps it to the members (and offline); declared dependencies include
/// optional and target-specific ones, which is stricter than the resolve.
fn load_workspace() -> Result<Workspace, MetadataError> {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
            "--manifest-path",
        ])
        .arg(workspace_manifest())
        .output()
        .map_err(MetadataError::Spawn)?;
    if !output.status.success() {
        return Err(MetadataError::Exit {
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    let json = serde_json::from_slice(&output.stdout).map_err(MetadataError::Json)?;
    parse_metadata(&json)
}

fn edge(from: &str, to: &str, kind: DepKind) -> Edge {
    Edge {
        from: format!("crosstalk-{from}"),
        to: format!("crosstalk-{to}"),
        kind,
    }
}

#[test]
fn workspace_obeys_the_dependency_rule() -> Result<(), MetadataError> {
    let ws = load_workspace()?;
    let found = violations(&ws.edges);
    let report: Vec<String> = found.iter().map(ToString::to_string).collect();
    assert!(
        found.is_empty(),
        "dependency rule broken:\n{}",
        report.join("\n")
    );
    Ok(())
}

#[test]
fn workspace_has_every_crate_the_rule_names() -> Result<(), MetadataError> {
    let ws = load_workspace()?;
    let expected = Layer::ALL
        .into_iter()
        .map(Layer::dir)
        .chain(Composer::ALL.into_iter().map(Composer::dir))
        .chain(TestSupport::ALL.into_iter().map(TestSupport::dir))
        .chain(["store", "spec"]);
    for dir in expected {
        let name = format!("crosstalk-{dir}");
        assert!(
            ws.members.contains(&name),
            "{name} is not a workspace member"
        );
    }
    Ok(())
}

#[test]
fn every_crate_depends_on_the_spec() -> Result<(), MetadataError> {
    let ws = load_workspace()?;
    for member in ws.members.iter().filter(|m| *m != "crosstalk-spec") {
        let on_spec = ws
            .edges
            .iter()
            .any(|e| &e.from == member && e.to == "crosstalk-spec" && e.kind == DepKind::Normal);
        assert!(on_spec, "{member} does not depend on crosstalk-spec");
    }
    Ok(())
}

#[test]
fn metadata_kinds_parse() -> Result<(), MetadataError> {
    let json = serde_json::json!({ "packages": [{
        "name": "crosstalk-flow",
        "dependencies": [
            { "name": "crosstalk-spec", "kind": null },
            { "name": "crosstalk-memory", "kind": "dev" },
            { "name": "crosstalk-sim", "kind": "build" }
        ]
    }]});
    let ws = parse_metadata(&json)?;
    assert_eq!(
        ws.edges,
        vec![
            edge("flow", "spec", DepKind::Normal),
            edge("flow", "memory", DepKind::Dev),
            edge("flow", "sim", DepKind::Build),
        ]
    );
    Ok(())
}

#[test]
fn metadata_unknown_kind_is_an_error() {
    let json = serde_json::json!({ "packages": [{
        "name": "crosstalk-flow",
        "dependencies": [{ "name": "crosstalk-spec", "kind": "weird" }]
    }]});
    assert!(matches!(
        parse_metadata(&json),
        Err(MetadataError::UnknownKind(_))
    ));
}

#[test]
fn layer_on_another_layer_is_refused_for_every_kind() {
    for from in Layer::ALL {
        for to in Layer::ALL.into_iter().filter(|l| *l != from) {
            for kind in [DepKind::Normal, DepKind::Dev, DepKind::Build] {
                let e = edge(from.dir(), to.dir(), kind);
                let allowed = to == Layer::Transport && kind == DepKind::Dev;
                let got = check(&e);
                if allowed {
                    assert_eq!(got, None, "{e:?} should be allowed");
                } else {
                    assert_eq!(
                        got,
                        Some(Violation::LayerOnLayer { edge: e.clone() }),
                        "{e:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn transport_is_only_a_dev_dependency_of_layers() {
    assert_eq!(check(&edge("flow", "transport", DepKind::Dev)), None);
    for kind in [DepKind::Normal, DepKind::Build] {
        let e = edge("flow", "transport", kind);
        assert_eq!(check(&e), Some(Violation::LayerOnLayer { edge: e.clone() }));
    }
}

#[test]
fn layer_on_composer_is_refused_for_every_kind() {
    for from in Layer::ALL {
        for to in Composer::ALL {
            for kind in [DepKind::Normal, DepKind::Dev, DepKind::Build] {
                let e = edge(from.dir(), to.dir(), kind);
                assert_eq!(
                    check(&e),
                    Some(Violation::LayerOnComposer { edge: e.clone() })
                );
            }
        }
    }
}

#[test]
fn test_support_is_only_a_dev_dependency_of_layers() {
    for from in Layer::ALL {
        for to in TestSupport::ALL {
            assert_eq!(check(&edge(from.dir(), to.dir(), DepKind::Dev)), None);
            for kind in [DepKind::Normal, DepKind::Build] {
                let e = edge(from.dir(), to.dir(), kind);
                assert_eq!(
                    check(&e),
                    Some(Violation::TestSupportNotDev { edge: e.clone() })
                );
            }
        }
    }
}

#[test]
fn layers_may_use_spec_store_and_third_party_crates() {
    for from in Layer::ALL {
        for kind in [DepKind::Normal, DepKind::Dev, DepKind::Build] {
            assert_eq!(check(&edge(from.dir(), "spec", kind)), None);
            assert_eq!(check(&edge(from.dir(), "store", kind)), None);
            let third = Edge {
                from: format!("crosstalk-{}", from.dir()),
                to: "serde".to_owned(),
                kind,
            };
            assert_eq!(check(&third), None);
        }
    }
}

#[test]
fn composers_and_support_crates_are_unrestricted() {
    let others = Composer::ALL
        .into_iter()
        .map(Composer::dir)
        .chain(TestSupport::ALL.into_iter().map(TestSupport::dir))
        .chain(["store", "spec"]);
    for from in others {
        for to in Layer::ALL {
            for kind in [DepKind::Normal, DepKind::Dev, DepKind::Build] {
                assert_eq!(
                    check(&edge(from, to.dir(), kind)),
                    None,
                    "{from} -> {}",
                    to.dir()
                );
            }
        }
    }
}

#[test]
fn violations_collects_each_broken_edge() {
    let edges = vec![
        edge("flow", "spec", DepKind::Normal),
        edge("flow", "provenance", DepKind::Normal),
        edge("surface", "gateway", DepKind::Dev),
        edge("analysis", "memory", DepKind::Normal),
        edge("analysis", "memory", DepKind::Dev),
        edge("gateway", "flow", DepKind::Normal),
    ];
    let found = violations(&edges);
    let expected: BTreeSet<Violation> = [
        Violation::LayerOnLayer {
            edge: edge("flow", "provenance", DepKind::Normal),
        },
        Violation::LayerOnComposer {
            edge: edge("surface", "gateway", DepKind::Dev),
        },
        Violation::TestSupportNotDev {
            edge: edge("analysis", "memory", DepKind::Normal),
        },
    ]
    .into_iter()
    .collect();
    assert_eq!(found, expected);
}

#[test]
fn roles_classify_by_package_name() {
    assert_eq!(Role::of("crosstalk-flow"), Role::Layer(Layer::Flow));
    assert_eq!(
        Role::of("crosstalk-gateway"),
        Role::Composer(Composer::Gateway)
    );
    assert_eq!(Role::of("crosstalk-eval"), Role::Composer(Composer::Eval));
    assert_eq!(
        Role::of("crosstalk-testkit"),
        Role::TestSupport(TestSupport::Testkit)
    );
    assert_eq!(Role::of("crosstalk-store"), Role::Open);
    assert_eq!(Role::of("crosstalk-spec"), Role::Open);
    assert_eq!(Role::of("flow"), Role::Open);
}

#[test]
fn eval_composes_gateway_and_layers() {
    for kind in [DepKind::Normal, DepKind::Dev, DepKind::Build] {
        assert_eq!(check(&edge("eval", "gateway", kind)), None);
        for layer in Layer::ALL {
            assert_eq!(check(&edge("eval", layer.dir(), kind)), None);
            assert_eq!(
                check(&edge(layer.dir(), "eval", kind)),
                Some(Violation::LayerOnComposer {
                    edge: edge(layer.dir(), "eval", kind)
                })
            );
        }
    }
}
