#!/usr/bin/env python3
"""Validate invariant TOML files against spec/invariants/README.md.

usage: inv_check.py DIR [--layer PREFIX] [--allow-pending] [--root REPO]
  --allow-pending  accept INV-X-<id>.toml (number not yet assigned)
  --root REPO      repository root holding crates/ and spec/types/tests
                   (default: two levels above DIR)

Beyond the schema, every evidence path (every kind except `lint`) must start
with `crosstalk_spec::` or `crosstalk_<crate>::` naming a directory under
crates/, and every evidence path marked `agent = "true"` under
`crosstalk_spec::tests::` must name a test function that exists: `fn <name>`
in the module file the path names (spec/types/tests/<mods>.rs or
<mods>/mod.rs, or the nearest enclosing module file for inline modules).
"""
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

LAYERS = {"ingress", "canonical", "transport", "reconstruct", "provenance",
          "flow", "analysis", "topology", "surface"}
KINDS = {"confidentiality", "performance", "coordination", "representation", "domain"}
PROPERTIES = {"invariant", "postcondition", "metamorphic", "inductive", "model-based"}
EVIDENCE = {"type", "unit", "property", "fuzz", "dst", "integration", "bench", "lint"}
ID_RE = re.compile(r"^[a-z]+(\.[a-z0-9]+(-[a-z0-9]+)*){2,}$")
NAME_RE = re.compile(r"^INV-(\d+|X)-(.+)\.toml$")
PATH_RE = re.compile(r"^crosstalk_([a-z][a-z0-9_]*)::[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*$")
SPEC_TESTS = "crosstalk_spec::tests::"


@dataclass(frozen=True)
class Repo:
    """What evidence paths are checked against."""

    crates: frozenset[str]
    tests_dir: Path

    @staticmethod
    def load(root: Path) -> "Repo":
        crates_dir = root / "crates"
        crates = frozenset(
            p.name for p in crates_dir.iterdir() if (p / "Cargo.toml").is_file()
        ) if crates_dir.is_dir() else frozenset()
        return Repo(crates=crates, tests_dir=root / "spec" / "types" / "tests")

    def path_error(self, path: str) -> str | None:
        """Why an implementation evidence path names no crate, if it does not."""
        m = PATH_RE.match(path)
        if not m:
            return f"evidence path {path!r} is not crosstalk_spec::... or crosstalk_<crate>::..."
        crate = m.group(1)
        # A library `crosstalk_<a>_<b>` is the package `crosstalk-<a>-<b>`
        # in crates/<a>-<b>.
        if crate != "spec" and crate not in self.crates and crate.replace("_", "-") not in self.crates:
            return f"evidence path {path!r} names crate crosstalk_{crate}, which is not in crates/"
        return None

    def spec_test_exists(self, path: str) -> bool:
        """Whether `crosstalk_spec::tests::<mods>::<name>` names `fn <name>` in its module file."""
        segs = path[len(SPEC_TESTS):].split("::")
        name, mods = segs[-1], segs[:-1]
        fn_re = re.compile(r"\bfn\s+" + re.escape(name) + r"\s*[(<]")
        # The deepest module file that exists; inline submodules live in it.
        for depth in range(len(mods), -1, -1):
            base = self.tests_dir.joinpath(*mods[:depth])
            cands = [base.with_suffix(".rs"), base / "mod.rs"] if depth else [self.tests_dir / "mod.rs"]
            existing = [c for c in cands if c.is_file()]
            if existing:
                return any(fn_re.search(c.read_text()) for c in existing)
        return False


@dataclass
class TestTally:
    """Reviewed `crosstalk_spec::tests::` paths: how many were checked, which failed."""

    checked: int = 0
    missing: list[str] = field(default_factory=list)


def check_paths(doc: dict, repo: Repo, err, tally: TestTally) -> None:
    ev = doc.get("evidence", {})
    rev = doc.get("evidence-review", {})
    for kind, v in ev.items():
        if kind == "lint":
            continue
        vals = v if isinstance(v, list) else [v]
        reviewed = isinstance(rev.get(kind), dict) and rev[kind].get("agent") == "true"
        for x in vals:
            if not isinstance(x, str):
                continue
            e = repo.path_error(x)
            if e:
                err(f"evidence.{kind}: {e}")
            elif reviewed and x.startswith(SPEC_TESTS):
                tally.checked += 1
                if not repo.spec_test_exists(x):
                    tally.missing.append(x)
                    err(f"evidence.{kind}: agent = \"true\" but no test fn for {x!r}")


def check(path: Path, layer: str | None, allow_pending: bool, errors: list[str],
          repo: Repo, tally: TestTally) -> str | None:
    def err(msg: str) -> None:
        errors.append(f"{path.name}: {msg}")

    m = NAME_RE.match(path.name)
    if not m:
        err("file name is not INV-<N>-<id>.toml")
        return None
    if m.group(1) == "X" and not allow_pending:
        err("number not assigned (INV-X)")
    try:
        doc = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        err(f"invalid TOML: {e}")
        return None

    allowed_tables = {"invariant", "evidence", "evidence-review"}
    for extra in set(doc) - allowed_tables:
        err(f"unknown table [{extra}]")
    inv = doc.get("invariant", {})
    allowed = {"id", "statement", "kind", "property", "requires", "rationale"}
    for extra in set(inv) - allowed:
        err(f"unknown key invariant.{extra}")

    iid = inv.get("id")
    if not isinstance(iid, str) or not ID_RE.match(iid):
        err(f"bad id {iid!r}: want <layer>.<subject>.<rule>, lowercase kebab segments")
    else:
        prefix = iid.split(".")[0]
        if prefix not in LAYERS:
            err(f"unknown layer prefix {prefix!r}")
        if layer and prefix != layer:
            err(f"layer prefix {prefix!r}, expected {layer!r}")
        if m.group(2) != iid:
            err(f"file name id {m.group(2)!r} does not match id {iid!r}")

    st = inv.get("statement")
    if not isinstance(st, str) or not st.strip().endswith("."):
        err("statement must be one sentence ending in '.'")
    if not isinstance(inv.get("rationale"), str) or not inv["rationale"].strip():
        err("rationale missing")

    kind = inv.get("kind")
    if kind not in KINDS:
        err(f"kind {kind!r} not in {sorted(KINDS)}")
    prop = inv.get("property")
    if kind == "domain":
        if prop not in PROPERTIES:
            err(f"domain invariant needs property in {sorted(PROPERTIES)}, got {prop!r}")
    elif prop is not None:
        err("property is only allowed when kind = 'domain'")

    req = inv.get("requires")
    if not isinstance(req, list) or not req:
        err("requires must be a non-empty list")
        req = []
    if len(set(req)) != len(req):
        err("requires has duplicates")
    for r in req:
        if r not in EVIDENCE:
            err(f"unknown evidence kind {r!r}")

    ev = doc.get("evidence", {})
    if set(ev) != set(req):
        err(f"[evidence] keys {sorted(ev)} != requires {sorted(req)}")
    for k, v in ev.items():
        vals = v if isinstance(v, list) else [v]
        if not vals or not all(isinstance(x, str) and x.strip() for x in vals):
            err(f"evidence.{k} must be a non-empty string or list of strings")

    rev = doc.get("evidence-review", {})
    if set(rev) != set(req):
        err(f"[evidence-review] keys {sorted(rev)} != requires {sorted(req)}")
    for k, v in rev.items():
        if not isinstance(v, dict) or set(v) != {"agent", "human"}:
            err(f"evidence-review.{k} must be {{ agent, human }}")
            continue
        for who in ("agent", "human"):
            if v[who] not in ("true", "false"):
                err(f"evidence-review.{k}.{who} must be \"true\" or \"false\"")
    check_paths(doc, repo, err, tally)
    return iid if isinstance(iid, str) else None


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    root = Path(args[0])
    layer = args[args.index("--layer") + 1] if "--layer" in args else None
    allow_pending = "--allow-pending" in args
    repo_root = Path(args[args.index("--root") + 1]) if "--root" in args else root.resolve().parents[1]
    repo = Repo.load(repo_root)
    if not repo.tests_dir.is_dir():
        print(f"no spec tests at {repo.tests_dir}; pass --root REPO")
        return 2
    errors: list[str] = []
    tally = TestTally()
    ids: dict[str, str] = {}
    numbers: dict[str, str] = {}
    files = sorted(root.glob("INV-*.toml"))
    for f in files:
        iid = check(f, layer, allow_pending, errors, repo, tally)
        if iid:
            if iid in ids:
                errors.append(f"{f.name}: duplicate id, also in {ids[iid]}")
            ids[iid] = f.name
        m = NAME_RE.match(f.name)
        if m and m.group(1) != "X":
            if m.group(1) in numbers:
                errors.append(f"{f.name}: duplicate number, also {numbers[m.group(1)]}")
            numbers[m.group(1)] = f.name
    for e in errors:
        print(e)
    print(f"{len(tally.missing)} of {tally.checked} reviewed spec test paths name no test fn")
    print(f"{len(files)} files, {len(errors)} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
