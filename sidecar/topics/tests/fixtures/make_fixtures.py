"""Write the contract fixtures' request files, deterministically.

    uv run python tests/fixtures/make_fixtures.py

Each `<name>.request.json` is compact JSON in the contract's field order,
exactly as Rust's serde_json writes the same value, with no trailing
newline; the Rust adapter's tests read these files too. The matching
`<name>.response.json` goldens are written by `tests/test_golden.py` under
`CROSSTALK_BLESS=1`, from the bytes the service actually serves.
"""

from pathlib import Path

import numpy as np
import numpy.typing as npt

from crosstalk_topics import matrix
from crosstalk_topics.wire import (
    LayoutFitRequest,
    LayoutParams,
    LayoutTransformRequest,
    TopicFitParams,
    TopicFitRequest,
    Wire,
)

HERE = Path(__file__).parent
DIMENSION = 8

VOCABULARIES = [
    ["wiki", "page", "edit", "article", "revision", "draft"],
    ["deploy", "build", "release", "pipeline", "container", "rollout"],
    ["invoice", "payment", "billing", "refund", "customer", "ledger"],
]
SHARED = ["agent", "notes", "update"]


def unit_rows(rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float32]:
    unit = (rows / np.linalg.norm(rows, axis=1, keepdims=True)).astype(np.float32)
    norms = np.linalg.norm(unit.astype(np.float64), axis=1)
    assert np.all(np.abs(norms - 1.0) < 1e-4), norms
    return unit


def clustered(
    rng: np.random.Generator, clusters: int, per_cluster: int, spread: float
) -> tuple[npt.NDArray[np.float64], list[int]]:
    centers = rng.normal(size=(clusters, DIMENSION))
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    rows = []
    owners = []
    for k in range(clusters):
        for _ in range(per_cluster):
            rows.append(centers[k] + spread * rng.normal(size=DIMENSION))
            owners.append(k)
    return np.array(rows), owners


def text_of(rng: np.random.Generator, vocabulary: list[str]) -> str:
    words = list(rng.choice(vocabulary, size=int(rng.integers(4, 7))))
    if rng.random() < 0.5:
        words.append(str(rng.choice(SHARED)))
    return " ".join(str(word) for word in words)


def topics_fit() -> TopicFitRequest:
    rng = np.random.default_rng(20261004)
    rows, owners = clustered(rng, clusters=3, per_cluster=11, spread=0.08)
    texts = [text_of(rng, VOCABULARIES[owner]) for owner in owners]
    outliers = rng.normal(size=(2, DIMENSION))
    every_word = [word for vocabulary in VOCABULARIES for word in vocabulary]
    texts += [text_of(rng, every_word) for _ in range(2)]
    rows = np.vstack([rows, outliers])
    order = rng.permutation(len(texts))
    embeddings = unit_rows(rows[order])
    return TopicFitRequest(
        embeddings=matrix.encode(embeddings),
        texts=[texts[i] for i in order],
        params=TopicFitParams(
            seed=7,
            min_cluster_size=5,
            min_samples=None,
            umap_neighbors=8,
            umap_components=3,
            top_terms=5,
        ),
    )


LAYOUT_PARAMS = LayoutParams(limit=100, neighbors=5, min_dist_milli=100, seed=42)


def layout_base() -> npt.NDArray[np.float32]:
    rng = np.random.default_rng(42)
    rows, _ = clustered(rng, clusters=4, per_cluster=10, spread=0.15)
    return unit_rows(rows[rng.permutation(len(rows))])


def layout_fit() -> LayoutFitRequest:
    return LayoutFitRequest(embeddings=matrix.encode(layout_base()), params=LAYOUT_PARAMS)


def layout_transform() -> LayoutTransformRequest:
    base = layout_base()
    rng = np.random.default_rng(7)
    points = unit_rows(
        base[[0, 11, 23]].astype(np.float64) + 0.05 * rng.normal(size=(3, DIMENSION))
    )
    return LayoutTransformRequest(
        base=matrix.encode(base), params=LAYOUT_PARAMS, points=matrix.encode(points)
    )


def build() -> dict[str, Wire]:
    return {
        "topics_fit": topics_fit(),
        "layout_fit": layout_fit(),
        "layout_transform": layout_transform(),
    }


def main() -> None:
    for name, request in build().items():
        (HERE / f"{name}.request.json").write_bytes(request.model_dump_json().encode())


if __name__ == "__main__":
    main()
