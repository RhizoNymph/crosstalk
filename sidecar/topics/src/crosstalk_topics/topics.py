"""`POST /v1/topics/fit`: UMAP reduction, HDBSCAN clusters, c-TF-IDF labels
(docs/features/topics_sidecar.md)."""

import numpy as np
import numpy.typing as npt
from sklearn.cluster import HDBSCAN
from umap import UMAP

from crosstalk_topics import seeding
from crosstalk_topics.ctfidf import class_terms
from crosstalk_topics.errors import InvalidRequest, TooFewSamples
from crosstalk_topics.matrix import decode
from crosstalk_topics.wire import TopicFitParams, TopicFitRequest, TopicFitResponse, TopicWire

OUTLIER = -1
LABEL_TERMS = 3


def needed(params: TopicFitParams) -> int:
    """How many documents a fit with `params` needs."""
    return max(params.min_cluster_size, params.umap_neighbors + 1, params.umap_components + 2)


def fit(request: TopicFitRequest) -> TopicFitResponse:
    embeddings = decode(request.embeddings, "embeddings")
    rows = embeddings.shape[0]
    if len(request.texts) != rows:
        raise InvalidRequest(
            f"texts: expected {rows} texts, one per embedding row, got {len(request.texts)}"
        )
    params = request.params
    if rows < needed(params):
        raise TooFewSamples(needed(params), rows)
    reduced = _reduce(embeddings, params)
    labels = renumber(_cluster(reduced, params))
    clusters = int(labels.max(initial=OUTLIER)) + 1
    documents = [
        " ".join(text for text, label in zip(request.texts, labels, strict=True) if label == k)
        for k in range(clusters)
    ]
    topics = [
        TopicWire(label=label_of(k, terms), terms=terms)
        for k, terms in enumerate(class_terms(documents, params.top_terms))
    ]
    return TopicFitResponse(labels=[int(label) for label in labels], topics=topics)


def _reduce(embeddings: npt.NDArray[np.float32], params: TopicFitParams) -> npt.NDArray[np.float32]:
    reducer = UMAP(
        n_neighbors=params.umap_neighbors,
        n_components=params.umap_components,
        min_dist=0.0,
        metric="cosine",
        random_state=seeding.random_state(params.seed),
        transform_seed=seeding.transform_seed(params.seed),
        n_jobs=1,
    )
    return np.asarray(reducer.fit_transform(embeddings), dtype=np.float32)


def _cluster(reduced: npt.NDArray[np.float32], params: TopicFitParams) -> npt.NDArray[np.int64]:
    clusterer = HDBSCAN(
        min_cluster_size=params.min_cluster_size,
        min_samples=params.min_samples,
        metric="euclidean",
        cluster_selection_method="eom",
        copy=True,
    )
    return np.asarray(clusterer.fit_predict(reduced.astype(np.float64)), dtype=np.int64)


def renumber(raw: npt.NDArray[np.int64]) -> npt.NDArray[np.int64]:
    """Clusters numbered `0..k` by size, largest first, ties to the cluster
    whose first member comes first; every negative label is an outlier."""
    sizes: dict[int, int] = {}
    first: dict[int, int] = {}
    for index, label in enumerate(raw.tolist()):
        if label < 0:
            continue
        sizes[label] = sizes.get(label, 0) + 1
        first.setdefault(label, index)
    order = sorted(sizes, key=lambda label: (-sizes[label], first[label]))
    mapping = {label: k for k, label in enumerate(order)}
    return np.array([mapping.get(label, OUTLIER) for label in raw.tolist()], dtype=np.int64)


def label_of(cluster: int, terms: list[tuple[str, float]]) -> str:
    if not terms:
        return f"topic {cluster}"
    return ", ".join(term for term, _ in terms[:LABEL_TERMS])
