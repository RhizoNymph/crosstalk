"""`POST /v1/layout/fit` and `/v1/layout/transform`: seeded UMAP to two
dimensions (docs/features/topics_sidecar.md).

Fitted bases are cached by a digest of the base and the params, and a
transform runs on a deep copy of the cached model, so the cache is never
changed by use: a hit and a miss give the same bytes. The cache is used
only from the single worker thread, so it needs no lock.
"""

import copy
import hashlib
from collections import OrderedDict

import numpy as np
import numpy.typing as npt
from umap import UMAP

from crosstalk_topics import seeding
from crosstalk_topics.errors import InvalidRequest, NonFiniteLayout, TooFewPoints
from crosstalk_topics.matrix import decode, encode
from crosstalk_topics.wire import (
    LayoutFitRequest,
    LayoutParams,
    LayoutResponse,
    LayoutTransformRequest,
    Matrix,
)


class LayoutCache:
    """At most `capacity` fitted models, least recently used evicted."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._models: OrderedDict[bytes, UMAP] = OrderedDict()

    def get(self, key: bytes) -> UMAP | None:
        model = self._models.get(key)
        if model is not None:
            self._models.move_to_end(key)
        return model

    def put(self, key: bytes, model: UMAP) -> None:
        if self._capacity == 0:
            return
        self._models[key] = model
        self._models.move_to_end(key)
        while len(self._models) > self._capacity:
            self._models.popitem(last=False)

    def __len__(self) -> int:
        return len(self._models)


def cache_key(base: Matrix, params: LayoutParams) -> bytes:
    digest = hashlib.sha256()
    digest.update(
        f"{base.rows}x{base.columns}|{params.limit}|{params.neighbors}|"
        f"{params.min_dist_milli}|{params.seed}|".encode()
    )
    digest.update(base.data.encode("ascii"))
    return digest.digest()


def fit(request: LayoutFitRequest, cache: LayoutCache) -> LayoutResponse:
    model = _fitted(request.embeddings, request.params, "embeddings", cache)
    return LayoutResponse(coordinates=encode(_finite(model.embedding_)))


def transform(request: LayoutTransformRequest, cache: LayoutCache) -> LayoutResponse:
    if request.points.columns != request.base.columns:
        raise InvalidRequest(
            f"points.columns: expected {request.base.columns} like base, "
            f"got {request.points.columns}"
        )
    points = decode(request.points, "points")
    model = _fitted(request.base, request.params, "base", cache)
    if points.shape[0] == 0:
        return LayoutResponse(coordinates=encode(np.zeros((0, 2), dtype=np.float32)))
    placed = copy.deepcopy(model).transform(points)
    return LayoutResponse(coordinates=encode(_finite(placed)))


def _fitted(base: Matrix, params: LayoutParams, field: str, cache: LayoutCache) -> UMAP:
    embeddings = decode(base, field)
    rows = embeddings.shape[0]
    if rows > params.limit:
        raise InvalidRequest(f"{field}.rows: {rows} is above params.limit {params.limit}")
    if rows < params.neighbors + 1:
        raise TooFewPoints(params.neighbors + 1, rows)
    key = cache_key(base, params)
    cached = cache.get(key)
    if cached is not None:
        return cached
    model = _umap(params).fit(embeddings)
    cache.put(key, model)
    return model


def _umap(params: LayoutParams) -> UMAP:
    return UMAP(
        n_neighbors=params.neighbors,
        n_components=2,
        min_dist=params.min_dist_milli / 1000,
        metric="cosine",
        random_state=seeding.random_state(params.seed),
        transform_seed=seeding.transform_seed(params.seed),
        n_jobs=1,
    )


def _finite(coordinates: npt.ArrayLike) -> npt.NDArray[np.float32]:
    array = np.asarray(coordinates, dtype=np.float32)
    if not bool(np.isfinite(array).all()):
        raise NonFiniteLayout()
    return array
