import json

import numpy as np
from fastapi.testclient import TestClient
from umap import UMAP

from crosstalk_topics import layout, matrix
from crosstalk_topics.wire import LayoutParams

from .conftest import request_bytes


def test_too_few_points(client: TestClient) -> None:
    body = json.loads(request_bytes("layout_fit"))
    body["params"]["neighbors"] = 40
    response = client.post("/v1/layout/fit", content=json.dumps(body))
    assert response.status_code == 422
    assert response.content == b'{"type":"too_few_points","data":{"needed":41,"got":40}}'


def test_too_few_points_on_transform_base(client: TestClient) -> None:
    body = json.loads(request_bytes("layout_transform"))
    body["params"]["neighbors"] = 45
    response = client.post("/v1/layout/transform", content=json.dumps(body))
    assert response.status_code == 422
    assert json.loads(response.content)["type"] == "too_few_points"


def test_fit_returns_one_finite_pair_per_row(client: TestClient) -> None:
    response = client.post("/v1/layout/fit", content=request_bytes("layout_fit"))
    assert response.status_code == 200
    coordinates = json.loads(response.content)["coordinates"]
    assert (coordinates["rows"], coordinates["columns"]) == (40, 2)
    decoded = np.frombuffer(bytes.fromhex(coordinates["data"]), dtype="<f4")
    assert decoded.shape == (80,)
    assert np.isfinite(decoded).all()


def test_transform_returns_one_pair_per_point(client: TestClient) -> None:
    response = client.post("/v1/layout/transform", content=request_bytes("layout_transform"))
    assert response.status_code == 200
    coordinates = json.loads(response.content)["coordinates"]
    assert (coordinates["rows"], coordinates["columns"]) == (3, 2)


def test_transform_of_no_points(client: TestClient) -> None:
    body = json.loads(request_bytes("layout_transform"))
    body["points"] = {"rows": 0, "columns": 8, "data": ""}
    response = client.post("/v1/layout/transform", content=json.dumps(body))
    assert response.status_code == 200
    assert response.content == b'{"coordinates":{"rows":0,"columns":2,"data":""}}'


def test_cache_evicts_the_least_recently_used() -> None:
    cache = layout.LayoutCache(2)
    first, second, third = UMAP(), UMAP(), UMAP()
    cache.put(b"a", first)
    cache.put(b"b", second)
    assert cache.get(b"a") is first
    cache.put(b"c", third)
    assert cache.get(b"b") is None
    assert cache.get(b"a") is first
    assert len(cache) == 2


def test_cache_of_zero_keeps_nothing() -> None:
    cache = layout.LayoutCache(0)
    cache.put(b"a", UMAP())
    assert len(cache) == 0


def test_cache_key_depends_on_every_param_and_the_data() -> None:
    base = matrix.encode(np.eye(3, dtype=np.float32))
    params = LayoutParams(limit=10, neighbors=2, min_dist_milli=100, seed=1)
    keys = {
        layout.cache_key(base, params),
        layout.cache_key(base, params.model_copy(update={"seed": 2})),
        layout.cache_key(base, params.model_copy(update={"neighbors": 3})),
        layout.cache_key(base, params.model_copy(update={"min_dist_milli": 0})),
        layout.cache_key(base, params.model_copy(update={"limit": 11})),
        layout.cache_key(matrix.encode(np.eye(3, dtype=np.float32) * 0.5), params),
    }
    assert len(keys) == 6
