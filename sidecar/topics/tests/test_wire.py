"""Request validation: every refusal is a 400 `invalid_request`."""

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from .conftest import request_bytes


def mutated(name: str, change: Any) -> bytes:
    body = json.loads(request_bytes(name))
    change(body)
    return json.dumps(body).encode()


def reason(client: TestClient, route: str, body: bytes) -> str:
    response = client.post(route, content=body)
    assert response.status_code == 400, response.content
    decoded = json.loads(response.content)
    assert decoded["type"] == "invalid_request"
    assert set(decoded) == {"type", "data"}
    return decoded["data"]["reason"]


def set_in(path: list[str], value: Any) -> Any:
    def change(body: dict[str, Any]) -> None:
        target = body
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return change


def drop(path: list[str]) -> Any:
    def change(body: dict[str, Any]) -> None:
        target = body
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]

    return change


@pytest.mark.parametrize(
    ("path", "value", "fragment"),
    [
        (["params", "neighbors"], 1, "params.neighbors"),
        (["params", "neighbors"], 201, "params.neighbors"),
        (["params", "min_dist_milli"], 1001, "params.min_dist_milli"),
        (["params", "limit"], 0, "params.limit"),
        (["params", "limit"], 100_001, "params.limit"),
        (["params", "seed"], -1, "params.seed"),
        (["params", "seed"], 1 << 64, "params.seed"),
        (["params", "seed"], 1.5, "params.seed"),
        (["params", "seed"], "42", "params.seed"),
        (["params", "extra"], 1, "params.extra"),
        (["extra"], 1, "extra"),
        (["embeddings", "columns"], 0, "embeddings.columns"),
        (["embeddings", "rows"], 41, "embeddings.data"),
        (["params", "limit"], 39, "embeddings.rows"),
    ],
)
def test_layout_fit_refusals(
    client: TestClient, path: list[str], value: Any, fragment: str
) -> None:
    body = mutated("layout_fit", set_in(path, value))
    assert fragment in reason(client, "/v1/layout/fit", body)


def test_missing_field_is_refused(client: TestClient) -> None:
    body = mutated("layout_fit", drop(["params", "seed"]))
    assert "params.seed" in reason(client, "/v1/layout/fit", body)


def test_maximal_seed_is_accepted(client: TestClient) -> None:
    body = mutated("layout_fit", set_in(["params", "seed"], (1 << 64) - 1))
    assert client.post("/v1/layout/fit", content=body).status_code == 200


@pytest.mark.parametrize(
    ("path", "value", "fragment"),
    [
        (["params", "min_cluster_size"], 1, "params.min_cluster_size"),
        (["params", "min_samples"], 0, "params.min_samples"),
        (["params", "umap_neighbors"], 201, "params.umap_neighbors"),
        (["params", "umap_components"], 0, "params.umap_components"),
        (["params", "umap_components"], 101, "params.umap_components"),
        (["params", "top_terms"], 0, "params.top_terms"),
        (["params", "top_terms"], 51, "params.top_terms"),
        (["texts"], ["only one"], "texts: expected 35 texts"),
    ],
)
def test_topics_fit_refusals(
    client: TestClient, path: list[str], value: Any, fragment: str
) -> None:
    body = mutated("topics_fit", set_in(path, value))
    assert fragment in reason(client, "/v1/topics/fit", body)


def test_min_samples_must_be_present(client: TestClient) -> None:
    body = mutated("topics_fit", drop(["params", "min_samples"]))
    assert "params.min_samples" in reason(client, "/v1/topics/fit", body)


def test_transform_columns_must_match(client: TestClient) -> None:
    def change(body: dict[str, Any]) -> None:
        body["points"] = {"rows": 1, "columns": 4, "data": "0000803f" + "00000000" * 3}

    body = mutated("layout_transform", change)
    assert "points.columns" in reason(client, "/v1/layout/transform", body)
