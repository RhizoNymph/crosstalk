"""The same request bytes give the same response bytes
(docs/features/topics_sidecar.md, "Determinism")."""

import json

import pytest
from fastapi.testclient import TestClient

from .conftest import NAMES, ROUTES, make_client, request_bytes


@pytest.mark.parametrize("name", NAMES)
def test_same_request_same_bytes(client: TestClient, name: str) -> None:
    first = client.post(ROUTES[name], content=request_bytes(name))
    second = client.post(ROUTES[name], content=request_bytes(name))
    assert first.status_code == 200
    assert first.content == second.content


@pytest.mark.parametrize("name", NAMES)
def test_a_fresh_service_gives_the_same_bytes(client: TestClient, name: str) -> None:
    warm = client.post(ROUTES[name], content=request_bytes(name))
    with make_client(layout_cache=0) as fresh:
        cold = fresh.post(ROUTES[name], content=request_bytes(name))
    assert warm.content == cold.content


def test_transform_after_fit_matches_a_cold_transform() -> None:
    with make_client() as warm:
        assert (
            warm.post(ROUTES["layout_fit"], content=request_bytes("layout_fit")).status_code == 200
        )
        hit_once = warm.post(ROUTES["layout_transform"], content=request_bytes("layout_transform"))
        hit_twice = warm.post(ROUTES["layout_transform"], content=request_bytes("layout_transform"))
    with make_client(layout_cache=0) as cold:
        miss = cold.post(ROUTES["layout_transform"], content=request_bytes("layout_transform"))
    assert hit_once.status_code == 200
    assert hit_once.content == hit_twice.content == miss.content


def with_seed(name: str, seed: int) -> bytes:
    body = json.loads(request_bytes(name))
    body["params"]["seed"] = seed
    return json.dumps(body).encode()


def test_the_high_32_bits_of_the_seed_count(client: TestClient) -> None:
    low = client.post(ROUTES["layout_fit"], content=with_seed("layout_fit", 42))
    high = client.post(ROUTES["layout_fit"], content=with_seed("layout_fit", 42 + (1 << 32)))
    assert low.status_code == high.status_code == 200
    assert low.content != high.content


def test_another_seed_gives_another_layout(client: TestClient) -> None:
    one = client.post(ROUTES["layout_fit"], content=with_seed("layout_fit", 1))
    two = client.post(ROUTES["layout_fit"], content=with_seed("layout_fit", 2))
    assert one.content != two.content


def test_request_formatting_does_not_matter(client: TestClient) -> None:
    compact = client.post(ROUTES["layout_fit"], content=request_bytes("layout_fit"))
    spaced = json.dumps(json.loads(request_bytes("layout_fit")), indent=2).encode()
    assert client.post(ROUTES["layout_fit"], content=spaced).content == compact.content
