"""Every error of the contract: status and adjacently tagged body."""

import json

import pytest
from fastapi.testclient import TestClient

from crosstalk_topics.errors import (
    Internal,
    InvalidRequest,
    MethodNotAllowed,
    NonFiniteLayout,
    NotFound,
    PayloadTooLarge,
    SidecarError,
    TooFewPoints,
    TooFewSamples,
)

from .conftest import make_client, request_bytes


@pytest.mark.parametrize(
    ("error", "status", "body"),
    [
        (InvalidRequest("bad"), 400, b'{"type":"invalid_request","data":{"reason":"bad"}}'),
        (NotFound(), 404, b'{"type":"not_found"}'),
        (MethodNotAllowed(), 405, b'{"type":"method_not_allowed"}'),
        (PayloadTooLarge(10), 413, b'{"type":"payload_too_large","data":{"limit_bytes":10}}'),
        (TooFewSamples(9, 3), 422, b'{"type":"too_few_samples","data":{"needed":9,"got":3}}'),
        (TooFewPoints(16, 9), 422, b'{"type":"too_few_points","data":{"needed":16,"got":9}}'),
        (NonFiniteLayout(), 422, b'{"type":"non_finite_layout"}'),
        (Internal("boom"), 500, b'{"type":"internal","data":{"reason":"boom"}}'),
    ],
)
def test_status_and_body(error: SidecarError, status: int, body: bytes) -> None:
    assert error.status == status
    assert error.body() == body


def test_unknown_route_is_not_found(client: TestClient) -> None:
    response = client.post("/v1/nothing", content=b"{}")
    assert response.status_code == 404
    assert response.content == b'{"type":"not_found"}'
    assert response.headers["content-type"] == "application/json"


def test_wrong_method_is_method_not_allowed(client: TestClient) -> None:
    response = client.get("/v1/topics/fit")
    assert response.status_code == 405
    assert response.content == b'{"type":"method_not_allowed"}'
    assert client.post("/healthz").status_code == 405


def test_body_above_the_limit_is_payload_too_large() -> None:
    with make_client(max_body_bytes=64) as small:
        response = small.post("/v1/layout/fit", content=request_bytes("layout_fit"))
    assert response.status_code == 413
    assert response.json() == {"type": "payload_too_large", "data": {"limit_bytes": 64}}


def test_streamed_body_above_the_limit_is_payload_too_large() -> None:
    def chunks():
        yield b"{" * 40
        yield b"{" * 40

    with make_client(max_body_bytes=64) as small:
        response = small.post("/v1/layout/fit", content=chunks())
    assert response.status_code == 413


def test_malformed_json_is_invalid_request(client: TestClient) -> None:
    response = client.post("/v1/layout/fit", content=b"{not json")
    assert response.status_code == 400
    body = json.loads(response.content)
    assert body["type"] == "invalid_request"
    assert body["data"]["reason"]
