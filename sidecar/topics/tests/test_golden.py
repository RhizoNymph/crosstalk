"""The served bytes of each fixture request equal its golden response.

`CROSSTALK_BLESS=1 uv run pytest tests/test_golden.py` rewrites the goldens
(after a deliberate change of the computation or of a pinned library). The
Rust adapter's tests decode the same files.
"""

import json
import os

import pytest
from fastapi.testclient import TestClient

from crosstalk_topics.wire import (
    LayoutFitRequest,
    LayoutResponse,
    LayoutTransformRequest,
    TopicFitRequest,
    TopicFitResponse,
)

from .conftest import FIXTURES, NAMES, ROUTES, request_bytes
from .fixtures import make_fixtures

BLESS = os.environ.get("CROSSTALK_BLESS") == "1"
REQUESTS = {
    "topics_fit": TopicFitRequest,
    "layout_fit": LayoutFitRequest,
    "layout_transform": LayoutTransformRequest,
}
RESPONSES = {
    "topics_fit": TopicFitResponse,
    "layout_fit": LayoutResponse,
    "layout_transform": LayoutResponse,
}


@pytest.mark.parametrize("name", NAMES)
def test_request_fixture_is_what_the_generator_writes(name: str) -> None:
    generated = make_fixtures.build()[name].model_dump_json().encode()
    if BLESS:
        (FIXTURES / f"{name}.request.json").write_bytes(generated)
    assert request_bytes(name) == generated


@pytest.mark.parametrize("name", NAMES)
def test_request_fixture_is_canonical(name: str) -> None:
    raw = request_bytes(name)
    assert not raw.endswith(b"\n")
    assert REQUESTS[name].model_validate_json(raw).model_dump_json().encode() == raw


@pytest.mark.parametrize("name", NAMES)
def test_response_matches_golden(client: TestClient, name: str) -> None:
    response = client.post(ROUTES[name], content=request_bytes(name))
    assert response.status_code == 200, response.content
    golden = FIXTURES / f"{name}.response.json"
    if BLESS:
        golden.write_bytes(response.content)
    assert response.content == golden.read_bytes()
    RESPONSES[name].model_validate_json(golden.read_bytes())
    assert json.loads(golden.read_bytes())
