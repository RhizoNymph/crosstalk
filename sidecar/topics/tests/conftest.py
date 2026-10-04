"""Shared fixtures. Importing the package first applies the thread pins."""

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import crosstalk_topics  # noqa: F401 - pins threads before numpy is imported
from crosstalk_topics.app import create_app
from crosstalk_topics.config import Config

FIXTURES = Path(__file__).parent / "fixtures"
NAMES = ["topics_fit", "layout_fit", "layout_transform"]
ROUTES = {
    "topics_fit": "/v1/topics/fit",
    "layout_fit": "/v1/layout/fit",
    "layout_transform": "/v1/layout/transform",
}


def make_client(
    *, max_body_bytes: int | None = None, layout_cache: int | None = None
) -> TestClient:
    config = Config()
    if max_body_bytes is not None:
        config = replace(config, max_body_bytes=max_body_bytes)
    if layout_cache is not None:
        config = replace(config, layout_cache=layout_cache)
    return TestClient(create_app(config))


@pytest.fixture(scope="session")
def client() -> Iterator[TestClient]:
    with make_client() as client:
        yield client


def request_bytes(name: str) -> bytes:
    return (FIXTURES / f"{name}.request.json").read_bytes()
