import json

from fastapi.testclient import TestClient

from crosstalk_topics import __main__ as cli
from crosstalk_topics.config import Config


def test_healthz(client: TestClient) -> None:
    response = client.get("/healthz")
    assert response.status_code == 200
    body = json.loads(response.content)
    assert body["status"] == "ok"
    assert body["contract"] == "v1"
    assert list(body["versions"]) == ["numba", "numpy", "python", "scikit_learn", "umap_learn"]
    assert body["versions"]["umap_learn"] == "0.5.12"
    assert response.content.startswith(b'{"status":"ok","contract":"v1","versions":{"numba":')


def test_healthcheck_fails_without_a_server() -> None:
    assert cli.healthcheck(Config(port=1)) == 1


def test_unknown_command_is_a_usage_error() -> None:
    assert cli.main(["dance"]) == 2


def test_pins_are_applied() -> None:
    import os

    import numba

    from crosstalk_topics import pins

    assert {name: os.environ[name] for name in pins.PINS} == pins.PINS
    assert numba.get_num_threads() == 1
    pins.verify()
