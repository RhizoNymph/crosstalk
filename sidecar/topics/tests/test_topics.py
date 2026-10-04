import json

import numpy as np
from fastapi.testclient import TestClient

from crosstalk_topics import matrix, topics
from crosstalk_topics.wire import TopicFitParams

from .conftest import request_bytes
from .fixtures import make_fixtures


def test_renumbering_is_by_size_then_first_member() -> None:
    raw = np.array([5, 3, 3, -1, 5, 9, 9, 3, -2], dtype=np.int64)
    # sizes: 3 -> 3, 5 -> 2, 9 -> 2; 5's first member (0) precedes 9's (5).
    assert topics.renumber(raw).tolist() == [1, 0, 0, -1, 1, 2, 2, 0, -1]


def test_renumbering_all_outliers() -> None:
    assert topics.renumber(np.array([-1, -1], dtype=np.int64)).tolist() == [-1, -1]


def test_label_is_the_first_three_terms_or_the_cluster_number() -> None:
    terms = [("wiki", 0.5), ("page", 0.4), ("edit", 0.3), ("draft", 0.2)]
    assert topics.label_of(0, terms) == "wiki, page, edit"
    assert topics.label_of(4, [("wiki", 0.5)]) == "wiki"
    assert topics.label_of(4, []) == "topic 4"


def test_needed_is_the_largest_requirement() -> None:
    def params(size: int, neighbors: int, components: int) -> TopicFitParams:
        return TopicFitParams(
            seed=0,
            min_cluster_size=size,
            min_samples=None,
            umap_neighbors=neighbors,
            umap_components=components,
            top_terms=5,
        )

    assert topics.needed(params(20, 15, 5)) == 20
    assert topics.needed(params(5, 15, 5)) == 16
    assert topics.needed(params(5, 2, 30)) == 32


def test_too_few_samples(client: TestClient) -> None:
    body = json.loads(request_bytes("topics_fit"))
    body["params"]["min_cluster_size"] = 40
    response = client.post("/v1/topics/fit", content=json.dumps(body))
    assert response.status_code == 422
    assert response.content == b'{"type":"too_few_samples","data":{"needed":40,"got":35}}'


def test_fit_labels_and_terms_are_consistent(client: TestClient) -> None:
    response = client.post("/v1/topics/fit", content=request_bytes("topics_fit"))
    assert response.status_code == 200
    body = json.loads(response.content)
    assert len(body["labels"]) == 35
    clusters = len(body["topics"])
    assert clusters == 3
    members = [body["labels"].count(k) for k in range(clusters)]
    assert all(count > 0 for count in members)
    assert members == sorted(members, reverse=True)
    assert all(label == -1 or 0 <= label < clusters for label in body["labels"])
    for topic in body["topics"]:
        weights = [weight for _, weight in topic["terms"]]
        assert all(weight > 0 for weight in weights)
        assert weights == sorted(weights, reverse=True)
        assert topic["label"] == ", ".join(term for term, _ in topic["terms"][:3])
    # Each topic is labelled with one fixture cluster's vocabulary.
    vocabularies = [set(vocabulary) for vocabulary in make_fixtures.VOCABULARIES]
    owners = []
    for topic in body["topics"]:
        words = set(topic["label"].split(", "))
        owners.append(next(k for k, vocabulary in enumerate(vocabularies) if words <= vocabulary))
    assert sorted(owners) == [0, 1, 2]


def test_documents_without_vocabulary_get_numbered_labels(client: TestClient) -> None:
    body = json.loads(request_bytes("topics_fit"))
    body["texts"] = ["the and of"] * len(body["texts"])
    response = client.post("/v1/topics/fit", content=json.dumps(body))
    assert response.status_code == 200
    decoded = json.loads(response.content)
    assert [topic["label"] for topic in decoded["topics"]] == [
        f"topic {k}" for k in range(len(decoded["topics"]))
    ]
    assert all(topic["terms"] == [] for topic in decoded["topics"])


def test_identical_points_cluster_without_error(client: TestClient) -> None:
    rows = np.tile(np.array([[1.0] + [0.0] * 7], dtype=np.float32), (12, 1))
    body = {
        "embeddings": matrix.encode(rows).model_dump(),
        "texts": ["wiki page"] * 12,
        "params": {
            "seed": 1,
            "min_cluster_size": 3,
            "min_samples": 2,
            "umap_neighbors": 4,
            "umap_components": 2,
            "top_terms": 3,
        },
    }
    response = client.post("/v1/topics/fit", content=json.dumps(body))
    assert response.status_code in (200, 422), response.content
