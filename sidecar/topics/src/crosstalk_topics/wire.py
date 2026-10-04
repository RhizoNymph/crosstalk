"""Request and response bodies of HTTP contract v1
(docs/features/topics_sidecar.md). Strict like the spec's serde: unknown
fields refused, every field present, integers only as JSON integers."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

U16 = 0xFFFF
U32 = 0xFFFF_FFFF
U64 = 0xFFFF_FFFF_FFFF_FFFF

CONTRACT = "v1"


class Wire(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Matrix(Wire):
    rows: Annotated[int, Field(ge=0, le=U32)]
    columns: Annotated[int, Field(ge=1, le=U16)]
    data: str


class TopicFitParams(Wire):
    seed: Annotated[int, Field(ge=0, le=U64)]
    min_cluster_size: Annotated[int, Field(ge=2, le=U32)]
    min_samples: Annotated[int, Field(ge=1, le=U32)] | None
    umap_neighbors: Annotated[int, Field(ge=2, le=200)]
    umap_components: Annotated[int, Field(ge=1, le=100)]
    top_terms: Annotated[int, Field(ge=1, le=50)]


class TopicFitRequest(Wire):
    embeddings: Matrix
    texts: list[str]
    params: TopicFitParams


class TopicWire(Wire):
    label: str
    terms: list[tuple[str, float]]


class TopicFitResponse(Wire):
    labels: list[int]
    topics: list[TopicWire]


class LayoutParams(Wire):
    """The spec's `ProjectionParams`."""

    limit: Annotated[int, Field(ge=1, le=100_000)]
    neighbors: Annotated[int, Field(ge=2, le=200)]
    min_dist_milli: Annotated[int, Field(ge=0, le=1_000)]
    seed: Annotated[int, Field(ge=0, le=U64)]


class LayoutFitRequest(Wire):
    embeddings: Matrix
    params: LayoutParams


class LayoutTransformRequest(Wire):
    base: Matrix
    params: LayoutParams
    points: Matrix


class LayoutResponse(Wire):
    coordinates: Matrix


class Health(Wire):
    status: str
    contract: str
    versions: dict[str, str]
