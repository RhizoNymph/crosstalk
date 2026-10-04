"""The HTTP service: routes, body limits, error bodies and the worker.

Every computation runs on one worker thread, one request at a time, so the
event loop stays free for `/healthz` and fits never compete for a core.
Bodies are read and size-checked on the loop, then parsed and computed on
the worker (parsing a large body is CPU work too).
"""

import asyncio
import logging
import platform
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from crosstalk_topics import layout, log, pins, topics
from crosstalk_topics.config import Config
from crosstalk_topics.errors import (
    Internal,
    InvalidRequest,
    MethodNotAllowed,
    NotFound,
    PayloadTooLarge,
    SidecarError,
)
from crosstalk_topics.wire import (
    CONTRACT,
    Health,
    LayoutFitRequest,
    LayoutTransformRequest,
    TopicFitRequest,
)

logger = logging.getLogger("crosstalk_topics")

JSON = "application/json"


def versions() -> dict[str, str]:
    """The versions the determinism guarantee is relative to, keys ascending."""
    return {
        "numba": version("numba"),
        "numpy": version("numpy"),
        "python": platform.python_version(),
        "scikit_learn": version("scikit-learn"),
        "umap_learn": version("umap-learn"),
    }


def parse[M: BaseModel](model: type[M], body: bytes) -> M:
    try:
        return model.model_validate_json(body)
    except ValidationError as error:
        problems = [
            f"{'.'.join(str(part) for part in problem['loc']) or '<body>'}: {problem['msg']}"
            for problem in error.errors(include_url=False)
        ]
        raise InvalidRequest("; ".join(problems)) from error


def error_response(error: SidecarError) -> Response:
    return Response(content=error.body(), status_code=error.status, media_type=JSON)


async def read_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise PayloadTooLarge(limit)
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise PayloadTooLarge(limit)
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(config: Config) -> FastAPI:
    pins.verify()
    cache = layout.LayoutCache(config.layout_cache)
    health = Health(status="ok", contract=CONTRACT, versions=versions()).model_dump_json().encode()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="fit") as worker:
            app.state.worker = worker
            yield

    app = FastAPI(lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None)

    async def compute(request: Request, work: Callable[[bytes], BaseModel]) -> Response:
        try:
            body = await read_body(request, config.max_body_bytes)
            loop = asyncio.get_running_loop()
            reply = await loop.run_in_executor(request.app.state.worker, work, body)
            return Response(content=reply.model_dump_json().encode(), media_type=JSON)
        except SidecarError as error:
            return error_response(error)
        except Exception as error:  # noqa: BLE001 - the outermost handler: anything else is a 500
            log.exception(logger, "request failed", route=request.url.path)
            return error_response(Internal(f"{type(error).__name__}: {error}"))

    @app.middleware("http")
    async def access_log(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        log.event(
            logger,
            logging.DEBUG if request.url.path == "/healthz" else logging.INFO,
            "request",
            method=request.method,
            route=request.url.path,
            status=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 3),
        )
        return response

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException) -> Response:
        match error.status_code:
            case 404:
                return error_response(NotFound())
            case 405:
                return error_response(MethodNotAllowed())
            case status:
                return error_response(Internal(f"http status {status}: {error.detail}"))

    @app.get("/healthz")
    async def healthz() -> Response:
        return Response(content=health, media_type=JSON)

    @app.post("/v1/topics/fit")
    async def topics_fit(request: Request) -> Response:
        return await compute(request, lambda body: topics.fit(parse(TopicFitRequest, body)))

    @app.post("/v1/layout/fit")
    async def layout_fit(request: Request) -> Response:
        return await compute(request, lambda body: layout.fit(parse(LayoutFitRequest, body), cache))

    @app.post("/v1/layout/transform")
    async def layout_transform(request: Request) -> Response:
        return await compute(
            request, lambda body: layout.transform(parse(LayoutTransformRequest, body), cache)
        )

    return app
