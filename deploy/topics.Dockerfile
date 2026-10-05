# syntax=docker/dockerfile:1.7
#
# The topics sidecar (sidecar/topics): topic fits and UMAP layouts over
# HTTP. Build context is the repository root:
#   docker build -f deploy/topics.Dockerfile -t crosstalk-topics:dev .
# Contract and run instructions: docs/features/topics_sidecar.md.

FROM ghcr.io/astral-sh/uv:0.12.19 AS uv

# The builder and the runtime share one base, so the venv's interpreter
# path (/usr/local/bin/python3.14) exists in both.
FROM python:3.14.4-slim-trixie AS build

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /src
COPY sidecar/topics/pyproject.toml sidecar/topics/uv.lock sidecar/topics/.python-version ./
# Dependencies first, so editing the sidecar's code reuses this layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY sidecar/topics/src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.14.4-slim-trixie

COPY --from=build /app/.venv /app/.venv

# Determinism (docs/features/topics_sidecar.md): one thread per pool and a
# generic JIT target. The package forces the same values at import; these
# make them visible to anything started in the image. Numba caches compiled
# kernels in a directory the runtime user can write.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMBA_NUM_THREADS=1 \
    NUMBA_CPU_NAME=generic \
    NUMBA_CACHE_DIR=/var/cache/numba \
    CROSSTALK_TOPICS_HOST=0.0.0.0 \
    CROSSTALK_TOPICS_PORT=8090

RUN install -d -o 65532 -g 65532 /var/cache/numba

EXPOSE 8090
USER 65532:65532
# Container healthcheck: CMD ["crosstalk-topics", "healthcheck"].
ENTRYPOINT ["crosstalk-topics"]
CMD ["serve"]
