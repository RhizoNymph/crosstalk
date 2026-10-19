# syntax=docker/dockerfile:1.7
#
# The operator UI (Topcoat), `crosstalk-ui`, a member of the root Cargo
# workspace. Build context is the repository root:
#   docker build -f deploy/ui.Dockerfile -t crosstalk-ui:dev .
# The config it reads is mounted at /etc/crosstalk/ui.json
# (`CROSSTALK_UI_CONFIG`); see ui/config.json.

FROM node:24.21.0-trixie-slim AS elements
WORKDIR /elements
RUN corepack enable && corepack prepare pnpm@11.27.1 --activate
COPY ui/elements/package.json ui/elements/pnpm-lock.yaml ui/elements/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY ui/elements/ ./
RUN pnpm run build

FROM rust:1.98.1-slim-trixie AS build
ARG RUST_TOOLCHAIN=nightly-2026-10-02
ENV RUSTUP_TOOLCHAIN=${RUST_TOOLCHAIN}
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal
# The Topcoat CLI writes the asset bundle (elements, stylesheet) the binary
# serves.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    cargo install topcoat-cli --version =0.9.0 --locked --root /tools
WORKDIR /src
# The workspace depends on the private a2a-transmission-bench repo by git
# tag (Cargo.lock pins its URL and commit). The build gets it from a bare
# mirror passed as the `a2a` build context (compose: A2A_BENCH_GIT); git's
# insteadOf sends cargo's fetch of the GitHub URL to that mirror, so no
# token is needed and the lockfile is unchanged.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
COPY --from=a2a . /deps/a2a-transmission-bench.git
RUN git config --global url."file:///deps/a2a-transmission-bench.git".insteadOf \
        "https://github.com/RhizoNymph/a2a-transmission-bench" \
    && git config --global --add safe.directory '*'
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
COPY . .
COPY --from=elements /elements/dist ./ui/elements/dist
# From the workspace root, into the workspace's target/. build.rs downloads
# Tailwind (checksum-pinned on linux-x64). `topcoat asset bundle` reuses the
# release build and writes the bundle beside the binary, in
# target/release/assets; both leave the cached target/ for /out.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/src/target \
    cargo build -p crosstalk-ui --release --locked \
    && /tools/bin/topcoat asset bundle -p crosstalk-ui --release \
    && install -D target/release/crosstalk-ui /out/crosstalk-ui \
    && cp -r target/release/assets /out/assets

FROM gcr.io/distroless/cc-debian13:nonroot
# `AssetBundle::load()` reads the bundle from `assets/` beside the binary.
COPY --from=build /out/crosstalk-ui /usr/local/bin/crosstalk-ui
COPY --from=build /out/assets /usr/local/bin/assets
EXPOSE 3000
USER 65532:65532
ENV CROSSTALK_UI_CONFIG=/etc/crosstalk/ui.json
ENTRYPOINT ["/usr/local/bin/crosstalk-ui"]
