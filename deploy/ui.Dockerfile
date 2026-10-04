# syntax=docker/dockerfile:1.7
#
# The operator UI (Topcoat). Build context is the repository root, because
# ui/ depends on spec/ by path and spec/ inherits from the root workspace:
#   docker build -f deploy/ui.Dockerfile -t crosstalk-ui:dev .
# It needs ui/ (on feat/ui) present in the checkout; the compose service sits
# behind the `ui` profile until it is merged.

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
WORKDIR /src
COPY . .
COPY --from=elements /elements/dist ./ui/elements/dist
WORKDIR /src/ui
# build.rs downloads Tailwind (checksum-pinned on linux-x64).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/ui/target \
    cargo build --locked --release \
    && install -D target/release/crosstalk-ui /out/crosstalk-ui

FROM gcr.io/distroless/cc-debian13:nonroot
WORKDIR /app
COPY --from=build /out/crosstalk-ui /usr/local/bin/crosstalk-ui
COPY --from=build /src/ui/elements/dist ./elements/dist
COPY --from=build /src/ui/styles ./styles
EXPOSE 3000
USER 65532:65532
ENV CROSSTALK_UI_CONFIG=/etc/crosstalk/ui.json
ENTRYPOINT ["/usr/local/bin/crosstalk-ui"]
