# syntax=docker/dockerfile:1.7
#
# The operator UI over the fixture world, replaying its last stretch
# (ui/config.demo.json). Built like deploy/ui.Dockerfile, from the
# repository root:
#   docker build -f deploy/ui.demo.Dockerfile -t crosstalk-ui-demo .
#   docker run --rm -p 0.0.0.0:3000:3000 crosstalk-ui-demo

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
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    cargo install topcoat-cli --version =0.9.0 --locked --root /tools
WORKDIR /src
COPY . .
COPY --from=elements /elements/dist ./ui/elements/dist
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/src/target \
    cargo build -p crosstalk-ui --release --locked \
    && /tools/bin/topcoat asset bundle -p crosstalk-ui --release \
    && install -D target/release/crosstalk-ui /out/crosstalk-ui \
    && cp -r target/release/assets /out/assets

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /out/crosstalk-ui /usr/local/bin/crosstalk-ui
COPY --from=build /out/assets /usr/local/bin/assets
COPY ui/config.demo.json /etc/crosstalk/ui.json
EXPOSE 3000
USER 65532:65532
ENV CROSSTALK_UI_CONFIG=/etc/crosstalk/ui.json
ENTRYPOINT ["/usr/local/bin/crosstalk-ui"]
