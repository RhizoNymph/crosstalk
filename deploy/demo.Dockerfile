# syntax=docker/dockerfile:1.7
#
# crosstalk-demo: the fake Anthropic upstream, the shared wiki and the agent
# swarm (one binary, subcommands). Build context is the repository root:
#   docker build -f deploy/demo.Dockerfile -t crosstalk-demo:dev .
# (deploy/compose.demo.yaml does this for you.)

# Built like deploy/crosstalk.Dockerfile: the nightly pinned in
# rust-toolchain.toml, minimal profile. The cache mounts are the same, so
# dependencies compiled for one image are reused by the other (cargo's
# directory lock serialises concurrent builds).
FROM rust:1.98.1-slim-trixie AS build

# Keep in step with rust-toolchain.toml.
ARG RUST_TOOLCHAIN=nightly-2026-10-02
ENV RUSTUP_TOOLCHAIN=${RUST_TOOLCHAIN}
RUN rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal

WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p crosstalk-demo --bin crosstalk-demo \
    && install -D target/release/crosstalk-demo /out/crosstalk-demo

FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /out/crosstalk-demo /usr/local/bin/crosstalk-demo

# 8070 fake upstream, 8090 wiki. The swarm listens on nothing.
EXPOSE 8070 8090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/crosstalk-demo"]
CMD ["help"]
