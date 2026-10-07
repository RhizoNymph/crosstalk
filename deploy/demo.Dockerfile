# syntax=docker/dockerfile:1.7
#
# crosstalk-demo: the fake Anthropic upstream, the shared wiki and the agent
# swarm (one binary, subcommands), plus ct-eval for `run.sh bench` (the
# deployment host has no Rust toolchain). Build context is the repository root:
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

# One cargo invocation for both binaries, so they share one dependency
# resolution and one lock on the cached target dir. ct-eval's default gates
# path is compiled in as /src/crates/eval/gates.toml, which the runtime image
# does not have; the file is shipped beside it and `bench` passes --gates.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/src/target \
    cargo build --locked --release \
        -p crosstalk-demo --bin crosstalk-demo \
        -p crosstalk-eval --bin ct-eval --bin ct-bench-detect \
    && install -D target/release/crosstalk-demo /out/crosstalk-demo \
    && install -D target/release/ct-eval /out/ct-eval \
    && install -D target/release/ct-bench-detect /out/ct-bench-detect \
    && install -D -m 0644 crates/eval/gates.toml /out/eval/gates.toml

FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /out/crosstalk-demo /usr/local/bin/crosstalk-demo
COPY --from=build /out/ct-eval /usr/local/bin/ct-eval
COPY --from=build /out/ct-bench-detect /usr/local/bin/ct-bench-detect
COPY --from=build /out/eval/gates.toml /usr/local/share/crosstalk-eval/gates.toml

# 8070 fake upstream, 8090 wiki. The swarm and ct-eval listen on nothing.
EXPOSE 8070 8090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/crosstalk-demo"]
CMD ["help"]
