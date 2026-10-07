# syntax=docker/dockerfile:1.7
#
# The crosstalk binary. Build context is the repository root:
#   docker build -f deploy/crosstalk.Dockerfile -t crosstalk:dev .
# (deploy/compose.yaml does this for you.)

# Base image only provides rustup and a C toolchain (ring and zstd-sys need
# cc); the compiler is the nightly pinned in rust-toolchain.toml, installed
# with the minimal profile instead of that file's dev components.
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

# Cache mounts keep the registry and target dir across builds; the binary is
# copied out of the mount because it does not survive the RUN step.
# sharing=locked: compose builds this, the demo and the UI images in
# parallel, and they share these caches by path; unlocked, two builds
# unpacking the same crate fail with "File exists".
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=locked,target=/src/target \
    cargo build --locked --release --bin crosstalk \
    && install -D target/release/crosstalk /out/crosstalk

# The data directory (blob store under blobs/, plus anything else the gateway
# keeps on disk), owned by the runtime user. A named volume mounted here on
# first use copies this ownership, so the non-root process can write.
RUN install -d -o 65532 -g 65532 /out/data /out/data/blobs

FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /out/crosstalk /usr/local/bin/crosstalk
COPY --from=build --chown=65532:65532 /out/data /var/lib/crosstalk

# 8080 proxy (agents), 8081 operator API, 9464 ops (metrics, health).
EXPOSE 8080 8081 9464
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/crosstalk"]
CMD ["serve", "--role", "all", "--config", "/etc/crosstalk/crosstalk.json"]
