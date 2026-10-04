# The operator UI over the fixture world, replaying its last stretch.
# Build from the repo root:
#   docker build -f deploy/ui.demo.Dockerfile -t crosstalk-ui-demo .
#   docker run --rm -p 0.0.0.0:3000:3000 crosstalk-ui-demo

FROM node:24-bookworm-slim AS elements
RUN npm install -g pnpm@11.13.1
WORKDIR /src/ui/elements
COPY ui/elements/ ./
RUN pnpm install --frozen-lockfile && pnpm build

FROM debian:bookworm-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gcc libc6-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo PATH=/usr/local/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain nightly-2026-10-02
RUN cargo install topcoat-cli --version =0.9.0 --locked --root /tools
WORKDIR /src
COPY spec/ spec/
COPY ui/ ui/
COPY --from=elements /src/ui/elements/dist ui/elements/dist
WORKDIR /src/ui
RUN cargo build --release --locked && /tools/bin/topcoat asset bundle --release

FROM gcr.io/distroless/cc-debian12
COPY --from=build /src/ui/target/release/crosstalk-ui /usr/local/bin/crosstalk-ui
COPY --from=build /src/ui/target/release/assets /usr/local/bin/assets
COPY ui/config.demo.json /etc/crosstalk/ui.json
ENV CROSSTALK_UI_CONFIG=/etc/crosstalk/ui.json
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/crosstalk-ui"]
