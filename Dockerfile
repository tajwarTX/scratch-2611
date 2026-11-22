# PROBE build (diagnostics only, fails the dev check on purpose).
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"
RUN cargo build --release --locked && strip target/release/pipeline target/release/residency target/release/inject target/release/maxrss
COPY probe/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM rust:1.85.1-slim-bookworm AS hybuild
WORKDIR /hy
COPY hy/Cargo.toml hy/Cargo.lock ./
COPY hy/src ./src
COPY hy/pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -Cprofile-use=/hy/pgo/x86.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu

FROM debian:bookworm-slim
COPY --from=hybuild /hy/target/x86_64-unknown-linux-gnu/release/sessions /app/pipeline
COPY --from=build /src/target/release/residency /app/residency
COPY --from=build /src/target/release/inject /app/inject
COPY --from=build /src/target/release/maxrss /app/maxrss
COPY --from=build /gen/target/release/generate /app/generate
COPY probe/probe.sh /app/probe.sh
COPY probe/fixtures /app/fixtures
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
CMD ["/app/probe.sh"]
