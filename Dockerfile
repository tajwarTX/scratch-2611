# PROBE build (diagnostics only, fails the dev check on purpose).
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"
RUN cargo build --release --locked && strip target/release/pipeline target/release/residency
COPY probe/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
COPY --from=build /src/target/release/residency /app/residency
COPY --from=build /gen/target/release/generate /app/generate
COPY probe/probe.sh /app/probe.sh
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
CMD ["/app/probe.sh"]
