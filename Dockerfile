# Native Rust pipeline.
# Build stage compiles the binary; the run stage holds only the binary.
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Cascade Lake or newer scorers: AVX2/BMI2 (x86-64-v3) is safe on every one.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"
RUN cargo build --release --locked && strip target/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
# mimalloc: back large allocations with 2 MB pages (fewer faults and TLB misses, faster exit).
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
# The scorer runs as uid 1000 with a read-only root; the binary needs neither
# a home nor any writable path besides /output.
CMD ["/app/pipeline"]
