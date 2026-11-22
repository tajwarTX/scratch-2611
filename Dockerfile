# Native Rust pipeline.
# Build stage compiles the binary; the run stage holds only the binary.
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Code generation for the scorer's Cascade Lake CPU (AVX-512), with hot
# branches kept inside 32-byte blocks (JCC erratum). The flags apply to the
# binary only (explicit --target), so build scripts still run on any builder.
ENV RUSTFLAGS="-C target-cpu=cascadelake -C llvm-args=-x86-branches-within-32B-boundaries"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu && strip target/x86_64-unknown-linux-gnu/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/pipeline /app/pipeline
# mimalloc: back large allocations with 2 MB pages (fewer faults and TLB misses, faster exit).
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
# The scorer runs as uid 1000 with a read-only root; the binary needs neither
# a home nor any writable path besides /output.
CMD ["/app/pipeline"]
