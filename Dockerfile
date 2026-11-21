# Native Rust pipeline: one static binary in an empty image.
FROM rust:1.88-alpine AS build
RUN apk add --no-cache musl-dev build-base
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2/BMI2) is safe on the Cascade Lake scorer; the branch-alignment
# flags avoid the Skylake-family JCC erratum penalty.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq,+crt-static -C relocation-model=static -C llvm-args=-x86-branches-within-32B-boundaries"
RUN cargo build --release --locked --target x86_64-unknown-linux-musl \
 && strip target/x86_64-unknown-linux-musl/release/pipeline \
 && cp target/x86_64-unknown-linux-musl/release/pipeline /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
ENV MIMALLOC_ALLOW_LARGE_OS_PAGES=1
USER 1000:1000
CMD ["/pipeline"]
