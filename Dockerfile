# Shopping Sessions: native Rust pipeline, one static binary in an empty image.
FROM rust:1.85.1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
# Profile-guided optimization with the committed profile (pgo/make_profile.sh makes it).
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -Cprofile-use=/src/pgo/x86.profdata"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
CMD ["/sessions"]
