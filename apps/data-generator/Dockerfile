#checkov:skip=CKV_DOCKER_2:Healthcheck not applicable for batch data generator - used for synthetic data generation only in prototype sandbox account
# ---- Stage 1: Builder ----
FROM public.ecr.aws/docker/library/rust:slim AS builder
WORKDIR /usr/src/app
COPY Cargo.toml ./
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src
COPY src ./src
COPY schemas ./schemas
RUN rustup target add x86_64-unknown-linux-gnu
ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN cargo build --release --target x86_64-unknown-linux-gnu

# ---- Stage 2: Final Image ----
FROM public.ecr.aws/docker/library/debian:bookworm-slim

# Install CA certificates for HTTPS
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Create user and copy binary with proper permissions
RUN groupadd --system app && useradd --system --gid app app
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-gnu/release/data-generator /usr/local/bin/data-generator
RUN chmod +x /usr/local/bin/data-generator && chown app:app /usr/local/bin/data-generator

# Enable Rust logging at info level
ENV RUST_LOG=debug

USER app
ENTRYPOINT ["/usr/local/bin/data-generator"]
