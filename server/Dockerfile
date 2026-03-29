# Stage 1 — compile the operator module inside a Linux container.
# Supports linux/amd64 and linux/arm64 via Docker buildx multi-platform build.
# The .so compiled on macOS (Mach-O) is incompatible with Linux (ELF) even on ARM —
# the .dockerignore excludes it so only C sources enter the build context.
#
# Local dev (kind on Apple Silicon or x86):
#   docker build -t valkey-operator-server:dev .
#
# Multi-arch release (push to registry):
#   docker buildx build \
#     --platform linux/amd64,linux/arm64 \
#     --push \
#     -t ghcr.io/geoffrey/valkey-operator-server:9.0.3 .
FROM debian:trixie-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc make libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN make

# Stage 2 — custom Valkey image with the operator module embedded.
# Trixie (glibc) matches the builder — no musl/glibc ABI mismatch.
# The base image is multi-arch — Docker pulls the correct variant automatically.
FROM valkey/valkey:9.0.3-trixie

COPY --from=builder /src/valkey-operator-module.so /usr/local/lib/valkey-operator-module.so
