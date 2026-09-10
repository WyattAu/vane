# vane — deterministic L4/L7 reverse proxy
#
# Multi-stage build. Runtime keeps glibc + CA certs for ACME and the
# /dev/shm default of the sidecar transport.

FROM rust:1.97-bookworm AS build
WORKDIR /build

# Layer cache: manifests first.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Workspace members declare their own Cargo.tomls via crates/; a full
# source copy is required for the workspace to resolve — keep the copy
# before any source edit so dependency churn stays cache-friendly.

RUN cargo build --release -p vane --features h2,file-provider,docker-provider

# ---- runtime ----
FROM debian:bookworm-slim
# curl-minimal: container-native healthchecks + debugging.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --no-create-home --shell /usr/sbin/nologin vane

COPY --from=build /build/target/release/vane /usr/local/bin/vane

# Config + ACME storage volumes.
VOLUME ["/etc/vane", "/var/lib/vane"]
EXPOSE 8080 8443 9090

USER vane
ENTRYPOINT ["/usr/local/bin/vane"]
CMD ["run", "-c", "/etc/vane/vane.toml"]
