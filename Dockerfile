# vane — multi-stage build (static musl for the sidecar profile)
FROM rust:1.85-slim AS build
WORKDIR /build
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p vane

FROM gcr.io/distroless/cc-debian12
COPY --from=build /build/target/release/vane /usr/local/bin/vane
COPY deploy/vane.example.toml /etc/vane/vane.toml
EXPOSE 8080 9100
ENTRYPOINT ["/usr/local/bin/vane"]
CMD ["run", "-c", "/etc/vane/vane.toml"]
