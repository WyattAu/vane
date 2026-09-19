# xDS gRPC transport design (hand-rolled, post-v0.3)

## Decision

Hand-roll the xDS gRPC transport on vane's own engine (h2 client + the
proto codecs we already own) instead of pulling tonic/hyper into
vane-control. This keeps the dependency tree pure, dogfoods the engine's
h2 client path, and matches the zero-copy ethos.

## Wire shape (ADS)

xDS v3 over gRPC: `AggregatedDiscoveryService/StreamAggregatedResources`.

- One h2 stream, long-lived, bidirectional streaming.
- gRPC framing: 5-byte prefix (1 compressed flag + 4 BE length) around
  protobuf messages — implement `GrpcFrame` in vane-proto (the gRPC relay
  already parses this shape for the length-delimited pass-through).
- Protobuf: hand-rolled encoders for the ~6 messages we need
  (`DiscoveryRequest`, `DiscoveryResponse`, `Resource`, and the
  `Cluster`/`Listener`/`RouteConfiguration` subset we consume) — no
  prost/tonic. Map into the existing `XdsSnapshot` types.

## State machine

```
connect → StreamAggregatedResources(open)
  → send DiscoveryRequest{ type_url, node, version_info="", resource_names=[] }
  ← DiscoveryResponse{ nonce, resources, version_info }
  → ACK DiscoveryRequest{ version_info, nonce }   (NACK on compile error)
  → apply → XdsSnapshot (atomic swap, same as the admin-plane path)
  ← incremental responses; repeat
```

- LDS first (listeners), then RDS (routes), then CDS/EDS (clusters) —
  we collapse all four into one snapshot compile; ACK each type_url
  independently (xDS requires per-type ACKs even from an ADS stream).
- Reconnect: re-request from the last ACKed version; the admin plane
  keeps serving the previous snapshot (never blank on control-plane loss).

## Milestones

1. ✅ GrpcFrame + protobuf wire encoders in vane-proto (fuzz target).
2. ✅ Blocking ADS client driver (`vane::xds_client`) over the engine's
   h2 + ADS session state machine (`vane-control::xds_grpc`); Envoy
   resource decoding + `map_snapshot` (vane-control::envoy);
   `vane xds-client` subcommand publishing snapshots to the admin
   plane; e2e: fake management plane (ACK/nonce verification,
   `ads_client.rs`) and full mapping e2e — hand-encoded Envoy Cluster
   + RouteConfiguration → driver → snapshot → live host-scoped route
   (`envoy_ads.rs`).
3. Interop test against envoy's `sample ADS server` in CI (kind job).
