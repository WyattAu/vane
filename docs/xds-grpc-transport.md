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
3. Interop test against envoy's `sample ADS server` in CI (kind job) —
   partially delivered: `scripts/xds_interop_test.sh` +
   `.github/workflows/interop.yml` run `vane xds-client` against a
   go-control-plane ADS server (this interop test found and fixed the
   DiscoveryRequest/Cluster wire bugs).

## Milestone 3a design: EDS + LDS breadth (pending implementation)

Today `vane xds-client` consumes CDS (clusters) and RDS (routes) and
maps them into `XdsSnapshot`; LDS/EDS subscriptions are opened but
their responses are ignored. Breadth plan, ordered by value:

### EDS (endpoints) — small, high value

Real fleets drive endpoint sets through EDS (a
`ClusterLoadAssignment` per cluster, keyed by cluster name) rather
than inline assignments. Implementation:

1. `vane_control::envoy` gains `decode_cla(buf) -> (name, backends)`:
   `ClusterLoadAssignment.cluster_name` = 1, `endpoints` = 2 — the
   same walk `decode_load_assignment` already performs, plus the name.
2. `xds_client_loop`: on an EDS response, decode each CLA into
   `eds_backends: BTreeMap<cluster_name, Vec<String>>` and ACK.
3. Snapshot publish (currently on the RDS ACK) merges: a cluster whose
   name is present in `eds_backends` takes those backends; otherwise
   the inline CDS assignment applies.
4. The CDS ACK already triggers the RDS subscription; an EDS response
   that changes an endpoint set should also re-trigger a publish
   (endpoint drift without config change is the whole point of EDS).
5. Fixture (`interop/ads`): switch the `shop` cluster to
   `EdsClusterConfig` (eds_config = ADS) and seed the snapshot's EDS
   resource; the interop script then asserts the same live route.

### LDS (listeners) — larger, design only for now

LDS responses carry `Listener` resources: `address` (1) →
`socket_address` (1/2), `filter_chains` = 25 →
`http_connection_manager` → `route_config_name` (RDS) or an inline
`RouteConfiguration`. Mapping that onto vane means extending
`XdsSnapshot` with a `listeners` list (address, port, TLS material
reference, routes) and extending `apply_snapshot` to compile listener
entries — the admin plane currently only owns routes + clusters.
Design sketch:

- `XdsListener { address, port, server_names, route_config_name }`;
  TLS material stays file-based (`listeners.tls.cert/key` style) until
  SDS exists — SDS is its own milestone.
- `apply_snapshot` compiles listeners into the shared bound-listener
  registry; a listener whose address is not pre-bound is logged and
  skipped (the process binds at startup only).
- Security note: dynamic listeners bind ports — gate behind an
  admin-plane opt-in flag (`[admin] allow_dynamic_listeners`).

Estimated: EDS ~150 LOC incl. fixture; LDS ~400–500 LOC plus the
snapshot/registry extension.
