  protobuf messages; implement `GrpcFrame` in vane-proto (the gRPC relay
  `Cluster`/`Listener`/`RouteConfiguration` subset we consume); no
- LDS first (listeners), then RDS (routes), then CDS/EDS (clusters) -
   `ads_client.rs`) and full mapping e2e; hand-encoded Envoy Cluster
3. Interop test against envoy's `sample ADS server` in CI (kind job) -
### EDS (endpoints): small, high value
   `ClusterLoadAssignment.cluster_name` = 1, `endpoints` = 2; the
### SDS: shipped (2026-10-02)
disk) and hot-swapped onto the pre-bound listener; certificate
### LDS (listeners): decode + multi-RDS merge shipped (2026-10-03)
listeners; vane keeps one route table, the same model as the Gateway
route TABLES would need per-listener routers in the worker bootstrap -
### LDS (listeners): TLS rotation shipped (2026-09-28)
entries; the admin plane currently only owns routes + clusters.
address and swaps the shared slot; the same hot-swap the file watcher
  SDS exists; SDS is its own milestone.
- Security note: dynamic listeners bind ports; gate behind an