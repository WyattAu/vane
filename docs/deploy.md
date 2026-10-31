- `shm_size: "256mb"`; `/dev/shm` defaults to 64 MB, too small for
- `cap_add: [IPC_LOCK]`; io_uring fixed buffers are page-locked
# gateway-api CRDs and RBAC: included in the chart)
`/healthz` + `/readyz` probes, Prometheus scrape annotations, and -
with `k8sProvider.enabled`; ServiceAccount + ClusterRole/ClusterRoleBinding
- **Providers**: the chart's static config and the K8s provider compose -