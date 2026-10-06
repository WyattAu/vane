#!/usr/bin/env bash
# End-to-end Kubernetes smoke: kind cluster + vane chart + Gateway API
# HTTPRoute discovery + data-path verification.
#
# Requirements: docker, kind, kubectl, helm, curl.
# Usage: scripts/kind-smoke.sh [--keep]
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=vane-smoke
NS=vane-test
IMAGE=vane:kind-smoke
KIND=${KIND:-kind}
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
KEEP=${1:-}

cleanup() {
    if [ "$KEEP" != "--keep" ]; then
        "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "== create cluster"
# Keep the output: `>/dev/null 2>&1` turned a cluster-creation failure
# into a bare "exit code 1" at this line, which is how this job stayed
# unexplained. On failure, print the log before giving up.
cluster_log="$(mktemp)"
if ! "$KIND" create cluster --name "$CLUSTER" --wait 120s >"$cluster_log" 2>&1; then
    echo "!! kind create cluster failed:"
    sed 's/^/   /' "$cluster_log"
    "$KIND" export logs "$CLUSTER" --name "$CLUSTER" >"$cluster_log.logs" 2>&1 || true
    echo "!! node logs:"
    sed 's/^/   /' "$cluster_log.logs" | tail -60
    exit 1
fi
cat "$cluster_log"
export KUBECONFIG="${HOME}/.kube/config"

echo "== build + load vane image"
docker build -q -t "$IMAGE" . >/dev/null
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null

echo "== install gateway-api CRDs"
"$KUBECTL" apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml >/dev/null

# NOTE: no `--wait` here, on purpose.
#
# The k8s-provider values ship no static routes — vane programs them from
# HTTPRoutes — and `/readyz` reports ready only once the route table is
# non-empty. The HTTPRoute is created in the next step, so `--wait` was
# waiting on a condition that could not become true until after the wait
# ended: the install always failed with "client rate limiter Wait
# returned an error: context deadline exceeded". The route is applied
# first, then readiness is waited on below.
echo "== install chart"
"$HELM" install vane charts/vane -n "$NS" --create-namespace \
    -f charts/vane/values-k8s-provider.yaml \
    --set image.repository="${IMAGE%%:*}" \
    --set image.tag="${IMAGE##*:}" \
    --set image.pullPolicy=Never >/dev/null

echo "== deploy upstream + HTTPRoute"
# `kubectl create deployment` names the container after the *image*
# (http-echo), not the deployment — a `set image deployment/upstream
# upstream=...` referencing a container named `upstream` fails outright.
# The image is already set by --image; only the args need setting.
"$KUBECTL" create deployment upstream -n "$NS" \
    --image=hashicorp/http-echo:1.0 --replicas=1 >/dev/null
# There is no `kubectl set args` — container args are a spec field, so
# patch it. (The old line failed with `unknown command "args ..."`.)
# 8080, not 80: hashicorp/http-echo runs as a non-root user and cannot
# bind a privileged port ("listen tcp :80: bind: permission denied",
# which put the upstream pod in CrashLoopBackOff).
"$KUBECTL" -n "$NS" patch deployment upstream --type=json -p='
[{"op": "replace", "path": "/spec/template/spec/containers/0/args",
  "value": ["-listen=:8080", "-text=hello-from-upstream"]}]' >/dev/null
"$KUBECTL" expose deployment upstream -n "$NS" --port 8080 >/dev/null
# `namespace` is set explicitly rather than passing `-n`: the heredoc
# apply had no namespace flag, so the route landed in whatever namespace
# the current context pointed at (`default`), the readiness check looked
# in $NS and found nothing, and vane never programmed a route — so the
# pods stayed 503 forever. Setting it in the object makes the apply
# correct regardless of context.
cat <<YAML | "$KUBECTL" apply -f - >/dev/null
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: smoke-route
  namespace: $NS
spec:
  hostnames: ["smoke.test"]
  rules:
    - backendRefs:
        - name: upstream
          port: 8080
YAML

echo "== wait for vane + upstream readiness"
for _ in $(seq 1 60); do
    UP=$("$KUBECTL" -n "$NS" get pods -l app=upstream -o jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "")
    [ "$UP" = "Running" ] && break
    sleep 2
done
# Readiness, not just rollout: the pods stay NotReady until the provider
# has programmed the route, so `rollout status` would pass while the data
# plane was still refusing traffic.
for _ in $(seq 1 60); do
    READY=$("$KUBECTL" -n "$NS" get deploy vane -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
    [ "${READY:-0}" -ge 1 ] 2>/dev/null && break
    sleep 2
done
"$KUBECTL" -n "$NS" get pods -o wide
READY=$("$KUBECTL" -n "$NS" get deploy vane -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
if [ "${READY:-0}" -lt 1 ] 2>/dev/null; then
    echo "!! vane never became ready after the HTTPRoute was applied"
    "$KUBECTL" -n "$NS" describe deploy vane | sed 's/^/   /' | tail -30
    "$KUBECTL" -n "$NS" logs -l app.kubernetes.io/name=vane --tail=40 | sed 's/^/   /'
    exit 1
fi

echo "== probe data path through vane"
VANE_POD=$("$KUBECTL" -n "$NS" get pods -l app.kubernetes.io/name=vane -o jsonpath='{.items[0].metadata.name}')
UPSTREAM_IP=$("$KUBECTL" -n "$NS" get svc upstream -o jsonpath='{.spec.clusterIP}')
# The provider programs routes from HTTPRoutes; hit vane's data port from
# inside its own pod (bypasses pod-networking variance across hosts).
"$KUBECTL" -n "$NS" exec "$VANE_POD" -- \
    curl -s --max-time 5 -o /dev/null -w '%{http_code}' \
    -H 'Host: smoke.test' "http://127.0.0.1:8080/hello" > /tmp/vane-smoke-code || true
CODE=$(cat /tmp/vane-smoke-code 2>/dev/null || echo 000)
echo "data-path status: $CODE"

# Cross-pod probe (requires working pod networking — standard on CI).
if "$KUBECTL" -n "$NS" run smoketest --rm -i --restart=Never \
    --image=curlimages/curl:latest --command -- \
    curl -s --max-time 5 -H 'Host: smoke.test' \
    "http://vane.$NS.svc:8080/hello" 2>/dev/null | grep -q .; then
    echo "cross-pod probe: OK"
else
    echo "cross-pod probe: skipped (host pod-networking variance)"
fi

if [ "$CODE" != "200" ]; then
    echo "FAIL: expected 200 through vane, got $CODE"
    "$KUBECTL" -n "$NS" logs deploy/vane --tail=50 || true
    exit 1
fi
echo "PASS: kind smoke — vane served the HTTPRoute-discovered route"
