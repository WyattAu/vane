# Handoff: proof-loop iteration 1 (TLS-h2 flow-control deadlock arc)
- **PLAN; done** (`.loop/plan.md`, revised after research): the
- **RESEARCH; done** (`.loop/research.md`): the mechanism pinned
  every sid completes `sent=65536 last=true held=0`; the shim
  concurrent 80/80, reconnect storms 30/30) while h2load churns -
- **PROVE; partial**: the metric exists (h2load read_err count,
  written; it needs the re-arm gate located first (see pick-up).
- **IMPLEMENT; partial**: the park-time FIONREAD guard was
  (in 735afc0f's tree; `git show HEAD:crates/vane/src/proxy.rs |
  grep -c "parked with UNREAD"`; wait, see the caveat below).
- **TEST; partial**: workspace tests green at 19bf9331 (CI
- **VALIDATE; done for the landed work**: CI + interop green on
  otelkit-2 migration regression (the trace-id loss; documented,
- `d88a08f7`; worker-budget fix (workers=0 shares the core
- `159426a0`; request-body credit fix (the Data-arm rewrite had
- `b19fb3b6`; h2 concurrent streams: the shim pending queue +
- `919e33d9`; loadgen --tls + h2load error capture
- `5ec69d66`; vane-proxy 0.16.0 + zstd advisory register
- `735afc0f`; splice-test drain hardening + the otlp full-payload
- `19bf9331`; the ledger: the TLS-h2 stall traced to client
1. **The checkout-side FIONREAD gate; LOST, must be re-written**
2. **The malformed-probe fd attribution; IN THE WORKING TREE,
1. **Re-apply the checkout gate** (deferred #1); 15 lines, the
2. **Locate the h2 upstream-read re-arm gate**; the actual stall
   held bytes await client credit"; the symmetric upstream-side
   fired on client credit); the resume must re-arm the upstream
   driver errors; the error capture is built in), the kit unpin
1. The take_pooled checkout-side FIONREAD gate (deferred #1) -
`.loop/`; this proof-loop's state (plan, research, handoff).
actor's otelkit-2 regression (the trace-id loss; documented in