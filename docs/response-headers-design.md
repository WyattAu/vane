# Response-headers stage: design proposal

Proposed in the SIS hosting research loop (round 7, iteration 61) as
work item 1 for the docker01 edge migration (SIS backlog #19): the
platform's per-tenant security headers (`Strict-Transport-Security`,
`X-Content-Type-Options`, `Referrer-Policy`, `Server` suppression -
OWASP OSHP universal subset) have no configuration surface in vane
today. Everything else the platform's Caddyfile does, vane already
does.

## Why the seam already exists

The proxy already performs per-route RESPONSE-header injection for
CORS:

- `Conn.cors_headers: Vec<(&'static str, String)>` (proxy.rs ~315)
  carries the route's response headers for the live connection.
- `CorsAction::Actual(headers)` populates it after the request-side
  pipeline evaluates the route (proxy.rs ~2890).
- Every relayed-response path injects them by rewriting the head:
  `crate::cors::inject_into_head(&mut head, &cors)` at the gzip arm,
  the h2 arm, the alt-svc arm, and the plain arm (proxy.rs ~1100-1200).
- Locally-answered responses hardcode `Server: vane` and take an
  explicit headers array (`write_full(..., &[], ...)` in
  `respond_full`, proxy.rs ~1726).

## Proposal

Generalize CORS's slot into a route-level response policy. No new
stage in the filter pipeline (filters are request-side by design);
the policy rides the same path CORS uses.

1. **Config** (`vane-kernel`):

   ```toml
   [[routes]]
   pattern = "/*rest"
   cluster = "up"

   [routes.response_headers]
   add    = { "Strict-Transport-Security" = "max-age=31536000",
              "X-Content-Type-Options" = "nosniff",
              "Referrer-Policy" = "strict-origin-when-cross-origin" }
   remove = ["Server"]
   ```

   Unknown keys are startup errors, per the config contract. `remove`
   suppresses a header from the upstream head (case-insensitive name
   match, all occurrences) and from the local-answer baseline.

2. **Kernel route struct**: `response_headers: ResponseHeaders`
   (`add: Vec<(String, String)>`, `remove: Vec<String>`), parsed with
   the same validation pass as `[routes.cors]`.

3. **Proxy wiring**: alongside `CorsAction::Actual`, stage the route's
   response policy on the `Conn` (one more field beside
   `cors_headers`). At each `inject_into_head` call site, apply the
   policy after CORS: removes first (delete header lines), adds second
   (append `Name: value\r\n` before the terminal CRLF). The head is
   already an owned, mutable `Vec<u8>` on every branch that injects -
   the zero-copy relay path (no cors, no gzip, no alt-svc) would need
   the same owned-copy treatment CORS forces today; acceptable, and
   only when a route actually declares a policy.

4. **Local answers** (`respond_full`): accept the route's add/remove
   instead of the hardcoded `Server: vane` + empty extras, so 404/429/
   502 answers carry the same policy as relayed ones (HSTS must be
   consistent across every answer on a hostname or browsers see a
   mixed signal).

5. **Tests** (mirroring the cors test layout): head-rewrite unit tests
   (add/remove/both/absent), an e2e asserting the three platform
   headers on a proxied 200 and on a locally-answered 404, and a
   config-validation test (unknown key rejected).

## Non-goals

- Per-header conditional logic (only set-if-absent vs overwrite) -
  add overwrites upstream occurrences of the same name; document it.
- Response-body filters, per-route compression policy changes.

## SIS acceptance for backlog #19

With this stage shipped, the staging evaluation (SIS iter 36's
docker02) proceeds: route-parity vs the Caddyfile, then the GCRA/
adaptive-concurrency soak. Caddy stays on docker01 until vane passes
staging with headers solved.
