# Iteration 1: research phase findings + pending mutation
  `sent=65536 last=true`, `held=0`; no stalled sids in the trace.
  park guard closed every one; zero pool poisoning via the pool.
  each on `ufd=Some(49)` with `unread_on_upstream=39424–57436` -
prevents pool poisoning; which is why malformed dropped from
5608 to ~130-197/run; but the connection close itself races the
- crates/vane/src/proxy.rs; the malformed probe gains
checkout gate needs re-adding (it is small; see the iteration-1
   crates/vane-router/Cargo.toml Cargo.lock`; or re-run the bumps
## RESEARCH COMPLETE: the mechanism pinned (2026-10-27)
   withholds the read while the shim holds; the exact location
   grants +64 KiB of WINDOW_UPDATE; but vane never re-arms the
   (post-fix) or poisoning the pool (pre-fix; the malformed
- crates/vane/src/proxy.rs:1092-1106; the [malformed] probe
  (fd, unread, h2, resp_head_len, prefix); the evidence site
- crates/vane/src/proxy.rs:1097-1111; the Malformed arm
- crates/vane/src/h2_server.rs:640-648; the QBFDBG take probe
- crates/vane/src/h2_server.rs:506-540; queue_body (the
- crates/vane/src/h2_server.rs:545-570; take_held (the
- crates/vane-kernel/src/worker.rs:1044,1148,1163; the h1
- crates/vane/tests/h2_server_protocol_edges.rs:302; the