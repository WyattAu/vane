### Vane: Technical Requirements Specification (TRS)

| Domain | ID | Component / Capability | Technical Specification & Implementation Primitives | Metric / Target Standard |
| :--- | :--- | :--- | :--- | :--- |
| **I/O Engine** | `IO-01` | **Primary Event Loop** | `io_uring` with `IORING_SETUP_SQPOLL` & `IORING_FEAT_NODROP`; single ring per worker core. | Zero syscalls in steady-state hot path. |
| | `IO-02` | **Buffer Registration** | Kernel fixed buffers via `IORING_REGISTER_BUFFERS`; page-locked UMEM regions. | Zero page-mapping overhead per I/O event. |
| | `IO-03` | **FD Registration** | Socket descriptor registration via `IORING_REGISTER_FILES`. | $O(1)$ kernel file-table lookup. |
| | `IO-04` | **L4 Passthrough** | Kernel zero-copy pipe via `splice(2)` / `vmsplice(2)` for non-terminated TCP. | Zero user-space data copies. |
| | `IO-05` | **Fallback Transport** | Edge-triggered `epoll(7)` / `kqueue(2)` abstraction layer via `mio`. | Auto-fallback when `io_uring` is disabled. |
| **Threading & Concurrency** | `TH-01` | **Execution Model** | Pinned Thread-per-Core (Share-Nothing / Actor Architecture) via `core_affinity`. | $0\%$ cross-core work-stealing jitter. |
| | `TH-02` | **Inter-Core Messaging** | SPSC lock-free ring buffers with `#[repr(align(64))]` cacheline-padded atomic indices. | $< 15\,\text{ns}$ cross-thread signal latency. |
| | `TH-03` | **Synchronization Constraints** | Global mutexes/rwlocks prohibited in data plane; atomics restricted to `Relaxed`/`Acquire`/`Release`. | Zero lock-contention CPU wait states. |
| **Memory & Allocations** | `MM-01` | **Hot-Path Allocator** | Pre-allocated per-connection Slab/Arena Allocator; returns memory on stream close. | Zero `malloc`/`free` calls per HTTP request. |
| | `MM-02` | **Global Allocator** | `mimalloc` or `jemalloc` configured with transparent huge pages (`MADV_HUGEPAGE`). | $0\%$ heap fragmentation over 72h sustained load. |
| | `MM-03` | **Buffer Slicing** | Lifetime-bounded byte views (`&[u8]`, `bytes::Bytes`) over contiguous frame rings. | Zero buffer cloning during request mutation. |
| **Control Plane & Routing** | `CP-01` | **Dynamic Route Engine** | Compressed Radix Tree / Compact Trie; matching via SIMD-accelerated prefix search. | $< 80\,\text{ns}$ path lookup time ($10^5$ routes). |
| | `CP-02` | **Atomic Route Swap** | Epoch-Based Reclamation (EBR) via `crossbeam-epoch`; lock-free atomic root pointer swap. | Route update applied across cores in $< 1\,\text{ms}$. |
| | `CP-03` | **Discovery Ingestion** | Native K8s Gateway API (v1) controller, Docker Socket watcher, and File provider. | Dynamic reconciliation loop $< 100\,\text{ms}$. |
| | `CP-04` | **Config Deserialization**| Zero-copy in-memory deserialization via `rkyv` or `flatbuffers`. | Zero-allocation configuration parsing. |
| **Protocols & Parsing** | `PR-01` | **HTTP/1.1 Engine** | Vectorized header parsing via `httparse` (AVX2/NEON SIMD intrinsics). | Full header parse in $< 15\,\text{ns}$. |
| | `PR-02` | **HTTP/2 Engine** | State machine handling HPACK compression, stream multiplexing, and flow control. | $\ge 200\text{k}$ concurrent streams per worker. |
| | `PR-03` | **HTTP/3 & QUIC** | UDP GSO/GRO packet batching; user-space congestion control via `s2n-quic` or `quiche`. | Line-rate UDP datagram ingest. |
| | `PR-04` | **TLS Termination** | `aws-lc-rs` / `rustls` (AES-NI / AVX-512 offload); ALPN negotiation (`h2`, `http/1.1`). | TLS handshake $< 1.2\,\text{ms}$ (CPU time). |
| | `PR-05` | **TLS Session Cache** | Lock-free SHM session ticket cache with ChaCha20-Poly1305 ticket encryption. | Zero-lock TLS 1.3 0-RTT resumption. |
| | `PR-06` | **ACME Engine** | In-memory ACME v2 client for automated Let's Encrypt challenge/certificate renewal. | Hot cert update without connection drops. |
| **IPC & Hot Reloading** | `IP-01` | **Sidecar IPC Bypass** | POSIX shared memory (`shm_open`, `mmap`, `memfd_create`) circular ring buffer. | Local pod request-response $< 25\,\mu\text{s}$. |
| | `IP-02` | **Zero-Loss Hot Upgrade**| Listening socket migration via UNIX Domain Socket `SCM_RIGHTS` + SHM state handover. | $0$ dropped TCP packets during binary swap. |
| **Plugin Sandbox** | `PL-01` | **Wasm Runtime** | Embedded `wasmtime` runtime with zero-copy host memory access via `WasmPtr`. | Plugin execution overhead $< 15\,\mu\text{s}$. |
| | `PL-02` | **Native Pipeline** | Compile-time monomorphized trait chains (`Pipeline<A, B, C>`) with inlined execution. | Zero dynamic dispatch (`vtable`) overhead. |
| **Observability** | `OB-01` | **Metrics Engine** | Atomic lock-free counters (`AtomicU64`) aligned to 64-byte boundaries; Prometheus scrape. | $0\,\text{ns}$ telemetry scrape impact on workers. |
| | `OB-02` | **Tracing** | W3C `traceparent` context injection/propagation; non-blocking OpenTelemetry OTLP drain. | Async trace emission via lock-free queue. |
| | `OB-03` | **Logging** | Structured, non-blocking zero-allocation logging via `tracing` crate. | $O(1)$ ring-buffer log write path. |
| **Reliability & Tooling** | `QA-01` | **Memory Safety** | `#![deny(unsafe_op_in_unsafe_fn)]`; strict Miri validation on all pointer/SHM arithmetic. | $0$ Undefined Behavior (UB) flags in Miri. |
| | `QA-02` | **Concurrency Testing** | Model-checking via `loom` for atomic memory ordering (`Acq/Rel`) validation. | Exhaustive state verification. |
| | `QA-03` | **Fuzzing** | Continuous fuzz testing of L7 parsers via `cargo-fuzz` (libFuzzer/AFL++). | $\ge 10^9$ parser execs without panic. |