# Project Proposal: Vane
**A Deterministic, Zero-Copy L4/L7 Reverse Proxy & Sidecar Gateway in Rust**

---

## 1. Executive Summary

**Vane** is a high-throughput, deterministic-latency L4/L7 reverse proxy, edge gateway, and micro-sidecar written in Rust. 

Modern distributed architectures are caught between two sub-optimal extremes:
1. **Ergonomic but High-Jitter Gateways:** Gateways like Traefik offer seamless dynamic discovery and developer ergonomics, but suffer from garbage collection (GC) latency spikes and high memory overhead under massive connection pools.
2. **Fast but Inflexible/Heavyweight Proxies:** Proxies like NGINX and Envoy provide raw speed or extensive mesh capabilities, but struggle with reload worker churn, dynamic module complexity, or extreme memory and vtable overhead in sidecar deployments. Pingora offers a high-performance framework, but lacks an out-of-the-box, dynamically reconfigurable runtime.

Vane closes this gap by combining **share-nothing thread-per-core parallelism**, an **`io_uring` event engine with fixed buffer pools**, **Epoch-Based Reclamation (EBR) dynamic routing**, and a **POSIX Shared-Memory (SHM) IPC ring** for zero-copy local service-to-service communication and seamless binary hot upgrades.

---

## 2. Problem Statement & Competitive Analysis

| Architectural Challenge | NGINX | Traefik | Envoy | Pingora | **Vane** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrency Model** | Multi-process (`fork`) | Goroutines (M:N) | Multi-threaded Worker | Async Multi-thread (Tokio) | **Thread-per-Core (Pinned, Share-Nothing)** |
| **Dynamic Configuration** | Reload (`SIGHUP`) creates worker storm | Dynamic in-memory (Go channels) | Dynamic (xDS Protobuf over gRPC) | Manual code implementation | **Lock-Free RCU / EBR Radix Trie** |
| **Filter Execution** | C Modules / Lua runtime | Go (Yaegi) / Wasm | Dynamic Virtual Dispatch (`vtable`) | Rust code composition | **Monomorphized Static Pipelines + Wasm** |
| **Local Sidecar Transport** | Loopback TCP / UDS | Loopback TCP | Loopback TCP / UDS | Loopback TCP / UDS | **Zero-Copy POSIX SHM Circular Rings** |
| **P99.99 Tail Latency** | Low, but spikes on reload | Medium/High (GC pause dependent) | Low/Medium (TCMalloc/vtable churn) | Low | **Ultra-Low & Deterministic (No GC, No Locks)** |
| **Memory Footprint** | ~15–30 MB | ~60–200 MB | ~80–300 MB | Variable (Library) | **~8–15 MB Baseline** |

### Key Bottlenecks in Existing Solutions
1. **Dynamic Reconfiguration Penalties:** NGINX requires reloading processes to update complex configurations, which causes CPU spikes and drops long-lived WebSockets or streaming connections. 
2. **Goroutine & GC Interference:** Go-based proxies experience stop-the-world (STW) GC pauses and runtime scheduling contention when handling hundreds of thousands of concurrent multiplexed streams.
3. **The "Envoy Tax" in Service Meshes:** Envoy's heavy reliance on Protobuf object trees, dynamic dispatch (`vtable` indirection at every filter stage), and `std::shared_ptr` atomic ref-counting leads to high CPU cache invalidation and large per-container memory footprints.

---

## 3. Core Architectural Pillars

```
                     ┌──────────────────────────────────────────────┐
                     │          Control Plane (Discovery)           │
                     │   K8s Gateway API / Dynamic File / ACME      │
                     └──────────────────────┬───────────────────────┘
                                            │ EBR Atomic Pointer Swap
                     ┌──────────────────────▼───────────────────────┐
                     │         Shared Memory Global State           │
                     │  - Zero-Copy Dynamic Routing Radix Trie      │
                     │  - TLS Session Ticket / Health State Cache   │
                     └──────────────────────┬───────────────────────┘
                                            │
        ┌───────────────────────────────────┼───────────────────────────────────┐
        ▼                                   ▼                                   ▼
┌──────────────────────┐         ┌──────────────────────┐            ┌──────────────────────┐
│  Worker Thread 0     │         │  Worker Thread 1     │            │  Worker Thread N     │
│  (Pinned CPU Core 0) │         │  (Pinned CPU Core 1) │            │  (Pinned CPU Core N) │
├──────────────────────┤         ├──────────────────────┤            ├──────────────────────┤
│ io_uring (SQPOLL)    │         │ io_uring (SQPOLL)    │            │ io_uring (SQPOLL)    │
│ Fixed Buffers        │         │ Fixed Buffers        │            │ Fixed Buffers        │
│ Arena Slab Allocator │         │ Arena Slab Allocator │            │ Arena Slab Allocator │
│ Monomorphized Engine │         │ Monomorphized Engine │            │ Monomorphized Engine │
└──────────┬───────────┘         └──────────┬───────────┘            └──────────┬───────────┘
           │                                │                                   │
           └────────────────────────────────┼───────────────────────────────────┘
                                            │
                             ┌──────────────▼──────────────┐
                             │    POSIX SHM Ring Buffers   │
                             │  (Local Sidecar IPC Bypass) │
                             └─────────────────────────────┘
```

### 3.1. Thread-per-Core Concurrency with `io_uring`
* **Core Pinning:** Each worker thread is strictly pinned to a dedicated physical CPU core using `core_affinity`.
* **Zero-Syscall Hot Paths:** Workers run independent `io_uring` instances configured with `IORING_SETUP_SQPOLL` (submission queue polling) and `IORING_REGISTER_BUFFERS` / `IORING_REGISTER_FILES`. This allows continuous request ingestion and response transmission without entering kernel space via system calls.
* **Share-Nothing State:** Workers allocate connection state from thread-local slab/arena allocators, eliminating cross-thread memory allocation locks and L1/L2 cacheline bouncing.

### 3.2. Lock-Free Dynamic Routing via Epoch-Based Reclamation (EBR)
* **Zero Contention Reads:** Routing tables are stored in a high-performance Radix Trie. Workers read the active table inside an epoch-pinned reference (`crossbeam-epoch`).
* **Non-Blocking Dynamic Swaps:** The control plane applies mutations (e.g., Kubernetes Gateway API updates, dynamic upstream scaling) to an isolated clone of the trie, then atomically swaps the root pointer. The previous routing table is automatically reclaimed once all active requests in that epoch terminate.

### 3.3. Monomorphized Filter Pipelines & SIMD Parsing
* **Compile-Time Static Composition:** Instead of Envoy’s dynamic vtables (`Http::StreamFilter`), Vane constructs its internal request pipeline via Rust traits:
  ```rust
  pub trait Filter {
      fn on_request(&self, ctx: &mut Context, req: &mut Request) -> FilterResult;
  }
  
  // Fully inlined at compile-time into a single continuous instruction stream
  pub struct Pipeline<Auth, RateLimit, Router> { ... }
  ```
* **SIMD-Accelerated Parsing:** HTTP/1.x headers are vectorized via AVX2/NEON instructions (`httparse`), enabling header decoding in single-digit nanoseconds with zero heap allocation.

### 3.4. POSIX SHM Ring Buffers (Sidecar Bypass & Zero-Downtime Upgrades)
* **Local Microservice Bypass:** In sidecar mode, co-located microservices with an embedded Vane client SDK communicate directly via POSIX shared-memory circular queues (`#[repr(align(64))]` cacheline-padded ring buffers). This bypasses the Linux loopback TCP/IP stack, slashing pod-to-pod latency from ~1.5ms to < 20 microseconds.
* **Zero-Downtime Hot Handover:** When updating the Vane binary, the running process maps active connection descriptors and routing states into an SHM segment. The new binary mounts the segment, claims the listening sockets, and drains connections with zero packet loss and zero TCP resets.

---

## 4. Extension & Plugin Architecture

To support third-party dynamic plugins without sacrificing security or core speed:

```
┌────────────────────────────────────────────────────────┐
│                   Vane Core Worker                     │
├──────────────────────────┬─────────────────────────────┤
│ Native Static Pipeline   │ WebAssembly (WASM) Sandbox  │
│ (Zero overhead, inlined) │ (Isolated Wasmtime Runtime) │
│ - Core TLS / Routing     │ - Custom Auth Plugins       │
│ - Rate Limiting          │ - Dynamic Body Transforms   │
└──────────────────────────┴─────────────────────────────┘
```

1. **Native Rust ABI (In-Tree Extensions):** High-throughput, critical paths are compiled directly into the binary with full compiler optimization and inlining.
2. **WebAssembly Sandboxing (Out-of-Tree Extensions):** Embeds `wasmtime` / `extism` for dynamic, multi-language user plugins (Go, Rust, TypeScript, C++). Data exchange between Vane and the Wasm sandbox uses pre-mapped linear memory slices to minimize serialization overhead.

---

## 5. Technical Specifications & Target Metrics

### Performance Targets
* **P99 Tail Latency:** $< 250\,\mu\text{s}$ at $100\text{k}$ concurrent HTTP requests.
* **Throughput:** $\ge 2.5\times$ RPS compared to NGINX under identical hardware constraints.
* **Local IPC Latency (SHM Mode):** $< 30\,\mu\text{s}$ end-to-end request-response cycle.
* **Memory Footprint:** $\le 12\,\text{MB}$ resident set size (RSS) per sidecar instance.
* **Configuration Swap Latency:** $< 1\,\text{ms}$ to apply 10,000 route updates across all workers without connection drops.

### Technology Stack
* **Language:** Rust (2024 Edition, strict `#![deny(unsafe_op_in_unsafe_fn)]`)
* **I/O Engine:** `io-uring` (with fallback to `polling`/`mio` on non-Linux kernels)
* **Crypto / TLS:** `aws-lc-rs` or `rustls` (Hardware-accelerated AES-NI/AVX-512)
* **Concurrency:** `crossbeam-epoch`, `crossbeam-channel`, custom cacheline-padded SPSC queues
* **Memory Management:** Thread-local Slab Allocators, `mimalloc` global allocator fallback
* **Serialization:** `rkyv` / `flatbuffers` (Zero-copy control plane deserialization)

---

## 6. Implementation Roadmap

```
                    PROJECT MILESTONES GANTT
┌───────────────────────────────────────────────────────────────┐
│ Phase 0: L4 Engine & io_uring Core   [ Weeks 1 - 6 ]          │
├───────────────────────────────────────────────────────────────┤
│ Phase 1: L7 HTTP/1.1 & Static Pipe   [ Weeks 7 - 12 ]         │
├───────────────────────────────────────────────────────────────┤
│ Phase 2: Dynamic RCU & K8s Gateway   [ Weeks 13 - 18 ]        │
├───────────────────────────────────────────────────────────────┤
│ Phase 3: SHM Ring IPC & Hot Upgrade  [ Weeks 19 - 24 ]        │
├───────────────────────────────────────────────────────────────┤
│ Phase 4: HTTP/2, TLS & Wasm Sandbox  [ Weeks 25 - 30 ]        │
├───────────────────────────────────────────────────────────────┤
│ Phase 5: Production Hardening & Chaos [ Weeks 31 - 36 ]        │
└───────────────────────────────────────────────────────────────┘
```

### Phase 0: Core Network Engine (L4 Foundation)
- [ ] Implement pinned thread-per-core event loop with `io_uring` and `SQPOLL`.
- [ ] Implement fixed buffer registration pools (`IORING_REGISTER_BUFFERS`).
- [ ] Build automated regression and latency benchmarking suite against NGINX stream and HAProxy.

### Phase 1: L7 Engine & Monomorphized Pipelines
- [ ] Integrate SIMD-based zero-allocation HTTP/1.1 parser.
- [ ] Implement compile-time static trait filter pipelines.
- [ ] Implement thread-local connection arena allocators.

### Phase 2: Control Plane & Dynamic Routing
- [ ] Build lock-free Radix Trie using `crossbeam-epoch`.
- [ ] Implement native Kubernetes Gateway API Controller and dynamic file watchers.
- [ ] Integrate automated ACME (Let's Encrypt) TLS certificate management.

### Phase 3: SHM Transport & Zero-Downtime State Handover
- [ ] Implement POSIX shared-memory circular queue for local service bypass.
- [ ] Build dynamic socket descriptor transfer and state persistence for seamless binary hot reloads.
- [ ] Develop micro-client SDKs (Rust / C++) for direct SHM sidecar communication.

### Phase 4: Modern Protocols & Sandboxing
- [ ] Integrate `rustls` / `aws-lc-rs` with lock-free TLS session ticket sharing.
- [ ] Add HTTP/2 multiplexing and stream management.
- [ ] Embed `wasmtime` runtime for dynamic user-defined middleware.

### Phase 5: Production Hardening & Chaos Validation
- [ ] Execute formal security audits and fuzz testing across the HTTP parser and SHM boundaries.
- [ ] Run Jepsen-style chaos testing to validate configuration consistency under network partitions.
- [ ] Publish production deployment charts (Helm, Kubernetes Operator, Docker Compose).

---

## 7. Risk Analysis & Mitigation Strategies

1. **Kernel Compatibility (`io_uring` Availability):**
   * *Risk:* Enterprise environments frequently run older LTS Linux kernels (e.g., Enterprise Linux distributions with restricted `io_uring` system call access).
   * *Mitigation:* Abstract the transport engine behind an `AsyncEngine` trait. Provide an optimized `epoll`/`kqueue` fallback engine using `mio` when `io_uring` is disabled or unsupported.

2. **HTTP/2 & QUIC State Complexity:**
   * *Risk:* Maintaining custom HTTP/2 flow-control and QUIC/HTTP/3 state machines in a strict share-nothing model can increase code complexity.
   * *Mitigation:* Integrate battle-tested protocol crates (`s2n-quic` or `quiche` for QUIC, `h2` for HTTP/2) and isolate their state machines inside worker thread boundaries.

3. **Memory Safety in SHM Boundaries:**
   * *Risk:* Multi-process shared memory manipulation requires `unsafe` Rust for raw pointer operations and synchronization.
   * *Mitigation:* Encapsulate all shared-memory interactions behind a safe, rigorously validated ring buffer abstraction. Validate correctness with **Miri**, **Loom** (concurrency permutation testing), and AddressSanitizer/ThreadSanitizer (ASan/TSan).

---

## 8. Conclusion

**Vane** reimagines the modern reverse proxy by replacing legacy process-based models and garbage-collected runtimes with a deterministic, thread-per-core, zero-copy architecture. By eliminating dynamic dispatch, syscall overhead, and TCP loopback hops, Vane delivers microsecond-level latency and minimal resource consumption while maintaining modern cloud-native dynamic configuration standards.