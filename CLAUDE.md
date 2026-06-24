# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is a **fork of [Shadow](https://github.com/shadow/shadow)** that adds **distributed
(multi-machine) execution via MPI**. Most of the codebase is upstream Shadow; the distributed
backend is the fork's contribution and is where the interesting, fork-specific complexity lives.

## Branch policy (important)

- Active development happens on **`dev`**. Changes are selectively "upstreamed" to **`main`**.
- `where-we-are.md` and `explainer-distributed-shadow.html` are **`dev`-only** — never add or
  update them on `main`. When porting a `dev` change to `main`, bring only the source/`README.md`/
  `docs/` changes (e.g. `git checkout dev -- <files>`), excluding those two.
- `docs/distributed_shadow.md` and `README.md` (with its distributed-fork notice) live on **both** branches.

## Build, test, run

The repo driver is the Python `./setup` script (wraps CMake + Cargo). There is **no `Cargo.toml`
at the repo root** — the Cargo workspace root is `src/Cargo.toml`.

```bash
# Standard (non-MPI) build + tests, installs to ~/.local
./setup build --clean --test
./setup test                      # run all CTest tests
./setup test <regex>              # run tests matching a regex (e.g. ./setup test tcp)
./setup test -- --output-on-failure   # args after -- pass through to ctest
./setup install

# Rust unit tests for the main crate (crate name is `shadow-rs`)
cd src && cargo test -p shadow-rs --lib core::distributed
cd src && cargo test -p shadow-rs --lib core::sim_stats
```

### MPI / distributed build (not exposed via ./setup — use CMake directly)

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSHADOW_TEST=ON -DSHADOW_USE_MPI=ON -DSHADOW_WERROR=OFF
cmake --build build -j$(nproc)        # produces build/src/main/shadow
ctest --test-dir build -L mpi --output-on-failure   # MPI-only CTests (registered only when SHADOW_USE_MPI=ON)
```

Run a distributed simulation (one Shadow process per MPI rank; each rank writes to
`<data-dir>.shard-N`):

```bash
mpirun -np 4 --hostfile hosts build/src/main/shadow \
  --parallelism 16 --use-new-tcp true \
  --distributed-shard-count 4 --distributed-partition-file partition.yaml shadow.yaml
```

`SHADOW_USE_MPI` adds the `distributed_mpi` Cargo feature **only to the main crate** (see
`src/CMakeLists.txt`: `WORKSPACE_FEATURES`); the shim never gets it. The full guide is
`docs/distributed_shadow.md`.

## Architecture

### Hybrid C/Rust, built through CMake

The Cargo workspace (`src/`) is compiled by a CMake `ExternalProject` into static libraries
(`libshadow_rs.a`, `libshadow_shim_helper_rs.a`, …) that are linked with the remaining C code.
Editing Rust still requires going through the CMake build (or `cargo build` inside `src/`) to
produce the `shadow` binary. `RUSTFLAGS`, features, and the debug/release profile are all set by
CMake, not by hand.

### How a simulation executes (single-process)

Shadow is a discrete-event simulator that runs **real, unmodified application binaries** as native
Linux processes, co-opted into the simulation by intercepting their syscalls via an `LD_PRELOAD`
shim (`src/lib/shim`, with shared types in `src/lib/shadow-shim-helper-rs`). The control hierarchy:

- **`core/controller.rs`** — top-level driver; owns the simulation and (in distributed mode) the
  `DistributedSynchronizer`. `manager_finished_current_round()` computes the next execution window.
- **`core/manager.rs`** — per-process engine. Runs the scheduling loop: execute all local hosts in
  parallel up to `window_end`, send/receive remote packets, reduce the minimum next-event time, ask
  the controller for the next window.
- **`core/worker.rs`** — per-thread. Executes a host's events; `send_packet()` routes packets
  (local vs. cross-shard) and applies the delivery-time clamp. `WorkerShared` holds cross-thread state.
- **`host/host.rs`, `host/process.rs`, `host/managed_thread.rs`** — a simulated host, its managed
  processes/threads, sockets, and per-host event queue.
- **`core/work/event.rs`** — the `Event` total ordering that makes the simulation deterministic.

### Scheduling rounds & runahead (key to performance *and* the distributed design)

Time advances in windows `[start, start + runahead)`. Within a window all hosts run in parallel; a
window is safe only if it is ≤ the smallest cross-host latency, so any packet sent in the window is
delivered in a *future* window. The runahead value is **derived from the topology at startup**
(`network/graph/mod.rs`: `get_smallest_latency_ns_between_hosts` vs `get_smallest_latency_ns`),
never hardcoded. The `core/runahead.rs` `Runahead` type holds it. The fork adds
`experimental.use_host_pair_runahead` (default `true`): exclude self-loop (loopback) edges → larger
window → ~12× fewer rounds; set `false` to include self-loops and match upstream vanilla's window
bit-for-bit.

### Distributed backend (fork-specific), `core/distributed.rs`

Each MPI rank owns a **shard** = a partition of the hosts (`PartitionMap`, `ShardId`). A packet
whose destination host is on another shard is serialized into a `RemotePacketEvent` (carrying the
exact `deliver_time` plus `src_host_id`/`src_host_event_id` so ordering is preserved) and staged by
destination shard. Per round, the `RemotePacketExchange` backend moves these between ranks; the MPI
backend (`mpi_backend` mod, gated by `distributed_mpi`) uses `MPI_Alltoall` (sizes) + `MPI_Alltoallv`
(payloads), and `MpiSynchronizer` uses `MPI_Allreduce(MIN)` for the global next-event time. The
static runahead floor is computed over the **global** host list, so every rank derives the same
window. Non-MPI `RemotePacketExchange` impls also exist (`Noop`, `InProcess`, `UnixSocket`) for
testing.

### Determinism is the product — and its known breakers

Repeated runs with the same binary/config/seed/partition must produce byte-identical output (logs,
metrics, per-process stdout; managed-process PIDs are deterministic, starting at 1000). The
distributed determinism CTests follow an `*-determinism-a` / `*-determinism-b` / `*-compare`
pattern (see `src/test/{tcp,udp}/CMakeLists.txt`, `add_mpi_shadow_tests` macro in
`src/test/CMakeLists.txt`). These settings **break determinism** and must be off for reproducible
runs (verified on a 16-node devnet that distributed == single-process == upstream vanilla, byte-for-byte):

- `experimental.native_preemption_enabled: true` — preempts long pure-CPU guest code on *real* CPU
  time (a stock Shadow option; the most common cause of "distributed looks nondeterministic").
- `experimental.model_unblocked_syscall_latency: true` — adds run-to-run timestamp drift.
- A multi-threaded guest async runtime (e.g. default `#[tokio::main]`) — build the application
  single-threaded for use under Shadow.

### Hard constraints in distributed mode

- `experimental.use_new_tcp: true` is **required** for `distributed_shard_count > 1` (legacy C TCP
  packets cannot be serialized across shards) — enforced at config validation.
- `use_dynamic_runahead` is **rejected** with `distributed_shard_count > 1` (a per-shard shrinking
  window would diverge across ranks and break cross-shard ordering).
- Absolute paths for binaries/configs/genesis must be identical on every node.
