# Distributed Shadow Implementation Notes

## Current Status

Phase 4 (MPI Cluster Backend) is **complete and tested**. The distributed simulation
runs across multiple MPI ranks with cross-shard UDP packet delivery verified
end-to-end.

## Implemented

### Core distributed types (`src/main/core/distributed/mod.rs`)
- `ShardId`, `PartitionMap` (modulo + YAML partition file)
- `SerializedPacket` (deterministic big-endian binary format for UDP/Rust TCP)
- `RemotePacketEvent` (versioned batch encode/decode with SHRB magic)
- `RemotePacketDelivery`, error types
- 22 unit tests

### Synchronizer trait (`src/main/core/distributed/synchronizer.rs`)
- `DistributedSynchronizer` trait: `wait()` + `global_min_next_event()`
- `SingleShardSynchronizer` (no-op default)
- `UnixSocketSynchronizer` (binary "SCTL" protocol over Unix sockets)

### Exchange trait (`src/main/core/distributed/exchange.rs`)
- `RemotePacketExchange` trait: `send()` + `receive()`
- `NoopRemotePacketExchange` (fail-fast on remote packets)
- `InProcessRemotePacketExchange` (Arc-shareable, test backend)
- `UnixSocketRemotePacketExchange` (one socket per shard)
- `DistributedPacketExchangeContext` (temp/external socket directories)
- 12 unit tests

### MPI backend (`src/main/core/distributed/mpi_backend.rs`)
- Feature-gated: `distributed_mpi` (Cargo) ↔ `SHADOW_USE_MPI` (CMake)
- Direct FFI to libmpi (OpenMPI 4.1.x compatible, 64-bit pointer types)
- `MpiSynchronizer`: `MPI_Barrier` + `MPI_Allreduce(MPI_MIN)` on i64 nanos
- `MpiRemotePacketExchange`: `MPI_Alltoall` for size exchange, ordered `MPI_Send`/`MPI_Recv`
- `initialize_mpi()` / `finalize_mpi()` lifecycle
- MPI rank auto-detected; overrides CLI shard_id
- Data directory rewritten to `<base>.shard-N` per rank

### Packet serialization and reconstruction
- `serialize_packet_for_remote()`: `PacketRc` → `SerializedPacket` (UDP + Rust TCP)
- `deserialize_packet_from_remote()`: `SerializedPacket` → `PacketRc` (full UDP/TCP fields)
- `Event::new_packet_with_meta()`: preserves source host/event metadata on receive
- Legacy C TCP rejected with clear error

### Existing code integration
- **configuration.rs**: Hidden `--distributed-shard-*` CLI options
- **sim_config.rs**: Global host IDs, PartitionMap, `use_new_tcp` enforcement
- **worker.rs**: `WorkerShared` distributed state, remote packet staging/routing,
  exchange send/receive, packet reconstruction on receive
- **manager.rs**: DNS from all hosts, local-only host execution, exchange calls
  per scheduling window, global window advancement with runahead
- **controller.rs**: MPI backend auto-selection when shard_count > 1
- **event.rs**: `Event::new_packet_with_meta` for source metadata preservation
- **shadow.rs**: Feature-gated MPI init/finalize, rank → shard_id, data dir rewrite

### Build system
- `SHADOW_USE_MPI` CMake option (default OFF)
- `distributed_mpi` Cargo feature (default OFF)
- `WORKSPACE_FEATURES` variable prevents shim crate from getting MPI feature
- `build.rs` detects feature via `CARGO_FEATURE_DISTRIBUTED_MPI` env var
- CMake links `MPI_C_LIBRARIES` into shadow executable

### End-to-end tests
- `udp-distributed-mpi-shadow` (CTest #46): 2-rank, **PASSES** (0.74s)
- `udp-distributed-mpi-4-shadow` (CTest #47): 4-rank, **PASSES** (0.60s)
- Cross-shard UDP delivery verified: `sendto`/`recvfrom` syscalls
- Both tests use `--oversubscribe` for single-machine execution

## Verification Status

```
cargo check -p shadow-rs --lib                              ✓ compiles
cargo check -p shadow-rs --lib --features distributed_mpi   ✓ compiles
cargo test -p shadow-rs --lib                               186 passed
ctest -R udp-distributed-mpi                                2/2 passed (1.30s)
```

## Next Steps (Phase 5+)

1. TCP distributed tests (requires `experimental.use_new_tcp=true`)
2. Partition file support for explicit host→shard assignment
3. Global dynamic runahead (min used latency across all shards)
4. Multi-machine testing (beyond `--oversubscribe`)
5. Phase 5: Partitioning and lookahead
6. Phase 6: Routing and topology scalability
7. Phase 7: Ethereum direct-execution harness
