# Distributed Shadow Implementation Notes

## Current Status

The `worktree-snuggly-seeking-pumpkin` branch has been merged with `origin/main` and
keeps the verified MPI distributed packet exchange fixes.

The merged code uses `origin/main`'s flat distributed module at
`src/main/core/distributed.rs`. The old `src/main/core/distributed/` directory module
was removed during the merge to avoid duplicate Rust module definitions.

## Implemented

### Distributed core
- `ShardId`, `PartitionMap`, `RemotePacketEvent`, `SerializedPacket`, and exchange/control
  types now live in `src/main/core/distributed.rs`.
- `ManagerConfig::from_sim_config()` filters execution to local hosts while preserving DNS
  records for all configured hosts.
- `WorkerShared` stages remote packets as `OutboundRemotePacket { dst_shard, event }`, so
  routing uses the configured partition map rather than host-id modulo.

### MPI backend
- Feature-gated by Cargo feature `distributed_mpi`, enabled from CMake option
  `SHADOW_USE_MPI=ON`.
- Uses direct libmpi FFI and links `MPI_C_LIBRARIES` through CMake.
- `MpiRemotePacketExchange` uses `MPI_Alltoall` for per-rank payload sizes and
  `MPI_Alltoallv` for payload exchange, avoiding the old ordered blocking send/recv
  deadlock.
- `MpiSynchronizer` uses `MPI_Barrier` and `MPI_Allreduce(MPI_MIN)` for global next-event
  synchronization.
- MPI rank/size override `distributed_shard_id` and `distributed_shard_count`, and each rank
  writes to `<data_directory>.shard-N`.
- MPI-launched runs bypass `origin/main`'s local subprocess launcher, preserving one Shadow
  process per MPI rank.

### Build system
- Top-level CMake finds MPI when `SHADOW_USE_MPI=ON`.
- `WORKSPACE_FEATURES` passes `distributed_mpi` only to the workspace/main Rust build.
- The shim remains on common `RUST_FEATURES`, since it does not define `distributed_mpi`.

### MPI CTests
- `add_mpi_shadow_tests()` registers MPI-backed Shadow tests only when
  `SHADOW_USE_MPI=ON`.
- UDP MPI coverage includes a 2-rank smoke test, a 4-rank explicit-partition
  test, and a repeated-run determinism comparison.
- TCP MPI coverage includes a 2-rank smoke test and a repeated-run determinism
  comparison.

## Verified

```text
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSHADOW_TEST=ON -DSHADOW_USE_MPI=ON -DSHADOW_WERROR=OFF
cmake --build build -j16
mpirun -np 2 build/src/main/shadow --version
mpirun -np 2 build/src/main/shadow -d /tmp/opencode/shadow-mpi-smoke-* --parallelism 1 --progress false --log-level error --distributed-shard-count 2 --use-new-tcp true src/test/udp/udp-distributed.yaml
ctest -L mpi --output-on-failure
```

Results:
- MPI-enabled build succeeds.
- Two-rank MPI `--version` smoke test succeeds.
- Two-rank MPI UDP simulation succeeds with cross-shard packet exchange exercised.
- Registered MPI CTests pass: TCP smoke, TCP determinism, UDP smoke, UDP 4-rank
  explicit partitioning, and UDP determinism.

## Notes

- The local-subprocess distributed CTests from `origin/main` are still available.
- The MPI CTests are registered only in MPI-enabled CMake builds.
- The previous explicit-partition `ethlambda` scenario should continue to use the configured
  `distributed_partition_file`; packet routing now flows through `OutboundRemotePacket.dst_shard`.
- Worker hosts still need the merged source rebuilt/synced before rerunning the 4-host
  `ethlambda` simulation.
- The current MPI backend binds OpenMPI exported symbols directly. MPICH portability
  requires replacing the direct OpenMPI FFI with a portable C shim or generated bindings.
