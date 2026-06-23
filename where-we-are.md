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

### Performance instrumentation
- Distributed stats now include aggregate MPI timing for `MPI_Barrier`,
  `MPI_Allreduce(MIN)`, `MPI_Alltoall` size exchange, and `MPI_Alltoallv`
  payload exchange.
- Distributed stats include Shadow-side timing for remote packet batch encoding,
  decoding, and inbound event injection.
- The existing UDP distributed large-partition stats check verifies that the new
  timing fields are emitted in shard `sim-stats.json` output.

### MPI packet-exchange optimization
- `RemotePacketExchange` backends now report whether they require an external
  post-send synchronization step.
- The MPI backend skips the extra post-send `MPI_Barrier`, since its packet
  exchange already synchronizes all ranks through `MPI_Alltoall` and
  `MPI_Alltoallv`.
- MPI packet exchange no longer serializes empty remote-packet batches; zero-byte
  `MPI_Alltoallv` calls use dummy one-byte buffers for OpenMPI compatibility.
- Packet exchange now records round counts, empty-payload rounds, encoded-byte
  totals, non-empty peer counts, and local execution time to make MPI wait time
  and load imbalance visible in shard stats.
- MPI packet exchange skips the `MPI_Alltoallv` payload collective when the
  preceding size exchange shows that all ranks have zero bytes to send and
  receive for the round.

### Runahead optimization
- Default runahead now uses the smallest latency between distinct configured
  hosts, rather than the raw smallest graph path latency.
- This avoids globally constraining scheduling rounds by graph self-loop edges
  that are only relevant to traffic within one host.
- Self-loop latency still contributes when two distinct hosts are configured on
  the same network node.
- Dynamic runahead no longer shrinks from packets sent back to the same host.

## Verified

```text
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSHADOW_TEST=ON -DSHADOW_USE_MPI=ON -DSHADOW_WERROR=OFF
cmake --build build -j16
mpirun -np 2 build/src/main/shadow --version
mpirun -np 2 build/src/main/shadow -d /tmp/opencode/shadow-mpi-smoke-* --parallelism 1 --progress false --log-level error --distributed-shard-count 2 --use-new-tcp true src/test/udp/udp-distributed.yaml
ctest -L mpi --output-on-failure
cargo test -p shadow-rs --lib core::sim_stats
cargo test -p shadow-rs --lib core::distributed
./setup build --test
./setup test udp-distributed-large-partition --verbose
```

Results:
- MPI-enabled build succeeds.
- Two-rank MPI `--version` smoke test succeeds.
- Two-rank MPI UDP simulation succeeds with cross-shard packet exchange exercised.
- Registered MPI CTests pass: TCP smoke, TCP determinism, UDP smoke, UDP 4-rank
  explicit partitioning, and UDP determinism.
- Performance instrumentation unit tests and distributed stats output checks pass.
- MPI shard stats include nonzero MPI timing totals after `ctest -L mpi`.
- The 4-node `ethlambda` 120s distributed run completes after the packet-exchange
  optimization; MPI barrier calls drop from 7,876 total to 4 total.
- The 64-node `ethlambda` 120s distributed run completes in 894s wall-clock after
  the optimization, compared with the prior 967s baseline.
- A vanilla single-process Shadow run of the same 64-node, 120s `ethlambda`
  scenario completed in 868s wall-clock on the 16-core comparison host.
- Extra metrics plus the empty-`MPI_Alltoallv` skip completed the 64-node run in
  948s wall-clock; the empty-payload skip alone was not enough to improve the
  real workload.
- Manually overriding runahead with `--runahead 12ms` completed the 64-node run
  in 501s wall-clock, showing that the 1ms graph self-loop was the dominant
  scheduling bottleneck.
- The host-pair default runahead change completed the 4-node safety run in 4s
  with 598 packet-exchange rounds per shard.
- The host-pair default runahead change completed the 64-node run in 487s
  wall-clock with 6,215 packet-exchange rounds per shard, faster than the manual
  12ms override and much faster than vanilla Shadow for this scenario.

## Notes

- The local-subprocess distributed CTests from `origin/main` are still available.
- The MPI CTests are registered only in MPI-enabled CMake builds.
- The previous explicit-partition `ethlambda` scenario should continue to use the configured
  `distributed_partition_file`; packet routing now flows through `OutboundRemotePacket.dst_shard`.
- Worker hosts have been synced and rebuilt through the host-pair runahead
  optimization for the latest distributed benchmarks.
- After the host-pair runahead optimization, the next visible MPI bottleneck is
  still `MPI_Alltoall` size exchange and load imbalance between shards.
- The current MPI backend binds OpenMPI exported symbols directly. MPICH portability
  requires replacing the direct OpenMPI FFI with a portable C shim or generated bindings.
