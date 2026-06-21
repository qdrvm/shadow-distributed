# Distributed Shadow Plan

## Goal

Build a distributed Shadow mode that preserves Shadow's core value: deterministic, repeatable, discrete-event execution of unmodified Linux application binaries through the existing shim, syscall emulation, simulated sockets, and conservative scheduler.

The target use case is large P2P experiments such as Ethereum networks using real consensus-layer and execution-layer binaries across multiple physical machines on a local network. The design must not require modifying CL/EL binaries. It may require generated configs, per-node data directories, environment variables, and orchestration around the binaries.

## Bottom Line

Use a federated conservative design first. Do not replace Shadow's network model with ns-3/INET and do not start with optimistic Time Warp.

Each shard is a normal Shadow simulator process that owns a subset of virtual hosts and all mutable state for those hosts: managed processes, shim shared memory, descriptors, sockets, TCP/UDP state, timers, event queues, RNG state, and host-local filesystem output. Shards exchange only timestamped future packet-arrival events. A coordinator or MPI collective computes the next safe execution window from all shards' local next-event times and remote packet arrivals.

For a credible 1M Ethereum-node target, distributed direct execution is necessary but not sufficient. A million fully direct-executed CL+EL nodes is likely tens of TB of RAM and millions of Linux processes. The long-term architecture must combine distributed Shadow for a hot set of real nodes with a multi-resolution cold-tail model.

## Source Findings That Shape The Plan

- `src/main/core/controller.rs` already has the correct conceptual hook: `Controller::manager_finished_current_round` has a TODO for blocking until multiple managers finish the current round.
- `src/main/core/manager.rs` runs a fixed scheduling loop over windows. Each round executes all local hosts until `window_end`, gathers per-thread `next_event_time`, and asks the controller for the next window.
- `src/main/core/runahead.rs` computes the current safe runahead from the minimum possible or used latency. A distributed controller must compute a global runahead, not independent per-shard runaheads.
- `src/main/core/worker.rs` has the hot packet-send seam. `Worker::send_packet` resolves the destination host, applies packet loss, computes latency, sets `deliver_time`, updates next-event time, and pushes a packet event to the destination host queue. The in-file TODO explicitly says this is where remote-manager delivery must change.
- `src/main/core/work/event.rs` orders packet events by source host id and source event id. Distributed mode must preserve that ordering by serializing those metadata fields instead of creating new ordering metadata on the receiving shard.
- `src/main/core/work/event_queue.rs` enforces monotonic event time per host. Remote arrivals must be delivered before the destination shard starts the window that can pop them.
- `src/main/host/host.rs` keeps per-host event queues and delivers packet events by pushing the packet into the host router, then notifying inbound relay/bandwidth logic. This means cross-shard packets can arrive as the same logical `EventData::Packet` as local cross-host packets.
- `src/main/network/relay/mod.rs` applies host upload and download bandwidth around local packet devices. Sending a remote event after the outbound relay and delivering it before the inbound relay preserves current bandwidth semantics.
- `src/main/network/packet.rs` supports UDP and Rust TCP packet construction cleanly. Legacy C TCP packets are represented as mutable `Data::LegacyTcp`; distributed serialization must either support this representation or initially require `experimental.use_new_tcp=true` for distributed TCP tests.
- `src/main/core/sim_config.rs` currently assigns host ids inside `Manager::run` by enumerating the host list. Distributed mode must assign global host ids before shard filtering, otherwise host ids and deterministic event ordering will change per shard.
- `src/main/network/graph/mod.rs` currently materializes all routing paths between in-use graph nodes. This is acceptable for tens of thousands only when many hosts share topology nodes. A 1M-node plan needs compressed or lazy routing if every host maps to a distinct graph node.
- `src/lib/shadow-shim-helper-rs/src/ipc.rs` and `shim_shmem.rs` show that the shim/control channel is per managed process/thread and should remain shard-local. Do not send shim IPC across physical machines.

## Non-Goals For The First Implementation

- No process migration between shards. Migrating Linux process state, shim state, memory mappings, file descriptors, and TCP state is too fragile for an MVP.
- No optimistic rollback. Rollback would include managed process memory, shim shared memory, descriptor state, socket buffers, epoll state, signals, timers, filesystem effects, and RNG state.
- No external high-fidelity network simulator on the hot path. Maintainer discussions warn that this would likely disable runahead or require global locking.
- No promise of 1M fully direct-executed nodes. The plan supports distributed direct execution first, then adds aggregation/surrogates for the cold tail.
- No binary modifications to Ethereum clients. Optional LD_PRELOAD acceleration or crypto stubbing can be a separate opt-in experiment, not a requirement.

## Required Invariants

1. One authoritative owner for each virtual host.
2. Mutable process, socket, descriptor, TCP, timer, epoll, futex, memory-manager, RNG, and shim state never crosses shards.
3. Shards exchange logical future events, not raw Ethernet frames and not shim/syscall messages.
4. A remote packet event is delivered only at or after its computed `deliver_time` and only after the global barrier has made that time safe.
5. Packet loss and latency are decided on the source shard, using the source host RNG and the replicated routing summary.
6. Event ordering uses global `HostId` plus source event id exactly like current packet events.
7. Repeated runs with the same seed, config, partition map, and shard count produce identical deterministic logs and metrics.
8. Single-shard distributed mode must be behaviorally equivalent to current Shadow.

## Target Architecture

```
                 +-----------------------------+
                 | Global window coordinator   |
                 | - partition map             |
                 | - global runahead           |
                 | - window barrier            |
                 | - deterministic metrics     |
                 +--------------+--------------+
                                |
                 batched timestamped packet events
                                |
        +-----------------------+-----------------------+
        |                       |                       |
+-------+--------+      +-------+--------+      +-------+--------+
| Shadow shard A |      | Shadow shard B |      | Shadow shard C |
| - local hosts  |      | - local hosts  |      | - local hosts  |
| - workers      |      | - workers      |      | - workers      |
| - sockets/TCP  |      | - sockets/TCP  |      | - sockets/TCP  |
| - shim IPC     |      | - shim IPC     |      | - shim IPC     |
| - event queues |      | - event queues |      | - event queues |
+----------------+      +----------------+      +----------------+
```

The first production backend should use one OS process per shard and one shard per MPI rank. Even on one physical host, prefer multiple Shadow processes over multiple managers in one process because current Shadow uses global worker state such as `WORKER_SHARED`.

## Window Protocol

Current single-manager behavior should be generalized, not replaced.

For each window `[window_start, window_end)`:

1. Coordinator releases the window to all shards.
2. Each shard runs current `Manager` scheduling locally for owned hosts until `window_end`.
3. Local-destination packets are pushed to local host event queues as today.
4. Remote-destination packets are serialized as `RemotePacketEvent` and buffered by destination shard id.
5. Each shard reports `local_min_next_event_time`, `min_used_latency_for_dynamic_runahead`, outbound remote batches, and metrics.
6. Event bus delivers all remote batches to destination shards before the next window is released.
7. Coordinator computes `global_next_start = min(all local_min_next_event_time, all remote deliver_time, simulation_end)`.
8. Coordinator computes `global_runahead` from the current global runahead policy.
9. Coordinator releases the next window `[global_next_start, min(global_next_start + global_runahead, simulation_end))`.

For MVP correctness, use one global window shared by all shards. Later optimization can use per-edge lookahead, null messages, and shard pairs with independent safe times.

## Core Data Structures

Add small, explicit distributed types. Keep them outside shim shared memory.

```rust
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ShardId(pub u32);

pub struct PartitionMap {
    pub host_to_shard: HashMap<HostId, ShardId>,
}

pub struct RemotePacketEvent {
    pub deliver_time: EmulatedTime,
    pub src_host_id: HostId,
    pub src_host_event_id: u64,
    pub dst_host_id: HostId,
    pub packet: SerializedPacket,
}
```

`SerializedPacket` should be deterministic and versioned. Avoid relying on Rust type layout. Use an explicit byte format with big-endian integer fields.

Start with:

- UDP: source IP/port, destination IP/port, priority, payload bytes.
- Rust TCP: source/destination IP/port, flags, seq, ack, window, window scale, SACK ranges, timestamp fields, priority, payload chunks.
- Legacy TCP: do not support in distributed mode; require `experimental.use_new_tcp=true` and fail fast if legacy C TCP is selected.

## Phase 0: Baseline And Safety Harness

Deliverables:

- Add a small set of deterministic two-host and three-host baseline configs for UDP, TCP, latency, packet loss, and bandwidth.
- Capture expected packet counts, pcap hashes where stable, process stdout/stderr hashes, and deterministic strace hashes.
- Add a debug-only per-window digest of local event queues and remote batches.

Acceptance:

- Existing single-host Shadow tests still pass.
- Baseline runs are deterministic across repeated runs with the same seed.
- `experimental.runahead=1ns`, default runahead, and dynamic runahead are covered by at least one baseline.

## Phase 1: Refactor For Global Host Identity And Controller Extensibility

Deliverables:

- Move global host id assignment from `Manager::run` into `SimConfig` or a new `HostDirectory`, before any shard filtering.
- Represent each host as `{host_id, host_info, shard_id}` while preserving hostname-sorted determinism.
- Split `ManagerConfig` inputs into global read-only metadata and local host build list.
- Build global DNS/IP/routing metadata from all hosts, but instantiate only local shard-owned `Host` objects.
- Change `Manager` to depend on the `SimController` trait or a generic controller rather than the concrete `Controller` type.
- Add `PartitionMap` plumbing with a default single-shard map.

Acceptance:

- A single-shard run produces the same host ids, logs, and test results as current Shadow.
- Unit tests verify stable host ids when the same config is partitioned into 1, 2, or N shards.
- `./setup build --test` succeeds.
- Relevant targeted tests pass: socket send/recv, determinism, basic time tests.

## Phase 2: Local Federated Mode On One Machine

Deliverables:

- Add `distributed.enabled`, `distributed.shard_id`, `distributed.num_shards`, and `distributed.partition_file` config or CLI options behind experimental naming.
- Add an `EventBus` trait with a local multi-process backend. Use Unix domain sockets or loopback TCP first if that is faster to implement; add shared memory later only if profiling proves it matters.
- Add a `DistributedController` that implements the window protocol with a central coordinator process or rank 0.
- Modify `WorkerShared` to contain local event queues plus the global partition map.
- Modify `Worker::send_packet` to route by destination shard. Local destination uses current path. Remote destination serializes a `RemotePacketEvent`.
- Add `Event::new_packet_with_meta` or equivalent so receiving shards can enqueue packet events with source host id/source event id from the sending shard.
- Deliver inbound remote events before releasing the next window.

Acceptance:

- Run two Shadow shard processes on one physical machine with a partitioned two-host config.
- UDP ping/pong across shards matches single-process timing and output.
- TCP send/recv across shards works with `experimental.use_new_tcp=true`.
- Repeated two-shard runs produce identical deterministic hashes.
- No remote event can be enqueued with time earlier than a host queue's last popped event time.

## Phase 3: Packet Serialization Completeness

Deliverables:

- Implement deterministic serializers/deserializers for UDP and Rust TCP packet data.
- Keep distributed mode gated on `experimental.use_new_tcp=true`; the Rust TCP stack is the supported distributed TCP path.
- Add fuzz/property tests for packet roundtrip serialization.
- Add cross-shard tests for packet priorities, zero-length payloads, multi-chunk TCP payloads, TCP options used by Shadow, and pcap display consistency.

Acceptance:

- Serialized then deserialized packet has equal protocol-visible fields and payload bytes.
- Existing local packet behavior remains unchanged.
- Distributed mode fails with a clear error for any unsupported packet type or TCP backend.

## Phase 4: MPI Cluster Backend

Deliverables:

- Add an MPI `EventBus` backend with one rank per shard.
- Use deterministic collective phases: gather window reports, exchange batch sizes, exchange batch payloads, broadcast next window.
- Keep batch order stable by sorting by destination shard, deliver time, source host id, source event id.
- Add shard-local metrics for serialization time, MPI wait time, sent/received events, sent/received bytes, and barrier idle time.
- Add a launcher wrapper that validates rank count, shard ids, partition file, and output directories.

Acceptance:

- The same two-shard test passes with local backend and MPI backend.
- A four-shard test across at least two physical machines is deterministic over repeated runs.
- MPI backend can detect shard failure and exits the whole experiment cleanly with useful logs.

Concrete design:

- Treat each MPI rank as one Shadow shard process. Do not run the existing parent shard launcher under MPI; the MPI launcher already owns process placement and rank count.
- Rank id maps directly to `distributed_shard_id`; MPI world size maps to `distributed_shard_count`.
- Keep the existing local Unix-socket parent launcher as the non-MPI development backend.
- Add an MPI-specific launch path or wrapper that validates MPI rank count, sets shard id/count from MPI, rewrites the data directory to `<base>.shard-N`, and then runs the normal `Controller` path.
- Split the current distributed control client role behind a small rank-local synchronization trait with two operations: `wait()` for startup/post-send barriers and `global_min_next_event(local_min)` for round advancement.
- Implement the MPI synchronization trait with `MPI_Barrier` for startup/post-send barriers and an `MPI_Allreduce(MIN)` over encoded emulated times for global next-event selection.
- Implement an `MpiRemotePacketExchange` behind the existing `RemotePacketExchange` trait.
- Reuse the current explicit binary packet-batch codec for MPI payloads. Make the codec transport-independent before adding MPI so Unix-socket and MPI backends serialize the same semantic `RemotePacketEvent` format.
- Use deterministic variable-size collectives: each rank groups outbound packets by destination rank, encodes one batch per destination, exchanges byte counts with all ranks, then exchanges payload bytes with `MPI_Alltoallv` or equivalent non-overlapping point-to-point phases ordered by rank id.
- Decode all inbound rank batches locally, concatenate, then sort by delivery time, source host id, source event id, and destination host id before injecting into host queues. This preserves the existing inbound ordering rule even if MPI receive completion order differs.
- On fatal backend/protocol errors, log the rank-local error and abort the whole MPI communicator rather than letting peers block at the next collective.
- Keep the first MPI backend behind hidden/experimental configuration until two-shard and four-shard deterministic runs pass on both one machine and at least two physical machines.
- The Cargo feature is `distributed_mpi`; the matching CMake option is `SHADOW_USE_MPI`. Both are default-off so normal builds do not require MPI headers, `mpicc`, or MPI pkg-config files.

First implementation slice:

1. Extract packet-batch encode/decode helpers from the Unix-socket backend into transport-neutral functions with unit coverage unchanged.
2. Add a `DistributedSynchronizer` trait and adapt the existing Unix control socket to implement it without changing behavior.
3. Add an MPI design-disabled stub or feature-gated module boundary so the controller can select a future MPI synchronizer/exchange without touching the manager loop again.
4. Only then add the actual MPI dependency/backend, because MPI availability and packaging are environment-sensitive.

## Phase 5: Partitioning And Lookahead

Deliverables:

- Build an offline partitioner that consumes the full Shadow config and an optional traffic hint file.
- Default heuristic: keep low-latency/high-traffic communities in the same shard; minimize cross-shard event rate; balance process count and estimated memory.
- Emit a stable `partition_file` mapping hostnames or host ids to shard ids.
- Add metrics that report the shard cut matrix: packets and bytes per source shard/destination shard.
- Implement global dynamic runahead by reducing the minimum used latency across all shards each window.
- Later, add partition-aware lookahead based on minimum cross-shard latency, but keep the global-window implementation as the correctness baseline.

Acceptance:

- Partitioning is deterministic for the same inputs.
- Cross-shard event rate and barrier idle fraction are visible in `sim-stats.json` or a new distributed stats file.
- Synthetic gossip benchmarks show expected degradation as the partition cut gets worse.

## Phase 6: Routing And Topology Scalability

Deliverables:

- Keep current `RoutingInfo` for initial distributed experiments where the number of in-use topology nodes is modest.
- Add a compressed or lazy routing backend before attempting very large host counts with unique topology nodes.
- Support common large-scale topology patterns directly: one-switch, regional complete graph, hierarchical region/AS graph, and hosts sharing an attachment node.
- Cache path properties by `(source_node, destination_node)` with bounded memory and deterministic eviction, or precompute only paths needed by configured host groups.
- Keep packet-loss and latency lookup pure and deterministic on every shard.

Acceptance:

- Memory for routing metadata grows with topology complexity, not with `hosts^2`, for large grouped topologies.
- 100k configured hosts sharing regional topology nodes can parse and start without all-pairs host routing memory blowup.

## Phase 7: Ethereum Direct-Execution Harness

Deliverables:

- Generate Shadow configs and per-node directories for CL+EL pairs on the same Shadow host.
- Run CL and EL as separate unmodified managed processes communicating over loopback HTTP/AuthRPC/Engine API.
- Assign deterministic keys, JWT secrets, ports, ENRs/static peers, genesis files, and data directories.
- Support client matrix entries such as Lighthouse, Prysm, Teku, Nimbus, Lodestar, Geth, Nethermind, Besu, and Erigon as binaries supplied by the user.
- Validate required syscall coverage for selected clients, especially networking, file IO, mmap, futex, epoll, timerfd, signals, random, and process/thread syscalls.
- Disable `native_preemption_enabled` for deterministic runs unless explicitly testing non-deterministic preemption behavior.
- Add compatibility smoke tests with small private Ethereum networks: 4, 16, 64, then 256 nodes.

Acceptance:

- A two-shard Ethereum smoke test with at least one CL+EL client pair per shard reaches stable peer connections and block production or deterministic syncing behavior.
- Same seed and partition produce identical block/import/log-level deterministic markers across repeated runs.
- No Ethereum client binary patching is required.

## Phase 8: Multi-Resolution 1M-Equivalent Experiments

Deliverables:

- Define a hot set of direct-executed nodes: validators, block producers, relays, bridge peers, crawlers, and adversarial or instrumented clients.
- Define cold-tail surrogates that represent many statistically similar peers without one Linux process per peer.
- Keep surrogate interaction at protocol-visible boundaries where possible. For Ethereum this likely means implementing enough devp2p/discv5/libp2p/gossipsub behavior to exchange valid messages with hot nodes, or using lightweight peer-process aggregators that own many virtual identities.
- Add deterministic traffic models for gossip fanout, peer churn, block propagation, transaction gossip, request/response load, and failures.
- Allow the cold-tail model to inject timestamped packet or message events into the same distributed window protocol.

Acceptance:

- A 1M-equivalent experiment documents how many nodes are direct-executed and how many are represented by surrogates.
- Hot-node observations are reproducible under replay.
- Metrics report model error bounds or calibration data against smaller all-direct experiments.

## Phase 9: Checkpointing And Recovery

Deliverables:

- Start with deterministic replay from seed/config/window logs, not live restart.
- Add coordinated window-boundary checkpoints only after distributed execution is stable.
- Evaluate CRIU for managed process checkpoint/restore per shard. Treat this as risky because Shadow also owns shim shared memory, memory-manager mappings, semaphores, host event queues, and descriptor/socket state.
- Checkpoint shard-local Shadow state, event queues, pending remote batches, RNG state, and enough process state to resume.

Acceptance:

- Replay can reproduce a failed run up to a selected window.
- Live checkpoint/restore is optional and not required for the first distributed release.

## Observability Requirements

Add metrics early. Without them, partition and synchronization bottlenecks will be invisible.

Required per-window metrics:

- `window_start`, `window_end`, `global_runahead`.
- Local execute time, barrier wait time, serialization time, transport time, deserialization time.
- Local events executed, local packets, remote packets sent, remote packets received.
- Remote bytes sent/received by shard pair.
- Minimum next local event time and minimum remote arrival time.
- Event-bus queue depth and batch sizes.
- RSS, virtual memory, open file descriptors, managed process count, host count.
- Determinism digest for inbound batches, outbound batches, and final window decision.

Required experiment-level metrics:

- Wall-clock time per simulated minute.
- Memory per direct-executed host and per managed process.
- Barrier idle fraction.
- Cross-shard cut matrix.
- Packet loss/drop counts by reason.
- Shard imbalance: CPU time, event count, memory, and process count.

## Benchmark Ladder

1. Single-shard no-op equivalence: existing tests unchanged.
2. Two-host UDP across two shards: latency, loss, bandwidth, deterministic replay.
3. Two-host TCP across two shards: connect, send/recv, close, retransmit/loss if supported.
4. Multi-host fanout: synthetic gossip with configurable cross-shard cut.
5. 1k, 5k, 10k direct P2P nodes on one machine with multiple shard processes.
6. 10k, 25k, 50k direct P2P nodes across multiple physical machines.
7. Ethereum 4, 16, 64, 256 direct nodes for client compatibility.
8. Ethereum 1k, 5k, 10k direct nodes for resource modeling.
9. 100k direct nodes only if memory/process counts are feasible on the cluster.
10. 1M-equivalent Ethereum using hot direct nodes plus cold-tail surrogates.

Primary acceptance metrics:

- Deterministic replay success.
- Wall-clock per simulated minute.
- Memory per direct node.
- Remote events per second.
- Barrier idle fraction.
- Cross-shard bytes per simulated second.
- Ethereum-specific propagation metrics: block propagation, attestation/subnet message propagation, peer churn convergence, and request/response latency.

## Resource Model For Ethereum

Planning assumptions:

- One Ethereum node usually means at least two managed processes, CL and EL, plus large on-disk state if running realistic clients.
- Shadow currently treats most CPU and disk IO as free in simulated time, but wall-clock CPU and disk still matter.
- BLS/KZG, database work, state sync, and peer discovery can dominate wall-clock time even when simulation time does not advance.
- Public Shadow user data already suggests 5k libp2p nodes can require around 200 GB RAM in one workload, while other 5k gossipsub tests only fit with `use_memory_manager=false` and heavy swap.

Implication:

- 10k to 100k direct-executed lightweight P2P nodes across a cluster is a reasonable distributed Shadow target.
- 1M fully direct-executed CL+EL nodes is not a reasonable first target.
- 1M-equivalent experiments need aggregation, surrogates, or many identities per process.

## Development Order

Recommended first implementation path:

1. Refactor host identity and controller trait with no behavior change.
2. Add local multi-process event bus and static partition maps.
3. Modify packet send path for local-vs-remote destination.
4. Add remote packet event constructor preserving source event metadata.
5. Implement UDP and Rust TCP serialization.
6. Prove deterministic equivalence on one physical host.
7. Add MPI backend.
8. Run multi-machine deterministic tests.
9. Add partitioner and metrics.
10. Add Ethereum harness.
11. Add routing scalability and multi-resolution models.

## Risks And Mitigations

- Risk: Legacy C TCP serialization work delays distributed TCP support. Mitigation: keep distributed mode gated on `experimental.use_new_tcp=true`; the Rust TCP stack is the supported distributed TCP path.
- Risk: Barrier idle time dominates with low-latency cross-shard chatter. Mitigation: partition low-latency communities together, add cut metrics, later add null-message or pairwise lookahead.
- Risk: Global routing metadata grows too large. Mitigation: grouped topology nodes first, lazy/compressed routing before 100k+ unique topology nodes.
- Risk: Ethereum clients hit missing syscalls or nondeterminism. Mitigation: small client compatibility matrix before scaling; deterministic strace mode; no native preemption by default.
- Risk: 1M direct execution is infeasible. Mitigation: plan for hot direct set plus cold-tail surrogate from the start.
- Risk: MPI or network jitter affects wall-clock order. Mitigation: make event ordering depend only on serialized event metadata and window decisions, never receive order.
- Risk: Shard imbalance stalls the whole simulation. Mitigation: partition by measured load, not only host count; no migration in MVP, but allow repartitioning between runs.

## Open Decisions

- Whether the first local backend should be Unix domain sockets, loopback TCP, or shared memory queues.
- Whether MPI is a hard dependency for distributed builds or an optional feature flag.
- How much Ethereum protocol surrogate fidelity is needed for the cold tail: packet-level peer surrogates, protocol-message surrogates, or process-level aggregators.
- What direct-node target defines success before multi-resolution work starts: 25k, 50k, or 100k.
