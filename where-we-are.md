# Distributed Shadow Implementation Notes

## Current Status

Started Phase 1 foundation work from `plan.md`. The code still runs as normal single-process Shadow by default, but it now has the first pieces needed for stable future sharding, outbound remote packet staging, inbound remote packet conversion, transport-independent packet exchange, manager-loop exchange hook wiring, in-process and Unix-socket remote UDP path tests, controlled distributed backend selection, Unix-socket directory lifecycle handling, a parent shard-process launcher, child shard Unix-socket backend selection, parent-owned binary control-socket synchronization, explicit control-server shutdown, global next-window agreement, global-DNS/local-host separation, exchange-backend injection, shareable exchange backends, centralized manager-config construction, a test-only multi-shard manager-config harness, distributed metrics and cut-matrix output, two-, three-, and four-shard smoke coverage, partition-file support, an initial MPI/cluster backend design, and hidden shard configuration.

Implemented so far:

- Host IDs are assigned in `SimConfig` while processing the full configuration, instead of inside `Manager::run`.
- `HostInfo` now stores its global `HostId`.
- `Manager` uses the preassigned `HostInfo::id` when registering DNS and building hosts.
- `Manager` now depends on `dyn SimController` instead of the concrete `Controller` type.
- Added `core::distributed` with `ShardId`, `DEFAULT_SHARD_ID`, and `PartitionMap`.
- Added `PartitionMap::from_host_shards` for constructing explicit host-to-shard mappings in internal tests and future partition loading.
- Added `PartitionMap::by_host_id_modulo` for deterministic host-id-based shard assignment.
- Added `PartitionMapError::ZeroShardCount` validation.
- Added hidden experimental config fields `distributed_shard_id` and `distributed_shard_count`, defaulting to `0` and `1`.
- `SimConfig` now builds its `PartitionMap` from `distributed_shard_count` using stable host-id modulo assignment.
- `SimConfig` validates that `distributed_shard_id < distributed_shard_count` and that the selected shard owns at least one host.
- `SimConfig`, `ManagerConfig`, and `WorkerShared` carry a `PartitionMap`; the default single-shard configuration remains all-local.
- `ManagerConfig` and `WorkerShared` carry the current shard id from the config.
- Added `HostDnsInfo` and `SimConfig::dns_hosts` so global DNS records can stay complete even after future local-host filtering.
- Added `ManagerConfig::dns_hosts`; `Manager::run` now registers DNS from that global list rather than from the executable host list.
- `ManagerConfig::from_sim_config` filters the executable host list by `PartitionMap::is_host_local`. With the default all-local partition this preserves behavior.
- `Worker::send_packet` now explicitly resolves the destination shard before enqueueing a packet event.
- Packet delivery still uses the existing local event queue path when the destination host is local.
- `Worker::send_packet` now assigns source packet-event metadata before the local/remote branch, so both paths use the same source host id and source event id.
- Added `OutboundRemotePacketBuffer`, a thread-safe shard-local staging buffer for future cross-shard packet delivery.
- Non-local packet delivery now serializes a `RemotePacketEvent` and stores it in `WorkerShared::outbound_remote_packets` instead of panicking.
- Added `RemotePacketEvent::into_local_event`, which validates destination locality and converts a received remote packet into a local packet event while preserving source metadata.
- Added `RemotePacketDeliveryError` for unknown destination hosts and non-local destination hosts.
- Added `WorkerShared::push_inbound_remote_packet`, which converts a received remote packet and pushes it into the destination host's local event queue.
- Added `RemotePacketExchange`, a small transport-independent send/receive trait for exchanging remote packet batches.
- `RemotePacketExchange` now requires `Send + Sync`.
- Added a forwarding `RemotePacketExchange` implementation for `Arc<T>`, enabling shared exchange backends across future managers/controllers.
- Added `RemotePacketExchangeError` for backend and delivery failures.
- Added `NoopRemotePacketExchange`, the default single-shard backend. It returns no inbound packets and fails if any outbound remote packet is produced without a real backend.
- Added `InProcessRemotePacketExchange`, a deterministic in-process exchange backend useful for tests and future single-process distributed prototyping.
- Added `UnixSocketRemotePacketExchange`, an initial IPC backend that binds one nonblocking Unix-domain socket per shard and sends binary-encoded remote packet batches to destination shard sockets.
- `UnixSocketRemotePacketExchange` now removes its bound shard socket path on drop.
- Added `DistributedPacketExchangeBackend` with a `UnixSocket` backend selection variant.
- Added `DistributedPacketExchangeContext`, a controlled distributed-only factory that owns either a temporary socket directory or an external socket directory and builds exchange backends for shard processes/tests.
- Added private IPC wire structs for `RemotePacketEvent` and UDP/Rust TCP `SerializedPacket` conversion.
- Added hidden top-level CLI option `--distributed-ipc-socket-dir` for shard child processes. This is not a YAML config option.
- Added `Controller::new_distributed_shard`, which selects the Unix-socket remote packet exchange from the internal socket directory while leaving `Controller::new` on `NoopRemotePacketExchange`.
- Added a parent distributed launcher path in `run_shadow`: when `distributed_shard_count > 1` and no internal socket directory is present, the parent creates a temporary IPC socket directory, spawns one Shadow child process per shard, passes `--distributed-shard-id`, `--distributed-shard-count`, and `--distributed-ipc-socket-dir`, waits for all children, and reports failed shard exits.
- The parent launcher now polls child shard process status and kills remaining shard children on the first shard failure to avoid leaving peers blocked at distributed barriers.
- The parent launcher now passes each child a shard-specific data directory (`<base>.shard-N`) so shard managers do not race to create the same output directory.
- The parent launcher rejects distributed runs that read config from stdin because child shard processes need to re-open the config file independently.
- Child argument construction strips any existing internal distributed shard/socket flags before appending the shard-specific values for each child process.
- Child argument construction also strips existing data-directory flags before appending the shard-specific data directory.
- Added `DistributedControlServer`, a parent-owned Unix-domain control socket in the launcher-owned IPC directory.
- Added `DistributedControlClient`, which child shards use for startup, post-send, and global-next-time synchronization.
- The control protocol uses length-prefixed big-endian binary request/response messages.
- `DistributedControlServer` now owns and joins its background thread through `shutdown()`, wakes the accept loop during cleanup, and removes its socket path.
- The parent launcher now calls `DistributedControlServer::shutdown()` after child shard processes finish and reports server-side errors on otherwise successful runs.
- The parent launcher now reports both the child failure and the control-server failure if both happen during distributed shutdown.
- Shard children now wait on the control socket after binding their Unix packet socket, so peers should not attempt to connect before all shard packet sockets exist.
- Added `SimController::remote_packet_send_complete` and wired `Manager::run` to call it after draining outbound remote packets and before receiving inbound remote packets.
- Distributed shard children implement `remote_packet_send_complete` with the control socket, so all shards send their remote packet batches before any shard receives for the next window.
- `SimController::manager_finished_current_round` now returns `anyhow::Result` so distributed synchronization failures can propagate out of the manager loop.
- Distributed shard children now use the control socket at the end of each round to publish their local `min_next_event_time` and advance using the global minimum next-event time returned by the parent.
- Added `WorkerShared::send_remote_packets`, which drains the outbound remote packet buffer in deterministic order and hands it to a `RemotePacketExchange`.
- Added `WorkerShared::receive_remote_packets`, which receives packets for the current shard, feeds them through `push_inbound_remote_packet`, and returns the minimum received packet delivery time.
- Wired `Manager::run` to call `send_remote_packets` and `receive_remote_packets` after each execution window using `NoopRemotePacketExchange`.
- Moved the remote packet exchange backend into `ManagerConfig::remote_packet_exchange`.
- Added `ManagerConfig::from_sim_config`, which consumes `SimConfig`, filters executable hosts by shard, keeps global DNS records, and injects the exchange backend.
- `Controller::run` injects `NoopRemotePacketExchange` by default.
- `Controller::run` now uses `ManagerConfig::from_sim_config` instead of open-coding shard filtering and manager config construction.
- `Controller::run` now passes `experimental.distributed_shard_id` into `ManagerConfig::from_sim_config`.
- Added `TestDistributedManagerHarness` under `manager.rs` tests, which builds multiple shard `ManagerConfig`s from one logical config with a shared `Arc<InProcessRemotePacketExchange>`.
- `PartitionMap` now derives `Eq` and `PartialEq` so tests can verify that shard configs preserve identical partition maps.
- `Manager::run` now includes received remote packet delivery time in the next-event-time value reported to the controller.
- Added `Event::new_packet_with_meta`, which creates packet events from source metadata assigned elsewhere.
- Added `Event::packet_source_metadata` for inspecting packet-event ordering metadata without consuming event internals.
- Existing `Event::new_packet` now delegates to `Event::new_packet_with_meta` after assigning metadata from the local source host.
- Added `RemotePacketEvent`, the future cross-shard packet arrival envelope.
- Added `SerializedPacket` with UDP and Rust TCP support.
- Added `Packet::is_legacy_tcp()` so distributed serialization can fail explicitly for legacy C TCP packets instead of hitting the existing `ipv4_tcp_header()` panic path.
- Added explicit `PacketSerializationError::LegacyTcp`; legacy C TCP serialization remains unsupported by design for distributed mode.
- Added a unit test for default all-local partition mapping.
- Added a unit test for current-shard locality checks.
- Added a unit test proving same-time packet events order by source host id, then source event id.
- Added a unit test for UDP packet serialization/deserialization round-trip.
- Added a unit test for Rust TCP packet serialization/deserialization round-trip, including flags, SACK ranges, window scale, timestamps, priority, and payload bytes.
- Added a unit test proving `RemotePacketEvent` preserves source host/event metadata.
- Added a unit test proving outbound remote packets drain in deterministic order.
- Added a unit test proving remote packet events convert back into local packet events with preserved source metadata.
- Added a unit test proving remote packet conversion rejects destinations that do not belong to the receiving shard.
- Added test-only in-process `RemotePacketExchange` coverage proving remote packets route to destination shards.
- Added test-only in-process `RemotePacketExchange` coverage proving received remote packets are ordered deterministically.
- Added a unit test proving `NoopRemotePacketExchange` rejects outbound remote packets instead of silently dropping them.
- Added an end-to-end unit test for one UDP packet through outbound staging, in-process exchange, inbound conversion, destination event queue insertion, and event queue pop.
- Added unit tests for deterministic modulo partition construction and zero-shard validation.
- Added a unit test proving an `InProcessRemotePacketExchange` can be shared through `Arc`.
- Added a unit test proving `ManagerConfig::from_sim_config` filters executable hosts to the selected shard while preserving global DNS records.
- Added a unit test proving the test-only multi-shard harness builds shard-specific manager configs while preserving identical global DNS records and partition maps.
- The same harness test proves shard configs share one in-process exchange by sending a `RemotePacketEvent` through one shard config and receiving it through the other.
- Added unit tests proving hidden shard config builds modulo partitions and rejects invalid shard IDs or empty selected shards.
- Added a unit test proving `RemotePacketEvent` round-trips through the binary IPC wire format while preserving UDP addresses, payload, and priority.
- Added a unit test proving `RemotePacketEvent` round-trips through the binary IPC wire format while preserving Rust TCP addresses, header fields, payload, and priority.
- Added a unit test proving IPC wire conversion rejects invalid emulated delivery times.
- Added a unit test proving `UnixSocketRemotePacketExchange` sends UDP remote packet batches between two shard sockets and receives them in deterministic order.
- Added a unit test proving `UnixSocketRemotePacketExchange` rejects receive calls for a shard other than the bound shard.
- Added a unit test proving `DistributedPacketExchangeContext::temporary` builds Unix-socket exchanges and cleans up the temporary socket directory after the exchanges and context are dropped.
- Added a unit test proving `DistributedPacketExchangeContext::external` preserves an externally owned socket directory while exchange drop removes the bound shard socket file.
- Added unit tests proving distributed child argv construction appends shard settings and replaces stale internal distributed flags.
- Added unit tests proving distributed shutdown reports control-server errors after successful child processes and reports both child/control errors when both fail.
- Added a unit test proving the distributed control socket waits for all shards across consecutive rounds.
- Added a unit test proving the distributed control socket returns the global minimum next-event time across shards.
- Added a unit test proving the distributed control server can shut down cleanly before any shard connects.
- Added a unit test proving a shard waiting for a control response is unblocked if another shard disconnects mid-round.
- Added a unit test proving the distributed control server reports malformed binary control requests through `shutdown()`.
- Added a unit test proving the distributed control server reports duplicate shard requests through `shutdown()`.
- Added `src/test/udp/udp-distributed.yaml` and registered `udp-distributed-shadow`, an automated two-shard CTest that exercises cross-shard UDP delivery through the Unix-socket backend.
- Added `src/test/tcp/tcp-distributed.yaml` and registered `tcp-distributed-shadow`, an automated two-shard CTest that exercises cross-shard Rust TCP delivery through the Unix-socket backend with `--use-new-tcp true`.
- Updated the common `add_shadow_tests` CMake macro to remove shard-specific data directories before tests (`<test>.data.shard-*`) so distributed tests are rerunnable.
- Added a unit test proving the parent wait path kills remaining shard children on the first shard failure.
- Added a unit test proving the parent wait path kills a real child that is blocked in the control protocol after another shard fails.
- Added `src/test/distributed/distributed-child-failure.yaml` and registered `distributed-child-failure-shadow`, an automated parent-launcher CTest that exercises real shard-child failure handling.
- Improved the legacy TCP distributed serialization error so it explicitly says distributed TCP currently requires `experimental.use_new_tcp=true`.
- Distributed mode with more than one shard now requires `experimental.use_new_tcp=true` at config validation time.
- Converted distributed control messages and Unix-socket packet batches from JSON to explicit big-endian binary framing.
- Added binary wire magic/version/message-kind headers to distributed control messages and Unix-socket packet batches.
- Added binary framing limits for control message size, packet batch size, packet batch event count, packet payload size, and TCP SACK count.
- Added unit tests proving the binary wire decoder rejects unsupported versions, oversized control messages, excessive packet counts, and excessive packet payload declarations.
- Updated distributed UDP and child-failure CTests to pass `--use-new-tcp true`, matching the stronger distributed-mode gate.
- Added distributed `sim-stats.json` metrics for remote packet counts, remote packet payload bytes, control barrier wait count, and control barrier wait time.
- Added a unit test proving distributed stats recording.
- Added `udp-distributed-3-shadow`, a three-shard CTest that reuses the UDP distributed scenario and exercises parent launch plus Unix-socket IPC with more than two shard children.
- Decided to keep distributed TCP gated on `experimental.use_new_tcp=true`; Rust TCP is the supported distributed TCP path.
- Added a launcher-style test proving parent shutdown reports a real malformed child process that sends an invalid binary control frame.
- Added a Unix-socket packet-transport test proving a real malformed peer batch is rejected by the receive path.
- Added repeated-run distributed UDP determinism coverage comparing normalized managed-process stdout across two two-shard runs.
- Added packet-shape serialization tests for zero-length UDP and zero-length Rust TCP without TCP options.
- Added repeated-run distributed Rust TCP determinism coverage comparing managed client/server stdout across two two-shard runs.
- Added hidden partition-file support through `--distributed-partition-file` / `experimental.distributed_partition_file`, using YAML mappings from hostname to shard id.
- Added partition-file validation for unknown hosts, missing hosts, and shard ids outside `distributed_shard_count`.
- Added shard cut-matrix metrics to distributed `sim-stats.json` using `src->dst` entries with per-cut packet and payload-byte counts.
- Added `udp-distributed-partition-shadow`, a four-host/four-shard CTest using an explicit partition file.
- Confirmed the four-shard partition CTest writes real cut-matrix metrics, including a `0->3` remote UDP packet cut in shard-local `sim-stats.json` output.
- Added `udp-distributed-large-partition-shadow`, an eight-host/four-shard UDP CTest with one client and one server per shard and four cross-shard packet cuts.
- Added `udp-distributed-large-partition-stats-shadow`, which verifies the expected `0->1`, `1->3`, `2->0`, and `3->2` cut-matrix entries in shard-local `sim-stats.json` output.
- Added a concrete Phase 4 MPI cluster-backend design to `plan.md`.
- Added `DistributedSynchronizer`, a transport-neutral control-plane trait for startup/post-send barriers and global next-event-time synchronization.
- Changed `Controller` to depend on `Box<dyn DistributedSynchronizer>` instead of the concrete Unix control client, preserving current behavior while leaving a clean insertion point for an MPI synchronizer.
- Added semantic packet-batch codec helpers `encode_remote_packet_batch` and `decode_remote_packet_batch`, so future transports can encode/decode `RemotePacketEvent` batches without depending on private wire structs.
- Updated the Unix-socket packet exchange and IPC round-trip tests to use the semantic packet-batch codec helpers while preserving the same binary wire format.
- Added default-off Cargo feature `distributed_mpi` with optional `mpi`/rsmpi dependency.
- Added default-off CMake option `SHADOW_USE_MPI`, which appends the `distributed_mpi` Rust feature when explicitly enabled.
- Added a feature-gated `mpi_backend` module boundary that re-exports the MPI crate for future backend implementation without changing default runtime behavior.

A first real IPC packet transport exists and is unit-tested, and there is now an explicit distributed-only context for selecting that backend and owning its socket directory. The default controller still uses `NoopRemotePacketExchange`; only shard child processes launched with the internal socket-dir flag select the Unix-socket backend. Hidden shard config can now trigger a parent process that launches one child process per shard and waits for them. The in-process and Unix-socket exchanges prove the packet boundary works inside one process and across a local IPC boundary, and the manager can now distinguish global DNS hosts from local execution hosts. The parent-owned control socket now prevents receiving before all shards have sent and keeps shards advancing on the same global next-event time. The parent kills remaining shard children on first detected shard failure, including a real child blocked in the control protocol. The wait path is unit-tested, waiting peers unblock on mid-round disconnect, and the control server has an explicit shutdown/join path with malformed and inconsistent control-message coverage. Parent shutdown reports both child and control-server failures when both exist, and now has real-child malformed-control coverage. Distributed mode fails during config validation unless `experimental.use_new_tcp=true`; Rust TCP is the supported distributed TCP path, and legacy C TCP serialization is intentionally not a goal. The local IPC/control protocol now uses explicit versioned binary framing instead of JSON, with malformed-peer coverage for both the control socket and packet batch socket. Distributed stats now report remote packet volume and control-barrier wait time. Packet serialization covers UDP, Rust TCP with options and multi-chunk payloads, and zero-length UDP/TCP payloads. No-traffic, cross-shard UDP, cross-shard Rust TCP, child-failure, repeated-run UDP determinism, and repeated-run TCP determinism smoke runs now succeed; UDP coverage now includes both two-shard and three-shard parent-launched runs. The distributed smokes are automated as `udp-distributed-shadow`, `udp-distributed-3-shadow`, `tcp-distributed-shadow`, `distributed-child-failure-shadow`, `udp-distributed-determinism-compare-shadow`, and `tcp-distributed-determinism-compare-shadow`. Remaining correctness gaps include production-scale backend/protocol hardening and broader distributed scenario coverage.

## Rationale

The first dependency for distributed execution is stable global host identity. In the old flow, host IDs were assigned inside `Manager::run` from the manager-local host list. That is fine for one manager, but it would become wrong once a shard filters the global host list down to local hosts: each shard could assign conflicting host IDs, breaking DNS, packet event ordering, and deterministic replay.

Assigning host IDs during `SimConfig` construction keeps IDs tied to the full sorted config, before any future partitioning. This preserves current behavior because `ConfigOptions.hosts` is already a `BTreeMap`, and the old manager assignment used the same host order before shuffling scheduler host placement.

Decoupling `Manager` from concrete `Controller` is needed because distributed mode will need a different controller implementation that performs global window synchronization. A trait object is the smallest useful change: it avoids introducing generics through the manager and preserves the current controller path.

The `PartitionMap` is intentionally minimal. It only supports all-local mapping and lookup for now. This gives later packet-routing changes a type to depend on without committing to config format, partitioner implementation, MPI, or transport protocol yet.

The packet-send branch is placed at the final enqueue point, after source-side packet decisions such as reliability, latency, and delivery time. That keeps the current behavior unchanged and leaves the future remote branch in the right place to serialize a fully timestamped `RemotePacketEvent`.

`Event::new_packet_with_meta` is needed because remote inbound batches must preserve source ordering metadata created by the sending shard. Reconstructing the event using the receiving host would corrupt packet ordering and determinism.

`SerializedPacket` remains a semantic in-memory envelope rather than relying on Rust layout. The Unix-socket backend now encodes that envelope with explicit big-endian binary fields, so packet-send call sites still depend on the semantic type while the IPC layer owns the byte representation.

UDP was implemented before TCP because it has a smaller state surface: source/destination addresses, FIFO priority, and payload. Rust TCP serialization now preserves source/destination addresses, flags, sequence/acknowledgement numbers, window size, SACK ranges, window scale, timestamps, priority, and payload bytes. Legacy C TCP still has mutable packet state and remains explicitly unsupported for distributed serialization.

Distributed mode now requires `--use-new-tcp true` whenever `distributed_shard_count > 1`. This over-rejects UDP-only distributed simulations unless they also set the new TCP flag, but it prevents a mixed or accidental legacy C TCP distributed run from failing later at packet serialization time. This is now intentional product direction rather than a temporary gap: the Rust TCP stack is the future supported distributed TCP implementation.

`OutboundRemotePacketBuffer` drains in a deterministic order by destination shard, delivery time, source host id, source event id, and destination host id. This keeps cross-thread packet production from leaking mutex acquisition order into future inter-shard exchange behavior.

Inbound conversion validates destination shard ownership before enqueueing. This keeps future transport bugs from silently injecting packets into the wrong shard's host queues.

The exchange abstraction uses separate `send` and `receive` calls instead of a single all-in-one collective. This lets the controller insert synchronization between draining outbound packets and receiving inbound packets at the end of an execution window.

The no-op exchange backend is deliberately strict: if a non-local destination is somehow configured before a real backend exists, Shadow reports an exchange backend error instead of dropping packets and continuing with incorrect behavior.

The in-process exchange backend is intentionally simple: it groups packets by destination shard under a mutex and sorts each received batch deterministically. It is not intended as the final backend, but it gives tests a realistic send/receive boundary.

The Unix-socket exchange backend is intentionally a minimal first IPC slice. It uses one socket path per shard and explicit binary packet batches, keeping the transport boundary deterministic while remaining local-process friendly before adding MPI or other cluster backends.

`DistributedPacketExchangeContext` is separate from the default controller path on purpose. It gives the future distributed controller a place to choose the IPC backend and own the socket directory lifecycle without making regular single-process runs create IPC sockets or interpret distributed backend settings prematurely.

The first parent launcher uses process relaunch rather than running managers in one process. This avoids the current process-global/thread-local worker state and gives each shard an independent manager/shmem/scheduler instance. The launcher strips and replaces internal distributed flags so recursively launched children do not launch more children and do not inherit stale shard ids.

Shard child processes use distinct data directories because existing manager startup exclusively creates its configured output directory. Sharing one directory caused immediate startup races between shard children.

The synchronization mechanism is now a parent-owned Unix control socket rather than a file marker barrier. This keeps synchronization state in the parent, lets children publish round state through one connection, and avoids peers independently polling marker files. The protocol is still intentionally small: one length-prefixed binary request per shard per round, followed by one binary response carrying the global minimum next-event time when needed. Every binary frame starts with magic bytes, a version byte, and a message-kind byte, and packet/control frames have explicit size limits to catch malformed peers before allocating unbounded buffers. The server now owns and joins its background thread so tests and successful parent runs do not detach control-server work silently.

DNS registration was split from executable host construction before adding shard filtering. Remote packet routing needs every shard to resolve every simulated IP to the same global `HostId`, while each manager should only execute its local hosts.

Exchange backend selection lives in `ManagerConfig` rather than inside the manager loop. This keeps `Manager::run` transport-independent and gives future distributed controllers or tests a clean injection point.

Exchange backends are explicitly `Send + Sync` and can be wrapped in `Arc`. This is needed for future single-process multi-manager tests and mirrors the eventual requirement that multiple shard workers/controllers can use a shared transport safely.

Manager config construction is centralized so future controllers and tests use the same host filtering, DNS preservation, and exchange injection logic. This reduces the risk of distributed-mode setup diverging from the default controller path.

The test-only multi-shard harness intentionally does not run two `Manager`s concurrently. Current worker state is process-global/thread-local, so the safe first step is proving that two shard configs can be constructed from the same logical simulation, keep the same global view, and share exchange state. Actual multi-manager execution still requires a controller/scheduler design.

Hidden shard config intentionally does not directly select an exchange backend in regular single-shard runs. Backend selection still comes from the parent-launched child path via the internal IPC socket-dir flag, which preserves fail-fast behavior outside orchestrated distributed execution.

## Alternatives Considered And Rejected

- Keep assigning host IDs in `Manager` and offset IDs by shard: rejected because it couples deterministic identity to shard count and partition layout. The same config would produce different IDs under different shard counts.
- Assign host IDs lazily in DNS registration: rejected because packet event ordering also needs stable source host IDs, not just name/IP lookup.
- Make `Manager` generic over `SimController`: rejected for now because a trait object is simpler and keeps type churn localized.
- Add full distributed config options immediately: rejected because static all-local partition plumbing is enough for this step, and config format should wait until the event-bus/controller design is implemented.
- Add multiple managers in one process first: rejected for now because current worker state uses process-global/thread-local structures such as `WORKER_SHARED`. Multiple shard processes are a cleaner first model.
- Branch before packet loss/latency in `Worker::send_packet`: rejected because the source shard must remain responsible for loss, latency, and delivery time even when the destination is remote.
- Recreate source event ids on the receiving shard: rejected because packet event ordering is defined by sending host id plus sending host event id.
- Implement TCP serialization in the same patch as UDP: rejected because UDP is enough to validate the remote event envelope and avoids mixing transport-shape work with TCP fidelity questions.
- Start with final MPI byte encoding: rejected because local event semantics should be validated before binding the representation to a transport backend.
- Keep panicking on non-local packet destinations: rejected because staging outbound remote packets is the next smallest safe step toward transport integration.
- Inject remote packets directly into host queues without a locality check: rejected because cross-shard transport needs an explicit guard against mismatched partition maps or misrouted packets.
- Use one `exchange` method that sends and receives in a single call: rejected for now because distributed execution will likely need an explicit synchronization point between send and receive.
- Silently drop remote packets in single-shard mode: rejected because it would hide partition or backend misconfiguration.
- Keep the in-process exchange only inside `#[cfg(test)]`: rejected because future single-process distributed prototyping and integration tests can reuse the same backend without duplicating exchange logic.
- Build DNS only from local hosts after shard filtering: rejected because remote destination IPs must remain resolvable to global host IDs on every shard.
- Expose shard-count config before host filtering was ready: rejected because it would create a user-visible mode where remote hosts might still execute locally or become unresolvable.
- Hard-code exchange backend selection in `Manager::run`: rejected because it would make the manager loop harder to reuse with test, in-process, or future IPC/MPI backends.
- Keep exchange backends unshared: rejected because in-process distributed tests need multiple managers or controllers to refer to the same exchange state.
- Keep shard filtering in `Controller::run`: rejected because future test/distributed controllers need the same construction behavior.
- Run multiple managers concurrently in the first harness patch: rejected because `WORKER_SHARED` and worker-local state are currently process-global, so a concurrent harness would mix setup validation with scheduler/controller redesign.
- Make hidden shard config select `InProcessRemotePacketExchange`: rejected because the default controller still runs only one manager; an in-process backend there would queue packets to shards that are not running.
- Allow hidden config to select a shard with no hosts: rejected for now because the current manager/scheduler path has not been validated for empty execution shards.
- Select the Unix-socket backend from hidden shard config without parent orchestration: rejected because a default single-process controller would create sockets for missing peers and fail at runtime.
- Keep JSON framing for the next milestone: rejected after the initial IPC slice because explicit binary framing better matches the deterministic transport format expected by future cluster backends.
- Put Unix-socket backend selection directly behind public shard config in `Controller::run`: rejected because the default controller is still single-process and must continue to use the fail-fast no-op backend unless the parent launcher provides peer processes and an IPC directory.
- Store the IPC socket directory in YAML config: rejected because the socket directory is launcher-owned runtime state, not a persistent simulation setting.
- Support `-` stdin config in the parent distributed launcher immediately: rejected because each shard child currently needs to open the config independently; duplicating stdin into a temp config file can be added later if needed.
- Keep the file-marker barrier after adding the parent launcher: rejected because the parent-owned control socket is a better place to centralize round state and future failure propagation.
- Share one `shadow.data` directory across shard children: rejected because current manager startup owns and creates the data directory exclusively.

## Next Implementation Step

The next practical step is implementing the first real MPI backend pieces behind `distributed_mpi`:

1. Add an `MpiSynchronizer` using MPI barrier/allreduce semantics.
2. Add an `MpiRemotePacketExchange` using deterministic rank-ordered size exchange and payload exchange.
3. Add MPI-specific launch/config validation once an MPI test environment is available.

This keeps the next patch small and makes the eventual remote event bus integration explicit.

## Verification Status

Completed checks:

- `cd src && cargo fmt -- --check`
- `cd src && cargo test -p shadow-rs --lib shadow::tests`
- `cd src && cargo test -p shadow-rs --lib shadow::tests::distributed_shutdown_reports_real_malformed_control_child`
- `cd src && cargo test -p shadow-rs --lib core::sim_config`
- `cd src && cargo test -p shadow-rs --lib core::sim_stats`
- `cd src && cargo test -p shadow-rs --lib core::distributed` after binary framing and the latest diagnostic assertion
- `cd src && cargo test -p shadow-rs --lib`
- `./setup build --test`
- `./setup test udp-distributed-determinism --verbose`
- `./setup test tcp-distributed-determinism --verbose`
- `./setup test tcp-distributed --verbose`
- `./setup test distributed-child-failure --verbose`
- `./setup test udp-distributed-3 --verbose`
- `./setup test udp-distributed-partition --verbose`
- `./setup test udp-distributed-large-partition --verbose`
- `cargo check -p shadow-rs --lib --features distributed_mpi` reached `mpi-sys` discovery and failed because this machine does not have `mpicc`, `mpich.pc`, or `ompi.pc` installed.
- `./build/src/main/shadow /tmp/opencode/distributed-smoke.yaml`
- `(cd build/src/test/udp && ../../main/shadow /tmp/opencode/distributed-udp-smoke.yaml)`
- `./setup test` before the control-socket replacement
- `./setup test` after binary framing hardening
- `./setup test udp-distributed --verbose`
- `git diff --check`

Note: the current `shadow::tests` filter reports 7 passing tests. The current `core::sim_config` unit-test filter reports 8 passing tests. The current `core::sim_stats` unit-test filter reports 1 passing test. The current `core::distributed` unit-test filter reports 37 passing tests. The full `shadow-rs` library test suite currently reports 218 passing tests. The full standard CTest suite currently reports 233/233 passing tests after partition-file support, cut-matrix metrics, the larger four-shard UDP scenario, the distributed synchronizer trait refactor, the semantic packet-batch codec extraction, and adding the default-off MPI feature boundary. The no-traffic, UDP, UDP 3-shard, UDP partition-file, UDP large partition-file, Rust TCP, child-failure, repeated-run UDP determinism, and repeated-run TCP determinism cross-shard smoke runs completed successfully. The focused distributed CTests `udp-distributed-shadow`, `udp-distributed-3-shadow`, `udp-distributed-partition-shadow`, `udp-distributed-large-partition-shadow`, `udp-distributed-large-partition-stats-shadow`, `udp-distributed-determinism-compare-shadow`, `tcp-distributed-shadow`, `tcp-distributed-determinism-compare-shadow`, and `distributed-child-failure-shadow` pass after rebuilding with `./setup build --test`.

Previous full standard CTest caveat:

- A final `./setup test` after the control-socket replacement did not complete because `ioctl-linux` failed with `Time difference was too large` in `test_siocgstamp <init_method=Inet, sock_type=2>`.
- `./setup test --rerun-failed --verbose` reproduced the same `ioctl-linux` timing failure.
- This failure did not reproduce after binary framing hardening; `./setup test` passed 223/223.
- After adding `udp-distributed-3-shadow`, `./setup test` ran 224 tests and failed only `ioctl-linux` with the same `test_siocgstamp <init_method=Inet, sock_type=2>` timing assertion; `./setup test --rerun-failed --verbose` reproduced it with a 51.04895 ms difference.
- After adding UDP/TCP distributed determinism coverage, `./setup test` ran 230 tests and failed only `ioctl-linux` with the same `test_siocgstamp <init_method=Inet, sock_type=2>` timing assertion; `./setup test --rerun-failed --verbose` reproduced it with a 50.847217 ms difference.

Not yet run:

- Extra tests requiring `--extra`.
