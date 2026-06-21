# Distributed Shadow — Multi-Node Simulation with MPI

Distributed Shadow allows you to split a simulation across multiple physical
machines (or multiple processes on one machine), with each shard owning a subset
of virtual hosts. Shards exchange timestamped packet-arrival events via MPI
collectives, preserving Shadow's deterministic, repeatable discrete-event
execution model.

## Prerequisites

- **OpenMPI** (4.x tested) with development headers
- **pkg-config** (`ompi-c` package)
- A Shadow configuration file with at least as many hosts as MPI ranks

### Install MPI on Ubuntu / Debian

```bash
sudo apt-get install libopenmpi-dev openmpi-bin
```

### Install MPI on Fedora / RHEL

```bash
sudo dnf install openmpi-devel openmpi
```

## Building with MPI Support

MPI support is a compile-time option, disabled by default.

```bash
# From the Shadow source directory:
mkdir build && cd build
cmake -DSHADOW_USE_MPI=ON ..
cmake --build . -j$(nproc)
```

The CMake output should confirm MPI was found:

```
-- MPI found: /usr/lib/x86_64-linux-gnu/openmpi/lib/libmpi.so
-- MPI found: TRUE (found version "3.1")
```

### Build options

| Option | Default | Description |
|--------|---------|-------------|
| `SHADOW_USE_MPI` | OFF | Enable MPI distributed backend |
| `SHADOW_TEST` | OFF | Build tests (required for MPI CTests) |

## Configuration

### Minimal two-host distributed config

Create `distributed.yaml`:

```yaml
general:
  stop_time: 60
network:
  graph:
    type: 1_gbit_switch
experimental:
  use_new_tcp: true   # REQUIRED for multi-shard mode
hosts:
  server:
    network_node_id: 0
    processes:
    - path: /path/to/your/server-binary
      args: --port 9000
      start_time: 1
  client:
    network_node_id: 0
    processes:
    - path: /path/to/your/client-binary
      args: --connect server:9000
      start_time: 5
```

### Important: `use_new_tcp` must be enabled

Multi-shard mode (`shard_count > 1`) **requires** `experimental.use_new_tcp: true`.
This is enforced at configuration validation time. Legacy C TCP packets cannot be
serialized for cross-shard delivery. If you forget, Shadow will exit with:

```
Distributed multi-shard mode (shard_count=N) requires experimental.use_new_tcp=true.
```

### Host partitioning

By default, hosts are assigned to shards by **modulo** of their global host ID.
Host IDs are assigned deterministically in sorted hostname order. With 2 shards:

- `client` → HostId(0) → shard 0
- `server` → HostId(1) → shard 1

#### Explicit partition file (optional)

Create a YAML file mapping hostnames to shard IDs:

```yaml
# partition.yaml
client: 0
server: 1
```

Then pass it at runtime:

```bash
mpirun -np 2 shadow --distributed-partition-file partition.yaml distributed.yaml
```

## Running a Distributed Simulation

### Single machine (testing / development)

Use `mpirun` with `--oversubscribe` to run multiple ranks on one host:

```bash
mpirun -np 2 --oversubscribe \
    shadow \
    --data-directory=shadow.data \
    --log-level=info \
    --use-new-tcp true \
    distributed.yaml
```

Each rank writes to a shard-specific data directory:
- Rank 0 → `shadow.data.shard-0/`
- Rank 1 → `shadow.data.shard-1/`

### Multiple machines (cluster)

Create a hostfile listing your cluster nodes:

```
# hostfile
node1 slots=4
node2 slots=4
node3 slots=4
node4 slots=4
```

Then launch:

```bash
mpirun -np 16 --hostfile hostfile \
    shadow \
    --data-directory=shadow.data \
    --log-level=info \
    --use-new-tcp true \
    distributed.yaml
```

Each process writes its output locally on the node where it runs. Collect logs
with your cluster's filesystem or a post-processing script.

### Verifying the run

Check that each shard produced output:

```bash
ls shadow.data.shard-*/
# shadow.data.shard-0/:
#   hosts/  processed-config.yaml  shadow.log  sim-stats.json
# shadow.data.shard-1/:
#   hosts/  processed-config.yaml  shadow.log  sim-stats.json
```

The `sim-stats.json` in each shard directory includes distributed metrics if
cross-shard packets were exchanged.

## MPI CTest Suite

When built with `-DSHADOW_TEST=ON -DSHADOW_USE_MPI=ON`, the following MPI tests
are available:

```bash
# Run all MPI tests
ctest -L mpi

# Run only the UDP 2-rank test
ctest -R "udp-distributed-mpi-shadow$"

# Run only the UDP 4-rank test
ctest -R "udp-distributed-mpi-4-shadow$"

# Run only the TCP 2-rank test
ctest -R "tcp-distributed-mpi-shadow$"

# Verbose output on failure
ctest -L mpi --output-on-failure
```

Example output; test numbers may differ:

```
Test #198: tcp-distributed-mpi-shadow .......................   Passed    0.60 sec
Test #199: tcp-distributed-mpi-determinism-a-shadow .........   Passed    0.57 sec
Test #200: tcp-distributed-mpi-determinism-b-shadow .........   Passed    0.58 sec
Test #201: tcp-distributed-mpi-determinism-compare-shadow ...   Passed    0.02 sec
Test #234: udp-distributed-mpi-shadow .......................   Passed    0.57 sec
Test #235: udp-distributed-mpi-4-shadow .....................   Passed    0.62 sec
Test #236: udp-distributed-mpi-determinism-a-shadow .........   Passed    0.59 sec
Test #237: udp-distributed-mpi-determinism-b-shadow .........   Passed    0.58 sec
Test #238: udp-distributed-mpi-determinism-compare-shadow ...   Passed    0.01 sec

100% tests passed, 0 tests failed out of 9
```

## Architecture Overview

```
                 +-----------------------------+
                 | MPI Collective Operations   |
                 | - MPI_Barrier (sync)        |
                 | - MPI_Allreduce(MIN) (time) |
                 | - MPI_Alltoall (sizes)      |
                 | - MPI_Alltoallv (payloads)  |
                 +--------------+--------------+
                                |
                 batched timestamped packet events
                                |
        +-----------------------+-----------------------+
        |                       |                       |
+-------+--------+      +-------+--------+      +-------+--------+
| Shadow shard 0 |      | Shadow shard 1 |      | Shadow shard N |
| - local hosts  |      | - local hosts  |      | - local hosts  |
| - workers      |      | - workers      |      | - workers      |
| - sockets/TCP  |      | - sockets/TCP  |      | - sockets/TCP  |
| - shim IPC     |      | - shim IPC     |      | - shim IPC     |
| - event queues |      | - event queues |      | - event queues |
+----------------+      +----------------+      +----------------+
```

### Window protocol

For each execution window `[window_start, window_end)`:

1. All shards run their local hosts until `window_end`
2. Local-destination packets are pushed to local event queues (as in single-process Shadow)
3. Remote-destination packets are serialized and staged by destination shard
4. **MPI_Barrier** — synchronize before exchange
5. **MPI_Alltoall** — exchange batch sizes
6. **MPI_Alltoallv** — exchange batch payloads
7. **MPI_Barrier** — synchronize after exchange
8. Each shard sorts and injects received packets into local host event queues
9. **MPI_Allreduce(MIN)** — compute global minimum next-event time across all shards
10. Next window: `[global_min, global_min + runahead)`

## Determinism

Distributed Shadow is designed to be deterministic. With the same:

- Shadow binary
- Configuration file
- Seed value
- Partition map
- Shard count

Repeated runs produce identical simulation outputs, logs, and metrics. Each rank
writes its own log file; the collection of all shard logs is the complete
deterministic record.

## Limitations

- **Rust TCP only** — Legacy C TCP packets cannot cross shards. Always use
  `experimental.use_new_tcp: true` in distributed mode.
- **No process migration** — A host and all its managed processes, file
  descriptors, sockets, and shim state stay on one shard for the entire
  simulation.
- **Global windows** — All shards advance on the same global window. Shards with
  no work must still participate in MPI collectives (they idle at barriers).
- **Single communicator** — All ranks must be in `MPI_COMM_WORLD`. Sub-communicators
  and dynamic process management are not yet supported.
- **OpenMPI FFI** — The current backend binds OpenMPI exported symbols directly.
  MPICH support requires replacing these bindings with a portable C shim or
  generated bindings.

## Troubleshooting

### "cannot find binary path" at startup

The managed process binary path must be accessible on all nodes. Use absolute
paths or ensure the binary is in `$PATH` on every machine.

### Hangs at startup

All ranks must be able to resolve and reach each other via MPI. Check that
`mpirun` can launch processes on all nodes:

```bash
mpirun -np 2 --host node1,node2 hostname
```

### "requires experimental.use_new_tcp=true"

Add `experimental: { use_new_tcp: true }` to your YAML config, or pass
`--use-new-tcp true` on the command line.

### Segmentation fault in MPI_Comm_rank

This indicates an MPI ABI mismatch. The current FFI is tested with OpenMPI
4.1.x on x86_64 and binds OpenMPI symbols directly. Rebuild with matching
OpenMPI development headers and libraries.
