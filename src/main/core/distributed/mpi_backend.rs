//! MPI backend for distributed Shadow simulation.
//!
//! This module is gated behind the `distributed_mpi` Cargo feature and the
//! `SHADOW_USE_MPI` CMake option (both default-off).
//!
//! Uses minimal direct FFI bindings to libmpi to avoid dependency compatibility
//! issues with higher-level MPI crates.
//!
//! # Design
//!
//! - Each MPI rank is one Shadow shard process.
//! - `MpiSynchronizer` implements `DistributedSynchronizer` via `MPI_Barrier`
//!   and `MPI_Allreduce(MPI_MIN)` over encoded emulated times.
//! - `MpiRemotePacketExchange` implements `RemotePacketExchange` via
//!   deterministic variable-size collectives using `MPI_Alltoall` for size
//!   exchange followed by `MPI_Alltoallv` for payload transfer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use anyhow::Result;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;

use super::exchange::RemotePacketExchange;
use super::synchronizer::DistributedSynchronizer;
use super::{PartitionMap, RemotePacketEvent, ShardId};

// ---------------------------------------------------------------------------
// Minimal MPI FFI
// ---------------------------------------------------------------------------

mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    /// MPI_Comm is an opaque pointer (ompi_communicator_t*).
    pub type MPI_Comm = *mut c_void;

    /// MPI_Datatype is an opaque pointer.
    pub type MPI_Datatype = *mut c_void;

    /// MPI_Op is an opaque pointer.
    pub type MPI_Op = *mut c_void;

    // External symbols from libmpi. These are resolved at link time.
    // The C header expands MPI_COMM_WORLD to &ompi_mpi_comm_world, etc.
    unsafe extern "C" {
        pub static ompi_mpi_comm_world: c_void;
        pub static ompi_mpi_int64_t: c_void;
        pub static ompi_mpi_uint32_t: c_void;
        pub static ompi_mpi_byte: c_void;
        pub static ompi_mpi_uint64_t: c_void;
        pub static ompi_mpi_op_min: c_void;
        pub static ompi_mpi_op_max: c_void;
    }

    unsafe extern "C" {
        pub fn MPI_Init(argc: *mut c_int, argv: *mut *mut *mut c_char) -> c_int;
        pub fn MPI_Finalize() -> c_int;
        pub fn MPI_Comm_rank(comm: MPI_Comm, rank: *mut c_int) -> c_int;
        pub fn MPI_Comm_size(comm: MPI_Comm, size: *mut c_int) -> c_int;
        pub fn MPI_Barrier(comm: MPI_Comm) -> c_int;
        pub fn MPI_Allreduce(
            sendbuf: *const c_void,
            recvbuf: *mut c_void,
            count: c_int,
            datatype: MPI_Datatype,
            op: MPI_Op,
            comm: MPI_Comm,
        ) -> c_int;
        pub fn MPI_Alltoall(
            sendbuf: *const c_void,
            sendcount: c_int,
            sendtype: MPI_Datatype,
            recvbuf: *mut c_void,
            recvcount: c_int,
            recvtype: MPI_Datatype,
            comm: MPI_Comm,
        ) -> c_int;
        pub fn MPI_Alltoallv(
            sendbuf: *const c_void,
            sendcounts: *const c_int,
            sdispls: *const c_int,
            sendtype: MPI_Datatype,
            recvbuf: *mut c_void,
            recvcounts: *const c_int,
            rdispls: *const c_int,
            recvtype: MPI_Datatype,
            comm: MPI_Comm,
        ) -> c_int;
        pub fn MPI_Abort(comm: MPI_Comm, errorcode: c_int) -> c_int;
    }

    /// Helper: get MPI_COMM_WORLD as an MPI_Comm pointer.
    pub fn mpi_comm_world() -> MPI_Comm {
        // Safety: `ompi_mpi_comm_world` is a global struct in libmpi.
        // MPI_COMM_WORLD is defined as &ompi_mpi_comm_world.
        unsafe { &ompi_mpi_comm_world as *const c_void as *mut c_void }
    }

    pub fn mpi_int64_t() -> MPI_Datatype {
        unsafe { &ompi_mpi_int64_t as *const c_void as *mut c_void }
    }

    pub fn mpi_uint32_t() -> MPI_Datatype {
        unsafe { &ompi_mpi_uint32_t as *const c_void as *mut c_void }
    }

    pub fn mpi_byte() -> MPI_Datatype {
        unsafe { &ompi_mpi_byte as *const c_void as *mut c_void }
    }

    pub fn mpi_uint64_t() -> MPI_Datatype {
        unsafe { &ompi_mpi_uint64_t as *const c_void as *mut c_void }
    }

    pub fn mpi_min() -> MPI_Op {
        unsafe { &ompi_mpi_op_min as *const c_void as *mut c_void }
    }

    pub fn mpi_max() -> MPI_Op {
        unsafe { &ompi_mpi_op_max as *const c_void as *mut c_void }
    }
}

/// Check MPI return code, converting to anyhow error on failure.
fn check_mpi(rc: i32, context: &str) -> Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("MPI error in {context}: code {rc}"))
    }
}

// ---------------------------------------------------------------------------
// Global MPI state
// ---------------------------------------------------------------------------

static MPI_INITIALIZED: AtomicBool = AtomicBool::new(false);
static MPI_UNIVERSE: OnceLock<MpiUniverse> = OnceLock::new();

struct MpiUniverse {
    rank: i32,
    size: i32,
}

impl MpiUniverse {
    fn get() -> &'static Self {
        MPI_UNIVERSE.get().expect("MPI not initialized")
    }
}

/// Initialize MPI. Safe to call multiple times.
pub fn initialize_mpi() -> Result<()> {
    if MPI_INITIALIZED.swap(true, Ordering::SeqCst) {
        return Ok(());
    }

    // Suppress the default mpi-sys init; we handle it ourselves.
    unsafe {
        let rc = ffi::MPI_Init(std::ptr::null_mut(), std::ptr::null_mut());
        check_mpi(rc, "MPI_Init")?;
    }

    let mut rank: i32 = 0;
    let mut size: i32 = 0;
    unsafe {
        check_mpi(ffi::MPI_Comm_rank(ffi::mpi_comm_world(), &mut rank), "MPI_Comm_rank")?;
        check_mpi(ffi::MPI_Comm_size(ffi::mpi_comm_world(), &mut size), "MPI_Comm_size")?;
    }

    log::info!("MPI initialized: rank {rank} of {size}");

    MPI_UNIVERSE
        .set(MpiUniverse { rank, size })
        .map_err(|_| anyhow::anyhow!("MPI already initialized"))?;

    Ok(())
}

/// Get rank and world size after initialization.
pub fn mpi_rank_size() -> Result<(i32, i32)> {
    let u = MpiUniverse::get();
    Ok((u.rank, u.size))
}

/// Finalize MPI. Call before process exit.
pub fn finalize_mpi() {
    if MPI_INITIALIZED.load(Ordering::SeqCst) {
        unsafe {
            ffi::MPI_Finalize();
        }
    }
}

/// Abort the MPI communicator on fatal error.
pub fn mpi_abort_on_error(result: Result<()>, context: &str) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = format!("Fatal MPI error ({context}): {e}");
            log::error!("{msg}");
            eprintln!("{msg}");
            unsafe {
                ffi::MPI_Abort(ffi::mpi_comm_world(), 1);
            }
            Err(anyhow::anyhow!("{msg}"))
        }
    }
}

// ---------------------------------------------------------------------------
// MpiSynchronizer
// ---------------------------------------------------------------------------

pub struct MpiSynchronizer;

impl MpiSynchronizer {
    pub fn new() -> Result<Self> {
        let _u = MpiUniverse::get();
        Ok(Self)
    }
}

impl DistributedSynchronizer for MpiSynchronizer {
    fn wait(&self) -> Result<()> {
        unsafe {
            check_mpi(ffi::MPI_Barrier(ffi::mpi_comm_world()), "MPI_Barrier")
        }
    }

    fn global_min_next_event(&self, local_min: EmulatedTime) -> Result<EmulatedTime> {
        let local_ns: i64 = if local_min == EmulatedTime::MAX {
            i64::MAX
        } else {
            local_min
                .duration_since(&EmulatedTime::SIMULATION_START)
                .as_nanos() as i64
        };

        let mut global_ns: i64 = 0;
        unsafe {
            check_mpi(
                ffi::MPI_Allreduce(
                    &local_ns as *const i64 as *const std::os::raw::c_void,
                    &mut global_ns as *mut i64 as *mut std::os::raw::c_void,
                    1,
                    ffi::mpi_int64_t(),
                    ffi::mpi_min(),
                    ffi::mpi_comm_world(),
                ),
                "MPI_Allreduce(MIN)",
            )?;
        }

        if global_ns == i64::MAX {
            Ok(EmulatedTime::MAX)
        } else {
            Ok(EmulatedTime::SIMULATION_START
                + shadow_shim_helper_rs::simulation_time::SimulationTime::from_nanos(
                    global_ns as u64,
                ))
        }
    }
}

// ---------------------------------------------------------------------------
// MpiRemotePacketExchange
// ---------------------------------------------------------------------------

pub struct MpiRemotePacketExchange {
    size: i32,
    partition_map: PartitionMap,
    pending_received: std::sync::Mutex<Vec<RemotePacketEvent>>,
}

impl MpiRemotePacketExchange {
    pub fn new(partition_map: PartitionMap) -> Result<Self> {
        let u = MpiUniverse::get();
        Ok(Self {
            size: u.size,
            partition_map,
            pending_received: std::sync::Mutex::new(Vec::new()),
        })
    }
}

impl RemotePacketExchange for MpiRemotePacketExchange {
    fn requires_external_synchronization(&self) -> bool {
        false
    }

    fn send(&self, _src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        let size = self.size as usize;

        // Group events by destination rank. In MPI mode rank ids are shard ids,
        // so use the configured partition map rather than host-id modulo.
        let mut groups: Vec<Vec<&RemotePacketEvent>> = vec![Vec::new(); size];
        for event in events {
            let dst_shard = self
                .partition_map
                .shard_for_host(event.dst_host_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("Unknown destination host {:?}", event.dst_host_id)
                })?;
            let dst_rank = dst_shard.as_usize();
            if dst_rank >= size {
                return Err(anyhow::anyhow!(
                    "Destination shard {} for host {:?} exceeds MPI world size {}",
                    dst_shard.0,
                    event.dst_host_id,
                    size,
                ));
            }
            groups[dst_rank].push(event);
        }

        // Encode each group
        let encoded: Vec<Vec<u8>> = groups
            .iter()
            .map(|g| {
                let mut sorted: Vec<&RemotePacketEvent> = g.to_vec();
                sorted.sort_by(|a, b| {
                    a.deliver_time
                        .cmp(&b.deliver_time)
                        .then_with(|| a.src_host_id.cmp(&b.src_host_id))
                        .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
                        .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
                });
                let events: Vec<RemotePacketEvent> =
                    sorted.into_iter().cloned().collect();
                RemotePacketEvent::encode_batch(&events)
            })
            .collect();

        // Exchange batch sizes via MPI_Alltoall (u32 per dst rank).
        let send_sizes: Vec<u32> = encoded.iter().map(|b| b.len() as u32).collect();
        let mut recv_sizes = vec![0u32; size];
        unsafe {
            check_mpi(
                ffi::MPI_Alltoall(
                    send_sizes.as_ptr() as *const std::os::raw::c_void,
                    1,
                    ffi::mpi_uint32_t(),
                    recv_sizes.as_mut_ptr() as *mut std::os::raw::c_void,
                    1,
                    ffi::mpi_uint32_t(),
                    ffi::mpi_comm_world(),
                ),
                "MPI_Alltoall(sizes)",
            )?;
        }

        let send_counts: Vec<i32> = send_sizes
            .iter()
            .map(|&x| i32::try_from(x).expect("MPI send batch too large"))
            .collect();
        let recv_counts: Vec<i32> = recv_sizes
            .iter()
            .map(|&x| i32::try_from(x).expect("MPI receive batch too large"))
            .collect();
        let send_displs = displacements(&send_counts)?;
        let recv_displs = displacements(&recv_counts)?;
        let send_total: usize = send_counts.iter().map(|&x| x as usize).sum();
        let recv_total: usize = recv_counts.iter().map(|&x| x as usize).sum();

        let mut send_buf = Vec::with_capacity(send_total);
        for batch in &encoded {
            send_buf.extend_from_slice(batch);
        }
        let mut recv_buf = vec![0u8; recv_total];

        unsafe {
            check_mpi(
                ffi::MPI_Alltoallv(
                    send_buf.as_ptr() as *const std::os::raw::c_void,
                    send_counts.as_ptr(),
                    send_displs.as_ptr(),
                    ffi::mpi_byte(),
                    recv_buf.as_mut_ptr() as *mut std::os::raw::c_void,
                    recv_counts.as_ptr(),
                    recv_displs.as_ptr(),
                    ffi::mpi_byte(),
                    ffi::mpi_comm_world(),
                ),
                "MPI_Alltoallv(payloads)",
            )?;
        }

        let mut all_events = Vec::new();
        for src_rank in 0..size {
            let batch_size = recv_counts[src_rank] as usize;
            if batch_size == 0 {
                continue;
            }
            let offset = recv_displs[src_rank] as usize;
            let events = RemotePacketEvent::decode_batch(&recv_buf[offset..offset + batch_size])
                .map_err(|e| {
                    anyhow::anyhow!("Failed to decode MPI batch from rank {src_rank}: {e}")
                })?;
            all_events.extend(events);
        }

        all_events.sort_by(|a, b| {
            a.deliver_time
                .cmp(&b.deliver_time)
                .then_with(|| a.src_host_id.cmp(&b.src_host_id))
                .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
                .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
        });
        *self.pending_received.lock().unwrap() = all_events;

        Ok(())
    }

    fn receive(
        &self,
        _dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        let all_events = std::mem::take(&mut *self.pending_received.lock().unwrap());

        let min_time = all_events.first().map(|e| e.deliver_time);
        Ok((all_events, min_time))
    }
}

fn displacements(counts: &[i32]) -> Result<Vec<i32>> {
    let mut total = 0i32;
    let mut displs = Vec::with_capacity(counts.len());
    for &count in counts {
        displs.push(total);
        total = total
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("MPI Alltoallv payload too large"))?;
    }
    Ok(displs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "MPI not initialized")]
    fn mpi_synchronizer_new_requires_init() {
        let _ = MpiSynchronizer::new();
    }

    #[test]
    #[should_panic(expected = "MPI not initialized")]
    fn mpi_exchange_new_requires_init() {
        let partition_map = PartitionMap::by_host_id_modulo(&[], 1).unwrap();
        let _ = MpiRemotePacketExchange::new(partition_map);
    }
}
