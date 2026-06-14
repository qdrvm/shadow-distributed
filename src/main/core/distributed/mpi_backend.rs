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
//!   exchange followed by ordered `MPI_Send`/`MPI_Recv` for payload transfer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use anyhow::Result;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;

use super::exchange::RemotePacketExchange;
use super::synchronizer::DistributedSynchronizer;
use super::{RemotePacketEvent, ShardId};

// ---------------------------------------------------------------------------
// Minimal MPI FFI
// ---------------------------------------------------------------------------

mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    pub type MPI_Comm = c_int;

    pub const MPI_COMM_WORLD: MPI_Comm = 0x44000000;

    // MPI_Status as an opaque blob. Size from OpenMPI: 5 * sizeof(c_int) = 20 bytes.
    // We use a generous size to be safe across MPI implementations.
    #[repr(C)]
    pub struct MpiStatus {
        _data: [u8; 32],
    }

    // MPI_Datatype: we only need the basic types.
    pub type MPI_Datatype = c_int;

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
        pub fn MPI_Send(
            buf: *const c_void,
            count: c_int,
            datatype: MPI_Datatype,
            dest: c_int,
            tag: c_int,
            comm: MPI_Comm,
        ) -> c_int;
        pub fn MPI_Recv(
            buf: *mut c_void,
            count: c_int,
            datatype: MPI_Datatype,
            source: c_int,
            tag: c_int,
            comm: MPI_Comm,
            status: *mut MpiStatus,
        ) -> c_int;
        pub fn MPI_Abort(comm: MPI_Comm, errorcode: c_int) -> c_int;
    }

    pub type MPI_Op = c_int;
    pub const MPI_MIN: MPI_Op = 0x4c000818;
    pub const MPI_MAX: MPI_Op = 0x4c00081c;

    // MPI_Datatype values
    pub const MPI_INT64_T: MPI_Datatype = 0x4c00082f;
    pub const MPI_UINT32_T: MPI_Datatype = 0x4c000867;
    pub const MPI_BYTE: MPI_Datatype = 0x4c00010d;
    pub const MPI_UINT64_T: MPI_Datatype = 0x4c00086b;
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
        check_mpi(ffi::MPI_Comm_rank(ffi::MPI_COMM_WORLD, &mut rank), "MPI_Comm_rank")?;
        check_mpi(ffi::MPI_Comm_size(ffi::MPI_COMM_WORLD, &mut size), "MPI_Comm_size")?;
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
                ffi::MPI_Abort(ffi::MPI_COMM_WORLD, 1);
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
            check_mpi(ffi::MPI_Barrier(ffi::MPI_COMM_WORLD), "MPI_Barrier")
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
                    ffi::MPI_INT64_T,
                    ffi::MPI_MIN,
                    ffi::MPI_COMM_WORLD,
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
    rank: i32,
    size: i32,
}

impl MpiRemotePacketExchange {
    pub fn new() -> Result<Self> {
        let u = MpiUniverse::get();
        Ok(Self {
            rank: u.rank,
            size: u.size,
        })
    }
}

impl RemotePacketExchange for MpiRemotePacketExchange {
    fn send(&self, _src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        let size = self.size as usize;

        // Group events by destination rank (host-id modulo world size)
        let mut groups: Vec<Vec<&RemotePacketEvent>> = vec![Vec::new(); size];
        for event in events {
            let dst_rank = (u32::from(event.dst_host_id) % (size as u32)) as usize;
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

        // Exchange batch sizes via MPI_Alltoall (u32 per dst rank)
        let send_sizes: Vec<u32> = encoded.iter().map(|b| b.len() as u32).collect();
        let mut recv_sizes = vec![0u32; size];
        unsafe {
            check_mpi(
                ffi::MPI_Alltoall(
                    send_sizes.as_ptr() as *const std::os::raw::c_void,
                    1,
                    ffi::MPI_UINT32_T,
                    recv_sizes.as_mut_ptr() as *mut std::os::raw::c_void,
                    1,
                    ffi::MPI_UINT32_T,
                    ffi::MPI_COMM_WORLD,
                ),
                "MPI_Alltoall(sizes)",
            )?;
        }

        // Send batches to each destination rank (point-to-point)
        for (dst_rank, batch) in encoded.iter().enumerate() {
            if dst_rank as i32 == self.rank || batch.is_empty() {
                continue;
            }
            unsafe {
                check_mpi(
                    ffi::MPI_Send(
                        batch.as_ptr() as *const std::os::raw::c_void,
                        batch.len() as i32,
                        ffi::MPI_BYTE,
                        dst_rank as i32,
                        0,
                        ffi::MPI_COMM_WORLD,
                    ),
                    &format!("MPI_Send to rank {dst_rank}"),
                )?;
            }
        }

        Ok(())
    }

    fn receive(
        &self,
        _dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        let size = self.size as usize;
        let rank = self.rank as usize;

        // Exchange batch sizes
        let send_sizes = vec![0u32; size];
        let mut recv_sizes = vec![0u32; size];
        unsafe {
            check_mpi(
                ffi::MPI_Alltoall(
                    send_sizes.as_ptr() as *const std::os::raw::c_void,
                    1,
                    ffi::MPI_UINT32_T,
                    recv_sizes.as_mut_ptr() as *mut std::os::raw::c_void,
                    1,
                    ffi::MPI_UINT32_T,
                    ffi::MPI_COMM_WORLD,
                ),
                "MPI_Alltoall(sizes-recv)",
            )?;
        }

        let mut all_events = Vec::new();

        // Receive from each source rank in deterministic order
        for src_rank in 0..size {
            if src_rank == rank {
                continue;
            }
            let batch_size = recv_sizes[src_rank] as usize;
            if batch_size == 0 {
                continue;
            }
            let mut buf = vec![0u8; batch_size];
            unsafe {
                check_mpi(
                    ffi::MPI_Recv(
                        buf.as_mut_ptr() as *mut std::os::raw::c_void,
                        batch_size as i32,
                        ffi::MPI_BYTE,
                        src_rank as i32,
                        0,
                        ffi::MPI_COMM_WORLD,
                        std::ptr::null_mut(),
                    ),
                    &format!("MPI_Recv from rank {src_rank}"),
                )?;
            }
            let events = RemotePacketEvent::decode_batch(&buf)
                .map_err(|e| anyhow::anyhow!("Failed to decode MPI batch from rank {src_rank}: {e}"))?;
            all_events.extend(events);
        }

        // Sort deterministically
        all_events.sort_by(|a, b| {
            a.deliver_time
                .cmp(&b.deliver_time)
                .then_with(|| a.src_host_id.cmp(&b.src_host_id))
                .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
                .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
        });

        let min_time = all_events.first().map(|e| e.deliver_time);
        Ok((all_events, min_time))
    }
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
        let _ = MpiRemotePacketExchange::new();
    }
}
