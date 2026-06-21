use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::Context;
use serde::Serialize;

use crate::utility::counter::Counter;

/// Simulation statistics to be accessed by a single thread.
#[derive(Debug)]
pub struct LocalSimStats {
    pub alloc_counts: RefCell<Counter>,
    pub dealloc_counts: RefCell<Counter>,
    pub syscall_counts: RefCell<Counter>,
}

impl LocalSimStats {
    pub fn new() -> Self {
        Self {
            alloc_counts: RefCell::new(Counter::new()),
            dealloc_counts: RefCell::new(Counter::new()),
            syscall_counts: RefCell::new(Counter::new()),
        }
    }
}

impl Default for LocalSimStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulation statistics to be accessed by multiple threads.
#[derive(Debug)]
pub struct SharedSimStats {
    pub alloc_counts: Mutex<Counter>,
    pub dealloc_counts: Mutex<Counter>,
    pub syscall_counts: Mutex<Counter>,
    pub distributed: Mutex<DistributedSimStats>,
}

impl SharedSimStats {
    pub fn new() -> Self {
        Self {
            alloc_counts: Mutex::new(Counter::new()),
            dealloc_counts: Mutex::new(Counter::new()),
            syscall_counts: Mutex::new(Counter::new()),
            distributed: Mutex::new(DistributedSimStats::default()),
        }
    }

    /// Add stats from a local object to a shared object. May reset fields of `local`.
    pub fn add_from_local_stats(&self, local: &LocalSimStats) {
        let mut shared_alloc_counts = self.alloc_counts.lock().unwrap();
        let mut shared_dealloc_counts = self.dealloc_counts.lock().unwrap();
        let mut shared_syscall_counts = self.syscall_counts.lock().unwrap();

        let mut local_alloc_counts = local.alloc_counts.borrow_mut();
        let mut local_dealloc_counts = local.dealloc_counts.borrow_mut();
        let mut local_syscall_counts = local.syscall_counts.borrow_mut();

        shared_alloc_counts.add_counter(&local_alloc_counts);
        shared_dealloc_counts.add_counter(&local_dealloc_counts);
        shared_syscall_counts.add_counter(&local_syscall_counts);

        *local_alloc_counts = Counter::new();
        *local_dealloc_counts = Counter::new();
        *local_syscall_counts = Counter::new();
    }

    pub fn record_remote_packets_sent(&self, count: usize, payload_bytes: usize) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.remote_packets_sent += u64::try_from(count).unwrap();
        distributed.remote_packet_payload_bytes_sent += u64::try_from(payload_bytes).unwrap();
    }

    pub fn record_remote_packet_cut_sent(
        &self,
        src_shard: u32,
        dst_shard: u32,
        count: usize,
        payload_bytes: usize,
    ) {
        let mut distributed = self.distributed.lock().unwrap();
        let cut = distributed.cut_mut(src_shard, dst_shard);
        cut.packets_sent += u64::try_from(count).unwrap();
        cut.payload_bytes_sent += u64::try_from(payload_bytes).unwrap();
    }

    pub fn record_remote_packets_received(&self, count: usize, payload_bytes: usize) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.remote_packets_received += u64::try_from(count).unwrap();
        distributed.remote_packet_payload_bytes_received += u64::try_from(payload_bytes).unwrap();
    }

    pub fn record_remote_packet_cut_received(
        &self,
        src_shard: u32,
        dst_shard: u32,
        count: usize,
        payload_bytes: usize,
    ) {
        let mut distributed = self.distributed.lock().unwrap();
        let cut = distributed.cut_mut(src_shard, dst_shard);
        cut.packets_received += u64::try_from(count).unwrap();
        cut.payload_bytes_received += u64::try_from(payload_bytes).unwrap();
    }

    pub fn record_distributed_barrier_wait(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.control_barrier_wait_count += 1;
        distributed.control_barrier_wait_time_ns =
            add_duration_ns(distributed.control_barrier_wait_time_ns, duration);
    }

    pub fn record_mpi_barrier_wait(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.mpi_barrier_wait_count += 1;
        distributed.mpi_barrier_wait_time_ns =
            add_duration_ns(distributed.mpi_barrier_wait_time_ns, duration);
    }

    pub fn record_mpi_allreduce_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.mpi_allreduce_count += 1;
        distributed.mpi_allreduce_time_ns =
            add_duration_ns(distributed.mpi_allreduce_time_ns, duration);
    }

    pub fn record_mpi_alltoall_sizes_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.mpi_alltoall_sizes_count += 1;
        distributed.mpi_alltoall_sizes_time_ns =
            add_duration_ns(distributed.mpi_alltoall_sizes_time_ns, duration);
    }

    pub fn record_mpi_alltoallv_payload_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.mpi_alltoallv_payload_count += 1;
        distributed.mpi_alltoallv_payload_time_ns =
            add_duration_ns(distributed.mpi_alltoallv_payload_time_ns, duration);
    }

    pub fn record_remote_packet_encode_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.remote_packet_encode_count += 1;
        distributed.remote_packet_encode_time_ns =
            add_duration_ns(distributed.remote_packet_encode_time_ns, duration);
    }

    pub fn record_remote_packet_decode_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.remote_packet_decode_count += 1;
        distributed.remote_packet_decode_time_ns =
            add_duration_ns(distributed.remote_packet_decode_time_ns, duration);
    }

    pub fn record_remote_packet_inbound_injection_time(&self, duration: std::time::Duration) {
        let mut distributed = self.distributed.lock().unwrap();
        distributed.remote_packet_inbound_injection_count += 1;
        distributed.remote_packet_inbound_injection_time_ns = add_duration_ns(
            distributed.remote_packet_inbound_injection_time_ns,
            duration,
        );
    }
}

fn add_duration_ns(total: u64, duration: std::time::Duration) -> u64 {
    total.saturating_add(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
}

impl Default for SharedSimStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Simulation statistics in the format to be output.
#[derive(Serialize, Clone, Debug)]
struct SimStatsForOutput {
    pub objects: ObjectStatsForOutput,
    pub syscalls: Counter,
    pub distributed: DistributedSimStats,
}

#[derive(Serialize, Clone, Debug)]
struct ObjectStatsForOutput {
    pub alloc_counts: Counter,
    pub dealloc_counts: Counter,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct DistributedSimStats {
    pub remote_packets_sent: u64,
    pub remote_packet_payload_bytes_sent: u64,
    pub remote_packets_received: u64,
    pub remote_packet_payload_bytes_received: u64,
    pub remote_packet_cut_matrix: BTreeMap<String, DistributedShardCutStats>,
    pub control_barrier_wait_count: u64,
    pub control_barrier_wait_time_ns: u64,
    pub mpi_barrier_wait_count: u64,
    pub mpi_barrier_wait_time_ns: u64,
    pub mpi_allreduce_count: u64,
    pub mpi_allreduce_time_ns: u64,
    pub mpi_alltoall_sizes_count: u64,
    pub mpi_alltoall_sizes_time_ns: u64,
    pub mpi_alltoallv_payload_count: u64,
    pub mpi_alltoallv_payload_time_ns: u64,
    pub remote_packet_encode_count: u64,
    pub remote_packet_encode_time_ns: u64,
    pub remote_packet_decode_count: u64,
    pub remote_packet_decode_time_ns: u64,
    pub remote_packet_inbound_injection_count: u64,
    pub remote_packet_inbound_injection_time_ns: u64,
}

impl DistributedSimStats {
    fn cut_mut(&mut self, src_shard: u32, dst_shard: u32) -> &mut DistributedShardCutStats {
        self.remote_packet_cut_matrix
            .entry(format!("{src_shard}->{dst_shard}"))
            .or_insert_with(|| DistributedShardCutStats {
                src_shard,
                dst_shard,
                ..Default::default()
            })
    }
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct DistributedShardCutStats {
    pub src_shard: u32,
    pub dst_shard: u32,
    pub packets_sent: u64,
    pub payload_bytes_sent: u64,
    pub packets_received: u64,
    pub payload_bytes_received: u64,
}

impl SimStatsForOutput {
    /// Takes data from `stats` and puts it into a structure designed for output. May reset fields
    /// of `stats`.
    pub fn new(stats: &SharedSimStats) -> Self {
        Self {
            objects: ObjectStatsForOutput {
                alloc_counts: std::mem::take(&mut stats.alloc_counts.lock().unwrap()),
                dealloc_counts: std::mem::take(&mut stats.dealloc_counts.lock().unwrap()),
            },
            syscalls: std::mem::take(&mut stats.syscall_counts.lock().unwrap()),
            distributed: std::mem::take(&mut stats.distributed.lock().unwrap()),
        }
    }
}

/// May reset fields of `stats`.
pub fn write_stats_to_file(
    filename: &std::path::Path,
    stats: &SharedSimStats,
) -> anyhow::Result<()> {
    let stats = SimStatsForOutput::new(stats);

    let file = std::fs::File::create(filename)
        .with_context(|| format!("Failed to create file '{}'", filename.display()))?;

    serde_json::to_writer_pretty(file, &stats).with_context(|| {
        format!(
            "Failed to write stats json to file '{}'",
            filename.display()
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_distributed_stats() {
        let stats = SharedSimStats::new();

        stats.record_remote_packets_sent(2, 100);
        stats.record_remote_packet_cut_sent(0, 1, 2, 100);
        stats.record_remote_packets_received(3, 200);
        stats.record_remote_packet_cut_received(0, 1, 3, 200);
        stats.record_distributed_barrier_wait(std::time::Duration::from_nanos(50));
        stats.record_mpi_barrier_wait(std::time::Duration::from_nanos(60));
        stats.record_mpi_allreduce_time(std::time::Duration::from_nanos(70));
        stats.record_mpi_alltoall_sizes_time(std::time::Duration::from_nanos(80));
        stats.record_mpi_alltoallv_payload_time(std::time::Duration::from_nanos(90));
        stats.record_remote_packet_encode_time(std::time::Duration::from_nanos(100));
        stats.record_remote_packet_decode_time(std::time::Duration::from_nanos(110));
        stats.record_remote_packet_inbound_injection_time(std::time::Duration::from_nanos(120));

        let distributed = stats.distributed.lock().unwrap().clone();
        assert_eq!(distributed.remote_packets_sent, 2);
        assert_eq!(distributed.remote_packet_payload_bytes_sent, 100);
        assert_eq!(distributed.remote_packets_received, 3);
        assert_eq!(distributed.remote_packet_payload_bytes_received, 200);
        let cut = distributed.remote_packet_cut_matrix.get("0->1").unwrap();
        assert_eq!(cut.src_shard, 0);
        assert_eq!(cut.dst_shard, 1);
        assert_eq!(cut.packets_sent, 2);
        assert_eq!(cut.payload_bytes_sent, 100);
        assert_eq!(cut.packets_received, 3);
        assert_eq!(cut.payload_bytes_received, 200);
        assert_eq!(distributed.control_barrier_wait_count, 1);
        assert_eq!(distributed.control_barrier_wait_time_ns, 50);
        assert_eq!(distributed.mpi_barrier_wait_count, 1);
        assert_eq!(distributed.mpi_barrier_wait_time_ns, 60);
        assert_eq!(distributed.mpi_allreduce_count, 1);
        assert_eq!(distributed.mpi_allreduce_time_ns, 70);
        assert_eq!(distributed.mpi_alltoall_sizes_count, 1);
        assert_eq!(distributed.mpi_alltoall_sizes_time_ns, 80);
        assert_eq!(distributed.mpi_alltoallv_payload_count, 1);
        assert_eq!(distributed.mpi_alltoallv_payload_time_ns, 90);
        assert_eq!(distributed.remote_packet_encode_count, 1);
        assert_eq!(distributed.remote_packet_encode_time_ns, 100);
        assert_eq!(distributed.remote_packet_decode_count, 1);
        assert_eq!(distributed.remote_packet_decode_time_ns, 110);
        assert_eq!(distributed.remote_packet_inbound_injection_count, 1);
        assert_eq!(distributed.remote_packet_inbound_injection_time_ns, 120);
    }
}
