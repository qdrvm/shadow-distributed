//! Transport-neutral remote packet exchange for distributed shards.
//!
//! The exchange handles the data plane: sending outbound remote-packet batches
//! and receiving inbound batches from other shards.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;
use shadow_shim_helper_rs::HostId;

use super::{RemotePacketEvent, ShardId};

/// A transport-neutral exchange for sending and receiving batches of remote
/// packet events between shards.
///
/// The `send` call delivers outbound events to destination shards. The
/// `receive` call collects inbound events from other shards that were sent
/// to this shard.
///
/// # Ordering
///
/// Events are exchanged in deterministic order: sorted by destination shard,
/// delivery time, source host id, source event id, and destination host id.
/// The send side sorts before transmitting; the receive side concatenates
/// all inbound batches and sorts again to preserve the global ordering
/// invariant regardless of transport receive order.
pub trait RemotePacketExchange: Send + Sync {
    /// Send a batch of remote packet events to their destination shards.
    ///
    /// This is called after draining the outbound remote packet buffer. The
    /// implementation must deliver events to the correct destination shards.
    fn send(&self, src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()>;

    /// Receive inbound remote packet events destined for this shard.
    ///
    /// Returns events sorted by delivery time, source host id, source event id,
    /// and destination host id, and the minimum delivery time (if any).
    fn receive(&self, dst_shard: ShardId) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)>;
}

/// Errors from remote packet exchange operations.
#[derive(Debug, thiserror::Error)]
pub enum RemotePacketExchangeError {
    #[error("No remote exchange backend configured")]
    NoBackend,
    #[error("Exchange backend error: {0}")]
    BackendError(String),
    #[error("Delivery error: {0}")]
    DeliveryError(#[from] super::RemotePacketDeliveryError),
}

/// A forwarding `RemotePacketExchange` implementation for `Arc<T>`.
impl<T: RemotePacketExchange> RemotePacketExchange for Arc<T> {
    fn send(&self, src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        self.as_ref().send(src_shard, events)
    }

    fn receive(
        &self,
        dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        self.as_ref().receive(dst_shard)
    }
}

// ---------------------------------------------------------------------------
// NoopRemotePacketExchange — the default single-shard backend
// ---------------------------------------------------------------------------

/// The default single-shard exchange backend.
///
/// `send` always returns an error if any outbound remote packet is produced
/// (since there should be no remote destinations in single-shard mode).
/// `receive` always returns an empty batch.
pub struct NoopRemotePacketExchange;

impl RemotePacketExchange for NoopRemotePacketExchange {
    fn send(&self, _src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        if !events.is_empty() {
            return Err(anyhow::anyhow!(
                "NoopRemotePacketExchange: {} outbound remote packets with no remote backend. \
                 Check distributed config and partition map.",
                events.len()
            ));
        }
        Ok(())
    }

    fn receive(
        &self,
        _dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        Ok((Vec::new(), None))
    }
}

// ---------------------------------------------------------------------------
// InProcessRemotePacketExchange — test-only shared exchange
// ---------------------------------------------------------------------------

/// An in-process exchange backend for unit tests and single-process
/// multi-shard prototyping.
///
/// All shards share the same `InProcessRemotePacketExchange` via `Arc`.
/// Events are sorted deterministically on both send and receive paths.
pub struct InProcessRemotePacketExchange {
    queues: Mutex<Vec<Vec<RemotePacketEvent>>>,
    num_shards: u32,
}

impl InProcessRemotePacketExchange {
    pub fn new(num_shards: u32) -> Self {
        Self {
            queues: Mutex::new(vec![Vec::new(); num_shards as usize]),
            num_shards,
        }
    }
}

impl RemotePacketExchange for InProcessRemotePacketExchange {
    fn send(&self, src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        let mut queues = self.queues.lock().unwrap();
        // Group by destination shard
        let mut groups: Vec<Vec<RemotePacketEvent>> =
            vec![Vec::new(); self.num_shards as usize];
        for event in events {
            let dst = event.dst_host_id;
            let shard: u32 = src_shard.into(); // actually need dst shard
            // We need the partition map to know the destination shard. Instead,
            // we assign by host-id modulo for tests.
            let dst_shard_idx = (u32::from(dst) % self.num_shards) as usize;
            groups[dst_shard_idx].push(event.clone());
        }
        for (i, group) in groups.into_iter().enumerate() {
            if !group.is_empty() {
                // Sort deterministically
                let mut sorted = group;
                sorted.sort_by(|a, b| {
                    a.deliver_time
                        .cmp(&b.deliver_time)
                        .then_with(|| a.src_host_id.cmp(&b.src_host_id))
                        .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
                        .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
                });
                queues[i].extend(sorted);
            }
        }
        Ok(())
    }

    fn receive(
        &self,
        dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        let mut queues = self.queues.lock().unwrap();
        let idx = dst_shard.as_usize();
        let mut events: Vec<RemotePacketEvent> = std::mem::take(&mut queues[idx]);
        // Sort again for determinism
        events.sort_by(|a, b| {
            a.deliver_time
                .cmp(&b.deliver_time)
                .then_with(|| a.src_host_id.cmp(&b.src_host_id))
                .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
                .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
        });
        let min_time = events
            .first()
            .map(|e| e.deliver_time);
        Ok((events, min_time))
    }
}

// ---------------------------------------------------------------------------
// UnixSocketRemotePacketExchange — local multi-process backend
// ---------------------------------------------------------------------------

/// A Unix-domain-socket-based exchange for local multi-process shard
/// communication.
///
/// Each shard binds a socket at `<socket_dir>/shadow-shard-<N>.sock`. Shards
/// connect to peers on demand and send binary-encoded packet batches.
pub struct UnixSocketRemotePacketExchange {
    shard_id: ShardId,
    num_shards: u32,
    socket_dir: std::path::PathBuf,
    listener: std::sync::Mutex<std::os::unix::net::UnixListener>,
}

impl UnixSocketRemotePacketExchange {
    /// Create a new Unix-socket exchange bound to the given socket directory.
    pub fn new(
        shard_id: ShardId,
        num_shards: u32,
        socket_dir: std::path::PathBuf,
    ) -> Result<Self> {
        std::fs::create_dir_all(&socket_dir)?;
        let socket_path = socket_dir.join(format!("shadow-shard-{}.sock", shard_id.0));
        // Remove stale socket
        let _ = std::fs::remove_file(&socket_path);
        let listener = std::os::unix::net::UnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            shard_id,
            num_shards,
            socket_dir,
            listener: std::sync::Mutex::new(listener),
        })
    }

    fn socket_path_for(&self, shard: ShardId) -> std::path::PathBuf {
        self.socket_dir
            .join(format!("shadow-shard-{}.sock", shard.0))
    }

    fn connect_to_peer(&self, dst_shard: ShardId) -> Result<std::os::unix::net::UnixStream> {
        if dst_shard == self.shard_id {
            return Err(anyhow::anyhow!("Cannot send to self"));
        }
        let path = self.socket_path_for(dst_shard);
        let mut attempts = 0;
        loop {
            match std::os::unix::net::UnixStream::connect(&path) {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) if attempts < 100 => {
                    attempts += 1;
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    if attempts == 1 {
                        log::debug!(
                            "Waiting for peer {} socket at {}: {e}",
                            dst_shard.0,
                            path.display()
                        );
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to peer shard {} at {}: {e}",
                        dst_shard.0,
                        path.display()
                    ));
                }
            }
        }
    }
}

impl RemotePacketExchange for UnixSocketRemotePacketExchange {
    fn send(&self, _src_shard: ShardId, events: &[RemotePacketEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Group events by destination shard using host-id modulo
        let mut groups: Vec<Vec<&RemotePacketEvent>> =
            vec![Vec::new(); self.num_shards as usize];
        for event in events {
            let dst_idx = (u32::from(event.dst_host_id) % self.num_shards) as usize;
            groups[dst_idx].push(event);
        }

        for (i, group) in groups.iter().enumerate() {
            if group.is_empty() || ShardId(i as u32) == self.shard_id {
                continue;
            }
            let dst_shard = ShardId(i as u32);
            let events: Vec<RemotePacketEvent> = group.iter().map(|e| (*e).clone()).collect();
            let batch_bytes = RemotePacketEvent::encode_batch(&events);
            let mut stream = self.connect_to_peer(dst_shard)?;
            let len = (batch_bytes.len() as u32).to_be_bytes();
            stream.write_all(&len)?;
            stream.write_all(&batch_bytes)?;
            stream.flush()?;
        }
        Ok(())
    }

    fn receive(
        &self,
        dst_shard: ShardId,
    ) -> Result<(Vec<RemotePacketEvent>, Option<EmulatedTime>)> {
        assert_eq!(dst_shard, self.shard_id);
        let mut all_events = Vec::new();

        // Accept connections from peers and read their batches.
        let num_peers = (self.num_shards - 1) as usize;
        let mut received = 0;

        // Set blocking mode for the receive phase
        {
            let listener = self.listener.lock().unwrap();
            listener.set_nonblocking(false)?;
        }

        while received < num_peers {
            let (mut stream, _addr) = {
                let listener = self.listener.lock().unwrap();
                match listener.accept() {
                    Ok(pair) => pair,
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        drop(listener);
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        continue;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Accept error: {e}"));
                    }
                }
            };
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).is_err() {
                continue;
            }
            let batch_len = u32::from_be_bytes(len_buf) as usize;
            if batch_len > 256 * 1024 * 1024 {
                return Err(anyhow::anyhow!(
                    "Batch too large from peer: {batch_len} bytes"
                ));
            }
            let mut batch_buf = vec![0u8; batch_len];
            stream.read_exact(&mut batch_buf)?;
            let events = RemotePacketEvent::decode_batch(&batch_buf)
                .map_err(|e| anyhow::anyhow!("Failed to decode peer batch: {e}"))?;
            all_events.extend(events);
            received += 1;
        }

        // Return to non-blocking for future use
        {
            let listener = self.listener.lock().unwrap();
            listener.set_nonblocking(true)?;
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

impl Drop for UnixSocketRemotePacketExchange {
    fn drop(&mut self) {
        let socket_path = self.socket_path_for(self.shard_id);
        let _ = std::fs::remove_file(&socket_path);
    }
}

// ---------------------------------------------------------------------------
// Helpers for encoding/decoding batches with shared binary format
// ---------------------------------------------------------------------------

/// Encode a batch of remote packet events into the transport-neutral binary
/// format. This is the canonical wire format shared by all exchange backends.
pub fn encode_remote_packet_batch(events: &[RemotePacketEvent]) -> Vec<u8> {
    RemotePacketEvent::encode_batch(events)
}

/// Decode a batch of remote packet events from the transport-neutral binary
/// format.
pub fn decode_remote_packet_batch(data: &[u8]) -> Result<Vec<RemotePacketEvent>> {
    RemotePacketEvent::decode_batch(data)
        .map_err(|e| anyhow::anyhow!("Batch decode error: {e}"))
}

// ---------------------------------------------------------------------------
// Test helpers and tests
// ---------------------------------------------------------------------------

/// A context for building exchange backends in tests and production.
pub enum DistributedPacketExchangeContext {
    /// Use a temporary directory (owned, cleaned up on drop).
    Temporary {
        dir: tempfile::TempDir,
    },
    /// Use an externally-managed directory.
    External {
        dir: std::path::PathBuf,
    },
}

impl DistributedPacketExchangeContext {
    /// Create a temporary IPC socket directory context.
    pub fn temporary() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        Ok(Self::Temporary { dir })
    }

    /// Create an external-directory context.
    pub fn external(dir: std::path::PathBuf) -> Self {
        Self::External { dir }
    }

    /// Get the socket directory path.
    pub fn socket_dir(&self) -> &std::path::Path {
        match self {
            Self::Temporary { dir } => dir.path(),
            Self::External { dir } => dir.as_path(),
        }
    }

    /// Build a Unix-socket exchange for the given shard.
    pub fn build_unix_exchange(
        &self,
        shard_id: ShardId,
        num_shards: u32,
    ) -> Result<UnixSocketRemotePacketExchange> {
        UnixSocketRemotePacketExchange::new(
            shard_id,
            num_shards,
            self.socket_dir().to_path_buf(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shadow_shim_helper_rs::simulation_time::SimulationTime;

    fn make_test_event(src: u32, dst: u32, id: u64) -> RemotePacketEvent {
        RemotePacketEvent {
            deliver_time: EmulatedTime::SIMULATION_START + SimulationTime::from_nanos(id * 100),
            src_host_id: HostId::from(src),
            src_host_event_id: id,
            dst_host_id: HostId::from(dst),
            packet: super::super::SerializedPacket::Udp {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, (src + 1) as u8),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, (dst + 1) as u8),
                src_port: 8000,
                dst_port: 9000,
                priority: 0,
                payload: b"test".to_vec(),
            },
        }
    }

    #[test]
    fn noop_send_empty_ok() {
        let ex = NoopRemotePacketExchange;
        assert!(ex.send(ShardId(0), &[]).is_ok());
    }

    #[test]
    fn noop_send_nonempty_err() {
        let ex = NoopRemotePacketExchange;
        let event = make_test_event(0, 1, 0);
        assert!(ex.send(ShardId(0), &[event]).is_err());
    }

    #[test]
    fn noop_receive_empty() {
        let ex = NoopRemotePacketExchange;
        let (events, min_time) = ex.receive(ShardId(0)).unwrap();
        assert!(events.is_empty());
        assert!(min_time.is_none());
    }

    #[test]
    fn inprocess_send_receive() {
        let ex = InProcessRemotePacketExchange::new(2);
        let event = make_test_event(0, 1, 0);
        // Send from shard 0
        ex.send(ShardId(0), &[event.clone()]).unwrap();
        // Receive on shard 1
        let (events, min_time) = ex.receive(ShardId(1)).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].src_host_id, HostId::from(0u32));
        assert_eq!(events[0].dst_host_id, HostId::from(1u32));
        assert!(min_time.is_some());
    }

    #[test]
    fn inprocess_deterministic_order() {
        let ex = InProcessRemotePacketExchange::new(2);
        let e1 = make_test_event(0, 1, 1);
        let e2 = make_test_event(0, 1, 0);
        ex.send(ShardId(0), &[e1, e2]).unwrap();
        let (events, _) = ex.receive(ShardId(1)).unwrap();
        assert_eq!(events[0].src_host_event_id, 0);
        assert_eq!(events[1].src_host_event_id, 1);
    }

    #[test]
    fn inprocess_arc_sharing() {
        let ex = Arc::new(InProcessRemotePacketExchange::new(2));
        let event = make_test_event(0, 1, 0);
        ex.send(ShardId(0), &[event]).unwrap();
        let (events, _) = ex.receive(ShardId(1)).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn unix_socket_send_receive_two_shards() {
        let ctx = DistributedPacketExchangeContext::temporary().unwrap();
        let ex0 = ctx.build_unix_exchange(ShardId(0), 2).unwrap();
        let ex1 = ctx.build_unix_exchange(ShardId(1), 2).unwrap();

        // Send from shard 0 → shard 1, then receive on shard 1
        let event = make_test_event(0, 1, 0);
        ex0.send(ShardId(0), &[event]).unwrap();

        let (events, min_time) = ex1.receive(ShardId(1)).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].src_host_id, HostId::from(0u32));
        assert_eq!(events[0].dst_host_id, HostId::from(1u32));
        assert!(min_time.is_some());
    }

    #[test]
    fn unix_socket_rejects_wrong_shard_receive() {
        let ctx = DistributedPacketExchangeContext::temporary().unwrap();
        let ex = ctx.build_unix_exchange(ShardId(0), 2).unwrap();
        // Should panic because the assertion fires before the receive
        // (the receive expects exactly shard 0)
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ex.receive(ShardId(1))
        }));
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[test]
    fn batch_encode_decode_roundtrip() {
        let events: Vec<_> = (0..5)
            .map(|i| make_test_event(i, (i + 1) % 5, i as u64))
            .collect();
        let encoded = encode_remote_packet_batch(&events);
        let decoded = decode_remote_packet_batch(&encoded).unwrap();
        assert_eq!(events.len(), decoded.len());
        for (a, b) in events.iter().zip(decoded.iter()) {
            assert_eq!(a.src_host_id, b.src_host_id);
            assert_eq!(a.src_host_event_id, b.src_host_event_id);
            assert_eq!(a.dst_host_id, b.dst_host_id);
        }
    }

    #[test]
    fn context_temporary_cleanup() {
        let ctx = DistributedPacketExchangeContext::temporary().unwrap();
        let dir = ctx.socket_dir().to_path_buf();
        assert!(dir.exists());
        let ex = ctx.build_unix_exchange(ShardId(0), 2).unwrap();
        drop(ex);
        drop(ctx);
        // TempDir cleanup removes everything
        assert!(!dir.exists());
    }
}
