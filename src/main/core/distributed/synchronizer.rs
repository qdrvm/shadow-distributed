//! Transport-neutral control-plane synchronization for distributed shards.
//!
//! The synchronizer provides two operations needed for the distributed window
//! protocol:
//!
//! 1. `wait` — a barrier ensuring all shards have reached the same point
//!    (startup synchronization and post-send coordination).
//! 2. `global_min_next_event` — a reduction that finds the minimum next-event
//!    time across all shards.

use anyhow::Result;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;

/// A transport-neutral synchronizer for distributed shard coordination.
///
/// Each shard calls the same sequence of operations in lockstep. The trait
/// abstracts over in-process testing, Unix-socket control protocol, and MPI
/// collective operations.
pub trait DistributedSynchronizer: Send + Sync {
    /// Barrier: all shards wait until every shard has reached this call.
    ///
    /// Used for:
    /// - Startup: ensure all shards are ready before beginning the simulation loop.
    /// - Post-send: ensure all shards have sent their outbound remote packets
    ///   before any shard starts receiving.
    fn wait(&self) -> Result<()>;

    /// Compute the global minimum of each shard's local next-event time.
    ///
    /// Returns the minimum across all shards, which becomes the start of the
    /// next execution window.
    fn global_min_next_event(&self, local_min: EmulatedTime) -> Result<EmulatedTime>;
}

/// A synchronizer for single-shard (non-distributed) runs.
///
/// `wait` is a no-op and `global_min_next_event` returns the local value
/// unchanged.
pub struct SingleShardSynchronizer;

impl DistributedSynchronizer for SingleShardSynchronizer {
    fn wait(&self) -> Result<()> {
        Ok(())
    }

    fn global_min_next_event(&self, local_min: EmulatedTime) -> Result<EmulatedTime> {
        Ok(local_min)
    }
}

/// A synchronizer that uses a Unix-domain control socket to a parent
/// coordinator process.
pub struct UnixSocketSynchronizer {
    client: std::sync::Arc<UnixControlClient>,
}

impl UnixSocketSynchronizer {
    pub fn new(socket_path: std::path::PathBuf) -> Result<Self> {
        let client = UnixControlClient::connect(socket_path)?;
        Ok(Self {
            client: std::sync::Arc::new(client),
        })
    }
}

impl DistributedSynchronizer for UnixSocketSynchronizer {
    fn wait(&self) -> Result<()> {
        self.client.send_recv(ControlMessage::Wait).map(|_| ())
    }

    fn global_min_next_event(&self, local_min: EmulatedTime) -> Result<EmulatedTime> {
        let dt_nanos = local_min
            .duration_since(&EmulatedTime::SIMULATION_START)
            .as_nanos() as u64;
        let response = self
            .client
            .send_recv(ControlMessage::NextEventTime(dt_nanos))?;
        match response {
            ControlResponse::NextEventTime(nanos) => Ok(EmulatedTime::SIMULATION_START
                + shadow_shim_helper_rs::simulation_time::SimulationTime::from_nanos(nanos)),
            _ => Err(anyhow::anyhow!("Unexpected control response")),
        }
    }
}

/// Binary control protocol messages (shard → coordinator).
#[derive(Clone, Debug)]
enum ControlMessage {
    Wait,
    NextEventTime(u64),
}

impl ControlMessage {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0x53, 0x43, 0x54, 0x4c]; // "SCTL" magic
        buf.push(0x01); // version
        match self {
            Self::Wait => {
                buf.push(0x00); // msg kind: wait
            }
            Self::NextEventTime(nanos) => {
                buf.push(0x01); // msg kind: next_event_time
                buf.extend_from_slice(&nanos.to_be_bytes());
            }
        }
        buf
    }
}

/// Binary control protocol responses (coordinator → shard).
#[derive(Clone, Debug)]
enum ControlResponse {
    Ack,
    NextEventTime(u64),
    Error(String),
}

/// A Unix-domain socket control client that communicates with a parent
/// coordinator process.
struct UnixControlClient {
    stream: std::os::unix::net::UnixStream,
}

impl UnixControlClient {
    fn connect(path: std::path::PathBuf) -> Result<Self> {
        // Retry loop: the parent may not have bound the socket yet.
        let mut attempts = 0;
        loop {
            match std::os::unix::net::UnixStream::connect(&path) {
                Ok(stream) => {
                    stream.set_nonblocking(false)?;
                    return Ok(Self { stream });
                }
                Err(e) if attempts < 50 => {
                    attempts += 1;
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    if attempts == 1 {
                        log::info!("Waiting for control socket at {}: {e}", path.display());
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to control socket {} after {attempts} attempts: {e}",
                        path.display()
                    ));
                }
            }
        }
    }

    fn send_recv(&self, msg: ControlMessage) -> Result<ControlResponse> {
        use std::io::{Read, Write};
        let mut stream = &self.stream;
        let request = msg.to_bytes();
        let len = (request.len() as u32).to_be_bytes();
        stream.write_all(&len)?;
        stream.write_all(&request)?;
        stream.flush()?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        if resp_len > 1024 * 1024 {
            return Err(anyhow::anyhow!("Control response too large: {resp_len}"));
        }
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf)?;

        if resp_buf.len() < 6 {
            return Err(anyhow::anyhow!("Control response too short"));
        }
        if &resp_buf[0..4] != b"SCTL" {
            return Err(anyhow::anyhow!("Bad control response magic"));
        }
        if resp_buf[4] != 0x01 {
            return Err(anyhow::anyhow!("Bad control response version"));
        }
        match resp_buf[5] {
            0x00 => Ok(ControlResponse::Ack),
            0x01 => {
                if resp_buf.len() < 14 {
                    return Err(anyhow::anyhow!("NextEventTime response too short"));
                }
                let nanos = u64::from_be_bytes([
                    resp_buf[6],
                    resp_buf[7],
                    resp_buf[8],
                    resp_buf[9],
                    resp_buf[10],
                    resp_buf[11],
                    resp_buf[12],
                    resp_buf[13],
                ]);
                Ok(ControlResponse::NextEventTime(nanos))
            }
            0xFF => {
                let msg = String::from_utf8_lossy(&resp_buf[6..]).to_string();
                Ok(ControlResponse::Error(msg))
            }
            other => Err(anyhow::anyhow!("Unknown control response kind: {other}")),
        }
    }
}
