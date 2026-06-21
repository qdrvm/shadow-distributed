//! Types for distributed multi-shard Shadow simulation.
//!
//! Each shard is a normal Shadow simulator process that owns a subset of virtual
//! hosts. Shards exchange timestamped future packet-arrival events through a
//! transport-independent exchange trait. A synchronizer coordinates global
//! window advancement.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shadow_shim_helper_rs::HostId;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;
use shadow_shim_helper_rs::simulation_time::SimulationTime;

pub mod exchange;
pub mod synchronizer;

#[cfg(feature = "distributed_mpi")]
pub mod mpi_backend;

/// Identifies a shard (partition) of the simulation.
///
/// In MPI mode this maps directly to the MPI rank. In the local multi-process
/// mode it is the shard index assigned by the parent launcher.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ShardId(pub u32);

impl ShardId {
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl From<u32> for ShardId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<ShardId> for u32 {
    fn from(s: ShardId) -> Self {
        s.0
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The default shard id for single-shard (non-distributed) runs.
pub const DEFAULT_SHARD_ID: ShardId = ShardId(0);

/// Maps each configured host to its owning shard.
///
/// Every shard has a complete copy so it can resolve destination shards when
/// sending packets and validate inbound remote events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionMap {
    host_to_shard: HashMap<HostId, ShardId>,
}

impl PartitionMap {
    /// Build a partition map from an explicit host→shard assignment.
    pub fn from_host_shards(
        mapping: HashMap<HostId, ShardId>,
        num_shards: u32,
    ) -> Result<Self, PartitionMapError> {
        if num_shards == 0 {
            return Err(PartitionMapError::ZeroShardCount);
        }
        Ok(Self {
            host_to_shard: mapping,
        })
    }

    /// Assign hosts to shards deterministically by host-id modulo.
    ///
    /// This is the default partitioning when no explicit partition file is given.
    pub fn by_host_id_modulo(
        host_ids: &[HostId],
        num_shards: u32,
    ) -> Result<Self, PartitionMapError> {
        if num_shards == 0 {
            return Err(PartitionMapError::ZeroShardCount);
        }
        let mapping = host_ids
            .iter()
            .map(|&id| {
                let shard = ShardId(u32::from(id) % num_shards);
                (id, shard)
            })
            .collect();
        Ok(Self {
            host_to_shard: mapping,
        })
    }

    /// The shard that owns the given host.
    pub fn shard_for_host(&self, host_id: HostId) -> Option<ShardId> {
        self.host_to_shard.get(&host_id).copied()
    }

    /// Check whether a host belongs to the given shard.
    pub fn is_host_local(&self, host_id: HostId, local_shard: ShardId) -> bool {
        self.shard_for_host(host_id) == Some(local_shard)
    }

    /// All host ids known to this partition map, in deterministic order.
    pub fn host_ids(&self) -> Vec<HostId> {
        let mut ids: Vec<_> = self.host_to_shard.keys().copied().collect();
        ids.sort_by_key(|id| u32::from(*id));
        ids
    }

    /// Number of shards in this partition.
    pub fn num_shards(&self) -> u32 {
        self.host_to_shard
            .values()
            .map(|s| s.0 + 1)
            .max()
            .unwrap_or(1)
    }
}

/// Errors constructing a partition map.
#[derive(Debug, thiserror::Error)]
pub enum PartitionMapError {
    #[error("Zero shard count")]
    ZeroShardCount,
    #[error("Unknown host {0:?}")]
    UnknownHost(HostId),
    #[error("Shard id {0} is out of range for {1} shards")]
    ShardOutOfRange(u32, u32),
    #[error("Missing host {0:?}")]
    MissingHost(HostId),
    #[error("Shard {0} has no hosts")]
    EmptyShard(ShardId),
}

/// A serializable, transport-neutral representation of a network packet for
/// cross-shard delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SerializedPacket {
    Udp {
        src_ip: std::net::Ipv4Addr,
        dst_ip: std::net::Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        priority: u64,
        payload: Vec<u8>,
    },
    Tcp {
        src_ip: std::net::Ipv4Addr,
        dst_ip: std::net::Ipv4Addr,
        src_port: u16,
        dst_port: u16,
        flags: u8,
        seq: u32,
        ack: u32,
        window_size: u16,
        window_scale: Option<u8>,
        selective_acks: Vec<(u32, u32)>,
        timestamp: Option<u32>,
        timestamp_echo: Option<u32>,
        priority: u64,
        payload: Vec<u8>,
    },
}

impl SerializedPacket {
    /// Serialize to deterministic big-endian bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::Udp {
                src_ip,
                dst_ip,
                src_port,
                dst_port,
                priority,
                payload,
            } => {
                buf.push(0x01); // tag: UDP
                buf.extend_from_slice(&src_ip.octets());
                buf.extend_from_slice(&dst_ip.octets());
                buf.extend_from_slice(&src_port.to_be_bytes());
                buf.extend_from_slice(&dst_port.to_be_bytes());
                buf.extend_from_slice(&priority.to_be_bytes());
                buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(payload);
            }
            Self::Tcp {
                src_ip,
                dst_ip,
                src_port,
                dst_port,
                flags,
                seq,
                ack,
                window_size,
                window_scale,
                selective_acks,
                timestamp,
                timestamp_echo,
                priority,
                payload,
            } => {
                buf.push(0x02); // tag: TCP
                buf.extend_from_slice(&src_ip.octets());
                buf.extend_from_slice(&dst_ip.octets());
                buf.extend_from_slice(&src_port.to_be_bytes());
                buf.extend_from_slice(&dst_port.to_be_bytes());
                buf.push(*flags);
                buf.extend_from_slice(&seq.to_be_bytes());
                buf.extend_from_slice(&ack.to_be_bytes());
                buf.extend_from_slice(&window_size.to_be_bytes());
                match window_scale {
                    Some(ws) => {
                        buf.push(1);
                        buf.push(*ws);
                    }
                    None => buf.push(0),
                }
                buf.extend_from_slice(&(selective_acks.len() as u32).to_be_bytes());
                for &(left, right) in selective_acks {
                    buf.extend_from_slice(&left.to_be_bytes());
                    buf.extend_from_slice(&right.to_be_bytes());
                }
                match timestamp {
                    Some(ts) => {
                        buf.push(1);
                        buf.extend_from_slice(&ts.to_be_bytes());
                    }
                    None => buf.push(0),
                }
                match timestamp_echo {
                    Some(te) => {
                        buf.push(1);
                        buf.extend_from_slice(&te.to_be_bytes());
                    }
                    None => buf.push(0),
                }
                buf.extend_from_slice(&priority.to_be_bytes());
                buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                buf.extend_from_slice(payload);
            }
        }
        buf
    }

    /// Deserialize from deterministic big-endian bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, PacketSerializationError> {
        if data.is_empty() {
            return Err(PacketSerializationError::EmptyData);
        }
        let mut pos = 1;
        match data[0] {
            0x01 => {
                // UDP: 1(tag) + 4+4(src+dst ip) + 2+2(ports) + 8(priority) + 4(payload_len) = 25 min
                if data.len() < 25 {
                    return Err(PacketSerializationError::Truncated);
                }
                let src_ip =
                    std::net::Ipv4Addr::new(data[pos], data[pos + 1], data[pos + 2], data[pos + 3]);
                pos += 4;
                let dst_ip =
                    std::net::Ipv4Addr::new(data[pos], data[pos + 1], data[pos + 2], data[pos + 3]);
                pos += 4;
                let src_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                let dst_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                let priority = u64::from_be_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                pos += 8;
                let payload_len =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if data.len() < pos + payload_len {
                    return Err(PacketSerializationError::Truncated);
                }
                let payload = data[pos..pos + payload_len].to_vec();
                Ok(Self::Udp {
                    src_ip,
                    dst_ip,
                    src_port,
                    dst_port,
                    priority,
                    payload,
                })
            }
            0x02 => {
                // TCP: minimum header size before payload
                if data.len() < 30 {
                    return Err(PacketSerializationError::Truncated);
                }
                let src_ip =
                    std::net::Ipv4Addr::new(data[pos], data[pos + 1], data[pos + 2], data[pos + 3]);
                pos += 4;
                let dst_ip =
                    std::net::Ipv4Addr::new(data[pos], data[pos + 1], data[pos + 2], data[pos + 3]);
                pos += 4;
                let src_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                let dst_port = u16::from_be_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                let flags = data[pos];
                pos += 1;
                let seq =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                pos += 4;
                let ack =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                pos += 4;
                let window_size = u16::from_be_bytes([data[pos], data[pos + 1]]);
                pos += 2;
                let window_scale = if data[pos] == 1 {
                    let ws = data[pos + 1];
                    pos += 2;
                    Some(ws)
                } else {
                    pos += 1;
                    None
                };
                if data.len() < pos + 4 {
                    return Err(PacketSerializationError::Truncated);
                }
                let sack_count =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if sack_count > 64 {
                    return Err(PacketSerializationError::TooManySackBlocks(sack_count));
                }
                if data.len() < pos + sack_count * 8 {
                    return Err(PacketSerializationError::Truncated);
                }
                let mut selective_acks = Vec::with_capacity(sack_count);
                for _ in 0..sack_count {
                    let left = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]);
                    pos += 4;
                    let right = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]);
                    pos += 4;
                    selective_acks.push((left, right));
                }
                let timestamp = if data.len() > pos && data[pos] == 1 {
                    pos += 1;
                    if data.len() < pos + 4 {
                        return Err(PacketSerializationError::Truncated);
                    }
                    let ts = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]);
                    pos += 4;
                    Some(ts)
                } else {
                    pos += 1;
                    None
                };
                let timestamp_echo = if data.len() > pos && data[pos] == 1 {
                    pos += 1;
                    if data.len() < pos + 4 {
                        return Err(PacketSerializationError::Truncated);
                    }
                    let te = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]);
                    pos += 4;
                    Some(te)
                } else {
                    pos += 1;
                    None
                };
                if data.len() < pos + 8 {
                    return Err(PacketSerializationError::Truncated);
                }
                let priority = u64::from_be_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                pos += 8;
                if data.len() < pos + 4 {
                    return Err(PacketSerializationError::Truncated);
                }
                let payload_len =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if data.len() < pos + payload_len {
                    return Err(PacketSerializationError::Truncated);
                }
                let payload = data[pos..pos + payload_len].to_vec();
                Ok(Self::Tcp {
                    src_ip,
                    dst_ip,
                    src_port,
                    dst_port,
                    flags,
                    seq,
                    ack,
                    window_size,
                    window_scale,
                    selective_acks,
                    timestamp,
                    timestamp_echo,
                    priority,
                    payload,
                })
            }
            _ => Err(PacketSerializationError::UnknownTag(data[0])),
        }
    }
}

/// Errors that can occur during packet serialization.
#[derive(Debug, thiserror::Error)]
pub enum PacketSerializationError {
    #[error("Empty data")]
    EmptyData,
    #[error("Truncated data")]
    Truncated,
    #[error("Unknown packet tag {0}")]
    UnknownTag(u8),
    #[error("Too many SACK blocks: {0}")]
    TooManySackBlocks(usize),
    #[error("Legacy TCP is not supported for distributed mode; use experimental.use_new_tcp=true")]
    LegacyTcp,
}

/// A remote packet event to be delivered to a destination host on another shard.
///
/// Carries full source ordering metadata so the receiving shard can enqueue it
/// with the correct source host id and source event id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemotePacketEvent {
    /// When the packet should be delivered in simulated time.
    pub deliver_time: EmulatedTime,
    /// The sending host's global id.
    pub src_host_id: HostId,
    /// Monotonically-increasing per-src-host event id assigned by the sending shard.
    pub src_host_event_id: u64,
    /// The receiving host's global id.
    pub dst_host_id: HostId,
    /// The serialized packet data.
    pub packet: SerializedPacket,
}

impl RemotePacketEvent {
    /// Maximum payload size for a single packet (16 MiB).
    pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

    /// Maximum number of events in a single batch exchange.
    pub const MAX_BATCH_EVENTS: u32 = 1_000_000;

    /// Encode a batch of remote packet events into big-endian bytes.
    pub fn encode_batch(events: &[Self]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(events.len() * 128);
        // 4-byte magic + 1-byte version + 4-byte event count
        buf.extend_from_slice(b"SHRB"); // magic
        buf.push(0x01); // version
        buf.extend_from_slice(&(events.len() as u32).to_be_bytes());
        for event in events {
            let packet_bytes = event.packet.to_bytes();
            // deliver_time as i64 nanos since SIMULATION_START
            let dt_nanos = event
                .deliver_time
                .duration_since(&EmulatedTime::SIMULATION_START)
                .as_nanos();
            buf.extend_from_slice(&(dt_nanos as u64).to_be_bytes());
            buf.extend_from_slice(&u32::from(event.src_host_id).to_be_bytes());
            buf.extend_from_slice(&event.src_host_event_id.to_be_bytes());
            buf.extend_from_slice(&u32::from(event.dst_host_id).to_be_bytes());
            buf.extend_from_slice(&(packet_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(&packet_bytes);
        }
        buf
    }

    /// Decode a batch of remote packet events from big-endian bytes.
    pub fn decode_batch(data: &[u8]) -> Result<Vec<Self>, PacketSerializationError> {
        if data.len() < 9 {
            return Err(PacketSerializationError::Truncated);
        }
        let magic = &data[0..4];
        if magic != b"SHRB" {
            return Err(PacketSerializationError::EmptyData); // reuse: bad magic
        }
        let version = data[4];
        if version != 0x01 {
            return Err(PacketSerializationError::UnknownTag(version));
        }
        let count = u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize;
        if count as u32 > Self::MAX_BATCH_EVENTS {
            return Err(PacketSerializationError::TooManySackBlocks(count));
        }
        let mut events = Vec::with_capacity(count);
        let mut pos = 9;
        for _ in 0..count {
            if data.len() < pos + 28 {
                return Err(PacketSerializationError::Truncated);
            }
            let dt_nanos = u64::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            pos += 8;
            let src_host_id = HostId::from(u32::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
            ]));
            pos += 4;
            let src_host_event_id = u64::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            pos += 8;
            let dst_host_id = HostId::from(u32::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
            ]));
            pos += 4;
            let packet_len =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;
            if packet_len > Self::MAX_PAYLOAD_BYTES + 1024 {
                return Err(PacketSerializationError::TooManySackBlocks(packet_len));
            }
            if data.len() < pos + packet_len {
                return Err(PacketSerializationError::Truncated);
            }
            let packet = SerializedPacket::from_bytes(&data[pos..pos + packet_len])?;
            pos += packet_len;
            let deliver_time =
                EmulatedTime::SIMULATION_START + SimulationTime::from_nanos(dt_nanos);
            events.push(Self {
                deliver_time,
                src_host_id,
                src_host_event_id,
                dst_host_id,
                packet,
            });
        }
        Ok(events)
    }

    /// Convert a remote packet event into a local packet event for injection
    /// into the destination host queue, after validating that the destination
    /// host belongs to this shard.
    pub fn into_local_event(
        self,
        local_shard: ShardId,
        partition_map: &PartitionMap,
    ) -> Result<RemotePacketDelivery, RemotePacketDeliveryError> {
        let dst_shard = partition_map.shard_for_host(self.dst_host_id).ok_or(
            RemotePacketDeliveryError::UnknownDestinationHost(self.dst_host_id),
        )?;
        if dst_shard != local_shard {
            return Err(RemotePacketDeliveryError::NonLocalDestination {
                host: self.dst_host_id,
                expected_shard: local_shard,
                actual_shard: dst_shard,
            });
        }
        Ok(RemotePacketDelivery {
            deliver_time: self.deliver_time,
            src_host_id: self.src_host_id,
            src_host_event_id: self.src_host_event_id,
            dst_host_id: self.dst_host_id,
            packet: self.packet,
        })
    }
}

/// A validated remote packet ready for local delivery.
#[derive(Clone, Debug)]
pub struct RemotePacketDelivery {
    pub deliver_time: EmulatedTime,
    pub src_host_id: HostId,
    pub src_host_event_id: u64,
    pub dst_host_id: HostId,
    pub packet: SerializedPacket,
}

/// Errors converting a remote packet event for local delivery.
#[derive(Debug, thiserror::Error)]
pub enum RemotePacketDeliveryError {
    #[error("Unknown destination host {0:?}")]
    UnknownDestinationHost(HostId),
    #[error(
        "Non-local destination: host {host:?} belongs to shard {actual_shard}, not {expected_shard}"
    )]
    NonLocalDestination {
        host: HostId,
        expected_shard: ShardId,
        actual_shard: ShardId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_shard_is_zero() {
        assert_eq!(DEFAULT_SHARD_ID, ShardId(0));
    }

    #[test]
    fn shard_id_round_trip() {
        let s = ShardId(42);
        let u: u32 = s.into();
        assert_eq!(u, 42);
        let s2: ShardId = u.into();
        assert_eq!(s, s2);
    }

    #[test]
    fn partition_map_all_local_default() {
        let hosts: Vec<HostId> = (0..4).map(|i| HostId::from(i)).collect();
        let map = PartitionMap::by_host_id_modulo(&hosts, 1).unwrap();
        for host in &hosts {
            assert!(map.is_host_local(*host, DEFAULT_SHARD_ID));
        }
        assert_eq!(map.num_shards(), 1);
    }

    #[test]
    fn partition_map_modulo_two_shards() {
        let hosts: Vec<HostId> = (0..4).map(|i| HostId::from(i)).collect();
        let map = PartitionMap::by_host_id_modulo(&hosts, 2).unwrap();
        assert!(map.is_host_local(HostId::from(0), ShardId(0)));
        assert!(map.is_host_local(HostId::from(2), ShardId(0)));
        assert!(map.is_host_local(HostId::from(1), ShardId(1)));
        assert!(map.is_host_local(HostId::from(3), ShardId(1)));
        assert_eq!(map.num_shards(), 2);
    }

    #[test]
    fn partition_map_zero_shards_rejected() {
        let hosts: Vec<HostId> = vec![];
        assert!(PartitionMap::by_host_id_modulo(&hosts, 0).is_err());
    }

    #[test]
    fn serialized_udp_round_trip() {
        let pkt = SerializedPacket::Udp {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 1234,
            dst_port: 5678,
            priority: 5u64,
            payload: b"hello world".to_vec(),
        };
        let bytes = pkt.to_bytes();
        let pkt2 = SerializedPacket::from_bytes(&bytes).unwrap();
        assert_eq!(pkt, pkt2);
    }

    #[test]
    fn serialized_tcp_round_trip() {
        let pkt = SerializedPacket::Tcp {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 1234,
            dst_port: 80,
            flags: 0x12,
            seq: 1000,
            ack: 2000,
            window_size: 65535,
            window_scale: Some(7),
            selective_acks: vec![(100, 200), (300, 400)],
            timestamp: Some(12345),
            timestamp_echo: Some(67890),
            priority: 3u64,
            payload: b"GET / HTTP/1.1\r\n\r\n".to_vec(),
        };
        let bytes = pkt.to_bytes();
        let pkt2 = SerializedPacket::from_bytes(&bytes).unwrap();
        assert_eq!(pkt, pkt2);
    }

    #[test]
    fn serialized_tcp_zero_payload() {
        let pkt = SerializedPacket::Tcp {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 1234,
            dst_port: 80,
            flags: 0x02,
            seq: 0,
            ack: 0,
            window_size: 0,
            window_scale: None,
            selective_acks: vec![],
            timestamp: None,
            timestamp_echo: None,
            priority: 0u64,
            payload: vec![],
        };
        let bytes = pkt.to_bytes();
        let pkt2 = SerializedPacket::from_bytes(&bytes).unwrap();
        assert_eq!(pkt, pkt2);
    }

    #[test]
    fn serialized_udp_zero_payload() {
        let pkt = SerializedPacket::Udp {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 0,
            dst_port: 0,
            priority: 0u64,
            payload: vec![],
        };
        let bytes = pkt.to_bytes();
        let pkt2 = SerializedPacket::from_bytes(&bytes).unwrap();
        assert_eq!(pkt, pkt2);
    }

    #[test]
    fn remote_packet_event_encode_decode_udp() {
        let event = RemotePacketEvent {
            deliver_time: EmulatedTime::SIMULATION_START + SimulationTime::from_nanos(1000),
            src_host_id: HostId::from(0u32),
            src_host_event_id: 42,
            dst_host_id: HostId::from(1u32),
            packet: SerializedPacket::Udp {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 8000,
                dst_port: 9000,
                priority: 0u64,
                payload: b"test".to_vec(),
            },
        };
        let batch = RemotePacketEvent::encode_batch(&[event]);
        let decoded = RemotePacketEvent::decode_batch(&batch).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].src_host_id, HostId::from(0u32));
        assert_eq!(decoded[0].src_host_event_id, 42);
        assert_eq!(decoded[0].dst_host_id, HostId::from(1u32));
    }

    #[test]
    fn remote_packet_event_deterministic_order() {
        let e1 = RemotePacketEvent {
            deliver_time: EmulatedTime::SIMULATION_START + SimulationTime::from_nanos(100),
            src_host_id: HostId::from(0u32),
            src_host_event_id: 0,
            dst_host_id: HostId::from(1u32),
            packet: SerializedPacket::Udp {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 0,
                dst_port: 0,
                priority: 0u64,
                payload: vec![],
            },
        };
        let e2 = RemotePacketEvent {
            deliver_time: EmulatedTime::SIMULATION_START + SimulationTime::from_nanos(200),
            src_host_id: HostId::from(0u32),
            src_host_event_id: 1,
            dst_host_id: HostId::from(1u32),
            packet: SerializedPacket::Udp {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 0,
                dst_port: 0,
                priority: 0u64,
                payload: vec![],
            },
        };
        let batch = RemotePacketEvent::encode_batch(&[e1.clone(), e2.clone()]);
        let decoded = RemotePacketEvent::decode_batch(&batch).unwrap();
        assert_eq!(decoded[0].deliver_time, e1.deliver_time);
        assert_eq!(decoded[1].deliver_time, e2.deliver_time);
    }

    #[test]
    fn into_local_event_rejects_non_local() {
        let hosts: Vec<HostId> = vec![HostId::from(0), HostId::from(1)];
        let map = PartitionMap::by_host_id_modulo(&hosts, 2).unwrap();
        let event = RemotePacketEvent {
            deliver_time: EmulatedTime::SIMULATION_START,
            src_host_id: HostId::from(0u32),
            src_host_event_id: 0,
            dst_host_id: HostId::from(1u32),
            packet: SerializedPacket::Udp {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 0,
                dst_port: 0,
                priority: 0u64,
                payload: vec![],
            },
        };
        // dst_host_id=1 belongs to shard 1, not shard 0
        let result = event.into_local_event(ShardId(0), &map);
        assert!(result.is_err());
    }
}
