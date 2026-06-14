use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, ErrorKind, Read, Write};
use std::net::SocketAddrV4;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use shadow_shim_helper_rs::HostId;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;

use crate::core::work::event::Event;
use crate::host::network::interface::FifoPacketPriority;
use crate::network::packet::{IanaProtocol, PacketRc};

/// Identifies a Shadow shard in a distributed simulation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ShardId(pub u32);

/// The default shard used by regular single-process Shadow runs.
pub const DEFAULT_SHARD_ID: ShardId = ShardId(0);

/// A remote packet event paired with the shard that should receive it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundRemotePacket {
    pub dst_shard: ShardId,
    pub event: RemotePacketEvent,
}

/// Thread-safe collection of remote packet events produced during a scheduling window.
#[derive(Debug, Default)]
pub struct OutboundRemotePacketBuffer {
    packets: Mutex<Vec<OutboundRemotePacket>>,
}

impl OutboundRemotePacketBuffer {
    pub fn push(&self, dst_shard: ShardId, event: RemotePacketEvent) {
        self.packets
            .lock()
            .unwrap()
            .push(OutboundRemotePacket { dst_shard, event });
    }

    pub fn drain_sorted(&self) -> Vec<OutboundRemotePacket> {
        let mut packets = std::mem::take(&mut *self.packets.lock().unwrap());
        packets.sort_by(|a, b| {
            a.dst_shard
                .cmp(&b.dst_shard)
                .then_with(|| remote_packet_event_cmp(&a.event, &b.event))
        });
        packets
    }

    pub fn len(&self) -> usize {
        self.packets.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait RemotePacketExchange: Send + Sync {
    fn send(&self, packets: Vec<OutboundRemotePacket>) -> Result<(), RemotePacketExchangeError>;

    fn receive(&self, shard: ShardId) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError>;
}

pub trait DistributedSynchronizer: Send + Sync {
    fn wait(&self) -> Result<(), RemotePacketExchangeError>;

    fn wait_for_global_min_next_event(
        &self,
        local_min_next_event_time: EmulatedTime,
    ) -> Result<EmulatedTime, RemotePacketExchangeError>;
}

#[cfg(feature = "distributed_mpi")]
pub(crate) mod mpi_backend {
    pub(crate) use rsmpi as mpi;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DistributedPacketExchangeBackend {
    UnixSocket,
}

#[derive(Debug)]
pub struct DistributedPacketExchangeContext {
    backend: DistributedPacketExchangeBackend,
    socket_dir: SocketDirectoryLifecycle,
}

#[derive(Debug)]
pub struct DistributedControlClient {
    stream: Mutex<UnixStream>,
    current_shard: ShardId,
    next_round: Mutex<u64>,
}

impl DistributedControlClient {
    pub fn connect(
        socket_dir: impl AsRef<Path>,
        current_shard: ShardId,
    ) -> Result<Self, RemotePacketExchangeError> {
        let path = unix_control_socket_path(socket_dir.as_ref());
        let stream = UnixStream::connect(&path).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to connect to distributed control socket '{}': {e}",
                path.display()
            ))
        })?;

        Ok(Self {
            stream: Mutex::new(stream),
            current_shard,
            next_round: Mutex::new(0),
        })
    }

    pub fn wait(&self) -> Result<(), RemotePacketExchangeError> {
        self.wait_round(None)?;
        Ok(())
    }

    pub fn wait_for_global_min_next_event(
        &self,
        local_min_next_event_time: EmulatedTime,
    ) -> Result<EmulatedTime, RemotePacketExchangeError> {
        let response = self.wait_round(Some(local_min_next_event_time))?;
        let value = response.min_next_event_time.ok_or_else(|| {
            RemotePacketExchangeError::Backend(
                "distributed control response did not include a global minimum next-event time"
                    .to_string(),
            )
        })?;

        EmulatedTime::from_c_emutime(value).ok_or_else(|| {
            RemotePacketExchangeError::Backend(format!(
                "invalid emulated time {value} in distributed control response"
            ))
        })
    }

    fn wait_round(
        &self,
        min_next_event_time: Option<EmulatedTime>,
    ) -> Result<WireControlResponse, RemotePacketExchangeError> {
        let mut next_round = self.next_round.lock().unwrap();
        let round = *next_round;
        *next_round += 1;
        drop(next_round);

        let request = WireControlRequest {
            shard_id: self.current_shard.0,
            round,
            min_next_event_time: min_next_event_time
                .map(|time| EmulatedTime::to_c_emutime(Some(time))),
        };

        let mut stream = self.stream.lock().unwrap();
        write_control_message(&mut stream, &request)?;
        read_control_response(&mut stream)
    }
}

impl DistributedSynchronizer for DistributedControlClient {
    fn wait(&self) -> Result<(), RemotePacketExchangeError> {
        DistributedControlClient::wait(self)
    }

    fn wait_for_global_min_next_event(
        &self,
        local_min_next_event_time: EmulatedTime,
    ) -> Result<EmulatedTime, RemotePacketExchangeError> {
        DistributedControlClient::wait_for_global_min_next_event(self, local_min_next_event_time)
    }
}

#[derive(Debug)]
pub struct DistributedControlServer {
    socket_path: PathBuf,
    shard_count: u32,
    thread: Option<std::thread::JoinHandle<Result<(), RemotePacketExchangeError>>>,
}

impl DistributedControlServer {
    pub fn start(
        socket_dir: impl AsRef<Path>,
        shard_count: u32,
    ) -> Result<Self, RemotePacketExchangeError> {
        let socket_path = unix_control_socket_path(socket_dir.as_ref());
        let listener = UnixListener::bind(&socket_path).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to bind distributed control socket '{}': {e}",
                socket_path.display()
            ))
        })?;
        let thread = std::thread::spawn(move || run_control_server(listener, shard_count));

        Ok(Self {
            socket_path,
            shard_count,
            thread: Some(thread),
        })
    }

    pub fn shutdown(mut self) -> Result<(), RemotePacketExchangeError> {
        let result = self.shutdown_inner();
        self.remove_socket_path();
        result
    }

    fn shutdown_inner(&mut self) -> Result<(), RemotePacketExchangeError> {
        self.wake_accept_loop();

        let Some(thread) = self.thread.take() else {
            return Ok(());
        };

        thread.join().map_err(|_| {
            RemotePacketExchangeError::Backend(
                "distributed control server thread panicked".to_string(),
            )
        })?
    }

    fn wake_accept_loop(&self) {
        for _ in 0..self.shard_count {
            let _ = UnixStream::connect(&self.socket_path);
        }
    }

    fn remove_socket_path(&self) {
        match std::fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => log::debug!(
                "failed to remove distributed control socket '{}': {e}",
                self.socket_path.display()
            ),
        }
    }
}

impl Drop for DistributedControlServer {
    fn drop(&mut self) {
        if let Err(e) = self.shutdown_inner() {
            log::debug!(
                "distributed control server '{}' stopped: {e}",
                self.socket_path.display()
            );
        }
        self.remove_socket_path();
    }
}

impl DistributedPacketExchangeContext {
    pub fn temporary(
        backend: DistributedPacketExchangeBackend,
    ) -> Result<Self, RemotePacketExchangeError> {
        let socket_dir = tempfile::Builder::new()
            .prefix("shadow-distributed-")
            .tempdir()
            .map_err(|e| {
                RemotePacketExchangeError::Backend(format!(
                    "failed to create temporary distributed IPC socket directory: {e}"
                ))
            })?;

        Ok(Self {
            backend,
            socket_dir: SocketDirectoryLifecycle::Temporary(socket_dir),
        })
    }

    pub fn external(
        backend: DistributedPacketExchangeBackend,
        socket_dir: impl AsRef<Path>,
    ) -> Result<Self, RemotePacketExchangeError> {
        let socket_dir = socket_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&socket_dir).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to create distributed IPC socket directory '{}': {e}",
                socket_dir.display()
            ))
        })?;

        Ok(Self {
            backend,
            socket_dir: SocketDirectoryLifecycle::External(socket_dir),
        })
    }

    pub fn backend(&self) -> DistributedPacketExchangeBackend {
        self.backend
    }

    pub fn socket_dir(&self) -> &Path {
        self.socket_dir.path()
    }

    pub fn build_exchange(
        &self,
        current_shard: ShardId,
    ) -> Result<Box<dyn RemotePacketExchange>, RemotePacketExchangeError> {
        match self.backend {
            DistributedPacketExchangeBackend::UnixSocket => Ok(Box::new(
                UnixSocketRemotePacketExchange::bind(self.socket_dir(), current_shard)?,
            )),
        }
    }
}

#[derive(Debug)]
enum SocketDirectoryLifecycle {
    Temporary(tempfile::TempDir),
    External(PathBuf),
}

impl SocketDirectoryLifecycle {
    fn path(&self) -> &Path {
        match self {
            Self::Temporary(dir) => dir.path(),
            Self::External(path) => path,
        }
    }
}

impl<T: RemotePacketExchange + ?Sized> RemotePacketExchange for Arc<T> {
    fn send(&self, packets: Vec<OutboundRemotePacket>) -> Result<(), RemotePacketExchangeError> {
        self.as_ref().send(packets)
    }

    fn receive(&self, shard: ShardId) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError> {
        self.as_ref().receive(shard)
    }
}

#[derive(Debug, Default)]
pub struct NoopRemotePacketExchange;

impl RemotePacketExchange for NoopRemotePacketExchange {
    fn send(&self, packets: Vec<OutboundRemotePacket>) -> Result<(), RemotePacketExchangeError> {
        if packets.is_empty() {
            Ok(())
        } else {
            Err(RemotePacketExchangeError::Backend(format!(
                "{} remote packets were produced, but no remote packet exchange backend is configured",
                packets.len()
            )))
        }
    }

    fn receive(
        &self,
        _shard: ShardId,
    ) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
pub struct InProcessRemotePacketExchange {
    pending: Mutex<HashMap<ShardId, Vec<RemotePacketEvent>>>,
}

impl RemotePacketExchange for InProcessRemotePacketExchange {
    fn send(&self, packets: Vec<OutboundRemotePacket>) -> Result<(), RemotePacketExchangeError> {
        let mut pending = self.pending.lock().unwrap();
        for packet in packets {
            pending
                .entry(packet.dst_shard)
                .or_default()
                .push(packet.event);
        }
        Ok(())
    }

    fn receive(&self, shard: ShardId) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError> {
        let mut packets = self
            .pending
            .lock()
            .unwrap()
            .remove(&shard)
            .unwrap_or_default();
        packets.sort_by(remote_packet_event_cmp);
        Ok(packets)
    }
}

#[derive(Debug)]
pub struct UnixSocketRemotePacketExchange {
    current_shard: ShardId,
    socket_dir: PathBuf,
    socket_path: PathBuf,
    listener: UnixListener,
}

impl UnixSocketRemotePacketExchange {
    pub fn bind(
        socket_dir: impl AsRef<Path>,
        current_shard: ShardId,
    ) -> Result<Self, RemotePacketExchangeError> {
        let socket_dir = socket_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&socket_dir).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to create IPC socket directory '{}': {e}",
                socket_dir.display()
            ))
        })?;

        let path = unix_socket_path(&socket_dir, current_shard);
        let listener = UnixListener::bind(&path).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to bind IPC socket '{}': {e}",
                path.display()
            ))
        })?;
        listener.set_nonblocking(true).map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to set IPC socket '{}' nonblocking: {e}",
                path.display()
            ))
        })?;

        Ok(Self {
            current_shard,
            socket_dir,
            socket_path: path,
            listener,
        })
    }
}

impl Drop for UnixSocketRemotePacketExchange {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.socket_path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => log::debug!(
                "failed to remove distributed IPC socket '{}': {e}",
                self.socket_path.display()
            ),
        }
    }
}

impl RemotePacketExchange for UnixSocketRemotePacketExchange {
    fn send(&self, packets: Vec<OutboundRemotePacket>) -> Result<(), RemotePacketExchangeError> {
        let mut by_shard: BTreeMap<ShardId, Vec<RemotePacketEvent>> = BTreeMap::new();
        for packet in packets {
            by_shard
                .entry(packet.dst_shard)
                .or_default()
                .push(packet.event);
        }

        for (dst_shard, packets) in by_shard {
            let path = unix_socket_path(&self.socket_dir, dst_shard);
            let mut stream = UnixStream::connect(&path).map_err(|e| {
                RemotePacketExchangeError::Backend(format!(
                    "failed to connect to shard {dst_shard:?} IPC socket '{}': {e}",
                    path.display()
                ))
            })?;
            let payload = encode_remote_packet_batch(&packets)?;
            stream.write_all(&payload).map_err(|e| {
                RemotePacketExchangeError::Backend(format!(
                    "failed to write remote packet batch for shard {dst_shard:?}: {e}"
                ))
            })?;
            stream.shutdown(std::net::Shutdown::Write).map_err(|e| {
                RemotePacketExchangeError::Backend(format!("failed to finish IPC write: {e}"))
            })?;
        }

        Ok(())
    }

    fn receive(&self, shard: ShardId) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError> {
        if shard != self.current_shard {
            return Err(RemotePacketExchangeError::Backend(format!(
                "IPC exchange for shard {:?} cannot receive packets for shard {:?}",
                self.current_shard, shard
            )));
        }

        let mut packets = Vec::new();
        loop {
            let (mut stream, _) = match self.listener.accept() {
                Ok(accepted) => accepted,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    return Err(RemotePacketExchangeError::Backend(format!(
                        "failed to accept IPC packet batch for shard {shard:?}: {e}"
                    )));
                }
            };

            let mut payload = Vec::new();
            stream.read_to_end(&mut payload).map_err(|e| {
                RemotePacketExchangeError::Backend(format!(
                    "failed to read IPC packet batch for shard {shard:?}: {e}"
                ))
            })?;
            packets.extend(decode_remote_packet_batch(&payload, shard)?);
        }

        packets.sort_by(remote_packet_event_cmp);
        Ok(packets)
    }
}

#[derive(Debug)]
pub enum RemotePacketExchangeError {
    Backend(String),
    Delivery(RemotePacketDeliveryError),
}

impl std::fmt::Display for RemotePacketExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(msg) => write!(f, "remote packet exchange failed: {msg}"),
            Self::Delivery(err) => write!(f, "remote packet delivery failed: {err}"),
        }
    }
}

impl std::error::Error for RemotePacketExchangeError {}

impl From<RemotePacketDeliveryError> for RemotePacketExchangeError {
    fn from(value: RemotePacketDeliveryError) -> Self {
        Self::Delivery(value)
    }
}

fn unix_socket_path(socket_dir: &Path, shard: ShardId) -> PathBuf {
    socket_dir.join(format!("shadow-shard-{}.sock", shard.0))
}

fn unix_control_socket_path(socket_dir: &Path) -> PathBuf {
    socket_dir.join("shadow-control.sock")
}

const WIRE_MAGIC: [u8; 4] = *b"SHDW";
const WIRE_VERSION: u8 = 1;
const WIRE_KIND_CONTROL_REQUEST: u8 = 1;
const WIRE_KIND_CONTROL_RESPONSE: u8 = 2;
const WIRE_KIND_PACKET_BATCH: u8 = 3;
const MAX_CONTROL_MESSAGE_BYTES: usize = 1024;
const MAX_PACKET_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_PACKET_BATCH_EVENTS: usize = 1_000_000;
const MAX_PACKET_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_TCP_SELECTIVE_ACKS: usize = 4;

fn run_control_server(
    listener: UnixListener,
    shard_count: u32,
) -> Result<(), RemotePacketExchangeError> {
    let mut streams = Vec::new();
    for _ in 0..shard_count {
        let (stream, _) = listener.accept().map_err(|e| {
            RemotePacketExchangeError::Backend(format!(
                "failed to accept distributed control connection: {e}"
            ))
        })?;
        streams.push(stream);
    }

    let mut expected_round = 0;
    loop {
        let mut requests = Vec::with_capacity(streams.len());
        for stream in &mut streams {
            match read_control_request(stream) {
                Ok(request) => requests.push(request),
                Err(RemotePacketExchangeError::Backend(msg))
                    if msg.contains("control channel closed") =>
                {
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }

        let response = control_response_for_round(&requests, expected_round, shard_count)?;
        expected_round += 1;

        for stream in &mut streams {
            write_control_response(stream, &response)?;
        }
    }
}

fn control_response_for_round(
    requests: &[WireControlRequest],
    expected_round: u64,
    shard_count: u32,
) -> Result<WireControlResponse, RemotePacketExchangeError> {
    let mut seen_shards = std::collections::HashSet::new();
    let mut min_next_event_time = None;

    for request in requests {
        if request.round != expected_round {
            return Err(RemotePacketExchangeError::Backend(format!(
                "distributed control expected round {}, got round {} from shard {}",
                expected_round, request.round, request.shard_id
            )));
        }
        if request.shard_id >= shard_count {
            return Err(RemotePacketExchangeError::Backend(format!(
                "distributed control got invalid shard id {} for shard count {}",
                request.shard_id, shard_count
            )));
        }
        if !seen_shards.insert(request.shard_id) {
            return Err(RemotePacketExchangeError::Backend(format!(
                "distributed control got duplicate request from shard {} in round {}",
                request.shard_id, expected_round
            )));
        }

        min_next_event_time = [min_next_event_time, request.min_next_event_time]
            .into_iter()
            .flatten()
            .min();
    }

    Ok(WireControlResponse {
        min_next_event_time,
    })
}

fn write_control_message<T: BinaryWire>(
    stream: &mut UnixStream,
    message: &T,
) -> Result<(), RemotePacketExchangeError> {
    let payload = encode_wire_message(T::KIND, &message.encode()?)?;
    let len = u32::try_from(payload.len()).map_err(|_| {
        RemotePacketExchangeError::Backend(format!(
            "distributed control message is too large: {} bytes",
            payload.len()
        ))
    })?;

    stream.write_all(&len.to_be_bytes()).map_err(|e| {
        RemotePacketExchangeError::Backend(format!(
            "failed to write distributed control message length: {e}"
        ))
    })?;
    stream.write_all(&payload).map_err(|e| {
        RemotePacketExchangeError::Backend(format!(
            "failed to write distributed control message payload: {e}"
        ))
    })
}

fn read_control_request(
    stream: &mut UnixStream,
) -> Result<WireControlRequest, RemotePacketExchangeError> {
    read_control_message(stream, "request")
}

fn read_control_response(
    stream: &mut UnixStream,
) -> Result<WireControlResponse, RemotePacketExchangeError> {
    read_control_message(stream, "response")
}

fn write_control_response(
    stream: &mut UnixStream,
    response: &WireControlResponse,
) -> Result<(), RemotePacketExchangeError> {
    write_control_message(stream, response)
}

fn read_control_message<T: BinaryWire>(
    stream: &mut UnixStream,
    message_kind: &str,
) -> Result<T, RemotePacketExchangeError> {
    let mut len = [0; 4];
    stream.read_exact(&mut len).map_err(|e| {
        if e.kind() == ErrorKind::UnexpectedEof {
            RemotePacketExchangeError::Backend("distributed control channel closed".to_string())
        } else {
            RemotePacketExchangeError::Backend(format!(
                "failed to read distributed control {message_kind} length: {e}"
            ))
        }
    })?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_CONTROL_MESSAGE_BYTES {
        return Err(RemotePacketExchangeError::Backend(format!(
            "distributed control {message_kind} is too large: {len} bytes exceeds {MAX_CONTROL_MESSAGE_BYTES}"
        )));
    }

    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).map_err(|e| {
        RemotePacketExchangeError::Backend(format!(
            "failed to read distributed control {message_kind} payload: {e}"
        ))
    })?;

    let context = format!("distributed control {message_kind}");
    let body = decode_wire_message(&payload, T::KIND, &context)?;
    T::decode(body, &context)
}

trait BinaryWire: Sized {
    const KIND: u8;

    fn encode(&self) -> Result<Vec<u8>, RemotePacketExchangeError>;

    fn decode(payload: &[u8], context: &str) -> Result<Self, RemotePacketExchangeError>;
}

#[derive(Debug)]
struct WireControlRequest {
    shard_id: u32,
    round: u64,
    min_next_event_time: Option<u64>,
}

impl BinaryWire for WireControlRequest {
    const KIND: u8 = WIRE_KIND_CONTROL_REQUEST;

    fn encode(&self) -> Result<Vec<u8>, RemotePacketExchangeError> {
        let mut out = Vec::new();
        write_u32(&mut out, self.shard_id);
        write_u64(&mut out, self.round);
        write_optional_u64(&mut out, self.min_next_event_time);
        Ok(out)
    }

    fn decode(payload: &[u8], context: &str) -> Result<Self, RemotePacketExchangeError> {
        let mut cursor = Cursor::new(payload);
        let value = Self {
            shard_id: read_u32(&mut cursor, context)?,
            round: read_u64(&mut cursor, context)?,
            min_next_event_time: read_optional_u64(&mut cursor, context)?,
        };
        reject_trailing_bytes(&cursor, context)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WireControlResponse {
    min_next_event_time: Option<u64>,
}

impl BinaryWire for WireControlResponse {
    const KIND: u8 = WIRE_KIND_CONTROL_RESPONSE;

    fn encode(&self) -> Result<Vec<u8>, RemotePacketExchangeError> {
        let mut out = Vec::new();
        write_optional_u64(&mut out, self.min_next_event_time);
        Ok(out)
    }

    fn decode(payload: &[u8], context: &str) -> Result<Self, RemotePacketExchangeError> {
        let mut cursor = Cursor::new(payload);
        let value = Self {
            min_next_event_time: read_optional_u64(&mut cursor, context)?,
        };
        reject_trailing_bytes(&cursor, context)?;
        Ok(value)
    }
}

#[derive(Debug)]
struct WireRemotePacketEvent {
    deliver_time: u64,
    src_host_id: u32,
    src_host_event_id: u64,
    dst_host_id: u32,
    packet: WireSerializedPacket,
}

impl WireRemotePacketEvent {
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), RemotePacketExchangeError> {
        write_u64(out, self.deliver_time);
        write_u32(out, self.src_host_id);
        write_u64(out, self.src_host_event_id);
        write_u32(out, self.dst_host_id);
        self.packet.encode_into(out)
    }

    fn decode_from(
        cursor: &mut Cursor<&[u8]>,
        context: &str,
    ) -> Result<Self, RemotePacketExchangeError> {
        Ok(Self {
            deliver_time: read_u64(cursor, context)?,
            src_host_id: read_u32(cursor, context)?,
            src_host_event_id: read_u64(cursor, context)?,
            dst_host_id: read_u32(cursor, context)?,
            packet: WireSerializedPacket::decode_from(cursor, context)?,
        })
    }
}

impl From<RemotePacketEvent> for WireRemotePacketEvent {
    fn from(value: RemotePacketEvent) -> Self {
        Self {
            deliver_time: EmulatedTime::to_c_emutime(Some(value.deliver_time)),
            src_host_id: value.src_host_id.into(),
            src_host_event_id: value.src_host_event_id,
            dst_host_id: value.dst_host_id.into(),
            packet: value.packet.into(),
        }
    }
}

impl TryFrom<WireRemotePacketEvent> for RemotePacketEvent {
    type Error = RemotePacketExchangeError;

    fn try_from(value: WireRemotePacketEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            deliver_time: EmulatedTime::from_c_emutime(value.deliver_time).ok_or_else(|| {
                RemotePacketExchangeError::Backend(format!(
                    "invalid emulated delivery time {} in IPC packet batch",
                    value.deliver_time
                ))
            })?,
            src_host_id: HostId::from(value.src_host_id),
            src_host_event_id: value.src_host_event_id,
            dst_host_id: HostId::from(value.dst_host_id),
            packet: value.packet.into(),
        })
    }
}

#[derive(Debug)]
enum WireSerializedPacket {
    Udp {
        src_ip: u32,
        src_port: u16,
        dst_ip: u32,
        dst_port: u16,
        priority: FifoPacketPriority,
        payload: Vec<u8>,
    },
    Tcp {
        src_ip: u32,
        src_port: u16,
        dst_ip: u32,
        dst_port: u16,
        priority: FifoPacketPriority,
        flags: u8,
        seq: u32,
        ack: u32,
        window_size: u16,
        selective_acks: Option<Vec<(u32, u32)>>,
        window_scale: Option<u8>,
        timestamp: Option<u32>,
        timestamp_echo: Option<u32>,
        payload: Vec<u8>,
    },
}

impl WireSerializedPacket {
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), RemotePacketExchangeError> {
        match self {
            Self::Udp {
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                priority,
                payload,
            } => {
                write_u8(out, 1);
                write_u32(out, *src_ip);
                write_u16(out, *src_port);
                write_u32(out, *dst_ip);
                write_u16(out, *dst_port);
                write_u64(out, *priority);
                write_bytes(out, payload)?;
            }
            Self::Tcp {
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                priority,
                flags,
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            } => {
                write_u8(out, 2);
                write_u32(out, *src_ip);
                write_u16(out, *src_port);
                write_u32(out, *dst_ip);
                write_u16(out, *dst_port);
                write_u64(out, *priority);
                write_u8(out, *flags);
                write_u32(out, *seq);
                write_u32(out, *ack);
                write_u16(out, *window_size);
                match selective_acks {
                    Some(selective_acks) => {
                        if selective_acks.len() > MAX_TCP_SELECTIVE_ACKS {
                            return Err(RemotePacketExchangeError::Backend(format!(
                                "TCP packet has {} selective ACK ranges, exceeding limit {MAX_TCP_SELECTIVE_ACKS}",
                                selective_acks.len()
                            )));
                        }
                        write_bool(out, true);
                        write_len(out, selective_acks.len())?;
                        for (left, right) in selective_acks {
                            write_u32(out, *left);
                            write_u32(out, *right);
                        }
                    }
                    None => write_bool(out, false),
                }
                write_optional_u8(out, *window_scale);
                write_optional_u32(out, *timestamp);
                write_optional_u32(out, *timestamp_echo);
                write_bytes(out, payload)?;
            }
        };

        Ok(())
    }

    fn decode_from(
        cursor: &mut Cursor<&[u8]>,
        context: &str,
    ) -> Result<Self, RemotePacketExchangeError> {
        match read_u8(cursor, context)? {
            1 => Ok(Self::Udp {
                src_ip: read_u32(cursor, context)?,
                src_port: read_u16(cursor, context)?,
                dst_ip: read_u32(cursor, context)?,
                dst_port: read_u16(cursor, context)?,
                priority: read_u64(cursor, context)?,
                payload: read_bytes(cursor, context)?,
            }),
            2 => {
                let src_ip = read_u32(cursor, context)?;
                let src_port = read_u16(cursor, context)?;
                let dst_ip = read_u32(cursor, context)?;
                let dst_port = read_u16(cursor, context)?;
                let priority = read_u64(cursor, context)?;
                let flags = read_u8(cursor, context)?;
                let seq = read_u32(cursor, context)?;
                let ack = read_u32(cursor, context)?;
                let window_size = read_u16(cursor, context)?;
                let selective_acks = if read_bool(cursor, context)? {
                    let count = read_len(cursor, context)?;
                    if count > MAX_TCP_SELECTIVE_ACKS {
                        return Err(decode_error(
                            context,
                            format!(
                                "TCP selective ACK count {count} exceeds limit {MAX_TCP_SELECTIVE_ACKS}"
                            ),
                        ));
                    }
                    let mut ranges = Vec::with_capacity(count);
                    for _ in 0..count {
                        ranges.push((read_u32(cursor, context)?, read_u32(cursor, context)?));
                    }
                    Some(ranges)
                } else {
                    None
                };

                Ok(Self::Tcp {
                    src_ip,
                    src_port,
                    dst_ip,
                    dst_port,
                    priority,
                    flags,
                    seq,
                    ack,
                    window_size,
                    selective_acks,
                    window_scale: read_optional_u8(cursor, context)?,
                    timestamp: read_optional_u32(cursor, context)?,
                    timestamp_echo: read_optional_u32(cursor, context)?,
                    payload: read_bytes(cursor, context)?,
                })
            }
            tag => Err(decode_error(
                context,
                format!("unknown serialized packet tag {tag}"),
            )),
        }
    }
}

pub(crate) fn encode_remote_packet_batch(
    packets: &[RemotePacketEvent],
) -> Result<Vec<u8>, RemotePacketExchangeError> {
    let packets: Vec<_> = packets
        .iter()
        .cloned()
        .map(WireRemotePacketEvent::from)
        .collect();
    encode_wire_packet_batch(&packets)
}

pub(crate) fn decode_remote_packet_batch(
    payload: &[u8],
    shard: ShardId,
) -> Result<Vec<RemotePacketEvent>, RemotePacketExchangeError> {
    decode_wire_packet_batch(payload, shard)?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

fn encode_wire_packet_batch(
    packets: &[WireRemotePacketEvent],
) -> Result<Vec<u8>, RemotePacketExchangeError> {
    if packets.len() > MAX_PACKET_BATCH_EVENTS {
        return Err(RemotePacketExchangeError::Backend(format!(
            "remote packet batch has {} events, exceeding limit {MAX_PACKET_BATCH_EVENTS}",
            packets.len()
        )));
    }

    let mut body = Vec::new();
    write_len(&mut body, packets.len())?;
    for packet in packets {
        packet.encode_into(&mut body)?;
    }
    let payload = encode_wire_message(WIRE_KIND_PACKET_BATCH, &body)?;
    if payload.len() > MAX_PACKET_BATCH_BYTES {
        return Err(RemotePacketExchangeError::Backend(format!(
            "remote packet batch is too large: {} bytes exceeds {MAX_PACKET_BATCH_BYTES}",
            payload.len()
        )));
    }
    Ok(payload)
}

fn decode_wire_packet_batch(
    payload: &[u8],
    shard: ShardId,
) -> Result<Vec<WireRemotePacketEvent>, RemotePacketExchangeError> {
    let context = format!("IPC packet batch for shard {shard:?}");
    if payload.len() > MAX_PACKET_BATCH_BYTES {
        return Err(RemotePacketExchangeError::Backend(format!(
            "{context} is too large: {} bytes exceeds {MAX_PACKET_BATCH_BYTES}",
            payload.len()
        )));
    }

    let body = decode_wire_message(payload, WIRE_KIND_PACKET_BATCH, &context)?;
    let mut cursor = Cursor::new(body);
    let count = read_len(&mut cursor, &context)?;
    if count > MAX_PACKET_BATCH_EVENTS {
        return Err(decode_error(
            &context,
            format!("packet count {count} exceeds limit {MAX_PACKET_BATCH_EVENTS}"),
        ));
    }
    let mut packets = Vec::with_capacity(count);
    for _ in 0..count {
        packets.push(WireRemotePacketEvent::decode_from(&mut cursor, &context)?);
    }
    reject_trailing_bytes(&cursor, &context)?;
    Ok(packets)
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    write_u8(out, u8::from(value));
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_optional_u8(out: &mut Vec<u8>, value: Option<u8>) {
    match value {
        Some(value) => {
            write_bool(out, true);
            write_u8(out, value);
        }
        None => write_bool(out, false),
    }
}

fn write_optional_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            write_bool(out, true);
            write_u32(out, value);
        }
        None => write_bool(out, false),
    }
}

fn write_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            write_bool(out, true);
            write_u64(out, value);
        }
        None => write_bool(out, false),
    }
}

fn write_len(out: &mut Vec<u8>, len: usize) -> Result<(), RemotePacketExchangeError> {
    let len = u32::try_from(len).map_err(|_| {
        RemotePacketExchangeError::Backend(format!("binary wire length {len} exceeds u32::MAX"))
    })?;
    write_u32(out, len);
    Ok(())
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RemotePacketExchangeError> {
    if bytes.len() > MAX_PACKET_PAYLOAD_BYTES {
        return Err(RemotePacketExchangeError::Backend(format!(
            "packet payload is too large: {} bytes exceeds {MAX_PACKET_PAYLOAD_BYTES}",
            bytes.len()
        )));
    }
    write_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<u8, RemotePacketExchangeError> {
    Ok(read_array::<1>(cursor, context)?[0])
}

fn read_bool(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<bool, RemotePacketExchangeError> {
    match read_u8(cursor, context)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(decode_error(
            context,
            format!("invalid boolean value {value}"),
        )),
    }
}

fn read_u16(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<u16, RemotePacketExchangeError> {
    Ok(u16::from_be_bytes(read_array(cursor, context)?))
}

fn read_u32(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<u32, RemotePacketExchangeError> {
    Ok(u32::from_be_bytes(read_array(cursor, context)?))
}

fn read_u64(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<u64, RemotePacketExchangeError> {
    Ok(u64::from_be_bytes(read_array(cursor, context)?))
}

fn read_optional_u8(
    cursor: &mut Cursor<&[u8]>,
    context: &str,
) -> Result<Option<u8>, RemotePacketExchangeError> {
    Ok(read_bool(cursor, context)?
        .then(|| read_u8(cursor, context))
        .transpose()?)
}

fn read_optional_u32(
    cursor: &mut Cursor<&[u8]>,
    context: &str,
) -> Result<Option<u32>, RemotePacketExchangeError> {
    Ok(read_bool(cursor, context)?
        .then(|| read_u32(cursor, context))
        .transpose()?)
}

fn read_optional_u64(
    cursor: &mut Cursor<&[u8]>,
    context: &str,
) -> Result<Option<u64>, RemotePacketExchangeError> {
    Ok(read_bool(cursor, context)?
        .then(|| read_u64(cursor, context))
        .transpose()?)
}

fn read_len(cursor: &mut Cursor<&[u8]>, context: &str) -> Result<usize, RemotePacketExchangeError> {
    Ok(read_u32(cursor, context)? as usize)
}

fn read_bytes(
    cursor: &mut Cursor<&[u8]>,
    context: &str,
) -> Result<Vec<u8>, RemotePacketExchangeError> {
    let len = read_len(cursor, context)?;
    if len > MAX_PACKET_PAYLOAD_BYTES {
        return Err(decode_error(
            context,
            format!("byte array length {len} exceeds limit {MAX_PACKET_PAYLOAD_BYTES}"),
        ));
    }
    let mut bytes = vec![0; len];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| decode_error(context, format!("truncated byte array of length {len}")))?;
    Ok(bytes)
}

fn read_array<const N: usize>(
    cursor: &mut Cursor<&[u8]>,
    context: &str,
) -> Result<[u8; N], RemotePacketExchangeError> {
    let mut bytes = [0; N];
    cursor
        .read_exact(&mut bytes)
        .map_err(|_| decode_error(context, format!("truncated field of length {N}")))?;
    Ok(bytes)
}

fn reject_trailing_bytes(
    cursor: &Cursor<&[u8]>,
    context: &str,
) -> Result<(), RemotePacketExchangeError> {
    let remaining = cursor.get_ref().len() - cursor.position() as usize;
    if remaining == 0 {
        Ok(())
    } else {
        Err(decode_error(
            context,
            format!("{remaining} trailing bytes after message"),
        ))
    }
}

fn decode_error(context: &str, msg: impl std::fmt::Display) -> RemotePacketExchangeError {
    RemotePacketExchangeError::Backend(format!("failed to decode {context}: {msg}"))
}

fn encode_wire_message(kind: u8, body: &[u8]) -> Result<Vec<u8>, RemotePacketExchangeError> {
    let mut out = Vec::with_capacity(WIRE_MAGIC.len() + 2 + body.len());
    out.extend_from_slice(&WIRE_MAGIC);
    write_u8(&mut out, WIRE_VERSION);
    write_u8(&mut out, kind);
    out.extend_from_slice(body);
    Ok(out)
}

fn decode_wire_message<'a>(
    payload: &'a [u8],
    expected_kind: u8,
    context: &str,
) -> Result<&'a [u8], RemotePacketExchangeError> {
    let mut cursor = Cursor::new(payload);
    let magic = read_array::<4>(&mut cursor, context)?;
    if magic != WIRE_MAGIC {
        return Err(decode_error(context, "invalid wire magic"));
    }

    let version = read_u8(&mut cursor, context)?;
    if version != WIRE_VERSION {
        return Err(decode_error(
            context,
            format!("unsupported wire version {version}"),
        ));
    }

    let kind = read_u8(&mut cursor, context)?;
    if kind != expected_kind {
        return Err(decode_error(
            context,
            format!("unexpected message kind {kind}, expected {expected_kind}"),
        ));
    }

    Ok(&payload[cursor.position() as usize..])
}

impl From<SerializedPacket> for WireSerializedPacket {
    fn from(value: SerializedPacket) -> Self {
        match value {
            SerializedPacket::Udp {
                src,
                dst,
                priority,
                payload,
            } => Self::Udp {
                src_ip: u32::from(*src.ip()),
                src_port: src.port(),
                dst_ip: u32::from(*dst.ip()),
                dst_port: dst.port(),
                priority,
                payload,
            },
            SerializedPacket::Tcp {
                src,
                dst,
                priority,
                flags,
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            } => Self::Tcp {
                src_ip: u32::from(*src.ip()),
                src_port: src.port(),
                dst_ip: u32::from(*dst.ip()),
                dst_port: dst.port(),
                priority,
                flags: flags.bits(),
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            },
        }
    }
}

impl From<WireSerializedPacket> for SerializedPacket {
    fn from(value: WireSerializedPacket) -> Self {
        match value {
            WireSerializedPacket::Udp {
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                priority,
                payload,
            } => Self::Udp {
                src: SocketAddrV4::new(src_ip.into(), src_port),
                dst: SocketAddrV4::new(dst_ip.into(), dst_port),
                priority,
                payload,
            },
            WireSerializedPacket::Tcp {
                src_ip,
                src_port,
                dst_ip,
                dst_port,
                priority,
                flags,
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            } => Self::Tcp {
                src: SocketAddrV4::new(src_ip.into(), src_port),
                dst: SocketAddrV4::new(dst_ip.into(), dst_port),
                priority,
                flags: tcp::TcpFlags::from_bits_retain(flags),
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            },
        }
    }
}

/// A packet arrival that must be delivered by another shard in a future window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemotePacketEvent {
    pub deliver_time: EmulatedTime,
    pub src_host_id: HostId,
    pub src_host_event_id: u64,
    pub dst_host_id: HostId,
    pub packet: SerializedPacket,
}

impl RemotePacketEvent {
    pub fn new(
        deliver_time: EmulatedTime,
        src_host_id: HostId,
        src_host_event_id: u64,
        dst_host_id: HostId,
        packet: &PacketRc,
    ) -> Result<Self, PacketSerializationError> {
        Ok(Self {
            deliver_time,
            src_host_id,
            src_host_event_id,
            dst_host_id,
            packet: SerializedPacket::try_from_packet(packet)?,
        })
    }

    pub fn into_local_event(
        self,
        current_shard: ShardId,
        partition_map: &PartitionMap,
    ) -> Result<(HostId, Event), RemotePacketDeliveryError> {
        let dst_shard = partition_map.shard_for_host(self.dst_host_id).ok_or(
            RemotePacketDeliveryError::UnknownDestinationHost(self.dst_host_id),
        )?;

        if dst_shard != current_shard {
            return Err(RemotePacketDeliveryError::NonLocalDestination {
                host_id: self.dst_host_id,
                host_shard: dst_shard,
                current_shard,
            });
        }

        let dst_host_id = self.dst_host_id;
        let event = Event::new_packet_with_meta(
            self.packet.into_packet(),
            self.deliver_time,
            self.src_host_id,
            self.src_host_event_id,
        );

        Ok((dst_host_id, event))
    }

    pub fn packet_payload_len(&self) -> usize {
        self.packet.payload_len()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RemotePacketDeliveryError {
    UnknownDestinationHost(HostId),
    NonLocalDestination {
        host_id: HostId,
        host_shard: ShardId,
        current_shard: ShardId,
    },
}

impl std::fmt::Display for RemotePacketDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownDestinationHost(host_id) => {
                write!(
                    f,
                    "destination host {host_id:?} is not in the partition map"
                )
            }
            Self::NonLocalDestination {
                host_id,
                host_shard,
                current_shard,
            } => write!(
                f,
                "destination host {host_id:?} belongs to shard {host_shard:?}, not current shard {current_shard:?}"
            ),
        }
    }
}

impl std::error::Error for RemotePacketDeliveryError {}

/// A deterministic, address-independent packet representation for shard transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SerializedPacket {
    Udp {
        src: SocketAddrV4,
        dst: SocketAddrV4,
        priority: FifoPacketPriority,
        payload: Vec<u8>,
    },
    Tcp {
        src: SocketAddrV4,
        dst: SocketAddrV4,
        priority: FifoPacketPriority,
        flags: tcp::TcpFlags,
        seq: u32,
        ack: u32,
        window_size: u16,
        selective_acks: Option<Vec<(u32, u32)>>,
        window_scale: Option<u8>,
        timestamp: Option<u32>,
        timestamp_echo: Option<u32>,
        payload: Vec<u8>,
    },
}

impl SerializedPacket {
    pub fn payload_len(&self) -> usize {
        match self {
            Self::Udp { payload, .. } | Self::Tcp { payload, .. } => payload.len(),
        }
    }

    pub fn try_from_packet(packet: &PacketRc) -> Result<Self, PacketSerializationError> {
        match packet.iana_protocol() {
            IanaProtocol::Udp => Ok(Self::Udp {
                src: packet.src_ipv4_address(),
                dst: packet.dst_ipv4_address(),
                priority: packet.priority(),
                payload: packet_payload_bytes(packet),
            }),
            IanaProtocol::Tcp if packet.is_legacy_tcp() => Err(PacketSerializationError::LegacyTcp),
            IanaProtocol::Tcp => {
                let header = packet
                    .ipv4_tcp_header()
                    .ok_or(PacketSerializationError::MissingTcpHeader)?;
                Ok(Self::Tcp {
                    src: header.src(),
                    dst: header.dst(),
                    priority: packet.priority(),
                    flags: header.flags,
                    seq: header.seq,
                    ack: header.ack,
                    window_size: header.window_size,
                    selective_acks: header
                        .selective_acks
                        .map(|selective_acks| selective_acks.as_ref().to_vec()),
                    window_scale: header.window_scale,
                    timestamp: header.timestamp,
                    timestamp_echo: header.timestamp_echo,
                    payload: packet_payload_bytes(packet),
                })
            }
        }
    }

    pub fn into_packet(self) -> PacketRc {
        match self {
            Self::Udp {
                src,
                dst,
                priority,
                payload,
            } => PacketRc::new_ipv4_udp(src, dst, payload.into(), priority),
            Self::Tcp {
                src,
                dst,
                priority,
                flags,
                seq,
                ack,
                window_size,
                selective_acks,
                window_scale,
                timestamp,
                timestamp_echo,
                payload,
            } => PacketRc::new_ipv4_tcp(
                tcp::TcpHeader {
                    ip: tcp::Ipv4Header {
                        src: *src.ip(),
                        dst: *dst.ip(),
                    },
                    flags,
                    src_port: src.port(),
                    dst_port: dst.port(),
                    seq,
                    ack,
                    window_size,
                    selective_acks: selective_acks.map(|selective_acks| {
                        tcp::util::SmallArrayBackedSlice::<4, (u32, u32)>::new(&selective_acks)
                            .unwrap()
                    }),
                    window_scale,
                    timestamp,
                    timestamp_echo,
                },
                tcp::Payload(vec![payload.into()]),
                priority,
            ),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PacketSerializationError {
    LegacyTcp,
    MissingTcpHeader,
}

impl std::fmt::Display for PacketSerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyTcp => write!(
                f,
                "serializing legacy TCP packets is not implemented; distributed TCP currently requires experimental.use_new_tcp=true"
            ),
            Self::MissingTcpHeader => write!(f, "TCP packet is missing an IPv4 TCP header"),
        }
    }
}

impl std::error::Error for PacketSerializationError {}

/// Maps globally assigned virtual hosts to their owning shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionMap {
    host_to_shard: HashMap<HostId, ShardId>,
}

impl PartitionMap {
    /// Create a partition map that places every host on the default shard.
    pub fn all_local(host_ids: impl IntoIterator<Item = HostId>) -> Self {
        Self {
            host_to_shard: host_ids
                .into_iter()
                .map(|host_id| (host_id, DEFAULT_SHARD_ID))
                .collect(),
        }
    }

    pub fn from_host_shards(host_shards: impl IntoIterator<Item = (HostId, ShardId)>) -> Self {
        Self {
            host_to_shard: host_shards.into_iter().collect(),
        }
    }

    pub fn by_host_id_modulo(
        host_ids: impl IntoIterator<Item = HostId>,
        shard_count: u32,
    ) -> Result<Self, PartitionMapError> {
        if shard_count == 0 {
            return Err(PartitionMapError::ZeroShardCount);
        }

        Ok(Self::from_host_shards(host_ids.into_iter().map(
            |host_id| {
                let id: u32 = host_id.into();
                (host_id, ShardId(id % shard_count))
            },
        )))
    }

    pub fn shard_for_host(&self, host_id: HostId) -> Option<ShardId> {
        self.host_to_shard.get(&host_id).copied()
    }

    pub fn is_host_local(&self, current_shard: ShardId, host_id: HostId) -> Option<bool> {
        self.shard_for_host(host_id)
            .map(|host_shard| host_shard == current_shard)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PartitionMapError {
    ZeroShardCount,
}

impl std::fmt::Display for PartitionMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroShardCount => write!(f, "shard count must be greater than zero"),
        }
    }
}

impl std::error::Error for PartitionMapError {}

fn packet_payload_bytes(packet: &PacketRc) -> Vec<u8> {
    let mut payload = Vec::with_capacity(packet.payload_len());
    for chunk in packet.payload() {
        payload.extend_from_slice(&chunk);
    }
    payload
}

fn remote_packet_event_cmp(a: &RemotePacketEvent, b: &RemotePacketEvent) -> Ordering {
    a.deliver_time
        .cmp(&b.deliver_time)
        .then_with(|| a.src_host_id.cmp(&b.src_host_id))
        .then_with(|| a.src_host_event_id.cmp(&b.src_host_event_id))
        .then_with(|| a.dst_host_id.cmp(&b.dst_host_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::work::event::EventData;
    use crate::core::work::event_queue::EventQueue;
    use std::net::Ipv4Addr;

    use bytes::Bytes;

    #[test]
    fn all_local_maps_every_host_to_default_shard() {
        let hosts = [HostId::from(0), HostId::from(42)];
        let partition = PartitionMap::all_local(hosts);

        assert_eq!(
            partition.shard_for_host(HostId::from(0)),
            Some(DEFAULT_SHARD_ID)
        );
        assert_eq!(
            partition.shard_for_host(HostId::from(42)),
            Some(DEFAULT_SHARD_ID)
        );
        assert_eq!(partition.shard_for_host(HostId::from(7)), None);
    }

    #[test]
    fn is_host_local_checks_current_shard() {
        let hosts = [HostId::from(0)];
        let partition = PartitionMap::all_local(hosts);

        assert_eq!(
            partition.is_host_local(DEFAULT_SHARD_ID, HostId::from(0)),
            Some(true)
        );
        assert_eq!(
            partition.is_host_local(ShardId(1), HostId::from(0)),
            Some(false)
        );
        assert_eq!(
            partition.is_host_local(DEFAULT_SHARD_ID, HostId::from(1)),
            None
        );
    }

    #[test]
    fn from_host_shards_maps_hosts_to_configured_shards() {
        let partition = PartitionMap::from_host_shards([
            (HostId::from(0), ShardId(0)),
            (HostId::from(1), ShardId(2)),
        ]);

        assert_eq!(partition.shard_for_host(HostId::from(0)), Some(ShardId(0)));
        assert_eq!(partition.shard_for_host(HostId::from(1)), Some(ShardId(2)));
        assert_eq!(partition.shard_for_host(HostId::from(2)), None);
    }

    #[test]
    fn by_host_id_modulo_assigns_stable_shards() {
        let partition = PartitionMap::by_host_id_modulo(
            [
                HostId::from(0),
                HostId::from(1),
                HostId::from(2),
                HostId::from(3),
            ],
            3,
        )
        .unwrap();

        assert_eq!(partition.shard_for_host(HostId::from(0)), Some(ShardId(0)));
        assert_eq!(partition.shard_for_host(HostId::from(1)), Some(ShardId(1)));
        assert_eq!(partition.shard_for_host(HostId::from(2)), Some(ShardId(2)));
        assert_eq!(partition.shard_for_host(HostId::from(3)), Some(ShardId(0)));
    }

    #[test]
    fn by_host_id_modulo_rejects_zero_shards() {
        let err = PartitionMap::by_host_id_modulo([HostId::from(0)], 0).unwrap_err();

        assert_eq!(err, PartitionMapError::ZeroShardCount);
    }

    #[test]
    fn udp_packet_roundtrips_through_serialized_packet() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let payload = Bytes::from_static(b"hello from shard 0");
        let priority = 99;
        let packet = PacketRc::new_ipv4_udp(src, dst, payload.clone(), priority);

        let serialized = SerializedPacket::try_from_packet(&packet).unwrap();
        let roundtripped = serialized.into_packet();

        assert_eq!(roundtripped.iana_protocol(), IanaProtocol::Udp);
        assert_eq!(roundtripped.src_ipv4_address(), src);
        assert_eq!(roundtripped.dst_ipv4_address(), dst);
        assert_eq!(roundtripped.priority(), priority);
        assert_eq!(packet_payload_bytes(&roundtripped), payload.to_vec());
    }

    #[test]
    fn zero_length_udp_packet_roundtrips_through_serialized_packet() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::new(), 3);

        let serialized = SerializedPacket::try_from_packet(&packet).unwrap();
        let roundtripped = serialized.into_packet();

        assert_eq!(roundtripped.iana_protocol(), IanaProtocol::Udp);
        assert_eq!(roundtripped.src_ipv4_address(), src);
        assert_eq!(roundtripped.dst_ipv4_address(), dst);
        assert_eq!(roundtripped.priority(), 3);
        assert_eq!(packet_payload_bytes(&roundtripped), Vec::<u8>::new());
    }

    #[test]
    fn tcp_packet_roundtrips_through_serialized_packet() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let selective_acks =
            tcp::util::SmallArrayBackedSlice::<4, (u32, u32)>::new(&[(10, 20), (30, 40)]).unwrap();
        let header = tcp::TcpHeader {
            ip: tcp::Ipv4Header {
                src: *src.ip(),
                dst: *dst.ip(),
            },
            flags: tcp::TcpFlags::SYN | tcp::TcpFlags::ACK,
            src_port: src.port(),
            dst_port: dst.port(),
            seq: 123,
            ack: 456,
            window_size: 789,
            selective_acks: Some(selective_acks),
            window_scale: Some(7),
            timestamp: Some(111),
            timestamp_echo: Some(222),
        };
        let priority = 99;
        let packet = PacketRc::new_ipv4_tcp(
            header,
            tcp::Payload(vec![
                Bytes::from_static(b"hello "),
                Bytes::from_static(b"tcp"),
            ]),
            priority,
        );

        let serialized = SerializedPacket::try_from_packet(&packet).unwrap();
        let roundtripped = serialized.into_packet();
        let roundtripped_header = roundtripped.ipv4_tcp_header().unwrap();

        assert_eq!(roundtripped.iana_protocol(), IanaProtocol::Tcp);
        assert_eq!(roundtripped.src_ipv4_address(), src);
        assert_eq!(roundtripped.dst_ipv4_address(), dst);
        assert_eq!(roundtripped.priority(), priority);
        assert_eq!(roundtripped_header.flags, header.flags);
        assert_eq!(roundtripped_header.seq, header.seq);
        assert_eq!(roundtripped_header.ack, header.ack);
        assert_eq!(roundtripped_header.window_size, header.window_size);
        assert_eq!(
            roundtripped_header
                .selective_acks
                .map(|selective_acks| selective_acks.as_ref().to_vec()),
            header
                .selective_acks
                .map(|selective_acks| selective_acks.as_ref().to_vec())
        );
        assert_eq!(roundtripped_header.window_scale, header.window_scale);
        assert_eq!(roundtripped_header.timestamp, header.timestamp);
        assert_eq!(roundtripped_header.timestamp_echo, header.timestamp_echo);
        assert_eq!(packet_payload_bytes(&roundtripped), b"hello tcp".to_vec());
    }

    #[test]
    fn zero_length_tcp_packet_without_options_roundtrips_through_serialized_packet() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let header = tcp::TcpHeader {
            ip: tcp::Ipv4Header {
                src: *src.ip(),
                dst: *dst.ip(),
            },
            flags: tcp::TcpFlags::ACK,
            src_port: src.port(),
            dst_port: dst.port(),
            seq: 123,
            ack: 456,
            window_size: 789,
            selective_acks: None,
            window_scale: None,
            timestamp: None,
            timestamp_echo: None,
        };
        let packet = PacketRc::new_ipv4_tcp(header, tcp::Payload(Vec::new()), 4);

        let serialized = SerializedPacket::try_from_packet(&packet).unwrap();
        let roundtripped = serialized.into_packet();
        let roundtripped_header = roundtripped.ipv4_tcp_header().unwrap();

        assert_eq!(roundtripped.iana_protocol(), IanaProtocol::Tcp);
        assert_eq!(roundtripped.src_ipv4_address(), src);
        assert_eq!(roundtripped.dst_ipv4_address(), dst);
        assert_eq!(roundtripped.priority(), 4);
        assert_eq!(roundtripped_header.flags, header.flags);
        assert_eq!(roundtripped_header.seq, header.seq);
        assert_eq!(roundtripped_header.ack, header.ack);
        assert_eq!(roundtripped_header.window_size, header.window_size);
        assert!(roundtripped_header.selective_acks.is_none());
        assert_eq!(roundtripped_header.window_scale, None);
        assert_eq!(roundtripped_header.timestamp, None);
        assert_eq!(roundtripped_header.timestamp_echo, None);
        assert_eq!(packet_payload_bytes(&roundtripped), Vec::<u8>::new());
    }

    #[test]
    fn remote_packet_event_preserves_source_metadata() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(10),
            20,
            HostId::from(30),
            &packet,
        )
        .unwrap();

        assert_eq!(event.deliver_time, EmulatedTime::SIMULATION_START);
        assert_eq!(event.src_host_id, HostId::from(10));
        assert_eq!(event.src_host_event_id, 20);
        assert_eq!(event.dst_host_id, HostId::from(30));
    }

    #[test]
    fn remote_packet_event_roundtrips_through_ipc_wire_format() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let payload = Bytes::from_static(b"wire payload");
        let packet = PacketRc::new_ipv4_udp(src, dst, payload.clone(), 17);
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(10),
            20,
            HostId::from(30),
            &packet,
        )
        .unwrap();

        let encoded = encode_remote_packet_batch(std::slice::from_ref(&event)).unwrap();
        let roundtripped = decode_remote_packet_batch(&encoded, ShardId(0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(roundtripped, event);
        let packet = roundtripped.packet.into_packet();
        assert_eq!(packet.src_ipv4_address(), src);
        assert_eq!(packet.dst_ipv4_address(), dst);
        assert_eq!(packet.priority(), 17);
        assert_eq!(packet_payload_bytes(&packet), payload.to_vec());
    }

    #[test]
    fn tcp_remote_packet_event_roundtrips_through_ipc_wire_format() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_tcp(
            tcp::TcpHeader {
                ip: tcp::Ipv4Header {
                    src: *src.ip(),
                    dst: *dst.ip(),
                },
                flags: tcp::TcpFlags::ACK,
                src_port: src.port(),
                dst_port: dst.port(),
                seq: 123,
                ack: 456,
                window_size: 789,
                selective_acks: None,
                window_scale: Some(3),
                timestamp: Some(111),
                timestamp_echo: Some(222),
            },
            tcp::Payload(vec![Bytes::from_static(b"tcp wire payload")]),
            17,
        );
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(10),
            20,
            HostId::from(30),
            &packet,
        )
        .unwrap();

        let encoded = encode_remote_packet_batch(std::slice::from_ref(&event)).unwrap();
        let roundtripped = decode_remote_packet_batch(&encoded, ShardId(0))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(roundtripped, event);
        let packet = roundtripped.packet.into_packet();
        assert_eq!(packet.src_ipv4_address(), src);
        assert_eq!(packet.dst_ipv4_address(), dst);
        assert_eq!(packet.priority(), 17);
        assert_eq!(packet_payload_bytes(&packet), b"tcp wire payload".to_vec());
    }

    #[test]
    fn ipc_wire_format_rejects_invalid_emulated_time() {
        let wire = WireRemotePacketEvent {
            deliver_time: u64::MAX,
            src_host_id: 10,
            src_host_event_id: 20,
            dst_host_id: 30,
            packet: WireSerializedPacket::Udp {
                src_ip: u32::from(Ipv4Addr::new(11, 0, 0, 1)),
                src_port: 1234,
                dst_ip: u32::from(Ipv4Addr::new(11, 0, 0, 2)),
                dst_port: 5678,
                priority: 1,
                payload: b"payload".to_vec(),
            },
        };

        let err = RemotePacketEvent::try_from(wire).unwrap_err();

        assert!(err.to_string().contains("invalid emulated delivery time"));
    }

    #[test]
    fn binary_wire_rejects_unsupported_version() {
        let payload = [
            WIRE_MAGIC.as_slice(),
            &[WIRE_VERSION + 1, WIRE_KIND_PACKET_BATCH],
        ]
        .concat();

        let err =
            decode_wire_message(&payload, WIRE_KIND_PACKET_BATCH, "test packet batch").unwrap_err();

        assert!(err.to_string().contains("unsupported wire version"));
    }

    #[test]
    fn control_message_rejects_oversized_payload() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();
        sender
            .write_all(
                &u32::try_from(MAX_CONTROL_MESSAGE_BYTES + 1)
                    .unwrap()
                    .to_be_bytes(),
            )
            .unwrap();

        let err = read_control_request(&mut receiver).unwrap_err();

        assert!(
            err.to_string()
                .contains("distributed control request is too large")
        );
    }

    #[test]
    fn packet_batch_rejects_excessive_packet_count() {
        let mut body = Vec::new();
        write_len(&mut body, MAX_PACKET_BATCH_EVENTS + 1).unwrap();
        let payload = encode_wire_message(WIRE_KIND_PACKET_BATCH, &body).unwrap();

        let err = decode_remote_packet_batch(&payload, ShardId(0)).unwrap_err();

        assert!(err.to_string().contains("packet count"));
    }

    #[test]
    fn packet_batch_rejects_excessive_packet_payload_length() {
        let mut body = Vec::new();
        write_len(&mut body, 1).unwrap();
        write_u64(
            &mut body,
            EmulatedTime::to_c_emutime(Some(EmulatedTime::SIMULATION_START)),
        );
        write_u32(&mut body, 1);
        write_u64(&mut body, 0);
        write_u32(&mut body, 2);
        write_u8(&mut body, 1);
        write_u32(&mut body, u32::from(Ipv4Addr::new(11, 0, 0, 1)));
        write_u16(&mut body, 1234);
        write_u32(&mut body, u32::from(Ipv4Addr::new(11, 0, 0, 2)));
        write_u16(&mut body, 5678);
        write_u64(&mut body, 1);
        write_len(&mut body, MAX_PACKET_PAYLOAD_BYTES + 1).unwrap();
        let payload = encode_wire_message(WIRE_KIND_PACKET_BATCH, &body).unwrap();

        let err = decode_remote_packet_batch(&payload, ShardId(0)).unwrap_err();

        assert!(err.to_string().contains("byte array length"));
    }

    #[test]
    fn outbound_remote_packet_buffer_drains_in_deterministic_order() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);

        let buffer = OutboundRemotePacketBuffer::default();
        buffer.push(
            ShardId(2),
            RemotePacketEvent::new(
                EmulatedTime::SIMULATION_START,
                HostId::from(2),
                0,
                HostId::from(20),
                &packet,
            )
            .unwrap(),
        );
        buffer.push(
            ShardId(1),
            RemotePacketEvent::new(
                EmulatedTime::SIMULATION_START,
                HostId::from(3),
                0,
                HostId::from(30),
                &packet,
            )
            .unwrap(),
        );
        buffer.push(
            ShardId(1),
            RemotePacketEvent::new(
                EmulatedTime::SIMULATION_START,
                HostId::from(1),
                1,
                HostId::from(10),
                &packet,
            )
            .unwrap(),
        );

        let packets = buffer.drain_sorted();

        assert!(buffer.is_empty());
        assert_eq!(packets[0].dst_shard, ShardId(1));
        assert_eq!(packets[0].event.src_host_id, HostId::from(1));
        assert_eq!(packets[1].dst_shard, ShardId(1));
        assert_eq!(packets[1].event.src_host_id, HostId::from(3));
        assert_eq!(packets[2].dst_shard, ShardId(2));
    }

    #[test]
    fn remote_packet_event_converts_to_local_event_with_source_metadata() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let remote_event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(10),
            20,
            HostId::from(30),
            &packet,
        )
        .unwrap();
        let partition = PartitionMap::all_local([HostId::from(30)]);

        let (dst_host_id, local_event) = remote_event
            .into_local_event(DEFAULT_SHARD_ID, &partition)
            .unwrap();

        assert_eq!(dst_host_id, HostId::from(30));
        assert_eq!(local_event.time(), EmulatedTime::SIMULATION_START);
        assert_eq!(
            local_event.packet_source_metadata(),
            Some((HostId::from(10), 20))
        );
    }

    #[test]
    fn remote_packet_event_rejects_non_local_destination() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let remote_event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(10),
            20,
            HostId::from(30),
            &packet,
        )
        .unwrap();
        let partition = PartitionMap {
            host_to_shard: HashMap::from([(HostId::from(30), ShardId(1))]),
        };

        let err = remote_event
            .into_local_event(DEFAULT_SHARD_ID, &partition)
            .unwrap_err();

        assert_eq!(
            err,
            RemotePacketDeliveryError::NonLocalDestination {
                host_id: HostId::from(30),
                host_shard: ShardId(1),
                current_shard: DEFAULT_SHARD_ID,
            }
        );
    }

    #[test]
    fn remote_packet_exchange_routes_packets_to_destination_shards() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let event_to_shard_1 = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();
        let event_to_shard_2 = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(2),
            0,
            HostId::from(20),
            &packet,
        )
        .unwrap();
        let exchange = InProcessRemotePacketExchange::default();

        exchange
            .send(vec![
                OutboundRemotePacket {
                    dst_shard: ShardId(2),
                    event: event_to_shard_2.clone(),
                },
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: event_to_shard_1.clone(),
                },
            ])
            .unwrap();

        assert_eq!(exchange.receive(ShardId(1)).unwrap(), [event_to_shard_1]);
        assert_eq!(exchange.receive(ShardId(2)).unwrap(), [event_to_shard_2]);
        assert!(exchange.receive(ShardId(1)).unwrap().is_empty());
    }

    #[test]
    fn remote_packet_exchange_can_be_shared_through_arc() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();
        let sender = Arc::new(InProcessRemotePacketExchange::default());
        let receiver = Arc::clone(&sender);

        sender
            .send(vec![OutboundRemotePacket {
                dst_shard: ShardId(1),
                event: event.clone(),
            }])
            .unwrap();

        assert_eq!(receiver.receive(ShardId(1)).unwrap(), [event]);
    }

    #[test]
    fn remote_packet_exchange_receives_in_deterministic_order() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let later_src = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(2),
            0,
            HostId::from(20),
            &packet,
        )
        .unwrap();
        let earlier_src = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            1,
            HostId::from(10),
            &packet,
        )
        .unwrap();
        let earliest_src_event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();
        let exchange = InProcessRemotePacketExchange::default();

        exchange
            .send(vec![
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: later_src.clone(),
                },
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: earlier_src.clone(),
                },
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: earliest_src_event.clone(),
                },
            ])
            .unwrap();

        assert_eq!(
            exchange.receive(ShardId(1)).unwrap(),
            [earliest_src_event, earlier_src, later_src]
        );
    }

    #[test]
    fn unix_socket_remote_packet_exchange_routes_packets_to_destination_shards() {
        let dir = tempfile::tempdir().unwrap();
        let sender = UnixSocketRemotePacketExchange::bind(dir.path(), ShardId(0)).unwrap();
        let receiver = UnixSocketRemotePacketExchange::bind(dir.path(), ShardId(1)).unwrap();
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let payload = Bytes::from_static(b"ipc payload");
        let packet = PacketRc::new_ipv4_udp(src, dst, payload.clone(), 23);
        let later_src = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(2),
            0,
            HostId::from(20),
            &packet,
        )
        .unwrap();
        let earlier_src = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();

        sender
            .send(vec![
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: later_src.clone(),
                },
                OutboundRemotePacket {
                    dst_shard: ShardId(1),
                    event: earlier_src.clone(),
                },
            ])
            .unwrap();

        let received = receiver.receive(ShardId(1)).unwrap();

        assert_eq!(received, [earlier_src, later_src]);
        let packet = received[0].packet.clone().into_packet();
        assert_eq!(packet.src_ipv4_address(), src);
        assert_eq!(packet.dst_ipv4_address(), dst);
        assert_eq!(packet.priority(), 23);
        assert_eq!(packet_payload_bytes(&packet), payload.to_vec());
        assert!(receiver.receive(ShardId(1)).unwrap().is_empty());
    }

    #[test]
    fn distributed_exchange_context_builds_unix_socket_backend_in_temporary_directory() {
        let context = DistributedPacketExchangeContext::temporary(
            DistributedPacketExchangeBackend::UnixSocket,
        )
        .unwrap();
        let socket_dir = context.socket_dir().to_path_buf();
        let sender = context.build_exchange(ShardId(0)).unwrap();
        let receiver = context.build_exchange(ShardId(1)).unwrap();
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"context ipc"), 1);
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();

        assert_eq!(
            context.backend(),
            DistributedPacketExchangeBackend::UnixSocket
        );
        assert!(socket_dir.exists());
        sender
            .send(vec![OutboundRemotePacket {
                dst_shard: ShardId(1),
                event: event.clone(),
            }])
            .unwrap();

        assert_eq!(receiver.receive(ShardId(1)).unwrap(), [event]);

        drop(sender);
        drop(receiver);
        drop(context);
        assert!(!socket_dir.exists());
    }

    #[test]
    fn distributed_exchange_context_preserves_external_directory() {
        let root = tempfile::tempdir().unwrap();
        let socket_dir = root.path().join("sockets");
        let context = DistributedPacketExchangeContext::external(
            DistributedPacketExchangeBackend::UnixSocket,
            &socket_dir,
        )
        .unwrap();
        let socket_path = unix_socket_path(context.socket_dir(), ShardId(0));
        let exchange = context.build_exchange(ShardId(0)).unwrap();

        assert!(socket_dir.exists());
        assert!(socket_path.exists());

        drop(exchange);
        assert!(!socket_path.exists());

        drop(context);
        assert!(socket_dir.exists());
    }

    #[test]
    fn distributed_control_waits_for_all_shards_across_rounds() {
        let dir = tempfile::tempdir().unwrap();
        let _server = DistributedControlServer::start(dir.path(), 3).unwrap();
        let handles: Vec<_> = (0..3)
            .map(|shard| {
                let socket_dir = dir.path().to_path_buf();
                std::thread::spawn(move || {
                    let control =
                        DistributedControlClient::connect(socket_dir, ShardId(shard)).unwrap();
                    control.wait().unwrap();
                    control.wait().unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn distributed_control_returns_global_min_next_event_time() {
        let dir = tempfile::tempdir().unwrap();
        let _server = DistributedControlServer::start(dir.path(), 3).unwrap();
        let local_times = [
            EmulatedTime::SIMULATION_START,
            EmulatedTime::UNIX_EPOCH,
            EmulatedTime::MAX,
        ];
        let handles: Vec<_> = local_times
            .into_iter()
            .enumerate()
            .map(|(shard, local_time)| {
                let socket_dir = dir.path().to_path_buf();
                std::thread::spawn(move || {
                    let control = DistributedControlClient::connect(
                        socket_dir,
                        ShardId(u32::try_from(shard).unwrap()),
                    )
                    .unwrap();
                    control.wait_for_global_min_next_event(local_time).unwrap()
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), EmulatedTime::UNIX_EPOCH);
        }
    }

    #[test]
    fn distributed_control_server_shutdown_without_clients() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = unix_control_socket_path(dir.path());
        let server = DistributedControlServer::start(dir.path(), 3).unwrap();

        assert!(socket_path.exists());
        server.shutdown().unwrap();
        assert!(!socket_path.exists());
    }

    #[test]
    fn distributed_control_unblocks_peer_when_shard_disconnects_mid_round() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = unix_control_socket_path(dir.path());
        let server = DistributedControlServer::start(dir.path(), 2).unwrap();
        let mut waiting_stream = UnixStream::connect(&socket_path).unwrap();
        let disconnected_stream = UnixStream::connect(&socket_path).unwrap();

        write_control_message(
            &mut waiting_stream,
            &WireControlRequest {
                shard_id: 0,
                round: 0,
                min_next_event_time: None,
            },
        )
        .unwrap();
        drop(disconnected_stream);

        let err = read_control_response(&mut waiting_stream).unwrap_err();

        assert!(err.to_string().contains("distributed control"));
        server.shutdown().unwrap();
    }

    #[test]
    fn distributed_control_server_reports_malformed_request() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = unix_control_socket_path(dir.path());
        let server = DistributedControlServer::start(dir.path(), 1).unwrap();
        let mut stream = UnixStream::connect(&socket_path).unwrap();

        stream.write_all(&4u32.to_be_bytes()).unwrap();
        stream.write_all(b"nope").unwrap();

        let err = server.shutdown().unwrap_err();

        assert!(
            err.to_string()
                .contains("failed to decode distributed control request")
        );
    }

    #[test]
    fn distributed_control_server_reports_duplicate_shard_request() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = unix_control_socket_path(dir.path());
        let server = DistributedControlServer::start(dir.path(), 2).unwrap();
        let mut first_stream = UnixStream::connect(&socket_path).unwrap();
        let mut second_stream = UnixStream::connect(&socket_path).unwrap();

        for stream in [&mut first_stream, &mut second_stream] {
            write_control_message(
                stream,
                &WireControlRequest {
                    shard_id: 0,
                    round: 0,
                    min_next_event_time: None,
                },
            )
            .unwrap();
        }

        let err = server.shutdown().unwrap_err();

        assert!(
            err.to_string()
                .contains("duplicate request from shard 0 in round 0")
        );
    }

    #[test]
    fn unix_socket_remote_packet_exchange_rejects_wrong_receive_shard() {
        let dir = tempfile::tempdir().unwrap();
        let exchange = UnixSocketRemotePacketExchange::bind(dir.path(), ShardId(1)).unwrap();

        let err = exchange.receive(ShardId(0)).unwrap_err();

        assert!(err.to_string().contains("cannot receive packets for shard"));
    }

    #[test]
    fn unix_socket_remote_packet_exchange_rejects_malformed_peer_batch() {
        let dir = tempfile::tempdir().unwrap();
        let shard = ShardId(1);
        let exchange = UnixSocketRemotePacketExchange::bind(dir.path(), shard).unwrap();
        let mut stream = UnixStream::connect(unix_socket_path(dir.path(), shard)).unwrap();

        stream.write_all(b"nope").unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();

        let err = exchange.receive(shard).unwrap_err();

        assert!(
            err.to_string()
                .contains("failed to decode IPC packet batch")
        );
    }

    #[test]
    fn noop_remote_packet_exchange_rejects_outbound_packets() {
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let packet = PacketRc::new_ipv4_udp(src, dst, Bytes::from_static(b"payload"), 1);
        let event = RemotePacketEvent::new(
            EmulatedTime::SIMULATION_START,
            HostId::from(1),
            0,
            HostId::from(10),
            &packet,
        )
        .unwrap();
        let exchange = NoopRemotePacketExchange;

        let err = exchange
            .send(vec![OutboundRemotePacket {
                dst_shard: ShardId(1),
                event,
            }])
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("no remote packet exchange backend")
        );
        assert!(exchange.receive(DEFAULT_SHARD_ID).unwrap().is_empty());
    }

    #[test]
    fn legacy_tcp_serialization_error_mentions_new_tcp_requirement() {
        assert!(
            PacketSerializationError::LegacyTcp
                .to_string()
                .contains("experimental.use_new_tcp=true")
        );
    }

    #[test]
    fn udp_packet_travels_through_remote_exchange_into_destination_queue() {
        let src_host_id = HostId::from(10);
        let dst_host_id = HostId::from(20);
        let src_shard = ShardId(0);
        let dst_shard = ShardId(1);
        let deliver_time = EmulatedTime::SIMULATION_START;
        let src = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 1), 1234);
        let dst = SocketAddrV4::new(Ipv4Addr::new(11, 0, 0, 2), 5678);
        let payload = Bytes::from_static(b"cross shard udp");
        let packet = PacketRc::new_ipv4_udp(src, dst, payload.clone(), 7);
        let remote_event =
            RemotePacketEvent::new(deliver_time, src_host_id, 42, dst_host_id, &packet).unwrap();
        let outbound_buffer = OutboundRemotePacketBuffer::default();
        let exchange = InProcessRemotePacketExchange::default();
        let partition =
            PartitionMap::from_host_shards([(src_host_id, src_shard), (dst_host_id, dst_shard)]);
        let mut dst_queue = EventQueue::new();

        outbound_buffer.push(dst_shard, remote_event);
        exchange.send(outbound_buffer.drain_sorted()).unwrap();
        let received = exchange.receive(dst_shard).unwrap();
        assert_eq!(received.len(), 1);

        let (received_dst_host_id, local_event) = received
            .into_iter()
            .next()
            .unwrap()
            .into_local_event(dst_shard, &partition)
            .unwrap();
        assert_eq!(received_dst_host_id, dst_host_id);
        assert_eq!(
            local_event.packet_source_metadata(),
            Some((src_host_id, 42))
        );
        dst_queue.push(local_event);

        assert_eq!(dst_queue.next_event_time(), Some(deliver_time));
        let popped = dst_queue.pop().unwrap();
        assert_eq!(popped.packet_source_metadata(), Some((src_host_id, 42)));
        let EventData::Packet(packet_data) = popped.data() else {
            panic!("expected packet event");
        };
        let received_packet = PacketRc::from(packet_data);
        assert_eq!(received_packet.src_ipv4_address(), src);
        assert_eq!(received_packet.dst_ipv4_address(), dst);
        assert_eq!(received_packet.priority(), 7);
        assert_eq!(packet_payload_bytes(&received_packet), payload.to_vec());
    }
}
