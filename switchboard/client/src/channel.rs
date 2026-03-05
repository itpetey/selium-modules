//! Queue/shm-backed channel primitives.

use selium_abi::{
    GuestResourceId, GuestUint, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueRole,
    QueueStatusCode,
};
use selium_userland::{driver::DriverError as HostDriverError, queue, shm};
use std::sync::Arc;
use thiserror::Error;

const DEFAULT_SLOT_BYTES: u32 = 64 * 1024;
const SHM_ALIGN: u32 = 64;
const PARK_TIMEOUT_MS: u32 = u32::MAX;
static NEXT_WRITER_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

/// Writer backpressure policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    /// Wait for queue capacity.
    Park,
    /// Drop frames when the queue is full.
    Drop,
}

/// Reader behavior when it falls behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReaderMode {
    /// Return an error when the reader falls behind.
    Strong,
    /// Skip forward when the reader falls behind.
    Weak,
}

/// Result of a send attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// Frame was committed.
    Sent,
    /// Frame was dropped by policy.
    Dropped,
}

/// Frame returned by a reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoFrame {
    /// Queue sequence number.
    pub seq: u64,
    /// Stable writer identifier.
    pub writer_id: u32,
    /// Frame payload.
    pub payload: Vec<u8>,
}

/// Transferable channel handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedChannel {
    /// Shared queue handle.
    pub queue_shared_id: GuestResourceId,
    /// Shared shm handle backing the ring.
    pub shm_shared_id: GuestResourceId,
    /// Number of frame slots in the ring.
    pub capacity_frames: u32,
    /// Bytes per ring slot and queue max frame.
    pub slot_bytes: u32,
    /// Queue overflow behavior.
    pub backpressure: Backpressure,
}

/// Channel creation configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Number of frame slots in the queue/ring.
    pub capacity_frames: u32,
    /// Maximum frame size in bytes.
    pub slot_bytes: u32,
    /// Queue overflow behavior.
    pub backpressure: Backpressure,
}

impl ChannelConfig {
    /// Build a channel config from a byte-capacity target.
    ///
    /// The queue/ring layout uses fixed-size frame slots.
    pub fn from_capacity_bytes(capacity_bytes: u32, backpressure: Backpressure) -> Self {
        let bytes = capacity_bytes.max(1);
        let slot_bytes = bytes.clamp(1, DEFAULT_SLOT_BYTES);
        let capacity_frames = bytes.div_ceil(slot_bytes).max(1);
        Self {
            capacity_frames,
            slot_bytes,
            backpressure,
        }
    }
}

#[derive(Clone, Debug)]
struct RingBuffer {
    resource_id: GuestUint,
    shared_id: GuestResourceId,
    ring_bytes: u32,
    capacity_frames: u32,
    slot_bytes: u32,
}

/// Queue/shm-backed channel.
#[derive(Debug)]
pub struct Channel {
    queue_resource_id: Option<GuestUint>,
    shared: SharedChannel,
    ring: Option<Arc<RingBuffer>>,
}

/// Queue writer endpoint.
#[derive(Debug)]
pub struct Writer {
    endpoint_id: GuestUint,
    ring: Option<Arc<RingBuffer>>,
    backpressure: Backpressure,
}

/// Queue reader endpoint.
#[derive(Debug)]
pub struct Reader {
    endpoint_id: GuestUint,
    ring: Option<Arc<RingBuffer>>,
    mode: ReaderMode,
}

/// Backwards-compatible channel handle alias.
pub type ChannelHandle = GuestResourceId;

/// Backwards-compatible driver error alias.
pub type DriverError = ChannelError;

/// Backwards-compatible backpressure alias.
pub type ChannelBackpressure = Backpressure;

/// Channel operation errors.
#[derive(Debug, Error)]
pub enum ChannelError {
    /// Host driver call failed.
    #[error("driver error: {0}")]
    Driver(#[from] HostDriverError),
    /// Invalid channel configuration.
    #[error("invalid channel config: {0}")]
    InvalidConfig(&'static str),
    /// Queue returned a status error.
    #[error("queue {op} failed with status {code:?}")]
    QueueStatus {
        op: &'static str,
        code: QueueStatusCode,
    },
    /// Payload exceeds channel frame size.
    #[error("payload too large: len={len}, max={max}")]
    TooLarge { len: u32, max: u32 },
    /// Reader fell behind retained frames.
    #[error("reader fell behind")]
    ReaderBehind,
    /// Host returned an invalid or missing frame.
    #[error("invalid frame returned by host")]
    InvalidFrame,
}

impl Channel {
    /// Create a queue/shm channel from a byte capacity.
    pub async fn create(capacity_bytes: u32) -> Result<Self, ChannelError> {
        Self::create_with_backpressure(capacity_bytes, Backpressure::Park).await
    }

    /// Create a queue/shm channel from a byte capacity and backpressure policy.
    pub async fn create_with_backpressure(
        capacity_bytes: u32,
        backpressure: Backpressure,
    ) -> Result<Self, ChannelError> {
        let cfg = ChannelConfig::from_capacity_bytes(capacity_bytes, backpressure);
        Self::create_with_config(cfg).await
    }

    /// Create a queue/shm channel with explicit slot configuration.
    pub async fn create_with_config(config: ChannelConfig) -> Result<Self, ChannelError> {
        if config.capacity_frames == 0 {
            return Err(ChannelError::InvalidConfig("capacity_frames must be > 0"));
        }
        if config.slot_bytes == 0 {
            return Err(ChannelError::InvalidConfig("slot_bytes must be > 0"));
        }

        let overflow = match config.backpressure {
            Backpressure::Park => QueueOverflow::Block,
            Backpressure::Drop => QueueOverflow::DropNewest,
        };

        let queue_desc = queue::create(QueueCreate {
            capacity_frames: config.capacity_frames,
            max_frame_bytes: config.slot_bytes,
            delivery: QueueDelivery::Lossless,
            overflow,
        })
        .await?;

        let ring = RingBuffer::allocate(config.capacity_frames, config.slot_bytes).await?;
        let shared = SharedChannel {
            queue_shared_id: queue_desc.shared_id,
            shm_shared_id: ring.shared_id,
            capacity_frames: config.capacity_frames,
            slot_bytes: config.slot_bytes,
            backpressure: config.backpressure,
        };

        Ok(Self {
            queue_resource_id: Some(queue_desc.resource_id),
            shared,
            ring: Some(Arc::new(ring)),
        })
    }

    /// Attach to an existing shared channel.
    pub async fn attach_shared(shared: SharedChannel) -> Result<Self, ChannelError> {
        let ring = Self::attach_ring_from_shared(shared).await?;

        Ok(Self {
            queue_resource_id: None,
            shared,
            ring,
        })
    }

    /// Create a channel from a raw shared queue handle.
    ///
    /// # Safety
    /// The caller must ensure `raw` is a valid shared queue identifier.
    pub unsafe fn from_raw(raw: GuestResourceId) -> Self {
        Self {
            queue_resource_id: None,
            shared: SharedChannel::from_raw(raw),
            ring: None,
        }
    }

    /// Return a local/raw identifier for compatibility.
    pub fn handle(&self) -> GuestResourceId {
        self.shared.raw()
    }

    /// Return the transferable shared handle.
    pub async fn share(&self) -> Result<SharedChannel, ChannelError> {
        Ok(self.shared)
    }

    /// Open a writer endpoint.
    pub async fn publish(&self) -> Result<Writer, ChannelError> {
        self.publish_with_writer(next_writer_id()).await
    }

    /// Open a writer endpoint with the supplied writer identifier.
    pub async fn publish_with_writer(&self, writer_id: u32) -> Result<Writer, ChannelError> {
        let endpoint =
            queue::attach(self.shared.queue_shared_id, QueueRole::Writer { writer_id }).await?;
        Ok(Writer {
            endpoint_id: endpoint.resource_id,
            ring: self.ring.clone(),
            backpressure: self.shared.backpressure,
        })
    }

    /// Backwards-compatible alias for `publish`.
    pub async fn publish_weak(&self) -> Result<Writer, ChannelError> {
        self.publish().await
    }

    /// Open a strong reader endpoint.
    pub async fn subscribe(&self, _chunk_size: u32) -> Result<Reader, ChannelError> {
        self.subscribe_with_mode(ReaderMode::Strong).await
    }

    /// Open a weak reader endpoint.
    pub async fn subscribe_weak(&self, _chunk_size: u32) -> Result<Reader, ChannelError> {
        self.subscribe_with_mode(ReaderMode::Weak).await
    }

    /// Open a reader endpoint with explicit mode.
    pub async fn subscribe_with_mode(&self, mode: ReaderMode) -> Result<Reader, ChannelError> {
        let endpoint = queue::attach(self.shared.queue_shared_id, QueueRole::Reader).await?;
        Ok(Reader {
            endpoint_id: endpoint.resource_id,
            ring: self.ring.clone(),
            mode,
        })
    }

    /// Close the channel queue (if owned) and detach shm.
    pub async fn delete(mut self) -> Result<(), ChannelError> {
        if let Some(queue_id) = self.queue_resource_id.take() {
            let status = queue::close(queue_id).await?;
            require_queue_status("close", status.code)?;
        }
        if let Some(ring) = &self.ring
            && Arc::strong_count(ring) == 1
        {
            ring.detach().await?;
        }
        Ok(())
    }

    /// Start draining by closing the queue if this process owns it.
    pub async fn drain(&mut self) -> Result<(), ChannelError> {
        if let Some(queue_id) = self.queue_resource_id.take() {
            let status = queue::close(queue_id).await?;
            require_queue_status("close", status.code)?;
        }
        Ok(())
    }
}

impl Clone for Channel {
    fn clone(&self) -> Self {
        // Clones are non-owning views to the same shared queue and ring metadata.
        // Queue close remains tied to `queue_resource_id` ownership.
        Self {
            queue_resource_id: None,
            shared: self.shared,
            ring: self.ring.clone(),
        }
    }
}

impl Writer {
    /// Send one frame.
    pub async fn send(&mut self, payload: impl AsRef<[u8]>) -> Result<SendOutcome, ChannelError> {
        let payload = payload.as_ref();
        if payload.is_empty() {
            return Ok(SendOutcome::Sent);
        }

        let payload_len = u32::try_from(payload.len())
            .map_err(|_| ChannelError::InvalidConfig("payload too large"))?;
        if let Some(ring) = &self.ring
            && payload_len > ring.slot_bytes
        {
            return Err(ChannelError::TooLarge {
                len: payload_len,
                max: ring.slot_bytes,
            });
        }

        let timeout_ms = match self.backpressure {
            Backpressure::Park => PARK_TIMEOUT_MS,
            Backpressure::Drop => 0,
        };

        let reserve = queue::reserve(self.endpoint_id, payload_len, timeout_ms).await?;
        let reservation = match reserve.code {
            QueueStatusCode::Ok => reserve.reservation.ok_or(ChannelError::InvalidFrame)?,
            QueueStatusCode::WouldBlock | QueueStatusCode::Full | QueueStatusCode::Timeout
                if matches!(self.backpressure, Backpressure::Drop) =>
            {
                return Ok(SendOutcome::Dropped);
            }
            other => {
                return Err(ChannelError::QueueStatus {
                    op: "reserve",
                    code: other,
                });
            }
        };

        let (shm_shared_id, offset, temp_shm_resource) = if let Some(ring) = &self.ring {
            let offset = slot_offset_for(reservation.seq, ring.capacity_frames, ring.slot_bytes)?;
            if let Err(err) = ring.write_payload(offset, payload).await {
                let _ = queue::cancel(self.endpoint_id, reservation.reservation_id).await;
                return Err(err);
            }
            (ring.shared_id, offset, None)
        } else {
            let shm_desc = shm::alloc(payload_len, SHM_ALIGN).await?;
            if let Err(err) = shm::write(shm_desc.resource_id, 0, payload.to_vec()).await {
                let _ = queue::cancel(self.endpoint_id, reservation.reservation_id).await;
                let _ = shm::detach(shm_desc.resource_id).await;
                return Err(ChannelError::Driver(err));
            }
            (shm_desc.shared_id, 0, Some(shm_desc.resource_id))
        };

        let status = queue::commit(QueueCommit {
            endpoint_id: self.endpoint_id,
            reservation_id: reservation.reservation_id,
            shm_shared_id,
            offset,
            len: payload_len,
        })
        .await?;

        if let Some(resource_id) = temp_shm_resource {
            let _ = shm::detach(resource_id).await;
        }

        require_queue_status("commit", status.code)?;
        Ok(SendOutcome::Sent)
    }

    /// Close this writer endpoint.
    pub async fn close(self) -> Result<(), ChannelError> {
        let status = queue::close(self.endpoint_id).await?;
        require_queue_status("close", status.code)
    }
}

impl Reader {
    /// Read one frame, blocking until data or closure.
    pub async fn read_next(&mut self) -> Result<Option<IoFrame>, ChannelError> {
        self.read_next_with_timeout(PARK_TIMEOUT_MS).await
    }

    /// Read one frame with an explicit wait timeout.
    ///
    /// Returns `Ok(None)` on timeout/would-block/closed.
    pub async fn read_next_with_timeout(
        &mut self,
        timeout_ms: u32,
    ) -> Result<Option<IoFrame>, ChannelError> {
        loop {
            let waited = queue::wait(self.endpoint_id, timeout_ms).await?;
            match waited.code {
                QueueStatusCode::Ok => {
                    let frame = waited.frame.ok_or(ChannelError::InvalidFrame)?;
                    let payload = match &self.ring {
                        Some(ring) if frame.shm_shared_id == ring.shared_id => {
                            ring.read_payload(frame.offset, frame.len).await?
                        }
                        _ => {
                            RingBuffer::read_external(frame.shm_shared_id, frame.offset, frame.len)
                                .await?
                        }
                    };

                    let status = queue::ack(self.endpoint_id, frame.seq).await?;
                    require_queue_status("ack", status.code)?;

                    return Ok(Some(IoFrame {
                        seq: frame.seq,
                        writer_id: frame.writer_id,
                        payload,
                    }));
                }
                QueueStatusCode::ReaderBehind => match self.mode {
                    ReaderMode::Strong => return Err(ChannelError::ReaderBehind),
                    ReaderMode::Weak => continue,
                },
                QueueStatusCode::Closed
                | QueueStatusCode::WouldBlock
                | QueueStatusCode::Timeout
                | QueueStatusCode::Empty => return Ok(None),
                other => {
                    return Err(ChannelError::QueueStatus {
                        op: "wait",
                        code: other,
                    });
                }
            }
        }
    }

    /// Close this reader endpoint.
    pub async fn close(self) -> Result<(), ChannelError> {
        let status = queue::close(self.endpoint_id).await?;
        require_queue_status("close", status.code)
    }
}

impl RingBuffer {
    async fn allocate(capacity_frames: u32, slot_bytes: u32) -> Result<Self, ChannelError> {
        let ring_bytes = capacity_frames
            .checked_mul(slot_bytes)
            .ok_or(ChannelError::InvalidConfig("ring byte size overflow"))?;
        let desc = shm::alloc(ring_bytes, SHM_ALIGN).await?;
        Ok(Self {
            resource_id: desc.resource_id,
            shared_id: desc.shared_id,
            ring_bytes,
            capacity_frames,
            slot_bytes,
        })
    }

    async fn attach(
        shared_id: GuestResourceId,
        capacity_frames: u32,
        slot_bytes: u32,
    ) -> Result<Self, ChannelError> {
        let ring_bytes = capacity_frames
            .checked_mul(slot_bytes)
            .ok_or(ChannelError::InvalidConfig("ring byte size overflow"))?;
        let desc = shm::attach(shared_id).await?;
        Ok(Self {
            resource_id: desc.resource_id,
            shared_id,
            ring_bytes,
            capacity_frames,
            slot_bytes,
        })
    }

    async fn write_payload(&self, offset: u32, payload: &[u8]) -> Result<(), ChannelError> {
        write_wrapped(self.resource_id, self.ring_bytes, offset, payload).await
    }

    async fn read_payload(&self, offset: u32, len: u32) -> Result<Vec<u8>, ChannelError> {
        read_wrapped(self.resource_id, self.ring_bytes, offset, len).await
    }

    async fn read_external(
        shared_id: GuestResourceId,
        offset: u32,
        len: u32,
    ) -> Result<Vec<u8>, ChannelError> {
        let desc = shm::attach(shared_id).await?;
        let payload = read_wrapped(desc.resource_id, desc.region.len, offset, len).await;
        let _ = shm::detach(desc.resource_id).await;
        payload
    }

    async fn detach(&self) -> Result<(), ChannelError> {
        shm::detach(self.resource_id).await?;
        Ok(())
    }
}

impl Channel {
    async fn attach_ring_from_shared(
        shared: SharedChannel,
    ) -> Result<Option<Arc<RingBuffer>>, ChannelError> {
        if shared.shm_shared_id == 0 || shared.capacity_frames == 0 || shared.slot_bytes == 0 {
            return Ok(None);
        }

        let ring = RingBuffer::attach(
            shared.shm_shared_id,
            shared.capacity_frames,
            shared.slot_bytes,
        )
        .await?;
        Ok(Some(Arc::new(ring)))
    }
}

impl SharedChannel {
    /// Create a shared-channel wrapper from the queue shared id.
    pub const fn from_raw(raw: GuestResourceId) -> Self {
        Self {
            queue_shared_id: raw,
            shm_shared_id: 0,
            capacity_frames: 0,
            slot_bytes: 0,
            backpressure: Backpressure::Park,
        }
    }

    /// Return the raw queue shared id.
    pub const fn raw(self) -> GuestResourceId {
        self.queue_shared_id
    }
}

fn require_queue_status(op: &'static str, code: QueueStatusCode) -> Result<(), ChannelError> {
    if code == QueueStatusCode::Ok {
        Ok(())
    } else {
        Err(ChannelError::QueueStatus { op, code })
    }
}

fn slot_offset_for(seq: u64, capacity_frames: u32, slot_bytes: u32) -> Result<u32, ChannelError> {
    if capacity_frames == 0 || slot_bytes == 0 {
        return Err(ChannelError::InvalidConfig(
            "capacity_frames and slot_bytes must be > 0",
        ));
    }
    let frame_slot = seq % u64::from(capacity_frames);
    let offset = frame_slot
        .checked_mul(u64::from(slot_bytes))
        .ok_or(ChannelError::InvalidConfig("slot offset overflow"))?;
    u32::try_from(offset).map_err(|_| ChannelError::InvalidConfig("slot offset overflow"))
}

fn next_writer_id() -> u32 {
    NEXT_WRITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

async fn write_wrapped(
    resource_id: GuestUint,
    ring_bytes: u32,
    offset: u32,
    payload: &[u8],
) -> Result<(), ChannelError> {
    let ring = usize::try_from(ring_bytes)
        .map_err(|_| ChannelError::InvalidConfig("ring size conversion overflow"))?;
    let base = usize::try_from(offset)
        .map_err(|_| ChannelError::InvalidConfig("offset conversion overflow"))?
        % ring;
    let first = payload.len().min(ring.saturating_sub(base));
    if first > 0 {
        shm::write(
            resource_id,
            u32::try_from(base).map_err(|_| ChannelError::InvalidConfig("offset overflow"))?,
            payload[..first].to_vec(),
        )
        .await?;
    }
    if payload.len() > first {
        shm::write(resource_id, 0, payload[first..].to_vec()).await?;
    }
    Ok(())
}

async fn read_wrapped(
    resource_id: GuestUint,
    ring_bytes: u32,
    offset: u32,
    len: u32,
) -> Result<Vec<u8>, ChannelError> {
    let ring = usize::try_from(ring_bytes)
        .map_err(|_| ChannelError::InvalidConfig("ring size conversion overflow"))?;
    let base = usize::try_from(offset)
        .map_err(|_| ChannelError::InvalidConfig("offset conversion overflow"))?
        % ring;
    let need = usize::try_from(len).map_err(|_| ChannelError::InvalidConfig("length overflow"))?;
    let first = need.min(ring.saturating_sub(base));

    let mut out = Vec::with_capacity(need);
    if first > 0 {
        let first_bytes = shm::read(
            resource_id,
            u32::try_from(base).map_err(|_| ChannelError::InvalidConfig("offset overflow"))?,
            u32::try_from(first).map_err(|_| ChannelError::InvalidConfig("length overflow"))?,
        )
        .await?;
        out.extend_from_slice(&first_bytes);
    }
    if need > first {
        let remain = need - first;
        let second_bytes = shm::read(
            resource_id,
            0,
            u32::try_from(remain).map_err(|_| ChannelError::InvalidConfig("length overflow"))?,
        )
        .await?;
        out.extend_from_slice(&second_bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_offset_wraps_by_frame_count() {
        let offset0 = slot_offset_for(0, 4, 1024).expect("offset");
        let offset1 = slot_offset_for(1, 4, 1024).expect("offset");
        let offset4 = slot_offset_for(4, 4, 1024).expect("offset");
        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1024);
        assert_eq!(offset4, 0);
    }

    #[test]
    fn config_from_capacity_has_nonzero_fields() {
        let cfg = ChannelConfig::from_capacity_bytes(0, Backpressure::Park);
        assert_eq!(cfg.capacity_frames, 1);
        assert_eq!(cfg.slot_bytes, 1);
    }
}
