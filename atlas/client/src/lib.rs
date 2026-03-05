//! Guest-side Atlas helpers and re-exports.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Flatbuffers protocol helpers for Atlas control messages.
pub use selium_atlas_protocol as protocol;
/// Atlas protocol types for convenience.
pub use selium_atlas_protocol::{AtlasId, uri::Uri};
use selium_atlas_protocol::{Message, ProtocolError, decode_message, encode_message};
use selium_switchboard::{Channel, ChannelError, SharedChannel};
use thiserror::Error;
use tracing::debug;

const REQUEST_CHUNK_SIZE: u32 = 64 * 1024;
const RESPONSE_CHANNEL_CAPACITY: u32 = 16 * 1024;

/// Atlas front-end that guests use to look up endpoints.
#[derive(Clone)]
pub struct Atlas {
    request_channel: Arc<Channel>,
    next_request_id: Arc<AtomicU64>,
}

/// Errors produced by the Atlas client.
#[derive(Error, Debug)]
pub enum AtlasError {
    /// Channel I/O failed.
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
    /// Control-plane protocol could not be decoded.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// The Atlas service returned an error.
    #[error("atlas error: {0}")]
    Remote(String),
    /// The Atlas response channel was closed.
    #[error("endpoint closed")]
    EndpointClosed,
}

impl Atlas {
    /// Connect to the Atlas service using the shared request channel handle.
    pub async fn attach(request_channel: SharedChannel) -> Result<Self, AtlasError> {
        let request_channel = Channel::attach_shared(request_channel).await?;
        Ok(Self {
            request_channel: Arc::new(request_channel),
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Retrieve an endpoint by URI.
    pub async fn get(&self, uri: &Uri) -> Result<Option<AtlasId>, AtlasError> {
        let response = self
            .send_request(|request_id, reply_channel| Message::GetRequest {
                request_id,
                uri: uri.clone(),
                reply_channel: reply_channel.raw(),
            })
            .await?;

        match response {
            Message::ResponseGet { id, found, .. } => Ok(found.then_some(id)),
            Message::ResponseError { message, .. } => Err(AtlasError::Remote(message)),
            _ => Err(AtlasError::Protocol(ProtocolError::UnknownPayload)),
        }
    }

    /// Add or update an endpoint in the Atlas.
    pub async fn insert(&self, uri: Uri, id: AtlasId) -> Result<(), AtlasError> {
        let response = self
            .send_request(|request_id, reply_channel| Message::InsertRequest {
                request_id,
                uri,
                id,
                reply_channel: reply_channel.raw(),
            })
            .await?;

        match response {
            Message::ResponseOk { .. } => Ok(()),
            Message::ResponseError { message, .. } => Err(AtlasError::Remote(message)),
            _ => Err(AtlasError::Protocol(ProtocolError::UnknownPayload)),
        }
    }

    /// Delete an endpoint from the Atlas.
    pub async fn remove(&self, uri: &Uri) -> Result<Option<AtlasId>, AtlasError> {
        let response = self
            .send_request(|request_id, reply_channel| Message::RemoveRequest {
                request_id,
                uri: uri.clone(),
                reply_channel: reply_channel.raw(),
            })
            .await?;

        match response {
            Message::ResponseRemove { id, found, .. } => Ok(found.then_some(id)),
            Message::ResponseError { message, .. } => Err(AtlasError::Remote(message)),
            _ => Err(AtlasError::Protocol(ProtocolError::UnknownPayload)),
        }
    }

    /// Lookup endpoints matching a URI pattern.
    pub async fn lookup(&self, pattern: &str) -> Result<Vec<AtlasId>, AtlasError> {
        let response = self
            .send_request(|request_id, reply_channel| Message::LookupRequest {
                request_id,
                pattern: pattern.to_string(),
                reply_channel: reply_channel.raw(),
            })
            .await?;

        match response {
            Message::ResponseLookup { ids, .. } => Ok(ids),
            Message::ResponseError { message, .. } => Err(AtlasError::Remote(message)),
            _ => Err(AtlasError::Protocol(ProtocolError::UnknownPayload)),
        }
    }

    async fn send_request<F>(&self, build: F) -> Result<Message, AtlasError>
    where
        F: FnOnce(u64, SharedChannel) -> Message,
    {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let response_channel = Channel::create(RESPONSE_CHANNEL_CAPACITY).await?;
        let response_shared = response_channel.share().await?;
        let mut response_reader = response_channel.subscribe(REQUEST_CHUNK_SIZE).await?;

        let message = build(request_id, response_shared);
        debug!(request_id, "atlas: sending request");
        let bytes = encode_message(&message)?;
        let mut writer = self.request_channel.publish_weak().await?;
        writer.send(bytes).await?;
        debug!(request_id, "atlas: request sent");

        let response = match response_reader.read_next().await? {
            Some(frame) => decode_message(&frame.payload)?,
            None => return Err(AtlasError::EndpointClosed),
        };
        debug!(request_id, "atlas: received response");

        response_channel.delete().await?;
        Ok(response)
    }
}
