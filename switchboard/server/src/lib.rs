//! Switchboard service module that manages endpoint wiring.

use std::collections::HashMap;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use selium_switchboard_core::{
    ChannelKey, SwitchboardCore, SwitchboardError as CoreError, best_compatible_match,
};
use selium_switchboard_protocol::{
    AdoptMode, Backpressure, Cardinality, EndpointDirections, EndpointId, Message, ProtocolError,
    WiringEgress, WiringIngress, decode_message, encode_message,
};
use selium_userland::{
    DependencyId, dependency_id, entrypoint,
    io::{Channel, ChannelBackpressure, DriverError, SharedChannel, Writer},
    singleton, spawn,
};
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

const REQUEST_CHUNK_SIZE: u32 = 64 * 1024;
const CHANNEL_CAPACITY: u32 = 64 * 1024;
const TAP_CHUNK_SIZE: u32 = 64 * 1024;
const SWITCHBOARD_SINGLETON_ID: DependencyId = dependency_id!("selium.switchboard.singleton");

#[derive(Debug, Error)]
enum SwitchboardServiceError {
    #[error("driver error: {0}")]
    Driver(#[from] DriverError),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("solver error: {0}")]
    Solver(#[from] CoreError),
}

#[allow(dead_code)] // @todo Note this is a temporary workaround
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChannelAnchor {
    Producer(EndpointId),
    Consumer(EndpointId),
}

struct ChannelEntry {
    channel: Channel,
    shared: SharedChannel,
    key: ChannelKey,
    anchor: Option<ChannelAnchor>,
    owned: bool,
    retained: bool,
}

struct EndpointRegistration {
    updates: Writer,
}

impl ChannelEntry {
    fn anchor_matches(&self, key: &ChannelKey) -> bool {
        match self.anchor {
            Some(ChannelAnchor::Producer(endpoint_id)) => key.producers().contains(&endpoint_id),
            Some(ChannelAnchor::Consumer(endpoint_id)) => key.consumers().contains(&endpoint_id),
            None => false,
        }
    }
}

#[derive(Default)]
struct EndpointWiring {
    inbound: Vec<WiringIngress>,
    outbound: Vec<WiringEgress>,
}

struct SwitchboardService {
    core: SwitchboardCore,
    endpoints: HashMap<EndpointId, EndpointRegistration>,
    channels: Vec<ChannelEntry>,
}

/// Entry point for the switchboard module.
#[entrypoint]
#[instrument(name = "switchboard.start")]
pub async fn start() -> Result<()> {
    let request_channel = Channel::create(CHANNEL_CAPACITY).await?;

    // Register `Switchboard` as a singleton that can be consumed globally
    let shared = request_channel.share().await?;
    singleton::register(SWITCHBOARD_SINGLETON_ID, shared.raw()).await?;

    info!(
        request_channel = shared.raw(),
        "switchboard: registered Switchboard singleton"
    );

    let mut reader = request_channel.subscribe(REQUEST_CHUNK_SIZE).await?;

    let mut service = SwitchboardService::new();

    while let Some(frame) = reader.next().await {
        match frame {
            Ok(frame) => {
                debug!(
                    len = frame.payload.len(),
                    "switchboard: received request frame"
                );
                if frame.payload.is_empty() {
                    continue;
                }
                if let Err(err) = service.handle_payload(&frame.payload).await {
                    warn!(?err, "switchboard: failed to handle request");
                }
            }
            Err(err) => {
                warn!(?err, "switchboard: request stream failed");
                break;
            }
        }
    }

    Ok(())
}

impl SwitchboardService {
    fn new() -> Self {
        Self {
            core: SwitchboardCore::default(),
            endpoints: HashMap::new(),
            channels: Vec::new(),
        }
    }

    async fn handle_payload(&mut self, payload: &[u8]) -> Result<(), SwitchboardServiceError> {
        let message = decode_message(payload)?;
        self.handle_message(message).await
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), SwitchboardServiceError> {
        match message {
            Message::RegisterRequest {
                request_id,
                directions,
                updates_channel,
            } => {
                debug!(
                    request_id,
                    updates_channel, "switchboard: register request received"
                );
                self.handle_register(request_id, directions, updates_channel)
                    .await
            }
            Message::AdoptRequest {
                request_id,
                directions,
                updates_channel,
                channel,
                mode,
            } => {
                debug!(
                    request_id,
                    updates_channel,
                    channel,
                    mode = ?mode,
                    "switchboard: adopt request received"
                );
                self.handle_adopt(request_id, directions, updates_channel, channel, mode)
                    .await
            }
            Message::ConnectRequest {
                request_id,
                from,
                to,
                reply_channel,
            } => {
                debug!(
                    request_id,
                    from, to, reply_channel, "switchboard: connect request received"
                );
                self.handle_connect(request_id, from, to, reply_channel)
                    .await
            }
            _ => Ok(()),
        }
    }

    async fn handle_register(
        &mut self,
        request_id: u64,
        directions: EndpointDirections,
        updates_channel: u64,
    ) -> Result<(), SwitchboardServiceError> {
        let updates_channel = unsafe { SharedChannel::from_raw(updates_channel) };
        let updates_writer = Channel::attach_shared(updates_channel)
            .await?
            .publish_weak()
            .await?;

        let endpoint_id = self.core.add_endpoint(directions);
        debug!(request_id, endpoint_id, "switchboard: registered endpoint");
        self.endpoints.insert(
            endpoint_id,
            EndpointRegistration {
                updates: updates_writer,
            },
        );

        if let Err(err) = self.reconcile().await {
            self.core.remove_endpoint(endpoint_id);
            self.endpoints.remove(&endpoint_id);
            self.send_error(updates_channel, request_id, err).await?;
            return Ok(());
        }

        let response = Message::ResponseRegister {
            request_id,
            endpoint_id,
        };
        let bytes = encode_message(&response)?;
        if let Some(registration) = self.endpoints.get_mut(&endpoint_id) {
            registration.updates.send(bytes).await?;
        }

        Ok(())
    }

    async fn handle_adopt(
        &mut self,
        request_id: u64,
        directions: EndpointDirections,
        updates_channel: u64,
        channel: u64,
        mode: AdoptMode,
    ) -> Result<(), SwitchboardServiceError> {
        let updates_channel = unsafe { SharedChannel::from_raw(updates_channel) };
        let updates_writer = Channel::attach_shared(updates_channel)
            .await?
            .publish_weak()
            .await?;

        if directions.output().cardinality() == Cardinality::Zero {
            self.send_error(
                updates_channel,
                request_id,
                SwitchboardServiceError::Solver(CoreError::Unsolveable),
            )
            .await?;
            return Ok(());
        }

        let output = (*directions.output()).with_exclusive(true);
        let directions = EndpointDirections::new(*directions.input(), output);

        let endpoint_id = self.core.add_endpoint(directions);
        debug!(request_id, endpoint_id, "switchboard: adopted endpoint");
        self.endpoints.insert(
            endpoint_id,
            EndpointRegistration {
                updates: updates_writer,
            },
        );

        let shared = unsafe { SharedChannel::from_raw(channel) };
        let entry = match self
            .adopt_channel_entry(endpoint_id, directions.output(), shared, mode)
            .await
        {
            Ok(entry) => entry,
            Err(err) => {
                self.core.remove_endpoint(endpoint_id);
                self.endpoints.remove(&endpoint_id);
                self.send_error(updates_channel, request_id, err).await?;
                return Ok(());
            }
        };

        self.channels.push(entry);

        if let Err(err) = self.reconcile().await {
            self.core.remove_endpoint(endpoint_id);
            self.endpoints.remove(&endpoint_id);
            self.remove_anchored_channel(endpoint_id).await;
            self.send_error(updates_channel, request_id, err).await?;
            return Ok(());
        }

        let response = Message::ResponseRegister {
            request_id,
            endpoint_id,
        };
        let bytes = encode_message(&response)?;
        if let Some(registration) = self.endpoints.get_mut(&endpoint_id) {
            registration.updates.send(bytes).await?;
        }

        Ok(())
    }

    async fn handle_connect(
        &mut self,
        request_id: u64,
        from: EndpointId,
        to: EndpointId,
        reply_channel: u64,
    ) -> Result<(), SwitchboardServiceError> {
        let reply_channel = unsafe { SharedChannel::from_raw(reply_channel) };

        if let Err(err) = self.core.add_intent(from, to) {
            self.send_error(reply_channel, request_id, err.into())
                .await?;
            return Ok(());
        }

        if let Err(err) = self.reconcile().await {
            self.core.remove_intent(from, to);
            self.send_error(reply_channel, request_id, err).await?;
            return Ok(());
        }

        let response = Message::ResponseOk { request_id };
        self.send_response(reply_channel, response).await?;
        Ok(())
    }

    async fn send_response(
        &self,
        channel: SharedChannel,
        message: Message,
    ) -> Result<(), SwitchboardServiceError> {
        let channel = Channel::attach_shared(channel).await?;
        let mut writer = channel.publish_weak().await?;
        let bytes = encode_message(&message)?;
        writer.send(bytes).await?;
        Ok(())
    }

    async fn send_error(
        &self,
        channel: SharedChannel,
        request_id: u64,
        err: SwitchboardServiceError,
    ) -> Result<(), SwitchboardServiceError> {
        let response = Message::ResponseError {
            request_id,
            message: err.to_string(),
        };
        self.send_response(channel, response).await
    }

    async fn reconcile(&mut self) -> Result<(), SwitchboardServiceError> {
        let solution = self.core.solve()?;
        let wiring = self.apply_solution(solution).await?;
        self.send_updates(wiring).await?;
        Ok(())
    }

    async fn apply_solution(
        &mut self,
        solution: selium_switchboard_core::Solution,
    ) -> Result<HashMap<EndpointId, EndpointWiring>, SwitchboardServiceError> {
        let mut available = std::mem::take(&mut self.channels);
        let mut retained = Vec::with_capacity(solution.channels.len());
        let mut resolved: Vec<SharedChannel> = Vec::with_capacity(solution.channels.len());

        for spec in &solution.channels {
            let desired_key = spec.key().clone();
            let anchored_positions: Vec<usize> = available
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.anchor_matches(&desired_key))
                .map(|(idx, _)| idx)
                .collect();

            let position = if anchored_positions.is_empty() {
                available
                    .iter()
                    .position(|entry| entry.key == desired_key)
                    .or_else(|| {
                        let keys: Vec<ChannelKey> =
                            available.iter().map(|entry| entry.key.clone()).collect();
                        best_compatible_match(&keys, &desired_key)
                    })
            } else if anchored_positions.len() == 1 {
                let pos = anchored_positions[0];
                if !available[pos].key.is_compatible(&desired_key) {
                    return Err(CoreError::Unsolveable.into());
                }
                Some(pos)
            } else {
                return Err(CoreError::Unsolveable.into());
            };

            if let Some(pos) = position {
                let mut entry = available.swap_remove(pos);
                if entry.key != desired_key {
                    entry.key = desired_key.clone();
                }
                resolved.push(entry.shared);
                retained.push(entry);
            } else {
                let entry = Self::create_channel(desired_key.clone()).await?;
                resolved.push(entry.shared);
                retained.push(entry);
            }
        }

        for entry in available {
            if entry.retained || !entry.owned {
                retained.push(entry);
                continue;
            }
            if let Err(err) = entry.channel.drain().await {
                warn!(?err, "switchboard: failed to drain channel");
            }
            if let Err(err) = entry.channel.delete().await {
                warn!(?err, "switchboard: failed to delete channel");
            }
        }

        self.channels = retained;

        let mut wiring: HashMap<EndpointId, EndpointWiring> = HashMap::new();
        for route in &solution.routes {
            for flow in &route.flows {
                let handle = resolved.get(flow.channel).ok_or(CoreError::Unsolveable)?;
                wiring
                    .entry(flow.producer)
                    .or_default()
                    .outbound
                    .push(WiringEgress {
                        to: flow.consumer,
                        channel: handle.raw(),
                    });
                wiring
                    .entry(flow.consumer)
                    .or_default()
                    .inbound
                    .push(WiringIngress {
                        from: flow.producer,
                        channel: handle.raw(),
                    });
            }
        }

        for entry in wiring.values_mut() {
            entry
                .inbound
                .sort_unstable_by_key(|ingress| (ingress.channel, ingress.from));
            entry.inbound.dedup_by_key(|ingress| ingress.channel);
            entry
                .outbound
                .sort_unstable_by_key(|egress| (egress.channel, egress.to));
            entry.outbound.dedup_by_key(|egress| egress.channel);
        }

        Ok(wiring)
    }

    async fn send_updates(
        &mut self,
        mut wiring: HashMap<EndpointId, EndpointWiring>,
    ) -> Result<(), SwitchboardServiceError> {
        for (endpoint_id, registration) in &mut self.endpoints {
            let update = wiring.remove(endpoint_id).unwrap_or_default();
            let message = Message::WiringUpdate {
                endpoint_id: *endpoint_id,
                inbound: update.inbound,
                outbound: update.outbound,
            };
            let bytes = encode_message(&message)?;
            if let Err(err) = registration.updates.send(bytes).await {
                warn!(?err, endpoint_id, "switchboard: failed to send update");
            }
        }
        Ok(())
    }

    async fn adopt_channel_entry(
        &self,
        endpoint_id: EndpointId,
        output: &selium_switchboard_protocol::Direction,
        shared: SharedChannel,
        mode: AdoptMode,
    ) -> Result<ChannelEntry, SwitchboardServiceError> {
        let key = ChannelKey::new(
            output.schema_id(),
            output.backpressure(),
            std::iter::once(endpoint_id),
            std::iter::empty(),
        );

        match mode {
            AdoptMode::Alias => {
                let channel = Channel::attach_shared(shared).await?;
                Ok(ChannelEntry {
                    channel,
                    shared,
                    key,
                    anchor: Some(ChannelAnchor::Producer(endpoint_id)),
                    owned: false,
                    retained: true,
                })
            }
            AdoptMode::Tap => {
                let source = Channel::attach_shared(shared).await?;
                let mut entry = Self::create_channel(key).await?;
                entry.anchor = Some(ChannelAnchor::Producer(endpoint_id));
                entry.retained = true;
                Self::spawn_tap(source, entry.channel.clone());
                Ok(entry)
            }
        }
    }

    async fn remove_anchored_channel(&mut self, endpoint_id: EndpointId) {
        if let Some(pos) = self.channels.iter().position(|entry| match entry.anchor {
            Some(ChannelAnchor::Producer(id)) | Some(ChannelAnchor::Consumer(id)) => {
                id == endpoint_id
            }
            None => false,
        }) {
            let entry = self.channels.swap_remove(pos);
            self.release_entry(entry).await;
        }
    }

    async fn release_entry(&self, entry: ChannelEntry) {
        if !entry.owned {
            return;
        }
        if let Err(err) = entry.channel.drain().await {
            warn!(?err, "switchboard: failed to drain channel");
        }
        if let Err(err) = entry.channel.delete().await {
            warn!(?err, "switchboard: failed to delete channel");
        }
    }

    fn spawn_tap(source: Channel, target: Channel) {
        spawn(async move {
            if let Err(err) = Self::run_tap(source, target).await {
                warn!(?err, "switchboard: tap stopped");
            }
        });
    }

    async fn run_tap(source: Channel, target: Channel) -> Result<(), DriverError> {
        let mut reader = source.subscribe_weak(TAP_CHUNK_SIZE).await?;
        let mut writer = target.publish_weak().await?;
        while let Some(frame) = reader.next().await {
            match frame {
                Ok(frame) => writer.send(frame.payload).await?,
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    async fn create_channel(key: ChannelKey) -> Result<ChannelEntry, SwitchboardServiceError> {
        let backpressure = match key.backpressure() {
            Backpressure::Park => ChannelBackpressure::Park,
            Backpressure::Drop => ChannelBackpressure::Drop,
        };
        let channel = Channel::create_with_backpressure(CHANNEL_CAPACITY, backpressure).await?;
        let shared = channel.share().await?;
        Ok(ChannelEntry {
            channel,
            shared,
            key,
            anchor: None,
            owned: true,
            retained: false,
        })
    }
}
