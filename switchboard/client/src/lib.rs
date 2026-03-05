//! Guest-side switchboard helpers and re-exports.

pub mod channel;
pub mod encoding;
pub mod messaging;
pub mod switchboard;

/// Queue/shm channel primitives.
pub use channel::{
    Channel, ChannelBackpressure, ChannelConfig, ChannelError, ChannelHandle, DriverError, IoFrame,
    Reader, ReaderMode, SendOutcome, SharedChannel, Writer,
};
/// Payload encoding traits for switchboard messaging.
pub use encoding::{FlatMsg, HasSchema, Schema};
/// Messaging helpers built on the switchboard.
pub use messaging::{
    Client, ClientTarget, ClientTargets, Fanout, Publisher, PublisherTarget, PublisherTargets,
    RequestCtx, Responder, Server, ServerTarget, ServerTargets, Subscriber, SubscriberTarget,
    SubscriberTargets,
};
/// Flatbuffers protocol types for switchboard control messages.
pub use selium_switchboard_protocol as protocol;
/// Switchboard client types for guest code.
pub use switchboard::{
    AdoptMode, Backpressure, Cardinality, EndpointBuilder, EndpointHandle, EndpointId, Switchboard,
    SwitchboardError,
};
