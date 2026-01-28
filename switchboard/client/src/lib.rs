//! Guest-side switchboard helpers and re-exports.

pub mod messaging;
pub mod switchboard;

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
