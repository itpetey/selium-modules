//! Flatbuffers protocol helpers for the remote-client control plane.

use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use thiserror::Error;

/// Generated Flatbuffers bindings for the remote-client protocol.
#[allow(missing_docs)]
#[allow(warnings)]
#[rustfmt::skip]
pub mod fbs;

use crate::fbs::remote_client::protocol as fb;

const REMOTE_CLIENT_IDENTIFIER: &str = "RMCL";

/// Guest pointer-sized unsigned integer.
pub type GuestUint = u32;
/// Guest-facing resource identifiers.
pub type GuestResourceId = u64;
/// Identifier for a writer that produced a frame.
pub type GuestWriterId = u16;

/// Kernel capability identifiers shared between host and guest.
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Capability {
    SessionLifecycle = 0,
    ChannelLifecycle = 1,
    ChannelReader = 2,
    ChannelWriter = 3,
    ProcessLifecycle = 4,
    NetBind = 5,
    NetAccept = 6,
    NetConnect = 7,
    NetRead = 8,
    NetWrite = 9,
}

/// Scalar value kinds supported by the ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiScalarType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
}

/// Logical parameter kinds supported by the ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbiParam {
    /// An immediate scalar value.
    Scalar(AbiScalarType),
    /// A byte buffer passed via (ptr, len) pair.
    Buffer,
}

/// Description of a guest entrypoint's parameters and results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiSignature {
    params: Vec<AbiParam>,
    results: Vec<AbiParam>,
}

/// Scalar values supported by the ABI.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AbiScalarValue {
    /// 8-bit signed integer.
    I8(i8),
    /// 8-bit unsigned integer.
    U8(u8),
    /// 16-bit signed integer.
    I16(i16),
    /// 16-bit unsigned integer.
    U16(u16),
    /// 32-bit signed integer.
    I32(i32),
    /// 32-bit unsigned integer.
    U32(u32),
    /// 64-bit signed integer.
    I64(i64),
    /// 64-bit unsigned integer.
    U64(u64),
    /// 32-bit IEEE float.
    F32(f32),
    /// 64-bit IEEE float.
    F64(f64),
}

/// Argument supplied to a process entrypoint.
#[derive(Debug, Clone, PartialEq)]
pub enum EntrypointArg {
    /// Immediate scalar value.
    Scalar(AbiScalarValue),
    /// Raw buffer.
    Buffer(Vec<u8>),
    /// Handle referring to a Selium resource.
    Resource(GuestResourceId),
}

/// Response carrying an attributed frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IoFrame {
    /// Identifier of the writer that produced this frame.
    pub writer_id: GuestWriterId,
    /// Frame payload.
    pub payload: Vec<u8>,
}

impl AbiScalarValue {
    pub fn kind(&self) -> AbiScalarType {
        match self {
            AbiScalarValue::I8(_) => AbiScalarType::I8,
            AbiScalarValue::U8(_) => AbiScalarType::U8,
            AbiScalarValue::I16(_) => AbiScalarType::I16,
            AbiScalarValue::U16(_) => AbiScalarType::U16,
            AbiScalarValue::I32(_) => AbiScalarType::I32,
            AbiScalarValue::U32(_) => AbiScalarType::U32,
            AbiScalarValue::I64(_) => AbiScalarType::I64,
            AbiScalarValue::U64(_) => AbiScalarType::U64,
            AbiScalarValue::F32(_) => AbiScalarType::F32,
            AbiScalarValue::F64(_) => AbiScalarType::F64,
        }
    }
}

impl AbiSignature {
    pub fn new(params: Vec<AbiParam>, results: Vec<AbiParam>) -> Self {
        Self { params, results }
    }

    pub fn params(&self) -> &[AbiParam] {
        &self.params
    }

    pub fn results(&self) -> &[AbiParam] {
        &self.results
    }
}

/// Requests accepted by the remote-client guest.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    /// Create a channel with the requested capacity.
    ChannelCreate(GuestUint),
    /// Delete the referenced channel.
    ChannelDelete(GuestResourceId),
    /// Subscribe to a channel with the requested chunk size.
    Subscribe(ChannelRef, GuestUint),
    /// Publish frames into a channel.
    Publish(GuestResourceId),
    /// Start a process with the supplied details.
    ProcessStart(ProcessStartRequest),
    /// Stop the referenced process.
    ProcessStop(GuestResourceId),
    /// Fetch the logging channel for the referenced process.
    ProcessLogChannel(GuestResourceId),
}

/// Responses emitted by the remote-client guest.
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    /// Channel creation response returning the channel handle.
    ChannelCreate(GuestResourceId),
    /// Frame payload returned from a channel read.
    ChannelRead(IoFrame),
    /// Acknowledgement of a channel write request.
    ChannelWrite(GuestUint),
    /// Process start response returning the process handle.
    ProcessStart(GuestResourceId),
    /// Logging channel response returning the channel handle.
    ProcessLogChannel(GuestResourceId),
    /// Success response with no additional payload.
    Ok,
    /// Error response containing a human-readable message.
    Error(String),
}

/// Reference to a channel capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelRef {
    /// Strong channel handle (read/write access).
    Strong(GuestResourceId),
    /// Shared channel handle (read access only).
    Shared(GuestResourceId),
}

/// Request payload used to start a process in the runtime.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessStartRequest {
    /// Module identifier that should be activated.
    pub module_id: String,
    /// Entrypoint symbol exposed by the module.
    pub entrypoint: String,
    /// Capabilities granted to the process.
    pub capabilities: Vec<Capability>,
    /// Entrypoint signature describing parameter and result layouts.
    pub signature: AbiSignature,
    /// Arguments supplied to the entrypoint.
    pub args: Vec<EntrypointArg>,
}

/// Errors produced while encoding or decoding remote-client payloads.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Flatbuffers payload failed to verify.
    #[error("invalid flatbuffer: {0:?}")]
    InvalidFlatbuffer(InvalidFlatbuffer),
    /// Message payload was not present.
    #[error("remote-client message missing payload")]
    MissingPayload,
    /// Message payload type is unsupported.
    #[error("unknown remote-client payload type")]
    UnknownPayload,
    /// Remote-client message identifier did not match.
    #[error("invalid remote-client message identifier")]
    InvalidIdentifier,
    /// Payload kind did not match the expected direction.
    #[error("remote-client payload was not a {expected}")]
    UnexpectedPayload { expected: &'static str },
    /// Capability variant was not recognised.
    #[error("unknown capability variant")]
    UnknownCapability,
    /// ABI scalar type was not recognised.
    #[error("unknown ABI scalar type")]
    UnknownAbiScalarType,
    /// ABI parameter kind was not recognised.
    #[error("unknown ABI parameter kind")]
    UnknownAbiParamKind,
    /// Channel reference kind was not recognised.
    #[error("unknown channel reference kind")]
    UnknownChannelRefKind,
    /// Entrypoint argument kind was not recognised.
    #[error("unknown entrypoint argument kind")]
    UnknownEntrypointArgKind,
    /// Required field was missing from the payload.
    #[error("missing remote-client field: {0}")]
    MissingField(&'static str),
}

impl From<InvalidFlatbuffer> for ProtocolError {
    fn from(value: InvalidFlatbuffer) -> Self {
        ProtocolError::InvalidFlatbuffer(value)
    }
}

/// Encode a remote-client request into Flatbuffers bytes.
pub fn encode_request(request: &Request) -> Result<Vec<u8>, ProtocolError> {
    let mut builder = FlatBufferBuilder::new();
    let (payload_type, payload) = encode_request_payload(&mut builder, request);
    let message = fb::RemoteClientMessage::create(
        &mut builder,
        &fb::RemoteClientMessageArgs {
            payload_type,
            payload,
        },
    );
    builder.finish(message, Some(REMOTE_CLIENT_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

/// Decode Flatbuffers bytes into a remote-client request.
pub fn decode_request(bytes: &[u8]) -> Result<Request, ProtocolError> {
    let message = decode_message(bytes)?;

    match message.payload_type() {
        fb::RemoteClientPayload::ChannelCreateRequest => {
            let req = message
                .payload_as_channel_create_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::ChannelCreate(req.capacity()))
        }
        fb::RemoteClientPayload::ChannelDeleteRequest => {
            let req = message
                .payload_as_channel_delete_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::ChannelDelete(req.handle()))
        }
        fb::RemoteClientPayload::SubscribeRequest => {
            let req = message
                .payload_as_subscribe_request()
                .ok_or(ProtocolError::MissingPayload)?;
            let target = decode_channel_ref(
                req.target()
                    .ok_or(ProtocolError::MissingField("subscribe.target"))?,
            )?;
            Ok(Request::Subscribe(target, req.chunk_size()))
        }
        fb::RemoteClientPayload::PublishRequest => {
            let req = message
                .payload_as_publish_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::Publish(req.handle()))
        }
        fb::RemoteClientPayload::ProcessStartRequest => {
            let req = message
                .payload_as_process_start_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::ProcessStart(decode_process_start_request(req)?))
        }
        fb::RemoteClientPayload::ProcessStopRequest => {
            let req = message
                .payload_as_process_stop_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::ProcessStop(req.handle()))
        }
        fb::RemoteClientPayload::ProcessLogChannelRequest => {
            let req = message
                .payload_as_process_log_channel_request()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Request::ProcessLogChannel(req.handle()))
        }
        fb::RemoteClientPayload::ChannelCreateResponse
        | fb::RemoteClientPayload::ChannelReadResponse
        | fb::RemoteClientPayload::ChannelWriteResponse
        | fb::RemoteClientPayload::ProcessStartResponse
        | fb::RemoteClientPayload::ProcessLogChannelResponse
        | fb::RemoteClientPayload::OkResponse
        | fb::RemoteClientPayload::ErrorResponse => Err(ProtocolError::UnexpectedPayload {
            expected: "request",
        }),
        _ => Err(ProtocolError::UnknownPayload),
    }
}

/// Encode a remote-client response into Flatbuffers bytes.
pub fn encode_response(response: &Response) -> Result<Vec<u8>, ProtocolError> {
    let mut builder = FlatBufferBuilder::new();
    let (payload_type, payload) = encode_response_payload(&mut builder, response);
    let message = fb::RemoteClientMessage::create(
        &mut builder,
        &fb::RemoteClientMessageArgs {
            payload_type,
            payload,
        },
    );
    builder.finish(message, Some(REMOTE_CLIENT_IDENTIFIER));
    Ok(builder.finished_data().to_vec())
}

/// Decode Flatbuffers bytes into a remote-client response.
pub fn decode_response(bytes: &[u8]) -> Result<Response, ProtocolError> {
    let message = decode_message(bytes)?;

    match message.payload_type() {
        fb::RemoteClientPayload::ChannelCreateResponse => {
            let resp = message
                .payload_as_channel_create_response()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Response::ChannelCreate(resp.handle()))
        }
        fb::RemoteClientPayload::ChannelReadResponse => {
            let resp = message
                .payload_as_channel_read_response()
                .ok_or(ProtocolError::MissingPayload)?;
            let frame = resp
                .frame()
                .ok_or(ProtocolError::MissingField("channel_read.frame"))?;
            Ok(Response::ChannelRead(decode_io_frame(frame)?))
        }
        fb::RemoteClientPayload::ChannelWriteResponse => {
            let resp = message
                .payload_as_channel_write_response()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Response::ChannelWrite(resp.len()))
        }
        fb::RemoteClientPayload::ProcessStartResponse => {
            let resp = message
                .payload_as_process_start_response()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Response::ProcessStart(resp.handle()))
        }
        fb::RemoteClientPayload::ProcessLogChannelResponse => {
            let resp = message
                .payload_as_process_log_channel_response()
                .ok_or(ProtocolError::MissingPayload)?;
            Ok(Response::ProcessLogChannel(resp.handle()))
        }
        fb::RemoteClientPayload::OkResponse => Ok(Response::Ok),
        fb::RemoteClientPayload::ErrorResponse => {
            let resp = message
                .payload_as_error_response()
                .ok_or(ProtocolError::MissingPayload)?;
            let message = resp
                .message()
                .ok_or(ProtocolError::MissingField("error.message"))?
                .to_string();
            Ok(Response::Error(message))
        }
        fb::RemoteClientPayload::ChannelCreateRequest
        | fb::RemoteClientPayload::ChannelDeleteRequest
        | fb::RemoteClientPayload::SubscribeRequest
        | fb::RemoteClientPayload::PublishRequest
        | fb::RemoteClientPayload::ProcessStartRequest
        | fb::RemoteClientPayload::ProcessStopRequest
        | fb::RemoteClientPayload::ProcessLogChannelRequest => {
            Err(ProtocolError::UnexpectedPayload {
                expected: "response",
            })
        }
        _ => Err(ProtocolError::UnknownPayload),
    }
}

fn decode_message(bytes: &[u8]) -> Result<fb::RemoteClientMessage<'_>, ProtocolError> {
    if !fb::remote_client_message_buffer_has_identifier(bytes) {
        return Err(ProtocolError::InvalidIdentifier);
    }
    Ok(flatbuffers::root::<fb::RemoteClientMessage>(bytes)?)
}

fn encode_request_payload<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    request: &Request,
) -> (
    fb::RemoteClientPayload,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) {
    match request {
        Request::ChannelCreate(capacity) => {
            let payload = fb::ChannelCreateRequest::create(
                builder,
                &fb::ChannelCreateRequestArgs {
                    capacity: *capacity,
                },
            );
            (
                fb::RemoteClientPayload::ChannelCreateRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::ChannelDelete(handle) => {
            let payload = fb::ChannelDeleteRequest::create(
                builder,
                &fb::ChannelDeleteRequestArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ChannelDeleteRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::Subscribe(target, chunk_size) => {
            let target = encode_channel_ref(builder, target);
            let payload = fb::SubscribeRequest::create(
                builder,
                &fb::SubscribeRequestArgs {
                    target: Some(target),
                    chunk_size: *chunk_size,
                },
            );
            (
                fb::RemoteClientPayload::SubscribeRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::Publish(handle) => {
            let payload =
                fb::PublishRequest::create(builder, &fb::PublishRequestArgs { handle: *handle });
            (
                fb::RemoteClientPayload::PublishRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::ProcessStart(request) => {
            let payload = encode_process_start_request(builder, request);
            (
                fb::RemoteClientPayload::ProcessStartRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::ProcessStop(handle) => {
            let payload = fb::ProcessStopRequest::create(
                builder,
                &fb::ProcessStopRequestArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ProcessStopRequest,
                Some(payload.as_union_value()),
            )
        }
        Request::ProcessLogChannel(handle) => {
            let payload = fb::ProcessLogChannelRequest::create(
                builder,
                &fb::ProcessLogChannelRequestArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ProcessLogChannelRequest,
                Some(payload.as_union_value()),
            )
        }
    }
}

fn encode_response_payload<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    response: &Response,
) -> (
    fb::RemoteClientPayload,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) {
    match response {
        Response::ChannelCreate(handle) => {
            let payload = fb::ChannelCreateResponse::create(
                builder,
                &fb::ChannelCreateResponseArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ChannelCreateResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::ChannelRead(frame) => {
            let frame = encode_io_frame(builder, frame);
            let payload = fb::ChannelReadResponse::create(
                builder,
                &fb::ChannelReadResponseArgs { frame: Some(frame) },
            );
            (
                fb::RemoteClientPayload::ChannelReadResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::ChannelWrite(len) => {
            let payload = fb::ChannelWriteResponse::create(
                builder,
                &fb::ChannelWriteResponseArgs { len: *len },
            );
            (
                fb::RemoteClientPayload::ChannelWriteResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::ProcessStart(handle) => {
            let payload = fb::ProcessStartResponse::create(
                builder,
                &fb::ProcessStartResponseArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ProcessStartResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::ProcessLogChannel(handle) => {
            let payload = fb::ProcessLogChannelResponse::create(
                builder,
                &fb::ProcessLogChannelResponseArgs { handle: *handle },
            );
            (
                fb::RemoteClientPayload::ProcessLogChannelResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::Ok => {
            let payload = fb::OkResponse::create(builder, &fb::OkResponseArgs {});
            (
                fb::RemoteClientPayload::OkResponse,
                Some(payload.as_union_value()),
            )
        }
        Response::Error(message) => {
            let message = builder.create_string(message);
            let payload = fb::ErrorResponse::create(
                builder,
                &fb::ErrorResponseArgs {
                    message: Some(message),
                },
            );
            (
                fb::RemoteClientPayload::ErrorResponse,
                Some(payload.as_union_value()),
            )
        }
    }
}

fn encode_channel_ref<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    target: &ChannelRef,
) -> flatbuffers::WIPOffset<fb::ChannelRef<'bldr>> {
    let (kind, handle) = match target {
        ChannelRef::Strong(handle) => (fb::ChannelRefKind::Strong, *handle),
        ChannelRef::Shared(handle) => (fb::ChannelRefKind::Shared, *handle),
    };

    fb::ChannelRef::create(builder, &fb::ChannelRefArgs { kind, handle })
}

fn decode_channel_ref(channel_ref: fb::ChannelRef<'_>) -> Result<ChannelRef, ProtocolError> {
    match channel_ref.kind() {
        fb::ChannelRefKind::Strong => Ok(ChannelRef::Strong(channel_ref.handle())),
        fb::ChannelRefKind::Shared => Ok(ChannelRef::Shared(channel_ref.handle())),
        _ => Err(ProtocolError::UnknownChannelRefKind),
    }
}

fn encode_process_start_request<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    request: &ProcessStartRequest,
) -> flatbuffers::WIPOffset<fb::ProcessStartRequest<'bldr>> {
    let module_id = builder.create_string(&request.module_id);
    let entrypoint = builder.create_string(&request.entrypoint);
    let capabilities = encode_capabilities(builder, &request.capabilities);
    let signature = encode_abi_signature(builder, &request.signature);
    let args = encode_entrypoint_args(builder, &request.args);

    fb::ProcessStartRequest::create(
        builder,
        &fb::ProcessStartRequestArgs {
            module_id: Some(module_id),
            entrypoint: Some(entrypoint),
            capabilities: Some(capabilities),
            signature: Some(signature),
            args: Some(args),
        },
    )
}

fn decode_process_start_request(
    request: fb::ProcessStartRequest<'_>,
) -> Result<ProcessStartRequest, ProtocolError> {
    let module_id = request
        .module_id()
        .ok_or(ProtocolError::MissingField("process_start.module_id"))?
        .to_string();
    let entrypoint = request
        .entrypoint()
        .ok_or(ProtocolError::MissingField("process_start.entrypoint"))?
        .to_string();
    let capabilities = decode_capabilities(request.capabilities())?;
    let signature = request
        .signature()
        .ok_or(ProtocolError::MissingField("process_start.signature"))?;
    let signature = decode_abi_signature(signature)?;
    let args = decode_entrypoint_args(request.args())?;

    Ok(ProcessStartRequest {
        module_id,
        entrypoint,
        capabilities,
        signature,
        args,
    })
}

fn encode_capabilities<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    caps: &[Capability],
) -> flatbuffers::WIPOffset<flatbuffers::Vector<'bldr, fb::Capability>> {
    let values: Vec<_> = caps.iter().map(|cap| encode_capability(*cap)).collect();
    builder.create_vector(&values)
}

fn decode_capabilities(
    caps: Option<flatbuffers::Vector<'_, fb::Capability>>,
) -> Result<Vec<Capability>, ProtocolError> {
    let mut out = Vec::new();
    if let Some(vec) = caps {
        for cap in vec.iter() {
            out.push(decode_capability(cap)?);
        }
    }
    Ok(out)
}

fn encode_capability(value: Capability) -> fb::Capability {
    match value {
        Capability::SessionLifecycle => fb::Capability::SessionLifecycle,
        Capability::ChannelLifecycle => fb::Capability::ChannelLifecycle,
        Capability::ChannelReader => fb::Capability::ChannelReader,
        Capability::ChannelWriter => fb::Capability::ChannelWriter,
        Capability::ProcessLifecycle => fb::Capability::ProcessLifecycle,
        Capability::NetBind => fb::Capability::NetBind,
        Capability::NetAccept => fb::Capability::NetAccept,
        Capability::NetConnect => fb::Capability::NetConnect,
        Capability::NetRead => fb::Capability::NetRead,
        Capability::NetWrite => fb::Capability::NetWrite,
    }
}

fn decode_capability(value: fb::Capability) -> Result<Capability, ProtocolError> {
    match value {
        fb::Capability::SessionLifecycle => Ok(Capability::SessionLifecycle),
        fb::Capability::ChannelLifecycle => Ok(Capability::ChannelLifecycle),
        fb::Capability::ChannelReader => Ok(Capability::ChannelReader),
        fb::Capability::ChannelWriter => Ok(Capability::ChannelWriter),
        fb::Capability::ProcessLifecycle => Ok(Capability::ProcessLifecycle),
        fb::Capability::NetBind => Ok(Capability::NetBind),
        fb::Capability::NetAccept => Ok(Capability::NetAccept),
        fb::Capability::NetConnect => Ok(Capability::NetConnect),
        fb::Capability::NetRead => Ok(Capability::NetRead),
        fb::Capability::NetWrite => Ok(Capability::NetWrite),
        _ => Err(ProtocolError::UnknownCapability),
    }
}

fn encode_abi_signature<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    signature: &AbiSignature,
) -> flatbuffers::WIPOffset<fb::AbiSignature<'bldr>> {
    let params = encode_abi_params(builder, signature.params());
    let results = encode_abi_params(builder, signature.results());

    fb::AbiSignature::create(
        builder,
        &fb::AbiSignatureArgs {
            params: Some(params),
            results: Some(results),
        },
    )
}

fn decode_abi_signature(signature: fb::AbiSignature<'_>) -> Result<AbiSignature, ProtocolError> {
    let params = decode_abi_params(signature.params())?;
    let results = decode_abi_params(signature.results())?;
    Ok(AbiSignature::new(params, results))
}

fn encode_abi_params<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    params: &[AbiParam],
) -> flatbuffers::WIPOffset<
    flatbuffers::Vector<'bldr, flatbuffers::ForwardsUOffset<fb::AbiParam<'bldr>>>,
> {
    let items: Vec<_> = params
        .iter()
        .map(|param| encode_abi_param(builder, param))
        .collect();
    builder.create_vector(&items)
}

fn decode_abi_params(
    params: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<fb::AbiParam<'_>>>>,
) -> Result<Vec<AbiParam>, ProtocolError> {
    let mut out = Vec::new();
    if let Some(vec) = params {
        for param in vec {
            out.push(decode_abi_param(param)?);
        }
    }
    Ok(out)
}

fn encode_abi_param<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    param: &AbiParam,
) -> flatbuffers::WIPOffset<fb::AbiParam<'bldr>> {
    let (kind, scalar_type) = match param {
        AbiParam::Scalar(scalar) => (fb::AbiParamKind::Scalar, encode_abi_scalar_type(*scalar)),
        AbiParam::Buffer => (fb::AbiParamKind::Buffer, fb::AbiScalarType::I8),
    };

    fb::AbiParam::create(builder, &fb::AbiParamArgs { kind, scalar_type })
}

fn decode_abi_param(param: fb::AbiParam<'_>) -> Result<AbiParam, ProtocolError> {
    match param.kind() {
        fb::AbiParamKind::Scalar => {
            let scalar = decode_abi_scalar_type(param.scalar_type())?;
            Ok(AbiParam::Scalar(scalar))
        }
        fb::AbiParamKind::Buffer => Ok(AbiParam::Buffer),
        _ => Err(ProtocolError::UnknownAbiParamKind),
    }
}

fn encode_entrypoint_args<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    args: &[EntrypointArg],
) -> flatbuffers::WIPOffset<
    flatbuffers::Vector<'bldr, flatbuffers::ForwardsUOffset<fb::EntrypointArg<'bldr>>>,
> {
    let items: Vec<_> = args
        .iter()
        .map(|arg| encode_entrypoint_arg(builder, arg))
        .collect();
    builder.create_vector(&items)
}

fn decode_entrypoint_args(
    args: Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<fb::EntrypointArg<'_>>>>,
) -> Result<Vec<EntrypointArg>, ProtocolError> {
    let mut out = Vec::new();
    if let Some(vec) = args {
        for arg in vec {
            out.push(decode_entrypoint_arg(arg)?);
        }
    }
    Ok(out)
}

fn encode_entrypoint_arg<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    arg: &EntrypointArg,
) -> flatbuffers::WIPOffset<fb::EntrypointArg<'bldr>> {
    match arg {
        EntrypointArg::Scalar(value) => {
            let scalar = encode_scalar_value(builder, value);
            fb::EntrypointArg::create(
                builder,
                &fb::EntrypointArgArgs {
                    kind: fb::EntrypointArgKind::Scalar,
                    scalar: Some(scalar),
                    buffer: None,
                    resource: 0,
                },
            )
        }
        EntrypointArg::Buffer(bytes) => {
            let buffer = builder.create_vector(bytes);
            fb::EntrypointArg::create(
                builder,
                &fb::EntrypointArgArgs {
                    kind: fb::EntrypointArgKind::Buffer,
                    scalar: None,
                    buffer: Some(buffer),
                    resource: 0,
                },
            )
        }
        EntrypointArg::Resource(handle) => fb::EntrypointArg::create(
            builder,
            &fb::EntrypointArgArgs {
                kind: fb::EntrypointArgKind::Resource,
                scalar: None,
                buffer: None,
                resource: *handle,
            },
        ),
    }
}

fn decode_entrypoint_arg(arg: fb::EntrypointArg<'_>) -> Result<EntrypointArg, ProtocolError> {
    match arg.kind() {
        fb::EntrypointArgKind::Scalar => {
            let scalar = arg
                .scalar()
                .ok_or(ProtocolError::MissingField("entrypoint_arg.scalar"))?;
            Ok(EntrypointArg::Scalar(decode_scalar_value(scalar)?))
        }
        fb::EntrypointArgKind::Buffer => {
            let payload = arg
                .buffer()
                .map(|buf| buf.iter().collect())
                .unwrap_or_default();
            Ok(EntrypointArg::Buffer(payload))
        }
        fb::EntrypointArgKind::Resource => Ok(EntrypointArg::Resource(arg.resource())),
        _ => Err(ProtocolError::UnknownEntrypointArgKind),
    }
}

fn encode_scalar_value<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    value: &AbiScalarValue,
) -> flatbuffers::WIPOffset<fb::ScalarValue<'bldr>> {
    let (kind, bits) = scalar_bits(value);
    fb::ScalarValue::create(builder, &fb::ScalarValueArgs { kind, bits })
}

fn decode_scalar_value(value: fb::ScalarValue<'_>) -> Result<AbiScalarValue, ProtocolError> {
    let kind = decode_abi_scalar_type(value.kind())?;
    let bits = value.bits();
    let bytes = bits.to_le_bytes();
    let scalar = match kind {
        AbiScalarType::I8 => AbiScalarValue::I8(i8::from_le_bytes([bytes[0]])),
        AbiScalarType::U8 => AbiScalarValue::U8(bytes[0]),
        AbiScalarType::I16 => AbiScalarValue::I16(i16::from_le_bytes([bytes[0], bytes[1]])),
        AbiScalarType::U16 => AbiScalarValue::U16(u16::from_le_bytes([bytes[0], bytes[1]])),
        AbiScalarType::I32 => {
            AbiScalarValue::I32(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        }
        AbiScalarType::U32 => {
            AbiScalarValue::U32(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        }
        AbiScalarType::I64 => AbiScalarValue::I64(i64::from_le_bytes(bytes)),
        AbiScalarType::U64 => AbiScalarValue::U64(bits),
        AbiScalarType::F32 => AbiScalarValue::F32(f32::from_bits(u32::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ]))),
        AbiScalarType::F64 => AbiScalarValue::F64(f64::from_bits(bits)),
    };
    Ok(scalar)
}

fn scalar_bits(value: &AbiScalarValue) -> (fb::AbiScalarType, u64) {
    match value {
        AbiScalarValue::I8(v) => (
            fb::AbiScalarType::I8,
            u64::from_le_bytes(i64::from(*v).to_le_bytes()),
        ),
        AbiScalarValue::U8(v) => (fb::AbiScalarType::U8, u64::from(*v)),
        AbiScalarValue::I16(v) => (
            fb::AbiScalarType::I16,
            u64::from_le_bytes(i64::from(*v).to_le_bytes()),
        ),
        AbiScalarValue::U16(v) => (fb::AbiScalarType::U16, u64::from(*v)),
        AbiScalarValue::I32(v) => (
            fb::AbiScalarType::I32,
            u64::from_le_bytes(i64::from(*v).to_le_bytes()),
        ),
        AbiScalarValue::U32(v) => (fb::AbiScalarType::U32, u64::from(*v)),
        AbiScalarValue::I64(v) => (fb::AbiScalarType::I64, u64::from_le_bytes(v.to_le_bytes())),
        AbiScalarValue::U64(v) => (fb::AbiScalarType::U64, *v),
        AbiScalarValue::F32(v) => (fb::AbiScalarType::F32, u64::from(v.to_bits())),
        AbiScalarValue::F64(v) => (fb::AbiScalarType::F64, v.to_bits()),
    }
}

fn encode_abi_scalar_type(value: AbiScalarType) -> fb::AbiScalarType {
    match value {
        AbiScalarType::I8 => fb::AbiScalarType::I8,
        AbiScalarType::U8 => fb::AbiScalarType::U8,
        AbiScalarType::I16 => fb::AbiScalarType::I16,
        AbiScalarType::U16 => fb::AbiScalarType::U16,
        AbiScalarType::I32 => fb::AbiScalarType::I32,
        AbiScalarType::U32 => fb::AbiScalarType::U32,
        AbiScalarType::I64 => fb::AbiScalarType::I64,
        AbiScalarType::U64 => fb::AbiScalarType::U64,
        AbiScalarType::F32 => fb::AbiScalarType::F32,
        AbiScalarType::F64 => fb::AbiScalarType::F64,
    }
}

fn decode_abi_scalar_type(value: fb::AbiScalarType) -> Result<AbiScalarType, ProtocolError> {
    match value {
        fb::AbiScalarType::I8 => Ok(AbiScalarType::I8),
        fb::AbiScalarType::U8 => Ok(AbiScalarType::U8),
        fb::AbiScalarType::I16 => Ok(AbiScalarType::I16),
        fb::AbiScalarType::U16 => Ok(AbiScalarType::U16),
        fb::AbiScalarType::I32 => Ok(AbiScalarType::I32),
        fb::AbiScalarType::U32 => Ok(AbiScalarType::U32),
        fb::AbiScalarType::I64 => Ok(AbiScalarType::I64),
        fb::AbiScalarType::U64 => Ok(AbiScalarType::U64),
        fb::AbiScalarType::F32 => Ok(AbiScalarType::F32),
        fb::AbiScalarType::F64 => Ok(AbiScalarType::F64),
        _ => Err(ProtocolError::UnknownAbiScalarType),
    }
}

fn encode_io_frame<'bldr>(
    builder: &mut FlatBufferBuilder<'bldr>,
    frame: &IoFrame,
) -> flatbuffers::WIPOffset<fb::IoFrame<'bldr>> {
    let payload = builder.create_vector(&frame.payload);
    fb::IoFrame::create(
        builder,
        &fb::IoFrameArgs {
            writer_id: frame.writer_id,
            payload: Some(payload),
        },
    )
}

fn decode_io_frame(frame: fb::IoFrame<'_>) -> Result<IoFrame, ProtocolError> {
    let payload = frame
        .payload()
        .map(|buf| buf.iter().collect())
        .unwrap_or_default();
    Ok(IoFrame {
        writer_id: frame.writer_id(),
        payload,
    })
}
