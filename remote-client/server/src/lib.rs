use std::future::ready;

use anyhow::{Result, anyhow};
use futures::{SinkExt, StreamExt, TryFutureExt};
use selium_remote_client_protocol::{
    self as protocol, ChannelRef, ProcessStartRequest, Request, Response, decode_request,
    encode_response,
};
use selium_userland::{
    abi as userland_abi, entrypoint,
    io::{Channel, SharedChannel},
    net::{Connection, Listener, NetError, Reader, Writer},
    process::{ProcessBuilder, ProcessHandle},
};
use tracing::{debug, error, instrument, warn};

/// Maximum number of incoming connections we can handle concurrently.
const MAX_CLIENTS: usize = 1000;
/// Capabilities a remote client is permitted to request for a started process.
const ALLOWED_PROCESS_CAPABILITIES: &[protocol::Capability] = &[
    protocol::Capability::ChannelLifecycle,
    protocol::Capability::ChannelReader,
    protocol::Capability::ChannelWriter,
];

#[entrypoint]
#[instrument(name = "start")]
async fn start(domain: &str, port: u16) -> Result<()> {
    let listener = Listener::bind(domain, port).await?;
    listener
        .incoming()
        .filter_map(|client| ready(client.ok()))
        .for_each_concurrent(Some(MAX_CLIENTS), handle_conn)
        .await;

    Ok(())
}

#[instrument(skip_all, fields(?conn))]
async fn handle_conn(mut conn: Connection) {
    loop {
        debug!("waiting for inbound frame");
        match conn.recv().await {
            Ok(Some(req)) => {
                debug!(len = req.payload.len(), "received raw frame");
                let (reader, writer) = conn.borrow_split();
                if let Err(e) = handle_req(req, reader, writer).await {
                    warn!(error = ?e, "request handler error");
                }
            }
            Ok(None) => break,
            Err(e) => {
                error!(error = ?e, "connection handler error");
                break;
            }
        }
    }
}

#[instrument(skip_all, fields(request_id = req.writer_id, reader_id = reader.handle(), writer_id = writer.handle()))]
async fn handle_req(
    req: userland_abi::IoFrame,
    reader: &mut Reader,
    writer: &mut Writer,
) -> Result<()> {
    let request = match decode_request(&req.payload) {
        Ok(r) => r,
        Err(e) => {
            debug!(error = ?e, "could not decode request");
            send(Response::Error(format!("decode request: {e}")), writer).await?;
            return Ok(());
        }
    };

    if let Err(e) = dispatch(request, reader, writer).await {
        send(Response::Error(e.to_string()), writer).await?;
    }

    Ok(())
}

#[instrument(skip_all, fields(?request, reader_id = reader.handle(), writer_id = writer.handle()))]
async fn dispatch(request: Request, reader: &mut Reader, writer: &mut Writer) -> Result<()> {
    debug!("dispatching request");

    match request {
        Request::ChannelCreate(capacity) => {
            let chan = Channel::create(capacity).await?;
            send(Response::ChannelCreate(chan.handle()), writer).await?;
        }
        Request::ChannelDelete(handle) => {
            unsafe { Channel::from_raw(handle) }
                .delete()
                .map_err(|e| anyhow!(e))
                .and_then(|_| send(Response::Ok, writer))
                .await?;
        }
        Request::Subscribe(reference, chunk_size) => {
            debug!(chunk_size, "handling subscribe request");

            let chan = match reference {
                ChannelRef::Strong(handle) => unsafe { Channel::from_raw(handle) },
                ChannelRef::Shared(handle) => {
                    Channel::attach_shared(unsafe { SharedChannel::from_raw(handle) }).await?
                }
            };
            let sub = chan.subscribe(chunk_size).await?;
            sub.map(|r| r.and_then(|f| prefix_frame(f.payload)))
                .forward(writer)
                .await?;
        }
        Request::Publish(handle) => {
            let chan = unsafe { Channel::from_raw(handle) };
            let publ = chan.publish().await?;
            send(Response::Ok, writer).await?;
            reader.map(|r| r.map(|f| f.payload)).forward(publ).await?;
        }
        Request::ProcessStart(start) => {
            let handle = start_process(start).await?;
            send(Response::ProcessStart(handle.raw()), writer).await?;
        }
        Request::ProcessStop(handle) => {
            unsafe { ProcessHandle::from_raw(handle) }
                .stop()
                .map_err(|e| anyhow!(e))
                .and_then(|_| send(Response::Ok, writer))
                .await?
        }
        Request::ProcessLogChannel(handle) => {
            debug!(process = handle, "looking up process log channel");
            let channel = unsafe { ProcessHandle::from_raw(handle) }
                .log_channel()
                .await
                .map_err(|e| anyhow!(e))?;
            debug!(
                process = handle,
                channel = channel.raw(),
                "resolved process log channel"
            );
            send(Response::ProcessLogChannel(channel.raw()), writer).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all, fields(?response, writer_id = writer.handle()))]
async fn send(response: Response, writer: &mut Writer) -> Result<()> {
    debug!("sending reply");

    let encoded = encode_response(&response).map_err(|e| anyhow!(e))?;
    let framed = prefix_frame(encoded).map_err(|e| anyhow!(e))?;
    writer.send(framed).await?;
    Ok(())
}

fn prefix_frame(payload: Vec<u8>) -> Result<Vec<u8>, NetError> {
    let len = u32::try_from(payload.len()).map_err(|_| NetError::InvalidArgument)?;
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&len.to_le_bytes());
    framed.extend_from_slice(&payload);
    Ok(framed)
}

async fn start_process(request: ProcessStartRequest) -> Result<ProcessHandle> {
    let ProcessStartRequest {
        module_id,
        entrypoint,
        capabilities,
        signature,
        args,
    } = request;

    ensure_permitted_capabilities(&capabilities)?;

    let signature = map_signature(&signature);
    let builder = capabilities.iter().fold(
        ProcessBuilder::new(module_id, entrypoint).signature(signature),
        |builder, capability| builder.capability(map_capability(*capability)),
    );

    args.iter()
        .fold(builder, |builder, arg| match arg {
            protocol::EntrypointArg::Scalar(value) => builder.arg_scalar(map_scalar_value(*value)),
            protocol::EntrypointArg::Buffer(bytes) => builder.arg_buffer(bytes.clone()),
            protocol::EntrypointArg::Resource(handle) => builder.arg_resource(*handle),
        })
        .start()
        .await
        .map_err(|e| anyhow!(e))
}

fn ensure_permitted_capabilities(capabilities: &[protocol::Capability]) -> Result<()> {
    if let Some(capability) = capabilities
        .iter()
        .find(|capability| !ALLOWED_PROCESS_CAPABILITIES.contains(capability))
    {
        return Err(anyhow!("capability not permitted: {capability:?}"));
    }

    Ok(())
}

fn map_capability(capability: protocol::Capability) -> userland_abi::Capability {
    match capability {
        protocol::Capability::SessionLifecycle => userland_abi::Capability::SessionLifecycle,
        protocol::Capability::ChannelLifecycle => userland_abi::Capability::ChannelLifecycle,
        protocol::Capability::ChannelReader => userland_abi::Capability::ChannelReader,
        protocol::Capability::ChannelWriter => userland_abi::Capability::ChannelWriter,
        protocol::Capability::ProcessLifecycle => userland_abi::Capability::ProcessLifecycle,
        protocol::Capability::NetBind => userland_abi::Capability::NetBind,
        protocol::Capability::NetAccept => userland_abi::Capability::NetAccept,
        protocol::Capability::NetConnect => userland_abi::Capability::NetConnect,
        protocol::Capability::NetRead => userland_abi::Capability::NetRead,
        protocol::Capability::NetWrite => userland_abi::Capability::NetWrite,
    }
}

fn map_signature(signature: &protocol::AbiSignature) -> userland_abi::AbiSignature {
    let params = signature.params().iter().copied().map(map_param).collect();
    let results = signature.results().iter().copied().map(map_param).collect();
    userland_abi::AbiSignature::new(params, results)
}

fn map_param(param: protocol::AbiParam) -> userland_abi::AbiParam {
    match param {
        protocol::AbiParam::Scalar(kind) => userland_abi::AbiParam::Scalar(map_scalar_type(kind)),
        protocol::AbiParam::Buffer => userland_abi::AbiParam::Buffer,
    }
}

fn map_scalar_type(kind: protocol::AbiScalarType) -> userland_abi::AbiScalarType {
    match kind {
        protocol::AbiScalarType::I8 => userland_abi::AbiScalarType::I8,
        protocol::AbiScalarType::U8 => userland_abi::AbiScalarType::U8,
        protocol::AbiScalarType::I16 => userland_abi::AbiScalarType::I16,
        protocol::AbiScalarType::U16 => userland_abi::AbiScalarType::U16,
        protocol::AbiScalarType::I32 => userland_abi::AbiScalarType::I32,
        protocol::AbiScalarType::U32 => userland_abi::AbiScalarType::U32,
        protocol::AbiScalarType::I64 => userland_abi::AbiScalarType::I64,
        protocol::AbiScalarType::U64 => userland_abi::AbiScalarType::U64,
        protocol::AbiScalarType::F32 => userland_abi::AbiScalarType::F32,
        protocol::AbiScalarType::F64 => userland_abi::AbiScalarType::F64,
    }
}

fn map_scalar_value(value: protocol::AbiScalarValue) -> userland_abi::AbiScalarValue {
    match value {
        protocol::AbiScalarValue::I8(val) => userland_abi::AbiScalarValue::I8(val),
        protocol::AbiScalarValue::U8(val) => userland_abi::AbiScalarValue::U8(val),
        protocol::AbiScalarValue::I16(val) => userland_abi::AbiScalarValue::I16(val),
        protocol::AbiScalarValue::U16(val) => userland_abi::AbiScalarValue::U16(val),
        protocol::AbiScalarValue::I32(val) => userland_abi::AbiScalarValue::I32(val),
        protocol::AbiScalarValue::U32(val) => userland_abi::AbiScalarValue::U32(val),
        protocol::AbiScalarValue::I64(val) => userland_abi::AbiScalarValue::I64(val),
        protocol::AbiScalarValue::U64(val) => userland_abi::AbiScalarValue::U64(val),
        protocol::AbiScalarValue::F32(val) => userland_abi::AbiScalarValue::F32(val),
        protocol::AbiScalarValue::F64(val) => userland_abi::AbiScalarValue::F64(val),
    }
}
