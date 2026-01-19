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
    net::{Connection, NetError, QuicListener, Reader, Writer},
    process::{ProcessBuilder, ProcessHandle},
};
use tracing::{debug, error, instrument, warn};

/// Maximum number of incoming connections we can handle concurrently.
const MAX_CLIENTS: usize = 1000;
/// Capabilities a remote client is permitted to request for a started process.
const ALLOWED_PROCESS_CAPABILITIES: &[protocol::Capability] = &protocol::Capability::ALL;

#[entrypoint]
#[instrument(name = "start")]
async fn start(domain: &str, port: u16) -> Result<()> {
    let listener = QuicListener::bind(domain, port).await?;
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

    let builder = capabilities.iter().fold(
        ProcessBuilder::new(module_id, entrypoint).signature(signature),
        |builder, capability| builder.capability(*capability),
    );

    args.iter()
        .fold(builder, |builder, arg| match arg {
            protocol::EntrypointArg::Scalar(value) => builder.arg_scalar(*value),
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
