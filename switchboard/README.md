# Switchboard

Switchboard now owns Selium's core queue/shm I/O primitives. The `selium-switchboard`
crate provides queue-synchronised channels backed by host-managed shared memory.

## Crate structure

This workspace has 4 crates:
- `selium-switchboard` (_client/_) - queue/shm I/O primitives plus endpoint/messaging helpers
- `selium-switchboard-core` (_core/_) - graph solver logic
- `selium-switchboard-protocol` (_protocol/_) - switchboard wire protocol types
- `selium-switchboard-server` (_server/_) - switchboard service entrypoint using queue/shm channels

## Usage

Use `selium-switchboard` directly from guest modules and exchange `SharedChannel`
handles between processes as needed.

The server entrypoint now expects the request channel shared id as its first
argument (`start(request_channel: u64)`).
