# Switchboard

The switchboard service provides an orchestration layer on top of core I/O. It provides realtime rationalising of channel infrastructure as the environment changes (e.g. a new subscriber being created, or a service being shut down), as well as a more ergonomic messaging library.

## Crate structure

This service has 4 crates:
- `selium-switchboard` (_client/_) - client library that guests consume
- `selium-switchboard-core` (_core/_) - core logic
- `selium-switchboard-protocol` (_protocol/_) - wire protocol
- `selium-switchboard-server` (_server/_) - WASM module run by the host

## Usage

Compile the `selium-switchboard-server` component to WebAssembly and install in the Runtime's work directory:

```bash
cargo build --release --target wasm32-unknown-unknown -p selium-switchboard-server
cp target/wasm32-unknown-unknown/release/selium_switchboard_server.wasm /path/to/selium-runtime/work/modules/
```

The `selium-switchboard-server` component should be added to the Selium Runtime's initialisation args:

```bash
selium-runtime \
  --work-dir /path/to/selium-runtime/work \
  --module "path=selium_switchboard_server.wasm;capabilities=ChannelLifecycle,ChannelReader,ChannelWriter,SingletonRegistry"
```
