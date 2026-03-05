//! Legacy remote-client server module.
//!
//! The new Selium ABI removed the legacy network/channel subsystem surface used
//! by this guest module. Keep this crate as an explicit stub so builds remain
//! deterministic during the hard cutover.

use anyhow::{Result, bail};
use selium_userland::entrypoint;

/// Deprecated entrypoint retained for packaging compatibility.
#[entrypoint]
pub async fn start(_domain: &str, _port: u16) -> Result<()> {
    bail!(
        "selium-remote-client-server is deprecated for the queue/shm ABI cutover; \
         use direct runtime integration instead"
    );
}
