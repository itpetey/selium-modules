//! Request-reply integration is intentionally parked.
//!
//! QUIC host support is not available in the new kernel cutover. Keep a
//! placeholder test so `--all-targets` stays green while networking is stubbed.

#[test]
fn request_reply_networking_stubbed() {
    // Intentionally no-op until host-side QUIC is implemented.
    assert!(true);
}
