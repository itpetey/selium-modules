//! Message encoding traits used by switchboard data-plane helpers.

use std::{convert::Infallible, error::Error as StdError};

use selium_switchboard_protocol::SchemaId;

/// Static schema descriptor used by endpoint registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schema {
    /// 16-byte schema identifier.
    pub hash: SchemaId,
}

/// Trait implemented by payload types that advertise a schema id.
pub trait HasSchema {
    /// Schema descriptor for this payload type.
    const SCHEMA: Schema;
}

/// Trait implemented by payload types that can be sent over switchboard channels.
pub trait FlatMsg: Sized {
    /// Decode error type.
    type Error: StdError + Send + Sync + 'static;

    /// Encode this value into bytes.
    fn encode(&self) -> Vec<u8>;

    /// Decode this value from bytes.
    fn decode(bytes: &[u8]) -> Result<Self, Self::Error>;
}

impl HasSchema for () {
    const SCHEMA: Schema = Schema {
        hash: *b"selium.unit.msg\0",
    };
}

impl FlatMsg for () {
    type Error = Infallible;

    fn encode(&self) -> Vec<u8> {
        Vec::new()
    }

    fn decode(_bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(())
    }
}

impl HasSchema for Vec<u8> {
    const SCHEMA: Schema = Schema {
        hash: *b"selium.bytes.msg",
    };
}

impl FlatMsg for Vec<u8> {
    type Error = Infallible;

    fn encode(&self) -> Vec<u8> {
        self.clone()
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(bytes.to_vec())
    }
}

impl HasSchema for String {
    const SCHEMA: Schema = Schema {
        hash: *b"selium.utf8.msg\0",
    };
}

impl FlatMsg for String {
    type Error = std::string::FromUtf8Error;

    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        String::from_utf8(bytes.to_vec())
    }
}
