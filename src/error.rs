//! Error struct and methods
use arrow::error::ArrowError;
use prost::DecodeError;
use std::{io, result};
use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    /// Failure to decode or encode a response or request respectively
    #[error("Encoding/Decoding Error")]
    CodecError,

    #[error("No host reachable")]
    NoHostReachable,

    #[error("Fluss Error ({0:?})")]
    Fluss(FlussCode),

    #[error("decode error")]
    DecodeError(#[from] DecodeError),

    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("invalid table")]
    InvalidTableError(String),

    #[error("table serde error")]
    TableSerDeError(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlussCode {
    Unknown = -1,

    NotLeaderOrFollower = 12,

    CorruptRecord = 14,

    LeaderNotAvaliable = 44,
}

impl FlussCode {
    pub fn from_protocol(n: i16) -> Option<FlussCode> {
        if n == 0 {
            return None;
        }

        if n >= FlussCode::NotLeaderOrFollower as i16 && n <= FlussCode::LeaderNotAvaliable as i16 {
            return Some(unsafe { std::mem::transmute(n as i8) });
        }
        Some(FlussCode::Unknown)
    }
}

impl Error {
    pub fn from_protocol(n: i16) -> Option<Error> {
        FlussCode::from_protocol(n).map(Error::Fluss)
    }
}
