//! This module covers everything related to error handling in this crate.

use r2d2::Error as R2d2Error;
use redis::RedisError;
use serde_json::Error as SerdeJsonError;
use std::error::Error;
use std::fmt;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::sync::PoisonError;
use std::time::SystemTimeError;

/// Error kinds used in this crate. For more specific error kinds handling use source error.
#[non_exhaustive]
#[derive(Debug)]
pub enum IpcErrorKind {
    /// Redis connection failure
    ConnectionFailure,
    /// Redis connection timeout. Should be used only with desired timeout.
    Timeout,
    /// Serializing/deserializing error.
    InvalidData,
    /// Error when accessing memory, e.g. poisoned lock. Should not ever happen.
    MemoryAccessError,
    /// IoError, which does not contain in any kind above.
    OtherIoError,
    /// Errors which can't be matched with other kind.
    Other,
}

/// Error type for this crate. It contains only basic kind of error, which may help with its
/// handling. For more exhaustive information please use [`IpcError::get_ref()`](IpcError::get_ref).
#[derive(Debug)]
pub struct IpcError {
    /// Error kind
    kind: IpcErrorKind,
    /// Source error or string.
    error: Box<dyn Error + Send + Sync>,
}

impl IpcError {
    /// Constructs new error from given `kind` and `error`. Error may be another structure, which
    /// implements [`Error`](std::io::Error) trait or often [`String`](String).
    pub fn new<E>(kind: IpcErrorKind, error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        Self {
            kind,
            error: error.into(),
        }
    }

    /// Returns [`kind`](Self::kind).
    pub fn kind(&self) -> &IpcErrorKind {
        &self.kind
    }

    /// Consumes self and returns source `error`.
    pub fn into_inner(self) -> Box<dyn Error + Send + Sync> {
        self.error
    }

    /// Returns reference to source error.
    pub fn get_ref(&self) -> &(dyn Error + 'static) {
        self.error.as_ref()
    }
}

impl From<RedisError> for IpcError {
    fn from(error: RedisError) -> Self {
        IpcError::new(IpcErrorKind::ConnectionFailure, error)
    }
}

impl From<SerdeJsonError> for IpcError {
    fn from(error: SerdeJsonError) -> Self {
        IpcError::new(IpcErrorKind::InvalidData, error)
    }
}

impl From<R2d2Error> for IpcError {
    fn from(error: R2d2Error) -> Self {
        IpcError::new(IpcErrorKind::ConnectionFailure, error)
    }
}

impl From<SystemTimeError> for IpcError {
    fn from(error: SystemTimeError) -> Self {
        IpcError::new(IpcErrorKind::Other, error)
    }
}

/// Converts [`PoisonError`](PoisonError) into [`IpcError`](IpcError). Error content is dropped.
impl<T> From<PoisonError<T>> for IpcError {
    fn from(_: PoisonError<T>) -> Self {
        IpcError::new(IpcErrorKind::MemoryAccessError, "Cannot access guard.")
    }
}

/// Converts [`io::Error`](IoError) into [`IpcError`](IpcError) and map its kind.
impl From<IoError> for IpcError {
    fn from(error: IoError) -> Self {
        match error.kind() {
            IoErrorKind::InvalidData | IoErrorKind::InvalidInput => {
                IpcError::new(IpcErrorKind::InvalidData, error)
            }
            IoErrorKind::Deadlock => IpcError::new(IpcErrorKind::MemoryAccessError, error),
            IoErrorKind::TimedOut => IpcError::new(IpcErrorKind::Timeout, error),
            IoErrorKind::ConnectionAborted
            | IoErrorKind::ConnectionRefused
            | IoErrorKind::ConnectionReset
            | IoErrorKind::NotConnected => IpcError::new(IpcErrorKind::Timeout, error),
            _ => IpcError::new(IpcErrorKind::OtherIoError, error),
        }
    }
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IpcError: {}", self.error.to_string())
    }
}

impl Error for IpcError {
    /// Returns [`IpcError::get_ref()`](Self::get_ref) result, which is error source.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.error.as_ref())
    }
}
