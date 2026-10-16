//! Error types for buddy-gateway.

use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, GatewayError>;

/// Unified error type for the gateway.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("missing required credentials for channel '{kind}': {missing:?}")]
    MissingCredentials { kind: String, missing: Vec<String> },

    #[error("unknown channel type: '{0}'")]
    UnknownChannel(String),

    #[error("channel already exists: {0}")]
    ChannelExists(String),

    #[error("channel not found: {0}")]
    ChannelNotFound(String),

    #[error("no processor available")]
    NoProcessor,

    #[error("no media backend configured for channel {0}")]
    NoMediaBackend(String),

    #[error("invalid base64 in ContentPart.data: {0}")]
    InvalidBase64(String),

    #[error("content part has no data/local_path/url")]
    NoMediaSource,

    #[error("config build failed for {channel}: {detail}")]
    ConfigBuild { channel: String, detail: String },

    #[error("media not found: {0}")]
    MediaNotFound(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}
