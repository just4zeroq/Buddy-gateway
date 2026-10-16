//! Shared helpers for the Rust examples.
//!
//! Mirrors the small runtime plumbing the Python examples get from
//! `python-dotenv` + `logging.basicConfig()`, plus a couple of processors.

#![allow(dead_code)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, Stream};
use serde_json::Value;

use buddy_gateway::channel::MessageProcessor;
use buddy_gateway::models::{InboundMessage, MessageEvent};

// ---------------------------------------------------------------------------
// Environment / logging
// ---------------------------------------------------------------------------

/// Load a `.env` file (simple `KEY=VALUE` lines) into the process environment
/// without overwriting already-set variables. Mirrors `load_dotenv()`.
///
/// The file path is taken from `DOTENV_PATH` (default: `./.env`). Also
/// initializes `tracing` logging at `INFO`.
pub fn load_dotenv() {
    let path = std::env::var("DOTENV_PATH").unwrap_or_else(|_| ".env".to_string());
    if let Ok(text) = std::fs::read_to_string(&path) {
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix("export ").unwrap_or(line);
            if let Some((k, v)) = line.split_once('=') {
                let k = k.trim();
                let v = v.trim().trim_matches('"').trim_matches('\'');
                if !k.is_empty() && std::env::var_os(k).is_none() {
                    std::env::set_var(k, v);
                }
            }
        }
    }
    init_tracing();
}

/// Initialize `tracing` logging once (safe to call multiple times).
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

/// Read an environment variable (empty string when unset).
pub fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

/// Read an environment variable with a default.
pub fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Parse a boolean env var (`"true"`/`"1"` case-insensitive), with a default.
pub fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            v == "true" || v == "1" || v == "yes"
        }
        Err(_) => default,
    }
}

/// Parse a `u16` env var with a default.
pub fn env_u16(key: &str, default: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Processors
// ---------------------------------------------------------------------------

/// An echo processor: replies `"Echo: {text}"` then completes.
pub fn echo_processor() -> MessageProcessor {
    echo_processor_prefixed("Echo")
}

/// An echo processor with a custom prefix.
pub fn echo_processor_prefixed(prefix: &'static str) -> MessageProcessor {
    Arc::new(move |msg: InboundMessage| {
        let text = msg.text();
        let s = stream::iter(vec![
            MessageEvent::text(format!("{prefix}: {text}")),
            MessageEvent::completed(),
        ]);
        Box::pin(s) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

/// A processor that always returns the same fixed reply.
pub fn static_reply_processor(reply: &'static str) -> MessageProcessor {
    Arc::new(move |_msg: InboundMessage| {
        let s = stream::iter(vec![
            MessageEvent::text(reply.to_string()),
            MessageEvent::completed(),
        ]);
        Box::pin(s) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

// ---------------------------------------------------------------------------
// Constraints
// ---------------------------------------------------------------------------

/// Build a runtime constraints override map from `(key, value)` pairs.
///
/// Recognised keys mirror `Channel::set_constraints`: `show_thinking`,
/// `show_tool_hints`, `reply_timeout`, `typing_keepalive_interval`,
/// `timeout_strategy`, `placeholder_texts`, `send_rate_limit`.
pub fn constraints(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Block until Ctrl-C — mirrors Python's `await asyncio.Event().wait()`.
pub async fn wait_forever() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        // No signal support (non-interactive): park forever.
        Err(_) => std::future::pending::<()>().await,
    }
}
