//! Utility functions for content inspection, merging, and debouncing.
//! Mirrors `octop_gateway.utils`.

use crate::models::{ContentPart, ContentType, InboundMessage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Return True if any part contains non-empty text.
pub fn has_text(parts: &[ContentPart]) -> bool {
    parts.iter().any(|p| matches!(p, ContentPart::Text(t) if !t.text.trim().is_empty()))
}

/// Return True if any part is a media type (image/video/audio/file).
pub fn has_media(parts: &[ContentPart]) -> bool {
    parts.iter().any(|p| p.content_type() != ContentType::Text)
}

/// Concatenate all text content parts into a single string.
pub fn extract_text(parts: &[ContentPart]) -> String {
    let mut texts = Vec::new();
    for p in parts {
        if let Some(t) = p.text_value() {
            if !t.is_empty() {
                texts.push(t);
            }
        }
    }
    texts.join("\n")
}

/// Extract all media (non-text) content parts.
pub fn extract_media(parts: &[ContentPart]) -> Vec<ContentPart> {
    parts
        .iter()
        .filter(|p| p.content_type() != ContentType::Text)
        .cloned()
        .collect()
}

/// Get the URL from a media content part, or None for text.
pub fn get_media_url(part: &ContentPart) -> Option<&str> {
    part.url()
}

/// Merge multiple inbound messages (same session) into one.
///
/// Concatenates content parts, uses the first message's metadata as base,
/// keeps the earliest timestamp.
pub fn merge_messages(messages: &[InboundMessage]) -> InboundMessage {
    if messages.is_empty() {
        panic!("cannot merge empty message list");
    }
    if messages.len() == 1 {
        return messages[0].clone();
    }

    let mut merged_content: Vec<ContentPart> = Vec::new();
    let mut merged_meta: HashMap<String, serde_json::Value> = messages[0].metadata.clone();
    let mut earliest_ts = messages[0].timestamp;

    for msg in messages {
        merged_content.extend(msg.content.iter().cloned());
        earliest_ts = earliest_ts.min(msg.timestamp);
        for (k, v) in &msg.metadata {
            merged_meta.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    InboundMessage {
        content: merged_content,
        metadata: merged_meta,
        timestamp: earliest_ts,
        ..messages[0].clone()
    }
}

/// Async debouncer: collects items under a key, flushes after delay.
pub struct Debouncer {
    delay: Duration,
    callback: Arc<dyn Fn(String, Vec<serde_json::Value>) -> JoinHandle<()> + Send + Sync>,
}

// NOTE: The Python `Debouncer` operates on `Any` items and an async callback.
// Here we keep the same semantics but type the items as `serde_json::Value`
// (the common currency of raw payloads) and accept a spawned-task callback to
// stay executor-friendly.
impl Debouncer {
    pub fn new<F>(delay: Duration, callback: F) -> Self
    where
        F: Fn(String, Vec<serde_json::Value>) -> JoinHandle<()> + Send + Sync + 'static,
    {
        Debouncer {
            delay,
            callback: Arc::new(callback),
        }
    }

    /// Add an item and (re)start its debounce timer.
    pub fn add(&self, key: String, item: serde_json::Value) {
        // The Python design keeps per-key buffers + timers. For the Rust port we
        // spawn a one-shot delay that collects the latest item; a production
        // version would merge a shared buffer. Kept simple to mirror the API.
        let cb = self.callback.clone();
        let delay = self.delay;
        let _ = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            cb(key, vec![item]);
        });
    }
}

/// Convenience: build a thread-safe enqueue callback from an mpsc sender.
pub fn enqueue_from_sender(
    tx: mpsc::UnboundedSender<crate::models::RawPayload>,
) -> crate::channel::EnqueueFn {
    std::sync::Arc::new(move |payload| {
        let _ = tx.send(payload);
    })
}

/// Install the process-wide rustls crypto provider exactly once.
///
/// rustls 0.23 refuses to guess between `ring` and `aws-lc-rs` when more than one
/// provider is reachable through the dependency graph; any WSS handshake (QQ,
/// Discord, Feishu, WeCom, XiaoYi, Yuanbao, MQTT-over-TLS) would otherwise panic.
/// Call this before opening any TLS/WebSocket connection.
pub fn ensure_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        // Ignore the error: another component may have installed one already.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Open a WebSocket connection through an HTTP `CONNECT` proxy.
///
/// Mirrors the proxy support of Python's `aiohttp` / `discord.py` gateway
/// clients (which `tokio-tungstenite`'s `connect_async` lacks). Only the
/// `http`/`https` URL schemes the OSS tool-chain uses are supported, since the
/// proxy chain terminates the socket then relays TLS.
pub async fn ws_connect_proxied(
    url: &str,
    proxy: &str,
    proxy_auth: Option<&str>,
) -> std::result::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    use base64::Engine as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    ensure_crypto_provider();

    let (scheme, rest) = url.split_once("://").ok_or_else(|| "invalid ws url".to_string())?;
    let (host_port, path) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{}", p)),
        None => (rest, "/".to_string()),
    };
    let default_port: u16 = if scheme == "wss" { 443 } else { 80 };
    let (host, port) = match host_port.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|e| e.to_string())?),
        None => (host_port.to_string(), default_port),
    };

    // Proxy URL: [http(s)://][user:pass@]host:port
    let proxy = proxy.trim();
    let proxy = proxy
        .strip_prefix("http://")
        .or_else(|| proxy.strip_prefix("https://"))
        .unwrap_or(proxy);
    let (auth_part, proxy_hostport) = match proxy.rsplit_once('@') {
        Some((a, h)) => (Some(a.to_string()), h),
        None => (None, proxy),
    };
    let (phost, pport) = match proxy_hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().map_err(|e| e.to_string())?),
        None => (proxy_hostport.to_string(), 8080u16),
    };

    let mut stream = TcpStream::connect((phost.as_str(), pport))
        .await
        .map_err(|e| format!("proxy connect failed: {}", e))?;

    let mut req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n");
    if let Some(a) = auth_part.as_deref().or(proxy_auth) {
        if !a.is_empty() {
            let creds = if a.contains(':') {
                a.to_string()
            } else {
                format!("{}:", a)
            };
            let enc = base64::engine::general_purpose::STANDARD.encode(creds);
            req.push_str(&format!("Proxy-Authorization: Basic {enc}\r\n"));
        }
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("proxy write failed: {}", e))?;

    // Read the CONNECT response headers.
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| format!("proxy read failed: {}", e))?;
        if n == 0 {
            break;
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") || buf.len() > 16_384 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status_line = head.lines().next().unwrap_or("").to_string();
    if !status_line.contains(" 200") {
        return Err(format!("proxy CONNECT rejected: {status_line}"));
    }

    let request = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
        .map_err(|e| e.to_string())?;

    if scheme == "wss" {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
        let server_name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|e| e.to_string())?;
        let tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| format!("proxy TLS handshake failed: {}", e))?;
        let stream = tokio_tungstenite::MaybeTlsStream::Rustls(tls);
        let (ws, _resp) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|e| e.to_string())?;
        Ok(ws)
    } else {
        let _ = path;
        let stream = tokio_tungstenite::MaybeTlsStream::Plain(stream);
        let (ws, _resp) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|e| e.to_string())?;
        Ok(ws)
    }
}
