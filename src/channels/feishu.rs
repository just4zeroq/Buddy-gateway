//! Feishu (Lark) channel.
//!
//! Connects to Feishu with a
//! WebSocket long connection for receiving messages and the REST API
//! (`open.feishu.cn/open-apis`) for sending. Supports text, images and file
//! messages in both DM and group chats.
//!
//! Components:
//! - tenant_access_token lifecycle (cached with expiry + a refresh margin),
//! - bot `open_id` resolution (`/bot/v3/info`) for group @mention detection,
//! - a WebSocket client (endpoint discovery + hello + ping/pong + event
//! dispatch) with reconnect backoff and an unhealthy-state watchdog,
//! - `im.message.receive_v1` handling (bot-skip, bounded dedup, typing
//! reaction ack, raw payload enqueue),
//! - `parse_inbound` for text/image/file/audio/sticker/post/interactive,
//! - outbound `post` (markdown) / `image` / `file` messages, thread replies,
//! media upload, `fetch_remote_media` (Bearer auth) and
//! `enrich_push_metadata` (`to_handle`).
//!
//! NOTE (fidelity): the real lark-oapi WebSocket transport exchanges protobuf
//! frames; this port has no protobuf dependency so it uses the documented
//! endpoint discovery plus JSON hello/ping/pong/event frames. The receive-side
//! event JSON is structurally identical to the webhook `im.message.receive_v1`
//! payload, so all event parsing/reaction/dedup logic lives here.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::channel::{Channel, ChannelConfig, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, TextContent,
};
use crate::push_routing::alias_subject_fields;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Feishu OpenAPI base URL.
const API_BASE: &str = "https://open.feishu.cn/open-apis";

/// Maximum number of message IDs tracked for deduplication.
const DEDUP_MAX_SIZE: usize = 2000;

/// Token refresh interval (1.5h; token is valid for 2h).
const TOKEN_REFRESH_INTERVAL: f64 = 5400.0;

// WebSocket health: detect stuck reconnects / dead loops and hard-restart.
const WS_WATCHDOG_INTERVAL: f64 = 30.0;
const WS_RECONNECT_STALE_SECONDS: f64 = 180.0;
const WS_ACTIVITY_STALE_SECONDS: f64 = 3600.0;
const WS_RESTART_BACKOFF_SECONDS: f64 = 5.0;

/// Documented lark WebSocket endpoint-discovery URL.
const WS_ENDPOINT_URL: &str = "https://open.feishu.cn/callback/ws/endpoint";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the Feishu channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FeishuConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Feishu application ID.
    pub app_id: String,
    /// Feishu application secret.
    pub app_secret: String,
    /// Webhook verification token (HTTP event callbacks).
    pub verification_token: String,
    /// Event encryption key (HTTP event callbacks).
    pub encrypt_key: String,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Cached tenant_access_token with its absolute expiry (unix seconds).
#[derive(Debug, Clone, Default)]
struct TokenState {
    token: String,
    expires_at: f64,
}

/// Bounded LRU-ish dedup set for message ids (`_seen_message_ids`).
///
/// NOTE: the shared `ChannelCore::is_duplicate` is capped at 1000 entries and
/// lives behind `&mut self`, which a detached WebSocket task cannot hold. The
/// Feishu channel keeps its own 2000-entry cache, so this port keeps an
/// equivalent dedicated cache (capacity [`DEDUP_MAX_SIZE`]) in `fields`.
#[derive(Default)]
struct DedupCache {
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl DedupCache {
    /// Returns true when `id` was already seen; otherwise records it.
    fn is_duplicate(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            return true;
        }
        self.seen.insert(id.to_string());
        self.order.push_back(id.to_string());
        while self.order.len() > DEDUP_MAX_SIZE {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        false
    }
}

/// Transport health / connection bookkeeping.
#[derive(Default)]
struct WsHealth {
    /// Last observed activity (unix millis; 0 = never).
    last_activity_at: AtomicU64,
    /// When a reconnect started (unix seconds) while not yet reconnected.
    reconnecting_since: StdMutex<Option<f64>>,
    /// Whether the transport is considered alive.
    alive: AtomicBool,
}

impl WsHealth {
    fn mark_activity(&self) {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_activity_at.store(ms, Ordering::Relaxed);
    }

    fn set_alive(&self, alive: bool) {
        self.alive.store(alive, Ordering::Relaxed);
    }

    fn set_reconnecting(&self, on: bool) {
        let mut g = self.reconnecting_since.lock().unwrap();
        if on {
            if g.is_none() {
                *g = Some(crate::channel::now_f64());
            }
        } else {
            *g = None;
        }
    }

    fn reconnecting_since(&self) -> Option<f64> {
        *self.reconnecting_since.lock().unwrap()
    }

    /// Return True when the WS transport needs a hard restart.
    fn is_unhealthy(&self) -> bool {
        if !self.alive.load(Ordering::Relaxed) {
            return true;
        }
        if let Some(since) = self.reconnecting_since() {
            return (crate::channel::now_f64() - since) >= WS_RECONNECT_STALE_SECONDS;
        }
        let ms = self.last_activity_at.load(Ordering::Relaxed);
        if ms == 0 {
            return false;
        }
        let idle = crate::channel::now_f64() - (ms as f64 / 1000.0);
        idle >= WS_ACTIVITY_STALE_SECONDS
    }

    /// Human-readable reason for the current unhealthy state.
    fn unhealthy_reason(&self) -> String {
        if !self.alive.load(Ordering::Relaxed) {
            return "thread_dead".to_string();
        }
        let since = self.reconnecting_since();
        if let Some(s) = since {
            let age = crate::channel::now_f64() - s;
            if age >= WS_RECONNECT_STALE_SECONDS {
                return format!("reconnect_stale={:.0}s", age);
            }
        }
        let ms = self.last_activity_at.load(Ordering::Relaxed);
        if ms > 0 {
            let idle = crate::channel::now_f64() - (ms as f64 / 1000.0);
            if since.is_none() && idle >= WS_ACTIVITY_STALE_SECONDS {
                return format!("activity_stale={:.0}s", idle);
            }
        }
        "unknown".to_string()
    }
}

crate::channel_adapter! {
    /// Feishu (Lark) channel.
    FeishuChannel, FeishuConfig, "feishu",
    required = [app_id, app_secret],
    fields = {
        token_state: Arc<AsyncMutex<TokenState>>,
        bot_open_id: Arc<StdMutex<Option<String>>>,
        ws_session_id: Arc<StdMutex<Option<String>>>,
        seen_messages: Arc<AsyncMutex<DedupCache>>,
        running: Arc<AtomicBool>,
        health: Arc<WsHealth>,
        restart_tx: Option<tokio::sync::mpsc::UnboundedSender<()>>,
        ws_task: Option<JoinHandle<()>>,
        watchdog_task: Option<JoinHandle<()>>,
    }
}

// ---------------------------------------------------------------------------
// Token management
// ---------------------------------------------------------------------------

/// Obtain or refresh the tenant_access_token (shared by `&self` methods and the
/// detached WebSocket task).
async fn refresh_token_core(
    app_id: &str,
    app_secret: &str,
    http: &reqwest::Client,
    state: &Arc<AsyncMutex<TokenState>>,
) -> Result<String> {
    let mut st = state.lock().await;
    let now = crate::channel::now_f64();
    if !st.token.is_empty() && now < st.expires_at {
        return Ok(st.token.clone());
    }

    let url = format!("{}/auth/v3/tenant_access_token/internal", API_BASE);
    let payload = json!({ "app_id": app_id, "app_secret": app_secret });
    let resp = http
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu token refresh HTTP error: {e}")))?;
    let data: Value = resp
        .json()
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu token refresh JSON error: {e}")))?;

    if data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
        return Err(GatewayError::Other(format!(
            "Feishu token refresh failed: {}",
            data.get("msg").and_then(|m| m.as_str()).unwrap_or("unknown error")
        )));
    }
    let token = data
        .get("tenant_access_token")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    if token.is_empty() {
        return Err(GatewayError::Other(
            "Feishu token refresh failed: empty tenant_access_token".to_string(),
        ));
    }
    let expire = data.get("expire").and_then(|e| e.as_i64()).unwrap_or(7200) as f64;
    st.token = token.clone();
    // Refresh slightly before actual expiry.
    st.expires_at = now + (expire - 300.0).min(TOKEN_REFRESH_INTERVAL);
    tracing::debug!("Feishu tenant token refreshed, expires in {}s", expire);
    Ok(token)
}

/// Trim a string to at most `n` characters (for compact log/format output).
fn short(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

impl FeishuChannel {
    /// Refresh (or reuse) the tenant_access_token.
    async fn refresh_token(&self) -> Result<String> {
        let http = self.core.ensure_http().await?;
        refresh_token_core(
            &self.config.app_id,
            &self.config.app_secret,
            &http,
            &self.token_state,
        )
        .await
    }

    /// Resolve and cache this bot's `open_id` via `/bot/v3/info` (best-effort).
    async fn ensure_bot_open_id(&self) -> Option<String> {
        if let Some(id) = self.bot_open_id.lock().unwrap().clone() {
            if !id.is_empty() {
                return Some(id);
            }
        }
        let http = match self.core.ensure_http().await {
            Ok(h) => h,
            Err(_) => return None,
        };
        let token = match self.refresh_token().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Failed to resolve Feishu bot open_id: {e}");
                return None;
            }
        };
        let url = format!("{}/bot/v3/info", API_BASE);
        let data: Value = match http
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
        {
            Ok(resp) => match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("Failed to resolve Feishu bot open_id: {e}");
                    return None;
                }
            },
            Err(e) => {
                tracing::warn!("Failed to resolve Feishu bot open_id: {e}");
                return None;
            }
        };
        if data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
            tracing::warn!(
                "Feishu bot info failed: {}",
                data.get("msg").and_then(|m| m.as_str()).unwrap_or("unknown")
            );
            return None;
        }
        let open_id = data
            .get("bot")
            .and_then(|b| b.get("open_id"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if open_id.is_empty() {
            return None;
        }
        tracing::info!("Feishu bot open_id resolved: {}", short(&open_id, 12));
        *self.bot_open_id.lock().unwrap() = Some(open_id.clone());
        Some(open_id)
    }

    // -----------------------------------------------------------------------
    // Outbound delivery
    // -----------------------------------------------------------------------

    /// Route one outbound message, staying inside the originating thread.
    ///
    /// Messages that arrived in a Feishu thread are answered with the reply API
    /// so the response lands in the same topic; otherwise the plain send API is
    /// used. Falls back to a chat send when the source message cannot be
    /// replied to.
    async fn deliver(&self, subject: &ChannelSubject, msg_type: &str, content: &str) -> Result<()> {
        let meta = &subject.metadata;
        let thread_id = meta.get("thread_id").and_then(|v| v.as_str()).unwrap_or("");
        let reply_to = meta.get("message_id").and_then(|v| v.as_str()).unwrap_or("");

        if !thread_id.is_empty() && !reply_to.is_empty() {
            let data = self.reply_message(reply_to, msg_type, content).await;
            if data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 {
                return Ok(());
            }
            tracing::warn!(
                "Feishu thread reply failed (code={:?}), falling back to chat send: thread={}",
                data.get("code"),
                short(thread_id, 16)
            );
        }

        let to_handle = meta.get("to_handle").and_then(|v| v.as_str()).unwrap_or("");
        let receive_id = if to_handle.is_empty() {
            subject.subject_id.clone()
        } else {
            to_handle.to_string()
        };
        let chat_type = meta.get("chat_type").and_then(|v| v.as_str()).unwrap_or("");
        let receive_id_type = helpers::resolve_receive_id_type(&receive_id, chat_type);
        self.send_message(&receive_id, &receive_id_type, msg_type, content)
            .await
    }

    /// Reply to a message inside its thread via the Feishu reply API.
    async fn reply_message(&self, message_id: &str, msg_type: &str, content: &str) -> Value {
        let http = match self.core.ensure_http().await {
            Ok(h) => h,
            Err(_) => return json!({ "code": -1, "msg": "http_error" }),
        };
        let token = match self.refresh_token().await {
            Ok(t) => t,
            Err(_) => return json!({ "code": -1, "msg": "http_error" }),
        };
        let url = format!(
            "{}/im/v1/messages/{}/reply",
            API_BASE,
            urlencoding::encode(message_id)
        );
        let payload = json!({
            "msg_type": msg_type,
            "content": content,
            "reply_in_thread": true,
        });
        match http
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => {
                let data: Value = resp
                    .json()
                    .await
                    .unwrap_or_else(|_| json!({ "code": -1, "msg": "invalid_json" }));
                if data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) == 0 {
                    tracing::info!(
                        "Feishu thread reply sent: parent={} msg_type={}",
                        short(message_id, 16),
                        msg_type
                    );
                } else {
                    tracing::error!(
                        "Feishu reply_message failed: code={:?} msg={:?} parent={}",
                        data.get("code"),
                        data.get("msg"),
                        short(message_id, 16)
                    );
                }
                data
            }
            Err(e) => {
                tracing::warn!(
                    "HTTP error replying to Feishu message {}: {e}",
                    short(message_id, 16)
                );
                json!({ "code": -1, "msg": "http_error" })
            }
        }
    }

    /// Send a message through the Feishu Messages API.
    async fn send_message(
        &self,
        receive_id: &str,
        receive_id_type: &str,
        msg_type: &str,
        content: &str,
    ) -> Result<()> {
        let http = self.core.ensure_http().await?;
        let token = self.refresh_token().await?;
        let url = format!("{}/im/v1/messages?receive_id_type={}", API_BASE, receive_id_type);
        let payload = json!({
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": content,
        });
        match http
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => {
                let data: Value = resp
                    .json()
                    .await
                    .unwrap_or_else(|_| json!({ "code": -1, "msg": "invalid_json" }));
                let code = data.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                if code != 0 {
                    tracing::error!(
                        "Feishu send_message failed: code={:?} msg={:?} receive_id={}",
                        data.get("code"),
                        data.get("msg"),
                        short(receive_id, 16)
                    );
                } else {
                    tracing::info!(
                        "Feishu message sent: receive_id={} msg_type={}",
                        short(receive_id, 16),
                        msg_type
                    );
                }
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "HTTP error sending Feishu message to {}: {e}",
                    short(receive_id, 16)
                );
                Ok(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // Media upload
    // -----------------------------------------------------------------------

    /// Upload an image to Feishu and return the image_key.
    async fn upload_image_to_feishu(&self, media: &ContentPart) -> Result<String> {
        let (data, mime) = self.load_media_bytes(media).await?;
        let http = self.core.ensure_http().await?;
        let token = self.refresh_token().await?;
        let url = format!("{}/im/v1/images", API_BASE);
        let mime = if mime.is_empty() {
            "image/png".to_string()
        } else {
            mime
        };
        let part = reqwest::multipart::Part::bytes(data.clone())
            .file_name("image.png".to_string())
            .mime_str(&mime)
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(data));
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part("image", part);

        let resp = http
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .multipart(form)
            .send()
            .await
            .map_err(|e| GatewayError::Other(format!("Feishu image upload HTTP error: {e}")))?;
        let result: Value = resp
            .json()
            .await
            .map_err(|e| GatewayError::Other(format!("Feishu image upload HTTP error: {e}")))?;
        if result.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
            return Err(GatewayError::Other(format!(
                "Feishu image upload failed: {}",
                result.get("msg").and_then(|m| m.as_str()).unwrap_or("")
            )));
        }
        Ok(result
            .get("data")
            .and_then(|d| d.get("image_key"))
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string())
    }

    /// Upload a file (or audio) to Feishu and return the file_key.
    async fn upload_file_to_feishu(&self, media: &ContentPart) -> Result<String> {
        let (data, mime) = self.load_media_bytes(media).await?;

        let (filename, file_type, content_type) = match media {
            ContentPart::File(f) => {
                let filename = if f.filename.is_empty() {
                    "file".to_string()
                } else {
                    f.filename.clone()
                };
                let content_type = match f.mime_type.as_deref() {
                    Some(mt) if !mt.is_empty() => mt.to_string(),
                    _ if !mime.is_empty() => mime.clone(),
                    _ => "application/octet-stream".to_string(),
                };
                let file_type = helpers::guess_feishu_file_type(&filename);
                (filename, file_type, content_type)
            }
            _ => {
                let content_type = match media.mime_type() {
                    Some(mt) if !mt.is_empty() => mt.to_string(),
                    _ if !mime.is_empty() => mime.clone(),
                    _ => "audio/ogg".to_string(),
                };
                ("audio.opus".to_string(), "opus".to_string(), content_type)
            }
        };

        let http = self.core.ensure_http().await?;
        let token = self.refresh_token().await?;
        let url = format!("{}/im/v1/files", API_BASE);
        let part = reqwest::multipart::Part::bytes(data.clone())
            .file_name(filename.clone())
            .mime_str(&content_type)
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(data));
        let form = reqwest::multipart::Form::new()
            .text("file_type", file_type)
            .text("file_name", filename.clone())
            .part("file", part);

        let resp = http
            .post(&url)
            .header("Authorization", format!("Bearer {token}"))
            .multipart(form)
            .send()
            .await
            .map_err(|e| GatewayError::Other(format!("Feishu file upload HTTP error: {e}")))?;
        let result: Value = resp
            .json()
            .await
            .map_err(|e| GatewayError::Other(format!("Feishu file upload HTTP error: {e}")))?;
        if result.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
            return Err(GatewayError::Other(format!(
                "Feishu file upload failed: {}",
                result.get("msg").and_then(|m| m.as_str()).unwrap_or("")
            )));
        }
        Ok(result
            .get("data")
            .and_then(|d| d.get("file_key"))
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string())
    }

    /// Send rich content parts (text then media) to Feishu.
    async fn send_media_inner(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        match media {
            ContentPart::Image(_) => {
                let image_key = self.upload_image_to_feishu(media).await?;
                let content = serde_json::to_string(&json!({ "image_key": image_key }))?;
                self.deliver(subject, "image", &content).await
            }
            ContentPart::File(_) | ContentPart::Audio(_) => {
                let file_key = self.upload_file_to_feishu(media).await?;
                let filename = match media {
                    ContentPart::File(f) => f.filename.clone(),
                    _ => "audio".to_string(),
                };
                let content =
                    serde_json::to_string(&json!({ "file_key": file_key, "file_name": filename }))?;
                self.deliver(subject, "file", &content).await
            }
            _ => {
                // Fallback: send URL as text.
                if let Some(url) = media.url() {
                    if !url.is_empty() {
                        let label = media.label_with_filename();
                        self.send_text(subject, &format!("[{label}: {url}]")).await?;
                    }
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Channel for FeishuChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    /// Feishu inherits the base defaults (does not override
    /// `_default_constraints`).
    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints::default()
    }

    async fn start(&mut self) -> Result<()> {
        // 1) Obtain the tenant_access_token (propagates on failure).
        let http = self.core.ensure_http().await?;
        refresh_token_core(
            &self.config.app_id,
            &self.config.app_secret,
            &http,
            &self.token_state,
        )
        .await?;

        // 2) Resolve the bot open_id for group @mention detection (best-effort).
        self.ensure_bot_open_id().await;

        // 3) New WebSocket session id (used as channel_session_id).
        let session = uuid::Uuid::new_v4().to_string();
        *self.ws_session_id.lock().unwrap() = Some(session.clone());
        self.core.set_connection_session(Some(session.clone()));

        self.running.store(true, Ordering::Relaxed);
        self.health.set_alive(true);

        // 4) Restart signal channel (watchdog -> ws supervisor).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        self.restart_tx = Some(tx.clone());

        // 5) WebSocket supervisor task.
        let ctx = WsCtx {
            app_id: self.config.app_id.clone(),
            app_secret: self.config.app_secret.clone(),
            http: http.clone(),
            enqueue: self.core.enqueue_sender(),
            dedup: self.seen_messages.clone(),
            session_state: self.ws_session_id.clone(),
            token_state: self.token_state.clone(),
            health: self.health.clone(),
            running: self.running.clone(),
        };
        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        self.ws_task = Some(tokio::spawn(async move {
            ws_supervisor(ctx, rx).await;
        }));

        // 6) Watchdog task.
        let health = self.health.clone();
        let running = self.running.clone();
        if let Some(t) = self.watchdog_task.take() {
            t.abort();
        }
        self.watchdog_task = Some(tokio::spawn(async move {
            ws_watchdog_loop(health, tx, running).await;
        }));

        tracing::info!("FeishuChannel started (app_id={})", self.config.app_id);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        // Cancel the watchdog first, then drop the WS supervisor (dropping its
        // future closes the socket). Rust's `select!` restart path guarantees a
        // single live connection, so the orphan-thread guard
        // (EXCEED_CONN_LIMIT) is unnecessary here.
        if let Some(t) = self.watchdog_task.take() {
            t.abort();
        }
        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        self.restart_tx = None;
        self.health.set_alive(false);
        self.health.set_reconnecting(false);
        self.core.set_connection_session(None);
        tracing::info!("FeishuChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        // Send as a "post" with markdown support so code blocks/bold/links render.
        let content = serde_json::to_string(&helpers::build_post_content(text))?;
        self.deliver(subject, "post", &content).await
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        match self.send_media_inner(subject, media).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::error!(
                    "Failed to send media to Feishu: {}: {e}",
                    media.label_with_filename()
                );
                // Fallback: prefer URL link, otherwise mark as local-only attachment.
                let label = media.label_with_filename();
                if let Some(url) = media.url() {
                    if !url.is_empty() {
                        return self.send_text(subject, &format!("[Attachment: {url}]")).await;
                    }
                }
                if media.local_path().is_some() {
                    return self
                        .send_text(subject, &format!("[{label} (local upload failed)]"))
                        .await;
                }
                Ok(())
            }
        }
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let v = raw.as_json().ok_or_else(|| {
            GatewayError::Other("FeishuChannel.parse_inbound expects JSON payload".into())
        })?;

        // Accept either the normalized payload built by the event handler or a
        // raw `im.message.receive_v1` event wrapper.
        let payload = helpers::normalize_event_payload(v);

        let sender_id = payload
            .pointer("/sender/sender_id")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let sender_name = payload
            .pointer("/sender/sender_name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let chat_id = payload
            .get("chat_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let chat_type = payload
            .get("chat_type")
            .and_then(|x| x.as_str())
            .unwrap_or("p2p")
            .to_string();
        let thread_id = payload
            .get("thread_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let message_type = payload
            .get("message_type")
            .and_then(|x| x.as_str())
            .unwrap_or("text")
            .to_string();
        let raw_content = payload.get("content").cloned().unwrap_or_else(|| json!("{}"));
        let create_time = payload
            .get("create_time")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let mentions: Vec<Value> = payload
            .get("mentions")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();

        // Session key: the WebSocket session id (or a stable placeholder).
        let session_id = self
            .ws_session_id
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "feishu-unconnected".to_string());

        // Timestamp: create_time is a millisecond epoch string.
        let mut timestamp = crate::channel::now_f64();
        if !create_time.is_empty() {
            if let Ok(millis) = create_time.parse::<i64>() {
                timestamp = millis as f64 / 1000.0;
            }
        }

        let content_data: Value = match &raw_content {
            Value::String(s) => serde_json::from_str(s).unwrap_or_else(|_| json!({})),
            other => other.clone(),
        };

        let message_id = payload
            .get("message_id")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();
        let parent_id = payload
            .get("parent_id")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();

        let mut parts = helpers::parse_content_parts(&message_type, &content_data, &message_id);

        // Strip @bot placeholders so the agent sees clean content.
        let bot_id = self.bot_open_id.lock().unwrap().clone();
        if !mentions.is_empty() {
            let keys = helpers::bot_mention_keys(&mentions, bot_id.as_deref());
            parts = helpers::strip_bot_mentions_from_parts(parts, &keys);
        }

        // Build metadata for reply routing.
        let to_handle = if chat_type == "group" {
            chat_id.clone()
        } else {
            sender_id.clone()
        };
        // A Feishu thread is its own conversation: key by thread_id when present.
        let subject_id = if !thread_id.is_empty() {
            thread_id.clone()
        } else {
            to_handle.clone()
        };
        let subject_chat_type = if chat_type == "group" { "group" } else { "direct" }.to_string();
        let bot_mentioned = helpers::is_bot_mentioned(&chat_type, &mentions, bot_id.as_deref());

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("message_id".to_string(), json!(message_id));
        metadata.insert("chat_id".to_string(), json!(chat_id));
        metadata.insert("chat_type".to_string(), json!(chat_type));
        metadata.insert("to_handle".to_string(), json!(to_handle));
        metadata.insert("sender_id".to_string(), json!(sender_id));
        metadata.insert("message_type".to_string(), json!(message_type));
        metadata.insert("bot_mentioned".to_string(), json!(bot_mentioned));
        metadata.insert("mentions".to_string(), json!(mentions));
        if !thread_id.is_empty() {
            metadata.insert("thread_id".to_string(), json!(thread_id));
        }
        if !parent_id.is_empty() {
            metadata.insert("parent_id".to_string(), json!(parent_id));
        }
        if !sender_name.is_empty() {
            metadata.insert("sender_name".to_string(), json!(sender_name));
        }

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "feishu".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id,
                first_seen: None,
                last_seen: None,
                display_name: String::new(),
                chat_type: subject_chat_type,
                metadata: metadata.clone(),
            }),
            channel_session_id: session_id,
            content: parts,
            group_context: None,
            metadata,
            timestamp,
        })
    }

    /// Download a URL, attaching the tenant_access_token for Feishu API URLs.
    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.core.ensure_http().await?;
        let mut req = http.get(url);
        if url.contains("open.feishu.cn") || url.contains("open-apis") {
            let token = self.refresh_token().await?;
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "media fetch failed: {}",
                resp.status()
            )));
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let data = resp.bytes().await?;
        Ok((data.to_vec(), content_type))
    }

    /// Back-fill the routing handle (`to_handle`) for proactive pushes.
    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        mut meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        if !subject.chat_type.is_empty() {
            meta.entry("chat_type".to_string())
                .or_insert_with(|| Value::String(subject.chat_type.clone()));
        }
        // subject_id may be a thread_id (omt_...), which is not a send target;
        // prefer the chat/open id when backfilling the routing handle.
        let chat_id = meta
            .get("chat_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let handle_src = if chat_id.is_empty() {
            subject.subject_id.clone()
        } else {
            chat_id
        };
        alias_subject_fields(meta, &handle_src, &["to_handle"])
    }
}

// ---------------------------------------------------------------------------
// WebSocket transport (endpoint discovery + supervisor + watchdog)
// ---------------------------------------------------------------------------

/// Everything the detached WebSocket task needs.
struct WsCtx {
    app_id: String,
    app_secret: String,
    http: reqwest::Client,
    enqueue: Option<EnqueueFn>,
    dedup: Arc<AsyncMutex<DedupCache>>,
    session_state: Arc<StdMutex<Option<String>>>,
    token_state: Arc<AsyncMutex<TokenState>>,
    health: Arc<WsHealth>,
    running: Arc<AtomicBool>,
}

/// Discover the WebSocket endpoint + client config via the documented API.
///
/// Returns `(url, ping_interval_seconds, reconnect_interval_seconds)`.
async fn fetch_ws_endpoint(
    app_id: &str,
    app_secret: &str,
    http: &reqwest::Client,
) -> Result<(String, f64, f64)> {
    let resp = http
        .post(WS_ENDPOINT_URL)
        .header("Content-Type", "application/json")
        .json(&json!({ "AppID": app_id, "AppSecret": app_secret }))
        .send()
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu ws endpoint HTTP error: {e}")))?;
    let v: Value = resp
        .json()
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu ws endpoint JSON error: {e}")))?;
    if v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) != 0 {
        return Err(GatewayError::Other(format!("Feishu ws endpoint failed: {v}")));
    }
    let data = v
        .get("data")
        .ok_or_else(|| GatewayError::Other("Feishu ws endpoint: missing data".to_string()))?;
    let url = data
        .get("URL")
        .or_else(|| data.get("url"))
        .and_then(|u| u.as_str())
        .ok_or_else(|| GatewayError::Other("Feishu ws endpoint: missing URL".to_string()))?
        .to_string();
    let cfg = data.get("ClientConfig").or_else(|| data.get("client_config"));
    let ping = cfg
        .and_then(|c| c.get("PingInterval").or_else(|| c.get("ping_interval")))
        .and_then(|p| p.as_f64())
        .unwrap_or(120.0);
    let reconnect = cfg
        .and_then(|c| c.get("ReconnectInterval").or_else(|| c.get("reconnect_interval")))
        .and_then(|p| p.as_f64())
        .unwrap_or(120.0);
    Ok((url, ping, reconnect))
}

/// Supervisor: keeps a WebSocket connection alive with reconnect backoff and
/// honours watchdog restart requests.
async fn ws_supervisor(ctx: WsCtx, mut restart_rx: tokio::sync::mpsc::UnboundedReceiver<()>) {
    ctx.health.set_alive(true);
    let mut backoff = 1.0f64;
    loop {
        if !ctx.running.load(Ordering::Relaxed) {
            break;
        }
        ctx.health.set_reconnecting(true);

        let mut forced = false;
        let mut connected = false;
        tokio::select! {
            _ = restart_rx.recv() => {
                forced = true;
            }
            r = run_ws_connection(&ctx) => {
                match r {
                    Ok(c) => connected = c,
                    Err(e) => tracing::warn!("Feishu WebSocket connection error: {e}"),
                }
            }
        }

        if forced {
            tracing::warn!("Feishu WebSocket restart requested by watchdog");
            backoff = 1.0;
        } else if connected {
            backoff = 1.0;
        } else {
            backoff = (backoff * 2.0).min(60.0);
        }

        ctx.health.set_reconnecting(true);
        // Backoff wait, but wake immediately when a restart is requested.
        let _ = tokio::time::timeout(
            Duration::from_secs_f64(backoff),
            restart_rx.recv(),
        )
        .await;
    }
    ctx.health.set_alive(false);
}

/// One connection attempt: discover the endpoint, connect, hello, then loop on
/// ping + inbound frames until the socket closes or an error occurs.
///
/// Returns `Ok(true)` when a connection was established (so the supervisor can
/// reset its backoff), `Ok(false)`/`Err(_)` otherwise.
async fn run_ws_connection(ctx: &WsCtx) -> Result<bool> {
    let (url, ping_interval, _reconnect_interval) =
        fetch_ws_endpoint(&ctx.app_id, &ctx.app_secret, &ctx.http).await?;

    let (ws, _resp) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu ws connect failed: {e}")))?;
    let (mut write, mut read) = ws.split();

    // Auth/hello frame.
    let hello = json!({
        "type": "hello",
        "app_id": ctx.app_id,
        "app_secret": ctx.app_secret,
    });
    write
        .send(Message::Text(hello.to_string().into()))
        .await
        .map_err(|e| GatewayError::Other(format!("Feishu ws hello failed: {e}")))?;

    // New session id for this connection.
    let session = uuid::Uuid::new_v4().to_string();
    *ctx.session_state.lock().unwrap() = Some(session.clone());
    ctx.health.set_reconnecting(false);
    ctx.health.mark_activity();
    tracing::info!("Feishu WebSocket connected (session={})", session);

    let mut ping = tokio::time::interval(Duration::from_secs_f64(ping_interval.max(1.0)));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if !ctx.running.load(Ordering::Relaxed) {
            return Ok(true);
        }
        tokio::select! {
            _ = ping.tick() => {
                let frame = json!({ "type": "ping" }).to_string();
                if write.send(Message::Text(frame.into())).await.is_err() {
                    ctx.health.set_reconnecting(true);
                    return Ok(true);
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        process_ws_message(t.as_str(), ctx).await;
                    }
                    Some(Ok(Message::Binary(b))) => {
                        if let Ok(text) = std::str::from_utf8(&b) {
                            let text = text.to_string();
                            process_ws_message(&text, ctx).await;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = write.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => {
                        ctx.health.set_reconnecting(true);
                        return Ok(true);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::warn!("Feishu ws recv error: {e}");
                        ctx.health.set_reconnecting(true);
                        return Ok(true);
                    }
                }
            }
        }
    }
}

/// Watchdog: hard-restart the transport when it looks dead or stuck.
async fn ws_watchdog_loop(
    health: Arc<WsHealth>,
    restart_tx: tokio::sync::mpsc::UnboundedSender<()>,
    running: Arc<AtomicBool>,
) {
    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_secs_f64(WS_WATCHDOG_INTERVAL)).await;
        if !running.load(Ordering::Relaxed) {
            break;
        }
        if !health.is_unhealthy() {
            continue;
        }
        let reason = health.unhealthy_reason();
        tracing::warn!("Feishu WebSocket unhealthy ({reason}); restarting");
        if restart_tx.send(()).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_secs_f64(WS_RESTART_BACKOFF_SECONDS)).await;
    }
}

/// Route one inbound WebSocket frame (JSON): control frames are ignored, and
/// `im.message.receive_v1` events are dispatched.
async fn process_ws_message(text: &str, ctx: &WsCtx) {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    // Control frames (hello/ping/pong/ack) — real lark uses protobuf here.
    if v.get("type").and_then(|x| x.as_str()).is_some() {
        return;
    }
    let et = v
        .pointer("/header/event_type")
        .and_then(|x| x.as_str())
        .unwrap_or("");
    if et != "im.message.receive_v1" {
        return;
    }
    handle_message_event(&v, ctx).await;
}

/// Handle one `im.message.receive_v1` event: bot-skip, dedup, typing reaction,
/// enqueue the normalized raw payload.
async fn handle_message_event(v: &Value, ctx: &WsCtx) {
    if !ctx.running.load(Ordering::Relaxed) {
        return;
    }
    let (message_id, raw) = match helpers::build_message_payload(v) {
        Some(x) => x,
        None => return,
    };

    {
        let mut dedup = ctx.dedup.lock().await;
        if dedup.is_duplicate(&message_id) {
            tracing::debug!("Duplicate Feishu message ignored: {message_id}");
            return;
        }
    }

    let (message_type, sender_id) = (
        raw.get("message_type").and_then(|x| x.as_str()).unwrap_or(""),
        raw.pointer("/sender/sender_id")
            .and_then(|x| x.as_str())
            .unwrap_or(""),
    );
    tracing::info!(
        "Feishu received message: id={} type={} sender={}",
        message_id,
        message_type,
        short(sender_id, 10)
    );
    ctx.health.mark_activity();

    // Acknowledge receipt with a "Typing" reaction (fire-and-forget).
    {
        let http = ctx.http.clone();
        let app_id = ctx.app_id.clone();
        let app_secret = ctx.app_secret.clone();
        let token_state = ctx.token_state.clone();
        let mid = message_id.clone();
        tokio::spawn(async move {
            add_reaction(mid, "Typing".to_string(), http, app_id, app_secret, token_state).await;
        });
    }

    // Enqueue for processing.
    match &ctx.enqueue {
        Some(cb) => {
            cb(RawPayload::Json(raw));
            tracing::info!("Feishu message enqueued: {message_id}");
        }
        None => {
            // falls back to dispatching through the main event loop; the
            // Rust worker pipeline always installs an enqueue callback, so
            // without one there is no `&mut self` to dispatch through.
            tracing::error!("Feishu: no enqueue callback, message DROPPED: {message_id}");
        }
    }
}

/// Add an emoji reaction to a message via API (fire-and-forget).
///
/// Token from the shared refresh path, POSTed over a fresh client.
async fn add_reaction(
    message_id: String,
    emoji_type: String,
    http: reqwest::Client,
    app_id: String,
    app_secret: String,
    token_state: Arc<AsyncMutex<TokenState>>,
) {
    let token = match refresh_token_core(&app_id, &app_secret, &http, &token_state).await {
        Ok(t) => t,
        Err(_) => return,
    };
    let url = format!("{}/im/v1/messages/{}/reactions", API_BASE, message_id);
    let payload = json!({ "reaction_type": { "emoji_type": emoji_type } });
    let fresh = reqwest::Client::new();
    match fresh
        .post(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                tracing::debug!(
                    "Feishu reaction added: {} on {}",
                    emoji_type,
                    short(&message_id, 16)
                );
            } else {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::debug!(
                    "Feishu reaction failed: {} {}",
                    status,
                    short(&body, 100)
                );
            }
        }
        Err(e) => {
            tracing::debug!("Feishu _add_reaction error for {}: {e}", short(&message_id, 16));
        }
    }
}

// ---------------------------------------------------------------------------
// Pure helpers (parsing / content construction / mention handling)
// ---------------------------------------------------------------------------

mod helpers {
    use super::*;

    /// Build the Feishu `post` content structure with a markdown tag.
    pub fn build_post_content(text: &str) -> Value {
        // Normalize: ensure a newline before code fences for proper rendering.
        let normalized = if text.is_empty() {
            String::new()
        } else {
            match regex::Regex::new(r"([^\n])(```)") {
                Ok(re) => re.replace_all(text, "${1}\n${2}").to_string(),
                Err(_) => text.to_string(),
            }
        };

        let rows = if !normalized.is_empty() {
            json!([[{ "tag": "md", "text": normalized }]])
        } else {
            json!([[{ "tag": "md", "text": "[empty]" }]])
        };
        json!({ "zh_cn": { "content": rows } })
    }

    /// Build the authenticated download URL for a resource in a message.
    pub fn build_resource_url(message_id: &str, resource_key: &str, resource_type: &str) -> String {
        format!(
            "{}/im/v1/messages/{}/resources/{}?type={}",
            API_BASE,
            urlencoding::encode(message_id),
            urlencoding::encode(resource_key),
            resource_type
        )
    }

    /// Map filename/mime to a Feishu `file_type` enum value.
    pub fn guess_feishu_file_type(filename: &str) -> String {
        let lower = filename.to_lowercase();
        if lower.ends_with(".opus") || lower.ends_with(".ogg") {
            "opus".to_string()
        } else if [".mp4", ".avi", ".mov", ".mkv"]
            .iter()
            .any(|e| lower.ends_with(e))
        {
            "mp4".to_string()
        } else if lower.ends_with(".pdf") {
            "pdf".to_string()
        } else if lower.ends_with(".doc") || lower.ends_with(".docx") {
            "doc".to_string()
        } else if lower.ends_with(".xls") || lower.ends_with(".xlsx") {
            "xls".to_string()
        } else if lower.ends_with(".ppt") || lower.ends_with(".pptx") {
            "ppt".to_string()
        } else {
            "stream".to_string()
        }
    }

    /// Convert Feishu mention objects into plain `{key, open_id, name}` dicts.
    pub fn normalize_mentions(raw: Option<&Value>) -> Vec<Value> {
        let items = match raw.and_then(|r| r.as_array()) {
            Some(a) if !a.is_empty() => a,
            _ => return Vec::new(),
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if let Some(obj) = item.as_object() {
                let mut open_id = obj
                    .get("open_id")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if open_id.is_empty() {
                    if let Some(id_obj) = obj.get("id").and_then(|x| x.as_object()) {
                        open_id = id_obj
                            .get("open_id")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                    }
                }
                out.push(json!({
                    "key": obj.get("key").and_then(|x| x.as_str()).unwrap_or(""),
                    "open_id": open_id,
                    "name": obj.get("name").and_then(|x| x.as_str()).unwrap_or(""),
                }));
            }
        }
        out
    }

    /// Whether this message @mentions the bot (group activation hint).
    pub fn is_bot_mentioned(chat_type: &str, mentions: &[Value], bot_open_id: Option<&str>) -> bool {
        if chat_type != "group" {
            return true;
        }
        if mentions.is_empty() {
            return false;
        }
        let bot_id = match bot_open_id {
            Some(b) if !b.is_empty() => b,
            // Identity unknown: any structured mention may be the bot.
            _ => return true,
        };
        mentions.iter().any(|m| {
            m.as_object()
                .and_then(|o| o.get("open_id"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                == bot_id
        })
    }

    /// Mention placeholder keys that refer to this bot.
    pub fn bot_mention_keys(mentions: &[Value], bot_open_id: Option<&str>) -> Vec<String> {
        let bot_id = match bot_open_id {
            Some(b) if !b.is_empty() => b,
            _ => return Vec::new(),
        };
        let mut keys = Vec::new();
        for m in mentions {
            if let Some(obj) = m.as_object() {
                if obj.get("open_id").and_then(|x| x.as_str()).unwrap_or("") != bot_id {
                    continue;
                }
                let key = obj
                    .get("key")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if !key.is_empty() {
                    keys.push(key);
                }
            }
        }
        keys
    }

    /// Remove Feishu `@_user_N` placeholders that refer to this bot.
    pub fn strip_bot_mentions_from_parts(
        parts: Vec<ContentPart>,
        bot_keys: &[String],
    ) -> Vec<ContentPart> {
        if bot_keys.is_empty() {
            return parts;
        }
        let re_multi = regex::Regex::new(r"[^\S\n]{2,}").unwrap();
        let re_before_nl = regex::Regex::new(r"[^\S\n]+\n").unwrap();
        let re_after_nl = regex::Regex::new(r"\n[^\S\n]+").unwrap();

        let mut updated = Vec::with_capacity(parts.len());
        for part in parts {
            match part {
                ContentPart::Text(tc) if !tc.text.is_empty() => {
                    let original = tc.text.clone();
                    let mut text = original.clone();
                    for key in bot_keys {
                        text = text.replace(key.as_str(), "");
                    }
                    // Collapse runs of spaces/tabs left by removal, keep \n.
                    text = re_multi.replace_all(&text, " ").to_string();
                    text = re_before_nl.replace_all(&text, "\n").to_string();
                    text = re_after_nl.replace_all(&text, "\n").to_string();
                    text = text
                        .trim_matches(|c| c == ' ' || c == '\t')
                        .to_string();
                    if text != original {
                        updated.push(ContentPart::Text(TextContent { text }));
                    } else {
                        updated.push(ContentPart::Text(tc));
                    }
                }
                other => updated.push(other),
            }
        }
        updated
    }

    /// Convert Feishu message content into a list of ContentPart objects.
    pub fn parse_content_parts(message_type: &str, content_data: &Value, message_id: &str) -> Vec<ContentPart> {
        let get_str = |k: &str| {
            content_data
                .get(k)
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string()
        };
        let mut parts: Vec<ContentPart> = Vec::new();

        match message_type {
            "text" => {
                let text = get_str("text");
                if !text.is_empty() {
                    parts.push(ContentPart::text(text));
                }
            }
            "image" => {
                let image_key = get_str("image_key");
                if !image_key.is_empty() && !message_id.is_empty() {
                    let url = build_resource_url(message_id, &image_key, "image");
                    parts.push(ContentPart::Image(ImageContent {
                        url,
                        alt_text: image_key,
                        ..Default::default()
                    }));
                }
            }
            "file" => {
                let file_key = get_str("file_key");
                let filename = get_str("file_name");
                if !file_key.is_empty() && !message_id.is_empty() {
                    let url = build_resource_url(message_id, &file_key, "file");
                    parts.push(ContentPart::File(FileContent {
                        url,
                        filename,
                        ..Default::default()
                    }));
                }
            }
            "audio" => {
                let file_key = get_str("file_key");
                if !file_key.is_empty() && !message_id.is_empty() {
                    let url = build_resource_url(message_id, &file_key, "file");
                    parts.push(ContentPart::Audio(AudioContent {
                        url,
                        ..Default::default()
                    }));
                }
            }
            "sticker" => {
                // Feishu does not allow downloading sticker bytes via the
                // resource API; keep a visible marker (and the file_key).
                let file_key = get_str("file_key");
                let label = if !file_key.is_empty() {
                    format!("[Sticker: {file_key}]")
                } else {
                    "[Sticker]".to_string()
                };
                parts.push(ContentPart::text(label));
            }
            "post" => {
                let (text, media) = extract_post_parts(content_data, message_id);
                if !text.is_empty() {
                    parts.push(ContentPart::text(text));
                }
                parts.extend(media);
            }
            "interactive" => {
                let t = content_data.get("text").and_then(|x| x.as_str()).unwrap_or("");
                let text = if t.is_empty() {
                    "[Interactive Card]".to_string()
                } else {
                    t.to_string()
                };
                parts.push(ContentPart::text(text));
            }
            other => {
                parts.push(ContentPart::text(format!(
                    "[Unsupported message type: {other}]"
                )));
            }
        }

        if parts.is_empty() {
            parts.push(ContentPart::text(""));
        }
        parts
    }

    /// Extract text and media from a Feishu post (rich text) message.
    pub fn extract_post_parts(content_data: &Value, message_id: &str) -> (String, Vec<ContentPart>) {
        let mut lines: Vec<String> = Vec::new();
        let mut media: Vec<ContentPart> = Vec::new();
        let title = content_data
            .get("title")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if !title.is_empty() {
            lines.push(title.to_string());
        }

        // Some Feishu post payloads nest under a language key (zh_cn / en_us).
        let mut paragraphs: Vec<Value> = content_data
            .get("content")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        if paragraphs.is_empty() {
            if let Some(obj) = content_data.as_object() {
                for value in obj.values() {
                    if let Some(vobj) = value.as_object() {
                        if let Some(arr) = vobj.get("content").and_then(|x| x.as_array()) {
                            paragraphs = arr.clone();
                            if title.is_empty() {
                                if let Some(t) = vobj.get("title").and_then(|x| x.as_str()) {
                                    lines.push(t.to_string());
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        for paragraph in &paragraphs {
            let elems = match paragraph.as_array() {
                Some(a) => a,
                None => continue,
            };
            let mut line_parts: Vec<String> = Vec::new();
            for element in elems {
                let obj = match element.as_object() {
                    Some(o) => o,
                    None => continue,
                };
                let tag = obj.get("tag").and_then(|x| x.as_str()).unwrap_or("");
                match tag {
                    "text" | "md" => line_parts.push(
                        obj.get("text")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                    ),
                    "a" => {
                        let href = obj.get("href").and_then(|x| x.as_str()).unwrap_or("");
                        let text = obj.get("text").and_then(|x| x.as_str()).unwrap_or(href);
                        if !href.is_empty() {
                            line_parts.push(format!("{text}({href})"));
                        } else {
                            line_parts.push(text.to_string());
                        }
                    }
                    "at" => {
                        let name = obj
                            .get("user_name")
                            .and_then(|x| x.as_str())
                            .or_else(|| obj.get("user_id").and_then(|x| x.as_str()))
                            .unwrap_or("");
                        line_parts.push(format!("@{name}"));
                    }
                    "emotion" => {
                        let emoji = obj
                            .get("emoji_type")
                            .and_then(|x| x.as_str())
                            .or_else(|| obj.get("emoji").and_then(|x| x.as_str()))
                            .unwrap_or("emoji");
                        line_parts.push(format!("[{emoji}]"));
                    }
                    "img" => {
                        let image_key = obj
                            .get("image_key")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        if !image_key.is_empty() && !message_id.is_empty() {
                            let url = build_resource_url(message_id, &image_key, "image");
                            media.push(ContentPart::Image(ImageContent {
                                url,
                                alt_text: image_key,
                                ..Default::default()
                            }));
                        } else if !image_key.is_empty() {
                            line_parts.push(format!("[Image: {image_key}]"));
                        }
                    }
                    "media" => {
                        let file_key = obj
                            .get("file_key")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        if !file_key.is_empty() && !message_id.is_empty() {
                            let url = build_resource_url(message_id, &file_key, "file");
                            let filename = obj
                                .get("file_name")
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .to_string();
                            media.push(ContentPart::File(FileContent {
                                url,
                                filename,
                                ..Default::default()
                            }));
                        } else if !file_key.is_empty() {
                            line_parts.push(format!("[File: {file_key}]"));
                        }
                    }
                    "code_block" => {
                        let language = obj.get("language").and_then(|x| x.as_str()).unwrap_or("");
                        let code = obj.get("text").and_then(|x| x.as_str()).unwrap_or("");
                        let fence = if language.is_empty() {
                            format!("```\n{code}\n```")
                        } else {
                            format!("```{language}\n{code}\n```")
                        };
                        line_parts.push(fence);
                    }
                    _ => {}
                }
            }
            if !line_parts.is_empty() {
                lines.push(line_parts.concat());
            }
        }

        (lines.join("\n"), media)
    }

    /// Determine the `receive_id_type` based on target handle and chat type.
    pub fn resolve_receive_id_type(to_handle: &str, chat_type: &str) -> String {
        if chat_type == "group" {
            return "chat_id".to_string();
        }
        // Feishu chat_ids start with "oc_", open_ids with "ou_".
        if to_handle.starts_with("oc_") {
            return "chat_id".to_string();
        }
        "open_id".to_string()
    }

    /// Build the normalized message payload from a raw `im.message.receive_v1`
    /// event. Returns `None` for bot messages / malformed events.
    pub fn build_message_payload(v: &Value) -> Option<(String, Value)> {
        let event = v.get("event")?;
        let message = event.get("message")?;
        let sender = event.get("sender")?;

        let sender_type = sender
            .get("sender_type")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        // Skip bot messages.
        if sender_type == "bot" {
            return None;
        }

        let message_id = message
            .get("message_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let sender_id = sender
            .pointer("/sender_id/open_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let sender_name = sender
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let mentions = normalize_mentions(message.get("mentions"));
        let create_time = v
            .pointer("/header/create_time")
            .cloned()
            .unwrap_or_else(|| json!(""));

        let raw = json!({
            "message_id": message_id.clone(),
            "message_type": message.get("message_type").cloned().unwrap_or_else(|| json!("")),
            "content": message.get("content").cloned().unwrap_or_else(|| json!("")),
            "chat_id": message.get("chat_id").cloned().unwrap_or_else(|| json!("")),
            "chat_type": message.get("chat_type").cloned().unwrap_or_else(|| json!("")),
            "thread_id": message.get("thread_id").cloned().unwrap_or_else(|| json!("")),
            "parent_id": message.get("parent_id").cloned().unwrap_or_else(|| json!("")),
            "mentions": mentions,
            "sender": {
                "sender_id": sender_id,
                "sender_type": sender_type,
                "sender_name": sender_name,
            },
            "create_time": create_time,
        });
        Some((message_id, raw))
    }

    /// Normalize `parse_inbound` input: pass through the already-normalized
    /// payload, or unwrap a raw event wrapper into its normalized form.
    pub fn normalize_event_payload(v: &Value) -> Value {
        if v.get("message_id").is_some() || v.get("event").is_none() {
            return v.clone();
        }
        build_message_payload(v)
            .map(|(_, raw)| raw)
            .unwrap_or_else(|| v.clone())
    }
}
