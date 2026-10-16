//! Xiaoyi (小艺 / 华为 OpenClaw) channel: A2A protocol over dual WebSocket links.
//!
//! Mirrors `octop_gateway.channels.xiaoyi`. Two WebSocket links (primary +
//! backup) are opened concurrently with AK/SK request-signature headers. Inbound
//! `message/stream` JSON-RPC requests are turned into inbound payloads; outbound
//! turns are delivered as A2A `artifact-update` frames (`agent_response`) with a
//! final `status-update` frame.
//!
//! Submodules inlined from Python: `xiaoyi/auth.py`, `xiaoyi/constants.py`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;

use crate::channel::{Channel, ChannelConfig};
use crate::constraints::ChannelConstraints;
use crate::errors::{GatewayError, Result};
use crate::models::{ChannelSubject, ContentPart, InboundMessage, MessageEventType, RawPayload};

// ---------------------------------------------------------------------------
// constants.py
// ---------------------------------------------------------------------------

const DEFAULT_WS_URL: &str = "wss://hag.cloud.huawei.com/openclaw/v1/ws/link";
const DEFAULT_WS_URL_BACKUP: &str = "wss://116.63.174.231/openclaw/v1/ws/link";

const HEARTBEAT_INTERVAL: u64 = 30;
const RECONNECT_DELAYS: [u64; 6] = [1, 2, 5, 10, 30, 60];
const MAX_RECONNECT_ATTEMPTS: usize = 50;
#[allow(dead_code)]
const CONNECTION_TIMEOUT: u64 = 30;
const TEXT_CHUNK_LIMIT: usize = 4000;

// ---------------------------------------------------------------------------
// auth.py
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// `Base64(HMAC-SHA256(sk, timestamp))`.
pub fn generate_signature(sk: &str, timestamp: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(sk.as_bytes()).expect("hmac accepts any key length");
    mac.update(timestamp.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Build WebSocket authentication headers for XiaoYi.
pub fn generate_auth_headers(ak: &str, sk: &str, agent_id: &str) -> Vec<(String, String)> {
    let timestamp = format!("{}", now_millis());
    let signature = generate_signature(sk, &timestamp);
    vec![
        ("x-access-key".to_string(), ak.to_string()),
        ("x-sign".to_string(), signature),
        ("x-ts".to_string(), timestamp),
        ("x-agent-id".to_string(), agent_id.to_string()),
    ]
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "localhost".to_string())
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for XiaoYi (Huawei OpenClaw) channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct XiaoyiConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Access Key for WebSocket authentication.
    pub ak: String,
    /// Secret Key for WebSocket authentication.
    pub sk: String,
    /// Agent identifier registered on the XiaoYi platform.
    pub agent_id: String,
    /// Primary WebSocket endpoint.
    pub ws_url: String,
    /// Backup WebSocket endpoint (IP direct).
    pub ws_url_backup: String,
}

impl Default for XiaoyiConfig {
    fn default() -> Self {
        XiaoyiConfig {
            base: ChannelConfig::default(),
            ak: String::new(),
            sk: String::new(),
            agent_id: String::new(),
            ws_url: DEFAULT_WS_URL.to_string(),
            ws_url_backup: DEFAULT_WS_URL_BACKUP.to_string(),
        }
    }
}

crate::channel_adapter! {
    /// XiaoYi channel.
    XiaoyiChannel, XiaoyiConfig, "xiaoyi",
    required = [ak, sk, agent_id],
    fields = {
        primary_task: Option<JoinHandle<()>>,
        backup_task: Option<JoinHandle<()>>,
        shared: Arc<XiaoyiShared>,
    }
}

// ---------------------------------------------------------------------------
// Shared cross-task state
// ---------------------------------------------------------------------------

/// LRU-bounded dedup set of message ids (mirrors the Python `OrderedDict`).
#[derive(Default)]
struct Dedup {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl Dedup {
    fn is_duplicate(&mut self, msg_id: &str) -> bool {
        if msg_id.is_empty() {
            return false;
        }
        if self.set.contains(msg_id) {
            return true;
        }
        self.set.insert(msg_id.to_string());
        self.order.push_back(msg_id.to_string());
        while self.order.len() > 1000 {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        false
    }
}

/// State shared between the channel handle and the background connection tasks.
#[derive(Default)]
struct XiaoyiShared {
    stopping: AtomicBool,
    primary_alive: AtomicBool,
    backup_alive: AtomicBool,
    primary_tx: StdMutex<Option<UnboundedSender<Value>>>,
    backup_tx: StdMutex<Option<UnboundedSender<Value>>>,
    session_server_map: StdMutex<HashMap<String, String>>,
    session_task_map: StdMutex<HashMap<String, String>>,
    dedup: StdMutex<Dedup>,
    ak: String,
    sk: String,
    agent_id: String,
    enqueue: Option<crate::channel::EnqueueFn>,
}

fn any_alive(shared: &XiaoyiShared) -> bool {
    shared.primary_alive.load(Ordering::SeqCst) || shared.backup_alive.load(Ordering::SeqCst)
}

fn set_alive(shared: &XiaoyiShared, primary: bool, alive: bool) {
    if primary {
        shared.primary_alive.store(alive, Ordering::SeqCst);
    } else {
        shared.backup_alive.store(alive, Ordering::SeqCst);
    }
}

fn try_send_json(shared: &XiaoyiShared, primary: bool, msg: &Value) -> bool {
    let (alive, slot) = if primary {
        (&shared.primary_alive, &shared.primary_tx)
    } else {
        (&shared.backup_alive, &shared.backup_tx)
    };
    if !alive.load(Ordering::SeqCst) {
        return false;
    }
    let guard = slot.lock().unwrap();
    match guard.as_ref() {
        Some(tx) => tx.send(msg.clone()).is_ok(),
        None => false,
    }
}

/// Route a frame to the connection that owns the session, with failover.
async fn send_to_session(shared: &XiaoyiShared, session_id: &str, msg: Value) {
    let target = shared
        .session_server_map
        .lock()
        .unwrap()
        .get(session_id)
        .cloned()
        .unwrap_or_else(|| "primary".to_string());
    if target == "backup" {
        if try_send_json(shared, false, &msg) {
            return;
        }
        if try_send_json(shared, true, &msg) {
            return;
        }
    } else {
        if try_send_json(shared, true, &msg) {
            return;
        }
        if try_send_json(shared, false, &msg) {
            return;
        }
    }
    tracing::warn!("Xiaoyi: no connection to send session={}", session_id);
}

// ---------------------------------------------------------------------------
// A2A outbound frame builders (shared variants so background tasks can reuse)
// ---------------------------------------------------------------------------

fn artifact_msg(
    shared: &XiaoyiShared,
    session_id: &str,
    task_id: &str,
    message_id: &str,
    parts: Vec<Value>,
    is_final: bool,
) -> Value {
    let artifact_id = format!("artifact_{}", &uuid::Uuid::new_v4().simple().to_string()[..16]);
    let json_rpc = json!({
        "jsonrpc": "2.0",
        "id": message_id,
        "result": {
            "taskId": task_id,
            "kind": "artifact-update",
            "append": true,
            "lastChunk": true,
            "final": is_final,
            "artifact": {"artifactId": artifact_id, "parts": parts},
        },
    });
    json!({
        "msgType": "agent_response",
        "agentId": shared.agent_id,
        "sessionId": session_id,
        "taskId": task_id,
        "msgDetail": json_rpc.to_string(),
    })
}

fn chunk_text(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= TEXT_CHUNK_LIMIT {
        return vec![text.to_string()];
    }
    chars
        .chunks(TEXT_CHUNK_LIMIT)
        .map(|c| c.iter().collect())
        .collect()
}

async fn send_text_chunk(
    shared: &XiaoyiShared,
    session_id: &str,
    task_id: &str,
    message_id: &str,
    text: &str,
) {
    if text.is_empty() || !any_alive(shared) {
        return;
    }
    for chunk in chunk_text(text) {
        let msg = artifact_msg(shared, session_id, task_id, message_id, vec![json!({"kind": "text", "text": chunk})], false);
        send_to_session(shared, session_id, msg).await;
    }
}

async fn send_reasoning_chunk(
    shared: &XiaoyiShared,
    session_id: &str,
    task_id: &str,
    message_id: &str,
    text: &str,
) {
    if text.is_empty() || !any_alive(shared) {
        return;
    }
    for chunk in chunk_text(text) {
        let msg = artifact_msg(
            shared,
            session_id,
            task_id,
            message_id,
            vec![json!({"kind": "reasoningText", "reasoningText": chunk})],
            false,
        );
        send_to_session(shared, session_id, msg).await;
    }
}

async fn send_media_artifact(
    shared: &XiaoyiShared,
    session_id: &str,
    task_id: &str,
    message_id: &str,
    media: &ContentPart,
) {
    let url = media.url().unwrap_or("").to_string();
    if url.is_empty() && (media.data().is_some() || media.local_path().is_some()) {
        let label = media.label_with_filename();
        send_text_chunk(shared, session_id, task_id, message_id, &format!("[{label} (upload failed)]")).await;
        return;
    }
    let artifact = match media {
        ContentPart::Image(_) => {
            json!({"kind": "file", "file": {"name": "image", "mimeType": "image/png", "uri": url}})
        }
        ContentPart::Video(_) => {
            json!({"kind": "file", "file": {"name": "video", "mimeType": "video/mp4", "uri": url}})
        }
        ContentPart::File(f) => {
            let name = if f.filename.is_empty() { "file".to_string() } else { f.filename.clone() };
            let mime = f
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string());
            json!({"kind": "file", "file": {"name": name, "mimeType": mime, "uri": url}})
        }
        _ => return,
    };
    let msg = artifact_msg(shared, session_id, task_id, message_id, vec![artifact], true);
    send_to_session(shared, session_id, msg).await;
}

async fn send_final_message(shared: &XiaoyiShared, session_id: &str, task_id: &str, message_id: &str) {
    if !any_alive(shared) || task_id.is_empty() {
        return;
    }
    let detail = json!({
        "jsonrpc": "2.0",
        "id": message_id,
        "result": {
            "taskId": task_id,
            "kind": "status-update",
            "final": false,
            "status": {
                "message": {"role": "agent", "parts": [{"kind": "text", "text": ""}]},
                "state": "completed",
            },
        },
    });
    let status_msg = json!({
        "msgType": "agent_response",
        "agentId": shared.agent_id,
        "sessionId": session_id,
        "taskId": task_id,
        "msgDetail": detail.to_string(),
    });
    send_to_session(shared, session_id, status_msg).await;
    let final_msg = artifact_msg(shared, session_id, task_id, message_id, vec![json!({"kind": "text", "text": ""})], true);
    send_to_session(shared, session_id, final_msg).await;
}

async fn send_clear_context_response(shared: &XiaoyiShared, request_id: &str, session_id: &str) {
    if !any_alive(shared) {
        return;
    }
    let detail = json!({"jsonrpc": "2.0", "id": request_id, "result": {"status": {"state": "cleared"}}});
    let msg = json!({
        "msgType": "agent_response",
        "agentId": shared.agent_id,
        "sessionId": session_id,
        "taskId": request_id,
        "msgDetail": detail.to_string(),
    });
    send_to_session(shared, session_id, msg).await;
}

async fn send_tasks_cancel_response(shared: &XiaoyiShared, request_id: &str, session_id: &str) {
    if !any_alive(shared) {
        return;
    }
    let detail = json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {"id": request_id, "status": {"state": "canceled"}},
    });
    let msg = json!({
        "msgType": "agent_response",
        "agentId": shared.agent_id,
        "sessionId": session_id,
        "taskId": request_id,
        "msgDetail": detail.to_string(),
    });
    send_to_session(shared, session_id, msg).await;
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

async fn connect_signed(
    url: &str,
    ak: &str,
    sk: &str,
    agent_id: &str,
) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let mut req = url
        .into_client_request()
        .map_err(|e| GatewayError::Other(format!("xiaoyi request build: {e}")))?;
    for (name, value) in generate_auth_headers(ak, sk, agent_id) {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| GatewayError::Other(format!("xiaoyi header name: {e}")))?;
        let header_value = HeaderValue::from_str(&value)
            .map_err(|e| GatewayError::Other(format!("xiaoyi header value: {e}")))?;
        req.headers_mut().insert(header_name, header_value);
    }
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| GatewayError::Other(format!("xiaoyi connect: {e}")))?;
    Ok(ws)
}

async fn run_session(
    shared: Arc<XiaoyiShared>,
    ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    server: &'static str,
) -> Result<()> {
    let (mut write, mut read) = ws.split();
    let (tx, mut rx) = unbounded_channel::<Value>();
    if server == "primary" {
        *shared.primary_tx.lock().unwrap() = Some(tx.clone());
    } else {
        *shared.backup_tx.lock().unwrap() = Some(tx.clone());
    }

    // Initial handshake frame.
    let init = json!({
        "msgType": "clawd_bot_init",
        "agentId": shared.agent_id,
        "msgDetail": json!({"agentId": shared.agent_id, "hostname": hostname()}).to_string(),
    });
    write
        .send(Message::text(init.to_string()))
        .await
        .map_err(|e| GatewayError::Other(e.to_string()))?;

    let mut hb = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL));
    hb.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(t))) => {
                        handle_text(&shared, server, t.as_str()).await;
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Ping(_)))
                    | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::warn!("Xiaoyi [{}]: ws error {}", server, e);
                        break;
                    }
                }
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(v) => {
                        if write.send(Message::text(v.to_string())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = hb.tick() => {
                let hb_msg = json!({
                    "msgType": "heartbeat",
                    "agentId": shared.agent_id,
                    "msgDetail": json!({"timestamp": now_millis()}).to_string(),
                });
                if write.send(Message::text(hb_msg.to_string())).await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn run_connection(shared: Arc<XiaoyiShared>, server: &'static str, url: String, primary: bool) {
    let mut attempt: usize = 0;
    loop {
        if shared.stopping.load(Ordering::SeqCst) {
            return;
        }
        match connect_signed(&url, &shared.ak, &shared.sk, &shared.agent_id).await {
            Ok(ws) => {
                attempt = 0;
                set_alive(&shared, primary, true);
                tracing::info!("Xiaoyi [{}]: connected to {}", server, url);
                if let Err(e) = run_session(shared.clone(), ws, server).await {
                    tracing::warn!("Xiaoyi [{}]: session ended: {}", server, e);
                }
                set_alive(&shared, primary, false);
                if primary {
                    *shared.primary_tx.lock().unwrap() = None;
                } else {
                    *shared.backup_tx.lock().unwrap() = None;
                }
                shared
                    .session_server_map
                    .lock()
                    .unwrap()
                    .retain(|_, srv| srv != server);
            }
            Err(e) => {
                set_alive(&shared, primary, false);
                tracing::warn!("Xiaoyi [{}]: connection error: {}", server, e);
            }
        }
        if shared.stopping.load(Ordering::SeqCst) {
            return;
        }
        if attempt >= MAX_RECONNECT_ATTEMPTS {
            tracing::warn!("Xiaoyi [{}]: max reconnect attempts reached", server);
            return;
        }
        let delay = RECONNECT_DELAYS[attempt.min(RECONNECT_DELAYS.len() - 1)];
        attempt += 1;
        sleep_or_stop(&shared, delay).await;
    }
}

async fn sleep_or_stop(shared: &XiaoyiShared, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if shared.stopping.load(Ordering::SeqCst) {
            return;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(250))).await;
    }
}

async fn handle_text(shared: &XiaoyiShared, server: &'static str, data: &str) {
    let message: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!("Xiaoyi [{}]: invalid JSON", server);
            return;
        }
    };
    handle_incoming_message(shared, server, message).await;
}

async fn handle_incoming_message(shared: &XiaoyiShared, server: &'static str, message: Value) {
    if shared.stopping.load(Ordering::SeqCst) {
        return;
    }
    if let Some(aid) = message.get("agentId").and_then(|v| v.as_str()) {
        if aid != shared.agent_id {
            return;
        }
    }

    let session_id = message
        .pointer("/params/sessionId")
        .and_then(|v| v.as_str())
        .or_else(|| message.get("sessionId").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    if let Some(sid) = &session_id {
        shared
            .session_server_map
            .lock()
            .unwrap()
            .insert(sid.clone(), server.to_string());
    }

    let method = message.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let action = message.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let request_id = message.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let session = session_id.clone().unwrap_or_default();

    if method == "clearContext" || action == "clear" {
        send_clear_context_response(shared, &request_id, &session).await;
        shared.session_task_map.lock().unwrap().remove(&session);
        return;
    }
    if method == "tasks/cancel" || action == "tasks/cancel" {
        send_tasks_cancel_response(shared, &request_id, &session).await;
        return;
    }
    if method == "message/stream" {
        handle_a2a_request(shared, message).await;
    }
}

async fn handle_a2a_request(shared: &XiaoyiShared, message: Value) {
    let session_id = message
        .pointer("/params/sessionId")
        .and_then(|v| v.as_str())
        .or_else(|| message.get("sessionId").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let session_id = match session_id {
        Some(s) if !s.is_empty() => s,
        _ => return,
    };
    let task_id = message
        .pointer("/params/id")
        .and_then(|v| v.as_str())
        .or_else(|| message.get("id").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    shared
        .session_task_map
        .lock()
        .unwrap()
        .insert(session_id.clone(), task_id.clone());

    let msg_id = message
        .get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    {
        let mut dedup = shared.dedup.lock().unwrap();
        if dedup.is_duplicate(&msg_id) {
            return;
        }
    }

    let mut text_parts: Vec<String> = Vec::new();
    let mut content_parts: Vec<Value> = Vec::new();
    if let Some(parts) = message
        .pointer("/params/message/parts")
        .and_then(|v| v.as_array())
    {
        for part in parts {
            let kind = part.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if kind == "text" {
                if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                    if !t.is_empty() {
                        text_parts.push(t.to_string());
                    }
                }
            } else if kind == "file" {
                let file_info = part.get("file").cloned().unwrap_or(Value::Null);
                let url = file_info.get("uri").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let filename = file_info
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("file")
                    .to_string();
                let mime = file_info
                    .get("mimeType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if !url.is_empty() {
                    let part_val = if mime.starts_with("image/") {
                        serde_json::to_value(ContentPart::Image(crate::models::ImageContent {
                            url,
                            alt_text: filename,
                            ..Default::default()
                        }))
                        .unwrap_or(Value::Null)
                    } else {
                        serde_json::to_value(ContentPart::File(crate::models::FileContent {
                            url,
                            filename,
                            ..Default::default()
                        }))
                        .unwrap_or(Value::Null)
                    };
                    content_parts.push(part_val);
                }
            }
        }
    }

    let text_content = text_parts.join(" ").trim().to_string();
    if !text_content.is_empty() {
        let text_val = serde_json::to_value(ContentPart::text(text_content)).unwrap_or(Value::Null);
        content_parts.insert(0, text_val);
    }
    if content_parts.is_empty() {
        return;
    }

    let native = json!({
        "session_id": session_id,
        "task_id": task_id,
        "message_id": msg_id,
        "content": content_parts,
    });
    if let Some(cb) = &shared.enqueue {
        cb(RawPayload::Json(native));
    }
}

// ---------------------------------------------------------------------------
// Channel impl
// ---------------------------------------------------------------------------

impl XiaoyiChannel {
    fn shared(&self) -> &Arc<XiaoyiShared> {
        &self.shared
    }

    /// Extract concatenated cleaned text from an event's text parts.
    fn extract_text_from_event(&self, event: &crate::models::MessageEvent) -> String {
        let mut parts = String::new();
        for part in &event.content {
            if let Some(t) = part.text_value() {
                if !t.is_empty() {
                    parts.push_str(&self.clean_output(t));
                }
            }
        }
        parts.trim().to_string()
    }

    async fn deliver_a2a_event(
        &self,
        event: &crate::models::MessageEvent,
        session_id: &str,
        task_id: &str,
        message_id: &str,
    ) {
        let shared = self.shared();
        match event.event_type {
            MessageEventType::Thinking | MessageEventType::ThinkingDelta => {
                if self.core.constraints().show_thinking {
                    for part in &event.content {
                        if let Some(t) = part.text_value() {
                            if !t.is_empty() {
                                send_reasoning_chunk(shared, session_id, task_id, message_id, t).await;
                            }
                        }
                    }
                }
            }
            MessageEventType::Message | MessageEventType::Delta | MessageEventType::Flush => {
                let text = self.extract_text_from_event(event);
                if !text.is_empty() {
                    send_text_chunk(shared, session_id, task_id, message_id, &text).await;
                }
            }
            MessageEventType::ToolStart => {
                if self.core.constraints().show_tool_hints {
                    let hint = crate::constraints::tool_hint_message(
                        &event.metadata,
                        self.core.constraints(),
                        "start",
                    );
                    send_text_chunk(shared, session_id, task_id, message_id, &format!("\n\n{hint}\n")).await;
                }
            }
            MessageEventType::ToolEnd => {
                if self.core.constraints().show_tool_hints {
                    let hint = crate::constraints::tool_hint_message(
                        &event.metadata,
                        self.core.constraints(),
                        "end",
                    );
                    send_text_chunk(shared, session_id, task_id, message_id, &format!("\n\n{hint}\n")).await;
                }
            }
            MessageEventType::Error => {
                let err = event
                    .error
                    .clone()
                    .unwrap_or_else(|| "An error occurred.".to_string());
                send_text_chunk(shared, session_id, task_id, message_id, &err).await;
            }
            MessageEventType::Completed => {
                for part in &event.content {
                    match part {
                        ContentPart::Image(_) | ContentPart::Video(_) | ContentPart::File(_) => {
                            send_media_artifact(shared, session_id, task_id, message_id, part).await;
                        }
                        ContentPart::Text(t) => {
                            if !t.text.is_empty() {
                                send_text_chunk(shared, session_id, task_id, message_id, &t.text).await;
                            }
                        }
                        _ => {}
                    }
                }
            }
            MessageEventType::Typing => {}
        }
    }
}

#[async_trait]
impl Channel for XiaoyiChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> ChannelConstraints {
        ChannelConstraints {
            send_rate_limit: Some((20, 60.0)),
            show_thinking: false,
            show_tool_hints: true,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        if self.config.ak.is_empty() || self.config.sk.is_empty() || self.config.agent_id.is_empty() {
            return Err(GatewayError::Other(
                "XiaoyiChannel: ak, sk, and agent_id are required".to_string(),
            ));
        }

        // Tear down any previous links.
        if let Some(t) = self.primary_task.take() {
            t.abort();
        }
        if let Some(t) = self.backup_task.take() {
            t.abort();
        }

        self.core
            .set_connection_session(Some(format!("xiaoyi-{}", self.config.agent_id)));

        let shared = Arc::new(XiaoyiShared {
            ak: self.config.ak.clone(),
            sk: self.config.sk.clone(),
            agent_id: self.config.agent_id.clone(),
            enqueue: self.core.enqueue_sender(),
            ..Default::default()
        });
        self.shared = shared.clone();

        let primary = tokio::spawn(run_connection(
            shared.clone(),
            "primary",
            self.config.ws_url.clone(),
            true,
        ));
        self.primary_task = Some(primary);

        if !self.config.ws_url_backup.is_empty() {
            let backup = tokio::spawn(run_connection(
                shared,
                "backup",
                self.config.ws_url_backup.clone(),
                false,
            ));
            self.backup_task = Some(backup);
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.shared.stopping.store(true, Ordering::SeqCst);
        self.shared.primary_alive.store(false, Ordering::SeqCst);
        self.shared.backup_alive.store(false, Ordering::SeqCst);
        if let Some(t) = self.primary_task.take() {
            t.abort();
        }
        if let Some(t) = self.backup_task.take() {
            t.abort();
        }
        self.shared.session_server_map.lock().unwrap().clear();
        self.shared.session_task_map.lock().unwrap().clear();
        self.core.set_connection_session(None);
        tracing::info!("XiaoyiChannel stopped");
        Ok(())
    }

    /// A2A artifact streaming replaces the buffered default pipeline.
    async fn process_inbound(&mut self, message: &InboundMessage, subject: &ChannelSubject) -> bool {
        let subject_id = subject.subject_id.clone();
        let session_id = message
            .metadata
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(subject_id);
        let task_id = message
            .metadata
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let message_id = message
            .metadata
            .get("message_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let mut processing_succeeded = false;
        match self.core.processor() {
            None => {
                tracing::warn!("Xiaoyi processing error: no processor set session={}", session_id);
                send_text_chunk(self.shared(), &session_id, &task_id, &message_id, "An error occurred.").await;
            }
            Some(processor) => {
                let mut stream = processor(message.clone());
                while let Some(event) = stream.next().await {
                    let is_completed = event.event_type == MessageEventType::Completed;
                    self.deliver_a2a_event(&event, &session_id, &task_id, &message_id)
                        .await;
                    if is_completed {
                        processing_succeeded = true;
                        break;
                    }
                }
            }
        }
        send_final_message(self.shared(), &session_id, &task_id, &message_id).await;
        processing_succeeded
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let session_id = subject
            .metadata
            .get("session_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone());
        let task_id = subject
            .metadata
            .get("task_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                self.shared()
                    .session_task_map
                    .lock()
                    .unwrap()
                    .get(&session_id)
                    .cloned()
                    .unwrap_or_default()
            });
        let message_id = subject
            .metadata
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        send_text_chunk(self.shared(), &session_id, &task_id, &message_id, text).await;
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let session_id = subject
            .metadata
            .get("session_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone());
        let task_id = subject
            .metadata
            .get("task_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                self.shared()
                    .session_task_map
                    .lock()
                    .unwrap()
                    .get(&session_id)
                    .cloned()
                    .unwrap_or_default()
            });
        let message_id = subject
            .metadata
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        send_media_artifact(self.shared(), &session_id, &task_id, &message_id, media).await;
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let data = match raw {
            RawPayload::Json(v) => v.clone(),
            RawPayload::Text(s) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
            RawPayload::Preparsed(m) => return Ok(m.clone()),
        };
        let obj = if data.is_object() { data } else { Value::Null };

        let session_id = obj
            .get("session_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown")
            .to_string();
        let task_id = obj
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let message_id = obj
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut content: Vec<ContentPart> = Vec::new();
        if let Some(arr) = obj.get("content").and_then(|v| v.as_array()) {
            for item in arr {
                if let Ok(part) = serde_json::from_value::<ContentPart>(item.clone()) {
                    content.push(part);
                }
            }
        }
        if content.is_empty() {
            content.push(ContentPart::text(""));
        }

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("session_id".to_string(), json!(session_id));
        metadata.insert("task_id".to_string(), json!(task_id));
        metadata.insert("message_id".to_string(), json!(message_id));

        let channel_session_id = self
            .core
            .connection_session()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}-unconnected", self.core.channel_id()));

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: self.core.channel_type().to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id: session_id.clone(),
                first_seen: None,
                last_seen: None,
                display_name: String::new(),
                chat_type: "direct".to_string(),
                metadata: metadata.clone(),
            }),
            channel_session_id,
            content,
            group_context: None,
            metadata,
            timestamp: crate::channel::now_f64(),
        })
    }
}
