//! Slack channel — Socket Mode inbound + Web API outbound.
//!
//! Inbound uses **Socket Mode**, so the bot needs no public URL:
//!
//! 1. `apps.connections.open` (app-level `xapp-` token) returns a short-lived
//!    `wss://` URL.
//! 2. A single WebSocket carries every event as an *envelope*. Each envelope
//!    must be acknowledged within 3 seconds, so the ack is sent before the
//!    payload is enqueued.
//! 3. `disconnect` envelopes (and socket errors) trigger a refresh of the URL
//!    and a reconnect with exponential backoff.
//!
//! Outbound uses the Web API with the bot (`xoxb-`) token:
//! `chat.postMessage` for text, `files.getUploadURLExternal` +
//! `files.completeUploadExternal` for media.
//!
//! Note that Slack answers with HTTP 200 even when the call failed — the
//! `ok` field carries the real result, so every response is inspected.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::channel::{Channel, ChannelConfig, ChannelCore, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, VideoContent,
};

const SLACK_API: &str = "https://slack.com/api";
/// Slack's practical message length; `chat.postMessage` accepts 40k but clients
/// render long blocks poorly.
const TEXT_CHUNK: usize = 4000;
/// Cap on `url_private` downloads.
const MAX_MEDIA_BYTES: usize = 25 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Slack channel configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SlackConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Bot token (`xoxb-...`) used for Web API calls.
    pub bot_token: String,
    /// App-level token (`xapp-...`) used for Socket Mode.
    pub app_token: String,
    /// Signing secret, kept for HTTP-mode deployments and signature checks.
    pub signing_secret: String,
    /// Optional HTTP proxy for REST calls.
    pub http_proxy: String,
    /// Optional `user:pass` for the proxy.
    pub http_proxy_auth: String,
    /// When false (default) only conversations listed in
    /// `allowed_channel_ids` are served.
    pub allow_all_channels: bool,
    /// Conversation ids to serve (`C...`, `D...`, `G...`).
    #[serde(default, deserialize_with = "crate::channels::string_or_vec")]
    pub allowed_channel_ids: Vec<String>,
    /// Member ids allowed to talk to the bot.
    #[serde(default, deserialize_with = "crate::channels::string_or_vec")]
    pub allowed_user_ids: Vec<String>,
    /// Answer channel messages inside a thread instead of the main timeline.
    /// Defaults to `true` when omitted.
    pub thread_replies: Option<bool>,
}

impl SlackConfig {
    /// Thread replies default to on when the field is omitted.
    fn reply_in_thread(&self) -> bool {
        self.thread_replies.unwrap_or(true)
    }
}

crate::channel_adapter! {
    /// Slack channel adapter.
    SlackChannel, SlackConfig, "slack",
    required = [bot_token, app_token],
    fields = {
        task: Option<tokio::task::JoinHandle<()>>,
    }
}

// ---------------------------------------------------------------------------
// Shared state for the Socket Mode task
// ---------------------------------------------------------------------------

struct LruSet {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl LruSet {
    fn new(cap: usize) -> Self {
        LruSet {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    /// Returns true when `id` was not present (i.e. it is fresh).
    fn fresh(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

struct SlackShared {
    app_token: String,
    bot_user_id: String,
    bot_id: String,
    http: reqwest::Client,
    enqueue: EnqueueFn,
    allow_all_channels: bool,
    allowed_channels: Vec<String>,
    allowed_users: Vec<String>,
    proxy: String,
    proxy_auth: String,
    dedup: StdMutex<LruSet>,
}

// ---------------------------------------------------------------------------
// Channel implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for SlackChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            // Slack Web API tier 3 is 50+ calls/minute.
            send_rate_limit: Some((20, 60.0)),
            reply_timeout: 0.0,
            typing_keepalive_interval: 0.0,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        let http = self.build_http()?;
        let auth = auth_test(&http, &self.config.bot_token).await?;
        let bot_user_id = auth
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let bot_id = auth
            .get("bot_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        tracing::info!(
            "Slack authenticated: team={} user={}",
            auth.get("team").and_then(|v| v.as_str()).unwrap_or("-"),
            if bot_user_id.is_empty() { "-" } else { &bot_user_id }
        );

        let enqueue = self
            .core
            .enqueue_sender()
            .ok_or_else(|| GatewayError::Other("slack channel has no enqueue callback".into()))?;
        self.core.set_connection_session(Some(bot_user_id.clone()));

        let shared = Arc::new(SlackShared {
            app_token: self.config.app_token.clone(),
            bot_user_id,
            bot_id,
            http,
            enqueue,
            allow_all_channels: self.config.allow_all_channels,
            allowed_channels: self.config.allowed_channel_ids.clone(),
            allowed_users: self.config.allowed_user_ids.clone(),
            proxy: self.config.http_proxy.trim().to_string(),
            proxy_auth: self.config.http_proxy_auth.trim().to_string(),
            dedup: StdMutex::new(LruSet::new(2000)),
        });

        let task = tokio::spawn(run_socket_mode(shared));
        self.task = Some(task);
        tracing::info!("Slack Socket Mode connecting…");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(t) = self.task.take() {
            t.abort();
            tracing::info!("Slack Socket Mode stopped");
        }
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let channel = subject_channel(subject)?;
        let thread_ts = subject
            .metadata
            .get("thread_ts")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let http = self.build_http()?;
        for chunk in chunk_text(&markdown_to_mrkdwn(text), TEXT_CHUNK) {
            let mut payload = json!({
                "channel": channel,
                "text": chunk,
                "mrkdwn": true,
                "unfurl_links": false,
                "unfurl_media": false,
            });
            if !thread_ts.is_empty() {
                payload["thread_ts"] = Value::String(thread_ts.clone());
            }
            post_api(&http, &self.config.bot_token, "chat.postMessage", payload).await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let channel = subject_channel(subject)?;
        if media.is_text() {
            return Ok(());
        }
        let (bytes, mime) = match self.load_media_bytes(media).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("Slack media load failed: {}", e);
                let label = media.label_with_filename();
                return self
                    .send_text(subject, &format!("[{label}: upload failed]"))
                    .await;
            }
        };
        let filename = media
            .filename()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .unwrap_or_else(|| default_filename(&mime, media.label()));

        let http = self.build_http()?;
        let token = self.config.bot_token.clone();

        // 1. Reserve an upload URL.
        let ticket = post_api(
            &http,
            &token,
            "files.getUploadURLExternal",
            json!({ "filename": filename, "length": bytes.len() }),
        )
        .await?;
        let upload_url = ticket
            .get("upload_url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| GatewayError::Other("slack: missing upload_url".into()))?
            .to_string();
        let file_id = ticket
            .get("file_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| GatewayError::Other("slack: missing file_id".into()))?
            .to_string();

        // 2. Upload the bytes.
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(filename.clone())
            .mime_str(&mime)
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(Vec::new()));
        let form = reqwest::multipart::Form::new().part("file", part);
        let resp = http
            .post(&upload_url)
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "slack upload failed: {}",
                resp.status()
            )));
        }

        // 3. Share it into the conversation.
        let mut payload = json!({
            "files": [{ "id": file_id, "title": filename }],
            "channel_id": channel,
        });
        if let Some(ts) = subject.metadata.get("thread_ts").and_then(|v| v.as_str()) {
            if !ts.is_empty() {
                payload["thread_ts"] = Value::String(ts.to_string());
            }
        }
        post_api(&http, &token, "files.completeUploadExternal", payload).await?;
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("slack expects a JSON payload".into()))?;
        let channel = v.get("channel").and_then(|x| x.as_str()).unwrap_or_default();
        if channel.is_empty() {
            return Err(GatewayError::Other("slack payload without channel".into()));
        }
        let chat_type = v
            .get("channel_type")
            .and_then(|x| x.as_str())
            .unwrap_or("channel")
            .to_string();
        let ts = v
            .get("ts")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let thread_ts = v
            .get("thread_ts")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                // Top-level channel messages are answered in a fresh thread.
                if self.config.reply_in_thread()
                    && !chat_type.eq_ignore_ascii_case("im")
                    && !ts.is_empty()
                {
                    Some(ts.clone())
                } else {
                    None
                }
            });

        let raw_text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
        let bot_mention = format!("<@{}>", self.bot_user_id());
        let text = strip_mention(raw_text, &bot_mention);
        let display_name = v
            .get("conversation_name")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                if chat_type.eq_ignore_ascii_case("im") {
                    format!("DM {channel}")
                } else {
                    format!("#{channel}")
                }
            });

        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(ContentPart::text(text));
        }
        if let Some(files) = v.get("files").and_then(|f| f.as_array()) {
            for f in files {
                if let Some(part) = file_to_part(f) {
                    content.push(part);
                }
            }
        }

        let mut subject = ChannelSubject::new(channel);
        subject.display_name = display_name;
        subject.chat_type = chat_type.clone();
        if let Some(t) = &thread_ts {
            subject
                .metadata
                .insert("thread_ts".into(), Value::String(t.clone()));
        }
        subject
            .metadata
            .insert("to_handle".into(), Value::String(channel.to_string()));

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("message_id".into(), Value::String(ts.clone()));
        metadata.insert("slack_channel".into(), Value::String(channel.to_string()));
        metadata.insert("to_handle".into(), Value::String(channel.to_string()));
        metadata.insert("chat_type".into(), Value::String(chat_type));
        if let Some(t) = &thread_ts {
            metadata.insert("thread_ts".into(), Value::String(t.clone()));
            metadata.insert("reply_thread_ts".into(), Value::String(t.clone()));
        }
        if let Some(u) = v.get("user").and_then(|x| x.as_str()) {
            metadata.insert("sender_id".into(), Value::String(u.to_string()));
        }
        if let Some(u) = v.get("user_name").and_then(|x| x.as_str()) {
            metadata.insert("sender_name".into(), Value::String(u.to_string()));
        }
        if let Some(t) = v.get("team_id").and_then(|x| x.as_str()) {
            metadata.insert("team_id".into(), Value::String(t.to_string()));
        }
        if let Some(t) = v.get("event_id").and_then(|x| x.as_str()) {
            metadata.insert("event_id".into(), Value::String(t.to_string()));
        }
        if v.get("bot_mentioned").and_then(|x| x.as_bool()) == Some(true) {
            metadata.insert("bot_mentioned".into(), Value::Bool(true));
        }
        if let Some(sub) = v.get("subtype").and_then(|x| x.as_str()) {
            metadata.insert("slack_subtype".into(), Value::String(sub.to_string()));
        }

        let session = thread_ts.clone().unwrap_or_else(|| channel.to_string());

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "slack".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(subject),
            channel_session_id: session,
            content,
            group_context: None,
            metadata,
            timestamp: parse_slack_ts(&ts),
        })
    }

    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.build_http()?;
        let resp = http
            .get(url)
            .header("Authorization", format!("Bearer {}", self.config.bot_token))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "slack media fetch failed: {}",
                resp.status()
            )));
        }
        let mime = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let data = resp.bytes().await?;
        if data.len() > MAX_MEDIA_BYTES {
            return Err(GatewayError::Other(
                "slack media exceeds 25 MiB".to_string(),
            ));
        }
        Ok((data.to_vec(), mime))
    }

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        mut meta: std::collections::HashMap<String, Value>,
    ) -> std::collections::HashMap<String, Value> {
        meta.insert(
            "to_handle".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta.insert(
            "slack_channel".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta
    }
}

impl SlackChannel {
    /// Bot user id captured from `auth.test`.
    fn bot_user_id(&self) -> String {
        self.core.connection_session().unwrap_or("").to_string()
    }

    fn build_http(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder();
        let proxy = self.config.http_proxy.trim();
        if !proxy.is_empty() {
            let mut p = reqwest::Proxy::all(proxy)?;
            let auth = self.config.http_proxy_auth.trim();
            if !auth.is_empty() {
                let (user, pass) = auth.split_once(':').unwrap_or((auth, ""));
                p = p.basic_auth(user, pass);
            }
            builder = builder.proxy(p);
        }
        builder
            .build()
            .map_err(|e| GatewayError::Other(format!("slack http client: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Socket Mode loop
// ---------------------------------------------------------------------------

enum PumpEnd {
    Disconnect,
    Closed(String),
}

async fn run_socket_mode(shared: Arc<SlackShared>) {
    let mut backoff = 1.0f64;
    loop {
        let url = match open_connection(&shared).await {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!("Slack apps.connections.open failed: {}", e);
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(30.0);
                continue;
            }
        };
        tracing::info!("Slack gateway connecting: {}", redact_url(&url));

        let ws = if shared.proxy.is_empty() {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    tracing::warn!("Slack gateway connect failed: {}", e);
                    tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                    backoff = (backoff * 2.0).min(30.0);
                    continue;
                }
            }
        } else {
            let auth = if shared.proxy_auth.is_empty() {
                None
            } else {
                Some(shared.proxy_auth.as_str())
            };
            match crate::utils::ws_connect_proxied(&url, &shared.proxy, auth).await {
                Ok(ws) => ws,
                Err(e) => {
                    tracing::warn!("Slack gateway connect via proxy failed: {}", e);
                    tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                    backoff = (backoff * 2.0).min(30.0);
                    continue;
                }
            }
        };

        backoff = 1.0;
        tracing::info!("Slack gateway READY");
        match pump(shared.clone(), ws).await {
            PumpEnd::Disconnect => {
                tracing::info!("Slack requested a reconnect; refreshing connection URL");
            }
            PumpEnd::Closed(reason) => {
                tracing::warn!("Slack gateway closed: {}", reason);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn open_connection(shared: &SlackShared) -> std::result::Result<String, String> {
    let resp = shared
        .http
        .post(format!("{SLACK_API}/apps.connections.open"))
        .header("Authorization", format!("Bearer {}", shared.app_token))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
        return Err(format!(
            "{}",
            v.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown error")
        ));
    }
    v.get("url")
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| "response without url".to_string())
}

async fn pump(
    shared: Arc<SlackShared>,
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> PumpEnd {
    loop {
        let next = ws.next().await;
        match next {
            Some(Ok(WsMessage::Text(txt))) => {
                if let Some(end) = handle_frame(&shared, &mut ws, &txt).await {
                    return end;
                }
            }
            Some(Ok(WsMessage::Binary(bin))) => {
                if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                    if let Some(end) = handle_frame(&shared, &mut ws, &txt).await {
                        return end;
                    }
                }
            }
            Some(Ok(WsMessage::Ping(p))) => {
                let _ = ws.send(WsMessage::Pong(p)).await;
            }
            Some(Ok(WsMessage::Close(frame))) => {
                return PumpEnd::Closed(format!("{frame:?}"));
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => return PumpEnd::Closed(e.to_string()),
            None => return PumpEnd::Closed("stream ended".to_string()),
        }
    }
}

async fn handle_frame(
    shared: &Arc<SlackShared>,
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    text: &str,
) -> Option<PumpEnd> {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("Slack: non-JSON frame ({})", e);
            return None;
        }
    };
    let frame_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
    match frame_type {
        "hello" => {
            tracing::debug!(
                "Slack hello (connections={})",
                v.get("num_connections").and_then(|n| n.as_i64()).unwrap_or(1)
            );
            None
        }
        "disconnect" => Some(PumpEnd::Disconnect),
        "events_api" => {
            // Acknowledge first — Slack requires this within 3 seconds.
            if let Some(id) = v.get("envelope_id").and_then(|i| i.as_str()) {
                let ack = json!({ "envelope_id": id }).to_string();
                if let Err(e) = ws.send(WsMessage::Text(ack)).await {
                    tracing::warn!("Slack ack failed: {}", e);
                    return Some(PumpEnd::Closed(e.to_string()));
                }
            }
            if let Some(payload) = v.get("payload") {
                dispatch_event(shared, payload);
            }
            None
        }
        other => {
            // slash_commands / interactive keep their envelope open until the
            // app responds; we simply acknowledge so Slack stops retrying.
            if let Some(id) = v.get("envelope_id").and_then(|i| i.as_str()) {
                let ack = json!({ "envelope_id": id }).to_string();
                let _ = ws.send(WsMessage::Text(ack)).await;
            }
            tracing::debug!("Slack: ignored frame type {other}");
            None
        }
    }
}

fn dispatch_event(shared: &Arc<SlackShared>, payload: &Value) {
    let event = match payload.get("event") {
        Some(e) => e,
        None => return,
    };
    let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");

    // Only user messages start turns.
    if !matches!(event_type, "message" | "app_mention") {
        return;
    }
    let subtype = event.get("subtype").and_then(|s| s.as_str()).unwrap_or("");
    if matches!(
        subtype,
        "bot_message" | "message_changed" | "message_deleted" | "channel_join" | "channel_leave"
    ) {
        return;
    }
    if event.get("bot_id").and_then(|b| b.as_str()).is_some() {
        return;
    }
    let user = event.get("user").and_then(|u| u.as_str()).unwrap_or("");
    if !shared.bot_user_id.is_empty() && user == shared.bot_user_id {
        return;
    }
    if !shared.bot_id.is_empty()
        && event.get("bot_id").and_then(|b| b.as_str()) == Some(shared.bot_id.as_str())
    {
        return;
    }

    let channel = event.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    if channel.is_empty() {
        return;
    }
    if !shared.allow_all_channels && !shared.allowed_channels.is_empty() {
        if !shared.allowed_channels.iter().any(|c| c == channel) {
            tracing::debug!("Slack: ignoring conversation {}", channel);
            return;
        }
    }
    if !shared.allowed_users.is_empty() {
        if !shared.allowed_users.iter().any(|u| u == user) {
            tracing::debug!("Slack: ignoring user {}", user);
            return;
        }
    }

    let ts = event.get("ts").and_then(|t| t.as_str()).unwrap_or("");
    if ts.is_empty() {
        return;
    }
    {
        let mut dedup = match shared.dedup.lock() {
            Ok(d) => d,
            Err(p) => p.into_inner(),
        };
        if !dedup.fresh(ts) {
            return;
        }
    }

    let text = event.get("text").and_then(|t| t.as_str()).unwrap_or("");
    let mention_token = format!("<@{}>", shared.bot_user_id);
    let mentioned = event_type == "app_mention"
        || (!shared.bot_user_id.is_empty() && text.contains(&mention_token));

    let native = json!({
        "_slack": true,
        "team_id": payload.get("team_id").cloned().unwrap_or(Value::Null),
        "api_app_id": payload.get("api_app_id").cloned().unwrap_or(Value::Null),
        "event_id": payload.get("event_id").cloned().unwrap_or(Value::Null),
        "event_type": event_type,
        "subtype": if subtype.is_empty() { Value::Null } else { Value::String(subtype.to_string()) },
        "channel": channel,
        "channel_type": event.get("channel_type").cloned().unwrap_or(Value::String("channel".into())),
        "user": event.get("user").cloned().unwrap_or(Value::Null),
        "user_name": Value::Null,
        "text": text,
        "ts": ts,
        "thread_ts": event.get("thread_ts").cloned().unwrap_or(Value::Null),
        "bot_mentioned": mentioned,
        "files": normalize_files(event.get("files")),
    });

    (shared.enqueue)(RawPayload::Json(native));
}

/// Flatten Slack's file objects into the shape `parse_inbound` reads.
fn normalize_files(files: Option<&Value>) -> Value {
    let Some(items) = files.and_then(|f| f.as_array()) else {
        return Value::Array(Vec::new());
    };
    let out: Vec<Value> = items
        .iter()
        .filter_map(|f| {
            let url = f
                .get("url_private_download")
                .and_then(|u| u.as_str())
                .or_else(|| f.get("url_private").and_then(|u| u.as_str()))?;
            Some(json!({
                "id": f.get("id").cloned().unwrap_or(Value::Null),
                "name": f.get("name").cloned().unwrap_or(Value::Null),
                "mimetype": f.get("mimetype").cloned().unwrap_or(Value::Null),
                "filetype": f.get("filetype").cloned().unwrap_or(Value::Null),
                "size": f.get("size").cloned().unwrap_or(Value::Null),
                "url_private": url,
            }))
        })
        .collect();
    Value::Array(out)
}

// ---------------------------------------------------------------------------
// Web API helpers
// ---------------------------------------------------------------------------

async fn auth_test(http: &reqwest::Client, token: &str) -> Result<Value> {
    post_api(http, token, "auth.test", json!({})).await
}

async fn post_api(
    http: &reqwest::Client,
    token: &str,
    method: &str,
    payload: Value,
) -> Result<Value> {
    let resp = http
        .post(format!("{SLACK_API}/{method}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&payload)
        .send()
        .await?;
    let status = resp.status();
    let v: Value = resp
        .json()
        .await
        .map_err(|e| GatewayError::Other(format!("slack {method}: bad response ({status}): {e}")))?;
    if v.get("ok").and_then(|b| b.as_bool()) == Some(true) {
        return Ok(v);
    }
    Err(GatewayError::Other(format!(
        "slack {method} failed: {}",
        v.get("error")
            .and_then(|e| e.as_str())
            .unwrap_or(if status.is_success() { "unknown" } else { "http error" })
    )))
}

fn subject_channel(subject: &ChannelSubject) -> Result<String> {
    let channel = subject
        .metadata
        .get("to_handle")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(subject.subject_id.as_str());
    if channel.is_empty() {
        return Err(GatewayError::Other("slack: empty destination channel".into()));
    }
    Ok(channel.to_string())
}

fn redact_url(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _)) => format!("{base}?<redacted>"),
        None => url.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

fn file_to_part(f: &Value) -> Option<ContentPart> {
    let url = f
        .get("url_private")
        .and_then(|u| u.as_str())
        .filter(|s| !s.is_empty())?;
    let mime = f
        .get("mimetype")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let name = f
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    let size = f.get("size").and_then(|s| s.as_u64());
    let filetype = f
        .get("filetype")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();

    let kind = if mime.starts_with("image/") {
        "image"
    } else if mime.starts_with("video/") {
        "video"
    } else if mime.starts_with("audio/") {
        "audio"
    } else {
        match filetype.as_str() {
            "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" => "image",
            "mp4" | "mov" | "webm" => "video",
            "mp3" | "wav" | "ogg" | "m4a" => "audio",
            _ => "file",
        }
    };

    Some(match kind {
        "image" => ContentPart::Image(ImageContent {
            url: url.to_string(),
            alt_text: name.clone(),
            mime_type: non_empty(&mime),
            size,
            ..Default::default()
        }),
        "video" => ContentPart::Video(VideoContent {
            url: url.to_string(),
            mime_type: non_empty(&mime),
            size,
            ..Default::default()
        }),
        "audio" => ContentPart::Audio(AudioContent {
            url: url.to_string(),
            mime_type: non_empty(&mime),
            size,
            ..Default::default()
        }),
        _ => ContentPart::File(FileContent {
            url: url.to_string(),
            filename: name,
            mime_type: non_empty(&mime),
            size,
            ..Default::default()
        }),
    })
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn parse_slack_ts(ts: &str) -> f64 {
    ts.split_once('.')
        .map(|(s, _)| s)
        .unwrap_or(ts)
        .parse::<f64>()
        .unwrap_or(0.0)
}

fn strip_mention(text: &str, token: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let cleaned = text.replace(token, " ");
    let re = regex::Regex::new(r"<@[A-Z0-9]+>").unwrap();
    re.replace_all(&cleaned, " ").trim().to_string()
}

/// Split text on character boundaries, preferring paragraph/line breaks.
fn chunk_text(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= limit {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + limit).min(chars.len());
        let slice: String = chars[start..end].iter().collect();
        let cut = slice
            .rfind("\n\n")
            .or_else(|| slice.rfind('\n'))
            .or_else(|| slice.rfind(' '))
            .filter(|i| *i > limit / 3)
            .unwrap_or(slice.len());
        let piece: String = slice.chars().take(cut).collect();
        out.push(piece.trim_end().to_string());
        let consumed = slice[..cut].chars().count().max(1);
        start += consumed;
    }
    out.retain(|s| !s.is_empty());
    if out.is_empty() {
        out.push(text.to_string());
    }
    out
}

/// Convert the common Markdown a model emits into Slack `mrkdwn`.
///
/// Slack is *not* Markdown: `**bold**` must become `*bold*`, links use
/// `<url|label>`, and `&`/`<`/`>` must be entity-escaped outside code spans.
fn markdown_to_mrkdwn(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    // 1. Protect fenced blocks and inline code behind placeholders.
    let fence_re = regex::Regex::new(r"(?s)```.*?```").unwrap();
    let code_re = regex::Regex::new(r"`[^`\n]+`").unwrap();

    let mut protected: Vec<String> = Vec::new();
    let mut stage1 = String::new();
    let mut last = 0usize;
    for m in fence_re.find_iter(text) {
        stage1.push_str(&text[last..m.start()]);
        protected.push(m.as_str().to_string());
        stage1.push_str(&format!("\u{0}{}\u{0}", protected.len() - 1));
        last = m.end();
    }
    stage1.push_str(&text[last..]);

    let mut stage2 = String::new();
    let mut last = 0usize;
    for m in code_re.find_iter(&stage1) {
        stage2.push_str(&stage1[last..m.start()]);
        protected.push(m.as_str().to_string());
        stage2.push_str(&format!("\u{0}{}\u{0}", protected.len() - 1));
        last = m.end();
    }
    stage2.push_str(&stage1[last..]);

    // 2. Escape entities in the remaining prose.
    let escaped = stage2
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");

    // 3. Inline conversions.
    let bold = regex::Regex::new(r"\*\*([^*\n]+)\*\*").unwrap();
    let bold_u = regex::Regex::new(r"__([^_\n]+)__").unwrap();
    let strike = regex::Regex::new(r"~~([^~\n]+)~~").unwrap();
    let link = regex::Regex::new(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)").unwrap();
    let heading = regex::Regex::new(r"(?m)^#{1,6}\s*(.+)$").unwrap();

    let s = bold.replace_all(&escaped, "*$1*").to_string();
    let s = bold_u.replace_all(&s, "*$1*").to_string();
    let s = strike.replace_all(&s, "~$1~").to_string();
    let s = link.replace_all(&s, "<$2|$1>").to_string();
    let s = heading.replace_all(&s, "*$1*").to_string();

    // 4. Restore protected spans.
    let mut out = String::with_capacity(s.len());
    let parts: Vec<&str> = s.split('\u{0}').collect();
    for (i, part) in parts.iter().enumerate() {
        if i % 2 == 1 {
            if let Ok(idx) = part.parse::<usize>() {
                if let Some(original) = protected.get(idx) {
                    out.push_str(original);
                    continue;
                }
            }
        }
        out.push_str(part);
    }
    collapse_blank_lines(&out)
}

fn collapse_blank_lines(text: &str) -> String {
    let re = regex::Regex::new(r"\n{3,}").unwrap();
    re.replace_all(text, "\n\n").to_string()
}

fn default_filename(mime: &str, label: &str) -> String {
    let ext = mime_guess::get_mime_extensions_str(mime)
        .and_then(|exts| exts.first().copied())
        .unwrap_or("bin");
    format!("{}.{}", label.to_ascii_lowercase(), ext)
}
