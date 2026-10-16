//! LINE channel — Messaging API webhook inbound + reply/push outbound.
//!
//! LINE delivers events by POSTing to a public HTTPS endpoint the developer
//! registers in the LINE Developers console, so `start()` opens no connection
//! at all: it only validates the channel access token against `GET /v2/bot/info`
//! so `probe_channel` can surface bad credentials. Inbound events are verified
//! and translated by [`LineWebhook`] (mounted through
//! [`crate::webhook::WebhookServer`]); the resulting native payloads are turned
//! into [`crate::models::InboundMessage`]s by [`LineChannel::parse_inbound`].
//!
//! Outbound uses two endpoints:
//!
//! * `POST /v2/bot/message/reply` — the one-time `replyToken` returned with an
//!   inbound event. It is cheap and needs no push permission, but it can only be
//!   used once and expires quickly.
//! * `POST /v2/bot/message/push` — a normal bot-initiated push to a
//!   `userId`/`groupId`/`roomId`.
//!
//! The reply API is preferred when a token is available (`use_reply_token`,
//! default on) and the same text is retried through push when the reply call is
//! rejected (expired / already used token).
//!
//! LINE has no Markdown rendering and no upload endpoint, so text is flattened
//! to plain text and media can only be referenced by a public HTTPS URL.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::channel::{Channel, ChannelConfig, ChannelCore};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, VideoContent,
};
use crate::webhook::{WebhookAdapter, WebhookOutcome, WebhookRequest, WebhookResponse};

const LINE_API: &str = "https://api.line.me";
/// Host that serves message content (images/video/audio/files) by message id.
const LINE_DATA_API: &str = "https://api-data.line.me";
const BOT_INFO_PATH: &str = "/v2/bot/info";
const MESSAGE_REPLY_PATH: &str = "/v2/bot/message/reply";
const MESSAGE_PUSH_PATH: &str = "/v2/bot/message/push";
/// LINE's hard limit for one text message object.
const TEXT_CHUNK: usize = 5000;
/// Cap on `api-data.line.me` content downloads.
const MAX_MEDIA_BYTES: usize = 25 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// LINE channel configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LineConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Long-lived channel access token, used as the Messaging API bearer token.
    pub channel_access_token: String,
    /// Channel secret, used to verify the `X-Line-Signature` header.
    pub channel_secret: String,
    /// Optional HTTP proxy for REST calls.
    pub http_proxy: String,
    /// Optional `user:pass` for the proxy.
    pub http_proxy_auth: String,
    /// User ids allowed to talk to the bot (empty = allow all).
    #[serde(default, deserialize_with = "crate::channels::string_or_vec")]
    pub allowed_user_ids: Vec<String>,
    /// Group / room ids the bot serves (empty = allow all).
    #[serde(default, deserialize_with = "crate::channels::string_or_vec")]
    pub allowed_group_ids: Vec<String>,
    /// Prefer the one-time reply API and fall back to push. Defaults to `true`.
    pub use_reply_token: Option<bool>,
}

impl LineConfig {
    /// Reply tokens are preferred unless explicitly disabled.
    fn prefers_reply_token(&self) -> bool {
        self.use_reply_token.unwrap_or(true)
    }
}

crate::channel_adapter! {
    /// LINE Messaging API channel adapter.
    LineChannel, LineConfig, "line",
    required = [channel_access_token, channel_secret],
    fields = {}
}

// ---------------------------------------------------------------------------
// Dedup state (the adapter owns it: `parse_inbound` is immutable)
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

// ---------------------------------------------------------------------------
// Channel implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for LineChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            // LINE's Messaging API allows 120 requests/minute, but a lower,
            // safer ceiling keeps bursty agent output well clear of the limit.
            send_rate_limit: Some((20, 60.0)),
            reply_timeout: 0.0,
            typing_keepalive_interval: 0.0,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        // LINE pushes events to our webhook, so there is no connection to open;
        // validating the token here is what makes `probe_channel` useful.
        let http = self.build_http()?;
        let resp = http
            .get(format!("{LINE_API}{BOT_INFO_PATH}"))
            .header(
                "Authorization",
                format!("Bearer {}", self.config.channel_access_token),
            )
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!(
                "line bot info failed: {status} {body}"
            )));
        }
        let info: Value = resp.json().await?;
        let display_name = info.get("displayName").and_then(|v| v.as_str()).unwrap_or("-");
        let basic_id = info.get("basicId").and_then(|v| v.as_str()).unwrap_or("-");
        tracing::info!(
            "LINE authenticated: display_name={} basic_id={}",
            display_name,
            basic_id
        );
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        // Nothing to tear down: no long connection is owned by the channel.
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let plain = markdown_to_plain(text);
        if plain.trim().is_empty() {
            return Ok(());
        }
        let http = self.build_http()?;
        let target = subject_target(subject)?;
        let reply_token = self.reply_token_for(subject);

        // A reply token is single-use, so only the first chunk of a turn may
        // consume it; the rest always go out over push.
        let mut first = true;
        for chunk in chunk_text(&plain, TEXT_CHUNK) {
            let prefer_reply = first && reply_token.is_some();
            first = false;
            let messages = vec![text_message(&chunk)];
            self.deliver_messages(&http, &target, reply_token.as_deref(), prefer_reply, messages)
                .await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        if media.is_text() {
            return Ok(());
        }
        let Some(url) = public_https_url(media) else {
            // LINE only accepts publicly reachable HTTPS URLs for media: it has
            // no endpoint that ingests bytes and no base64 variant, so local
            // files and inline data cannot be delivered as media at all.
            let label = media.label_with_filename();
            return self
                .send_text(subject, &format!("[{label}: upload not supported]"))
                .await;
        };

        let message = match media {
            ContentPart::Image(_) => json!({
                "type": "image",
                // LINE mirrors this URL for both the original and the preview.
                "originalContentUrl": url,
                "previewImageUrl": url,
            }),
            ContentPart::Video(v) => {
                // A video message requires a preview image; fall back to the
                // video URL itself when no separate HTTPS thumbnail is present.
                let preview = v
                    .thumbnail_url
                    .as_deref()
                    .filter(|u| is_https(u))
                    .unwrap_or(url);
                json!({
                    "type": "video",
                    "originalContentUrl": url,
                    "previewImage": preview,
                })
            }
            ContentPart::Audio(a) => {
                // LINE wants the duration in milliseconds and rejects 0.
                let duration_ms = a.duration.unwrap_or(0).saturating_mul(1000);
                if duration_ms == 0 {
                    let label = media.label_with_filename();
                    return self
                        .send_text(subject, &format!("[{label}: upload not supported]"))
                        .await;
                }
                json!({
                    "type": "audio",
                    "originalContentUrl": url,
                    "duration": duration_ms,
                })
            }
            _ => {
                // LINE gained a bot-sent file message late; it is not available
                // on every plan, but it is the closest mapping for a `File`.
                let name = media.filename().filter(|n| !n.is_empty()).unwrap_or("file");
                let size = media.size().unwrap_or(0);
                json!({
                    "type": "file",
                    "originalContentUrl": url,
                    "fileName": name,
                    "fileSize": size,
                })
            }
        };

        let http = self.build_http()?;
        let target = subject_target(subject)?;
        let reply_token = self.reply_token_for(subject);
        let prefer_reply = reply_token.is_some();
        self.deliver_messages(&http, &target, reply_token.as_deref(), prefer_reply, vec![message])
            .await
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("line expects a JSON payload".into()))?;

        let source_type = v.get("source_type").and_then(Value::as_str).unwrap_or("");
        let user_id = str_field(v, "user_id");
        let group_id = str_field(v, "group_id");
        let room_id = str_field(v, "room_id");

        // The reply target is the user in 1:1 chats and the group/room otherwise.
        let subject_id = match source_type {
            "group" => group_id.clone(),
            "room" => room_id.clone(),
            _ => user_id.clone(),
        };
        if subject_id.is_empty() {
            return Err(GatewayError::Other(
                "line payload without a reply target".into(),
            ));
        }
        let chat_type = match source_type {
            "group" => "group",
            "room" => "room",
            _ => "user",
        }
        .to_string();

        let sender_name = str_field(v, "sender_name");
        let display_name = if !sender_name.is_empty() {
            sender_name.clone()
        } else if !user_id.is_empty() {
            user_id.clone()
        } else {
            subject_id.clone()
        };

        let text = str_field(v, "text");
        let message_type = str_field(v, "message_type");
        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(ContentPart::text(text));
        }
        if let Some(files) = v.get("files").and_then(Value::as_array) {
            for f in files {
                if let Some(part) = file_part(&message_type, f) {
                    content.push(part);
                }
            }
        }

        let mut subject = ChannelSubject::new(subject_id.clone());
        subject.display_name = display_name;
        subject.chat_type = chat_type.clone();
        subject
            .metadata
            .insert("to_handle".into(), Value::String(subject_id.clone()));
        subject
            .metadata
            .insert("line_source_id".into(), Value::String(subject_id.clone()));

        let mut metadata = HashMap::new();
        metadata.insert("to_handle".into(), Value::String(subject_id.clone()));
        metadata.insert("line_source_id".into(), Value::String(subject_id.clone()));
        metadata.insert("sender_id".into(), Value::String(user_id.clone()));
        metadata.insert("sender_name".into(), Value::String(sender_name));
        metadata.insert("message_type".into(), Value::String(message_type));
        metadata.insert(
            "reply_token".into(),
            Value::String(str_field(v, "reply_token")),
        );
        metadata.insert(
            "message_id".into(),
            Value::String(str_field(v, "message_id")),
        );
        if !user_id.is_empty() {
            metadata.insert("line_user_id".into(), Value::String(user_id));
        }
        if !group_id.is_empty() {
            metadata.insert("line_group_id".into(), Value::String(group_id));
        }
        if !room_id.is_empty() {
            metadata.insert("line_room_id".into(), Value::String(room_id));
        }
        if let Some(event_id) = v.get("event_id").and_then(Value::as_str) {
            if !event_id.is_empty() {
                metadata.insert("line_event_id".into(), Value::String(event_id.to_string()));
            }
        }
        metadata.insert("chat_type".into(), Value::String(chat_type));

        let timestamp = v.get("timestamp").and_then(Value::as_f64).unwrap_or(0.0) / 1000.0;

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "line".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(subject),
            channel_session_id: subject_id,
            content,
            group_context: None,
            metadata,
            timestamp,
        })
    }

    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        // LINE content (`api-data.line.me`) is authenticated with the same
        // bearer token; anything else is fetched anonymously.
        let http = self.build_http()?;
        let mut req = http.get(url);
        if is_line_host(url) {
            req = req.header(
                "Authorization",
                format!("Bearer {}", self.config.channel_access_token),
            );
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "line media fetch failed: {}",
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
                "line media exceeds 25 MiB".to_string(),
            ));
        }
        Ok((data.to_vec(), mime))
    }

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        mut meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        meta.insert(
            "to_handle".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta.insert(
            "line_source_id".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta
    }
}

impl LineChannel {
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
            .map_err(|e| GatewayError::Other(format!("line http client: {e}")))
    }

    /// The one-time reply token for this turn, when reply is enabled and one is
    /// available in the subject metadata.
    fn reply_token_for(&self, subject: &ChannelSubject) -> Option<String> {
        if !self.config.prefers_reply_token() {
            return None;
        }
        subject
            .metadata
            .get("reply_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    /// Post one batch of messages, preferring the reply API and falling back to
    /// push when the reply token is rejected (expired or already used).
    async fn deliver_messages(
        &self,
        http: &reqwest::Client,
        target: &str,
        reply_token: Option<&str>,
        prefer_reply: bool,
        messages: Vec<Value>,
    ) -> Result<()> {
        if prefer_reply {
            if let Some(token) = reply_token {
                let body = json!({ "replyToken": token, "messages": messages.clone() });
                match post_message(
                    http,
                    &self.config.channel_access_token,
                    MESSAGE_REPLY_PATH,
                    body,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        tracing::warn!("LINE reply token rejected, retrying via push: {e}");
                    }
                }
            }
        }
        let body = json!({ "to": target, "messages": messages });
        post_message(
            http,
            &self.config.channel_access_token,
            MESSAGE_PUSH_PATH,
            body,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Webhook adapter
// ---------------------------------------------------------------------------

/// Signature verification + event translation for LINE's webhook.
pub struct LineWebhook {
    config: LineConfig,
    /// LINE retries deliveries; dedup by `webhookEventId`.
    dedup: StdMutex<LruSet>,
}

impl LineWebhook {
    pub fn new(config: LineConfig) -> Self {
        LineWebhook {
            config,
            dedup: StdMutex::new(LruSet::new(2000)),
        }
    }

    fn user_allowed(&self, user_id: &str) -> bool {
        let allowed = &self.config.allowed_user_ids;
        allowed.is_empty() || (!user_id.is_empty() && allowed.iter().any(|id| id == user_id))
    }

    fn conversation_allowed(&self, source_type: &str, group_id: &str, room_id: &str) -> bool {
        // The group whitelist only constrains group/room chats; 1:1 chats are
        // governed by the user whitelist alone.
        if !matches!(source_type, "group" | "room") {
            return true;
        }
        let allowed = &self.config.allowed_group_ids;
        if allowed.is_empty() {
            return true;
        }
        let id = if source_type == "group" { group_id } else { room_id };
        !id.is_empty() && allowed.iter().any(|g| g == id)
    }

    /// Translate one LINE event into a native payload, or `None` when the event
    /// should be silently acknowledged (ignored type, filtered or duplicate).
    fn translate_event(&self, event: &Value) -> Option<RawPayload> {
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(event_type, "message" | "postback") {
            // follow / join / leave / unfollow / beacon / account link ... are
            // acknowledged without producing a turn.
            return None;
        }

        let event_id = event
            .get("webhookEventId")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !event_id.is_empty() {
            let mut dedup = match self.dedup.lock() {
                Ok(d) => d,
                Err(p) => p.into_inner(),
            };
            if !dedup.fresh(event_id) {
                tracing::debug!("LINE: dropping duplicate event {event_id}");
                return None;
            }
        }

        let source = event.get("source");
        let source_type = source
            .and_then(|s| s.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let user_id = source
            .and_then(|s| s.get("userId"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let group_id = source
            .and_then(|s| s.get("groupId"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let room_id = source
            .and_then(|s| s.get("roomId"))
            .and_then(Value::as_str)
            .unwrap_or("");

        // LINE does not include the bot's own user id in webhook events, so a
        // self-message cannot be recognised here; the whitelists are the gate.
        if !self.user_allowed(user_id) {
            tracing::debug!("LINE: ignoring user {user_id}");
            return None;
        }
        if !self.conversation_allowed(source_type, group_id, room_id) {
            tracing::debug!("LINE: ignoring conversation {group_id}{room_id}");
            return None;
        }

        let message = event.get("message");
        let (message_type, text, files) = if event_type == "message" {
            match build_message(message) {
                Some(x) => x,
                None => return None,
            }
        } else {
            // postback: surface the opaque `data` so the agent can react to it.
            let data = event
                .get("postback")
                .and_then(|p| p.get("data"))
                .and_then(Value::as_str)
                .unwrap_or("");
            (
                "postback".to_string(),
                format!("[Postback: {data}]"),
                Vec::new(),
            )
        };

        let payload = json!({
            "_line": true,
            "event_id": event_id,
            "event_type": event_type,
            "source_type": source_type,
            "user_id": user_id,
            "group_id": group_id,
            "room_id": room_id,
            "reply_token": event.get("replyToken").cloned().unwrap_or(Value::Null),
            "message_id": message.and_then(|m| m.get("id")).cloned().unwrap_or(Value::Null),
            "message_type": message_type,
            "text": text,
            "timestamp": event.get("timestamp").cloned().unwrap_or(Value::Null),
            "files": files,
        });
        Some(RawPayload::Json(payload))
    }
}

impl WebhookAdapter for LineWebhook {
    fn channel_type(&self) -> &'static str {
        "line"
    }

    fn verify_challenge(&self, _req: &WebhookRequest) -> Option<WebhookResponse> {
        // LINE has no GET handshake; it verifies the endpoint out of band.
        None
    }

    fn handle_request(&self, req: &WebhookRequest) -> Result<WebhookOutcome> {
        let signature = req.header("x-line-signature").ok_or_else(|| {
            GatewayError::Other("line: missing X-Line-Signature header".into())
        })?;
        if !crate::webhook::verify_sha256_base64_signature(
            &self.config.channel_secret,
            &req.body,
            signature,
        ) {
            return Err(GatewayError::Other("line: invalid signature".into()));
        }

        let body = req.body_json()?;
        let mut payloads = Vec::new();
        if let Some(events) = body.get("events").and_then(Value::as_array) {
            for event in events {
                if let Some(payload) = self.translate_event(event) {
                    payloads.push(payload);
                }
            }
        }

        // LINE always expects a 200 with this exact body.
        Ok(WebhookOutcome::with_response(
            WebhookResponse::json(&json!({ "status": "ok" })),
            payloads,
        ))
    }
}

// ---------------------------------------------------------------------------
// Outbound helpers
// ---------------------------------------------------------------------------

async fn post_message(
    http: &reqwest::Client,
    token: &str,
    path: &str,
    payload: Value,
) -> Result<()> {
    let resp = http
        .post(format!("{LINE_API}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&payload)
        .send()
        .await?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    Err(GatewayError::Other(format!(
        "line {path} failed: {status} {body}"
    )))
}

fn text_message(text: &str) -> Value {
    json!({ "type": "text", "text": text })
}

fn subject_target(subject: &ChannelSubject) -> Result<String> {
    let target = subject
        .metadata
        .get("to_handle")
        .and_then(Value::as_str)
        .or_else(|| subject.metadata.get("line_source_id").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .unwrap_or(subject.subject_id.as_str());
    if target.is_empty() {
        return Err(GatewayError::Other("line: empty destination".into()));
    }
    Ok(target.to_string())
}

// ---------------------------------------------------------------------------
// Inbound helpers
// ---------------------------------------------------------------------------

fn str_field(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Translate a LINE `message` object into `(message_type, text, files)`.
///
/// Returns `None` for message types the gateway does not surface.
fn build_message(message: Option<&Value>) -> Option<(String, String, Vec<Value>)> {
    let message = message?;
    let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
    match message_type {
        "text" => Some((
            "text".to_string(),
            message
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            Vec::new(),
        )),
        "image" | "video" | "audio" | "file" => {
            // Only the content id is needed; the bytes are fetched later from
            // `api-data.line.me` by `fetch_remote_media`.
            let mut files = Vec::new();
            if let Some(id) = message.get("id").and_then(Value::as_str) {
                files.push(json!({
                    "id": id,
                    "name": message.get("fileName").cloned().unwrap_or(Value::Null),
                    "size": message.get("fileSize").cloned().unwrap_or(Value::Null),
                }));
            }
            Some((message_type.to_string(), String::new(), files))
        }
        "sticker" => {
            let keywords = message
                .get("keywords")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let text = if keywords.is_empty() {
                "[Sticker]".to_string()
            } else {
                format!("[Sticker: {keywords}]")
            };
            Some(("sticker".to_string(), text, Vec::new()))
        }
        "location" => {
            let title = message.get("title").and_then(Value::as_str).unwrap_or("");
            let address = message.get("address").and_then(Value::as_str).unwrap_or("");
            let detail = [title, address]
                .iter()
                .filter(|s| !s.is_empty())
                .copied()
                .collect::<Vec<_>>()
                .join(" ");
            let coords = match (
                message.get("latitude").and_then(Value::as_f64),
                message.get("longitude").and_then(Value::as_f64),
            ) {
                (Some(lat), Some(lon)) => format!(" (lat={lat}, lon={lon})"),
                _ => String::new(),
            };
            Some(("location".to_string(), format!("[Location: {detail}{coords}]"), Vec::new()))
        }
        _ => None,
    }
}

/// Build a media content part from a native `files` entry. The message type of
/// the enclosing event decides the part kind.
fn file_part(message_type: &str, f: &Value) -> Option<ContentPart> {
    let id = f
        .get("id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?;
    let url = format!("{LINE_DATA_API}/v2/bot/message/{id}/content");
    let name = f
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let size = f.get("size").and_then(Value::as_u64);
    Some(match message_type {
        "image" => ContentPart::Image(ImageContent {
            url,
            alt_text: name,
            size,
            ..Default::default()
        }),
        "video" => ContentPart::Video(VideoContent {
            url,
            size,
            ..Default::default()
        }),
        "audio" => ContentPart::Audio(AudioContent {
            url,
            size,
            ..Default::default()
        }),
        _ => ContentPart::File(FileContent {
            url,
            filename: name,
            size,
            ..Default::default()
        }),
    })
}

// ---------------------------------------------------------------------------
// Text / URL helpers
// ---------------------------------------------------------------------------

/// LINE renders no Markdown, so flatten the common constructs to plain text.
///
/// Fenced code keeps its content but loses the fence lines; links become
/// `label (url)`; emphasis markers and heading `#`s are dropped.
fn markdown_to_plain(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    // Drop the opening/closing fence lines, keeping the code body verbatim.
    let fence = regex::Regex::new(r"(?ms)^```[^\n]*\n(.*?)^```").unwrap();
    let s = fence.replace_all(text, "$1").to_string();
    // An unterminated opening fence still has its marker line removed.
    let open_fence = regex::Regex::new(r"(?m)^```[^\n]*$").unwrap();
    let s = open_fence.replace_all(&s, "").to_string();
    // Links keep the label and expose the target in parentheses.
    let link = regex::Regex::new(r"\[([^\]\n]+)\]\(([^)\s]+)\)").unwrap();
    let s = link.replace_all(&s, "$1 ($2)").to_string();
    // Emphasis markers have no LINE counterpart.
    let bold = regex::Regex::new(r"\*\*([^*\n]+)\*\*").unwrap();
    let s = bold.replace_all(&s, "$1").to_string();
    let bold_u = regex::Regex::new(r"__([^_\n]+)__").unwrap();
    let s = bold_u.replace_all(&s, "$1").to_string();
    let strike = regex::Regex::new(r"~~([^~\n]+)~~").unwrap();
    let s = strike.replace_all(&s, "$1").to_string();
    // Headings keep their text, lose the leading `#`s.
    let heading = regex::Regex::new(r"(?m)^#{1,6}\s+(.+)$").unwrap();
    let s = heading.replace_all(&s, "$1").to_string();
    // Inline code backticks are invisible on LINE.
    let code = regex::Regex::new(r"`([^`\n]+)`").unwrap();
    code.replace_all(&s, "$1").to_string()
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
        // The byte offset of the last break we are willing to cut at.
        let cut = slice
            .rfind("\n\n")
            .or_else(|| slice.rfind('\n'))
            .or_else(|| slice.rfind(' '))
            .filter(|i| *i > limit / 3)
            .unwrap_or(slice.len());
        out.push(slice[..cut].trim_end().to_string());
        let consumed = slice[..cut].chars().count().max(1);
        start += consumed;
    }
    out.retain(|s| !s.is_empty());
    if out.is_empty() {
        out.push(text.to_string());
    }
    out
}

fn is_https(url: &str) -> bool {
    url.starts_with("https://")
}

/// The part's URL when it is a publicly usable HTTPS URL, else `None`.
fn public_https_url(part: &ContentPart) -> Option<&str> {
    part.url().filter(|u| is_https(u))
}

/// True for the LINE hosts that require the channel access token.
fn is_line_host(url: &str) -> bool {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let host = authority.split(':').next().unwrap_or(authority).to_ascii_lowercase();
    host == "line.me"
        || host.ends_with(".line.me")
        || host == "line-scdn.net"
        || host.ends_with(".line-scdn.net")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::MessageProcessor;
    use std::sync::Arc;

    fn config() -> LineConfig {
        LineConfig {
            channel_access_token: "token".into(),
            channel_secret: "secret".into(),
            ..Default::default()
        }
    }

    fn processor() -> MessageProcessor {
        Arc::new(|_msg: InboundMessage| {
            Box::pin(futures::stream::empty::<crate::models::MessageEvent>())
        })
    }

    fn channel(cfg: LineConfig) -> LineChannel {
        LineChannel::new(processor(), cfg, None, None, 0.0)
    }

    /// Sign a JSON body the way LINE does, producing a valid `WebhookRequest`.
    fn signed_request(payload: Value, secret: &str) -> WebhookRequest {
        let body = serde_json::to_vec(&payload).expect("serialize body");
        let signature = crate::webhook::hmac_sha256_base64(secret.as_bytes(), &body);
        let mut headers = HashMap::new();
        headers.insert("x-line-signature".to_string(), signature);
        WebhookRequest {
            method: "POST".to_string(),
            path: "/webhooks/line".to_string(),
            query: HashMap::new(),
            headers,
            body,
        }
    }

    fn message_event(source: Value, message: Value, event_id: &str) -> Value {
        json!({
            "destination": "Ubot",
            "events": [{
                "type": "message",
                "webhookEventId": event_id,
                "replyToken": "reply-1",
                "timestamp": 1_729_000_000_000u64,
                "source": source,
                "message": message,
            }]
        })
    }

    #[test]
    fn signature_accepts_and_rejects() {
        let wh = LineWebhook::new(config());
        let body = message_event(
            json!({"type": "user", "userId": "U1"}),
            json!({"type": "text", "id": "M1", "text": "hi"}),
            "evt-1",
        );

        // Correct signature.
        let ok = wh
            .handle_request(&signed_request(body.clone(), "secret"))
            .expect("valid signature must be accepted");
        assert_eq!(ok.payloads.len(), 1);

        // Signature made with the wrong secret.
        let bad = wh.handle_request(&signed_request(body.clone(), "wrong"));
        assert!(bad.is_err());

        // Tampered signature value.
        let mut tampered = signed_request(body, "secret");
        tampered
            .headers
            .insert("x-line-signature".into(), "AAAA".into());
        assert!(wh.handle_request(&tampered).is_err());

        // Missing header entirely.
        let mut missing = signed_request(
            message_event(
                json!({"type": "user", "userId": "U1"}),
                json!({"type": "text", "id": "M2", "text": "hi"}),
                "evt-2",
            ),
            "secret",
        );
        missing.headers.clear();
        assert!(wh.handle_request(&missing).is_err());
    }

    #[test]
    fn parses_text_message() {
        let wh = LineWebhook::new(config());
        let body = message_event(
            json!({"type": "user", "userId": "U1"}),
            json!({"type": "text", "id": "M1", "text": "hello world"}),
            "evt-text",
        );
        let outcome = wh
            .handle_request(&signed_request(body, "secret"))
            .expect("ok");
        assert_eq!(outcome.payloads.len(), 1);

        let ch = channel(config());
        let msg = ch.parse_inbound(&outcome.payloads[0]).expect("parse");
        assert_eq!(msg.channel_type, "line");
        assert_eq!(msg.text(), "hello world");
        assert_eq!(msg.channel_session_id, "U1");
        assert_eq!(msg.timestamp, 1_729_000_000.0);

        let subject = msg.channel_subject.expect("subject");
        assert_eq!(subject.subject_id, "U1");
        assert_eq!(subject.chat_type, "user");
        assert_eq!(
            msg.metadata.get("reply_token").and_then(Value::as_str),
            Some("reply-1")
        );
        assert_eq!(
            msg.metadata.get("to_handle").and_then(Value::as_str),
            Some("U1")
        );
    }

    #[test]
    fn parses_image_message() {
        let wh = LineWebhook::new(config());
        let body = message_event(
            json!({"type": "user", "userId": "U1"}),
            json!({"type": "image", "id": "M-image"}),
            "evt-image",
        );
        let outcome = wh
            .handle_request(&signed_request(body, "secret"))
            .expect("ok");
        assert_eq!(outcome.payloads.len(), 1);

        let ch = channel(config());
        let msg = ch.parse_inbound(&outcome.payloads[0]).expect("parse");
        assert!(msg.text().is_empty());
        match msg.content.first() {
            Some(ContentPart::Image(img)) => {
                assert!(img.url.contains("/v2/bot/message/M-image/content"));
            }
            other => panic!("expected an image part, got {other:?}"),
        }
    }

    #[test]
    fn whitelist_filters_strangers_and_groups() {
        // User whitelist.
        let mut cfg = config();
        cfg.allowed_user_ids = vec!["U-allowed".into()];
        let wh = LineWebhook::new(cfg);

        let denied = message_event(
            json!({"type": "user", "userId": "U-other"}),
            json!({"type": "text", "id": "M1", "text": "hi"}),
            "evt-deny",
        );
        let outcome = wh
            .handle_request(&signed_request(denied, "secret"))
            .expect("ok");
        assert!(outcome.payloads.is_empty());

        let allowed = message_event(
            json!({"type": "user", "userId": "U-allowed"}),
            json!({"type": "text", "id": "M2", "text": "hi"}),
            "evt-allow",
        );
        let outcome = wh
            .handle_request(&signed_request(allowed, "secret"))
            .expect("ok");
        assert_eq!(outcome.payloads.len(), 1);

        // Group whitelist only serves a listed group.
        let mut cfg = config();
        cfg.allowed_group_ids = vec!["G-ok".into()];
        let wh = LineWebhook::new(cfg);
        let bad_group = message_event(
            json!({"type": "group", "groupId": "G-no", "userId": "U1"}),
            json!({"type": "text", "id": "M3", "text": "hi"}),
            "evt-group-deny",
        );
        let outcome = wh
            .handle_request(&signed_request(bad_group, "secret"))
            .expect("ok");
        assert!(outcome.payloads.is_empty());
    }

    #[test]
    fn dedupes_repeated_deliveries() {
        let wh = LineWebhook::new(config());
        let body = message_event(
            json!({"type": "user", "userId": "U1"}),
            json!({"type": "text", "id": "M1", "text": "hi"}),
            "evt-dup",
        );

        let first = wh
            .handle_request(&signed_request(body.clone(), "secret"))
            .expect("ok");
        assert_eq!(first.payloads.len(), 1);

        // LINE retries the same event; the second delivery must be ack-only.
        let second = wh
            .handle_request(&signed_request(body, "secret"))
            .expect("ok");
        assert!(second.payloads.is_empty());
    }
}
