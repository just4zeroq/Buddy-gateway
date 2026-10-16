//! DingTalk channel (钉钉).
//!
//! Mirrors `octop_gateway.channels.dingtalk`. Receives robot messages through
//! the DingTalk **Stream** protocol (a WebSocket long connection) and sends
//! replies either via the per-message `sessionWebhook` or through the robot
//! OpenAPI (`robot/oToMessages/batchSend`). Media is uploaded via the legacy
//! `/media/upload` OAPI endpoint.
//!
//! Python uses the `dingtalk-stream` SDK; here the stream handshake
//! (connection open → WebSocket → ack frames → CALLBACK dispatch) is
//! re-implemented directly on `tokio-tungstenite` (see [`mod stream`]).
//!
//! NOTE: `app_key` / `app_secret` are the canonical config keys; the manager
//! already rewrites the `client_id` / `client_secret` aliases (see
//! `channels::aliases_for("dingtalk")`), because `ChannelConfigLike` (whose
//! `field_aliases` hook is unused by the `channel_adapter!` macro) lives in a
//! shared file we must not edit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::channel::Channel;
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage, RawPayload,
};

// ---------------------------------------------------------------------------
// Endpoints / constants
// ---------------------------------------------------------------------------

/// DingTalk OpenAPI base URL (v2 / access-token APIs).
const API_BASE: &str = "https://api.dingtalk.com";
/// DingTalk legacy OAPI base URL (media upload).
const OAPI_BASE: &str = "https://oapi.dingtalk.com";
/// Token refresh interval (1.5 hours; token valid for 2 hours).
const TOKEN_REFRESH_INTERVAL: f64 = 5400.0;
/// The robot chatbot callback topic registered with the stream gateway.
const ROBOT_TOPIC: &str = "/v1.0/im/bot/messages/get";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the DingTalk channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DingTalkConfig {
    #[serde(flatten)]
    pub base: crate::channel::ChannelConfig,
    /// DingTalk application key (Client ID).
    pub app_key: String,
    /// DingTalk application secret (Client Secret).
    pub app_secret: String,
    /// Robot code for proactive messaging. If empty, falls back to `app_key`.
    pub robot_code: String,
}

impl DingTalkConfig {
    /// Return `robot_code` if set, otherwise fall back to `app_key`.
    pub fn effective_robot_code(&self) -> String {
        if self.robot_code.is_empty() {
            self.app_key.clone()
        } else {
            self.robot_code.clone()
        }
    }
}

#[derive(Clone, Default)]
struct TokenState {
    token: String,
    expires_at: f64,
}

crate::channel_adapter! {
    /// DingTalk channel.
    DingTalkChannel, DingTalkConfig, "dingtalk",
    required = [app_key, app_secret],
    fields = {
        stream_task: Option<JoinHandle<()>>,
        token_state: Arc<AsyncMutex<TokenState>>,
        running: Arc<AtomicBool>,
        stream_session_id: Option<String>,
    }
}

impl DingTalkChannel {
    // =======================================================================
    // Token Management
    // =======================================================================

    /// Obtain or refresh the DingTalk `access_token` (new v2 credential API).
    async fn refresh_token(&self) -> Result<String> {
        let http = self.core.ensure_http().await?;
        refresh_token_free(
            &self.config.app_key,
            &self.config.app_secret,
            &http,
            &self.token_state,
        )
        .await
    }

    /// Authorization headers carrying a valid token.
    async fn auth_token(&self) -> Result<String> {
        self.refresh_token().await
    }

    // =======================================================================
    // Sending
    // =======================================================================

    /// Send text: prefer the inbound `sessionWebhook`, else the DM OpenAPI.
    async fn send_via_webhook(&self, webhook_url: &str, text: &str) -> Result<()> {
        let http = self.core.ensure_http().await?;
        let payload = if text.chars().count() > 3500 {
            json!({ "msgtype": "text", "text": { "content": text } })
        } else {
            json!({ "msgtype": "markdown", "markdown": { "title": "Octop", "text": text } })
        };
        match http.post(webhook_url).json(&payload).send().await {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(data) => {
                    let errcode = data.get("errcode").and_then(|v| v.as_i64()).unwrap_or(0);
                    if errcode != 0 {
                        tracing::error!(
                            "DingTalk webhook send failed: errcode={} errmsg={}",
                            errcode,
                            data.get("errmsg").and_then(|v| v.as_str()).unwrap_or("unknown")
                        );
                    }
                }
                Err(e) => tracing::warn!("DingTalk webhook response decode error: {}", e),
            },
            Err(e) => tracing::error!("HTTP error sending DingTalk webhook message: {}", e),
        }
        Ok(())
    }

    /// Send a single-chat (DM) message via `robot/oToMessages/batchSend`.
    async fn send_via_openapi(
        &self,
        subject_id: &str,
        text: &str,
        _meta: &HashMap<String, Value>,
    ) -> Result<()> {
        let http = self.core.ensure_http().await?;
        let token = self.auth_token().await?;
        let url = format!("{API_BASE}/v1.0/robot/oToMessages/batchSend");

        let (msg_key, msg_param) = if text.chars().count() > 3500 {
            ("sampleText", json!({ "content": text }))
        } else {
            ("sampleMarkdown", json!({ "title": "Octop", "text": text }))
        };

        let payload = json!({
            "robotCode": self.config.effective_robot_code(),
            "userIds": [subject_id],
            "msgKey": msg_key,
            "msgParam": serde_json::to_string(&msg_param).unwrap_or_default(),
        });

        match http
            .post(&url)
            .header("x-acs-dingtalk-access-token", token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(data) => {
                    if data.get("processQueryKey").is_none() && data.get("requestId").is_none() {
                        tracing::error!("DingTalk OpenAPI send failed: {}", data);
                    }
                }
                Err(e) => tracing::warn!("DingTalk OpenAPI response decode error: {}", e),
            },
            Err(e) => tracing::error!("HTTP error sending DingTalk OpenAPI message: {}", e),
        }
        Ok(())
    }

    /// Send a media message (image/file/voice) via `robot/oToMessages/batchSend`.
    async fn send_media_message(
        &self,
        subject_id: &str,
        media_type: &str,
        media_id: &str,
        meta: &HashMap<String, Value>,
        filename: &str,
    ) -> Result<()> {
        let http = self.core.ensure_http().await?;
        let token = self.auth_token().await?;
        let url = format!("{API_BASE}/v1.0/robot/oToMessages/batchSend");

        let msg_key = match media_type {
            "image" => "sampleImageMsg",
            "file" => "sampleFile",
            "voice" => "sampleAudio",
            _ => "sampleFile",
        };

        let msg_param = if media_type == "image" {
            json!({ "photoURL": media_id })
        } else if media_type == "file" {
            let resolved_filename = if filename.is_empty() { "file" } else { filename };
            json!({
                "mediaId": media_id,
                "fileName": resolved_filename,
                "fileType": file_type_for_name(resolved_filename),
            })
        } else {
            json!({ "mediaId": media_id })
        };

        // Session webhooks support text/Markdown replies but not uploaded media;
        // group conversations therefore cannot receive uploaded media.
        let conversation_type = meta
            .get("conversation_type")
            .and_then(|v| v.as_str())
            .unwrap_or("1");
        if conversation_type != "1" {
            return Err(GatewayError::Other(
                "DingTalk group session webhooks do not support uploaded media".to_string(),
            ));
        }

        let payload_dm = json!({
            "robotCode": self.config.effective_robot_code(),
            "userIds": [subject_id],
            "msgKey": msg_key,
            "msgParam": serde_json::to_string(&msg_param).unwrap_or_default(),
        });

        let resp = http
            .post(&url)
            .header("x-acs-dingtalk-access-token", token)
            .header("Content-Type", "application/json")
            .json(&payload_dm)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!(
                "DingTalk OpenAPI media send error: HTTP {} {}",
                status, body
            )));
        }
        let data: Value = serde_json::from_str(&resp.text().await.unwrap_or_default())
            .unwrap_or(Value::Null);
        if data.get("processQueryKey").is_none() && data.get("requestId").is_none() {
            return Err(GatewayError::Other(format!(
                "DingTalk media send failed: {}",
                data
            )));
        }
        Ok(())
    }

    // =======================================================================
    // Media — upload (+ download via the shared `fetch_remote_media` hook)
    // =======================================================================

    /// Upload media bytes to DingTalk and return the `media_id`.
    async fn upload_to_dingtalk(&self, part: &ContentPart, media_type: &str) -> Result<String> {
        let (data, content_type) = self.load_media_bytes(part).await?;
        let http = self.core.ensure_http().await?;
        let token = self.refresh_token().await?;

        // Use the legacy OAPI for media upload (more reliable).
        let url = format!("{OAPI_BASE}/media/upload?access_token={token}&type={media_type}");

        let ctype = if content_type.is_empty() {
            mime_for_type(media_type).to_string()
        } else {
            content_type
        };
        let filename = upload_filename(part, media_type);

        let file_part = reqwest::multipart::Part::bytes(data)
            .file_name(filename)
            .mime_str(&ctype)?;
        let form = reqwest::multipart::Form::new()
            .text("type", media_type.to_string())
            .part("media", file_part);

        let resp = http.post(&url).multipart(form).send().await?;
        let result: Value = serde_json::from_str(&resp.text().await.unwrap_or_default())
            .unwrap_or(Value::Null);
        let media_id = result
            .get("media_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if media_id.is_empty() {
            let errmsg = result
                .get("errmsg")
                .map(|v| v.to_string())
                .unwrap_or_else(|| result.to_string());
            return Err(GatewayError::Other(format!(
                "DingTalk media upload failed: {errmsg}"
            )));
        }
        Ok(media_id)
    }
}

#[async_trait]
impl Channel for DingTalkChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    // Python DingTalkChannel does not override `_default_constraints`, so it
    // inherits the base `ChannelConstraints()` defaults.
    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints::default()
    }

    async fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        // Refresh token up-front (mirrors `await self._refresh_token()`).
        let _ = self.refresh_token().await?;

        // Generate a new session ID for this stream connection.
        let session_id = uuid::Uuid::new_v4().to_string();
        self.stream_session_id = Some(session_id.clone());
        self.core.set_connection_session(Some(session_id));

        if let Some(t) = self.stream_task.take() {
            t.abort();
        }
        let enqueue = self.core.enqueue_sender();
        let app_key = self.config.app_key.clone();
        let app_secret = self.config.app_secret.clone();
        let running = self.running.clone();
        let token_state = self.token_state.clone();
        let task = tokio::spawn(async move {
            stream::run_stream(app_key, app_secret, enqueue, running, token_state).await;
        });
        self.stream_task = Some(task);
        tracing::info!("DingTalkChannel started (app_key={})", self.config.app_key);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(t) = self.stream_task.take() {
            t.abort();
        }
        self.stream_session_id = None;
        self.core.set_connection_session(None);
        tracing::info!("DingTalkChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let webhook_url = subject
            .metadata
            .get("webhook_url")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !webhook_url.is_empty() {
            self.send_via_webhook(webhook_url, text).await
        } else {
            self.send_via_openapi(&subject.subject_id, text, &subject.metadata)
                .await
        }
    }

    /// Concatenate text parts into one message; send each media part separately.
    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        let mut text_segments: Vec<String> = Vec::new();
        let mut media_parts: Vec<ContentPart> = Vec::new();
        for part in parts {
            if let Some(t) = part.text_value() {
                if !t.is_empty() {
                    text_segments.push(t.to_string());
                }
            } else {
                media_parts.push(part.clone());
            }
        }
        if !text_segments.is_empty() {
            self.send_text(subject, &text_segments.join("\n")).await?;
        }
        for media in &media_parts {
            self.send_media(subject, media).await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let meta = subject.metadata.clone();
        let sent: Result<()> = async {
            match media {
                ContentPart::Image(_) => {
                    let media_id = self.upload_to_dingtalk(media, "image").await?;
                    self.send_media_message(&subject.subject_id, "image", &media_id, &meta, "")
                        .await
                }
                ContentPart::File(f) => {
                    let media_id = self.upload_to_dingtalk(media, "file").await?;
                    self.send_media_message(
                        &subject.subject_id,
                        "file",
                        &media_id,
                        &meta,
                        &f.filename,
                    )
                    .await
                }
                ContentPart::Audio(_) => {
                    let media_id = self.upload_to_dingtalk(media, "voice").await?;
                    self.send_media_message(&subject.subject_id, "voice", &media_id, &meta, "")
                        .await
                }
                _ => {
                    // Fallback: send URL as text.
                    let url = media.url().unwrap_or("");
                    if !url.is_empty() {
                        let label = media.label_with_filename();
                        self.send_text(subject, &format!("[{label}: {url}]")).await?;
                    }
                    Ok(())
                }
            }
        }
        .await;

        if let Err(e) = sent {
            tracing::error!("Failed to send DingTalk media: {}", e);
            let url = media.url().unwrap_or("");
            let label = media.label_with_filename();
            if !url.is_empty() {
                let _ = self.send_text(subject, &format!("[Attachment: {url}]")).await;
            } else if media.data().is_some() || media.local_path().is_some() {
                let _ = self
                    .send_text(subject, &format!("[{label} (local upload failed)]"))
                    .await;
            }
        }
        Ok(())
    }

    /// Download from DingTalk: resolve non-http identifiers (`downloadCode`)
    /// through `messageFiles/download` before fetching the bytes.
    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.core.ensure_http().await?;
        let mut download_url = url.to_string();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            let token = self.auth_token().await?;
            let resolve_url = format!("{API_BASE}/v1.0/robot/messageFiles/download");
            let payload = json!({
                "downloadCode": url,
                "robotCode": self.config.effective_robot_code(),
            });
            let resp = http
                .post(&resolve_url)
                .header("x-acs-dingtalk-access-token", token)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await?;
            if !resp.status().is_success() {
                return Err(GatewayError::Other(format!(
                    "DingTalk messageFiles/download HTTP {}",
                    resp.status()
                )));
            }
            let result: Value = resp.json().await?;
            download_url = result
                .get("downloadUrl")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !download_url.starts_with("http://") && !download_url.starts_with("https://") {
                return Err(GatewayError::Other(
                    "DingTalk message file response missing a valid downloadUrl".to_string(),
                ));
            }
        }

        let resp = http.get(&download_url).send().await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "DingTalk media fetch HTTP {}",
                resp.status()
            )));
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let data = resp.bytes().await?;
        Ok((data.to_vec(), content_type))
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let v = raw.as_json().ok_or_else(|| {
            GatewayError::Other("DingTalkChannel.parse_inbound expects dict".to_string())
        })?;
        if !v.is_object() {
            return Err(GatewayError::Other(
                "DingTalkChannel.parse_inbound expects dict".to_string(),
            ));
        }

        let sender_id = v
            .get("sender_id")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let sender_nick = v
            .get("sender_nick")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let conversation_id = v
            .get("conversation_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let conversation_type = value_to_string(v.get("conversation_type"))
            .unwrap_or_else(|| "1".to_string());
        let msgtype = v
            .get("msgtype")
            .and_then(|x| x.as_str())
            .unwrap_or("text")
            .to_string();
        let msg_id = v
            .get("msg_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();

        // Build session key.
        let session_id = self
            .stream_session_id
            .clone()
            .unwrap_or_else(|| "dingtalk-unconnected".to_string());

        // Parse timestamp.
        let mut timestamp = crate::channel::now_f64();
        if let Some(create_at) = v.get("create_at") {
            let truthy = match create_at {
                Value::String(s) => !s.is_empty(),
                Value::Number(n) => n.as_f64().map(|x| x != 0.0).unwrap_or(false),
                _ => false,
            };
            if truthy {
                let parsed = match create_at {
                    Value::Number(n) => n.as_f64(),
                    Value::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                };
                if let Some(n) = parsed {
                    timestamp = (n as i64) as f64 / 1000.0;
                }
            }
        }

        let parts = parse_content_parts(&msgtype, v);

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("msg_id".to_string(), json!(msg_id));
        metadata.insert("conversation_id".to_string(), json!(conversation_id));
        metadata.insert("conversation_type".to_string(), json!(conversation_type));
        metadata.insert("sender_nick".to_string(), json!(sender_nick));
        metadata.insert("to_handle".to_string(), json!(sender_id));

        let webhook_url = v
            .get("webhook_url")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if !webhook_url.is_empty() {
            metadata.insert("webhook_url".to_string(), json!(webhook_url));
        }

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "dingtalk".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id: sender_id,
                first_seen: None,
                last_seen: None,
                display_name: String::new(),
                chat_type: String::new(),
                metadata: metadata.clone(),
            }),
            channel_session_id: session_id,
            content: parts,
            group_context: None,
            metadata,
            timestamp,
        })
    }
}

// ---------------------------------------------------------------------------
// Inbound content parsing helpers
// ---------------------------------------------------------------------------

fn value_to_string(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn parse_content_parts(msgtype: &str, payload: &Value) -> Vec<ContentPart> {
    let mut parts: Vec<ContentPart> = Vec::new();

    match msgtype {
        "text" => {
            let text_data = payload.get("text");
            let content = match text_data {
                Some(Value::Object(o)) => o
                    .get("content")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string(),
                Some(Value::String(s)) => s.clone(),
                _ => String::new(),
            };
            if !content.is_empty() {
                parts.push(ContentPart::text(strip_at_mention(&content)));
            }
        }
        "richText" => {
            let rich_text = payload.get("content").cloned().unwrap_or(Value::Null);
            let text = extract_rich_text(&rich_text);
            if !text.is_empty() {
                parts.push(ContentPart::text(text));
            }
        }
        "picture" => {
            let content = payload.get("content");
            let mut picture_url = content
                .and_then(|c| c.get("downloadCode"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if picture_url.is_empty() {
                picture_url = content
                    .and_then(|c| c.get("pictureDownloadCode"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            if !picture_url.is_empty() {
                parts.push(ContentPart::Image(ImageContent {
                    url: picture_url,
                    ..Default::default()
                }));
            } else {
                parts.push(ContentPart::text("[Image]"));
            }
        }
        "file" => {
            let file_info = payload.get("content");
            let download_code = file_info
                .and_then(|c| c.get("downloadCode"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let filename = file_info
                .and_then(|c| c.get("fileName"))
                .and_then(|x| x.as_str())
                .unwrap_or("file")
                .to_string();
            if !download_code.is_empty() {
                parts.push(ContentPart::File(FileContent {
                    url: download_code,
                    filename,
                    ..Default::default()
                }));
            } else {
                parts.push(ContentPart::text(format!("[File: {filename}]")));
            }
        }
        "audio" => {
            let audio_info = payload.get("content");
            let download_code = audio_info
                .and_then(|c| c.get("downloadCode"))
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let duration = audio_info
                .and_then(|c| c.get("duration"))
                .and_then(|x| x.as_u64());
            if !download_code.is_empty() {
                parts.push(ContentPart::Audio(AudioContent {
                    url: download_code,
                    duration,
                    ..Default::default()
                }));
            } else {
                parts.push(ContentPart::text("[Audio]"));
            }
        }
        other => {
            tracing::debug!("Unknown DingTalk msgtype: {}", other);
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

/// Remove a leading `@bot` mention from message text.
fn strip_at_mention(text: &str) -> String {
    let stripped = text.trim();
    let mut out = stripped.to_string();
    if stripped.starts_with('@') {
        if let Some(idx) = stripped.find(' ') {
            if idx > 0 && idx < 30 {
                out = stripped[idx + 1..].trim().to_string();
            }
        }
    }
    if out.is_empty() {
        text.to_string()
    } else {
        out
    }
}

/// Extract plain text from a DingTalk `richText` structure.
fn extract_rich_text(content: &Value) -> String {
    match content {
        Value::Object(o) => {
            let items = match o.get("richText") {
                Some(Value::Array(a)) => a.clone(),
                _ => return String::new(),
            };
            let mut text_parts: Vec<String> = Vec::new();
            for item in items {
                if let Some(t) = item.get("text").and_then(|x| x.as_str()) {
                    if !t.is_empty() {
                        text_parts.push(t.to_string());
                    }
                }
            }
            text_parts.join("")
        }
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Module-level helpers (media naming / mime)
// ---------------------------------------------------------------------------

fn extension_for_type(media_type: &str) -> &'static str {
    match media_type {
        "image" => "png",
        "voice" => "amr",
        "file" => "bin",
        _ => "bin",
    }
}

/// The extension expected by DingTalk's `sampleFile` template.
fn file_type_for_name(filename: &str) -> String {
    let basename = filename.rsplit('/').next().unwrap_or(filename);
    if !basename.contains('.') {
        return "file".to_string();
    }
    let ext = basename.rsplit('.').next().unwrap_or("").to_lowercase();
    if ext.is_empty() {
        "file".to_string()
    } else {
        ext
    }
}

/// Preserve file names so DingTalk can infer the uploaded media type.
fn upload_filename(part: &ContentPart, media_type: &str) -> String {
    if let Some(name) = part.filename() {
        if !name.is_empty() {
            return name.to_string();
        }
    }
    format!("upload.{}", extension_for_type(media_type))
}

/// Default MIME type for a DingTalk media type.
fn mime_for_type(media_type: &str) -> &'static str {
    match media_type {
        "image" => "image/png",
        "voice" => "audio/amr",
        "file" => "application/octet-stream",
        _ => "application/octet-stream",
    }
}

// ---------------------------------------------------------------------------
// Token helper (shared by the channel methods and the stream task)
// ---------------------------------------------------------------------------

async fn refresh_token_free(
    app_key: &str,
    app_secret: &str,
    http: &reqwest::Client,
    state: &AsyncMutex<TokenState>,
) -> Result<String> {
    let mut st = state.lock().await;
    let now = crate::channel::now_f64();
    if !st.token.is_empty() && now < st.expires_at {
        return Ok(st.token.clone());
    }

    let url = format!("{API_BASE}/v1.0/oauth2/accessToken");
    let payload = json!({ "appKey": app_key, "appSecret": app_secret });
    let resp = http.post(&url).json(&payload).send().await?;
    let data: Value = resp.json().await?;

    let token = data
        .get("accessToken")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if token.is_empty() {
        let msg = data
            .get("message")
            .or_else(|| data.get("errmsg"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        return Err(GatewayError::Other(format!(
            "DingTalk token refresh failed: {msg}"
        )));
    }
    let expire_in = data.get("expireIn").and_then(|v| v.as_f64()).unwrap_or(7200.0);
    st.token = token.clone();
    st.expires_at = now + (expire_in - 300.0).min(TOKEN_REFRESH_INTERVAL);
    Ok(token)
}

// ---------------------------------------------------------------------------
// Stream client (dingtalk-stream protocol, inlined)
// ---------------------------------------------------------------------------

mod stream {
    use super::*;
    use crate::channel::EnqueueFn;

    /// Background loop: token → open connection → WebSocket, with reconnect.
    pub(super) async fn run_stream(
        app_key: String,
        app_secret: String,
        enqueue: Option<EnqueueFn>,
        running: Arc<AtomicBool>,
        token_state: Arc<AsyncMutex<TokenState>>,
    ) {
        let http = reqwest::Client::new();
        let mut backoff = 1.0f64;

        loop {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let token = match refresh_token_free(&app_key, &app_secret, &http, &token_state).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("DingTalk stream token error: {}", e);
                    tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                    backoff = (backoff * 2.0).min(60.0);
                    continue;
                }
            };

            let (endpoint, ticket) =
                match open_connection(&http, &token, &app_key, &app_secret).await {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!("DingTalk stream open error: {}", e);
                        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                        backoff = (backoff * 2.0).min(60.0);
                        continue;
                    }
                };

            let url = format!("{endpoint}?ticket={ticket}");
            tracing::debug!("DingTalk stream connecting: {}", endpoint);
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => {
                    backoff = 1.0;
                    if let Err(e) = stream_loop(ws, &enqueue, &running).await {
                        tracing::warn!("DingTalk stream loop ended: {}", e);
                    }
                }
                Err(e) => tracing::warn!("DingTalk stream connect failed: {}", e),
            }

            if !running.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(60.0);
        }
    }

    /// Open a stream connection and return `(endpoint, ticket)`.
    async fn open_connection(
        http: &reqwest::Client,
        token: &str,
        app_key: &str,
        app_secret: &str,
    ) -> Result<(String, String)> {
        let resp = http
            .post(format!("{API_BASE}/v1.0/gateway/connections/open"))
            .header("x-acs-dingtalk-access-token", token)
            .header("Content-Type", "application/json")
            .json(&json!({
                "clientId": app_key,
                "clientSecret": app_secret,
                "subscriptions": [{
                    "type": "CALLBACK",
                    "topic": ROBOT_TOPIC,
                    "desiredEventTypes": ["*"],
                }],
                "ua": "buddy-gateway/0.1",
                "localIp": "",
            }))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "DingTalk stream open HTTP {}",
                resp.status()
            )));
        }
        let v: Value = resp.json().await?;
        let endpoint = v
            .get("endpoint")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let ticket = v
            .get("ticket")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if endpoint.is_empty() || ticket.is_empty() {
            return Err(GatewayError::Other(format!(
                "DingTalk stream open failed: {v}"
            )));
        }
        Ok((endpoint, ticket))
    }

    /// Acknowledge a stream frame.
    fn ack_frame(topic: &str, message_id: &str, data: &str) -> String {
        json!({
            "code": 200,
            "headers": {
                "topic": topic,
                "messageId": message_id,
                "contentType": "application/json",
            },
            "message": "OK",
            "data": data,
        })
        .to_string()
    }

    /// Read frames: ack SYSTEM/CALLBACK frames and enqueue CALLBACK payloads.
    async fn stream_loop(
        ws: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        enqueue: &Option<EnqueueFn>,
        running: &Arc<AtomicBool>,
    ) -> Result<()> {
        let (mut write, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let msg = match msg {
                Ok(m) => m,
                Err(_) => break,
            };
            let txt = match msg {
                Message::Text(t) => t,
                Message::Close(_) => break,
                _ => continue,
            };
            let v: Value = match serde_json::from_str(txt.as_str()) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let ftype = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
            let topic = v
                .pointer("/headers/topic")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let message_id = v
                .pointer("/headers/messageId")
                .and_then(|x| x.as_str())
                .unwrap_or("");

            match ftype {
                "SYSTEM" => {
                    let ack = ack_frame(topic, message_id, "{}");
                    let _ = write.send(Message::Text(ack.into())).await;
                }
                "CALLBACK" | "EVENT" => {
                    let data = v.get("data").cloned().unwrap_or(Value::Null);
                    let data_obj = match data {
                        Value::String(s) => {
                            serde_json::from_str::<Value>(&s).unwrap_or(Value::Null)
                        }
                        other => other,
                    };
                    if let Some(payload) = build_payload(&data_obj) {
                        if let Some(cb) = enqueue {
                            cb(RawPayload::Json(payload));
                        }
                    }
                    let ack = ack_frame(topic, message_id, "{\"response\":null}");
                    let _ = write.send(Message::Text(ack.into())).await;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Normalize a ChatbotMessage `data` object into the channel's raw payload.
    fn build_payload(data: &Value) -> Option<Value> {
        if !data.is_object() {
            return None;
        }
        let mut p = json!({
            "sender_id": data
                .get("senderStaffId")
                .or_else(|| data.get("senderId"))
                .and_then(|x| x.as_str())
                .unwrap_or(""),
            "sender_nick": data.get("senderNick").and_then(|x| x.as_str()).unwrap_or(""),
            "sender_corp_id": data.get("senderCorpId").and_then(|x| x.as_str()).unwrap_or(""),
            "conversation_id": data.get("conversationId").and_then(|x| x.as_str()).unwrap_or(""),
            "conversation_type": value_to_string(data.get("conversationType"))
                .unwrap_or_else(|| "1".to_string()),
            "msg_id": data.get("msgId").and_then(|x| x.as_str()).unwrap_or(""),
            "msgtype": data.get("msgtype").and_then(|x| x.as_str()).unwrap_or("text"),
            "text": data.get("text").cloned().unwrap_or(Value::Null),
            "content": data.get("content").cloned().unwrap_or(Value::Null),
            "create_at": data.get("createAt").cloned().unwrap_or(Value::Null),
        });
        if let Some(wh) = data.get("sessionWebhook").and_then(|x| x.as_str()) {
            if !wh.is_empty() {
                p["webhook_url"] = json!(wh);
            }
        }
        Some(p)
    }
}
