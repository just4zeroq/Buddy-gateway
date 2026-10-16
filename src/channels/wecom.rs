//! WeCom (企业微信) AI-bot channel.
//!
//! Connects to the WeCom AI-bot WebSocket for bidirectional messaging with no
//! public URL, supports streamed replies (thinking bubble + progressive output)
//! and encrypted media transfer.
//!
//! This channel overrides `handle_inbound` to stream deltas through the
//! SDK's `reply_stream` (finish=False) and it overrides `_persist_media` to
//! decrypt media with the per-message `aeskey`. Both provided trait methods are
//! overridden here as well — Rust permits overriding trait methods that have a
//! default body.
//!
//! NOTE (residual differences, all protocol-level):
//! * The WeCom AI-bot wire protocol is not published; the connect
//! (subscribe), heartbeat, message-callback, reply-stream, upload and
//! download frames implemented in [`mod ws_client`] reproduce the *shape*
//! documented by the SDK (`{"cmd", "headers": {"req_id"}, "body": {...}}`)
//! and the `_on_message` docstring, but exact field names may need tuning
//! against a live deployment.
//! * `metadata["_ws_client"]` (a live connection object) cannot be carried in a
//! `serde_json::Value`; the equivalent state lives on the channel itself
//! (`self.ws_tx` / `self.pending`), and `_frame` is carried as JSON.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::channel::Channel;
use crate::constraints::ChannelConstraints;
use crate::errors::{GatewayError, Result};
use crate::models::{
    ContentPart, InboundMessage, MessageEvent, MessageEventType, RawPayload,
};

/// Default WeCom AI-bot WebSocket endpoint (SDK default).
const DEFAULT_WS_URL: &str = "wss://open.work.weixin.qq.com/aibot/ws";
/// Whitelist of inbound message types the channel processes.
const MSG_TYPE_WHITELIST: &[&str] = &["text", "image", "file", "voice", "video", "mixed"];

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the WeCom AI-bot channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WeComConfig {
    #[serde(flatten)]
    pub base: crate::channel::ChannelConfig,
    /// AI-bot ID from the WeCom admin console.
    pub bot_id: String,
    /// Bot secret for authentication.
    pub secret: String,
    /// Optional custom WebSocket URL (empty → SDK default).
    pub ws_url: String,
}

crate::channel_adapter! {
    /// WeCom (Enterprise WeChat) channel.
    WeComChannel, WeComConfig, "wecom",
    required = [bot_id, secret],
    fields = {
        ws_task: Option<JoinHandle<()>>,
        ws_tx: Option<tokio::sync::mpsc::UnboundedSender<Value>>,
        pending: Arc<AsyncMutex<HashMap<String, oneshot::Sender<Value>>>>,
        running: Arc<AtomicBool>,
        connected: Arc<AtomicBool>,
        ws_session_id: Arc<StdMutex<Option<String>>>,
    }
}

/// Outcome of handling a single streamed processor event.
enum StreamAction {
    Continue,
    Break,
    Finish,
}

impl WeComChannel {
    // =======================================================================
    // WebSocket primitives
    // =======================================================================

    /// Send a fire-and-forget command frame to the WebSocket writer task.
    fn ws_send(&self, frame: Value) -> Result<()> {
        match &self.ws_tx {
            Some(tx) => tx.send(frame).map_err(|e| {
                GatewayError::Other(format!("WeCom send channel closed: {e}"))
            }),
            None => Err(GatewayError::Other(
                "WeComChannel: no ws_client available for proactive send".to_string(),
            )),
        }
    }

    /// Send a command and await its correlated response (matched on `req_id`).
    async fn ws_request(&self, req_id: &str, frame: Value) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(req_id.to_string(), tx);
        self.ws_send(frame)?;
        match tokio::time::timeout(Duration::from_secs(15), rx).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                self.pending.lock().await.remove(req_id);
                Err(GatewayError::Other("WeCom request timeout".to_string()))
            }
        }
    }

    /// `reply_stream(frame, stream_id, content, finish)` — SDK streamed reply.
    async fn reply_stream(
        &self,
        frame: &Value,
        stream_id: &str,
        content: &str,
        finish: bool,
    ) -> Result<()> {
        self.ws_send(json!({
            "cmd": "aibot_respond",
            "headers": { "req_id": new_req_id("stream") },
            "body": {
                "stream_id": stream_id,
                "frame": frame,
                "content": content,
                "finish": finish,
            }
        }))
    }

    /// `send_message(chat_id, ...)` — proactive send (no frame context).
    async fn send_message(&self, chat_id: &str, content: &str) -> Result<()> {
        self.ws_send(json!({
            "cmd": "aibot_send",
            "headers": { "req_id": new_req_id("send") },
            "body": {
                "chat_id": chat_id,
                "msgtype": "markdown",
                "markdown": { "content": content },
            }
        }))
    }

    /// `upload_media(data, type, filename)` → returns `media_id`.
    async fn upload_media(
        &self,
        data: &[u8],
        media_type: &str,
        filename: &str,
    ) -> Result<String> {
        let req_id = new_req_id("upload");
        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
        let frame = json!({
            "cmd": "aibot_upload_media",
            "headers": { "req_id": req_id },
            "body": { "type": media_type, "filename": filename, "data": encoded }
        });
        let resp = self.ws_request(&req_id, frame).await?;
        Ok(resp
            .pointer("/body/media_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string())
    }

    /// `download_file(url, aes_key)` → `(buffer, filename)` (decrypts with aeskey).
    async fn download_file(&self, url: &str, aes_key: &str) -> Result<(Vec<u8>, Option<String>)> {
        let req_id = new_req_id("download");
        let frame = json!({
            "cmd": "aibot_download_file",
            "headers": { "req_id": req_id },
            "body": { "url": url, "aes_key": aes_key }
        });
        let resp = self.ws_request(&req_id, frame).await?;
        let encoded = resp
            .pointer("/body/buffer")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let data = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap_or_default();
        let filename = resp
            .pointer("/body/filename")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        Ok((data, filename))
    }

    // =======================================================================
    // Native media send
    // =======================================================================

    async fn send_media_native(
        &self,
        subject: &crate::models::ChannelSubject,
        media: &ContentPart,
        media_type: &str,
        filename: &str,
    ) -> Result<()> {
        let meta = &subject.metadata;
        let (data, _mime) = self.load_media_bytes(media).await?;
        let media_id = self.upload_media(&data, media_type, filename).await?;
        if media_id.is_empty() {
            return Err(GatewayError::Other(
                "WeCom media upload returned no media_id".to_string(),
            ));
        }

        if let Some(frame) = meta.get("_frame") {
            return self.ws_send(json!({
                "cmd": "aibot_reply_media",
                "headers": { "req_id": new_req_id("media") },
                "body": { "frame": frame, "media_type": media_type, "media_id": media_id }
            }));
        }

        let chat_id = meta
            .get("chat_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone());
        if chat_id.is_empty() {
            return Err(GatewayError::Other(
                "WeComChannel: no chat_id or subject_id for media send".to_string(),
            ));
        }
        self.ws_send(json!({
            "cmd": "aibot_send_media",
            "headers": { "req_id": new_req_id("media") },
            "body": { "chat_id": chat_id, "media_type": media_type, "media_id": media_id }
        }))
    }

    // =======================================================================
    // Streamed reply event handling
    // =======================================================================

    #[allow(clippy::too_many_arguments)]
    async fn handle_stream_event(
        &self,
        event: MessageEvent,
        frame: &Value,
        stream_id: &str,
        constraints: &ChannelConstraints,
        content_buffer: &mut String,
        thinking_buffer: &mut String,
        media_buffer: &mut Vec<ContentPart>,
        stream_open: &mut bool,
    ) -> StreamAction {
        match event.event_type {
            MessageEventType::Typing => {}
            MessageEventType::ThinkingDelta => {
                if constraints.show_thinking {
                    for part in &event.content {
                        if let Some(t) = part.text_value() {
                            if !t.is_empty() {
                                thinking_buffer.push_str(t);
                            }
                        }
                    }
                    let text = constraints
                        .thinking_template
                        .replace("{content}", thinking_buffer.trim());
                    if self.reply_stream(frame, stream_id, &text, false).await.is_ok() {
                        *stream_open = true;
                    }
                }
            }
            MessageEventType::Flush => {
                thinking_buffer.clear();
            }
            MessageEventType::Delta => {
                for part in &event.content {
                    if let Some(t) = part.text_value() {
                        if !t.is_empty() {
                            content_buffer.push_str(t);
                        }
                    }
                }
                if !content_buffer.is_empty()
                    && self
                        .reply_stream(frame, stream_id, content_buffer, false)
                        .await
                        .is_ok()
                {
                    *stream_open = true;
                }
            }
            MessageEventType::ToolStart => {
                if constraints.show_tool_hints {
                    let hint = crate::constraints::tool_hint_message(
                        &event.metadata,
                        constraints,
                        "start",
                    );
                    if self.reply_stream(frame, stream_id, &hint, false).await.is_ok() {
                        *stream_open = true;
                    }
                }
            }
            MessageEventType::ToolEnd => {}
            MessageEventType::Error => {
                let text = event
                    .error
                    .clone()
                    .unwrap_or_else(|| "An unexpected error occurred.".to_string());
                let _ = self.reply_stream(frame, stream_id, &text, true).await;
                return StreamAction::Finish;
            }
            MessageEventType::Message => {
                let text = crate::utils::extract_text(&event.content);
                if !text.is_empty() {
                    content_buffer.push_str(&text);
                }
                media_buffer.extend(crate::utils::extract_media(&event.content));
            }
            MessageEventType::Completed => return StreamAction::Break,
            MessageEventType::Thinking => {}
        }
        StreamAction::Continue
    }
}

#[async_trait]
impl Channel for WeComChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> ChannelConstraints {
        ChannelConstraints {
            reply_timeout: 10.0,
            timeout_strategy: "placeholder".to_string(),
            send_rate_limit: Some((2, 5.0)),
            show_thinking: false,
            show_tool_hints: true,
            placeholder_texts: vec![
                "⏳ 思考中...".to_string(),
                "🤔 处理中...".to_string(),
                "💭 让我想想...".to_string(),
            ],
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        if self.config.bot_id.is_empty() || self.config.secret.is_empty() {
            return Err(GatewayError::Other(
                "WeComChannel: bot_id and secret are required".to_string(),
            ));
        }
        self.running.store(true, Ordering::SeqCst);

        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
        self.ws_tx = Some(tx);

        let ws_url = if self.config.ws_url.is_empty() {
            DEFAULT_WS_URL.to_string()
        } else {
            self.config.ws_url.clone()
        };
        let bot_id = self.config.bot_id.clone();
        let secret = self.config.secret.clone();
        let enqueue = self.core.enqueue_sender();
        let running = self.running.clone();
        let connected = self.connected.clone();
        let session_id = self.ws_session_id.clone();
        let pending = self.pending.clone();
        let task = tokio::spawn(async move {
            ws_client::run(
                ws_url, bot_id, secret, enqueue, running, connected, session_id, pending, rx,
            )
            .await;
        });
        self.ws_task = Some(task);

        let short: String = self.config.bot_id.chars().take(12).collect();
        tracing::info!("WeComChannel started (bot_id={})", short);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        self.ws_tx = None;
        self.connected.store(false, Ordering::SeqCst);
        *self.ws_session_id.lock().unwrap() = None;
        self.core.set_connection_session(None);
        tracing::info!("WeComChannel stopped");
        Ok(())
    }

    fn enrich_push_metadata(
        &self,
        subject: &crate::models::ChannelSubject,
        meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        crate::push_routing::alias_subject_fields(meta, &subject.subject_id, &["chat_id"])
    }

    async fn send_text(&mut self, subject: &crate::models::ChannelSubject, text: &str) -> Result<()> {
        let meta = subject.metadata.clone();
        let frame = meta.get("_frame").cloned();

        // Method 1: reply via the SDK stream reply.
        if self.ws_tx.is_some() {
            if let Some(frame) = frame.as_ref() {
                let stream_id = new_req_id("him");
                if self.reply_stream(frame, &stream_id, text, true).await.is_ok() {
                    let preview: String = text.chars().take(50).collect();
                    tracing::info!("WeComChannel sent via reply_stream: {}", preview);
                    return Ok(());
                }
                tracing::warn!("WeComChannel reply_stream failed, trying response_url");
            }
        }

        // Method 2: reply via `response_url` (HTTP webhook).
        let response_url = meta
            .get("response_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !response_url.is_empty() {
            let http = self.core.ensure_http().await?;
            let payload = json!({ "msgtype": "text", "text": { "content": text } });
            let resp = http
                .post(&response_url)
                .json(&payload)
                .timeout(Duration::from_secs(10))
                .send()
                .await
                .map_err(|e| {
                    GatewayError::Other(format!("WeCom response_url send failed: {e}"))
                })?;
            if resp.status().as_u16() == 200 {
                tracing::debug!("WeComChannel sent via response_url");
                return Ok(());
            }
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            tracing::error!(
                "WeComChannel response_url failed: {} {}",
                status,
                body.chars().take(200).collect::<String>()
            );
            return Err(GatewayError::Other(format!(
                "WeCom response_url failed: HTTP {status}"
            )));
        }

        // Method 3: proactive send (no frame context, e.g. cron).
        if self.ws_tx.is_none() {
            tracing::warn!("WeComChannel: no ws_client available for proactive send");
            return Err(GatewayError::Other(
                "WeComChannel: no ws_client available for proactive send".to_string(),
            ));
        }
        let chat_id = meta
            .get("chat_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| subject.subject_id.clone());
        if chat_id.is_empty() {
            tracing::warn!("WeComChannel: no chat_id or subject_id for proactive send");
            return Err(GatewayError::Other(
                "WeComChannel: no chat_id or subject_id for proactive send".to_string(),
            ));
        }
        self.send_message(&chat_id, text).await.map_err(|e| {
            GatewayError::Other(format!("WeCom send_message failed: {e}"))
        })
    }

    async fn send_content(
        &mut self,
        subject: &crate::models::ChannelSubject,
        parts: &[ContentPart],
    ) -> Result<()> {
        let text = crate::utils::extract_text(parts);
        if !text.is_empty() {
            self.send_text(subject, &text).await?;
        }
        for media in crate::utils::extract_media(parts) {
            self.send_media(subject, &media).await?;
        }
        Ok(())
    }

    async fn send_media(
        &mut self,
        subject: &crate::models::ChannelSubject,
        media: &ContentPart,
    ) -> Result<()> {
        if self.ws_tx.is_none() {
            return Err(GatewayError::Other(
                "WeComChannel: no ws_client available for media send".to_string(),
            ));
        }

        let (media_type, filename) = match wecom_media_type_and_filename(media) {
            Ok(x) => x,
            Err(e) => {
                let url = media.url().unwrap_or("");
                let label = media.label_with_filename();
                if !url.is_empty() {
                    let _ = self.send_text(subject, &format!("[{label}: {url}]")).await;
                } else {
                    let _ = self
                        .send_text(subject, &format!("[{label} (native upload failed)]"))
                        .await;
                }
                tracing::warn!("WeComChannel unsupported media: {}", e);
                return Ok(());
            }
        };

        if let Err(e) = self
            .send_media_native(subject, media, &media_type, &filename)
            .await
        {
            tracing::error!("WeComChannel native media send failed: {}", e);
            let url = media.url().unwrap_or("");
            let label = media.label_with_filename();
            if !url.is_empty() {
                let _ = self.send_text(subject, &format!("[{label}: {url}]")).await;
            } else {
                let _ = self
                    .send_text(subject, &format!("[{label} (native upload failed)]"))
                    .await;
            }
        }
        Ok(())
    }

    /// Override: decrypt+persist inbound media through the SDK (`aeskey` applied).
    async fn persist_media(&mut self, message: &mut InboundMessage) -> Result<()> {
        if self.core.media_backend().is_none() {
            return Ok(());
        }

        let aes_keys: HashMap<String, String> = message
            .metadata
            .get("_media_aes_keys")
            .and_then(|v| v.as_object())
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let has_ws = self.ws_tx.is_some();
        let timestamp = crate::channel::now_secs();
        let channel_type = self.core.channel_type().to_string();
        let channel_id = self.core.channel_id().to_string();

        for (index, part) in message.content.iter_mut().enumerate() {
            if part.is_text() || part.local_path().is_some() {
                continue;
            }
            let url = match part.url() {
                Some(u) if !u.is_empty() => u.to_string(),
                _ => continue,
            };

            let outcome: Result<()> = async {
                let (data, mime) = if has_ws {
                    if let Some(aes_key) = aes_keys.get(&url) {
                        let (data, downloaded_name) = self.download_file(&url, aes_key).await?;
                        if let ContentPart::File(f) = part {
                            if f.filename.is_empty() {
                                if let Some(name) = &downloaded_name {
                                    f.filename = name.clone();
                                }
                            }
                        }
                        let original_name = match part {
                            ContentPart::File(f) => f.filename.clone(),
                            _ => String::new(),
                        };
                        let guess_src = downloaded_name.unwrap_or(original_name);
                        let guessed = if guess_src.is_empty() {
                            mime_guess::from_path(&url)
                                .first()
                                .map(|m| m.to_string())
                        } else {
                            mime_guess::from_path(&guess_src)
                                .first()
                                .map(|m| m.to_string())
                        };
                        let mime = part
                            .mime_type()
                            .map(|s| s.to_string())
                            .or(guessed)
                            .unwrap_or_else(|| "application/octet-stream".to_string());
                        (data, mime)
                    } else {
                        self.fetch_remote_media(&url).await?
                    }
                } else {
                    self.fetch_remote_media(&url).await?
                };

                let filename = resolve_media_filename(part, index, timestamp);
                let key = format!("{channel_type}/{channel_id}/{filename}");
                let backend = self.core.media_backend().unwrap();
                backend.save(&data, &key).await?;
                set_part_local(part, key.clone(), data.len() as u64, mime);
                tracing::info!(
                    "WeCom media decrypted and persisted: {} bytes -> key={}",
                    data.len(),
                    key
                );
                Ok(())
            }
            .await;

            if let Err(e) = outcome {
                tracing::warn!(
                    "Failed to decrypt/persist WeCom media: url={} error={}",
                    url.chars().take(100).collect::<String>(),
                    e
                );
            }
        }
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let data = match raw.as_json() {
            Some(v) => v.clone(),
            None => Value::Null,
        };

        // Extract sender info.
        let from = data.get("from");
        let sender_id = from
            .and_then(|f| f.get("userid"))
            .and_then(|x| x.as_str())
            .or_else(|| from.and_then(|f| f.get("user_id")).and_then(|x| x.as_str()))
            .unwrap_or("unknown")
            .to_string();
        let chat_type = data
            .get("chattype")
            .and_then(|x| x.as_str())
            .unwrap_or("single")
            .to_string();
        let chat_id = data
            .get("chatid")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let session_id = self
            .ws_session_id
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "wecom-unconnected".to_string());

        let mut content_parts: Vec<ContentPart> = Vec::new();
        let mut media_aes_keys: HashMap<String, String> = HashMap::new();
        let msg_type = data.get("msgtype").and_then(|x| x.as_str()).unwrap_or("text");

        match msg_type {
            "text" => {
                let text_obj = data.get("text");
                let text = match text_obj {
                    Some(Value::Object(o)) => o
                        .get("content")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    Some(other) => other.as_str().unwrap_or("").to_string(),
                    None => String::new(),
                };
                if !text.is_empty() {
                    content_parts.push(ContentPart::text(text));
                }
            }
            "image" => {
                let obj = data.get("image");
                let url = obj
                    .and_then(|o| o.get("url"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !url.is_empty() {
                    content_parts.push(ContentPart::Image(crate::models::ImageContent {
                        url: url.to_string(),
                        ..Default::default()
                    }));
                    remember_aes_key(obj, url, &mut media_aes_keys);
                }
            }
            "file" => {
                let obj = data.get("file");
                let url = obj
                    .and_then(|o| o.get("url"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                let filename = obj
                    .and_then(|o| o.get("file_name"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !url.is_empty() {
                    content_parts.push(ContentPart::File(crate::models::FileContent {
                        url: url.to_string(),
                        filename: filename.to_string(),
                        ..Default::default()
                    }));
                    remember_aes_key(obj, url, &mut media_aes_keys);
                }
            }
            "voice" => {
                let obj = data.get("voice");
                let url = obj
                    .and_then(|o| o.get("url"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !url.is_empty() {
                    content_parts.push(ContentPart::Audio(crate::models::AudioContent {
                        url: url.to_string(),
                        ..Default::default()
                    }));
                    remember_aes_key(obj, url, &mut media_aes_keys);
                }
            }
            "video" => {
                let obj = data.get("video");
                let url = obj
                    .and_then(|o| o.get("url"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("");
                if !url.is_empty() {
                    content_parts.push(ContentPart::Video(crate::models::VideoContent {
                        url: url.to_string(),
                        ..Default::default()
                    }));
                    remember_aes_key(obj, url, &mut media_aes_keys);
                }
            }
            "mixed" => {
                let items = data
                    .get("mixed")
                    .and_then(|m| m.get("msg_item"))
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();
                for item in items {
                    let item_type = item.get("msgtype").and_then(|x| x.as_str()).unwrap_or("");
                    match item_type {
                        "text" => {
                            let text = item
                                .get("text")
                                .and_then(|t| t.get("content"))
                                .and_then(|x| x.as_str())
                                .unwrap_or("");
                            if !text.is_empty() {
                                content_parts.push(ContentPart::text(text));
                            }
                        }
                        "image" => {
                            let image_obj = item.get("image");
                            let url = image_obj
                                .and_then(|o| o.get("url"))
                                .and_then(|x| x.as_str())
                                .unwrap_or("");
                            if !url.is_empty() {
                                content_parts.push(ContentPart::Image(
                                    crate::models::ImageContent {
                                        url: url.to_string(),
                                        ..Default::default()
                                    },
                                ));
                                remember_aes_key(image_obj, url, &mut media_aes_keys);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                // Fallback: try the text field.
                let text = match data.get("text") {
                    Some(Value::Object(o)) => o
                        .get("content")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                    Some(Value::String(s)) => s.clone(),
                    _ => data
                        .get("content")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string(),
                };
                if !text.is_empty() {
                    content_parts.push(ContentPart::text(text));
                }
            }
        }

        if content_parts.is_empty() {
            content_parts.push(ContentPart::text(""));
        }

        let frame = data.get("_frame").cloned().unwrap_or_else(|| data.clone());
        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("to_handle".to_string(), json!(sender_id));
        metadata.insert("chat_id".to_string(), json!(chat_id));
        metadata.insert("chat_type".to_string(), json!(chat_type));
        metadata.insert(
            "msgid".to_string(),
            json!(data.get("msgid").and_then(|x| x.as_str()).unwrap_or("")),
        );
        metadata.insert(
            "response_url".to_string(),
            json!(data
                .get("response_url")
                .and_then(|x| x.as_str())
                .unwrap_or("")),
        );
        metadata.insert("_media_aes_keys".to_string(), json!(media_aes_keys));
        metadata.insert("_frame".to_string(), frame);

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "wecom".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(crate::models::ChannelSubject {
                subject_id: sender_id,
                first_seen: None,
                last_seen: None,
                display_name: String::new(),
                chat_type: String::new(),
                metadata: HashMap::new(),
            }),
            channel_session_id: session_id,
            content: content_parts,
            group_context: None,
            metadata,
            timestamp: crate::channel::now_f64(),
        })
    }

    /// Override: stream deltas progressively via WeCom `reply_stream`.
    ///
    /// Streamed reply pipeline. When no frame /
    /// WebSocket is available it falls back to the shared base pipeline
    /// (`prepare_inbound` → `process_inbound` → `finalize_inbound`), exactly as
    /// calls `super().handle_inbound(...)`.
    async fn handle_inbound(&mut self, raw: &RawPayload) -> Result<()> {
        let mut message = self.parse_inbound(raw)?;

        // Auto-persist media if a backend is configured.
        if self.core.media_backend().is_some() {
            let _ = self.persist_media(&mut message).await;
        }
        self.core_mut().track_subject(&message);

        let meta = message.metadata.clone();
        let frame = meta.get("_frame").cloned();

        // If no frame / ws_client is available, fall back to base behavior.
        if self.ws_tx.is_none() || frame.is_none() {
            let prepared = self.prepare_inbound(raw).await?;
            if let Some((m, subject)) = prepared {
                let ok = self.process_inbound(&m, &subject).await;
                self.finalize_inbound(&m, ok);
            }
            return Ok(());
        }
        let frame = frame.unwrap();

        // One rate-limit slot for the whole stream reply.
        self.acquire_rate_slot().await;

        let processor = match self.core().processor() {
            Some(p) => p,
            None => return Ok(()),
        };
        let constraints = self.core().constraints().clone();
        let stream_id = new_req_id("him");
        let mut stream = processor(message.clone());

        let mut content_buffer = String::new();
        let mut thinking_buffer = String::new();
        let mut media_buffer: Vec<ContentPart> = Vec::new();
        let mut stream_open = false;

        // First-token timeout handling: if the first event does not arrive in
        // `reply_timeout * 0.8` seconds, stream a placeholder and keep waiting.
        let mut first_event: Option<MessageEvent> = None;
        let mut stream_exhausted = false;
        if constraints.reply_timeout > 0.0 && constraints.timeout_strategy == "placeholder" {
            let dur = Duration::from_secs_f64(constraints.reply_timeout * 0.8);
            let outcome = tokio::time::timeout(dur, stream.next()).await;
            match outcome {
                Ok(Some(ev)) => first_event = Some(ev),
                Ok(None) => stream_exhausted = true,
                Err(_) => {
                    let placeholder = constraints.placeholder_text();
                    if !placeholder.is_empty()
                        && self
                            .reply_stream(&frame, &stream_id, &placeholder, false)
                            .await
                            .is_ok()
                    {
                        stream_open = true;
                    }
                    match stream.next().await {
                        Some(ev) => first_event = Some(ev),
                        None => stream_exhausted = true,
                    }
                }
            }
        }
        if stream_exhausted {
            // Nothing was delivered; release the producer explicitly instead of
            // waiting for an event that will never come.
            crate::utils::close_event_stream(stream).await;
            return Ok(());
        }

        let mut action = StreamAction::Continue;
        if let Some(ev) = first_event.take() {
            action = self
                .handle_stream_event(
                    ev,
                    &frame,
                    &stream_id,
                    &constraints,
                    &mut content_buffer,
                    &mut thinking_buffer,
                    &mut media_buffer,
                    &mut stream_open,
                )
                .await;
        }
        while matches!(action, StreamAction::Continue) {
            match stream.next().await {
                Some(ev) => {
                    action = self
                        .handle_stream_event(
                            ev,
                            &frame,
                            &stream_id,
                            &constraints,
                            &mut content_buffer,
                            &mut thinking_buffer,
                            &mut media_buffer,
                            &mut stream_open,
                        )
                        .await;
                }
                None => action = StreamAction::Break,
            }
        }
        if matches!(action, StreamAction::Finish) {
            // `Finish` already replied; release the producer explicitly.
            crate::utils::close_event_stream(stream).await;
            return Ok(());
        }

        // Close a text stream before replying with native media messages.
        let final_text = content_buffer.clone();
        if !final_text.is_empty() || stream_open || media_buffer.is_empty() {
            let content = if final_text.is_empty() {
                "✅".to_string()
            } else {
                final_text.clone()
            };
            let _ = self.reply_stream(&frame, &stream_id, &content, true).await;
        }

        if !media_buffer.is_empty() {
            let subject_id = message
                .channel_subject
                .as_ref()
                .map(|s| s.subject_id.clone())
                .unwrap_or_default();
            let mut media_meta = meta.clone();
            if !final_text.is_empty() || stream_open {
                // The callback frame already completed its stream reply; send
                // subsequent media proactively to the same chat.
                media_meta.remove("_frame");
            }
            let reply_subject = crate::models::ChannelSubject {
                subject_id,
                first_seen: None,
                last_seen: None,
                display_name: String::new(),
                chat_type: String::new(),
                metadata: media_meta,
            };
            for media in &media_buffer {
                let _ = self.send_media(&reply_subject, media).await;
            }
        }
        crate::utils::close_event_stream(stream).await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a request id with a prefix. `generate_req_id("him")` analogue.
fn new_req_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

/// Record a media AES key under its URL when present.
fn remember_aes_key(obj: Option<&Value>, url: &str, out: &mut HashMap<String, String>) {
    if url.is_empty() {
        return;
    }
    let key = obj
        .and_then(|o| o.get("aeskey").or_else(|| o.get("aes_key")))
        .and_then(|x| x.as_str());
    if let Some(k) = key {
        if !k.is_empty() {
            out.insert(url.to_string(), k.to_string());
        }
    }
}

/// Map a `ContentPart` to `(wecom_media_type, filename)`.
fn wecom_media_type_and_filename(media: &ContentPart) -> Result<(String, String)> {
    let (media_type, default_name) = match media {
        ContentPart::Image(_) => ("image", "image.png"),
        ContentPart::Audio(_) => ("voice", "voice.amr"),
        ContentPart::Video(_) => ("video", "video.mp4"),
        ContentPart::File(_) => ("file", "file.bin"),
        ContentPart::Text(_) => {
            return Err(GatewayError::Other(
                "Unsupported WeCom media type: Text".to_string(),
            ))
        }
    };

    let mut filename = media.filename().unwrap_or("").to_string();
    if filename.is_empty() {
        if let Some(mime) = media.mime_type() {
            if let Some(ext) = guess_extension(mime) {
                filename = format!("{media_type}{ext}");
            }
        }
    }
    let filename = if filename.is_empty() {
        default_name.to_string()
    } else {
        filename
    };
    Ok((media_type.to_string(), filename))
}

/// Extension lookup for common media types.
fn guess_extension(mime: &str) -> Option<String> {
    let m = mime.split(';').next().unwrap_or("").trim().to_lowercase();
    let ext = match m.as_str() {
        "image/png" => ".png",
        "image/jpeg" | "image/jpg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/bmp" => ".bmp",
        "audio/amr" => ".amr",
        "audio/mpeg" => ".mp3",
        "audio/wav" | "audio/x-wav" => ".wav",
        "audio/ogg" => ".ogg",
        "video/mp4" => ".mp4",
        "video/quicktime" => ".mov",
        "application/pdf" => ".pdf",
        "application/zip" => ".zip",
        _ => return None,
    };
    Some(ext.to_string())
}

fn mime_to_ext(mime: &str, default: &str) -> String {
    let m = mime.split(';').next().unwrap_or("").trim().to_lowercase();
    let ext = match m.as_str() {
        "image/png" => ".png",
        "image/jpeg" | "image/jpg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "video/mp4" => ".mp4",
        "audio/mpeg" => ".mp3",
        "audio/amr" => ".amr",
        "application/pdf" => ".pdf",
        _ => default,
    };
    ext.to_string()
}

fn md5_short(s: &str) -> String {
    let digest = md5::compute(s.as_bytes());
    let hex = format!("{digest:x}");
    hex.chars().take(8).collect()
}

/// Storage-safe basename retaining Unicode.
fn safe_media_filename(filename: &str) -> String {
    let basename = filename.replace('\\', "/");
    let basename = basename.rsplit('/').next().unwrap_or("").to_string();
    let cleaned: String = basename
        .chars()
        .map(|c| if c.is_control() { '_' } else { c })
        .collect();
    let trimmed = cleaned.trim().trim_matches(|c| c == ' ' || c == '.').to_string();
    if trimmed.is_empty() {
        "attachment".to_string()
    } else {
        trimmed
    }
}
fn resolve_media_filename(part: &ContentPart, index: usize, timestamp: u64) -> String {
    if let ContentPart::File(f) = part {
        if !f.filename.is_empty() {
            let name = safe_media_filename(&f.filename);
            return format!("{timestamp}_{index}_{name}");
        }
    }
    if let Some(url) = part.url() {
        if !url.is_empty() {
            let ext = match part {
                ContentPart::Image(_) => mime_to_ext(part.mime_type().unwrap_or(""), ".png"),
                ContentPart::Video(_) => mime_to_ext(part.mime_type().unwrap_or(""), ".mp4"),
                ContentPart::Audio(_) => mime_to_ext(part.mime_type().unwrap_or(""), ".mp3"),
                _ => mime_to_ext(part.mime_type().unwrap_or(""), ".bin"),
            };
            let hash = md5_short(url);
            return format!("{timestamp}_{hash}{ext}");
        }
    }
    let hash = md5_short(&index.to_string());
    format!("{timestamp}_{hash}.bin")
}

/// Stamp persisted-media facts onto a content part.
fn set_part_local(part: &mut ContentPart, key: String, size: u64, mime: String) {
    match part {
        ContentPart::Image(x) => {
            x.local_path = Some(key);
            x.size = Some(size);
            if x.mime_type.is_none() {
                x.mime_type = Some(mime);
            }
        }
        ContentPart::Video(x) => {
            x.local_path = Some(key);
            x.size = Some(size);
            if x.mime_type.is_none() {
                x.mime_type = Some(mime);
            }
        }
        ContentPart::Audio(x) => {
            x.local_path = Some(key);
            x.size = Some(size);
            if x.mime_type.is_none() {
                x.mime_type = Some(mime);
            }
        }
        ContentPart::File(x) => {
            x.local_path = Some(key);
            x.size = Some(size);
            if x.mime_type.is_none() {
                x.mime_type = Some(mime);
            }
        }
        ContentPart::Text(_) => {}
    }
}

// ---------------------------------------------------------------------------
// WebSocket client (wecom-aibot-sdk protocol, inlined)
// ---------------------------------------------------------------------------

mod ws_client {
    use super::*;
    use crate::channel::EnqueueFn;
    use futures::SinkExt;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio_tungstenite::tungstenite::Message;

    /// Infinite reconnect loop (`max_reconnect_attempts=-1`).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run(
        ws_url: String,
        bot_id: String,
        secret: String,
        enqueue: Option<EnqueueFn>,
        running: Arc<AtomicBool>,
        connected: Arc<AtomicBool>,
        session_id: Arc<StdMutex<Option<String>>>,
        pending: Arc<AsyncMutex<HashMap<String, oneshot::Sender<Value>>>>,
        mut out_rx: UnboundedReceiver<Value>,
    ) {
        let mut backoff = 1.0f64;
        loop {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            match tokio_tungstenite::connect_async(&ws_url).await {
                Ok((ws, _)) => {
                    backoff = 1.0;
                    if let Err(e) = session_loop(
                        ws,
                        &bot_id,
                        &secret,
                        &enqueue,
                        &running,
                        &connected,
                        &session_id,
                        &pending,
                        &mut out_rx,
                    )
                    .await
                    {
                        tracing::warn!("WeCom ws loop ended: {}", e);
                    }
                }
                Err(e) => tracing::warn!("WeCom ws connect failed: {}", e),
            }
            connected.store(false, Ordering::SeqCst);
            *session_id.lock().unwrap() = None;
            if !running.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(60.0);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn session_loop(
        ws: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        bot_id: &str,
        secret: &str,
        enqueue: &Option<EnqueueFn>,
        running: &Arc<AtomicBool>,
        connected: &Arc<AtomicBool>,
        session_id: &Arc<StdMutex<Option<String>>>,
        pending: &Arc<AsyncMutex<HashMap<String, oneshot::Sender<Value>>>>,
        out_rx: &mut UnboundedReceiver<Value>,
    ) -> Result<()> {
        let (mut write, mut read) = ws.split();

        // Connect frame: bot_id + secret → authenticated.
        let subscribe = json!({
            "cmd": "aibot_subscribe",
            "headers": { "req_id": new_req_id("sub") },
            "body": { "bot_id": bot_id, "secret": secret }
        });
        write
            .send(Message::Text(subscribe.to_string().into()))
            .await
            .map_err(|e| GatewayError::Other(e.to_string()))?;

        // Heartbeat every 30000 ms.
        let mut heartbeat = tokio::time::interval(Duration::from_millis(30000));
        heartbeat.tick().await; // consume the immediate first tick

        loop {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            tokio::select! {
                maybe = read.next() => {
                    let msg = match maybe {
                        Some(Ok(m)) => m,
                        _ => break,
                    };
                    match msg {
                        Message::Text(t) => {
                            handle_frame(&t, enqueue, connected, session_id, pending).await;
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
                Some(cmd) = out_rx.recv() => {
                    let _ = write.send(Message::Text(cmd.to_string().into())).await;
                }
                _ = heartbeat.tick() => {
                    let ping = json!({
                        "cmd": "ping",
                        "headers": { "req_id": new_req_id("hb") },
                        "body": {}
                    });
                    let _ = write.send(Message::Text(ping.to_string().into())).await;
                }
            }
        }

        let _ = write.close().await;
        Ok(())
    }

    async fn handle_frame(
        txt: &str,
        enqueue: &Option<EnqueueFn>,
        connected: &Arc<AtomicBool>,
        session_id: &Arc<StdMutex<Option<String>>>,
        pending: &Arc<AsyncMutex<HashMap<String, oneshot::Sender<Value>>>>,
    ) {
        let v: Value = match serde_json::from_str(txt) {
            Ok(v) => v,
            Err(_) => return,
        };

        // Resolve a pending request/response first.
        let req_id = v
            .pointer("/headers/req_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if !req_id.is_empty() {
            if let Some(tx) = pending.lock().await.remove(&req_id) {
                let _ = tx.send(v.clone());
            }
        }

        let cmd = v.get("cmd").and_then(|x| x.as_str()).unwrap_or("");
        match cmd {
            "aibot_subscribe" | "aibot_auth" => {
                let errcode = v
                    .pointer("/body/errcode")
                    .and_then(|x| x.as_i64())
                    .unwrap_or(0);
                if errcode == 0 {
                    connected.store(true, Ordering::SeqCst);
                    let sid = uuid::Uuid::new_v4().to_string();
                    *session_id.lock().unwrap() = Some(sid.clone());
                    tracing::info!("WeComChannel WebSocket authenticated (session={})", sid);
                } else {
                    tracing::error!("WeComChannel WebSocket auth failed: {}", v);
                }
            }
            "aibot_msg_callback" => {
                let body = v.get("body").cloned().unwrap_or_else(|| json!({}));
                let msg_type = body.get("msgtype").and_then(|x| x.as_str()).unwrap_or("");
                if !MSG_TYPE_WHITELIST.contains(&msg_type) {
                    tracing::debug!("WeComChannel ignoring msgtype={}", msg_type);
                    return;
                }
                let mut payload = body;
                payload["_frame"] = v.clone();
                if let Some(cb) = enqueue {
                    cb(RawPayload::Json(payload));
                }
            }
            _ => tracing::debug!("WeComChannel ignoring cmd={}", cmd),
        }
    }
}
