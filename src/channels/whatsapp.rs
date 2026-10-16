//! WhatsApp Cloud API channel — webhook inbound + Graph API outbound.
//!
//! Unlike the long-connection platforms, WhatsApp *posts* every inbound event to
//! a public HTTPS endpoint, so this channel pairs with
//! [`crate::webhook::WebhookServer`]:
//!
//! * `GET` with `hub.mode` / `hub.verify_token` / `hub.challenge` — Meta's
//!   subscription handshake, answered by echoing the challenge.
//! * `POST` carrying `entry[].changes[].value.messages[]`, protected by
//!   `X-Hub-Signature-256` (HMAC-SHA256 over the **raw** body with the app
//!   secret).
//!
//! Media never arrives inline. Each attachment is referenced by a `media_id`
//! that expires, and downloading it is a two-step dance (resolve the id to a
//! short-lived URL, then fetch that URL with the same bearer token). Both steps
//! happen in [`Channel::preprocess_inbound`] so the standard media-persistence
//! path still runs.
//!
//! Outbound goes through `POST /{version}/{phone_number_id}/messages`, with a
//! preceding `POST /{phone_number_id}/media` upload for anything that is not
//! text. Note Meta's 24-hour customer-service window: outside it only
//! pre-approved template messages are accepted, and the API returns an error
//! that this channel surfaces verbatim.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::channel::{Channel, ChannelConfig, ChannelCore};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, VideoContent,
};
use crate::webhook::{
    WebhookAdapter, WebhookOutcome, WebhookRequest, WebhookResponse,
    verify_sha256_hex_signature,
};

const DEFAULT_API_VERSION: &str = "v22.0";
const DEFAULT_GRAPH_BASE: &str = "https://graph.facebook.com";
/// Cloud API text body limit.
const TEXT_CHUNK: usize = 4096;
/// Marker scheme for an unresolved `media_id`.
const MEDIA_SCHEME: &str = "whatsapp-media://";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// WhatsApp Cloud API configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WhatsAppConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// The sender phone number id (not the display number).
    pub phone_number_id: String,
    /// Permanent system-user access token.
    pub access_token: String,
    /// The token echoed back during the `hub.verify_token` handshake.
    pub verify_token: String,
    /// App secret used to validate `X-Hub-Signature-256`.
    pub app_secret: String,
    /// Graph API version, e.g. `v22.0`.
    pub api_version: Option<String>,
    /// Graph API base, for on-premise/region-pinned deployments.
    pub graph_base: Option<String>,
    /// Optional HTTP proxy for Graph calls.
    pub http_proxy: String,
    /// Optional `user:pass` for the proxy.
    pub http_proxy_auth: String,
    /// Wa ids allowed to talk to the bot (empty = everyone).
    #[serde(default, deserialize_with = "crate::channels::string_or_vec")]
    pub allowed_user_ids: Vec<String>,
}

impl WhatsAppConfig {
    fn version(&self) -> String {
        self.api_version
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_API_VERSION)
            .to_string()
    }

    fn base_url(&self) -> String {
        self.graph_base
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_GRAPH_BASE)
            .trim_end_matches('/')
            .to_string()
    }
}

crate::channel_adapter! {
    /// WhatsApp Cloud API channel adapter.
    WhatsAppChannel, WhatsAppConfig, "whatsapp",
    required = [phone_number_id, access_token, verify_token, app_secret],
    fields = {}
}

// ---------------------------------------------------------------------------
// Small LRU (kept local so each adapter stays self-contained)
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
// Channel
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for WhatsAppChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            // Meta publishes a throughput limit on the phone number; stay well
            // inside it and let the pipeline coalesce deltas.
            send_rate_limit: Some((20, 60.0)),
            reply_timeout: 0.0,
            typing_keepalive_interval: 0.0,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        // Validate the token eagerly so `probe_channel` reports bad credentials
        // instead of failing much later on the first send.
        let http = self.build_http()?;
        let url = format!(
            "{}/{}/{}?fields=display_phone_number,verified_name",
            self.config.base_url(),
            self.config.version(),
            self.config.phone_number_id
        );
        let resp = http
            .get(&url)
            .bearer_auth(&self.config.access_token)
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(GatewayError::Other(format!(
                "whatsapp: credential check failed ({status}): {}",
                error_message(&body)
            )));
        }
        tracing::info!(
            "WhatsApp phone number ready: {} ({})",
            body.get("display_phone_number")
                .and_then(|v| v.as_str())
                .unwrap_or("-"),
            body.get("verified_name")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
        );
        self.core
            .set_connection_session(Some(self.config.phone_number_id.clone()));
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let to = destination(subject)?;
        let http = self.build_http()?;
        for chunk in chunk_text(&markdown_to_whatsapp(text), TEXT_CHUNK) {
            let payload = json!({
                "messaging_product": "whatsapp",
                "recipient_type": "individual",
                "to": to,
                "type": "text",
                "text": { "preview_url": false, "body": chunk },
            });
            self.post_message(&http, payload).await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let to = destination(subject)?;
        if media.is_text() {
            return Ok(());
        }
        let (bytes, mime) = match self.load_media_bytes(media).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("WhatsApp media load failed: {}", e);
                let label = media.label_with_filename();
                return self
                    .send_text(subject, &format!("[{label}: upload failed]"))
                    .await;
            }
        };

        let http = self.build_http()?;
        let media_id = self.upload_media(&http, &bytes, &mime, media).await?;
        let (kind, body) = media_message(media, &mime, &media_id);
        let payload = json!({
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": kind,
            kind: body,
        });
        self.post_message(&http, payload).await
    }

    async fn preprocess_inbound(&mut self, message: &mut InboundMessage) -> Result<()> {
        // Resolve every `whatsapp-media://<id>` marker into stored bytes. This
        // runs before `persist_media`, so the standard path then skips the part.
        let mut index = 0usize;
        for part in message.content.iter_mut() {
            let Some(id) = part
                .url()
                .and_then(|u| u.strip_prefix(MEDIA_SCHEME))
                .map(|s| s.to_string())
            else {
                index += 1;
                continue;
            };
            match self.download_media(&id, part.mime_type()).await {
                Ok((bytes, mime)) => {
                    let filename = format!(
                        "{}_{}{}",
                        crate::channel::now_secs(),
                        index,
                        extension_for(&mime, part.filename())
                    );
                    let key = format!(
                        "whatsapp/{}/{}",
                        self.core.channel_id(),
                        filename
                    );
                    match self.core.media_backend() {
                        Some(backend) => match backend.save(&bytes, &key).await {
                            Ok(()) => set_media_meta(part, key, bytes.len() as u64, mime),
                            Err(e) => tracing::warn!("whatsapp media store failed: {}", e),
                        },
                        None => tracing::debug!("whatsapp: no media backend; media kept remote"),
                    }
                    // The resolved URL is short-lived; drop it so nothing else
                    // tries to reuse it after the 5-minute TTL.
                    clear_url(part);
                }
                Err(e) => {
                    tracing::warn!("whatsapp media download failed: id={} error={}", id, e);
                }
            }
            index += 1;
        }
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("whatsapp expects a JSON payload".into()))?;
        let wa_id = v
            .get("wa_id")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| GatewayError::Other("whatsapp payload without wa_id".into()))?;
        let message_id = v
            .get("message_id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let message_type = v
            .get("message_type")
            .and_then(|x| x.as_str())
            .unwrap_or("text")
            .to_string();
        let sender_name = v
            .get("sender_name")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();

        let mut content = Vec::new();
        if let Some(caption) = v.get("caption").and_then(|x| x.as_str()) {
            if !caption.is_empty() {
                content.push(ContentPart::text(caption));
            }
        }
        if let Some(text) = v.get("text").and_then(|x| x.as_str()) {
            if !text.is_empty() {
                content.push(ContentPart::text(text));
            }
        }
        if let Some(files) = v.get("files").and_then(|f| f.as_array()) {
            for f in files {
                if let Some(part) = media_to_part(f) {
                    content.push(part);
                }
            }
        }

        let mut subject = ChannelSubject::new(wa_id);
        subject.display_name = if sender_name.is_empty() {
            wa_id.to_string()
        } else {
            sender_name.clone()
        };
        subject.chat_type = "user".to_string();
        subject
            .metadata
            .insert("to_handle".into(), Value::String(wa_id.to_string()));

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("message_id".into(), Value::String(message_id));
        metadata.insert("to_handle".into(), Value::String(wa_id.to_string()));
        metadata.insert("wa_id".into(), Value::String(wa_id.to_string()));
        metadata.insert("message_type".into(), Value::String(message_type));
        if !sender_name.is_empty() {
            metadata.insert("sender_name".into(), Value::String(sender_name));
        }
        if let Some(pid) = v.get("phone_number_id").and_then(|x| x.as_str()) {
            metadata.insert("phone_number_id".into(), Value::String(pid.to_string()));
        }
        if let Some(waba) = v.get("waba_id").and_then(|x| x.as_str()) {
            metadata.insert("waba_id".into(), Value::String(waba.to_string()));
        }

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "whatsapp".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(subject),
            channel_session_id: wa_id.to_string(),
            content,
            group_context: None,
            metadata,
            timestamp: v
                .get("timestamp")
                .and_then(|t| t.as_str())
                .and_then(|t| t.parse::<f64>().ok())
                .unwrap_or_else(crate::channel::now_f64),
        })
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
        meta
    }
}

impl WhatsAppChannel {
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
            .map_err(|e| GatewayError::Other(format!("whatsapp http client: {e}")))
    }

    fn messages_url(&self) -> String {
        format!(
            "{}/{}/{}/messages",
            self.config.base_url(),
            self.config.version(),
            self.config.phone_number_id
        )
    }

    async fn post_message(&self, http: &reqwest::Client, payload: Value) -> Result<()> {
        let resp = http
            .post(self.messages_url())
            .bearer_auth(&self.config.access_token)
            .json(&payload)
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(());
        }
        Err(GatewayError::Other(format!(
            "whatsapp send failed ({status}): {}",
            error_message(&body)
        )))
    }

    /// Upload bytes and return the resulting `media_id`.
    async fn upload_media(
        &self,
        http: &reqwest::Client,
        bytes: &[u8],
        mime: &str,
        media: &ContentPart,
    ) -> Result<String> {
        let filename = media
            .filename()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .unwrap_or_else(|| format!("upload{}", extension_for(mime, None)));
        let part = reqwest::multipart::Part::bytes(bytes.to_vec())
            .file_name(filename)
            .mime_str(mime)
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(bytes.to_vec()));
        let form = reqwest::multipart::Form::new()
            .text("messaging_product", "whatsapp")
            .text("type", mime.to_string())
            .part("file", part);
        let url = format!(
            "{}/{}/{}/media",
            self.config.base_url(),
            self.config.version(),
            self.config.phone_number_id
        );
        let resp = http
            .post(&url)
            .bearer_auth(&self.config.access_token)
            .multipart(form)
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(GatewayError::Other(format!(
                "whatsapp media upload failed ({status}): {}",
                error_message(&body)
            )));
        }
        body.get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| GatewayError::Other("whatsapp media upload returned no id".into()))
    }

    /// Two-step download: resolve the id to a signed URL, then fetch it.
    async fn download_media(
        &self,
        media_id: &str,
        hint_mime: Option<&str>,
    ) -> Result<(Vec<u8>, String)> {
        let http = self.build_http()?;
        let meta_url = format!(
            "{}/{}/{}",
            self.config.base_url(),
            self.config.version(),
            media_id
        );
        let resp = http
            .get(&meta_url)
            .bearer_auth(&self.config.access_token)
            .send()
            .await?;
        let status = resp.status();
        let meta: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(GatewayError::Other(format!(
                "whatsapp media lookup failed ({status}): {}",
                error_message(&meta)
            )));
        }
        let url = meta
            .get("url")
            .and_then(|u| u.as_str())
            .ok_or_else(|| GatewayError::Other("whatsapp media lookup returned no url".into()))?;
        let mime = meta
            .get("mime_type")
            .and_then(|m| m.as_str())
            .or(hint_mime)
            .unwrap_or("application/octet-stream")
            .to_string();

        let resp = http
            .get(url)
            .bearer_auth(&self.config.access_token)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "whatsapp media download failed: {}",
                resp.status()
            )));
        }
        let bytes = resp.bytes().await?;
        Ok((bytes.to_vec(), mime))
    }
}

// ---------------------------------------------------------------------------
// Webhook adapter
// ---------------------------------------------------------------------------

/// Verifies Meta's handshake/signature and translates inbound events.
pub struct WhatsAppWebhook {
    config: Arc<WhatsAppConfig>,
    dedup: StdMutex<LruSet>,
}

impl WhatsAppWebhook {
    pub fn new(config: WhatsAppConfig) -> Self {
        WhatsAppWebhook {
            config: Arc::new(config),
            dedup: StdMutex::new(LruSet::new(2000)),
        }
    }

    fn fresh(&self, id: &str) -> bool {
        let mut d = match self.dedup.lock() {
            Ok(d) => d,
            Err(p) => p.into_inner(),
        };
        d.fresh(id)
    }

    fn allowed(&self, wa_id: &str) -> bool {
        self.config.allowed_user_ids.is_empty()
            || self.config.allowed_user_ids.iter().any(|u| u == wa_id)
    }

    /// Translate one `value.messages[]` entry into a native payload.
    fn message_payload(&self, value: &Value, message: &Value, sender_name: &str) -> Option<Value> {
        let wa_id = message.get("from").and_then(|f| f.as_str())?;
        let id = message.get("id").and_then(|i| i.as_str()).unwrap_or("");
        if id.is_empty() || !self.fresh(id) || !self.allowed(wa_id) {
            return None;
        }
        let message_type = message
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("text");

        let mut text = String::new();
        let mut caption = String::new();
        let mut files: Vec<Value> = Vec::new();

        match message_type {
            "text" => {
                text = message
                    .get("text")
                    .and_then(|t| t.get("body"))
                    .and_then(|b| b.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            "button" => {
                text = message
                    .get("button")
                    .and_then(|b| b.get("text"))
                    .and_then(|b| b.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            "interactive" => {
                let interactive = message.get("interactive");
                let reply = interactive
                    .and_then(|i| i.get("button_reply"))
                    .or_else(|| interactive.and_then(|i| i.get("list_reply")));
                text = reply
                    .and_then(|r| r.get("title"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(pid) = reply.and_then(|r| r.get("id")).and_then(|i| i.as_str()) {
                    text = format!("{text} [{pid}]");
                }
            }
            "reaction" => {
                let emoji = message
                    .get("reaction")
                    .and_then(|r| r.get("emoji"))
                    .and_then(|e| e.as_str())
                    .unwrap_or("");
                text = format!("Reaction: {emoji}");
            }
            "location" => {
                let loc = message.get("location");
                let lat = loc.and_then(|l| l.get("latitude")).and_then(|v| v.as_f64());
                let lon = loc
                    .and_then(|l| l.get("longitude"))
                    .and_then(|v| v.as_f64());
                let label = loc
                    .and_then(|l| l.get("name"))
                    .and_then(|v| v.as_str())
                    .or_else(|| loc.and_then(|l| l.get("address")).and_then(|v| v.as_str()))
                    .unwrap_or("");
                text = match (lat, lon) {
                    (Some(a), Some(b)) => format!("Location: {label} ({a}, {b})"),
                    _ => format!("Location: {label}"),
                };
            }
            "image" | "video" | "audio" | "document" | "sticker" => {
                let node = message.get(message_type);
                let media_id = node
                    .and_then(|n| n.get("id"))
                    .and_then(|i| i.as_str())
                    .unwrap_or("");
                if media_id.is_empty() {
                    return None;
                }
                caption = node
                    .and_then(|n| n.get("caption"))
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                files.push(json!({
                    "kind": message_type,
                    "id": media_id,
                    "mime": node.and_then(|n| n.get("mime_type")).and_then(|m| m.as_str()),
                    "name": node.and_then(|n| n.get("filename")).and_then(|f| f.as_str()),
                    "voice": node
                        .and_then(|n| n.get("voice"))
                        .cloned()
                        .unwrap_or(Value::Null),
                }));
            }
            "contacts" => {
                text = "Shared a contact".to_string();
            }
            other => {
                text = format!("Unsupported WhatsApp message type: {other}");
            }
        }

        if text.is_empty()
            && caption.is_empty()
            && files.is_empty()
        {
            return None;
        }

        Some(json!({
            "_whatsapp": true,
            "waba_id": value.get("metadata").and_then(|m| m.get("phone_number_id")).cloned().unwrap_or(Value::Null),
            "phone_number_id": value.get("metadata").and_then(|m| m.get("phone_number_id")).cloned().unwrap_or(Value::Null),
            "wa_id": wa_id,
            "sender_name": sender_name,
            "message_id": id,
            "message_type": message_type,
            "timestamp": message.get("timestamp").cloned().unwrap_or(Value::Null),
            "text": text,
            "caption": caption,
            "files": files,
        }))
    }
}

impl WebhookAdapter for WhatsAppWebhook {
    fn channel_type(&self) -> &'static str {
        "whatsapp"
    }

    fn verify_challenge(&self, req: &WebhookRequest) -> Option<WebhookResponse> {
        let mode = req.query_param("hub.mode")?;
        let token = req.query_param("hub.verify_token")?;
        let challenge = req.query_param("hub.challenge")?;
        if mode == "subscribe"
            && crate::webhook::constant_time_eq(
                token.as_bytes(),
                self.config.verify_token.as_bytes(),
            )
        {
            // Meta expects the raw challenge echoed back.
            Some(WebhookResponse::text(challenge))
        } else {
            Some(WebhookResponse::status(403, "verification failed"))
        }
    }

    fn handle_request(&self, req: &WebhookRequest) -> Result<WebhookOutcome> {
        let signature = req
            .header("x-hub-signature-256")
            .ok_or_else(|| GatewayError::Other("whatsapp: missing X-Hub-Signature-256".into()))?;
        if !verify_sha256_hex_signature(&self.config.app_secret, &req.body, signature) {
            return Err(GatewayError::Other(
                "whatsapp: invalid X-Hub-Signature-256".into(),
            ));
        }

        let body = req.body_json()?;
        let mut payloads = Vec::new();

        if let Some(entries) = body.get("entry").and_then(|e| e.as_array()) {
            for entry in entries {
                let changes = match entry.get("changes").and_then(|c| c.as_array()) {
                    Some(c) => c,
                    None => continue,
                };
                for change in changes {
                    let value = match change.get("value") {
                        Some(v) => v,
                        None => continue,
                    };
                    // Delivery/read receipts need no handling beyond the ack.
                    let sender_name = value
                        .get("contacts")
                        .and_then(|c| c.as_array())
                        .and_then(|c| c.first())
                        .and_then(|c| c.get("profile"))
                        .and_then(|p| p.get("name"))
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string();
                    let messages = match value.get("messages").and_then(|m| m.as_array()) {
                        Some(m) => m,
                        None => continue,
                    };
                    for message in messages {
                        if let Some(payload) = self.message_payload(value, message, &sender_name)
                        {
                            payloads.push(RawPayload::Json(payload));
                        }
                    }
                }
            }
        }

        Ok(WebhookOutcome::with_response(
            WebhookResponse::text("EVENT_RECEIVED"),
            payloads,
        ))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn destination(subject: &ChannelSubject) -> Result<String> {
    let to = subject
        .metadata
        .get("to_handle")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(subject.subject_id.as_str());
    if to.is_empty() {
        return Err(GatewayError::Other(
            "whatsapp: empty destination wa_id".into(),
        ));
    }
    Ok(to.to_string())
}

fn error_message(body: &Value) -> String {
    body.get("error")
        .and_then(|e| {
            e.get("message")
                .and_then(|m| m.as_str())
                .or_else(|| e.as_str())
        })
        .unwrap_or("unknown error")
        .to_string()
}

fn media_to_part(f: &Value) -> Option<ContentPart> {
    let id = f.get("id").and_then(|v| v.as_str())?;
    let url = format!("{MEDIA_SCHEME}{id}");
    let mime = f
        .get("mime")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let name = f
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    let kind = f.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    let is_voice = f.get("voice").and_then(|v| v.as_bool()).unwrap_or(false);

    // `kind` is authoritative; the mime is only a fallback (Meta sometimes
    // reports a generic type for voice notes).
    let effective = match kind {
        "image" | "sticker" => "image",
        "video" => "video",
        "audio" => "audio",
        "document" => {
            if mime.starts_with("image/") {
                "image"
            } else if mime.starts_with("video/") {
                "video"
            } else if mime.starts_with("audio/") {
                "audio"
            } else {
                "file"
            }
        }
        _ => "file",
    };

    let _ = is_voice;
    Some(match effective {
        "image" => ContentPart::Image(ImageContent {
            url,
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        "video" => ContentPart::Video(VideoContent {
            url,
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        "audio" => ContentPart::Audio(AudioContent {
            url,
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        _ => ContentPart::File(FileContent {
            url,
            filename: name,
            mime_type: non_empty(&mime),
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

fn media_message(
    media: &ContentPart,
    mime: &str,
    media_id: &str,
) -> (&'static str, Value) {
    match media {
        ContentPart::Image(_) => ("image", json!({ "id": media_id })),
        ContentPart::Video(_) => ("video", json!({ "id": media_id })),
        ContentPart::Audio(_) => ("audio", json!({ "id": media_id })),
        _ => (
            "document",
            json!({
                "id": media_id,
                "filename": media
                    .filename()
                    .filter(|f| !f.is_empty())
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| format!("document{}", extension_for(mime, None))),
            }),
        ),
    }
}

fn extension_for(mime: &str, filename: Option<&str>) -> String {
    if let Some(name) = filename {
        if let Some((_, ext)) = name.rsplit_once('.') {
            if !ext.is_empty() && ext.len() <= 5 {
                return format!(".{ext}");
            }
        }
    }
    mime_guess::get_mime_extensions_str(mime.split(';').next().unwrap_or(mime).trim())
        .and_then(|exts| exts.first().copied())
        .map(|e| format!(".{e}"))
        .unwrap_or_else(|| ".bin".to_string())
}

fn clear_url(part: &mut ContentPart) {
    match part {
        ContentPart::Image(x) => x.url.clear(),
        ContentPart::Video(x) => x.url.clear(),
        ContentPart::Audio(x) => x.url.clear(),
        ContentPart::File(x) => x.url.clear(),
        ContentPart::Text(_) => {}
    }
}

fn set_media_meta(part: &mut ContentPart, key: String, size: u64, mime: String) {
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

/// Split on character boundaries, preferring natural break points.
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
        start += slice[..cut].chars().count().max(1);
    }
    out.retain(|s| !s.is_empty());
    if out.is_empty() {
        out.push(text.to_string());
    }
    out
}

/// Convert Markdown to WhatsApp's formatting dialect.
///
/// WhatsApp renders `*bold*`, `_italic_`, `~strike~` and ``` ```mono``` ```; it
/// has no headings and no link syntax, so links are flattened to `label (url)`.
fn markdown_to_whatsapp(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
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

    let bold = regex::Regex::new(r"\*\*([^*\n]+)\*\*").unwrap();
    let bold_u = regex::Regex::new(r"__([^_\n]+)__").unwrap();
    let strike = regex::Regex::new(r"~~([^~\n]+)~~").unwrap();
    let link = regex::Regex::new(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)").unwrap();
    let heading = regex::Regex::new(r"(?m)^#{1,6}\s*(.+)$").unwrap();

    let s = bold.replace_all(&stage2, "*$1*").to_string();
    let s = bold_u.replace_all(&s, "_${1}_").to_string();
    let s = strike.replace_all(&s, "~$1~").to_string();
    let s = link.replace_all(&s, "$1 ($2)").to_string();
    let s = heading.replace_all(&s, "*$1*").to_string();

    let mut out = String::with_capacity(s.len());
    for (i, part) in s.split('\u{0}').enumerate() {
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
    let blank = regex::Regex::new(r"\n{3,}").unwrap();
    blank.replace_all(&out, "\n\n").to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::hmac_sha256_hex;

    fn config() -> WhatsAppConfig {
        WhatsAppConfig {
            phone_number_id: "123".into(),
            access_token: "token".into(),
            verify_token: "verify-me".into(),
            app_secret: "secret".into(),
            ..Default::default()
        }
    }

    fn request(method: &str, headers: &[(&str, &str)], body: &[u8]) -> WebhookRequest {
        WebhookRequest {
            method: method.to_string(),
            path: "/webhooks/whatsapp".to_string(),
            query: Default::default(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn verify_token_handshake_echoes_challenge() {
        let adapter = WhatsAppWebhook::new(config());
        let mut req = request("GET", &[], b"");
        req.query.insert("hub.mode".into(), "subscribe".into());
        req.query.insert("hub.verify_token".into(), "verify-me".into());
        req.query.insert("hub.challenge".into(), "987654".into());
        let resp = adapter.verify_challenge(&req).expect("handshake");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "987654");
    }

    #[test]
    fn verify_token_handshake_rejects_wrong_token() {
        let adapter = WhatsAppWebhook::new(config());
        let mut req = request("GET", &[], b"");
        req.query.insert("hub.mode".into(), "subscribe".into());
        req.query.insert("hub.verify_token".into(), "nope".into());
        req.query.insert("hub.challenge".into(), "987654".into());
        let resp = adapter.verify_challenge(&req).expect("handshake");
        assert_eq!(resp.status, 403);
    }

    #[test]
    fn signature_is_required_and_checked() {
        let adapter = WhatsAppWebhook::new(config());
        let body = br#"{"entry":[]}"#;

        // Missing header.
        assert!(adapter.handle_request(&request("POST", &[], body)).is_err());

        // Wrong signature.
        let bad = request(
            "POST",
            &[("x-hub-signature-256", "sha256=deadbeef")],
            body,
        );
        assert!(adapter.handle_request(&bad).is_err());

        // Correct signature.
        let sig = format!("sha256={}", hmac_sha256_hex(b"secret", body));
        let good = request("POST", &[("x-hub-signature-256", &sig)], body);
        assert!(adapter.handle_request(&good).is_ok());
    }

    #[test]
    fn text_message_becomes_payload() {
        let adapter = WhatsAppWebhook::new(config());
        let body = json!({
            "object": "whatsapp_business_account",
            "entry": [{
                "id": "WABA",
                "changes": [{
                    "value": {
                        "metadata": { "phone_number_id": "123" },
                        "contacts": [{ "profile": { "name": "Alice" }, "wa_id": "16505551234" }],
                        "messages": [{
                            "from": "16505551234",
                            "id": "wamid.1",
                            "timestamp": "1729000000",
                            "type": "text",
                            "text": { "body": "hello" }
                        }]
                    }
                }]
            }]
        });
        let raw = serde_json::to_vec(&body).unwrap();
        let sig = format!("sha256={}", hmac_sha256_hex(b"secret", &raw));
        let out = adapter
            .handle_request(&request("POST", &[("x-hub-signature-256", &sig)], &raw))
            .unwrap();
        assert_eq!(out.payloads.len(), 1);
        let payload = out.payloads[0].as_json().unwrap();
        assert_eq!(payload["wa_id"], "16505551234");
        assert_eq!(payload["text"], "hello");
        assert_eq!(payload["sender_name"], "Alice");
    }

    #[test]
    fn duplicate_message_is_dropped() {
        let adapter = WhatsAppWebhook::new(config());
        assert!(adapter.fresh("wamid.dup"));
        assert!(!adapter.fresh("wamid.dup"));
    }

    #[test]
    fn whitelist_filters_strangers() {
        let mut cfg = config();
        cfg.allowed_user_ids = vec!["16505551234".into()];
        let adapter = WhatsAppWebhook::new(cfg);
        assert!(adapter.allowed("16505551234"));
        assert!(!adapter.allowed("19998887777"));
    }

    #[test]
    fn media_marker_and_parse_round_trip() {
        let value = json!({ "metadata": { "phone_number_id": "123" } });
        let message = json!({
            "from": "16505551234",
            "id": "wamid.img",
            "timestamp": "1729000000",
            "type": "image",
            "image": { "id": "MEDIA123", "mime_type": "image/jpeg", "caption": "look" }
        });
        let adapter = WhatsAppWebhook::new(config());
        let payload = adapter
            .message_payload(&value, &message, "Alice")
            .expect("payload");
        assert_eq!(payload["caption"], "look");
        let part = media_to_part(&payload["files"][0]).unwrap();
        assert_eq!(part.url().unwrap(), "whatsapp-media://MEDIA123");
        assert_eq!(part.mime_type().unwrap(), "image/jpeg");
    }

    #[test]
    fn markdown_conversion_matches_whatsapp_dialect() {
        assert_eq!(markdown_to_whatsapp("**bold**"), "*bold*");
        assert_eq!(markdown_to_whatsapp("__bold__"), "_bold_");
        assert_eq!(markdown_to_whatsapp("~~gone~~"), "~gone~");
        assert_eq!(
            markdown_to_whatsapp("[docs](https://example.com)"),
            "docs (https://example.com)"
        );
        assert_eq!(markdown_to_whatsapp("# Title"), "*Title*");
        assert_eq!(markdown_to_whatsapp("`**keep**`"), "`**keep**`");
    }

    #[test]
    fn media_message_shapes() {
        let image = ContentPart::Image(ImageContent::default());
        let (kind, body) = media_message(&image, "image/png", "MID");
        assert_eq!(kind, "image");
        assert_eq!(body["id"], "MID");

        let file = ContentPart::File(FileContent {
            filename: "report.pdf".into(),
            ..Default::default()
        });
        let (kind, body) = media_message(&file, "application/pdf", "MID");
        assert_eq!(kind, "document");
        assert_eq!(body["filename"], "report.pdf");
    }

    #[test]
    fn chunking_respects_limit() {
        let text = "a".repeat(10_000);
        let chunks = chunk_text(&text, TEXT_CHUNK);
        assert!(chunks.len() >= 3);
        assert!(chunks.iter().all(|c| c.chars().count() <= TEXT_CHUNK));
    }
}
