//! Base channel abstraction, channel configuration, and the shared message pipeline.
//!
//! The base channel abstraction is split into two pieces:
//! - [`ChannelCore`] — shared state + (via helper fns) the pipeline mechanics
//! - [`Channel`] — the platform contract (abstract + overridable methods) with
//! the shared pipeline implemented as provided trait methods.
//!
//! This reproduces the semantics: channels implement only platform logic; the
//! rest is inherited.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::constraints::{ChannelConstraints, RateLimiter, ReplyTimeoutGuard, TypingKeepalive};
use crate::errors::{GatewayError, Result};
use crate::group_context::{GroupContextConfig, GroupContextManager};
use crate::media::DynMediaBackend;
use crate::models::{
    ChannelSubject, ContentPart, InboundMessage, MessageEvent, MessageEventType, RawPayload,
};
use crate::push_routing::strip_ephemeral_push_meta;
use crate::utils::{extract_media, extract_text, merge_messages};

/// Boxed, shared processor: maps an inbound message to an event stream.
pub type MessageProcessor = Arc<
    dyn Fn(InboundMessage) -> std::pin::Pin<Box<dyn Stream<Item = MessageEvent> + Send>> + Send + Sync,
>;

/// Thread-safe callback used to enqueue raw payloads for processing.
pub type EnqueueFn = Arc<dyn Fn(RawPayload) + Send + Sync>;

// ---------------------------------------------------------------------------
// ChannelConfig — base config shared by all platform configs
// ---------------------------------------------------------------------------

/// Base configuration fields shared by every platform-specific config.
///
/// Platform configs embed this via `#[serde(flatten)]` so callers can set
/// `channel_id` / `tenant_id` / `show_thinking` / `show_tool_hints` directly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelConfig {
    pub channel_id: Option<String>,
    pub tenant_id: Option<String>,
    pub show_thinking: bool,
    pub show_tool_hints: bool,
    pub group_context: Option<GroupContextConfig>,
}

/// Contract for platform config structs, providing credential checks.
pub trait ChannelConfigLike {
    /// Access the embedded [`ChannelConfig`] base.
    fn base(&self) -> &ChannelConfig;
    /// Platform-specific required credential field names.
    fn required_credentials(&self) -> &'static [&'static str];
    /// Return the required fields that are currently empty.
    fn missing_credentials(&self) -> Vec<String>;
    /// Field aliases as `(alias_key, canonical_key)` pairs.
    /// `ChannelConfig.field_aliases` (applied only when the canonical key is
    /// absent). Default: none.
    fn field_aliases(&self) -> &'static [(&'static str, &'static str)] {
        &[]
    }

    fn channel_id(&self) -> Option<&str> {
        self.base().channel_id.as_deref()
    }
    fn tenant_id(&self) -> Option<&str> {
        self.base().tenant_id.as_deref()
    }
    fn show_thinking(&self) -> bool {
        self.base().show_thinking
    }
    fn show_tool_hints(&self) -> bool {
        self.base().show_tool_hints
    }
    fn group_context(&self) -> Option<&GroupContextConfig> {
        self.base().group_context.as_ref()
    }
}

/// Raised when a channel config is missing required credentials.
#[derive(Debug, thiserror::Error)]
#[error("channel {kind} missing required credentials: {missing:?}")]
pub struct ChannelCredentialsError {
    pub kind: String,
    pub missing: Vec<String>,
}

// ---------------------------------------------------------------------------
// ChannelCore — shared state
// ---------------------------------------------------------------------------

/// Shared, platform-agnostic state for a channel.
pub struct ChannelCore {
    channel_id: String,
    channel_type: String,
    tenant_id: Option<String>,
    media_backend: Option<DynMediaBackend>,
    rate_limiter: Option<RateLimiter>,
    constraints: ChannelConstraints,
    known_subjects: HashMap<String, ChannelSubject>,
    subject_order: VecDeque<String>,
    on_new_subject: Option<Arc<dyn Fn(ChannelSubject) + Send + Sync>>,
    processor: Option<MessageProcessor>,
    enqueue_callback: Option<EnqueueFn>,
    group_context: GroupContextManager,
    connection_session: Option<String>,
    #[allow(dead_code)]
    debounce_seconds: f64,
    http_client: Mutex<Option<reqwest::Client>>,
    dedup_order: VecDeque<String>,
    dedup_set: HashSet<String>,
}

impl ChannelCore {
    pub fn new(
        channel_id: String,
        tenant_id: Option<String>,
        debounce_seconds: f64,
        constraints: ChannelConstraints,
        group_config: Option<GroupContextConfig>,
    ) -> Self {
        // Ensure a rustls crypto provider is installed before any TLS/WSS
        // connection is attempted by a channel (idempotent, once-only).
        crate::utils::ensure_crypto_provider();
        let rate_limiter = constraints
            .send_rate_limit
            .map(|(max, win)| RateLimiter::new(max, win));
        ChannelCore {
            channel_id,
            channel_type: "base".to_string(),
            tenant_id,
            media_backend: None,
            rate_limiter,
            constraints,
            known_subjects: HashMap::new(),
            subject_order: VecDeque::new(),
            on_new_subject: None,
            processor: None,
            enqueue_callback: None,
            group_context: GroupContextManager::new(group_config),
            connection_session: None,
            debounce_seconds,
            http_client: Mutex::new(None),
            dedup_order: VecDeque::new(),
            dedup_set: HashSet::new(),
        }
    }

    pub fn set_channel_type(&mut self, t: &str) {
        self.channel_type = t.to_string();
    }

    /// Override the debounce window (used when a channel is constructed with a
    /// non-zero `debounce_seconds`).
    pub fn set_debounce_seconds(&mut self, d: f64) {
        self.debounce_seconds = d;
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub fn channel_type(&self) -> &str {
        &self.channel_type
    }

    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    pub fn set_media_backend(&mut self, backend: DynMediaBackend) {
        self.media_backend = Some(backend);
    }

    pub fn media_backend(&self) -> Option<&DynMediaBackend> {
        self.media_backend.as_ref()
    }

    pub fn set_processor(&mut self, processor: MessageProcessor) {
        self.processor = Some(processor);
    }

    pub fn processor(&self) -> Option<MessageProcessor> {
        self.processor.clone()
    }

    pub fn set_enqueue_callback(&mut self, cb: EnqueueFn) {
        self.enqueue_callback = Some(cb);
    }

    pub fn enqueue(&self, payload: RawPayload) {
        if let Some(cb) = &self.enqueue_callback {
            cb(payload);
        }
    }

    /// Clone of the registered enqueue callback, for use by background tasks
    /// (polling loops, websocket listeners) that run outside `&mut self`.
    pub fn enqueue_sender(&self) -> Option<EnqueueFn> {
        self.enqueue_callback.clone()
    }

    pub fn constraints(&self) -> &ChannelConstraints {
        &self.constraints
    }

    pub fn constraints_mut(&mut self) -> &mut ChannelConstraints {
        &mut self.constraints
    }

    /// Replace the constraints and rebuild the derived rate limiter
    /// (same derivation as used at channel construction).
    pub fn apply_constraints(&mut self, constraints: ChannelConstraints) {
        self.rate_limiter = constraints
            .send_rate_limit
            .map(|(max, win)| RateLimiter::new(max, win));
        self.constraints = constraints;
    }

    pub fn group_context_manager(&self) -> &GroupContextManager {
        &self.group_context
    }

    pub fn group_context_manager_mut(&mut self) -> &mut GroupContextManager {
        &mut self.group_context
    }

    pub fn set_connection_session(&mut self, s: Option<String>) {
        self.connection_session = s;
    }

    pub fn connection_session(&self) -> Option<&str> {
        self.connection_session.as_deref()
    }

    pub fn rate_limiter(&self) -> Option<&RateLimiter> {
        self.rate_limiter.as_ref()
    }

    pub fn list_subjects(&self) -> Vec<ChannelSubject> {
        self.known_subjects.values().cloned().collect()
    }

    pub fn get_subject(&self, subject_id: &str) -> Option<ChannelSubject> {
        self.known_subjects.get(subject_id).cloned()
    }

    pub fn set_on_new_subject(&mut self, cb: Arc<dyn Fn(ChannelSubject) + Send + Sync>) {
        self.on_new_subject = Some(cb);
    }

    pub async fn ensure_http(&self) -> Result<reqwest::Client> {
        let mut guard = self.http_client.lock().await;
        match guard.as_ref() {
            Some(c) => Ok(c.clone()),
            None => {
                let c = reqwest::Client::new();
                *guard = Some(c.clone());
                Ok(c)
            }
        }
    }

    /// Track a subject from an inbound message.
    pub fn track_subject(&mut self, message: &InboundMessage) {
        let subject = match &message.channel_subject {
            Some(s) => s,
            None => return,
        };
        if subject.subject_id.is_empty() || subject.subject_id == "unknown" {
            return;
        }
        let now = if message.timestamp > 0.0 {
            message.timestamp
        } else {
            now_f64()
        };
        let meta = message.metadata.clone();
        if let Some(existing) = self.known_subjects.get_mut(&subject.subject_id) {
            existing.last_seen = Some(now);
            existing.metadata = meta;
            if !subject.display_name.is_empty() {
                existing.display_name = subject.display_name.clone();
            }
            if !subject.chat_type.is_empty() {
                existing.chat_type = subject.chat_type.clone();
            }
        } else {
            let subject_obj = ChannelSubject {
                subject_id: subject.subject_id.clone(),
                first_seen: Some(now),
                last_seen: Some(now),
                display_name: subject.display_name.clone(),
                chat_type: subject.chat_type.clone(),
                metadata: meta,
            };
            self.known_subjects
                .insert(subject.subject_id.clone(), subject_obj.clone());
            self.subject_order.push_back(subject.subject_id.clone());
            if let Some(cb) = &self.on_new_subject {
                cb(subject_obj);
            }
        }
    }

    /// In-memory dedup (LRU, max 1000). Returns true if already seen.
    pub fn is_duplicate(&mut self, msg_id: &str) -> bool {
        if self.dedup_set.contains(msg_id) {
            return true;
        }
        self.dedup_set.insert(msg_id.to_string());
        self.dedup_order.push_back(msg_id.to_string());
        if self.dedup_order.len() > 1000 {
            if let Some(old) = self.dedup_order.pop_front() {
                self.dedup_set.remove(&old);
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Channel trait — platform contract + shared pipeline
// ---------------------------------------------------------------------------

/// The platform channel contract. Channels implement only platform logic; the rest
/// of the message pipeline is provided here and operates through `core()` /
/// `core_mut()`.
#[async_trait]
pub trait Channel: Send + Sync {
    // ----- platform abstract methods -----
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()>;
    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()>;
    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage>;

    /// Parse a queued payload, short-circuiting payloads the manager already
    /// parsed.
    ///
    /// The worker parses each item once to derive its session key and to decide
    /// what to batch, then hands the merged result back to the pipeline. Without
    /// this, every channel would have to accept its own already-normalized
    /// [`InboundMessage`] as a second payload shape.
    fn parse_payload(&self, raw: &RawPayload) -> Result<InboundMessage> {
        match raw {
            RawPayload::Preparsed(m) => Ok(m.clone()),
            other => self.parse_inbound(other),
        }
    }

    // ----- platform overridable hooks (defaults provided) -----
    async fn preprocess_inbound(&mut self, _message: &mut InboundMessage) -> Result<()> {
        Ok(())
    }

    async fn send_typing_indicator(&mut self, _subject: &ChannelSubject) -> Result<()> {
        Ok(())
    }

    /// Default content fan-out: text parts via `send_text`, media via `send_media`.
    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        for p in parts {
            if p.is_text() {
                if let Some(t) = p.text_value() {
                    self.send_text(subject, t).await?;
                }
            } else {
                self.send_media(subject, p).await?;
            }
        }
        Ok(())
    }

    /// Platform default constraints.
    ///
    /// `new` starts from these and then applies the config's `show_thinking`
    /// / `show_tool_hints` on top, exactly like the base initializer.
    fn default_constraints() -> ChannelConstraints
    where
        Self: Sized,
    {
        ChannelConstraints::default()
    }

    /// Config-dependent constraint adjustments, for channels whose
    /// `_default_constraints(self)` reads `self._config` (e.g. Telegram's
    /// `typing_keepalive_interval = 4.0 if self._config.show_typing else 0.0`).
    ///
    /// Called by the generated `new(...)` after `default_constraints()` and the
    /// config's `show_thinking` / `show_tool_hints` have been applied.
    fn configure_constraints(&self, _constraints: &mut ChannelConstraints) {}

    fn channel_type(&self) -> &str {
        self.core().channel_type()
    }

    fn enrich_push_metadata(
        &self,
        _subject: &ChannelSubject,
        meta: HashMap<String, serde_json::Value>,
    ) -> HashMap<String, serde_json::Value> {
        meta
    }

    fn get_debounce_key(&self, message: &InboundMessage) -> String {
        message
            .channel_subject
            .as_ref()
            .map(|s| s.subject_id.clone())
            .unwrap_or_default()
    }

    fn should_batch_inbound(&self, message: &InboundMessage) -> bool {
        !self.core().group_context_manager().handles(message)
    }

    fn merge_inbound(&self, messages: &[InboundMessage]) -> Result<InboundMessage> {
        Ok(merge_messages(messages))
    }

    // ----- access to core -----
    fn core(&self) -> &ChannelCore;
    fn core_mut(&mut self) -> &mut ChannelCore;

    // ----- shared provided methods -----

    fn channel_id(&self) -> String {
        self.core().channel_id().to_string()
    }

    fn constraints(&self) -> &ChannelConstraints {
        self.core().constraints()
    }

    /// Apply runtime constraint overrides.
    fn set_constraints(&mut self, overrides: &HashMap<String, serde_json::Value>) {
        let mut new_limiter: Option<Option<RateLimiter>> = None;
        {
        let c = self.core_mut().constraints_mut();
        if let Some(v) = overrides.get("show_thinking") {
            if let Some(b) = v.as_bool() {
                c.show_thinking = b;
            }
        }
        if let Some(v) = overrides.get("show_tool_hints") {
            if let Some(b) = v.as_bool() {
                c.show_tool_hints = b;
            }
        }
        if let Some(v) = overrides.get("reply_timeout") {
            if let Some(n) = v.as_f64() {
                c.reply_timeout = n;
            }
        }
        if let Some(v) = overrides.get("typing_keepalive_interval") {
            if let Some(n) = v.as_f64() {
                c.typing_keepalive_interval = n;
            }
        }
        if let Some(v) = overrides.get("timeout_strategy") {
            if let Some(s) = v.as_str() {
                c.timeout_strategy = s.to_string();
            }
        }
        if let Some(v) = overrides.get("placeholder_texts") {
            if let Some(arr) = v.as_array() {
                let texts: Vec<String> = arr
                    .iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect();
                if !texts.is_empty() {
                    c.placeholder_texts = texts;
                }
            }
        }
        if let Some(v) = overrides.get("send_rate_limit") {
            if let Some(arr) = v.as_array() {
                if arr.len() == 2 {
                    let max = arr[0].as_i64().unwrap_or(0);
                    let win = arr[1].as_f64().unwrap_or(0.0);
                    c.send_rate_limit = if max > 0 {
                        Some((max, win))
                    } else {
                        None
                    };
                    new_limiter = Some(c.send_rate_limit.map(|(m, w)| RateLimiter::new(m, w)));
                }
            }
        }
        if let Some(v) = overrides.get("tool_hint_throttle") {
            if let Some(n) = v.as_f64() {
                c.tool_hint_throttle = n;
            }
        }
        if let Some(v) = overrides.get("tool_hint_template") {
            if let Some(s) = v.as_str() {
                c.tool_hint_template = s.to_string();
            }
        }
        if let Some(v) = overrides.get("tool_end_template") {
            if let Some(s) = v.as_str() {
                c.tool_end_template = s.to_string();
            }
        }
        if let Some(v) = overrides.get("thinking_template") {
            if let Some(s) = v.as_str() {
                c.thinking_template = s.to_string();
            }
        }
        }
        if let Some(rl) = new_limiter {
            self.core_mut().rate_limiter = rl;
        }
    }

    async fn acquire_rate_slot(&self) {
        if let Some(rl) = self.core().rate_limiter() {
            rl.acquire().await;
        }
    }

    fn set_media_backend(&mut self, backend: DynMediaBackend) {
        self.core_mut().set_media_backend(backend);
    }

    fn set_processor(&mut self, processor: MessageProcessor) {
        self.core_mut().set_processor(processor);
    }

    fn set_enqueue_callback(&mut self, cb: EnqueueFn) {
        self.core_mut().set_enqueue_callback(cb);
    }

    fn enqueue(&self, payload: RawPayload) {
        self.core().enqueue(payload);
    }

    fn list_subjects(&self) -> Vec<ChannelSubject> {
        self.core().list_subjects()
    }

    fn get_subject(&self, subject_id: &str) -> Option<ChannelSubject> {
        self.core().get_subject(subject_id)
    }

    fn set_on_new_subject(&mut self, cb: Arc<dyn Fn(ChannelSubject) + Send + Sync>) {
        self.core_mut().set_on_new_subject(cb);
    }

    fn set_connection_session(&mut self, s: Option<String>) {
        self.core_mut().set_connection_session(s);
    }

    fn is_duplicate(&mut self, msg_id: &str) -> bool {
        self.core_mut().is_duplicate(msg_id)
    }

    // ------- Reply (in inbound context) -------------------------------------

    async fn reply_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        self.acquire_rate_slot().await;
        self.send_text(subject, text).await
    }

    async fn reply_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        self.acquire_rate_slot().await;
        self.send_content(subject, parts).await
    }

    async fn reply_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        self.acquire_rate_slot().await;
        self.send_media(subject, media).await
    }

    // ------- Push (bot-initiated) -------------------------------------------

    fn resolve_push_subject(&self, subject: &ChannelSubject) -> ChannelSubject {
        let mut meta = subject.metadata.clone();
        let known = self.core().get_subject(&subject.subject_id);
        if let Some(k) = &known {
            if !k.metadata.is_empty() {
                for (kk, vv) in &k.metadata {
                    meta.entry(kk.clone()).or_insert_with(|| vv.clone());
                }
            }
        }
        let mut meta = self.enrich_push_metadata(subject, meta);
        let meta = strip_ephemeral_push_meta(&mut meta);
        let chat_type = if !subject.chat_type.is_empty() {
            subject.chat_type.clone()
        } else {
            known.as_ref().map(|k| k.chat_type.clone()).unwrap_or_default()
        };
        let first = subject
            .first_seen
            .or_else(|| known.as_ref().and_then(|k| k.first_seen))
            .unwrap_or(0.0);
        let last = subject
            .last_seen
            .or_else(|| known.as_ref().and_then(|k| k.last_seen))
            .unwrap_or(0.0);
        ChannelSubject {
            subject_id: subject.subject_id.clone(),
            first_seen: Some(first),
            last_seen: Some(last),
            display_name: subject.display_name.clone(),
            chat_type,
            metadata: meta.clone(),
        }
    }

    async fn push_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        self.acquire_rate_slot().await;
        let resolved = self.resolve_push_subject(subject);
        self.send_text(&resolved, text).await
    }

    async fn push_message(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        self.acquire_rate_slot().await;
        let resolved = self.resolve_push_subject(subject);
        self.send_content(&resolved, parts).await
    }

    // ------- Media ----------------------------------------------------------

    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.core().ensure_http().await?;
        let resp = http.get(url).send().await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "media fetch failed: {}",
                resp.status()
            )));
        }
        let mime = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| guess_mime(url));
        let data = resp.bytes().await?;
        Ok((data.to_vec(), mime))
    }

    async fn load_media_bytes(&self, part: &ContentPart) -> Result<(Vec<u8>, String)> {
        if part.is_text() {
            return Err(GatewayError::Other(
                "load_media_bytes called on TextContent".to_string(),
            ));
        }
        if let Some(data) = part.data() {
            let mime = part
                .mime_type()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "application/octet-stream".to_string());
            match base64::engine::general_purpose::STANDARD.decode(data) {
                Ok(raw) => return Ok((raw, mime)),
                Err(e) => {
                    return Err(GatewayError::InvalidBase64(e.to_string()));
                }
            }
        }
        if let Some(local) = part.local_path() {
            match self.core().media_backend() {
                Some(backend) => {
                    let raw = backend.read(local).await?;
                    let mime = part
                        .mime_type()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| guess_mime(local));
                    return Ok((raw, mime));
                }
                None => {
                    return Err(GatewayError::NoMediaBackend(self.channel_id()));
                }
            }
        }
        match part.url() {
            Some(url) if !url.is_empty() => {
                let (raw, mime) = self.fetch_remote_media(url).await?;
                // Cache the fetched bytes into the backend so subsequent reads
                // are local.
                if let Some(backend) = self.core().media_backend() {
                    let key = derive_storage_key(self.core().channel_type(), part, &mime);
                    if let Err(e) = backend.save(&raw, &key).await {
                        tracing::debug!("Failed to cache fetched media to backend: {}", e);
                    }
                }
                Ok((raw, mime))
            }
            _ => Err(GatewayError::NoMediaSource),
        }
    }

    async fn persist_media(&mut self, message: &mut InboundMessage) -> Result<()> {
        if self.core().media_backend().is_none() {
            return Ok(());
        }
        let ts = now_secs();
        let mut index = 0usize;
        for part in message.content.iter_mut() {
            self.persist_media_part(part, index, ts).await;
            index += 1;
        }
        if let Some(gc) = message.group_context.as_mut() {
            for item in gc.messages.iter_mut() {
                for part in item.content.iter_mut() {
                    self.persist_media_part(part, index, ts).await;
                    index += 1;
                }
            }
        }
        Ok(())
    }

    async fn persist_media_part(&mut self, part: &mut ContentPart, index: usize, ts: u64) {
        if part.is_text() || part.local_path().is_some() {
            return;
        }
        let url = match part.url() {
            Some(u) if !u.is_empty() => u.to_string(),
            _ => return,
        };
        let filename = resolve_media_filename(part, index, ts);
        let key = format!(
            "{}/{}/{}",
            self.core().channel_type(),
            self.core().channel_id(),
            filename
        );
        match self.fetch_remote_media(&url).await {
            Ok((data, mime)) => {
                if self.core().media_backend().is_some() {
                    // Save through the media backend; on success stamp the local
                    // path / size / mime onto the inner content struct.
                    let save_ok = {
                        let backend = self.core().media_backend().unwrap();
                        backend.save(&data, &key).await.is_ok()
                    };
                    if save_ok {
                        set_media_meta(part, key, data.len() as u64, mime);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to persist media: url={} error={}", url, e);
            }
        }
    }

    // ------- Inbound pipeline ----------------------------------------------

    async fn handle_inbound(&mut self, raw: &RawPayload) -> Result<()> {
        let prepared = self.prepare_inbound(raw).await?;
        let (message, subject) = match prepared {
            Some(x) => x,
            None => return Ok(()),
        };
        let processing_succeeded = self.process_inbound(&message, &subject).await;
        self.finalize_inbound(&message, processing_succeeded);
        Ok(())
    }

    async fn prepare_inbound(
        &mut self,
        raw: &RawPayload,
    ) -> Result<Option<(InboundMessage, ChannelSubject)>> {
        let mut message = self.parse_payload(raw)?;
        self.preprocess_inbound(&mut message).await?;
        self.core_mut().track_subject(&message);
        if self.core().media_backend().is_some()
            && self.core().group_context_manager().should_persist_media(&message)
        {
            self.persist_media(&mut message).await?;
        }
        let prepared = self.core_mut().group_context_manager_mut().prepare(&message);
        let mut message = match prepared {
            Some(m) => m,
            None => return Ok(None),
        };
        let subject_id = message
            .channel_subject
            .as_ref()
            .map(|s| s.subject_id.clone())
            .unwrap_or_default();
        let subject = self
            .core()
            .get_subject(&subject_id)
            .unwrap_or_else(|| ChannelSubject {
                subject_id: subject_id.clone(),
                first_seen: Some(if message.timestamp > 0.0 {
                    message.timestamp
                } else {
                    now_f64()
                }),
                last_seen: Some(if message.timestamp > 0.0 {
                    message.timestamp
                } else {
                    now_f64()
                }),
                display_name: String::new(),
                chat_type: String::new(),
                metadata: HashMap::new(),
            });
        message.channel_subject = Some(subject.clone());
        Ok(Some((message, subject)))
    }

    fn finalize_inbound(&mut self, message: &InboundMessage, processing_succeeded: bool) {
        if processing_succeeded {
            self.core_mut().group_context_manager_mut().mark_replied(message);
        }
    }

    /// Default inbound pipeline (delta accumulation, thinking, timeout
    /// placeholder, typing keepalive). Overridable; channels that override it can
    /// still run the standard behavior via [`Channel::default_process_inbound_body`]
    ///._process_inbound(...)`).
    async fn process_inbound(&mut self, message: &InboundMessage, subject: &ChannelSubject) -> bool {
        self.default_process_inbound_body(message, subject).await
    }

    /// Body of the standard pipeline, kept callable for channels that override
    /// [`Channel::process_inbound`] but delegate the core loop.
    #[doc(hidden)]
    async fn default_process_inbound_body(&mut self, message: &InboundMessage, subject: &ChannelSubject) -> bool {
        let processor = match self.core().processor() {
            Some(p) => p,
            None => {
                tracing::warn!("No processor set for channel {}", self.channel_id());
                return false;
            }
        };
        let mut stream = processor(message.clone());

        let constraints = self.core().constraints().clone();
        let (to_tx, mut to_rx): (UnboundedSender<()>, _) = tokio::sync::mpsc::unbounded_channel();
        let (ty_tx, mut ty_rx): (UnboundedSender<()>, _) = tokio::sync::mpsc::unbounded_channel();
        let mut timeout_guard = if constraints.reply_timeout > 0.0 {
            Some(ReplyTimeoutGuard::spawn(constraints.reply_timeout, to_tx))
        } else {
            None
        };
        let mut typing_keepalive = if constraints.typing_keepalive_interval > 0.0 {
            Some(TypingKeepalive::spawn(
                constraints.typing_keepalive_interval,
                ty_tx,
            ))
        } else {
            None
        };

        let mut delta_buffer = String::new();
        let mut thinking_buffer = String::new();
        let mut placeholder_sent = false;
        let mut processing_succeeded = false;

        let cancel_guards = |timeout_guard: &mut Option<ReplyTimeoutGuard>,
                            typing_keepalive: &mut Option<TypingKeepalive>| {
            if let Some(g) = timeout_guard.take() {
                drop(g);
            }
            if let Some(k) = typing_keepalive.take() {
                drop(k);
            }
        };

        loop {
            tokio::select! {
                maybe_ev = stream.next() => {
                    match maybe_ev {
                        Some(ev) => {
                            match ev.event_type {
                                MessageEventType::Delta => {
                                    for p in &ev.content {
                                        if let Some(t) = p.text_value() {
                                            delta_buffer.push_str(t);
                                        }
                                    }
                                }
                                MessageEventType::Thinking => {
                                    if constraints.show_thinking {
                                        for p in &ev.content {
                                            if let Some(t) = p.text_value() {
                                                let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                                let _ = self.rate_limited_send(subject, &formatted).await;
                                            }
                                        }
                                    }
                                }
                                MessageEventType::ThinkingDelta => {
                                    if constraints.show_thinking {
                                        for p in &ev.content {
                                            if let Some(t) = p.text_value() {
                                                thinking_buffer.push_str(t);
                                            }
                                        }
                                    }
                                }
                                MessageEventType::Flush => {
                                    cancel_guards(&mut timeout_guard, &mut typing_keepalive);
                                    if !thinking_buffer.is_empty() {
                                        let t = std::mem::take(&mut thinking_buffer);
                                        let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                        let _ = self.rate_limited_send(subject, &formatted).await;
                                    }
                                    if !delta_buffer.is_empty() {
                                        let d = std::mem::take(&mut delta_buffer);
                                        let _ = self.rate_limited_send(subject, &d).await;
                                    }
                                }
                                MessageEventType::Completed => {
                                    cancel_guards(&mut timeout_guard, &mut typing_keepalive);
                                    if !thinking_buffer.is_empty() {
                                        let t = std::mem::take(&mut thinking_buffer);
                                        let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                        let _ = self.rate_limited_send(subject, &formatted).await;
                                    }
                                    if !delta_buffer.is_empty() {
                                        let d = std::mem::take(&mut delta_buffer);
                                        let _ = self.rate_limited_send(subject, &d).await;
                                    }
                                    processing_succeeded = true;
                                    break;
                                }
                                MessageEventType::Message => {
                                    cancel_guards(&mut timeout_guard, &mut typing_keepalive);
                                    if !thinking_buffer.is_empty() {
                                        let t = std::mem::take(&mut thinking_buffer);
                                        let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                        let _ = self.rate_limited_send(subject, &formatted).await;
                                    }
                                    if !delta_buffer.is_empty() {
                                        let d = std::mem::take(&mut delta_buffer);
                                        let _ = self.rate_limited_send(subject, &d).await;
                                    }
                                    let _ = self.deliver_event(subject, &ev).await;
                                }
                                MessageEventType::Typing => {}
                                MessageEventType::ToolStart => {
                                    self.on_tool_start(subject, &ev).await;
                                }
                                MessageEventType::ToolEnd => {
                                    self.on_tool_end(subject, &ev).await;
                                }
                                MessageEventType::Error => {
                                    let err = ev.error.clone().unwrap_or_else(|| "An unexpected error occurred.".to_string());
                                    let _ = self.rate_limited_send(subject, &err).await;
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = to_rx.recv() => {
                    if !placeholder_sent && constraints.timeout_strategy == "placeholder" {
                        placeholder_sent = true;
                        let _ = self.rate_limited_send(subject, &constraints.placeholder_text()).await;
                    }
                }
                _ = ty_rx.recv() => {
                    let _ = self.send_typing_indicator(subject).await;
                }
            }
        }
        processing_succeeded
    }

    async fn rate_limited_send(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let text = self.clean_output(text);
        if text.is_empty() {
            return Ok(());
        }
        self.reply_text(subject, &text).await
    }

    fn clean_output(&self, text: &str) -> String {
        if text.is_empty() {
            return String::new();
        }
        let show_thinking = self.core().constraints().show_thinking;
        let template = self.core().constraints().thinking_template.clone();
        if show_thinking {
            let re = regex::Regex::new(r"(?s)<think>(.*?)</think>").unwrap();
            let out = re.replace_all(text, |caps: &regex::Captures| {
                let content = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
                if content.is_empty() {
                    String::new()
                } else {
                    format!("{}\n\n", template.replace("{content}", content))
                }
            });
            let mut s = out.to_string();
            if let Some(m) = regex::Regex::new(r"(?s)<think>(.*)$").unwrap().find(text) {
                let rest = &text[m.start()..];
                let content = rest
                    .trim_start_matches("<think>")
                    .trim();
                if !content.is_empty() {
                    s = format!(
                        "{}{}\n\n",
                        text[..m.start()].to_string(),
                        template.replace("{content}", content)
                    );
                } else {
                    s = text[..m.start()].to_string();
                }
            }
            s.trim().to_string()
        } else {
            let re = regex::Regex::new(r"(?s)<think>.*?</think>").unwrap();
            let s = re.replace_all(text, "").to_string();
            let re2 = regex::Regex::new(r"(?s)<think>.*$").unwrap();
            re2.replace_all(&s, "").trim().to_string()
        }
    }

    async fn deliver_event(&mut self, subject: &ChannelSubject, event: &MessageEvent) -> Result<()> {
        match event.event_type {
            MessageEventType::Completed
            | MessageEventType::Delta
            | MessageEventType::ThinkingDelta
            | MessageEventType::Flush => return Ok(()),
            MessageEventType::Thinking => return Ok(()),
            MessageEventType::Error => {
                let err = event
                    .error
                    .clone()
                    .unwrap_or_else(|| "An unexpected error occurred.".to_string());
                self.rate_limited_send(subject, &err).await?;
            }
            MessageEventType::Typing => {}
            MessageEventType::ToolStart => self.on_tool_start(subject, event).await,
            MessageEventType::ToolEnd => self.on_tool_end(subject, event).await,
            MessageEventType::Message => {
                let text = extract_text(&event.content);
                if !text.is_empty() {
                    self.rate_limited_send(subject, &text).await?;
                }
                for media in extract_media(&event.content) {
                    self.reply_media(subject, &media).await?;
                }
            }
        }
        Ok(())
    }

    async fn on_thinking(&mut self, subject: &ChannelSubject, event: &MessageEvent) {
        if !self.core().constraints().show_thinking {
            return;
        }
        for part in &event.content {
            if let Some(t) = part.text_value() {
                let formatted = self
                    .core()
                    .constraints()
                    .thinking_template
                    .replace("{content}", t.trim());
                let _ = self.rate_limited_send(subject, &formatted).await;
            }
        }
    }

    async fn on_tool_start(&mut self, subject: &ChannelSubject, event: &MessageEvent) {
        if !self.core().constraints().show_tool_hints {
            return;
        }
        let text = crate::constraints::tool_hint_message(
            &event.metadata,
            self.core().constraints(),
            "start",
        );
        let _ = self.rate_limited_send(subject, &text).await;
    }

    async fn on_tool_end(&mut self, subject: &ChannelSubject, event: &MessageEvent) {
        if !self.core().constraints().show_tool_hints {
            return;
        }
        let text = crate::constraints::tool_hint_message(
            &event.metadata,
            self.core().constraints(),
            "end",
        );
        let _ = self.rate_limited_send(subject, &text).await;
    }
}

// ---------------------------------------------------------------------------
// Free helpers (storage keys / mime)
// ---------------------------------------------------------------------------

const MIME_EXT_MAP: &[(&str, &str)] = &[
    ("image/png", ".png"),
    ("image/jpeg", ".jpg"),
    ("image/gif", ".gif"),
    ("image/webp", ".webp"),
    ("image/bmp", ".bmp"),
    ("video/mp4", ".mp4"),
    ("video/webm", ".webm"),
    ("audio/mpeg", ".mp3"),
    ("audio/mp3", ".mp3"),
    ("audio/wav", ".wav"),
    ("audio/ogg", ".ogg"),
    ("audio/silk", ".silk"),
    ("application/pdf", ".pdf"),
];

fn mime_to_ext(mime: &str, default: &str) -> String {
    let m = mime.to_lowercase();
    let m = m.split(';').next().unwrap_or("").trim().to_string();
    for (k, v) in MIME_EXT_MAP {
        if *k == m {
            return v.to_string();
        }
    }
    default.to_string()
}

fn guess_mime(path_or_url: &str) -> String {
    let path = path_or_url
        .rsplit_once('?')
        .map(|(p, _)| p)
        .unwrap_or(path_or_url);
    mime_guess::from_path(path)
        .first()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

fn safe_media_filename(filename: &str, fallback: &str) -> String {
    let basename = filename.replace('\\', "/").rsplit('/').next().unwrap_or("").to_string();
    let cleaned: String = basename
        .chars()
        .map(|c| if c.is_control() { '_' } else { c })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').to_string();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed
    }
}

fn md5_short(s: &str) -> String {
    // Small dependency-free FNV-ish hash is overkill; use a truncated hex of the
    // bytes via std. We approximate the md5 short with a 8-char slice of a
    // simple djb2 hash for stable keys (collisions are acceptable for cache keys).
    let mut hash: u64 = 5381;
    for b in s.bytes() {
        hash = ((hash << 5).wrapping_add(hash)).wrapping_add(b as u64);
    }
    format!("{:08x}", hash & 0xffff_ffff)
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

/// Deterministic backend key for an outbound-cached part.
/// `BaseChannel._derive_storage_key` (`{channel_type}/_outbound/{ts}_...`).
fn derive_storage_key(channel_type: &str, part: &ContentPart, mime: &str) -> String {
    let ts = now_secs();
    if let Some(fname) = part.filename() {
        if !fname.is_empty() {
            let name = safe_media_filename(fname, "attachment");
            return format!("{}/_outbound/{}_{}", channel_type, ts, name);
        }
    }
    let url = part.url().unwrap_or("");
    let url_hash = md5_short(url);
    let ext = mime_to_ext(mime, ".bin");
    format!("{}/_outbound/{}_{}{}", channel_type, ts, url_hash, ext)
}

fn resolve_media_filename(part: &ContentPart, index: usize, ts: u64) -> String {
    if let Some(filename) = part.filename() {
        if !filename.is_empty() {
            let name = safe_media_filename(filename, "attachment");
            return format!("{}_{}_{}", ts, index, name);
        }
    }
    if let Some(url) = part.url() {
        if !url.is_empty() {
            let ext = mime_to_ext(part.mime_type().unwrap_or(""), ".png");
            let h = md5_short(url);
            return format!("{}_{}{}", ts, h, ext);
        }
    }
    let h = md5_short(&format!("{}", index));
    format!("{}_{}.bin", ts, h)
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

pub fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build a thread-safe enqueue callback from an mpsc sender.
pub fn enqueue_callback(tx: tokio::sync::mpsc::Sender<RawPayload>) -> EnqueueFn {
    Arc::new(move |payload: RawPayload| {
        let _ = tx.try_send(payload);
    })
}
