#![allow(dead_code)]
//! WeChat (iLink Bot) channel: HTTP long-poll based message receiving.
//!
//! Faithful 1:1 Rust port of `octop_gateway.channels.weixin`. All Python
//! submodules (`api.py`, `media.py`, `text.py`, `types.py`, `login_qr.py`) are
//! inlined below as `mod` blocks so the whole adapter lives in one file.
//!
//! Uses the WeChat iLink Bot API (`https://ilinkai.weixin.qq.com`) with:
//! - HTTP long-poll for receiving messages (`getUpdates`)
//! - HTTP API for sending messages (`sendMessage`)
//! - Typing indicator support (`sendTyping`)
//! - No public callback URL required — the client pulls messages from server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc::UnboundedSender;

use crate::channel::{Channel, ChannelConfig, ChannelCore, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{
    ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage, MessageEventType,
    RawPayload, TextContent, VideoContent,
};
use crate::push_routing::{alias_subject_fields, WEIXIN_CONTEXT_TOKEN_KEY};

// ---------------------------------------------------------------------------
// Constants (mirror the Python module-level constants)
// ---------------------------------------------------------------------------

const API_BASE: &str = "https://ilinkai.weixin.qq.com";

/// Default long-poll timeout in milliseconds.
const DEFAULT_LONG_POLL_TIMEOUT_MS: i64 = 35_000;

/// iLink message_type for user-authored inbound messages.
const MSG_TYPE_USER: i64 = 1;

/// iLink getUploadUrl media_type.
const MEDIA_TYPE_IMAGE: i64 = 1;
const MEDIA_TYPE_VIDEO: i64 = 2;
const MEDIA_TYPE_FILE: i64 = 3;

/// Message item types.
const ITEM_TYPE_TEXT: i64 = 1;
const ITEM_TYPE_IMAGE: i64 = 2;
const ITEM_TYPE_VOICE: i64 = 3;
const ITEM_TYPE_FILE: i64 = 4;
const ITEM_TYPE_VIDEO: i64 = 5;

const MAX_MEDIA_SIZE: usize = 100 * 1024 * 1024;
const CDN_MARKER_PREFIX: &str = "weixin-cdn:";

// Session lifecycle.
const SESSION_EXPIRED_CODE: i64 = -14;
const AUTH_FAILED_CODE: i64 = -2;
/// getUpdates -14 means the iLink session expired and must be re-established by
/// re-scanning the QR. Pausing avoids hammering the API.
const SESSION_PAUSE_S: f64 = 300.0;

// Typing indicator status values. status=2 clears the WeChat "thinking" light.
const TYPING_START: i64 = 1;
const TYPING_STOP: i64 = 2;
/// One placeholder bubble text (mirrors `_PLACEHOLDER_TEXT`).
const PLACEHOLDER_TEXT: &str = "⏳ 思考中...";
/// How long to wait for the thinking light before sending one placeholder bubble
/// (mirrors `_PLACEHOLDER_WAIT_S`).
const PLACEHOLDER_WAIT_S: f64 = 4.0;
/// Thinking-light refresh interval (mirrors `_TYPING_KEEPALIVE_S`).
const TYPING_KEEPALIVE_S: f64 = 5.0;
/// Thinking-light calls must not hold up the answer.
const TYPING_HTTP_TIMEOUT_S: f64 = 3.0;

/// A JSON object of context tokens keyed by `account_id:user_id`.
type ContextTokenMap = HashMap<String, String>;

/// Callback used to persist a refreshed context token.
/// Mirrors Python `ContextTokenListener` — signature `(account_id, user_id, token, ms)`.
pub type ContextTokenListener = Arc<dyn Fn(String, String, String, i64) + Send + Sync>;

// ---------------------------------------------------------------------------
// Config (mirrors `WeixinAccountConfig` / `WeixinConfig`)
// ---------------------------------------------------------------------------

/// Single WeChat iLink account configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WeixinAccountConfig {
    pub account_id: String,
    pub token: String,
    pub account_name: String,
    pub base_url: String,
    pub bot_uin: String,
    pub user_uin: String,
}

impl Default for WeixinAccountConfig {
    fn default() -> Self {
        WeixinAccountConfig {
            account_id: String::new(),
            token: String::new(),
            account_name: String::new(),
            base_url: API_BASE.to_string(),
            bot_uin: String::new(),
            user_uin: String::new(),
        }
    }
}

/// WeChat iLink channel configuration.
///
/// Attributes mirror the Python dataclass:
///   - `accounts`: one entry per bot account (`account_id` + `token`).
///   - `media_dir`: local directory for media file storage.
///   - `base`: embedded [`ChannelConfig`] (channel_id / tenant_id / display flags).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WeixinConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    pub accounts: Vec<WeixinAccountConfig>,
    pub media_dir: String,
}

impl Default for WeixinConfig {
    fn default() -> Self {
        WeixinConfig {
            base: ChannelConfig::default(),
            accounts: Vec::new(),
            media_dir: "~/.lightclaw/media".to_string(),
        }
    }
}

impl WeixinConfig {
    /// Parse flat `{token, bot_uin}` or `accounts[]` config.
    ///
    /// Faithful port of `WeixinConfig.from_dict`: lenient — tokenless / malformed
    /// account entries are skipped rather than raising, so completeness is
    /// reported uniformly via the `start()` credentials check.
    ///
    /// NOTE: the shared `build_channel` dispatcher drives construction through
    /// `serde::Deserialize`, so this helper is API-parity only (used when a
    /// caller wants the Python `from_dict` semantics directly).
    pub fn from_dict(data: &Value) -> WeixinConfig {
        let known = [
            "channel_id",
            "tenant_id",
            "show_thinking",
            "show_tool_hints",
            "group_context",
            "media_dir",
        ];
        let mut cfg = WeixinConfig::default();
        if let Some(obj) = data.as_object() {
            for k in known {
                if k != "accounts" {
                    if let Some(v) = obj.get(k) {
                        apply_base_field(&mut cfg, k, v);
                    }
                }
            }
            if let Some(md) = obj.get("media_dir").and_then(|v| v.as_str()) {
                cfg.media_dir = md.to_string();
            }
        }

        let mut accounts: Vec<WeixinAccountConfig> = Vec::new();
        match data.get("accounts") {
            Some(Value::Array(items)) => {
                for item in items {
                    let item = match item.as_object() {
                        Some(o) => o,
                        None => continue,
                    };
                    let token = item
                        .get("token")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if token.is_empty() {
                        continue;
                    }
                    let account_id = item
                        .get("account_id")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| item.get("bot_uin").and_then(|v| v.as_str()))
                        .filter(|s| !s.is_empty())
                        .unwrap_or("weixin")
                        .to_string();
                    let base_url = item
                        .get("base_url")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or(API_BASE)
                        .to_string();
                    let bot_uin = item
                        .get("bot_uin")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| item.get("account_id").and_then(|v| v.as_str()))
                        .unwrap_or("")
                        .to_string();
                    accounts.push(WeixinAccountConfig {
                        account_id,
                        token,
                        account_name: item
                            .get("account_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        base_url,
                        bot_uin,
                        user_uin: item
                            .get("user_uin")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    });
                }
            }
            _ => {
                let token = data
                    .get("token")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if !token.is_empty() {
                    let account_id = data
                        .get("account_id")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| data.get("bot_uin").and_then(|v| v.as_str()))
                        .filter(|s| !s.is_empty())
                        .unwrap_or("weixin")
                        .to_string();
                    let base_url = data
                        .get("base_url")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or(API_BASE)
                        .to_string();
                    let bot_uin = data
                        .get("bot_uin")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or(account_id.as_str())
                        .to_string();
                    accounts.push(WeixinAccountConfig {
                        account_id,
                        token,
                        account_name: data
                            .get("account_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        base_url,
                        bot_uin,
                        user_uin: data
                            .get("user_uin")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    });
                }
            }
        }
        cfg.accounts = accounts;
        cfg
    }
}

fn apply_base_field(cfg: &mut WeixinConfig, key: &str, v: &Value) {
    match key {
        "channel_id" => cfg.base.channel_id = v.as_str().map(|s| s.to_string()),
        "tenant_id" => cfg.base.tenant_id = v.as_str().map(|s| s.to_string()),
        "show_thinking" => cfg.base.show_thinking = v.as_bool().unwrap_or(false),
        "show_tool_hints" => cfg.base.show_tool_hints = v.as_bool().unwrap_or(false),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Shared cross-task state (context token cache)
// ---------------------------------------------------------------------------

/// Cloneable handle to the shared `account_id:user_id -> context_token` map.
/// Mirrors Python `self._context_tokens` (shared between the poll loop, which
/// writes refreshed tokens on dispatch, and the send path).
#[derive(Clone)]
struct ContextTokens(Arc<tokio::sync::Mutex<ContextTokenMap>>);

impl Default for ContextTokens {
    fn default() -> Self {
        ContextTokens(Arc::new(tokio::sync::Mutex::new(ContextTokenMap::new())))
    }
}

impl ContextTokens {
    async fn get(&self, key: &str) -> Option<String> {
        self.0.lock().await.get(key).cloned()
    }
    async fn set(&self, key: String, value: String) {
        self.0.lock().await.insert(key, value);
    }
}

fn context_cache_key(account_id: &str, user_id: &str) -> String {
    format!("{account_id}:{user_id}")
}

// ---------------------------------------------------------------------------
// Macro scaffolding
// ---------------------------------------------------------------------------

crate::channel_adapter! {
    /// Weixin (iLink Bot) channel.
    WeixinChannel, WeixinConfig, "weixin",
    // `accounts` is a `Vec`, so the macro's `required = [...]` (`.is_empty()` on
    // String fields) can't express the Python `missing_credentials` check. The
    // validation is performed inside `start()` instead (see the NOTE there).
    required = [],
    fields = {
        poll_tasks: Vec<tokio::task::JoinHandle<()>>,
        running: Arc<AtomicBool>,
        context_tokens: ContextTokens,
        listener: Option<ContextTokenListener>,
    }
}

impl WeixinChannel {
    /// Persist a refreshed token. The listener must not log the token.
    pub fn set_context_token_listener(&mut self, listener: Option<ContextTokenListener>) {
        self.listener = listener;
    }

    // --- Internal ---------------------------------------------------------

    fn get_account(&self, account_id: &str) -> Option<WeixinAccountConfig> {
        for acc in &self.config.accounts {
            if acc.account_id == account_id {
                return Some(acc.clone());
            }
        }
        self.config.accounts.first().cloned()
    }

    /// Build an API client bound to the channel's shared HTTP session.
    async fn make_api(
        &self,
        account: &WeixinAccountConfig,
        timeout_s: Option<f64>,
    ) -> Result<api::WeixinAPIClient> {
        let session = self.core.ensure_http().await?;
        let uin = if !account.bot_uin.is_empty() {
            account.bot_uin.clone()
        } else {
            account.user_uin.clone()
        };
        Ok(api::WeixinAPIClient::new(
            account.base_url.clone(),
            account.token.clone(),
            session,
            timeout_s,
            uin,
        ))
    }

    fn user_key(&self, subject: &ChannelSubject) -> String {
        let key = subject
            .metadata
            .get("from_user_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(subject.subject_id.as_str())
            .to_string();
        key
    }

    /// Prefer the live cache, then the durable session copy, then this turn's token.
    async fn resolve_context_token(&self, account_id: &str, to_user_id: &str, meta: &HashMap<String, Value>) -> String {
        if let Some(cached) = self.context_tokens.get(&context_cache_key(account_id, to_user_id)).await {
            if !cached.is_empty() {
                return cached;
            }
        }
        let durable = meta
            .get(WEIXIN_CONTEXT_TOKEN_KEY)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if !durable.is_empty() {
            return durable;
        }
        meta.get("context_token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    async fn remember_context_token(&self, account_id: &str, user_id: &str, token: &str) {
        let cleaned = token.trim();
        if cleaned.is_empty() || user_id.is_empty() {
            return;
        }
        self.context_tokens
            .set(context_cache_key(account_id, user_id), cleaned.to_string())
            .await;
        if let Some(listener) = &self.listener {
            let ms = (crate::channel::now_f64() * 1000.0) as i64;
            listener(
                account_id.to_string(),
                user_id.to_string(),
                cleaned.to_string(),
                ms,
            );
        }
    }

    // --- Outbound text ----------------------------------------------------

    async fn dispatch_text(
        &mut self,
        subject: &ChannelSubject,
        text: &str,
        prepaid_first: bool,
    ) -> Result<()> {
        let parts = text::split_weixin_text(text);
        let total = parts.len();
        for (index, part) in parts.iter().enumerate() {
            if !(prepaid_first && index == 0) {
                self.acquire_rate_slot().await;
            }
            self.post_text(subject, part, index, total).await?;
        }
        Ok(())
    }

    async fn post_text(
        &mut self,
        subject: &ChannelSubject,
        text: &str,
        index: usize,
        total: usize,
    ) -> Result<()> {
        let meta = subject.metadata.clone();
        let account_id = meta
            .get("account_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let account = match self.get_account(&account_id) {
            Some(a) => a,
            None => {
                tracing::warn!("WeixinChannel: no account for send (account_id={account_id})");
                return Ok(());
            }
        };
        let user_id = meta
            .get("from_user_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(subject.subject_id.as_str())
            .to_string();

        let mut context_token = self
            .resolve_context_token(&account.account_id, &user_id, &meta)
            .await;
        let api = self.make_api(&account, None).await?;
        let mut last_error: Option<types::WeixinAPIError> = None;
        for _attempt in 0..2 {
            match api.send_message(&user_id, text, &context_token).await {
                Ok(resp) => {
                    let refreshed = resp
                        .context_token
                        .clone()
                        .filter(|s| !s.is_empty())
                        .or_else(|| {
                            resp.data
                                .get("context_token")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    if !refreshed.is_empty() {
                        self.remember_context_token(&account.account_id, &user_id, &refreshed)
                            .await;
                    }
                    tracing::info!(
                        "WeixinChannel send ok: user={} chars={} part={}/{} message_id={:?} ret={:?}",
                        short(&user_id),
                        text.chars().count(),
                        index + 1,
                        total,
                        resp.message_id,
                        resp.ret
                    );
                    return Ok(());
                }
                Err(exc) => {
                    tracing::warn!(
                        "WeixinChannel send failed: user={} chars={} part={}/{} ret={} errcode={:?} errmsg={:?}",
                        short(&user_id),
                        text.chars().count(),
                        index + 1,
                        total,
                        exc.ret,
                        exc.errcode,
                        exc.errmsg
                    );
                    last_error = Some(exc.clone());
                    if !context_token.is_empty()
                        && (exc.ret == SESSION_EXPIRED_CODE || exc.ret == AUTH_FAILED_CODE)
                    {
                        context_token.clear();
                        continue;
                    }
                    return Err(exc.into());
                }
            }
        }
        match last_error {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    // --- Outbound media ---------------------------------------------------

    async fn deliver_media(&self, subject: &ChannelSubject, media: &ContentPart) -> bool {
        let meta = &subject.metadata;
        let account_id = meta
            .get("account_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let account = match self.get_account(&account_id) {
            Some(a) => a,
            None => {
                tracing::warn!("WeixinChannel: no account for media send (account_id={account_id})");
                return false;
            }
        };

        let to_user_id = meta
            .get("from_user_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| meta.get("ilink_user_id").and_then(|v| v.as_str()).filter(|s| !s.is_empty()))
            .unwrap_or(subject.subject_id.as_str())
            .to_string();
        if to_user_id.is_empty() {
            tracing::warn!("WeixinChannel: no target user for media send");
            return false;
        }

        let (media_bytes, _mime) = match self.load_media_bytes(media).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("WeixinChannel failed to load outbound media: {e}");
                return false;
            }
        };

        if media_bytes.is_empty() || media_bytes.len() > MAX_MEDIA_SIZE {
            tracing::warn!(
                "WeixinChannel outbound media invalid size={}",
                media_bytes.len()
            );
            return false;
        }

        let item = match media {
            ContentPart::Image(_) => {
                self.upload_media_part(
                    &media_bytes,
                    &account,
                    &to_user_id,
                    MEDIA_TYPE_IMAGE,
                    ITEM_TYPE_IMAGE,
                    "image_item",
                )
                .await
            }
            ContentPart::File(f) => {
                let mut it = self
                    .upload_media_part(
                        &media_bytes,
                        &account,
                        &to_user_id,
                        MEDIA_TYPE_FILE,
                        ITEM_TYPE_FILE,
                        "file_item",
                    )
                    .await;
                if let Some(Value::Object(o)) = it.as_mut() {
                    if let Some(Value::Object(fo)) = o.get_mut("file_item") {
                        let name = if f.filename.is_empty() {
                            "file".to_string()
                        } else {
                            f.filename.clone()
                        };
                        fo.insert("file_name".to_string(), Value::String(name));
                    }
                }
                it
            }
            ContentPart::Video(_) => {
                self.upload_media_part(
                    &media_bytes,
                    &account,
                    &to_user_id,
                    MEDIA_TYPE_VIDEO,
                    ITEM_TYPE_VIDEO,
                    "video_item",
                )
                .await
            }
            _ => None,
        };

        let item = match item {
            Some(i) => i,
            None => return false,
        };

        let context_token = self
            .resolve_context_token(&account.account_id, &to_user_id, meta)
            .await;
        let api = match self.make_api(&account, None).await {
            Ok(a) => a,
            Err(_) => return false,
        };
        match api.send_items(&to_user_id, &[item], &context_token).await {
            Ok(resp) => {
                let refreshed = resp
                    .context_token
                    .clone()
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        resp.data
                            .get("context_token")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default();
                if !refreshed.is_empty() {
                    self.context_tokens
                        .set(
                            context_cache_key(&account.account_id, &to_user_id),
                            refreshed,
                        )
                        .await;
                }
                tracing::info!(
                    "WeixinChannel media sent: type={} user={}",
                    media.label(),
                    short(&to_user_id)
                );
                true
            }
            Err(exc) => {
                tracing::warn!("WeixinChannel media send failed: {exc}");
                false
            }
        }
    }

    async fn upload_media_part(
        &self,
        data: &[u8],
        account: &WeixinAccountConfig,
        to_user_id: &str,
        media_type: i64,
        item_type: i64,
        item_key: &str,
    ) -> Option<Value> {
        let aes_key = media::generate_aes_key();
        let aes_key_hex = hex::encode(&aes_key);
        let raw_md5 = media::md5_hex(data);
        let uuid = uuid::Uuid::new_v4().simple().to_string();
        let filekey = format!("harness_{}_{}", &uuid[..8], &raw_md5[..8]);
        let upload_request = json!({
            "filekey": filekey,
            "media_type": media_type,
            "to_user_id": to_user_id,
            "rawsize": data.len(),
            "rawfilemd5": raw_md5,
            "filesize": media::encrypted_size(data.len()),
            "no_need_thumb": true,
            "aeskey": aes_key_hex,
        });

        let api = match self.make_api(account, None).await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("WeixinChannel getuploadurl failed: {e}");
                return None;
            }
        };
        let upload_resp = match api.get_upload_url(&upload_request).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("WeixinChannel getuploadurl failed: {e}");
                return None;
            }
        };

        let upload_data = upload_resp
            .get("data")
            .filter(|v| v.is_object())
            .unwrap_or(&upload_resp);
        let upload_url = upload_data
            .get("upload_full_url")
            .or_else(|| upload_data.get("uploadFullUrl"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let upload_param = upload_data
            .get("upload_param")
            .or_else(|| upload_data.get("uploadParam"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let upload_url = if upload_url.is_empty() && !upload_param.is_empty() {
            media::build_upload_url(&upload_param, &filekey)
        } else {
            upload_url
        };
        if upload_url.is_empty() {
            tracing::warn!("WeixinChannel getuploadurl response missing upload URL");
            return None;
        }

        let http = match self.core.ensure_http().await {
            Ok(h) => h,
            Err(_) => return None,
        };
        let encrypted_param =
            match media::encrypt_and_upload(&http, &upload_url, data, &aes_key).await {
                Some(p) => p,
                None => return None,
            };

        let encoded_key = base64::engine::general_purpose::STANDARD.encode(aes_key_hex.as_bytes());
        let mut inner = json!({
            "media": {
                "encrypt_query_param": encrypted_param,
                "aes_key": encoded_key,
                "encrypt_type": 1,
            }
        });
        if item_key == "image_item" {
            inner["mid_size"] = json!(media::encrypted_size(data.len()));
        } else if item_key == "video_item" {
            inner["video_size"] = json!(media::encrypted_size(data.len()));
        } else if item_key == "file_item" {
            inner["len"] = json!(data.len().to_string());
            inner["rawfilemd5"] = json!(raw_md5);
        }
        let mut item = json!({ "type": item_type });
        item[item_key] = inner;
        Some(item)
    }

    async fn send_media_fallback(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let url = media.url().unwrap_or("").to_string();
        let label = media.label_with_filename();
        if !url.is_empty() {
            self.dispatch_text(subject, &format!("[{label}: {url}]"), true)
                .await?;
        } else if media.data().is_some() || media.local_path().is_some() {
            self.dispatch_text(subject, &format!("[{label} (local file)]"), true)
                .await?;
        }
        Ok(())
    }

    // --- Typing indicator -------------------------------------------------

    async fn typing_once(&self, subject: &ChannelSubject, status: i64) -> bool {
        let meta = &subject.metadata;
        let account = match self.get_account(
            meta.get("account_id")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ) {
            Some(a) => a,
            None => return false,
        };
        let user_id = meta
            .get("from_user_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(subject.subject_id.as_str())
            .to_string();
        if user_id.is_empty() {
            return false;
        }
        let context_token = self
            .resolve_context_token(&account.account_id, &user_id, meta)
            .await;
        let api = match self.make_api(&account, Some(TYPING_HTTP_TIMEOUT_S)).await {
            Ok(a) => a,
            Err(_) => return false,
        };
        let config_resp = match api.get_config(&user_id, &context_token).await {
            Ok(c) => c,
            Err(exc) => {
                tracing::warn!(
                    "WeixinChannel typing status={} failed: user={} ret={} errcode={:?} errmsg={:?}",
                    status,
                    short(&user_id),
                    exc.ret,
                    exc.errcode,
                    exc.errmsg
                );
                return false;
            }
        };
        let ticket = config_resp.typing_ticket.unwrap_or_default();
        if ticket.is_empty() {
            tracing::warn!(
                "WeixinChannel typing status={} failed: user={} missing typing_ticket",
                status,
                short(&user_id)
            );
            return false;
        }
        match api.send_typing(&user_id, &ticket, status).await {
            Ok(()) => true,
            Err(exc) => {
                tracing::warn!(
                    "WeixinChannel typing status={} failed: user={} ret={} errcode={:?} errmsg={:?}",
                    status,
                    short(&user_id),
                    exc.ret,
                    exc.errcode,
                    exc.errmsg
                );
                false
            }
        }
    }

    // --- Inbound pipeline turnaround --------------------------------------

    /// Standard inbound pipeline extended with the weixin "turn" side-effects.
    ///
    /// Faithful port of Python `WeixinChannel._process_inbound`, which wraps
    /// `super()._process_inbound(...)` with a concurrent `_typing_loop` task and
    /// a one-shot `_placeholder_after` task, then runs `_finish_turn` in a
    /// `finally`. A concurrent task here cannot borrow `&mut self` at the same
    /// time as the pipeline, so the body of
    /// [`Channel::default_process_inbound_body`] is reproduced inline and its
    /// `tokio::select!` loop is extended with:
    ///   - a typing-keepalive branch (`_typing_once(status=1)` every
    ///     `_TYPING_KEEPALIVE_S`), and
    ///   - a one-shot placeholder branch (`_PLACEHOLDER_WAIT_S` after which, if
    ///     the thinking light never came on and nothing was sent, one
    ///     `_PLACEHOLDER_TEXT` bubble is sent).
    /// The `finally` typing stop (`_stop_typing` → `status=2`) runs after the
    /// loop, mirroring `_finish_turn`.
    async fn process_inbound_with_turn(
        &mut self,
        message: &InboundMessage,
        subject: &ChannelSubject,
    ) -> bool {
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
            Some(crate::constraints::ReplyTimeoutGuard::spawn(
                constraints.reply_timeout,
                to_tx,
            ))
        } else {
            None
        };
        let mut typing_keepalive = if constraints.typing_keepalive_interval > 0.0 {
            Some(crate::constraints::TypingKeepalive::spawn(
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

        // Weixin turn state (mirrors `_WeixinTurn`).
        //
        // `typing_ok` mirrors `turn.typing_ok` — whether a status=1 ever landed.
        let mut typing_ok = false;
        // `content_enqueued` mirrors `turn.content_enqueued` — whether the
        // pipeline has already sent anything to the user this turn.
        let mut content_enqueued = false;
        // Mirrors the one-shot `_placeholder_after` task: armed until the wait
        // elapses (or the turn ends), then disarmed so it can never re-fire.
        let mut placeholder_armed = true;

        let cancel_guards = |timeout_guard: &mut Option<crate::constraints::ReplyTimeoutGuard>,
                             typing_keepalive: &mut Option<crate::constraints::TypingKeepalive>| {
            if let Some(g) = timeout_guard.take() {
                drop(g);
            }
            if let Some(k) = typing_keepalive.take() {
                drop(k);
            }
        };

        // One-shot placeholder timer (`_placeholder_after`'s sleep). Armed at
        // turn start, like the Python task spawned before the pipeline runs.
        let placeholder_sleep = tokio::time::sleep(Duration::from_secs_f64(PLACEHOLDER_WAIT_S));
        tokio::pin!(placeholder_sleep);

        // Python `_typing_loop` runs its first `_typing_once(status=1)`
        // immediately (the task is spawned before the pipeline starts).
        if self.typing_once(subject, TYPING_START).await {
            typing_ok = true;
        }

        // Periodic refresh. The first tick is deferred one period so it does not
        // duplicate the immediate send above; afterwards it fires every
        // `_TYPING_KEEPALIVE_S`, like the Python loop.
        let mut typing_interval = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs_f64(TYPING_KEEPALIVE_S),
            Duration::from_secs_f64(TYPING_KEEPALIVE_S),
        );

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
                                                content_enqueued = true;
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
                                        content_enqueued = true;
                                        let _ = self.rate_limited_send(subject, &formatted).await;
                                    }
                                    if !delta_buffer.is_empty() {
                                        let d = std::mem::take(&mut delta_buffer);
                                        content_enqueued = true;
                                        let _ = self.rate_limited_send(subject, &d).await;
                                    }
                                }
                                MessageEventType::Completed => {
                                    // Terminal branch: the turn is over, so the
                                    // placeholder can never fire again and
                                    // `content_enqueued` is moot here (in Python
                                    // `_finish_turn` cancels the placeholder task).
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
                                    // Marks both the buffer flush above and the
                                    // `deliver_event` fan-out below as content.
                                    content_enqueued = true;
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
                                    content_enqueued = true;
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
                        content_enqueued = true;
                        let _ = self.rate_limited_send(subject, &constraints.placeholder_text()).await;
                    }
                }
                _ = ty_rx.recv() => {
                    let _ = self.send_typing_indicator(subject).await;
                }
                _ = typing_interval.tick() => {
                    // Mirrors the next `_typing_loop` iteration.
                    if self.typing_once(subject, TYPING_START).await {
                        typing_ok = true;
                    }
                }
                _ = &mut placeholder_sleep, if placeholder_armed => {
                    // Mirrors `_placeholder_after`: one bubble, only if the
                    // thinking light never came on and nothing was sent yet.
                    placeholder_armed = false;
                    if !content_enqueued && !typing_ok {
                        content_enqueued = true;
                        let _ = self.rate_limited_send(subject, PLACEHOLDER_TEXT).await;
                    }
                }
            }
        }

        // `finally` semantics (mirrors `_finish_turn` → `_stop_typing`): clear
        // the thinking light, but only if a status=1 was actually delivered.
        // `_stop_typing` attempts status=2 and retries once on failure.
        if typing_ok {
            if !self.typing_once(subject, TYPING_STOP).await {
                let _ = self.typing_once(subject, TYPING_STOP).await;
            }
        }

        processing_succeeded
    }

    // --- Inbound media resolution -----------------------------------------

    async fn resolve_inbound_media(&self, message: &mut InboundMessage) -> Result<()> {
        let indices: Vec<usize> = message
            .content
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                p.local_path()
                    .map(|lp| lp.starts_with(CDN_MARKER_PREFIX))
                    .unwrap_or(false)
            })
            .map(|(i, _)| i)
            .collect();
        if indices.is_empty() {
            return Ok(());
        }

        let http = self.core.ensure_http().await?;
        let backend = self.core.media_backend().cloned();
        for index in indices {
            let marker = message.content[index].local_path().unwrap_or("").to_string();
            let info: Value = match marker
                .strip_prefix(CDN_MARKER_PREFIX)
                .and_then(|s| serde_json::from_str(s).ok())
            {
                Some(v) => v,
                None => {
                    set_part_local_path(&mut message.content[index], None);
                    continue;
                }
            };
            let media_obj = info.get("media").cloned().unwrap_or_else(|| json!({}));
            let aeskey = info.get("aeskey").and_then(|v| v.as_str()).unwrap_or("");
            let data = match media::download_and_decrypt(&http, &media_obj, aeskey).await {
                Some(d) if !d.is_empty() => d,
                _ => {
                    tracing::warn!("WeixinChannel failed to download inbound media");
                    set_part_local_path(&mut message.content[index], None);
                    continue;
                }
            };

            let filename = inbound_filename(&message.content[index], &info, &data, index);
            let mime = message.content[index]
                .mime_type()
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| {
                    mime_guess::from_path(&filename)
                        .first()
                        .map(|m| m.to_string())
                        .unwrap_or_else(|| "application/octet-stream".to_string())
                });
            set_part_mime(&mut message.content[index], Some(mime));
            set_part_size(&mut message.content[index], Some(data.len() as u64));
            if part_is_file(&message.content[index]) {
                set_part_filename_if_empty(&mut message.content[index], filename.clone());
            }

            match &backend {
                Some(backend) => {
                    let key = format!(
                        "{}/{}/{}_{}",
                        self.core.channel_type(),
                        self.core.channel_id(),
                        crate::channel::now_secs(),
                        filename
                    );
                    if backend.save(&data, &key).await.is_ok() {
                        set_part_local_path(&mut message.content[index], Some(key));
                        set_part_url(&mut message.content[index], String::new());
                    } else {
                        set_part_local_path(&mut message.content[index], None);
                    }
                }
                None => {
                    set_part_local_path(&mut message.content[index], None);
                    set_part_data(
                        &mut message.content[index],
                        Some(base64::engine::general_purpose::STANDARD.encode(&data)),
                    );
                }
            }
            tracing::info!(
                "WeixinChannel downloaded {}: {} ({} bytes)",
                message.content[index].label(),
                filename,
                data.len()
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Channel impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for WeixinChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            reply_timeout: 0.0,
            timeout_strategy: "none".to_string(),
            send_rate_limit: Some((5, 60.0)),
            typing_keepalive_interval: 0.0,
            show_thinking: false,
            show_tool_hints: false,
            placeholder_texts: vec![PLACEHOLDER_TEXT.to_string()],
            ..Default::default()
        }
    }

    // NOTE: Python validates `accounts` via `WeixinConfig.missing_credentials`
    // and raises `ChannelCredentialsError("weixin", missing or ["token"])` when
    // no poll task is started. Because `accounts` is a `Vec`, the macro's
    // `required = [...]` machinery can't check it, so the Python semantics are
    // reproduced here: each account must carry account_id / token / base_url.
    async fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        let http = self.core.ensure_http().await?;

        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for account in self.config.accounts.clone() {
            if !account.token.is_empty() {
                let enqueue = self.core.enqueue_sender();
                let running = self.running.clone();
                let tokens = self.context_tokens.clone();
                let http = http.clone();
                let handle = tokio::spawn(async move {
                    poll_loop(account, http, enqueue, running, tokens).await;
                });
                handles.push(handle);
            }
        }
        self.poll_tasks = handles;

        if self.poll_tasks.is_empty() {
            let mut missing: Vec<String> = Vec::new();
            if self.config.accounts.is_empty() {
                missing.push("token".to_string());
            } else {
                for acc in &self.config.accounts {
                    if acc.account_id.is_empty() {
                        missing.push("account_id".to_string());
                    }
                    if acc.token.is_empty() {
                        missing.push("token".to_string());
                    }
                    if acc.base_url.is_empty() {
                        missing.push("base_url".to_string());
                    }
                }
                if missing.is_empty() {
                    missing.push("token".to_string());
                }
            }
            return Err(GatewayError::MissingCredentials {
                kind: self.core.channel_type().to_string(),
                missing,
            });
        }

        tracing::info!("WeixinChannel started ({} accounts)", self.poll_tasks.len());
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        for task in self.poll_tasks.drain(..) {
            task.abort();
        }
        tracing::info!("WeixinChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        // iLink has no 5-second passive-reply window; text is sent via sendMessage.
        self.dispatch_text(subject, text, true).await
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let sent = self.deliver_media(subject, media).await;
        if !sent {
            self.send_media_fallback(subject, media).await?;
        }
        Ok(())
    }

    async fn preprocess_inbound(&mut self, message: &mut InboundMessage) -> Result<()> {
        // Resolve encrypted Weixin media before shared inbound policy.
        self.resolve_inbound_media(message).await
    }

    /// Overrides the default pipeline to add the weixin thinking-light and
    /// one-shot placeholder bubble around the standard processing loop.
    ///
    /// Mirrors Python `WeixinChannel._process_inbound` (which delegates to
    /// `super()._process_inbound` while its typing/placeholder tasks run). The
    /// standard body is reproduced in [`Self::process_inbound_with_turn`]
    /// because the concurrent tasks it needs cannot share `&mut self`.
    async fn process_inbound(&mut self, message: &InboundMessage, subject: &ChannelSubject) -> bool {
        self.process_inbound_with_turn(message, subject).await
    }

    async fn send_typing_indicator(&mut self, subject: &ChannelSubject) -> Result<()> {
        // One thinking-light refresh (status=1). Failures are warned internally.
        let _ = self.typing_once(subject, TYPING_START).await;
        Ok(())
    }

    /// Download media URLs, attaching iLink bot auth headers when available.
    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.core.ensure_http().await?;
        let mut req = http.get(url);
        let token = self
            .config
            .accounts
            .first()
            .map(|a| a.token.clone())
            .unwrap_or_default();
        if !token.is_empty() {
            req = req
                .header("AuthorizationType", "ilink_bot_token")
                .header("Authorization", format!("Bearer {token}"));
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

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        let mut out = alias_subject_fields(
            meta,
            &subject.subject_id,
            &["from_user_id", "ilink_user_id", "to_handle"],
        );
        let has_account = out
            .get("account_id")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_account && !out.contains_key("account_id") {
            if let Some(acc) = self.config.accounts.first() {
                out.insert("account_id".to_string(), json!(acc.account_id));
            }
        }
        out
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let value = raw.as_json().cloned().unwrap_or(Value::Null);
        let obj = value.as_object();

        let get = |k: &str| -> String {
            obj.and_then(|o| o.get(k))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let account_id = get("_account_id");
        let from_user_id = first_nonempty(&[get("from_user_id"), get("fromUserId"), get("ilink_user_id")]);
        let mut session_id = first_nonempty(&[get("session_id"), get("sessionId")]);
        if session_id.is_empty() {
            session_id = from_user_id.clone();
        }
        let context_token = first_nonempty(&[get("context_token"), get("contextToken")]);

        let mut content_parts: Vec<ContentPart> = Vec::new();
        let items: Vec<Value> = obj
            .and_then(|o| {
                o.get("item_list")
                    .or_else(|| o.get("itemList"))
                    .or_else(|| o.get("msg_items"))
            })
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for item in &items {
            if !item.is_object() {
                continue;
            }
            let item_type = item.get("type").and_then(|v| v.as_i64());
            match item_type {
                Some(ITEM_TYPE_TEXT) => {
                    let text_item = item_payload(item, "text_item");
                    let text = first_nonempty(&[
                        str_of(text_item.get("text")),
                        str_of(item.get("content")),
                    ]);
                    if !text.is_empty() {
                        content_parts.push(ContentPart::Text(TextContent { text }));
                    }
                }
                Some(ITEM_TYPE_IMAGE) => {
                    let image_item = item_payload(item, "image_item");
                    let media = cdn_media(image_item.get("media"));
                    let aeskey = first_nonempty(&[
                        str_of(image_item.get("aeskey")),
                        str_of(image_item.get("aesKey")),
                    ]);
                    let url = first_nonempty(&[
                        str_of(image_item.get("url")),
                        str_of(item.get("url")),
                    ]);
                    if let Some(m) = media {
                        content_parts.push(ContentPart::Image(ImageContent {
                            local_path: Some(cdn_marker(&m, &aeskey, "image", "")),
                            ..Default::default()
                        }));
                    } else if !url.is_empty() {
                        content_parts.push(ContentPart::Image(ImageContent {
                            url,
                            ..Default::default()
                        }));
                    }
                }
                Some(ITEM_TYPE_FILE) => {
                    let file_item = item_payload(item, "file_item");
                    let media = cdn_media(file_item.get("media"));
                    let filename = first_nonempty(&[
                        str_of(file_item.get("file_name")),
                        str_of(file_item.get("fileName")),
                        str_of(item.get("filename")),
                        str_of(item.get("file_name")),
                    ]);
                    let url = first_nonempty(&[
                        str_of(file_item.get("url")),
                        str_of(item.get("url")),
                    ]);
                    if let Some(m) = media {
                        content_parts.push(ContentPart::File(FileContent {
                            filename: filename.clone(),
                            local_path: Some(cdn_marker(&m, "", "file", &filename)),
                            ..Default::default()
                        }));
                    } else if !url.is_empty() {
                        content_parts.push(ContentPart::File(FileContent {
                            url,
                            filename,
                            ..Default::default()
                        }));
                    }
                }
                Some(ITEM_TYPE_VIDEO) => {
                    let video_item = item_payload(item, "video_item");
                    let media = cdn_media(video_item.get("media"));
                    let url = first_nonempty(&[
                        str_of(video_item.get("url")),
                        str_of(item.get("url")),
                    ]);
                    if let Some(m) = media {
                        content_parts.push(ContentPart::Video(VideoContent {
                            local_path: Some(cdn_marker(&m, "", "video", "")),
                            ..Default::default()
                        }));
                    } else if !url.is_empty() {
                        content_parts.push(ContentPart::Video(VideoContent {
                            url,
                            ..Default::default()
                        }));
                    }
                }
                Some(ITEM_TYPE_VOICE) => {
                    let voice_item = item_payload(item, "voice_item");
                    let text = str_of(voice_item.get("text"));
                    if !text.is_empty() {
                        content_parts.push(ContentPart::Text(TextContent { text }));
                    }
                }
                _ => {}
            }
        }

        if content_parts.is_empty() {
            content_parts.push(ContentPart::Text(TextContent {
                text: str_of(obj.and_then(|o| o.get("content"))),
            }));
        }

        let msg_id = obj
            .and_then(|o| o.get("message_id").or_else(|| o.get("messageId")))
            .map(value_to_string)
            .unwrap_or_default();

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("account_id".to_string(), json!(account_id));
        metadata.insert("from_user_id".to_string(), json!(from_user_id));
        metadata.insert("ilink_user_id".to_string(), json!(from_user_id));
        metadata.insert("to_handle".to_string(), json!(from_user_id));
        metadata.insert("context_token".to_string(), json!(context_token));
        metadata.insert("_weixin_msg_id".to_string(), json!(msg_id));

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "weixin".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject::new(from_user_id.clone())),
            channel_session_id: session_id,
            content: content_parts,
            group_context: None,
            metadata,
            timestamp: crate::channel::now_f64(),
        })
    }
}

// ---------------------------------------------------------------------------
// Poll loop + dispatch (free fns, mirror `_poll_loop` / `_dispatch_message`)
// ---------------------------------------------------------------------------

async fn poll_loop(
    account: WeixinAccountConfig,
    http: reqwest::Client,
    enqueue: Option<EnqueueFn>,
    running: Arc<AtomicBool>,
    tokens: ContextTokens,
) {
    let account_id = account.account_id.clone();
    tracing::info!("WeChat poll loop started: account={account_id}");

    let uin = if !account.bot_uin.is_empty() {
        account.bot_uin.clone()
    } else {
        account.user_uin.clone()
    };
    let api = api::WeixinAPIClient::new(
        account.base_url.clone(),
        account.token.clone(),
        http,
        None,
        uin,
    );

    let mut buf = String::new();
    let mut timeout_ms: i64 = DEFAULT_LONG_POLL_TIMEOUT_MS;
    let mut failures: u32 = 0;
    let mut pause_until: Option<f64> = None;

    while running.load(Ordering::SeqCst) {
        if let Some(until) = pause_until {
            let remaining = until - crate::channel::now_f64();
            if remaining > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(remaining.min(30.0))).await;
                continue;
            }
            pause_until = None;
        }

        match api.get_updates(&buf, Some(timeout_ms)).await {
            Ok(resp) => {
                failures = 0;
                if !resp.get_updates_buf.is_empty() {
                    buf = resp.get_updates_buf.clone();
                }
                if let Some(t) = resp.longpolling_timeout_ms {
                    if t > 0 {
                        timeout_ms = t;
                    }
                }
                for msg in &resp.msgs {
                    dispatch_message(&account_id, msg, &enqueue, &tokens).await;
                }
            }
            Err(exc) => {
                // Long-poll idle timeout is normal — just loop again.
                if exc.timed_out {
                    continue;
                }
                if exc.errcode == Some(SESSION_EXPIRED_CODE) || exc.ret == SESSION_EXPIRED_CODE {
                    tracing::error!(
                        "WeChat {}: session expired (errcode -14) — pausing {:.0}min, re-scan QR to recover",
                        account_id,
                        SESSION_PAUSE_S / 60.0
                    );
                    pause_until = Some(crate::channel::now_f64() + SESSION_PAUSE_S);
                    failures = 0;
                    continue;
                }
                failures += 1;
                tracing::warn!("WeChat {account_id} poll failed: {exc} ({failures})");
                let backoff = 2f64.powi(failures as i32).min(60.0);
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
            }
        }
    }
}

/// Enqueue a single inbound user message for processing.
async fn dispatch_message(
    account_id: &str,
    msg: &types::WeixinMessage,
    enqueue: &Option<EnqueueFn>,
    tokens: &ContextTokens,
) {
    if msg.message_type != Some(MSG_TYPE_USER) {
        return;
    }
    let from_user_id = msg.from_user_id.clone().unwrap_or_default();
    if let Some(ct) = &msg.context_token {
        if !ct.is_empty() && !from_user_id.is_empty() {
            tokens
                .set(context_cache_key(account_id, &from_user_id), ct.clone())
                .await;
        }
    }
    let payload = json!({
        "_account_id": account_id,
        "from_user_id": from_user_id,
        "session_id": msg.session_id.clone().unwrap_or_else(|| from_user_id.clone()),
        "context_token": msg.context_token.clone().unwrap_or_default(),
        "message_id": msg.message_id,
        "item_list": msg.item_list.clone().unwrap_or_default(),
    });
    if let Some(cb) = enqueue {
        cb(RawPayload::Json(payload));
    }
}

// ---------------------------------------------------------------------------
// Small helpers (mirror the Python static helpers)
// ---------------------------------------------------------------------------

const NULL_VALUE: Value = Value::Null;

fn to_camel(snake: &str) -> String {
    let mut parts = snake.split('_');
    let mut out = String::from(parts.next().unwrap_or(""));
    for part in parts {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            out.push_str(&chars.as_str().to_ascii_lowercase());
        }
    }
    out
}

/// Port of `_item_payload`: read `snake_name`, else the camelCase alias.
fn item_payload<'a>(item: &'a Value, snake_name: &str) -> &'a Value {
    let camel = to_camel(snake_name);
    item.get(snake_name)
        .or_else(|| item.get(camel.as_str()))
        .unwrap_or(&NULL_VALUE)
}

/// Port of `_cdn_media`: normalize the CDN media descriptor.
fn cdn_media(media: Option<&Value>) -> Option<Value> {
    let m = media.and_then(|v| v.as_object())?;
    let encrypt_query_param = m
        .get("encrypt_query_param")
        .or_else(|| m.get("encryptQueryParam"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if encrypt_query_param.is_empty() {
        return None;
    }
    let aes_key = m
        .get("aes_key")
        .or_else(|| m.get("aesKey"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let encrypt_type = m
        .get("encrypt_type")
        .or_else(|| m.get("encryptType"))
        .cloned()
        .unwrap_or_else(|| json!(1));
    Some(json!({
        "encrypt_query_param": encrypt_query_param,
        "aes_key": aes_key,
        "encrypt_type": encrypt_type,
    }))
}

/// Port of `_cdn_marker`: encode the CDN descriptor into a `local_path` marker.
fn cdn_marker(media: &Value, aeskey: &str, kind: &str, filename: &str) -> String {
    let payload = json!({
        "media": media,
        "aeskey": aeskey,
        "kind": kind,
        "filename": filename,
    });
    format!("{CDN_MARKER_PREFIX}{payload}")
}

/// Port of `_inbound_filename` (`Path(raw_name).name` + magic-byte extension).
fn inbound_filename(part: &ContentPart, info: &Value, data: &[u8], index: usize) -> String {
    let mut raw_name = String::new();
    if let ContentPart::File(f) = part {
        raw_name = f.filename.clone();
    }
    if raw_name.is_empty() {
        raw_name = info
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
    }
    let base = std::path::Path::new(&raw_name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    if !base.is_empty() {
        return base;
    }
    let ext = media::detect_extension(data);
    let prefix = if part_is_image(part) {
        "image"
    } else if part_is_video(part) {
        "video"
    } else {
        "file"
    };
    format!("in_{prefix}_{index}{ext}")
}

fn first_nonempty(xs: &[String]) -> String {
    xs.iter().find(|s| !s.is_empty()).cloned().unwrap_or_default()
}

fn str_of(v: Option<&Value>) -> String {
    v.and_then(|x| x.as_str()).unwrap_or("").to_string()
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn short(s: &str) -> &str {
    // Python logs `user_id[:12]`; slicing a UTF-8 &str there is unsafe, so log
    // the whole id (it is an opaque handle, never a secret).
    let _ = s;
    s
}

fn part_is_file(part: &ContentPart) -> bool {
    matches!(part, ContentPart::File(_))
}
fn part_is_image(part: &ContentPart) -> bool {
    matches!(part, ContentPart::Image(_))
}
fn part_is_video(part: &ContentPart) -> bool {
    matches!(part, ContentPart::Video(_))
}

fn set_part_local_path(part: &mut ContentPart, v: Option<String>) {
    match part {
        ContentPart::Image(x) => x.local_path = v,
        ContentPart::Video(x) => x.local_path = v,
        ContentPart::Audio(x) => x.local_path = v,
        ContentPart::File(x) => x.local_path = v,
        ContentPart::Text(_) => {}
    }
}

fn set_part_url(part: &mut ContentPart, v: String) {
    match part {
        ContentPart::Image(x) => x.url = v,
        ContentPart::Video(x) => x.url = v,
        ContentPart::Audio(x) => x.url = v,
        ContentPart::File(x) => x.url = v,
        ContentPart::Text(_) => {}
    }
}

fn set_part_data(part: &mut ContentPart, v: Option<String>) {
    match part {
        ContentPart::Image(x) => x.data = v,
        ContentPart::Video(x) => x.data = v,
        ContentPart::Audio(x) => x.data = v,
        ContentPart::File(x) => x.data = v,
        ContentPart::Text(_) => {}
    }
}

fn set_part_mime(part: &mut ContentPart, v: Option<String>) {
    match part {
        ContentPart::Image(x) => x.mime_type = v,
        ContentPart::Video(x) => x.mime_type = v,
        ContentPart::Audio(x) => x.mime_type = v,
        ContentPart::File(x) => x.mime_type = v,
        ContentPart::Text(_) => {}
    }
}

fn set_part_size(part: &mut ContentPart, v: Option<u64>) {
    match part {
        ContentPart::Image(x) => x.size = v,
        ContentPart::Video(x) => x.size = v,
        ContentPart::Audio(x) => x.size = v,
        ContentPart::File(x) => x.size = v,
        ContentPart::Text(_) => {}
    }
}

fn set_part_filename_if_empty(part: &mut ContentPart, v: String) {
    if let ContentPart::File(f) = part {
        if f.filename.is_empty() {
            f.filename = v;
        }
    }
}

// ===========================================================================
// Inlined submodule: text.py
// ===========================================================================

pub mod text {
    /// Community iLink clients fail or truncate past about 2000 Unicode chars.
    pub const WEIXIN_TEXT_LIMIT: usize = 2000;

    /// Split *text* on paragraph, line, then space boundaries.
    /// Faithful port of `split_weixin_text` (character-indexed, not byte-indexed).
    pub fn split_weixin_text(text: &str) -> Vec<String> {
        split_weixin_text_limit(text, WEIXIN_TEXT_LIMIT)
    }

    pub fn split_weixin_text_limit(text: &str, limit: usize) -> Vec<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let mut remaining: Vec<char> = trimmed.chars().collect();
        if remaining.len() <= limit {
            return vec![remaining.iter().collect()];
        }

        let mut parts: Vec<String> = Vec::new();
        while !remaining.is_empty() {
            if remaining.len() <= limit {
                parts.push(remaining.iter().collect());
                break;
            }
            let window = &remaining[..limit];
            let cut_opt = [
                rfind_chars(window, &['\n', '\n']),
                rfind_chars(window, &['\n']),
                rfind_chars(window, &[' ']),
            ]
            .into_iter()
            .flatten()
            .max();
            let cut = match cut_opt {
                Some(c) if c > 0 => c,
                _ => limit,
            };
            let piece: String = remaining[..cut].iter().collect::<String>().trim().to_string();
            let rest: String = remaining[cut..].iter().collect();
            remaining = rest.trim().chars().collect();
            if !piece.is_empty() {
                parts.push(piece);
            }
        }
        parts
    }

    /// Last index at which `needle` occurs in `hay` (mirrors Python `rfind`).
    fn rfind_chars(hay: &[char], needle: &[char]) -> Option<usize> {
        if needle.is_empty() || hay.len() < needle.len() {
            return None;
        }
        let mut i = hay.len() - needle.len();
        loop {
            if hay[i..i + needle.len()] == *needle {
                return Some(i);
            }
            if i == 0 {
                return None;
            }
            i -= 1;
        }
    }
}

// ===========================================================================
// Inlined submodule: types.py
// ===========================================================================

pub mod types {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    /// One iLink message item (kept loose: the raw value is re-enqueued).
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct MessageItem {
        #[serde(rename = "type")]
        pub item_type: i64,
        pub text_item: Option<Value>,
        pub image_item: Option<Value>,
        pub voice_item: Option<Value>,
        pub file_item: Option<Value>,
        pub video_item: Option<Value>,
    }

    /// A single inbound iLink message. Camel-case aliases match the wire format.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct WeixinMessage {
        pub seq: Option<i64>,
        #[serde(alias = "messageId")]
        pub message_id: Option<i64>,
        #[serde(alias = "fromUserId")]
        pub from_user_id: Option<String>,
        #[serde(alias = "toUserId")]
        pub to_user_id: Option<String>,
        #[serde(alias = "createTimeMs")]
        pub create_time_ms: Option<i64>,
        #[serde(alias = "sessionId")]
        pub session_id: Option<String>,
        #[serde(alias = "messageType")]
        pub message_type: Option<i64>,
        #[serde(alias = "messageState")]
        pub message_state: Option<i64>,
        #[serde(alias = "itemList")]
        pub item_list: Option<Vec<Value>>,
        #[serde(alias = "contextToken")]
        pub context_token: Option<String>,
    }

    fn default_longpolling_timeout_ms() -> Option<i64> {
        Some(35000)
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct GetUpdatesResponse {
        pub ret: Option<i64>,
        pub errcode: Option<i64>,
        pub errmsg: Option<String>,
        pub msgs: Vec<WeixinMessage>,
        #[serde(alias = "getUpdatesBuf")]
        pub get_updates_buf: String,
        #[serde(
            default = "default_longpolling_timeout_ms",
            alias = "longpollingTimeoutMs"
        )]
        pub longpolling_timeout_ms: Option<i64>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct SendMessageResponse {
        pub ret: Option<i64>,
        #[serde(alias = "messageId")]
        pub message_id: Option<i64>,
        #[serde(alias = "contextToken")]
        pub context_token: Option<String>,
        pub data: serde_json::Map<String, Value>,
        pub errcode: Option<i64>,
        pub errmsg: Option<String>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct GetConfigResponse {
        pub ret: Option<i64>,
        #[serde(alias = "typingTicket")]
        pub typing_ticket: Option<String>,
        pub errcode: Option<i64>,
        pub errmsg: Option<String>,
    }

    /// Raised when the WeChat iLink API returns a non-success code.
    #[derive(Debug, Clone)]
    pub struct WeixinAPIError {
        pub ret: i64,
        pub errcode: Option<i64>,
        pub errmsg: Option<String>,
        /// True when the underlying transport timed out (long-poll idle).
        pub timed_out: bool,
    }

    impl WeixinAPIError {
        pub fn new(ret: i64, errcode: Option<i64>, errmsg: Option<String>, timed_out: bool) -> Self {
            WeixinAPIError {
                ret,
                errcode,
                errmsg,
                timed_out,
            }
        }
    }

    impl std::fmt::Display for WeixinAPIError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "WeChat API error: ret={}", self.ret)?;
            if let Some(ec) = self.errcode {
                write!(f, ", errcode={ec}")?;
            }
            if let Some(msg) = &self.errmsg {
                write!(f, ", errmsg={msg}")?;
            }
            Ok(())
        }
    }

    impl std::error::Error for WeixinAPIError {}
}

impl From<types::WeixinAPIError> for GatewayError {
    fn from(e: types::WeixinAPIError) -> Self {
        GatewayError::Other(e.to_string())
    }
}

// ===========================================================================
// Inlined submodule: media.py
// ===========================================================================

pub mod media {
    use base64::Engine;
    use serde_json::Value;

    pub const CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";
    const MAX_DOWNLOAD_SIZE: usize = 100 * 1024 * 1024;

    const MAGIC_TABLE: &[(&[u8], &str)] = &[
        (b"\x89PNG\r\n\x1a\n", ".png"),
        (b"\xff\xd8\xff", ".jpg"),
        (b"GIF87a", ".gif"),
        (b"GIF89a", ".gif"),
        (b"%PDF", ".pdf"),
        (b"PK\x03\x04", ".zip"),
        (b"\x1aE\xdf\xa3", ".webm"),
        (b"ID3", ".mp3"),
        (b"\xff\xfb", ".mp3"),
        (b"\xff\xf3", ".mp3"),
        (b"\xff\xf2", ".mp3"),
        (b"#!AMR\n", ".amr"),
        (b"#!SILK_V3", ".silk"),
    ];

    /// AES-128-ECB encrypt with PKCS7 padding.
    pub fn aes_ecb_encrypt(data: &[u8], key: &[u8]) -> Vec<u8> {
        use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyInit};
        let mut k = [0u8; 16];
        k.copy_from_slice(&key[..16]);
        ecb::Encryptor::<aes::Aes128>::new(&k.into()).encrypt_padded_vec_mut::<Pkcs7>(data)
    }

    /// AES-128-ECB decrypt and strip PKCS7 padding. `None` on failure.
    pub fn aes_ecb_decrypt(data: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyInit};
        let mut k = [0u8; 16];
        k.copy_from_slice(&key[..16]);
        ecb::Decryptor::<aes::Aes128>::new(&k.into())
            .decrypt_padded_vec_mut::<Pkcs7>(data)
            .ok()
    }

    pub fn generate_aes_key() -> Vec<u8> {
        use rand::RngCore;
        let mut k = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut k);
        k.to_vec()
    }

    pub fn md5_hex(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    pub fn encrypted_size(raw_size: usize) -> usize {
        raw_size + (16 - raw_size % 16)
    }

    /// Detect a file extension from magic bytes (mirrors `detect_extension`).
    pub fn detect_extension(data: &[u8]) -> &'static str {
        let header: &[u8] = if data.len() >= 32 { &data[..32] } else { data };
        for &(magic, ext) in MAGIC_TABLE {
            if header.starts_with(magic) {
                return ext;
            }
        }
        if data.len() >= 12 {
            if header.starts_with(b"RIFF") && &data[8..12] == b"WEBP" {
                return ".webp";
            }
            if header.starts_with(b"RIFF") && &data[8..12] == b"WAVE" {
                return ".wav";
            }
            if data[..12].windows(4).any(|w| w == &b"ftyp"[..]) {
                return ".mp4";
            }
        }
        ".bin"
    }

    /// Encrypt `data` and POST it to the CDN upload URL; returns the
    /// `x-encrypted-param` response header on success.
    pub async fn encrypt_and_upload(
        http: &reqwest::Client,
        upload_url: &str,
        data: &[u8],
        key: &[u8],
    ) -> Option<String> {
        let encrypted = aes_ecb_encrypt(data, key);
        let resp = http
            .post(upload_url)
            .header("Content-Type", "application/octet-stream")
            .body(encrypted)
            .send()
            .await
            .ok()?;
        let status = resp.status().as_u16();
        if !(status == 200 || status == 201 || status == 204) {
            tracing::warn!("Weixin CDN upload failed: http={status}");
            return None;
        }
        resp.headers()
            .get("x-encrypted-param")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    pub fn build_upload_url(upload_param: &str, filekey: &str) -> String {
        format!(
            "{}/upload?encrypted_query_param={}&filekey={}",
            CDN_BASE_URL,
            urlencoding::encode(upload_param),
            urlencoding::encode(filekey)
        )
    }

    pub fn build_download_url(encrypt_query_param: &str) -> String {
        if encrypt_query_param.is_empty() {
            return String::new();
        }
        format!(
            "{}/download?encrypted_query_param={}",
            CDN_BASE_URL,
            urlencoding::encode(encrypt_query_param)
        )
    }

    /// Decode an AES key given as hex, or base64-wrapped hex. Padded/truncated to 16 bytes.
    pub fn decode_aes_key(value: &str) -> Option<Vec<u8>> {
        if value.is_empty() {
            return None;
        }
        if let Ok(raw) = hex::decode(value) {
            return Some(normalize_key(raw));
        }
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(value) {
            if let Ok(s) = String::from_utf8(decoded) {
                if let Ok(raw) = hex::decode(s.trim()) {
                    return Some(normalize_key(raw));
                }
            }
        }
        None
    }

    fn normalize_key(mut raw: Vec<u8>) -> Vec<u8> {
        if raw.len() >= 16 {
            raw.truncate(16);
            raw
        } else {
            raw.resize(16, 0);
            raw
        }
    }

    /// Download and AES-ECB decrypt CDN media (mirrors `download_and_decrypt`).
    pub async fn download_and_decrypt(
        http: &reqwest::Client,
        cdn_media: &Value,
        extra_key: &str,
    ) -> Option<Vec<u8>> {
        let encrypt_param = cdn_media
            .get("encrypt_query_param")
            .or_else(|| cdn_media.get("encryptQueryParam"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if encrypt_param.is_empty() {
            return None;
        }
        let url = build_download_url(encrypt_param);
        let resp = http.get(&url).send().await.ok()?;
        if resp.status().as_u16() != 200 {
            tracing::warn!("Weixin CDN download failed: http={}", resp.status());
            return None;
        }
        let raw = resp.bytes().await.ok()?.to_vec();
        if raw.len() > MAX_DOWNLOAD_SIZE {
            tracing::warn!("Weixin CDN download too large: {} bytes", raw.len());
            return None;
        }
        let key = decode_aes_key(extra_key).or_else(|| {
            decode_aes_key(
                cdn_media
                    .get("aes_key")
                    .or_else(|| cdn_media.get("aesKey"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            )
        });
        Some(match key {
            Some(k) if raw.len() % 16 == 0 => match aes_ecb_decrypt(&raw, &k) {
                Some(decrypted) => decrypted,
                None => {
                    tracing::warn!("Weixin CDN decrypt failed; using raw bytes");
                    raw
                }
            },
            _ => raw,
        })
    }
}

// ===========================================================================
// Inlined submodule: api.py
// ===========================================================================

pub mod api {
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};
    use std::time::Duration;

    use super::types::{
        GetConfigResponse, GetUpdatesResponse, SendMessageResponse, WeixinAPIError,
    };

    const EP_GET_UPDATES: &str = "ilink/bot/getupdates";
    const EP_SEND_MESSAGE: &str = "ilink/bot/sendmessage";
    const EP_GET_UPLOAD_URL: &str = "ilink/bot/getuploadurl";
    const EP_GET_CONFIG: &str = "ilink/bot/getconfig";
    const EP_SEND_TYPING: &str = "ilink/bot/sendtyping";

    const CHANNEL_VERSION: &str = "2.2.0";
    const AUTH_TYPE: &str = "ilink_bot_token";
    const ILINK_APP_ID: &str = "bot";
    /// `str((2 << 16) | (2 << 8) | 0)`.
    const ILINK_APP_CLIENT_VERSION: &str = "131584";
    const HTTP_TIMEOUT_S: f64 = 45.0;

    const MSG_TYPE_BOT: i64 = 2;
    const MSG_STATE_FINISH: i64 = 2;
    const ITEM_TYPE_TEXT: i64 = 1;

    /// Random `X-WECHAT-UIN` value: base64 of a 4-byte big-endian uint32.
    fn wechat_uin() -> String {
        use base64::Engine;
        use rand::RngCore;
        let v = rand::thread_rng().next_u32();
        base64::engine::general_purpose::STANDARD.encode(v.to_be_bytes())
    }

    fn client_id() -> String {
        format!("hg-weixin-{}", uuid::Uuid::new_v4().simple())
    }

    fn or_default(a: Option<i64>, b: Option<i64>) -> i64 {
        match a {
            Some(x) if x != 0 => x,
            _ => match b {
                Some(y) if y != 0 => y,
                _ => -1,
            },
        }
    }

    /// Thin async HTTP client for the WeChat iLink Bot API.
    ///
    /// Shares the channel's `reqwest::Client`; never owns it.
    pub struct WeixinAPIClient {
        base_url: String,
        token: String,
        session: reqwest::Client,
        timeout_s: Option<f64>,
        uin: String,
    }

    impl WeixinAPIClient {
        pub fn new(
            base_url: String,
            token: String,
            session: reqwest::Client,
            timeout_s: Option<f64>,
            uin: String,
        ) -> Self {
            WeixinAPIClient {
                base_url: base_url.trim_end_matches('/').to_string(),
                token,
                session,
                timeout_s,
                uin,
            }
        }

        fn uin_value(&self) -> String {
            if self.uin.is_empty() {
                wechat_uin()
            } else {
                self.uin.clone()
            }
        }

        async fn post_json(
            &self,
            endpoint: &str,
            data: &Value,
            timeout_s: Option<f64>,
        ) -> std::result::Result<Value, WeixinAPIError> {
            let url = format!("{}/{}", self.base_url, endpoint);
            let body = serde_json::to_string(data)
                .map_err(|e| WeixinAPIError::new(-1, None, Some(format!("serialize error: {e}")), false))?;
            let dur = Duration::from_secs_f64(timeout_s.or(self.timeout_s).unwrap_or(HTTP_TIMEOUT_S));
            let resp = self
                .session
                .post(&url)
                .header("Content-Type", "application/json")
                .header("AuthorizationType", AUTH_TYPE)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("X-WECHAT-UIN", self.uin_value())
                .header("iLink-App-Id", ILINK_APP_ID)
                .header("iLink-App-ClientVersion", ILINK_APP_CLIENT_VERSION)
                .timeout(dur)
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    WeixinAPIError::new(
                        -1,
                        None,
                        Some(format!("transport error: {e}")),
                        e.is_timeout(),
                    )
                })?;
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            if status != 200 {
                let snippet: String = text.chars().take(100).collect();
                return Err(WeixinAPIError::new(
                    status as i64,
                    Some(status as i64),
                    Some(format!("HTTP {status}: {snippet}")),
                    false,
                ));
            }
            let payload: Value = serde_json::from_str(&text).map_err(|e| {
                WeixinAPIError::new(-1, None, Some(format!("parse error: {e}")), false)
            })?;
            let ret = payload.get("ret").and_then(|v| v.as_i64());
            let errcode = payload.get("errcode").and_then(|v| v.as_i64());
            let errmsg = payload
                .get("errmsg")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(ec) = errcode {
                if ec != 0 {
                    return Err(WeixinAPIError::new(or_default(ret, Some(ec)), Some(ec), errmsg, false));
                }
            }
            if let Some(r) = ret {
                if r != 0 {
                    return Err(WeixinAPIError::new(r, errcode, errmsg, false));
                }
            }
            Ok(payload)
        }

        async fn post<T: DeserializeOwned>(
            &self,
            endpoint: &str,
            data: &Value,
            timeout_s: Option<f64>,
        ) -> std::result::Result<T, WeixinAPIError> {
            let payload = self.post_json(endpoint, data, timeout_s).await?;
            serde_json::from_value(payload)
                .map_err(|e| WeixinAPIError::new(-1, None, Some(format!("parse error: {e}")), false))
        }

        pub async fn get_updates(
            &self,
            sync_cursor: &str,
            timeout_ms: Option<i64>,
        ) -> std::result::Result<GetUpdatesResponse, WeixinAPIError> {
            let t = match timeout_ms {
                Some(ms) => Some((ms as f64) / 1000.0 + 5.0),
                None => self.timeout_s,
            };
            let mut data = json!({
                "get_updates_buf": sync_cursor,
                "base_info": { "channel_version": CHANNEL_VERSION },
            });
            if let Some(ms) = timeout_ms {
                data["longpolling_timeout_ms"] = json!(ms);
            }
            self.post(EP_GET_UPDATES, &data, t).await
        }

        pub async fn send_message(
            &self,
            to_user_id: &str,
            text: &str,
            context_token: &str,
        ) -> std::result::Result<SendMessageResponse, WeixinAPIError> {
            self.send_items(
                to_user_id,
                &[json!({ "type": ITEM_TYPE_TEXT, "text_item": { "text": text } })],
                context_token,
            )
            .await
        }

        pub async fn send_items(
            &self,
            to_user_id: &str,
            items: &[Value],
            context_token: &str,
        ) -> std::result::Result<SendMessageResponse, WeixinAPIError> {
            let ct = if context_token.is_empty() {
                Value::Null
            } else {
                Value::String(context_token.to_string())
            };
            let data = json!({
                "msg": {
                    "from_user_id": "",
                    "to_user_id": to_user_id,
                    "client_id": client_id(),
                    "message_type": MSG_TYPE_BOT,
                    "message_state": MSG_STATE_FINISH,
                    "context_token": ct,
                    "item_list": items,
                },
                "base_info": { "channel_version": CHANNEL_VERSION },
            });
            self.post(EP_SEND_MESSAGE, &data, None).await
        }

        pub async fn get_upload_url(
            &self,
            payload: &Value,
        ) -> std::result::Result<Value, WeixinAPIError> {
            let mut data = payload.clone();
            if let Some(obj) = data.as_object_mut() {
                if !obj.contains_key("base_info") {
                    obj.insert(
                        "base_info".to_string(),
                        json!({ "channel_version": CHANNEL_VERSION }),
                    );
                }
            }
            self.post_json(EP_GET_UPLOAD_URL, &data, None).await
        }

        pub async fn get_config(
            &self,
            ilink_user_id: &str,
            context_token: &str,
        ) -> std::result::Result<GetConfigResponse, WeixinAPIError> {
            let data = json!({
                "ilink_user_id": ilink_user_id,
                "context_token": context_token,
                "base_info": { "channel_version": CHANNEL_VERSION },
            });
            self.post(EP_GET_CONFIG, &data, None).await
        }

        pub async fn send_typing(
            &self,
            ilink_user_id: &str,
            typing_ticket: &str,
            status: i64,
        ) -> std::result::Result<(), WeixinAPIError> {
            let url = format!("{}/{}", self.base_url, EP_SEND_TYPING);
            let data = json!({
                "ilink_user_id": ilink_user_id,
                "typing_ticket": typing_ticket,
                "status": status,
                "base_info": { "channel_version": CHANNEL_VERSION },
            });
            let body = serde_json::to_string(&data).unwrap_or_default();
            let dur = Duration::from_secs_f64(self.timeout_s.unwrap_or(10.0));
            let resp = self
                .session
                .post(&url)
                .header("Content-Type", "application/json")
                .header("AuthorizationType", AUTH_TYPE)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("X-WECHAT-UIN", self.uin_value())
                .header("iLink-App-Id", ILINK_APP_ID)
                .header("iLink-App-ClientVersion", ILINK_APP_CLIENT_VERSION)
                .timeout(dur)
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    WeixinAPIError::new(
                        -1,
                        None,
                        Some(format!("transport error: {e}")),
                        e.is_timeout(),
                    )
                })?;
            let status_code = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            if status_code != 200 {
                return Err(WeixinAPIError::new(
                    status_code as i64,
                    Some(status_code as i64),
                    Some(format!("HTTP {status_code}")),
                    false,
                ));
            }
            if text.is_empty() {
                return Ok(());
            }
            let payload: Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let ret = payload.get("ret").and_then(|v| v.as_i64());
            let errcode = payload.get("errcode").and_then(|v| v.as_i64());
            let bad_ec = matches!(errcode, Some(x) if x != 0);
            let bad_ret = matches!(ret, Some(x) if x != 0);
            if bad_ec || bad_ret {
                let errmsg = payload
                    .get("errmsg")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                return Err(WeixinAPIError::new(or_default(ret, errcode), errcode, errmsg, false));
            }
            Ok(())
        }
    }
}

// ===========================================================================
// Inlined submodule: login_qr.py
// ===========================================================================

pub mod login_qr {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    const QR_API_BASE_URL: &str = "https://ilinkai.weixin.qq.com";
    const QR_BOT_ENDPOINT: &str = "ilink/bot/get_bot_qrcode";
    const QR_STATUS_ENDPOINT: &str = "ilink/bot/get_qrcode_status";
    const DEFAULT_BOT_TYPE: &str = "3";
    const ILINK_CLIENT_VERSION: &str = "1";
    const QR_LONG_POLL_TIMEOUT_S: f64 = 35.0;
    const LOGIN_TIMEOUT_S: f64 = 480.0; // 8 minutes overall
    const MAX_QR_REFRESH_COUNT: u32 = 3;
    const ALLOWED_QR_HOST: &str = "ilinkai.weixin.qq.com";

    /// Response from WeChat QR code fetch endpoint.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct QRCodeResponse {
        /// Opaque polling token.
        pub qrcode: String,
        /// URL to display as QR image.
        pub qrcode_img_content: String,
    }

    /// Internal: raw polling response from iLink.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    struct QrStatusResponse {
        status: String,
        bot_token: Option<String>,
        ilink_bot_id: Option<String>,
        baseurl: Option<String>,
        ilink_user_id: Option<String>,
    }

    /// Result of waiting for QR code confirmation.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct WeixinQrWaitResult {
        pub connected: bool,
        pub bot_token: Option<String>,
        /// Normalised `ilink_bot_id`.
        pub account_id: Option<String>,
        /// Server-returned base URL for messaging.
        pub base_url: Option<String>,
        pub user_id: Option<String>,
        pub message: String,
    }

    /// Normalise WeChat `ilink_bot_id` by replacing `@` and `.` with `-`.
    pub fn normalize_account_id(ilink_bot_id: Option<&str>) -> Option<String> {
        let id = ilink_bot_id?;
        if id.is_empty() {
            return None;
        }
        Some(id.replace('@', "-").replace('.', "-"))
    }

    /// Validate URL to prevent SSRF. Only HTTPS + whitelisted host are allowed.
    pub fn validate_qr_base_url(url: &str) -> Result<String, String> {
        let url = url.trim_end_matches('/').to_string();
        let rest = url
            .strip_prefix("https://")
            .ok_or_else(|| "Only HTTPS allowed for QR API".to_string())?;
        // The host is everything before the first '/', ':' or '?'.
        let host = rest
            .split(|c: char| c == '/' || c == ':' || c == '?')
            .next()
            .unwrap_or("");
        if host != ALLOWED_QR_HOST {
            return Err(format!(
                "Unauthorized host for QR API: {host}. Allowed: {ALLOWED_QR_HOST}"
            ));
        }
        Ok(url)
    }

    /// Two-phase QR code login for WeChat iLink Bot.
    pub struct WeixinQRLogin {
        pub qr_base_url: String,
        pub route_tag: String,
        active_qrcode: Option<String>,
        refresh_count: u32,
    }

    impl WeixinQRLogin {
        pub fn new(qr_base_url: &str, route_tag: &str) -> Result<Self, String> {
            let validated = validate_qr_base_url(qr_base_url)?;
            Ok(WeixinQRLogin {
                qr_base_url: validated,
                route_tag: route_tag.to_string(),
                active_qrcode: None,
                refresh_count: 0,
            })
        }

        pub fn active_qrcode(&self) -> Option<&str> {
            self.active_qrcode.as_deref()
        }

        fn client(timeout_s: f64) -> Result<reqwest::Client, String> {
            reqwest::Client::builder()
                .timeout(Duration::from_secs_f64(timeout_s))
                .build()
                .map_err(|e| e.to_string())
        }

        /// Phase 1: fetch a new QR code from iLink.
        pub async fn fetch_qr_code(
            &mut self,
            reset_refresh_count: bool,
        ) -> Result<QRCodeResponse, String> {
            let url = format!("{}/{}", self.qr_base_url, QR_BOT_ENDPOINT);
            let client = Self::client(15.0)?;
            let mut req = client
                .get(&url)
                .query(&[("bot_type", DEFAULT_BOT_TYPE)])
                .header("iLink-App-ClientVersion", ILINK_CLIENT_VERSION);
            if !self.route_tag.is_empty() {
                req = req.header("SKRouteTag", &self.route_tag);
            }
            let resp = req.send().await.map_err(|e| e.to_string())?;
            if !resp.status().is_success() {
                return Err(format!("HTTP {}", resp.status()));
            }
            let raw: Value = resp.json().await.map_err(|e| e.to_string())?;
            let result: QRCodeResponse =
                serde_json::from_value(raw).map_err(|e| e.to_string())?;
            self.active_qrcode = Some(result.qrcode.clone());
            if reset_refresh_count {
                self.refresh_count = 0;
            }
            Ok(result)
        }

        /// Phase 2: long-poll for QR status until confirmed / expired / timeout.
        ///
        /// Auto-refreshes the QR up to `MAX_QR_REFRESH_COUNT` times if it expires
        /// before the user scans.
        pub async fn wait_for_login(
            &mut self,
            qrcode: String,
            timeout_s: f64,
            on_scanned: Option<Arc<dyn Fn() + Send + Sync>>,
        ) -> Result<WeixinQrWaitResult, String> {
            self.active_qrcode = Some(qrcode.clone());
            let start_time = crate::channel::now_f64();
            let mut scanned_notified = false;
            let mut qrcode = qrcode;

            let client = Self::client(QR_LONG_POLL_TIMEOUT_S + 5.0)?;
            while crate::channel::now_f64() - start_time < timeout_s {
                let url = format!("{}/{}", self.qr_base_url, QR_STATUS_ENDPOINT);
                let mut req = client
                    .get(&url)
                    .query(&[("qrcode", qrcode.as_str())])
                    .header("iLink-App-ClientVersion", ILINK_CLIENT_VERSION);
                if !self.route_tag.is_empty() {
                    req = req.header("SKRouteTag", &self.route_tag);
                }
                let raw: Value = match req.send().await {
                    Ok(r) => {
                        if !r.status().is_success() {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        match r.json::<Value>().await {
                            Ok(v) => v,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let status_resp: QrStatusResponse = match serde_json::from_value(raw) {
                    Ok(s) => s,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                match status_resp.status.as_str() {
                    "wait" => continue,
                    "scaned" => {
                        if let Some(cb) = &on_scanned {
                            if !scanned_notified {
                                scanned_notified = true;
                                cb();
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    "confirmed" => {
                        tracing::info!("WeChat QR login confirmed");
                        return Ok(WeixinQrWaitResult {
                            connected: true,
                            bot_token: status_resp.bot_token,
                            account_id: normalize_account_id(status_resp.ilink_bot_id.as_deref()),
                            base_url: status_resp.baseurl,
                            user_id: status_resp.ilink_user_id,
                            message: "QR code confirmed".to_string(),
                        });
                    }
                    "expired" => {
                        tracing::info!(
                            "QR expired (refresh {}/{})",
                            self.refresh_count,
                            MAX_QR_REFRESH_COUNT
                        );
                        if self.refresh_count < MAX_QR_REFRESH_COUNT {
                            self.refresh_count += 1;
                            let new_qr = self.fetch_qr_code(false).await?;
                            qrcode = new_qr.qrcode;
                            scanned_notified = false;
                            continue;
                        }
                        return Ok(WeixinQrWaitResult {
                            connected: false,
                            message: format!(
                                "QR expired after {MAX_QR_REFRESH_COUNT} refresh attempts"
                            ),
                            ..Default::default()
                        });
                    }
                    other => {
                        tracing::warn!("Unknown QR status: {other}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            Ok(WeixinQrWaitResult {
                connected: false,
                message: format!("QR login timeout (no confirmation within {timeout_s}s)"),
                ..Default::default()
            })
        }
    }
}
