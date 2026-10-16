#![allow(dead_code)]
//! QQ (腾讯QQ 频道/群/私聊机器人) channel.
//!
//! Faithful 1:1 Rust port of `octop_gateway.channels.qq`. All Python submodules
//! (`stream.py`, `stream_blocks.py`, `login_qr.py`) are inlined below as `mod`
//! blocks so the whole adapter lives in one file.
//!
//! Implements: OAuth2 app access token (`bots.qq.com/app/getAppAccessToken`),
//! gateway URL (`/gateway/bot`), a WebSocket gateway loop (op10 HELLO / op2
//! IDENTIFY / op6 RESUME / op1 heartbeat / op7 RECONNECT / op9 INVALID_SESSION),
//! inbound rich-media parsing (`qqimg`/`qqvideo`/`qqaudio`/`qqfile` tags,
//! `attachments`, `msg_elements`), two-tier rich-media upload, C2C replace-mode
//! streaming (`stream_messages`), passive-reply `msg_seq`, the 401 token-refresh
//! retry, C2C `input_notify` typing, and the QR-binding login flow.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::channel::{Channel, ChannelConfig, EnqueueFn};
use crate::constraints::{
    tool_hint_message, ChannelConstraints, ReplyTimeoutGuard, TypingKeepalive,
};
use crate::errors::{GatewayError, Result};
use crate::group_context::GroupContextConfig;
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, GroupContext, GroupContextMessage,
    ImageContent, InboundMessage, MessageEventType, RawPayload, VideoContent,
};
use crate::utils::{extract_media, extract_text};

// ---------------------------------------------------------------------------
// REST API base URLs / gateway / token endpoints
// ---------------------------------------------------------------------------

const QQ_API: &str = "https://api.sgroup.qq.com";
const QQ_SANDBOX_API: &str = "https://sandbox.api.sgroup.qq.com";
const QQ_TOKEN_URL: &str = "https://bots.qq.com/app/getAppAccessToken";

/// Default intents: public @-messages + guild members + direct messages + group/c2c.
const DEFAULT_INTENTS: i64 = (1 << 30) | (1 << 1) | (1 << 12) | (1 << 25);

/// A previous generating frame may already have locked this prefix. Mobile QQ
/// paints a newline-only frame as an empty bubble, so new turns do not send it.
const C2C_STREAM_HOLD: &str = "\n";

// ---------------------------------------------------------------------------
// Regexes (module-scope, matching Python's compiled patterns)
// ---------------------------------------------------------------------------

static MEDIA_TAG_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"<qq(?:img|video|audio|file)\s+[^>]*?(?:src|url)\s*=\s*["']([^"']+)["'][^>]*?>"#)
        .unwrap()
});
static IMG_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<qqimg\s+[^>]*?src\s*=\s*["']([^"']+)["'][^>]*?>"#).unwrap());
static VIDEO_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<qqvideo\s+[^>]*?src\s*=\s*["']([^"']+)["'][^>]*?>"#).unwrap());
static AUDIO_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<qqaudio\s+[^>]*?src\s*=\s*["']([^"']+)["'][^>]*?>"#).unwrap());
static FILE_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<qqfile\s+[^>]*?src\s*=\s*["']([^"']+)["'][^>]*?>"#).unwrap());
static AT_MENTION_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"<@!?[A-Za-z0-9_-]+>\s*").unwrap());
static THINK_BLOCK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<(?:think|thinking)>(.*?)</(?:think|thinking)>").unwrap());
static THINK_OPEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)<(?:think|thinking)>").unwrap());
static THINK_CLOSE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)</(?:think|thinking)>").unwrap());

/// Return inner text of complete `<think>` / `<thinking>` blocks.
pub fn extract_think_blocks(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let mut parts: Vec<String> = Vec::new();
    for cap in THINK_BLOCK_RE.captures_iter(text) {
        if let Some(m) = cap.get(1) {
            let s = m.as_str().trim();
            if !s.is_empty() {
                parts.push(s.to_string());
            }
        }
    }
    parts.join("\n")
}

/// Remove think markup so only the user-visible remainder remains.
pub fn strip_think_tags(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let mut cleaned = THINK_BLOCK_RE.replace_all(text, "").to_string();
    loop {
        let close = match THINK_CLOSE_RE.find(&cleaned) {
            Some(m) => m,
            None => break,
        };
        if let Some(open) = THINK_OPEN_RE.find(&cleaned) {
            if open.start() < close.start() {
                break;
            }
        }
        cleaned = cleaned[close.end()..].to_string();
    }
    if let Some(open) = THINK_OPEN_RE.find(&cleaned) {
        cleaned = cleaned[..open.start()].to_string();
    }
    cleaned
}

// ---------------------------------------------------------------------------
// JSON helpers mirroring Python truthiness / str() coercions
// ---------------------------------------------------------------------------

fn jget<'a>(v: &'a Value, k: &str) -> Option<&'a Value> {
    match v.get(k) {
        Some(Value::Null) | None => None,
        Some(x) => Some(x),
    }
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
    }
}

fn value_str(x: &Value) -> String {
    match x {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Mirrors Python `str(x or "")`.
fn jstr(v: Option<&Value>) -> String {
    match v {
        None => String::new(),
        Some(x) => {
            if !truthy(Some(x)) {
                String::new()
            } else {
                value_str(x)
            }
        }
    }
}

/// First truthy value coerced to string (mirrors a Python `a or b or ""` chain).
fn first_str(vals: &[Option<&Value>]) -> String {
    for v in vals {
        if truthy(*v) {
            return value_str(v.unwrap());
        }
    }
    String::new()
}

fn meta_get_str(meta: &HashMap<String, Value>, k: &str) -> Option<String> {
    match meta.get(k) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(v) if truthy(Some(v)) => Some(value_str(v)),
        _ => None,
    }
}

fn meta_nonempty(meta: &HashMap<String, Value>, k: &str) -> bool {
    truthy(meta.get(k))
}

// ---------------------------------------------------------------------------
// Module-level msg_seq counter (per msg_id), mirroring Python `_msg_seq`.
// QQ API requires incrementing msg_seq to avoid deduplication (error 40054005).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MsgSeqStore {
    map: HashMap<String, u64>,
    order: VecDeque<String>,
}

static MSG_SEQ: Lazy<StdMutex<MsgSeqStore>> = Lazy::new(|| StdMutex::new(MsgSeqStore::default()));

fn get_next_msg_seq(msg_id: &str) -> u64 {
    let mut store = MSG_SEQ.lock().unwrap();
    if !store.map.contains_key(msg_id) {
        let seed = (crate::channel::now_secs() % 1_000_000) as u64;
        store.map.insert(msg_id.to_string(), seed);
        store.order.push_back(msg_id.to_string());
    }
    let entry = store.map.get_mut(msg_id).unwrap();
    *entry += 1;
    let n = *entry;
    if store.map.len() > 1000 {
        let keys: Vec<String> = store.order.iter().take(500).cloned().collect();
        for k in keys {
            if k == "c2c-rich" || k == "group-rich" || k == "input-notify" {
                continue;
            }
            store.map.remove(&k);
            store.order.retain(|x| x != &k);
        }
    }
    n
}

// ---------------------------------------------------------------------------
// Shared inbound-dedup cache (mirrors QQChannel._seen_message_ids LRU<1000>).
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Dedup(Arc<StdMutex<DedupInner>>);

#[derive(Default)]
struct DedupInner {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl Dedup {
    /// Returns true when `id` was already seen; otherwise records it.
    fn seen(&self, id: &str) -> bool {
        let mut g = self.0.lock().unwrap();
        if g.set.contains(id) {
            return true;
        }
        g.set.insert(id.to_string());
        g.order.push_back(id.to_string());
        while g.order.len() > 1000 {
            if let Some(old) = g.order.pop_front() {
                g.set.remove(&old);
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// OAuth access-token state + transport
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct TokenState {
    token: String,
    expires_at: f64,
    refresh_at: f64,
}

/// Cloneable REST transport bound to a channel's credentials + shared HTTP client.
#[derive(Clone)]
struct QqTransport {
    api_base: String,
    app_id: String,
    secret: String,
    http: reqwest::Client,
    token_state: Arc<AsyncMutex<TokenState>>,
}

impl QqTransport {
    fn token_still_fresh(state: &TokenState, now: f64) -> bool {
        if state.token.is_empty() {
            return false;
        }
        let mut refresh_at = state.refresh_at;
        if refresh_at <= 0.0 && state.expires_at > 0.0 {
            refresh_at = state.expires_at - 60.0;
        }
        now < refresh_at
    }

    /// Ensure a valid OAuth2 access token is available (`bots.qq.com/app/getAppAccessToken`).
    async fn ensure_token(&self) -> Result<String> {
        let mut state = self.token_state.lock().await;
        let now = crate::channel::now_f64();
        if Self::token_still_fresh(&state, now) {
            return Ok(state.token.clone());
        }
        if self.app_id.is_empty() || self.secret.is_empty() {
            return Err(GatewayError::Other(
                "QQChannel: app_id & secret are required".to_string(),
            ));
        }
        let resp = self
            .http
            .post(QQ_TOKEN_URL)
            .json(&json!({ "appId": self.app_id, "clientSecret": self.secret }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!(
                "Failed to get QQ access token: {status} {body}"
            )));
        }
        let data: Value = resp.json().await?;
        let token = data
            .get("access_token")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let expires_in = data.get("expires_in").and_then(|x| x.as_i64()).unwrap_or(7200);
        if token.is_empty() {
            return Err(GatewayError::Other(format!(
                "No access_token in response: {data}"
            )));
        }
        let ttl = expires_in.max(1);
        let ttl = ttl as f64;
        let now = crate::channel::now_f64();
        state.token = token.clone();
        state.expires_at = now + ttl;
        let skew = 60f64.min(1f64.max((ttl / 2.0).floor()));
        state.refresh_at = now + 1f64.max(ttl - skew);
        Ok(token)
    }

    async fn fresh_auth(&self) -> Result<String> {
        let token = self.ensure_token().await?;
        Ok(format!("QQBot {token}"))
    }

    async fn invalidate(&self) {
        let mut state = self.token_state.lock().await;
        state.token.clear();
        state.expires_at = 0.0;
        state.refresh_at = 0.0;
    }

    fn is_access_token_error(status: u16, body: &str) -> bool {
        if status != 401 {
            return false;
        }
        let lowered = body.to_lowercase();
        body.contains("40011027")
            || body.contains("11244")
            || lowered.contains("accesstoken")
            || lowered.contains("access token")
    }

    async fn do_post(&self, url: &str, auth: &str, payload: &Value) -> Result<(u16, String)> {
        let resp = self
            .http
            .post(url)
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .json(payload)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Ok((status, body))
    }

    /// POST to QQ REST and retry once after an expired-token 401.
    async fn post(&self, url: &str, payload: &Value) -> Result<(u16, String)> {
        let auth = self.fresh_auth().await?;
        let (mut status, mut body) = self.do_post(url, &auth, payload).await?;
        if Self::is_access_token_error(status, &body) {
            tracing::warn!("QQ access token rejected; refreshing and retrying: url={url}");
            self.invalidate().await;
            let auth = self.fresh_auth().await?;
            let r = self.do_post(url, &auth, payload).await?;
            status = r.0;
            body = r.1;
        }
        Ok((status, body))
    }

    fn resolve_endpoint(
        to_handle: &str,
        msg_type: &str,
        meta: &HashMap<String, Value>,
    ) -> Option<String> {
        match msg_type {
            "c2c" => {
                let openid = meta_get_str(meta, "user_openid").unwrap_or_else(|| to_handle.to_string());
                Some(format!("/v2/users/{openid}/messages"))
            }
            "group" => {
                let g = meta_get_str(meta, "group_openid").unwrap_or_else(|| to_handle.to_string());
                Some(format!("/v2/groups/{g}/messages"))
            }
            "direct" => {
                let g = meta_get_str(meta, "guild_id").unwrap_or_else(|| to_handle.to_string());
                Some(format!("/dms/{g}/messages"))
            }
            "channel" => {
                let c =
                    meta_get_str(meta, "channel_id_native").unwrap_or_else(|| to_handle.to_string());
                Some(format!("/channels/{c}/messages"))
            }
            _ => None,
        }
    }

    /// POST a message payload, injecting `msg_type`/`msg_seq` for C2C/group.
    async fn send_message(
        &self,
        to_handle: &str,
        payload: &mut Value,
        msg_type: &str,
        meta: &HashMap<String, Value>,
    ) -> Result<()> {
        let endpoint = match Self::resolve_endpoint(to_handle, msg_type, meta) {
            Some(e) => e,
            None => {
                tracing::warn!("QQChannel cannot resolve send endpoint: to={to_handle} type={msg_type}");
                return Ok(());
            }
        };

        if msg_type == "c2c" || msg_type == "group" {
            let obj = payload
                .as_object_mut()
                .ok_or_else(|| GatewayError::Other("qq payload not an object".to_string()))?;
            if !obj.contains_key("msg_type") {
                obj.insert("msg_type".to_string(), json!(0));
            }
            let msg_id = match obj.get("msg_id") {
                Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                _ => meta_get_str(meta, "msg_id"),
            };
            if let Some(mid) = msg_id {
                obj.entry("msg_id".to_string()).or_insert_with(|| json!(mid));
                let seq = get_next_msg_seq(&mid);
                obj.insert("msg_seq".to_string(), json!(seq));
            } else {
                obj.remove("msg_seq");
            }
        }

        let url = format!("{}{endpoint}", self.api_base);
        let (status, body) = self.post(&url, payload).await?;
        if !matches!(status, 200 | 201 | 202 | 204) {
            tracing::error!("QQChannel send failed: status={status} url={url} body={body}");
            return Err(GatewayError::Other(format!(
                "QQChannel send failed: status={status} body={}",
                truncate(&body, 500)
            )));
        }
        Ok(())
    }

    /// Best-effort send returning `(ok, status, body)` (markdown-first fallback).
    async fn try_send_message(
        &self,
        to_handle: &str,
        payload: &Value,
        msg_type: &str,
        meta: &HashMap<String, Value>,
    ) -> (bool, u16, String) {
        let endpoint = match Self::resolve_endpoint(to_handle, msg_type, meta) {
            Some(e) => e,
            None => return (false, 0, String::new()),
        };
        let url = format!("{}{endpoint}", self.api_base);
        match self.post(&url, payload).await {
            Ok((status, body)) => {
                if matches!(status, 200 | 201 | 202 | 204) {
                    (true, status, body)
                } else {
                    (false, status, body)
                }
            }
            Err(e) => {
                tracing::warn!("QQChannel try_send error: url={url} err={e}");
                (false, 0, String::new())
            }
        }
    }
}

fn truncate(s: &str, n: usize) -> &str {
    if s.len() <= n {
        s
    } else {
        let mut end = n;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn default_true() -> bool {
    true
}

fn de_c2c_streaming<'de, D>(de: D) -> std::result::Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = bool;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bool or string")
        }
        fn visit_bool<E>(self, v: bool) -> std::result::Result<bool, E> {
            Ok(v)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<bool, E> {
            let s = v.trim().to_lowercase();
            Ok(!matches!(s.as_str(), "0" | "false" | "no" | "off"))
        }
        fn visit_i64<E>(self, v: i64) -> std::result::Result<bool, E> {
            Ok(v != 0)
        }
        fn visit_u64<E>(self, v: u64) -> std::result::Result<bool, E> {
            Ok(v != 0)
        }
        fn visit_none<E>(self) -> std::result::Result<bool, E> {
            Ok(true)
        }
        fn visit_unit<E>(self) -> std::result::Result<bool, E> {
            Ok(true)
        }
    }
    de.deserialize_any(V)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QQConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    pub app_id: String,
    pub token: String,
    pub secret: String,
    pub sandbox: bool,
    pub intents: Option<i64>,
    #[serde(default = "default_true", deserialize_with = "de_c2c_streaming")]
    pub c2c_streaming: bool,
    pub stream_throttle_ms: i64,
    pub stream_hold_keepalive_s: f64,
    pub stream_done_retries: i64,
}

impl Default for QQConfig {
    fn default() -> Self {
        QQConfig {
            base: ChannelConfig {
                show_tool_hints: false,
                group_context: Some(GroupContextConfig {
                    enabled: true,
                    visibility: "auto".to_string(),
                    activation: "mention".to_string(),
                    history: "recent".to_string(),
                    history_limit: 10,
                    ..Default::default()
                }),
                ..Default::default()
            },
            app_id: String::new(),
            token: String::new(),
            secret: String::new(),
            sandbox: false,
            intents: None,
            c2c_streaming: true,
            stream_throttle_ms: 150,
            stream_hold_keepalive_s: 3.0,
            stream_done_retries: 3,
        }
    }
}

impl QQConfig {
    /// The OAuth client secret, falling back to the legacy `token` field.
    pub fn client_secret(&self) -> String {
        if !self.secret.is_empty() {
            self.secret.clone()
        } else {
            self.token.clone()
        }
    }

    /// Base REST/gateway host (sandbox-aware).
    pub fn api_base(&self) -> &'static str {
        if self.sandbox {
            QQ_SANDBOX_API
        } else {
            QQ_API
        }
    }

    /// Build a channel config from a successful QQ Bot QR binding.
    pub fn from_qr_credentials(credentials: &login_qr::QQBotQRCredentials) -> Self {
        QQConfig {
            app_id: credentials.app_id.clone(),
            secret: credentials.app_secret.clone(),
            ..Default::default()
        }
    }
}

crate::channel_adapter! {
    /// QQ channel.
    QQChannel, QQConfig, "qq",
    required = [app_id, secret],
    fields = {
        ws_task: Option<JoinHandle<()>>,
        token_state: Arc<AsyncMutex<TokenState>>,
        ws_session: Arc<StdMutex<Option<String>>>,
        seen_messages: Dedup,
        markdown_unsupported: Arc<StdMutex<HashSet<String>>>,
        active_stream: Arc<StdMutex<Option<Arc<stream::StreamSession>>>>,
    }
}

// ---------------------------------------------------------------------------
// Routing helpers (pure; no channel state beyond the subject)
// ---------------------------------------------------------------------------

/// Resolve `msg_type` + metadata for send; infer proactive C2C/group when needed.
fn resolve_send_routing(subject: &ChannelSubject) -> (String, HashMap<String, Value>) {
    let mut meta = subject.metadata.clone();
    let msg_type = match meta.get("msg_type") {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        Some(v) if truthy(Some(v)) => value_str(v),
        _ => "channel".to_string(),
    };

    if meta_nonempty(&meta, "msg_id") {
        return (msg_type, meta);
    }
    if msg_type == "c2c" || msg_type == "group" || msg_type == "direct" {
        return (msg_type, meta);
    }
    if msg_type == "channel" {
        if meta_nonempty(&meta, "channel_id_native") {
            return (msg_type, meta);
        }
        if meta_nonempty(&meta, "group_openid") {
            return ("group".to_string(), meta);
        }
        if meta_nonempty(&meta, "user_openid") {
            return ("c2c".to_string(), meta);
        }
        if subject.chat_type == "group" {
            meta.entry("group_openid".to_string())
                .or_insert_with(|| json!(subject.subject_id));
            return ("group".to_string(), meta);
        }
        meta.entry("user_openid".to_string())
            .or_insert_with(|| json!(subject.subject_id));
        return ("c2c".to_string(), meta);
    }
    (msg_type, meta)
}

fn is_message_event(t: &str) -> bool {
    matches!(
        t,
        "AT_MESSAGE_CREATE"
            | "DIRECT_MESSAGE_CREATE"
            | "C2C_MESSAGE_CREATE"
            | "GROUP_AT_MESSAGE_CREATE"
            | "GROUP_MESSAGE_CREATE"
            | "MESSAGE_CREATE"
    )
}

fn classify_msg_type(event_type: &str) -> &'static str {
    match event_type {
        "C2C_MESSAGE_CREATE" => "c2c",
        "GROUP_AT_MESSAGE_CREATE" | "GROUP_MESSAGE_CREATE" => "group",
        "DIRECT_MESSAGE_CREATE" => "direct",
        _ => "channel",
    }
}

/// Parse an ISO-8601 timestamp into epoch seconds (mirrors Python `fromisoformat`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn parse_iso8601(s: &str) -> Option<f64> {
    let s = s.replace('Z', "+00:00");
    let (date, rest) = s.split_once('T')?;
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;

    let tz_pos = rest[1..].find(['+', '-']).map(|i| i + 1);
    let (time_str, tz_str) = match tz_pos {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let (hms, frac) = match time_str.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (time_str, None),
    };
    let mut hp = hms.split(':');
    let h: i64 = hp.next()?.parse().ok()?;
    let mi: i64 = hp.next()?.parse().ok()?;
    let sec: i64 = hp.next().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0) as i64;
    let frac_s: f64 = match frac {
        Some(f) => format!("0.{f}").parse::<f64>().unwrap_or(0.0),
        None => 0.0,
    };
    let mut offset: i64 = 0;
    if !tz_str.is_empty() {
        let sign: i64 = if tz_str.starts_with('-') { -1 } else { 1 };
        let body = &tz_str[1..];
        let (oh, om) = match body.split_once(':') {
            Some((a, b)) => (a, b),
            None => (body, "0"),
        };
        let oh: i64 = oh.parse().unwrap_or(0);
        let om: i64 = om.parse().unwrap_or(0);
        offset = sign * (oh * 3600 + om * 60);
    }
    let days = days_from_civil(y, mo, d);
    let epoch =
        days * 86400 + h * 3600 + mi * 60 + sec - offset;
    Some(epoch as f64 + frac_s)
}

fn parse_event_timestamp(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(n)) => n.as_f64().unwrap_or_else(crate::channel::now_f64),
        Some(Value::String(s)) if !s.is_empty() => {
            parse_iso8601(s).unwrap_or_else(crate::channel::now_f64)
        }
        _ => crate::channel::now_f64(),
    }
}

fn is_image_url(url: &str, filename: &str) -> bool {
    const SUFFIXES: &[&str] = &[".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff"];
    let path = url.split('?').next().unwrap_or(url).to_lowercase();
    if SUFFIXES.iter().any(|s| path.ends_with(s)) {
        return true;
    }
    !filename.is_empty() && SUFFIXES.iter().any(|s| filename.to_lowercase().ends_with(s))
}

// ---------------------------------------------------------------------------
// Streaming helpers (mirror the pipeline-side closures in `_process_inbound`)
// ---------------------------------------------------------------------------

fn full_text(parts: &[String]) -> String {
    parts.concat()
}

fn answer_text(parts: &[String]) -> String {
    strip_think_tags(&full_text(parts))
}

fn format_thinking(constraints: &ChannelConstraints, content: &str) -> String {
    constraints.thinking_template.replace("{content}", content)
}

/// Take + clear accumulated thinking, returning the formatted block when shown.
fn take_thinking_text(
    parts: &mut Vec<String>,
    show: bool,
    constraints: &ChannelConstraints,
) -> Option<String> {
    if !show {
        parts.clear();
        return None;
    }
    let raw = parts.concat();
    let raw = raw.trim();
    let text = if raw.is_empty() {
        String::new()
    } else {
        format_thinking(constraints, raw)
    };
    parts.clear();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn locked_stream_text(session: &Arc<stream::StreamSession>, text: &str) -> String {
    let accepted = session.prefix_base();
    let text = if accepted.starts_with(C2C_STREAM_HOLD) && !text.starts_with(C2C_STREAM_HOLD) {
        format!("{C2C_STREAM_HOLD}{text}")
    } else {
        text.to_string()
    };
    stream::reconcile_stream_text(&accepted, &text)
}

fn offer_answer(session: &Arc<stream::StreamSession>, text_parts: &[String], final_: bool) {
    let text = answer_text(text_parts);
    if session.failed() || session.closing() {
        return;
    }
    let payload = if final_ {
        text
    } else {
        stream_blocks::stable_markdown_prefix(&text)
    };
    if payload.is_empty() {
        return;
    }
    let locked = locked_stream_text(session, &payload);
    Arc::clone(session).offer(locked);
}

/// POST one replace-mode C2C stream frame. Returns the QQ stream msg id.
async fn send_stream_frame_impl(
    transport: &QqTransport,
    frame: &stream::StreamFrame,
) -> Result<Option<String>> {
    let mut content_raw = frame.text.clone();
    if frame.state == stream::STREAM_DONE
        && !content_raw.is_empty()
        && !content_raw.ends_with('\n')
    {
        content_raw.push('\n');
    }
    let mut body = json!({
        "input_mode": "replace",
        "input_state": frame.state,
        "content_type": "markdown",
        "content_raw": content_raw,
        "event_id": frame.msg_id,
        "msg_id": frame.msg_id,
        "msg_seq": frame.msg_seq,
        "index": frame.index,
    });
    if let Some(smid) = &frame.stream_msg_id {
        body["stream_msg_id"] = json!(smid);
    }
    let url = format!(
        "{}/v2/users/{}/stream_messages",
        transport.api_base, frame.user_id
    );
    let (status, raw) = transport.post(&url, &body).await?;
    if !matches!(status, 200 | 201 | 202 | 204) {
        return Err(GatewayError::Other(format!(
            "QQ stream frame failed: status={status} body={}",
            truncate(&raw, 500)
        )));
    }
    if raw.is_empty() {
        return Ok(None);
    }
    let payload: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if !payload.is_object() {
        return Ok(None);
    }
    let code = payload.get("code");
    let code_ok = match code {
        None => true,
        Some(Value::Number(n)) => n.as_i64() == Some(0),
        Some(Value::String(s)) => s == "0",
        Some(Value::Null) => true,
        _ => false,
    };
    if !code_ok {
        return Err(GatewayError::Other(format!(
            "QQ stream frame failed: status={status} body={}",
            truncate(&raw, 500)
        )));
    }
    Ok(extract_stream_id(&payload))
}

/// Return the replace-chain id from a QQ stream response.
fn extract_stream_id(payload: &Value) -> Option<String> {
    let mut sources: Vec<&Value> = vec![payload];
    if let Some(nested) = payload.get("data") {
        if nested.is_object() {
            sources.push(nested);
        }
    }
    for source in sources {
        for key in ["stream_msg_id", "id"] {
            match source.get(key) {
                Some(Value::String(s)) if !s.trim().is_empty() => return Some(s.clone()),
                Some(Value::Number(n)) => {
                    if n.as_i64().is_some() {
                        return Some(n.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Inbound typing (C2C input_notify) — usable from background tasks too
// ---------------------------------------------------------------------------

async fn qq_input_notify(transport: &QqTransport, subject: &ChannelSubject) -> Result<()> {
    let (msg_type, meta) = resolve_send_routing(subject);
    if msg_type != "c2c" {
        return Ok(());
    }
    let user_id = meta_get_str(&meta, "user_openid").unwrap_or_else(|| subject.subject_id.clone());
    let msg_id = meta_get_str(&meta, "msg_id").unwrap_or_default();
    if user_id.is_empty() || msg_id.is_empty() {
        return Ok(());
    }
    let mut payload = json!({
        "msg_type": 6,
        "input_notify": { "input_type": 1, "input_second": 60 },
        "msg_id": msg_id,
    });
    transport
        .send_message(&user_id, &mut payload, "c2c", &meta)
        .await
}

// ---------------------------------------------------------------------------
// Base pipeline (mirrors the provided `Channel::process_inbound`)
// ---------------------------------------------------------------------------

async fn base_process_inbound<C: Channel>(
    channel: &mut C,
    message: &InboundMessage,
    subject: &ChannelSubject,
) -> bool {
    let processor = match channel.core().processor() {
        Some(p) => p,
        None => {
            tracing::warn!("No processor set for channel {}", channel.channel_id());
            return false;
        }
    };
    let mut stream = processor(message.clone());

    let constraints = channel.core().constraints().clone();
    let (to_tx, mut to_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (ty_tx, mut ty_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut timeout_guard = if constraints.reply_timeout > 0.0 {
        Some(ReplyTimeoutGuard::spawn(constraints.reply_timeout, to_tx))
    } else {
        None
    };
    let mut typing_keepalive = if constraints.typing_keepalive_interval > 0.0 {
        Some(TypingKeepalive::spawn(constraints.typing_keepalive_interval, ty_tx))
    } else {
        None
    };

    let mut delta_buffer = String::new();
    let mut thinking_buffer = String::new();
    let mut placeholder_sent = false;
    let mut processing_succeeded = false;

    loop {
        tokio::select! {
            maybe_ev = stream.next() => {
                match maybe_ev {
                    Some(ev) => match ev.event_type {
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
                                        let _ = channel.rate_limited_send(subject, &formatted).await;
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
                            drop(timeout_guard.take());
                            drop(typing_keepalive.take());
                            if !thinking_buffer.is_empty() {
                                let t = std::mem::take(&mut thinking_buffer);
                                let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                let _ = channel.rate_limited_send(subject, &formatted).await;
                            }
                            if !delta_buffer.is_empty() {
                                let d = std::mem::take(&mut delta_buffer);
                                let _ = channel.rate_limited_send(subject, &d).await;
                            }
                        }
                        MessageEventType::Completed => {
                            drop(timeout_guard.take());
                            drop(typing_keepalive.take());
                            if !thinking_buffer.is_empty() {
                                let t = std::mem::take(&mut thinking_buffer);
                                let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                let _ = channel.rate_limited_send(subject, &formatted).await;
                            }
                            if !delta_buffer.is_empty() {
                                let d = std::mem::take(&mut delta_buffer);
                                let _ = channel.rate_limited_send(subject, &d).await;
                            }
                            processing_succeeded = true;
                            break;
                        }
                        MessageEventType::Message => {
                            drop(timeout_guard.take());
                            drop(typing_keepalive.take());
                            if !thinking_buffer.is_empty() {
                                let t = std::mem::take(&mut thinking_buffer);
                                let formatted = constraints.thinking_template.replace("{content}", t.trim());
                                let _ = channel.rate_limited_send(subject, &formatted).await;
                            }
                            if !delta_buffer.is_empty() {
                                let d = std::mem::take(&mut delta_buffer);
                                let _ = channel.rate_limited_send(subject, &d).await;
                            }
                            let _ = channel.deliver_event(subject, &ev).await;
                        }
                        MessageEventType::Typing => {}
                        MessageEventType::ToolStart => {
                            channel.on_tool_start(subject, &ev).await;
                        }
                        MessageEventType::ToolEnd => {
                            channel.on_tool_end(subject, &ev).await;
                        }
                        MessageEventType::Error => {
                            let err = ev.error.clone().unwrap_or_else(|| "An unexpected error occurred.".to_string());
                            let _ = channel.rate_limited_send(subject, &err).await;
                        }
                    },
                    None => break,
                }
            }
            _ = to_rx.recv() => {
                if !placeholder_sent && constraints.timeout_strategy == "placeholder" {
                    placeholder_sent = true;
                    let _ = channel.rate_limited_send(subject, &constraints.placeholder_text()).await;
                }
            }
            _ = ty_rx.recv() => {
                let _ = channel.send_typing_indicator(subject).await;
            }
        }
    }
    processing_succeeded
}

// ---------------------------------------------------------------------------
// Channel implementation
// ---------------------------------------------------------------------------

impl QQChannel {
    fn make_transport(&self, http: reqwest::Client) -> QqTransport {
        QqTransport {
            api_base: self.config.api_base().to_string(),
            app_id: self.config.app_id.clone(),
            secret: self.config.client_secret(),
            http,
            token_state: self.token_state.clone(),
        }
    }

    fn session_id(&self) -> String {
        self.ws_session
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "qq-unconnected".to_string())
    }

    fn enrich_push_metadata_inner(
        &self,
        subject: &ChannelSubject,
        meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        let mut out = meta;
        let routed_subject = ChannelSubject {
            subject_id: subject.subject_id.clone(),
            first_seen: subject.first_seen,
            last_seen: subject.last_seen,
            display_name: subject.display_name.clone(),
            chat_type: subject.chat_type.clone(),
            metadata: out.clone(),
        };
        let (routed_type, routed_meta) = resolve_send_routing(&routed_subject);
        for (k, v) in routed_meta {
            out.insert(k, v);
        }
        out.entry("msg_type".to_string()).or_insert_with(|| json!(routed_type));
        out
    }

    // --- content parsing helpers ------------------------------------------

    fn extract_sender_id(&self, data: &Value, event_type: &str) -> String {
        if event_type == "C2C_MESSAGE_CREATE" {
            if let Some(author) = jget(data, "author") {
                if author.is_object() {
                    let openid = first_str(&[jget(author, "user_openid"), jget(author, "id")]);
                    if !openid.is_empty() {
                        return openid;
                    }
                }
            }
            let v = jstr(jget(data, "user_openid"));
            return if v.is_empty() { "unknown".to_string() } else { v };
        }
        if event_type == "GROUP_AT_MESSAGE_CREATE" || event_type == "GROUP_MESSAGE_CREATE" {
            if let Some(author) = jget(data, "author") {
                if author.is_object() {
                    let v = first_str(&[jget(author, "member_openid"), jget(author, "id")]);
                    return if v.is_empty() { "unknown".to_string() } else { v };
                }
            }
            let v = jstr(jget(data, "member_openid"));
            return if v.is_empty() { "unknown".to_string() } else { v };
        }
        if let Some(author) = jget(data, "author") {
            if author.is_object() {
                let v = first_str(&[jget(author, "id"), jget(author, "user_openid")]);
                return if v.is_empty() { "unknown".to_string() } else { v };
            }
        }
        "unknown".to_string()
    }

    fn extract_to_handle(&self, data: &Value, event_type: &str) -> String {
        if event_type == "C2C_MESSAGE_CREATE" {
            if let Some(author) = jget(data, "author") {
                if author.is_object() {
                    let openid = first_str(&[jget(author, "user_openid"), jget(author, "id")]);
                    if !openid.is_empty() {
                        return openid;
                    }
                }
            }
            let v = jstr(jget(data, "user_openid"));
            return if v.is_empty() {
                self.extract_sender_id(data, event_type)
            } else {
                v
            };
        }
        if event_type == "GROUP_AT_MESSAGE_CREATE" || event_type == "GROUP_MESSAGE_CREATE" {
            let v = first_str(&[jget(data, "group_openid"), jget(data, "group_id")]);
            return v;
        }
        if event_type == "DIRECT_MESSAGE_CREATE" {
            return jstr(jget(data, "guild_id"));
        }
        jstr(jget(data, "channel_id"))
    }

    fn is_bot_mentioned(&self, data: &Value, event_type: &str) -> bool {
        if event_type == "GROUP_AT_MESSAGE_CREATE" {
            return true;
        }
        if let Some(mentions) = jget(data, "mentions") {
            if let Some(arr) = mentions.as_array() {
                for mention in arr {
                    if !mention.is_object() {
                        continue;
                    }
                    if matches!(mention.get("is_you"), Some(Value::Bool(true))) {
                        return true;
                    }
                    let mention_id =
                        first_str(&[jget(mention, "id"), jget(mention, "user_openid"), jget(mention, "member_openid")]);
                    if !mention_id.is_empty() && mention_id == self.config.app_id {
                        return true;
                    }
                }
            }
        }
        let content = jstr(jget(data, "content"));
        let app_id = &self.config.app_id;
        content.contains(&format!("<@!{app_id}>")) || content.contains(&format!("<@{app_id}>"))
    }

    /// Remove only the bot's QQ mention token from agent-facing text.
    fn strip_bot_mention_raw(&self, text: &str, data: &Value, event_type: &str) -> String {
        let mut bot_ids: Vec<String> = Vec::new();
        if let Some(mentions) = jget(data, "mentions") {
            if let Some(arr) = mentions.as_array() {
                for mention in arr {
                    if !mention.is_object() {
                        continue;
                    }
                    if !matches!(mention.get("is_you"), Some(Value::Bool(true))) {
                        continue;
                    }
                    for key in ["id", "user_openid", "member_openid"] {
                        let v = jstr(jget(mention, key));
                        if !v.is_empty() {
                            bot_ids.push(v);
                        }
                    }
                }
            }
        }

        let mut clean = text.to_string();
        for bot_id in bot_ids {
            let pattern = format!(r"<@!?{}>\s*", regex::escape(&bot_id));
            if let Ok(re) = Regex::new(&format!("(?i){pattern}")) {
                clean = re.replace_all(&clean, "").to_string();
            }
        }

        if event_type == "GROUP_AT_MESSAGE_CREATE" || event_type == "AT_MESSAGE_CREATE" {
            clean = AT_MENTION_RE.replace(&clean, "").to_string();
        }
        clean.trim().to_string()
    }

    /// Append normalized QQ attachments, returning platform voice transcripts.
    fn append_attachments(&self, parts: &mut Vec<ContentPart>, attachments: &[Value]) -> Vec<String> {
        let mut existing_urls: HashSet<String> = parts
            .iter()
            .filter(|p| !p.is_text())
            .filter_map(|p| p.url().map(|u| u.to_string()))
            .collect();
        let mut voice_transcripts: Vec<String> = Vec::new();

        for att in attachments {
            if !att.is_object() {
                continue;
            }
            let content_type = jstr(jget(att, "content_type")).to_lowercase();
            let wav_url = jstr(jget(att, "voice_wav_url")).trim().to_string();
            let is_voice =
                content_type == "voice" || content_type.starts_with("audio/") || !wav_url.is_empty();
            let transcript = jstr(jget(att, "asr_refer_text")).trim().to_string();
            if is_voice && !transcript.is_empty() && !voice_transcripts.contains(&transcript) {
                voice_transcripts.push(transcript.clone());
            }
            if is_voice && !transcript.is_empty() {
                continue;
            }
            let mut att_url = if is_voice && !wav_url.is_empty() {
                wav_url.clone()
            } else {
                jstr(jget(att, "url")).trim().to_string()
            };
            if att_url.is_empty() {
                continue;
            }
            if att_url.starts_with("//") {
                att_url = format!("https:{att_url}");
            } else if !att_url.starts_with("http") {
                att_url = format!("https://{att_url}");
            }
            if existing_urls.contains(&att_url) {
                continue;
            }
            let mime_type: Option<String> = if is_voice && !wav_url.is_empty() {
                Some("audio/wav".to_string())
            } else if content_type.contains('/') {
                Some(content_type.clone())
            } else {
                None
            };
            let filename = first_str(&[
                jget(att, "filename"),
                jget(att, "file_name"),
                jget(att, "name"),
            ]);

            if content_type.starts_with("image/") || is_image_url(&att_url, &filename) {
                parts.push(ContentPart::Image(ImageContent {
                    url: att_url.clone(),
                    mime_type,
                    ..Default::default()
                }));
            } else if content_type.starts_with("video/") {
                parts.push(ContentPart::Video(VideoContent {
                    url: att_url.clone(),
                    mime_type,
                    ..Default::default()
                }));
            } else if is_voice {
                parts.push(ContentPart::Audio(AudioContent {
                    url: att_url.clone(),
                    mime_type,
                    ..Default::default()
                }));
            } else {
                parts.push(ContentPart::File(FileContent {
                    url: att_url.clone(),
                    filename,
                    mime_type,
                    ..Default::default()
                }));
            }
            existing_urls.insert(att_url);
        }
        voice_transcripts
    }

    /// Find the referenced QQ `msg_elements` entry by message index.
    fn find_quoted_element(data: &Value) -> Option<Value> {
        let elements = match jget(data, "msg_elements").and_then(|v| v.as_array()) {
            Some(a) if !a.is_empty() => a,
            _ => return None,
        };
        let ext = jget(data, "message_scene")
            .and_then(|s| jget(s, "ext"))
            .and_then(|e| e.as_array());
        let mut ref_idx = String::new();
        let mut own_idx = String::new();
        if let Some(ext) = ext {
            for entry in ext {
                if let Some(s) = entry.as_str() {
                    if let Some(rest) = s.strip_prefix("ref_msg_idx=") {
                        ref_idx = rest.to_string();
                    } else if let Some(rest) = s.strip_prefix("msg_idx=") {
                        own_idx = rest.to_string();
                    }
                }
            }
        }
        if ref_idx.is_empty() {
            return None;
        }
        for element in elements {
            if element.is_object() {
                let idx = jstr(jget(element, "msg_idx"));
                if idx == ref_idx {
                    return Some(element.clone());
                }
            }
        }
        for element in elements {
            if !element.is_object() {
                continue;
            }
            let idx = jstr(jget(element, "msg_idx"));
            if !idx.is_empty() && idx != own_idx {
                return Some(element.clone());
            }
        }
        None
    }

    fn parse_content(&self, data: &Value, event_type: &str) -> Vec<ContentPart> {
        let mut parts: Vec<ContentPart> = Vec::new();
        let raw_content = jstr(jget(data, "content"));
        let mut text_parts: Vec<String> = Vec::new();

        for cap in IMG_TAG_RE.captures_iter(&raw_content) {
            if let Some(m) = cap.get(1) {
                parts.push(ContentPart::Image(ImageContent {
                    url: m.as_str().to_string(),
                    ..Default::default()
                }));
            }
        }
        for cap in VIDEO_TAG_RE.captures_iter(&raw_content) {
            if let Some(m) = cap.get(1) {
                parts.push(ContentPart::Video(VideoContent {
                    url: m.as_str().to_string(),
                    ..Default::default()
                }));
            }
        }
        for cap in AUDIO_TAG_RE.captures_iter(&raw_content) {
            if let Some(m) = cap.get(1) {
                parts.push(ContentPart::Audio(AudioContent {
                    url: m.as_str().to_string(),
                    ..Default::default()
                }));
            }
        }
        for cap in FILE_TAG_RE.captures_iter(&raw_content) {
            if let Some(m) = cap.get(1) {
                parts.push(ContentPart::File(FileContent {
                    url: m.as_str().to_string(),
                    ..Default::default()
                }));
            }
        }

        let clean_text = MEDIA_TAG_RE.replace_all(&raw_content, "").to_string();
        let clean_text = self.strip_bot_mention_raw(&clean_text, data, event_type);

        if let Some(quoted) = Self::find_quoted_element(data) {
            let quoted_text = jstr(jget(&quoted, "content")).trim().to_string();
            let quoted_attachments = jget(&quoted, "attachments")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let mut quoted_voice_transcripts: Vec<String> = Vec::new();
            if !quoted_text.is_empty() {
                text_parts.push(format!("[quoted message: {quoted_text}]"));
            }
            if jget(&quoted, "attachments").map(|v| v.is_array()).unwrap_or(false) {
                quoted_voice_transcripts = self.append_attachments(&mut parts, &quoted_attachments);
            }
            if quoted_text.is_empty() && !quoted_voice_transcripts.is_empty() {
                text_parts.push(format!("[quoted message: {}]", quoted_voice_transcripts.join(" ")));
            } else if quoted_text.is_empty() && quoted_attachments.is_empty() {
                text_parts.push("[quoted message]".to_string());
            }
        }

        if !clean_text.is_empty() {
            text_parts.push(clean_text);
        }
        if !text_parts.is_empty() {
            parts.insert(0, ContentPart::text(text_parts.join("\n")));
        }

        if let Some(attachments) = jget(data, "attachments").and_then(|v| v.as_array()) {
            let voice_transcripts = self.append_attachments(&mut parts, attachments);
            let existing_texts: HashSet<String> = parts
                .iter()
                .filter_map(|p| p.text_value())
                .map(|t| t.trim().to_string())
                .collect();
            let new_transcripts: Vec<String> = voice_transcripts
                .into_iter()
                .filter(|t| !existing_texts.contains(t))
                .collect();
            if !new_transcripts.is_empty() {
                parts.insert(0, ContentPart::text(new_transcripts.join("\n")));
            }
        }

        if parts.is_empty() {
            parts.push(ContentPart::text(""));
        }
        parts
    }

    /// Normalize QQ's mention-time recent messages into shared context.
    fn parse_platform_group_context(&self, data: &Value, conversation_id: &str) -> Option<GroupContext> {
        let elements = match jget(data, "msg_elements").and_then(|v| v.as_array()) {
            Some(a) if !a.is_empty() => a,
            _ => return None,
        };
        if Self::find_quoted_element(data).is_some() {
            return None;
        }

        let ext = jget(data, "message_scene")
            .and_then(|s| jget(s, "ext"))
            .and_then(|e| e.as_array());
        let mut own_idx = String::new();
        if let Some(ext) = ext {
            for entry in ext {
                if let Some(s) = entry.as_str() {
                    if let Some(rest) = s.strip_prefix("msg_idx=") {
                        own_idx = rest.to_string();
                        break;
                    }
                }
            }
        }

        let mut messages: Vec<GroupContextMessage> = Vec::new();
        for element in elements {
            if !element.is_object() {
                continue;
            }
            let element_idx = jstr(jget(element, "msg_idx"));
            if !own_idx.is_empty() && element_idx == own_idx {
                continue;
            }
            let mut text = jstr(jget(element, "content")).trim().to_string();
            let mut media: Vec<ContentPart> = Vec::new();
            if let Some(attachments) = jget(element, "attachments").and_then(|v| v.as_array()) {
                let voice_transcripts = self.append_attachments(&mut media, attachments);
                if !voice_transcripts.is_empty() {
                    if !text.is_empty() {
                        let mut seen: Vec<String> = Vec::new();
                        let mut combined: Vec<String> = Vec::new();
                        for t in std::iter::once(text.clone()).chain(voice_transcripts.iter().cloned()) {
                            if !seen.contains(&t) {
                                seen.push(t.clone());
                                combined.push(t);
                            }
                        }
                        text = combined.join("\n");
                    } else {
                        text = voice_transcripts.join("\n");
                    }
                }
            }
            if text.is_empty() && media.is_empty() {
                continue;
            }
            let author = jget(element, "author").cloned().unwrap_or(Value::Null);
            let sender_id = {
                let v = first_str(&[
                    jget(&author, "member_openid"),
                    jget(&author, "user_openid"),
                    jget(&author, "id"),
                ]);
                if v.is_empty() { "unknown".to_string() } else { v }
            };
            messages.push(GroupContextMessage {
                message_id: {
                    let id = jstr(jget(element, "id"));
                    if id.is_empty() { element_idx.clone() } else { id }
                },
                sender_id,
                sender_name: jstr(jget(&author, "username")),
                text,
                content: media,
                timestamp: parse_event_timestamp(jget(element, "timestamp")),
            });
        }
        if messages.is_empty() {
            return None;
        }
        Some(GroupContext {
            conversation_id: conversation_id.to_string(),
            visibility: "mention_recent".to_string(),
            activation: "mention".to_string(),
            messages,
            capability_degraded: false,
        })
    }

    // --- media / upload ---------------------------------------------------

    /// Map a content part to `(qq_file_type, filename_or_none)`.
    fn classify_qq_file(media: &ContentPart) -> (i64, Option<String>) {
        match media {
            ContentPart::Image(_) => (1, None),
            ContentPart::Video(_) => (2, None),
            ContentPart::Audio(_) => (3, None),
            ContentPart::File(f) => {
                let name = if f.filename.is_empty() {
                    "attachment".to_string()
                } else {
                    f.filename.clone()
                };
                (4, Some(name))
            }
            ContentPart::Text(_) => (1, None),
        }
    }

    /// Upload media to QQ and return a `file_info` body for the rich-media send.
    async fn upload_media(
        &self,
        transport: &QqTransport,
        to_handle: &str,
        msg_type: &str,
        meta: &HashMap<String, Value>,
        file_type: i64,
        url: Option<&str>,
        data: Option<&[u8]>,
        filename: Option<&str>,
    ) -> Option<Value> {
        if url.is_none() && data.is_none() {
            return None;
        }
        let auth = match transport.fresh_auth().await {
            Ok(a) => a,
            Err(_) => return None,
        };

        let mut body = json!({ "file_type": file_type, "srv_send_msg": false });
        if let Some(u) = url {
            body["url"] = json!(u);
        } else if let Some(d) = data {
            body["file_data"] = json!(base64::engine::general_purpose::STANDARD.encode(d));
        }
        if file_type == 4 {
            if let Some(name) = filename {
                body["file_name"] = json!(name);
            }
        }

        let endpoint = if msg_type == "c2c" {
            let openid = meta_get_str(meta, "user_openid").unwrap_or_else(|| to_handle.to_string());
            format!("/v2/users/{openid}/files")
        } else if msg_type == "group" {
            let g = meta_get_str(meta, "group_openid").unwrap_or_else(|| to_handle.to_string());
            format!("/v2/groups/{g}/files")
        } else {
            return None;
        };
        let api_url = format!("{}{endpoint}", transport.api_base);

        let resp = transport
            .http
            .post(&api_url)
            .header("Authorization", auth)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .ok()?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status == 200 || status == 201 {
            let payload: Value = serde_json::from_str(&text).ok()?;
            if let Some(fi) = payload.get("file_info") {
                return Some(json!({ "file_info": fi.clone() }));
            }
            return Some(payload);
        }
        tracing::error!(
            "QQ media upload failed: status={status} body={}",
            truncate(&text, 200)
        );
        None
    }

    /// Three-tier upload: URL pull → base64 → fetch+base64 retry.
    async fn upload_with_fallback(
        &self,
        transport: &QqTransport,
        to_handle: &str,
        msg_type: &str,
        meta: &HashMap<String, Value>,
        file_type: i64,
        url: Option<&str>,
        data: Option<&[u8]>,
        filename: Option<&str>,
    ) -> Option<Value> {
        // Priority 1: URL direct upload.
        if let Some(u) = url {
            if let Some(info) = self
                .upload_media(transport, to_handle, msg_type, meta, file_type, Some(u), None, filename)
                .await
            {
                return Some(info);
            }
        }
        // Priority 2: base64 direct upload from already-loaded bytes.
        if let Some(d) = data {
            if let Some(info) = self
                .upload_media(transport, to_handle, msg_type, meta, file_type, None, Some(d), filename)
                .await
            {
                return Some(info);
            }
        }
        // Priority 3: URL given but pull-through failed and we have no bytes yet.
        if url.is_some() && data.is_none() {
            let fetched = match self.fetch_remote_media(url.unwrap()).await {
                Ok((bytes, _)) => bytes,
                Err(e) => {
                    tracing::warn!(
                        "QQChannel fallback download failed: {} err={e}",
                        truncate(url.unwrap(), 80)
                    );
                    return None;
                }
            };
            if let Some(info) = self
                .upload_media(transport, to_handle, msg_type, meta, file_type, None, Some(&fetched), filename)
                .await
            {
                tracing::info!("QQChannel upload succeeded via fetch+base64 fallback");
                return Some(info);
            }
        }
        None
    }

    // --- C2C streaming ----------------------------------------------------

    fn should_stream_c2c(&self, subject: &ChannelSubject) -> bool {
        if !self.config.c2c_streaming {
            return false;
        }
        let (msg_type, meta) = resolve_send_routing(subject);
        msg_type == "c2c" && meta_nonempty(&meta, "msg_id")
    }

    async fn qq_deliver_static(&mut self, subject: &ChannelSubject, text: &str) {
        let cleaned = text.trim();
        if cleaned.is_empty() {
            return;
        }
        if let Err(e) = self.send_text(subject, cleaned).await {
            tracing::warn!(
                "QQ static markdown send failed: channel={} err={e}",
                self.channel_id()
            );
        }
    }

    async fn process_inbound_streaming(
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
        self.acquire_rate_slot().await;
        let (_, meta) = resolve_send_routing(subject);
        let user_id = meta_get_str(&meta, "user_openid").unwrap_or_else(|| subject.subject_id.clone());
        let inbound_msg_id = meta_get_str(&meta, "msg_id").unwrap_or_default();

        let http = match self.core.ensure_http().await {
            Ok(h) => h,
            Err(_) => return base_process_inbound(self, message, subject).await,
        };
        let transport = self.make_transport(http);

        let send_frame: stream::SendFrameFn = {
            let t = transport.clone();
            Arc::new(move |frame: stream::StreamFrame| {
                let t = t.clone();
                Box::pin(async move { send_stream_frame_impl(&t, &frame).await })
                    as Pin<Box<dyn std::future::Future<Output = Result<Option<String>>> + Send>>
            })
        };

        let session = Arc::new(stream::StreamSession::new(
            user_id,
            inbound_msg_id,
            None,
            self.config.stream_throttle_ms,
            self.config.stream_done_retries,
            send_frame,
        ));
        *self.active_stream.lock().unwrap() = Some(session.clone());

        let constraints = self.core.constraints().clone();
        let show_thinking = constraints.show_thinking;
        let show_tool_hints = constraints.show_tool_hints;

        // Typing keepalive until the first readable stream frame.
        let typing_handle: Option<JoinHandle<()>> = {
            let interval = self.config.stream_hold_keepalive_s;
            if interval > 0.0 {
                let s = session.clone();
                let t = transport.clone();
                let subj = subject.clone();
                Some(tokio::spawn(async move {
                    while !s.closing() && !s.failed() {
                        if s.is_visible() {
                            return;
                        }
                        let _ = qq_input_notify(&t, &subj).await;
                        tokio::time::sleep(Duration::from_secs_f64(interval)).await;
                    }
                }))
            } else {
                None
            }
        };

        let mut event_stream = processor(message.clone());

        let mut text_parts: Vec<String> = Vec::new();
        let mut thinking_parts: Vec<String> = Vec::new();
        let mut media_buffer: Vec<ContentPart> = Vec::new();
        let mut processing_succeeded = false;
        let mut errored = false;

        while let Some(event) = event_stream.next().await {
            match event.event_type {
                MessageEventType::Delta => {
                    if let Some(t) = take_thinking_text(&mut thinking_parts, show_thinking, &constraints) {
                        self.qq_deliver_static(subject, &t).await;
                    }
                    for part in &event.content {
                        if let Some(raw) = part.text_value() {
                            if raw.is_empty() {
                                continue;
                            }
                            let extracted = extract_think_blocks(raw);
                            if !extracted.is_empty() && show_thinking {
                                thinking_parts.push(extracted);
                            }
                            let visible = strip_think_tags(raw);
                            if !visible.is_empty() {
                                text_parts.push(visible);
                            }
                        }
                    }
                    offer_answer(&session, &text_parts, false);
                }
                MessageEventType::Message => {
                    if let Some(t) = take_thinking_text(&mut thinking_parts, show_thinking, &constraints) {
                        self.qq_deliver_static(subject, &t).await;
                    }
                    let extra = extract_text(&event.content);
                    let extracted = extract_think_blocks(&extra);
                    if !extracted.is_empty() && show_thinking {
                        thinking_parts.push(extracted);
                        if let Some(t) =
                            take_thinking_text(&mut thinking_parts, show_thinking, &constraints)
                        {
                            self.qq_deliver_static(subject, &t).await;
                        }
                    }
                    let extra = strip_think_tags(&extra);
                    if !extra.is_empty() {
                        let current = full_text(&text_parts);
                        if current.is_empty() {
                            text_parts.push(extra);
                        } else if extra.starts_with(&current) || current.starts_with(&extra) {
                            if extra.len() > current.len() {
                                text_parts.clear();
                                text_parts.push(extra);
                            }
                        } else {
                            text_parts.push(extra);
                        }
                    }
                    offer_answer(&session, &text_parts, false);
                    media_buffer.extend(extract_media(&event.content));
                }
                MessageEventType::ThinkingDelta => {
                    if show_thinking {
                        for part in &event.content {
                            if let Some(t) = part.text_value() {
                                if !t.is_empty() {
                                    thinking_parts.push(t.to_string());
                                }
                            }
                        }
                    }
                }
                MessageEventType::Thinking => {
                    if show_thinking {
                        let block = extract_text(&event.content).trim().to_string();
                        if !block.is_empty() {
                            thinking_parts.clear();
                            thinking_parts.push(block);
                        }
                        if let Some(t) =
                            take_thinking_text(&mut thinking_parts, show_thinking, &constraints)
                        {
                            self.qq_deliver_static(subject, &t).await;
                        }
                    }
                }
                MessageEventType::ToolStart => {
                    if let Some(t) = take_thinking_text(&mut thinking_parts, show_thinking, &constraints) {
                        self.qq_deliver_static(subject, &t).await;
                    }
                    text_parts.clear();
                    Arc::clone(&session).discard_unsent();
                    if show_tool_hints {
                        let hint = tool_hint_message(&event.metadata, &constraints, "start");
                        if !hint.is_empty() {
                            self.qq_deliver_static(subject, &hint).await;
                        }
                    }
                }
                MessageEventType::ToolEnd => {
                    if show_tool_hints {
                        let hint = tool_hint_message(&event.metadata, &constraints, "end");
                        if !hint.is_empty() {
                            self.qq_deliver_static(subject, &hint).await;
                        }
                    }
                }
                MessageEventType::Error => {
                    let last = session.last_accepted();
                    Arc::clone(&session).finish(last, true).await;
                    let msg = event
                        .error
                        .clone()
                        .unwrap_or_else(|| "An unexpected error occurred.".to_string());
                    self.qq_deliver_static(subject, &msg).await;
                    errored = true;
                    break;
                }
                MessageEventType::Completed => {
                    processing_succeeded = true;
                    break;
                }
                MessageEventType::Typing | MessageEventType::Flush => {}
            }
        }

        if !errored {
            let answer = answer_text(&text_parts);
            offer_answer(&session, &text_parts, true);
            let locked = if !answer.is_empty() {
                locked_stream_text(&session, &answer)
            } else {
                session.last_accepted()
            };
            Arc::clone(&session).finish(locked, false).await;
            if let Some(t) = take_thinking_text(&mut thinking_parts, show_thinking, &constraints) {
                self.qq_deliver_static(subject, &t).await;
            }
            if session.unlinked() || session.sent_frames() <= 0 || session.last_accepted().trim().is_empty()
            {
                self.qq_deliver_static(subject, &answer).await;
            }
            for media in &media_buffer {
                let _ = self.reply_media(subject, media).await;
            }
        }

        // finally: stop typing, drop the active stream, flush if still open.
        if let Some(h) = typing_handle {
            h.abort();
        }
        {
            let mut guard = self.active_stream.lock().unwrap();
            let is_same = guard
                .as_ref()
                .map(|s| Arc::ptr_eq(s, &session))
                .unwrap_or(false);
            if is_same {
                *guard = None;
            }
        }
        if !session.closing() {
            let last = session.last_accepted();
            Arc::clone(&session).finish(last, true).await;
        }

        if errored {
            return false;
        }
        processing_succeeded
    }
}

#[async_trait]
impl Channel for QQChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }
    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            send_rate_limit: Some((20, 60.0)),
            typing_keepalive_interval: 5.0,
            show_thinking: false,
            show_tool_hints: false,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        if self.config.app_id.is_empty() {
            return Err(GatewayError::Other("QQChannel: app_id is required".into()));
        }
        // Python has no core-level connection session; the ws session id is the
        // channel session and falls back to "qq-unconnected".
        self.core
            .set_connection_session(Some("qq-unconnected".to_string()));
        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        let enqueue = self.core.enqueue_sender();
        let app_id = self.config.app_id.clone();
        let secret = self.config.client_secret();
        let sandbox = self.config.sandbox;
        let intents = self.config.intents.unwrap_or(DEFAULT_INTENTS);
        let token_state = self.token_state.clone();
        let dedup = self.seen_messages.clone();
        let ws_session = self.ws_session.clone();
        let task = tokio::spawn(async move {
            run_gateway(
                app_id,
                secret,
                sandbox,
                intents,
                enqueue,
                token_state,
                dedup,
                ws_session,
            )
            .await;
        });
        self.ws_task = Some(task);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        let stream = self.active_stream.lock().unwrap().take();
        if let Some(s) = stream {
            let last = s.last_accepted();
            let _ = s.finish(last, true).await;
        }
        if let Some(t) = self.ws_task.take() {
            t.abort();
        }
        self.core.set_connection_session(None);
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let (msg_type, meta) = resolve_send_routing(subject);
        let msg_id = meta_get_str(&meta, "msg_id");
        let http = self.core.ensure_http().await?;
        let transport = self.make_transport(http);

        // Guild channel / DM: plain content (markdown not applicable).
        if msg_type == "channel" || msg_type == "direct" {
            let mut payload = json!({ "content": text });
            if let Some(mid) = &msg_id {
                payload["msg_id"] = json!(mid);
            }
            return transport
                .send_message(&subject.subject_id, &mut payload, &msg_type, &meta)
                .await;
        }

        // C2C / group: try markdown first, fall back to plain text.
        let target_key = meta_get_str(&meta, "user_openid")
            .or_else(|| meta_get_str(&meta, "group_openid"))
            .unwrap_or_else(|| subject.subject_id.clone());

        if self.markdown_unsupported.lock().unwrap().contains(&target_key) {
            let mut payload = json!({ "content": text, "msg_type": 0 });
            if let Some(mid) = &msg_id {
                payload["msg_id"] = json!(mid);
            }
            return transport
                .send_message(&subject.subject_id, &mut payload, &msg_type, &meta)
                .await;
        }

        let mut md_payload = json!({
            "content": "",
            "msg_type": 2,
            "markdown": { "content": text },
        });
        if let Some(mid) = &msg_id {
            md_payload["msg_id"] = json!(mid);
            md_payload["msg_seq"] = json!(get_next_msg_seq(mid));
        }

        let (ok, _status, _body) = transport
            .try_send_message(&subject.subject_id, &md_payload, &msg_type, &meta)
            .await;
        if ok {
            return Ok(());
        }

        tracing::info!("QQChannel markdown not supported for {target_key}, falling back to plain text");
        self.markdown_unsupported
            .lock()
            .unwrap()
            .insert(target_key.clone());
        let mut payload = json!({ "content": text, "msg_type": 0 });
        if let Some(mid) = &msg_id {
            payload["msg_id"] = json!(mid);
        }
        transport
            .send_message(&subject.subject_id, &mut payload, &msg_type, &meta)
            .await
    }

    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        let (msg_type, meta) = resolve_send_routing(subject);
        let msg_id = meta_get_str(&meta, "msg_id");
        let http = self.core.ensure_http().await?;
        let transport = self.make_transport(http);

        let mut text_parts: Vec<String> = Vec::new();
        let mut media_parts: Vec<ContentPart> = Vec::new();
        for part in parts {
            if let Some(t) = part.text_value() {
                text_parts.push(t.to_string());
            } else {
                media_parts.push(part.clone());
            }
        }

        if !text_parts.is_empty() {
            let mut payload = json!({ "content": text_parts.join("\n") });
            if let Some(mid) = &msg_id {
                payload["msg_id"] = json!(mid);
            }
            transport
                .send_message(&subject.subject_id, &mut payload, &msg_type, &meta)
                .await?;
        }

        for media in &media_parts {
            self.send_media(subject, media).await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let meta = subject.metadata.clone();
        let msg_type = match meta.get("msg_type") {
            Some(Value::String(s)) if !s.is_empty() => s.clone(),
            Some(v) if truthy(Some(v)) => value_str(v),
            _ => "channel".to_string(),
        };
        let msg_id = meta_get_str(&meta, "msg_id");

        let url = media.url().map(|u| u.to_string()).filter(|u| !u.is_empty());
        let has_inline = media
            .data()
            .map(|d| !d.is_empty())
            .unwrap_or(false)
            || media.local_path().map(|p| !p.is_empty()).unwrap_or(false);
        if url.is_none() && !has_inline {
            return Ok(());
        }

        // Guild / DM channel: protocol only accepts a public URL.
        if msg_type == "channel" || msg_type == "direct" {
            if url.is_none() {
                let label = media.label_with_filename();
                return self
                    .send_text(subject, &format!("[{label} (local file, not deliverable on guild)]"))
                    .await;
            }
            let mut payload = json!({ "content": "", "image": url.clone().unwrap() });
            if let Some(mid) = &msg_id {
                payload["msg_id"] = json!(mid);
            }
            let http = self.core.ensure_http().await?;
            let transport = self.make_transport(http);
            return transport
                .send_message(&subject.subject_id, &mut payload, &msg_type, &meta)
                .await;
        }

        if msg_type != "c2c" && msg_type != "group" {
            return Ok(());
        }

        let (file_type, filename) = Self::classify_qq_file(media);

        let local_bytes: Option<Vec<u8>> = if has_inline {
            match self.load_media_bytes(media).await {
                Ok((bytes, _)) => Some(bytes),
                Err(e) => {
                    tracing::warn!("QQChannel send_media: failed to read inline bytes err={e}");
                    None
                }
            }
        } else {
            None
        };

        let http = self.core.ensure_http().await?;
        let transport = self.make_transport(http);
        let file_info = self
            .upload_with_fallback(
                &transport,
                &subject.subject_id,
                &msg_type,
                &meta,
                file_type,
                url.as_deref(),
                local_bytes.as_deref(),
                filename.as_deref(),
            )
            .await;

        let file_info = match file_info {
            Some(fi) => fi,
            None => {
                let label = media.label_with_filename();
                let text = match &url {
                    Some(u) => format!("[{label}: {u}]"),
                    None => format!("[{label}]"),
                };
                return self.send_text(subject, &text).await;
            }
        };

        let seq_key = msg_id.clone().unwrap_or_else(|| format!("{msg_type}-rich"));
        let mut send_payload = json!({
            "msg_type": 7,
            "msg_seq": get_next_msg_seq(&seq_key),
            "media": file_info,
            "content": "",
        });
        if let Some(mid) = &msg_id {
            send_payload["msg_id"] = json!(mid);
        }
        transport
            .send_message(&subject.subject_id, &mut send_payload, &msg_type, &meta)
            .await
    }

    async fn send_typing_indicator(&mut self, subject: &ChannelSubject) -> Result<()> {
        // Show C2C「正在输入」without writing into the replace bubble.
        let (msg_type, meta) = resolve_send_routing(subject);
        if msg_type != "c2c" {
            return Ok(());
        }
        let user_id = meta_get_str(&meta, "user_openid").unwrap_or_else(|| subject.subject_id.clone());
        let msg_id = meta_get_str(&meta, "msg_id").unwrap_or_default();
        if user_id.is_empty() || msg_id.is_empty() {
            return Ok(());
        }
        let mut payload = json!({
            "msg_type": 6,
            "input_notify": { "input_type": 1, "input_second": 60 },
            "msg_id": msg_id,
        });
        let http = self.core.ensure_http().await?;
        let transport = self.make_transport(http);
        if let Err(e) = transport
            .send_message(&user_id, &mut payload, "c2c", &meta)
            .await
        {
            tracing::debug!("QQ C2C input notify failed: {e}");
        }
        Ok(())
    }

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        self.enrich_push_metadata_inner(subject, meta)
    }

    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let http = self.core.ensure_http().await?;
        let transport = self.make_transport(http);
        let auth = transport.fresh_auth().await?;
        let resp = transport
            .http
            .get(url)
            .header("Authorization", auth)
            .send()
            .await?;
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
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let data = resp.bytes().await?;
        Ok((data.to_vec(), mime))
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("qq: expected JSON payload".into()))?;

        let event_type = jstr(jget(v, "t"));
        let data = jget(v, "d").cloned().unwrap_or(Value::Null);

        let sender_id = self.extract_sender_id(&data, &event_type);
        let session_id = self.session_id();
        let content_parts = self.parse_content(&data, &event_type);
        let msg_type = classify_msg_type(&event_type).to_string();
        let is_group = msg_type == "group";
        let conversation_id = if is_group {
            self.extract_to_handle(&data, &event_type)
        } else {
            String::new()
        };
        let sender_name = jget(&data, "author")
            .map(|a| jstr(jget(a, "username")))
            .unwrap_or_default();

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("event_type".to_string(), json!(event_type.clone()));
        metadata.insert("msg_type".to_string(), json!(msg_type));
        metadata.insert("msg_id".to_string(), json!(jstr(jget(&data, "id"))));
        metadata.insert(
            "to_handle".to_string(),
            json!(self.extract_to_handle(&data, &event_type)),
        );
        metadata.insert(
            "chat_type".to_string(),
            json!(if is_group { "group" } else { "dm" }),
        );
        metadata.insert("sender_id".to_string(), json!(sender_id.clone()));
        metadata.insert("sender_name".to_string(), json!(sender_name.clone()));
        if is_group {
            metadata.insert("conversation_id".to_string(), json!(conversation_id.clone()));
            metadata.insert(
                "bot_mentioned".to_string(),
                json!(self.is_bot_mentioned(&data, &event_type)),
            );
        }

        if let Some(x) = jget(&data, "guild_id") {
            metadata.insert("guild_id".to_string(), x.clone());
        }
        if let Some(x) = jget(&data, "channel_id") {
            metadata.insert("channel_id_native".to_string(), x.clone());
        }
        if let Some(x) = jget(&data, "group_id") {
            metadata.insert("group_id".to_string(), x.clone());
        }
        if let Some(x) = jget(&data, "group_openid") {
            metadata.insert("group_openid".to_string(), x.clone());
        }
        if event_type == "C2C_MESSAGE_CREATE" {
            let user_openid = jget(&data, "author")
                .and_then(|a| jget(a, "user_openid").cloned())
                .or_else(|| jget(&data, "user_openid").cloned());
            if let Some(uo) = user_openid {
                if truthy(Some(&uo)) {
                    metadata.insert("user_openid".to_string(), uo);
                }
            }
        }

        let subject_id = if is_group {
            conversation_id.clone()
        } else {
            sender_id.clone()
        };
        let display_name = if !is_group {
            sender_name.clone()
        } else {
            String::new()
        };

        let mut message = InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "qq".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id,
                first_seen: None,
                last_seen: None,
                display_name,
                chat_type: if is_group { "group" } else { "direct" }.to_string(),
                metadata: metadata.clone(),
            }),
            channel_session_id: session_id,
            content: content_parts,
            group_context: None,
            metadata,
            timestamp: parse_event_timestamp(jget(&data, "timestamp")),
        };
        if is_group {
            message.group_context = self.parse_platform_group_context(&data, &conversation_id);
        }
        Ok(message)
    }

    /// C2C streaming replaces the buffered default pipeline when enabled.
    async fn process_inbound(&mut self, message: &InboundMessage, subject: &ChannelSubject) -> bool {
        if !self.should_stream_c2c(subject) {
            return base_process_inbound(self, message, subject).await;
        }
        self.process_inbound_streaming(message, subject).await
    }
}

// ---------------------------------------------------------------------------
// WebSocket gateway
// ---------------------------------------------------------------------------

async fn run_gateway(
    app_id: String,
    secret: String,
    sandbox: bool,
    intents: i64,
    enqueue: Option<EnqueueFn>,
    token_state: Arc<AsyncMutex<TokenState>>,
    dedup: Dedup,
    ws_session: Arc<StdMutex<Option<String>>>,
) {
    let api_base = if sandbox { QQ_SANDBOX_API } else { QQ_API };
    let transport = QqTransport {
        api_base: api_base.to_string(),
        app_id,
        secret,
        http: reqwest::Client::new(),
        token_state,
    };
    let mut session_id: Option<String> = None;
    let mut last_seq: Option<i64> = None;
    let mut backoff = 1.0f64;
    let mut force_refresh = false;

    loop {
        if force_refresh {
            tracing::info!("QQChannel forcing access_token refresh before reconnect");
            transport.invalidate().await;
            force_refresh = false;
        }

        if let Err(e) = transport.ensure_token().await {
            tracing::warn!("QQ token error: {e}");
            tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(60.0);
            continue;
        }
        let gateway = match fetch_gateway(&transport).await {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!("QQ gateway url error: {e}");
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(60.0);
                continue;
            }
        };
        tracing::info!("QQ gateway connecting: {gateway}");

        let end = match tokio_tungstenite::connect_async(&gateway).await {
            Ok((ws, _)) => {
                match handle_connection(
                    ws,
                    &transport,
                    intents,
                    &enqueue,
                    &dedup,
                    &ws_session,
                    &mut session_id,
                    &mut last_seq,
                    &mut backoff,
                    &mut force_refresh,
                )
                .await
                {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("QQ ws loop ended: {e}");
                        WsEnd::Closed
                    }
                }
            }
            Err(e) => {
                tracing::warn!("QQ ws connect failed: {e}");
                WsEnd::Closed
            }
        };

        if matches!(end, WsEnd::Reconnect) {
            // Server-initiated cycling — reconnect at the current delay, no inflation.
            tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
        } else {
            tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
            backoff = (backoff * 2.0).min(60.0);
        }
    }
}

async fn fetch_gateway(transport: &QqTransport) -> Result<String> {
    let auth = transport.fresh_auth().await?;
    let resp = transport
        .http
        .get(format!("{}/gateway/bot", transport.api_base))
        .header("Authorization", auth)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(GatewayError::Other(format!(
            "Failed to fetch gateway URL: {status} {body}"
        )));
    }
    let v: Value = resp.json().await?;
    v.get("url")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| GatewayError::Other(format!("No gateway URL in response: {v}")))
}

enum WsEnd {
    ReadyOrResumed,
    Reconnect,
    Invalid,
    Closed,
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    transport: &QqTransport,
    intents: i64,
    enqueue: &Option<EnqueueFn>,
    dedup: &Dedup,
    ws_session: &Arc<StdMutex<Option<String>>>,
    session_id: &mut Option<String>,
    last_seq: &mut Option<i64>,
    backoff: &mut f64,
    force_refresh: &mut bool,
) -> Result<WsEnd> {
    let (mut write, mut read) = ws.split();
    let mut hb_interval = Duration::from_secs_f64(41.25);
    let mut next_hb = tokio::time::Instant::now() + hb_interval;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_hb) => {
                let hb = json!({ "op": 1, "d": last_seq });
                write
                    .send(Message::Text(hb.to_string().into()))
                    .await
                    .map_err(|e| GatewayError::Other(e.to_string()))?;
                next_hb = tokio::time::Instant::now() + hb_interval;
            }
            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    _ => return Ok(WsEnd::Closed),
                };
                let txt = match msg {
                    Message::Text(t) => t,
                    Message::Close(_) => return Ok(WsEnd::Closed),
                    _ => continue,
                };
                let v: Value = match serde_json::from_str(txt.as_str()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let op = v.get("op").and_then(|x| x.as_i64()).unwrap_or(-1);
                match op {
                    10 => {
                        if let Some(iv) = v.pointer("/d/heartbeat_interval").and_then(|x| x.as_f64()) {
                            hb_interval = Duration::from_secs_f64(iv / 1000.0);
                            next_hb = tokio::time::Instant::now() + hb_interval;
                        }
                        let token = transport.ensure_token().await.unwrap_or_default();
                        let payload = if session_id.is_some() && last_seq.is_some() {
                            json!({ "op": 6, "d": { "token": format!("QQBot {token}"), "session_id": session_id, "seq": last_seq } })
                        } else {
                            json!({ "op": 2, "d": { "token": format!("QQBot {token}"), "intents": intents, "shard": [0, 1] } })
                        };
                        write
                            .send(Message::Text(payload.to_string().into()))
                            .await
                            .map_err(|e| GatewayError::Other(e.to_string()))?;
                    }
                    0 => {
                        if let Some(s) = v.get("s").and_then(|x| x.as_i64()) {
                            *last_seq = Some(s);
                        }
                        let t = v.get("t").and_then(|x| x.as_str()).unwrap_or("");
                        match t {
                            "READY" => {
                                *session_id = v
                                    .pointer("/d/session_id")
                                    .and_then(|x| x.as_str())
                                    .map(|s| s.to_string());
                                *ws_session.lock().unwrap() = session_id.clone();
                                // Reset geometric backoff only after a full IDENTIFY success.
                                *backoff = 1.0;
                                tracing::info!("QQ gateway READY");
                            }
                            "RESUMED" => {
                                *backoff = 1.0;
                                tracing::info!("QQ gateway RESUMED");
                            }
                            other => {
                                if is_message_event(other) {
                                    let msg_id = v
                                        .pointer("/d/id")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("");
                                    let duplicate = !msg_id.is_empty() && dedup.seen(msg_id);
                                    if !duplicate {
                                        if let Some(cb) = enqueue {
                                            cb(RawPayload::Json(v.clone()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    7 => {
                        tracing::info!("QQ gateway RECONNECT");
                        return Ok(WsEnd::Reconnect);
                    }
                    9 => {
                        let can_resume = v.get("d").map(|x| truthy(Some(x))).unwrap_or(false);
                        tracing::warn!("QQ invalid session (can_resume={can_resume})");
                        if !can_resume {
                            *session_id = None;
                            *last_seq = None;
                            *ws_session.lock().unwrap() = None;
                            *force_refresh = true;
                        }
                        return Ok(WsEnd::Invalid);
                    }
                    _ => {}
                }
            }
        }
    }
}

// ===========================================================================
// Inlined submodule: stream.py
// ===========================================================================

pub mod stream {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::{Duration, Instant};

    use crate::errors::Result;
    use once_cell::sync::Lazy;

    pub const STREAM_GENERATING: i32 = 1;
    pub const STREAM_DONE: i32 = 10;

    static STREAM_SEQ: AtomicU32 = AtomicU32::new(0);

    /// Process-wide `msg_seq` in `0..65535` (one value per C2C stream).
    pub fn next_stream_msg_seq() -> u32 {
        STREAM_SEQ.fetch_add(1, Ordering::Relaxed).wrapping_add(1) % 65_536
    }

    fn collapse_ws(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// True when `incoming` continues or equals `accepted` (whitespace-tolerant).
    pub fn prefix_matches(accepted: &str, incoming: &str) -> bool {
        if incoming.starts_with(accepted) {
            return true;
        }
        if accepted.is_empty() {
            return true;
        }
        // A newline-only hold must stay; collapsing it to "" would drop the prefix.
        if accepted.trim().is_empty() {
            return false;
        }
        // Incoming that dropped a leading newline is not a legal replace prefix.
        if accepted.starts_with('\r') || accepted.starts_with('\n') {
            match incoming.chars().next() {
                Some(c) if c == '\r' || c == '\n' => {}
                _ => return false,
            }
        }
        collapse_ws(incoming).starts_with(&collapse_ws(accepted))
    }

    fn longest_common_prefix(left: &str, right: &str) -> usize {
        let lb = left.as_bytes();
        let rb = right.as_bytes();
        let limit = lb.len().min(rb.len());
        let mut index = 0;
        while index < limit && lb[index] == rb[index] {
            index += 1;
        }
        index
    }

    /// Build the next replace-mode full text from the last accepted frame.
    pub fn reconcile_stream_text(accepted: &str, incoming: &str) -> String {
        if accepted.is_empty() {
            return incoming.to_string();
        }
        if prefix_matches(accepted, incoming) {
            return incoming.to_string();
        }
        if prefix_matches(incoming, accepted) {
            return accepted.to_string();
        }
        let lcp = longest_common_prefix(accepted, incoming);
        format!("{accepted}{}", &incoming[lcp..])
    }

    /// One QQ `stream_messages` replace frame.
    #[derive(Debug, Clone)]
    pub struct StreamFrame {
        pub user_id: String,
        pub msg_id: String,
        pub msg_seq: u32,
        pub index: i64,
        pub text: String,
        pub state: i32,
        pub stream_msg_id: Option<String>,
    }

    pub type SendFrameFn = Arc<
        dyn Fn(StreamFrame) -> Pin<Box<dyn std::future::Future<Output = Result<Option<String>>> + Send>>
            + Send
            + Sync,
    >;

    static MONO_START: Lazy<Instant> = Lazy::new(Instant::now);

    fn now_monotonic() -> f64 {
        MONO_START.elapsed().as_secs_f64()
    }

    struct StreamInner {
        index: i64,
        last_accepted: String,
        pending: Option<String>,
        in_flight: bool,
        closing: bool,
        stream_msg_id: Option<String>,
        sent_frames: i64,
        failed: bool,
        unlinked: bool,
        last_sent_at: f64,
        inflight_text: Option<String>,
        drain_task: Option<tokio::task::JoinHandle<()>>,
    }

    fn prefix_base_of(g: &StreamInner) -> String {
        let locked = match &g.inflight_text {
            Some(t) => t.clone(),
            None => g.last_accepted.clone(),
        };
        match &g.pending {
            Some(p) => reconcile_stream_text(&locked, p),
            None => locked,
        }
    }

    fn same_visible(g: &StreamInner, merged: &str) -> bool {
        if merged.trim().is_empty() {
            return false;
        }
        if g.pending.as_deref() == Some(merged) {
            return true;
        }
        if g.inflight_text.as_deref() == Some(merged) {
            return true;
        }
        merged == g.last_accepted && g.pending.is_none() && g.inflight_text.is_none()
    }

    /// Single-slot drain for one C2C inbound turn.
    pub struct StreamSession {
        user_id: String,
        msg_id: String,
        msg_seq: u32,
        throttle_ms: i64,
        done_retries: i64,
        send_frame: SendFrameFn,
        inner: Arc<StdMutex<StreamInner>>,
    }

    impl StreamSession {
        pub fn new(
            user_id: String,
            msg_id: String,
            msg_seq: Option<u32>,
            throttle_ms: i64,
            done_retries: i64,
            send_frame: SendFrameFn,
        ) -> Self {
            StreamSession {
                user_id,
                msg_id,
                msg_seq: msg_seq.unwrap_or_else(next_stream_msg_seq),
                throttle_ms: throttle_ms.max(0),
                done_retries: done_retries.max(1),
                send_frame,
                inner: Arc::new(StdMutex::new(StreamInner {
                    index: 0,
                    last_accepted: String::new(),
                    pending: None,
                    in_flight: false,
                    closing: false,
                    stream_msg_id: None,
                    sent_frames: 0,
                    failed: false,
                    unlinked: false,
                    last_sent_at: 0.0,
                    inflight_text: None,
                    drain_task: None,
                })),
            }
        }

        pub fn closing(&self) -> bool {
            self.inner.lock().unwrap().closing
        }

        pub fn failed(&self) -> bool {
            self.inner.lock().unwrap().failed
        }

        pub fn unlinked(&self) -> bool {
            self.inner.lock().unwrap().unlinked
        }

        pub fn sent_frames(&self) -> i64 {
            self.inner.lock().unwrap().sent_frames
        }

        pub fn last_accepted(&self) -> String {
            self.inner.lock().unwrap().last_accepted.clone()
        }

        /// True once a readable frame has been accepted or is queued/in flight.
        pub fn is_visible(&self) -> bool {
            let g = self.inner.lock().unwrap();
            if !g.last_accepted.trim().is_empty() {
                return true;
            }
            g.pending
                .as_deref()
                .map(|p| !p.trim().is_empty())
                .unwrap_or(false)
        }

        pub fn prefix_base(&self) -> String {
            let g = self.inner.lock().unwrap();
            prefix_base_of(&g)
        }

        /// Record the latest candidate full text and kick the drain loop.
        pub fn offer(self: Arc<Self>, text: String) {
            {
                let mut g = self.inner.lock().unwrap();
                if g.failed || g.closing {
                    return;
                }
                let base = prefix_base_of(&g);
                let merged = reconcile_stream_text(&base, &text);
                if same_visible(&g, &merged) {
                    return;
                }
                g.pending = Some(merged);
            }
            self.kick();
        }

        /// Drop queued text that has not been accepted yet.
        pub fn discard_unsent(self: Arc<Self>) {
            let mut g = self.inner.lock().unwrap();
            if g.failed || g.closing {
                return;
            }
            let locked = match &g.inflight_text {
                Some(t) => t.clone(),
                None => g.last_accepted.clone(),
            };
            if locked.trim().is_empty() {
                g.pending = if locked.is_empty() { None } else { Some(locked) };
            } else {
                g.pending = None;
            }
        }

        fn kick(self: Arc<Self>) {
            {
                let mut g = self.inner.lock().unwrap();
                if g.in_flight || g.failed || g.closing {
                    return;
                }
                if g.pending.is_none() {
                    return;
                }
                g.in_flight = true;
            }
            let me = self.clone();
            let handle = tokio::spawn(async move {
                me.drain().await;
            });
            self.inner.lock().unwrap().drain_task = Some(handle);
        }

        async fn drain(self: Arc<Self>) {
            loop {
                let plan: Option<(String, f64)> = {
                    let g = self.inner.lock().unwrap();
                    if g.failed || g.closing {
                        break;
                    }
                    match g.pending.clone() {
                        Some(p) => {
                            let wait = (self.throttle_ms as f64 / 1000.0)
                                - (now_monotonic() - g.last_sent_at);
                            Some((p, wait))
                        }
                        None => break,
                    }
                };
                let (pending, wait) = match plan {
                    Some(x) => x,
                    None => break,
                };
                if wait > 0.0 {
                    tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                    continue;
                }
                {
                    let mut g = self.inner.lock().unwrap();
                    g.pending = None;
                    g.inflight_text = Some(pending.clone());
                }
                self.emit(pending, STREAM_GENERATING).await;
            }
            let mut g = self.inner.lock().unwrap();
            g.in_flight = false;
            let again = !g.failed && !g.closing && g.pending.is_some();
            drop(g);
            if again {
                self.kick();
            }
        }

        pub async fn finish(self: Arc<Self>, text: String, discard_pending: bool) {
            if discard_pending {
                let mut g = self.inner.lock().unwrap();
                g.closing = true;
                g.pending = None;
            } else {
                self.clone().wait_idle().await;
                let need_kick = {
                    let g = self.inner.lock().unwrap();
                    g.pending.is_some() && !g.failed
                };
                if need_kick {
                    self.clone().kick();
                    self.clone().wait_idle().await;
                }
                let mut g = self.inner.lock().unwrap();
                g.closing = true;
                g.pending = None;
            }
            self.clone().wait_idle().await;
            let (sent, unlinked, last) = {
                let g = self.inner.lock().unwrap();
                (g.sent_frames, g.unlinked, g.last_accepted.clone())
            };
            if sent <= 0 || unlinked {
                return;
            }
            let payload = if !text.is_empty() { text } else { last };
            for attempt in 1..=self.done_retries {
                if self.emit(payload.clone(), STREAM_DONE).await {
                    return;
                }
                if attempt == self.done_retries {
                    return;
                }
                tokio::time::sleep(Duration::from_secs_f64(0.4 * attempt as f64)).await;
            }
        }

        async fn wait_idle(self: Arc<Self>) {
            loop {
                let task = { self.inner.lock().unwrap().drain_task.take() };
                match task {
                    Some(h) => {
                        let _ = h.await;
                    }
                    None => break,
                }
            }
            loop {
                let in_flight = self.inner.lock().unwrap().in_flight;
                if !in_flight {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(0.05)).await;
            }
        }

        async fn emit(&self, text: String, state: i32) -> bool {
            let frame = {
                let mut g = self.inner.lock().unwrap();
                // The hold frame may leave without an id. Do not post visible text
                // after that: QQ would show each frame as a new message.
                if state == STREAM_GENERATING
                    && !text.trim().is_empty()
                    && g.sent_frames > 0
                    && g.stream_msg_id.is_none()
                {
                    g.unlinked = true;
                    g.failed = true;
                    return false;
                }
                let frame = StreamFrame {
                    user_id: self.user_id.clone(),
                    msg_id: self.msg_id.clone(),
                    msg_seq: self.msg_seq,
                    index: g.index,
                    text: text.clone(),
                    state,
                    stream_msg_id: g.stream_msg_id.clone(),
                };
                g.inflight_text = Some(text.clone());
                frame
            };

            let result = (self.send_frame)(frame).await;
            let mut g = self.inner.lock().unwrap();
            match result {
                Ok(stream_msg_id) => {
                    g.index += 1;
                    g.last_accepted = text.clone();
                    g.last_sent_at = now_monotonic();
                    g.sent_frames += 1;
                    if stream_msg_id.is_some() && g.stream_msg_id.is_none() {
                        g.stream_msg_id = stream_msg_id;
                    }
                    if state == STREAM_GENERATING
                        && !text.trim().is_empty()
                        && g.stream_msg_id.is_none()
                    {
                        g.unlinked = true;
                        g.failed = true;
                    }
                    if g.inflight_text.as_deref() == Some(text.as_str()) {
                        g.inflight_text = None;
                    }
                    true
                }
                Err(_) => {
                    g.failed = true;
                    tracing::warn!("QQ stream frame failed; falling back to static delivery");
                    if g.inflight_text.as_deref() == Some(text.as_str()) {
                        g.inflight_text = None;
                    }
                    false
                }
            }
        }
    }
}

// ===========================================================================
// Inlined submodule: stream_blocks.py
// ===========================================================================

pub mod stream_blocks {
    use once_cell::sync::Lazy;
    use regex::Regex;

    static FENCE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^( {0,3})(`{3,}|~{3,})(.*)$").unwrap());
    static ATX_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^ {0,3}#{1,6}(?:\s|$)").unwrap());
    static HR_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"^(?: {0,3})(?:(?:\*(?:\s*\*){2,})|(?:-(?:\s*-){2,})|(?:_(?:\s*_){2,}))\s*$").unwrap()
    });
    static LIST_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^ {0,3}(?:[*+-]|\d{1,9}[.)])(?:\s+|$)").unwrap());
    static QUOTE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^ {0,3}>").unwrap());
    static TABLE_SEP_CELL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^:?-+:?$").unwrap());
    static INDENT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(?: {4,}|\t)").unwrap());

    /// Return the longest complete-block prefix of `text`.
    pub fn stable_markdown_prefix(text: &str) -> String {
        if text.is_empty() {
            return String::new();
        }
        let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
        let (complete, _leftover) = split_complete_lines(&normalized);
        let end = stable_line_end(&complete);
        if end <= 0 {
            return String::new();
        }
        let subset = complete[..end].join("\n");
        format!("{subset}\n")
    }

    fn split_complete_lines(text: &str) -> (Vec<String>, String) {
        if !text.contains('\n') {
            return (Vec::new(), text.to_string());
        }
        if text.ends_with('\n') {
            let body = &text[..text.len() - 1];
            return (body.split('\n').map(|s| s.to_string()).collect(), String::new());
        }
        let mut parts: Vec<String> = text.split('\n').map(|s| s.to_string()).collect();
        let last = parts.pop().unwrap_or_default();
        (parts, last)
    }

    fn stable_line_end(lines: &[String]) -> usize {
        let mut index = 0usize;
        let mut stable = 0usize;
        while index < lines.len() {
            let line = lines[index].as_str();
            if line.trim().is_empty() {
                index += 1;
                stable = index;
                continue;
            }
            if let Some(fence) = FENCE_RE.captures(line) {
                let marker = fence.get(2).map(|m| m.as_str().to_string()).unwrap_or_default();
                match find_fence_close(lines, index, &marker) {
                    Some(closer) => {
                        index = closer + 1;
                        stable = index;
                    }
                    None => break,
                }
                continue;
            }
            if ATX_RE.is_match(line) {
                index += 1;
                stable = index;
                continue;
            }
            if is_thematic_break(line) {
                index += 1;
                stable = index;
                continue;
            }
            if let Some(table_end) = try_consume_table(lines, index) {
                if table_end == 0 {
                    break;
                }
                index = table_end;
                stable = index;
                continue;
            }
            if LIST_RE.is_match(line) {
                index = consume_tight_item(lines, index);
                stable = index;
                continue;
            }
            if QUOTE_RE.is_match(line) {
                index = consume_blockquote(lines, index);
                stable = index;
                continue;
            }
            if INDENT_RE.is_match(line) {
                index = consume_indented_code(lines, index);
                stable = index;
                continue;
            }
            match consume_paragraph(lines, index) {
                Some(para_end) => {
                    index = para_end;
                    stable = index;
                }
                None => break,
            }
        }
        stable
    }

    fn find_fence_close(lines: &[String], start: usize, marker: &str) -> Option<usize> {
        let marker_char = marker.chars().next()?;
        let min_len = marker.chars().count();
        let closer = Regex::new(&format!(
            r"^ {{0,3}}{}{{{min_len},}}\s*$",
            regex::escape(&marker_char.to_string())
        ))
        .ok()?;
        for index in (start + 1)..lines.len() {
            if closer.is_match(lines[index].as_str()) {
                return Some(index);
            }
        }
        None
    }

    fn is_thematic_break(line: &str) -> bool {
        if line.contains('|') {
            return false;
        }
        HR_RE.is_match(line)
    }

    fn is_table_row(line: &str) -> bool {
        let stripped = line.trim();
        if !stripped.contains('|') {
            return false;
        }
        if is_thematic_break(stripped) {
            return false;
        }
        stripped.matches('|').count() >= 1 && !ATX_RE.is_match(stripped)
    }

    fn is_table_separator(line: &str) -> bool {
        let stripped = line.trim();
        if !stripped.contains('|') {
            return false;
        }
        let body = stripped.trim_matches('|');
        let cells: Vec<&str> = body.split('|').map(|c| c.trim()).collect();
        if cells.is_empty() || cells.iter().any(|c| c.is_empty()) {
            return false;
        }
        cells.iter().all(|c| TABLE_SEP_CELL_RE.is_match(c))
    }

    /// Exclusive end index, 0 when a table started but is incomplete, None otherwise.
    fn try_consume_table(lines: &[String], start: usize) -> Option<usize> {
        if !is_table_row(&lines[start]) || is_table_separator(&lines[start]) {
            return None;
        }
        if start + 1 >= lines.len() {
            return Some(0);
        }
        if !is_table_separator(&lines[start + 1]) {
            return None;
        }
        let mut index = start + 2;
        if index >= lines.len() || !is_table_row(&lines[index]) || is_table_separator(&lines[index]) {
            return Some(0);
        }
        index += 1;
        while index < lines.len()
            && is_table_row(&lines[index])
            && !is_table_separator(&lines[index])
        {
            index += 1;
        }
        Some(index)
    }

    fn consume_tight_item(lines: &[String], start: usize) -> usize {
        let mut index = start + 1;
        while index < lines.len() {
            let nxt = lines[index].as_str();
            if nxt.trim().is_empty() {
                break;
            }
            if is_block_start(nxt) && !INDENT_RE.is_match(nxt) {
                break;
            }
            if LIST_RE.is_match(nxt) {
                break;
            }
            if nxt.starts_with(' ') || nxt.starts_with('\t') {
                index += 1;
                continue;
            }
            break;
        }
        index
    }

    fn consume_blockquote(lines: &[String], start: usize) -> usize {
        let mut index = start + 1;
        while index < lines.len() {
            let nxt = lines[index].as_str();
            if nxt.trim().is_empty() {
                break;
            }
            if QUOTE_RE.is_match(nxt) || nxt.starts_with("  ") || nxt.starts_with('\t') {
                index += 1;
                continue;
            }
            break;
        }
        index
    }

    fn consume_indented_code(lines: &[String], start: usize) -> usize {
        let mut index = start + 1;
        while index < lines.len() && (lines[index].trim().is_empty() || INDENT_RE.is_match(&lines[index])) {
            if lines[index].trim().is_empty()
                && index + 1 < lines.len()
                && !INDENT_RE.is_match(&lines[index + 1])
            {
                break;
            }
            index += 1;
        }
        index
    }

    fn is_block_start(line: &str) -> bool {
        if line.trim().is_empty() {
            return true;
        }
        FENCE_RE.is_match(line)
            || ATX_RE.is_match(line)
            || is_thematic_break(line)
            || LIST_RE.is_match(line)
            || QUOTE_RE.is_match(line)
    }

    fn consume_paragraph(lines: &[String], start: usize) -> Option<usize> {
        let mut index = start + 1;
        while index < lines.len() && !is_block_start(&lines[index]) {
            if looks_like_table_open(lines, index) {
                break;
            }
            index += 1;
        }
        if index >= lines.len() {
            return None;
        }
        if lines[index].trim().is_empty() {
            return Some(index + 1);
        }
        Some(index)
    }

    fn looks_like_table_open(lines: &[String], index: usize) -> bool {
        if !is_table_row(&lines[index]) {
            return false;
        }
        if index + 1 < lines.len() && is_table_separator(&lines[index + 1]) {
            return true;
        }
        index + 1 >= lines.len()
    }
}

// ===========================================================================
// Inlined submodule: login_qr.py
// ===========================================================================

pub mod login_qr {
    use std::collections::HashMap;
    use std::time::Duration;

    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Nonce};
    use base64::Engine;
    use rand::RngCore;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};

    use super::{jstr, value_str};

    const QQ_CONNECT_BASE: &str = "https://q.qq.com";
    const QQ_BIND_CREATE_PATH: &str = "/lite/create_bind_task";
    const QQ_BIND_POLL_PATH: &str = "/lite/poll_bind_result";
    const QQ_BIND_STATUS_COMPLETED: i64 = 2;
    const QQ_BIND_STATUS_EXPIRED: i64 = 3;

    /// QQ Bot application credentials returned after QR confirmation.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct QQBotQRCredentials {
        pub app_id: String,
        #[serde(skip_serializing)]
        pub app_secret: String,
        pub user_openid: Option<String>,
    }

    /// A QQ Bot QR binding task ready to be rendered by a client.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct QQBotQRCodeResponse {
        pub task_id: String,
        pub qrcode_url: String,
    }

    /// QQ Bot QR binding status.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum QQBotQRStatus {
        Pending,
        Success,
        Expired,
    }

    /// Result of one QQ Bot QR binding status poll.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct QQBotQRPollResult {
        pub status: QQBotQRStatus,
        pub credentials: Vec<QQBotQRCredentials>,
    }

    impl Default for QQBotQRStatus {
        fn default() -> Self {
            QQBotQRStatus::Pending
        }
    }

    /// Terminal result of waiting for a QQ Bot QR binding task.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(default)]
    pub struct QQBotQRWaitResult {
        pub connected: bool,
        pub credentials: Vec<QQBotQRCredentials>,
        pub message: String,
    }

    /// Two-phase QQ Bot QR binding flow.
    pub struct QQBotQRLogin {
        source: String,
        poll_interval: f64,
        bind_keys: HashMap<String, String>,
    }

    impl QQBotQRLogin {
        pub fn new(source: &str, poll_interval: f64) -> std::result::Result<Self, String> {
            if poll_interval <= 0.0 {
                return Err("poll_interval must be greater than zero".to_string());
            }
            Ok(QQBotQRLogin {
                source: source.to_string(),
                poll_interval,
                bind_keys: HashMap::new(),
            })
        }

        /// Create a QQ Bot binding task and return its QR target URL.
        pub async fn fetch_qr_code(
            &mut self,
        ) -> std::result::Result<QQBotQRCodeResponse, String> {
            let mut raw = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut raw);
            let key = base64::engine::general_purpose::STANDARD.encode(raw);

            let response = Self::post_json(QQ_BIND_CREATE_PATH, &json!({ "key": key })).await?;
            let data = Self::response_data(&response, "create bind task")?;
            let task_id = jstr(data.get("task_id"));
            if task_id.is_empty() {
                return Err("QQ Bot create bind task response missing task_id".to_string());
            }
            self.bind_keys.insert(task_id.clone(), key);

            let query = format!(
                "task_id={}&source={}&_wv=2",
                urlencoding::encode(&task_id),
                urlencoding::encode(&self.source)
            );
            Ok(QQBotQRCodeResponse {
                qrcode_url: format!("{QQ_CONNECT_BASE}/qqbot/openclaw/connect.html?{query}"),
                task_id,
            })
        }

        /// Poll one binding task without blocking between requests.
        pub async fn poll(
            &mut self,
            task_id: &str,
        ) -> std::result::Result<QQBotQRPollResult, String> {
            let key = match self.bind_keys.get(task_id).cloned() {
                Some(k) => k,
                None => return Err("Unknown or expired QQ Bot QR task".to_string()),
            };

            let response = Self::post_json(QQ_BIND_POLL_PATH, &json!({ "task_id": task_id })).await?;
            let data = Self::response_data(&response, "poll bind result")?;
            let status: i64 = jstr(data.get("status")).parse().unwrap_or(0);

            if status == QQ_BIND_STATUS_COMPLETED {
                let app_id = jstr(data.get("bot_appid"));
                let encrypted_secret = jstr(data.get("bot_encrypt_secret"));
                if app_id.is_empty() || encrypted_secret.is_empty() {
                    return Err("QQ Bot bind result missing application credentials".to_string());
                }
                let app_secret = Self::decrypt_secret(&encrypted_secret, &key)?;
                self.bind_keys.remove(task_id);
                let user_openid = jstr(data.get("user_openid"));
                return Ok(QQBotQRPollResult {
                    status: QQBotQRStatus::Success,
                    credentials: vec![QQBotQRCredentials {
                        app_id,
                        app_secret,
                        user_openid: if user_openid.is_empty() {
                            None
                        } else {
                            Some(user_openid)
                        },
                    }],
                });
            }

            if status == QQ_BIND_STATUS_EXPIRED {
                self.bind_keys.remove(task_id);
                return Ok(QQBotQRPollResult {
                    status: QQBotQRStatus::Expired,
                    credentials: vec![],
                });
            }

            Ok(QQBotQRPollResult {
                status: QQBotQRStatus::Pending,
                credentials: vec![],
            })
        }

        /// Wait until a QR task succeeds, expires, or reaches the timeout.
        pub async fn wait_for_login(
            &mut self,
            task_id: &str,
            timeout_s: f64,
        ) -> std::result::Result<QQBotQRWaitResult, String> {
            let deadline = crate::channel::now_f64() + timeout_s;
            while crate::channel::now_f64() < deadline {
                match self.poll(task_id).await {
                    Ok(result) => {
                        if result.status == QQBotQRStatus::Success {
                            return Ok(QQBotQRWaitResult {
                                connected: true,
                                credentials: result.credentials,
                                message: "QR code confirmed".to_string(),
                            });
                        }
                        if result.status == QQBotQRStatus::Expired {
                            return Ok(QQBotQRWaitResult {
                                connected: false,
                                credentials: vec![],
                                message: "QR code expired".to_string(),
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!("QQ Bot QR poll failed; retrying: {e}");
                    }
                }
                let remaining = (deadline - crate::channel::now_f64()).max(0.0);
                tokio::time::sleep(Duration::from_secs_f64(self.poll_interval.min(remaining))).await;
            }

            self.cancel(task_id);
            Ok(QQBotQRWaitResult {
                connected: false,
                credentials: vec![],
                message: format!("QR login timeout after {} seconds", timeout_s),
            })
        }

        /// Forget a pending QR task and its one-time decryption key.
        pub fn cancel(&mut self, task_id: &str) {
            self.bind_keys.remove(task_id);
        }

        async fn post_json(path: &str, payload: &Value) -> std::result::Result<Value, String> {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs_f64(10.0))
                .build()
                .map_err(|e| e.to_string())?;
            let url = format!("{QQ_CONNECT_BASE}{path}");
            let response = client
                .post(&url)
                .json(payload)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if !response.status().is_success() {
                return Err(format!("QQ Bot QR endpoint HTTP {}", response.status()));
            }
            let body: Value = response.json().await.map_err(|e| e.to_string())?;
            if !body.is_object() {
                return Err("QQ Bot QR endpoint returned a malformed response".to_string());
            }
            Ok(body)
        }

        fn response_data(response: &Value, action: &str) -> std::result::Result<Value, String> {
            let retcode: i64 = jstr(response.get("retcode")).parse().unwrap_or(0);
            if retcode != 0 {
                let msg = {
                    let m = jstr(response.get("msg"));
                    if m.is_empty() {
                        "unknown error".to_string()
                    } else {
                        m
                    }
                };
                return Err(format!("QQ Bot {action} failed: {msg}"));
            }
            let data = response
                .get("data")
                .ok_or_else(|| format!("QQ Bot {action} response missing data"))?;
            if !data.is_object() {
                return Err(format!("QQ Bot {action} response missing data"));
            }
            Ok(data.clone())
        }

        fn decrypt_secret(encrypted_secret: &str, key: &str) -> std::result::Result<String, String> {
            let encrypted = base64::engine::general_purpose::STANDARD
                .decode(encrypted_secret)
                .map_err(|e| e.to_string())?;
            let raw_key = base64::engine::general_purpose::STANDARD
                .decode(key)
                .map_err(|e| e.to_string())?;
            if encrypted.len() < 29 || raw_key.len() != 32 {
                return Err("QQ Bot bind result contains invalid encrypted credentials".to_string());
            }
            let nonce = &encrypted[..12];
            let ciphertext_and_tag = &encrypted[12..];
            let cipher = Aes256Gcm::new_from_slice(&raw_key).map_err(|e| format!("{e:?}"))?;
            let plain = cipher
                .decrypt(Nonce::from_slice(nonce), ciphertext_and_tag)
                .map_err(|_| "QQ Bot secret decryption failed".to_string())?;
            String::from_utf8(plain).map_err(|e| e.to_string())
        }
    }

    // Silence an unused-import warning when this helper is only used indirectly.
    #[allow(dead_code)]
    fn _touch(v: &Value) -> String {
        value_str(v)
    }
}
