//! Discord channel.
//!
//! Mirrors `octop_gateway.channels.discord`. The Python SDK (`discord.py`) is
//! replaced by a direct implementation of:
//!   - the Discord Gateway WebSocket protocol (`GET …/gateway/bot`, IDENTIFY,
//!     heartbeat, RESUME, reconnects) driven by `tokio-tungstenite`, and
//!   - the Discord REST API (`discord.com/api/v10`) driven by `reqwest`
//!     (outbound text, multipart attachments, typing indicator, media fetch).
//!
//! Behavior is kept 1:1 with the Python channel: the access whitelist, the
//! bot/webhook/message-type gate, message-id deduplication, mention handling,
//! attachment mapping, the group-context mention gate, `split_discord_text`
//! (UTF-16 aware, code-fence balanced), proxy-aware media download with a
//! 25 MiB cap, and the connection-session bookkeeping.
//!
//! See the module-level NOTE in the final report about `ChannelCore::is_duplicate`
//! (it requires `&mut self`, which a detached Gateway task cannot hold, so
//! dedup uses the channel-local `DedupCache` in `fields` — the same 1000-entry
//! LRU semantics as the Python `_seen` OrderedDict).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::channel::{Channel, ChannelConfig, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::group_context::GroupContextConfig;
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, VideoContent,
};

/// Discord REST + Gateway base.
const DISCORD_API: &str = "https://discord.com/api/v10";
/// Gateway transport (mirrors Python `discord.Client.start`) connect timeout.
const CONNECT_TIMEOUT: f64 = 30.0;
/// Hard cap for downloaded attachments (Python `_MAX_DOWNLOAD_BYTES`).
const MAX_DOWNLOAD_BYTES: u64 = 25 * 1024 * 1024;
/// Discord per-message text budget (UTF-16 code units).
const DISCORD_TEXT_LIMIT: usize = 2000;
/// Python `_seen` OrderedDict capacity.
const DEDUP_MAX_SIZE: usize = 1000;

/// Gateway intents bitmask: GUILDS(1) | GUILD_MESSAGES(512) | DIRECT_MESSAGES(4096)
/// | MESSAGE_CONTENT(32768). Python sets `guilds`, `guild_messages`,
/// `dm_messages`, `message_content` on an otherwise-empty `Intents`.
const DISCORD_INTENTS: i64 = 1 | 512 | 4096 | 32768;

/// Discord message type DEFAULT — the only non-reply type accepted.
const MESSAGE_TYPE_DEFAULT: i64 = 0;
/// Discord message type REPLY.
const MESSAGE_TYPE_REPLY: i64 = 19;

/// Discord channel configuration.
///
/// Mirrors the Python `DiscordConfig` dataclass. `allow_all_channels` defaults to
/// `true` (guild channels are allowed by default); DM senders always require an
/// entry in `allowed_user_ids`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DiscordConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Bot token used for both the Gateway IDENTIFY and REST `Bot` auth.
    pub bot_token: String,
    /// Optional HTTP(S) proxy for REST calls (Gateway WS does not use it — see NOTE).
    pub http_proxy: String,
    /// Optional proxy credentials as `user:password` (password may contain `:`).
    pub http_proxy_auth: String,
    /// Allow every guild channel/thread (subject to Discord permissions).
    pub allow_all_channels: bool,
    /// DM senders allowed to reach the bot.
    #[serde(default)]
    pub allowed_user_ids: Vec<String>,
    /// Guild channels/threads allowed when `allow_all_channels` is false.
    #[serde(default)]
    pub allowed_channel_ids: Vec<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        DiscordConfig {
            base: ChannelConfig {
                // Python default: enabled=True, visibility="all", activation="mention".
                group_context: Some(GroupContextConfig {
                    enabled: true,
                    visibility: "all".to_string(),
                    activation: "mention".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            bot_token: String::new(),
            http_proxy: String::new(),
            http_proxy_auth: String::new(),
            allow_all_channels: true,
            allowed_user_ids: Vec::new(),
            allowed_channel_ids: Vec::new(),
        }
    }
}

crate::channel_adapter! {
    /// Discord channel.
    DiscordChannel, DiscordConfig, "discord",
    required = [bot_token],
    fields = {
        task: Option<JoinHandle<()>>,
        ready: Arc<AtomicBool>,
        session: Arc<StdMutex<Option<String>>>,
        runtime_error: Arc<StdMutex<Option<String>>>,
        seen: Arc<StdMutex<DedupCache>>,
        bot_id: Arc<StdMutex<Option<String>>>,
    }
}

// ---------------------------------------------------------------------------
// Dedup cache (Python `DiscordChannel._seen`, an OrderedDict capped at 1000)
// ---------------------------------------------------------------------------

/// Bounded LRU-ish set of message ids.
///
/// NOTE: the shared `ChannelCore::is_duplicate` needs `&mut self`, and the
/// Gateway dispatch runs in a detached `tokio` task that only holds `&self`.
/// The Python channel likewise keeps its own `_seen` cache, so this preserves
/// the exact Python semantics (capacity [`DEDUP_MAX_SIZE`]).
#[derive(Default)]
struct DedupCache {
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl DedupCache {
    fn is_duplicate(&mut self, id: &str) -> bool {
        if self.seen.contains(id) {
            // Python moves the id to the end on re-seen; only the membership
            // check affects behavior.
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

// ---------------------------------------------------------------------------
// Gateway transport: shared context, outcome, and the connect/reconnect loop
// ---------------------------------------------------------------------------

/// Everything the detached Gateway task needs.
struct GatewayCtx {
    config: DiscordConfig,
    http: reqwest::Client,
    cb: Option<EnqueueFn>,
    group_context: GroupContextConfig,
    ready: Arc<AtomicBool>,
    session: Arc<StdMutex<Option<String>>>,
    runtime_error: Arc<StdMutex<Option<String>>>,
    seen: Arc<StdMutex<DedupCache>>,
    bot_id: Arc<StdMutex<Option<String>>>,
}

impl GatewayCtx {
    fn set_error(&self, code: &str) {
        *self.runtime_error.lock().unwrap() = Some(code.to_string());
    }

    /// Mirror Python `on_ready` / `on_resumed`.
    fn mark_connected(&self) {
        *self.session.lock().unwrap() =
            Some(format!("discord-{}", uuid::Uuid::new_v4().simple()));
        *self.runtime_error.lock().unwrap() = None;
        self.ready.store(true, Ordering::SeqCst);
    }
}

/// Why a single Gateway session ended.
enum Outcome {
    /// Reconnect (transient error, op7, or a zombie heartbeat).
    Reconnect,
    /// op9 invalid session; the bool says whether RESUME is permitted.
    InvalidSession(bool),
    /// Fatal, non-retryable (invalid token / disallowed intents).
    Fatal(String),
}

/// Resolve the Gateway URL via `GET /gateway/bot`.
async fn resolve_gateway_url(http: &reqwest::Client, token: &str) -> std::result::Result<String, String> {
    let resp = http
        .get(format!("{DISCORD_API}/gateway/bot"))
        .header("Authorization", format!("Bot {token}"))
        .send()
        .await
        .map_err(|_| "discord_connection_failed".to_string())?;
    if resp.status().as_u16() == 401 {
        return Err("discord_invalid_token".to_string());
    }
    if !resp.status().is_success() {
        return Err("discord_connection_failed".to_string());
    }
    let v: Value = resp
        .json()
        .await
        .map_err(|_| "discord_connection_failed".to_string())?;
    v.get("url")
        .and_then(|u| u.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| "discord_connection_failed".to_string())
}

/// Connect-and-run loop with exponential backoff, mirroring Python
/// `DiscordChannel._run_client` + `start()` readiness handshake.
async fn run_gateway(ctx: GatewayCtx, handshake: oneshot::Sender<std::result::Result<(), String>>) {
    let mut handshake = Some(handshake);
    let mut session_id: Option<String> = None;
    let mut last_seq: Option<i64> = None;
    let mut backoff = 1.0f64;

    loop {
        // 1. Resolve the gateway endpoint.
        let url = match resolve_gateway_url(&ctx.http, &ctx.config.bot_token).await {
            Ok(u) => u,
            Err(code) => {
                // Before the first READY this is a fatal probe error; after a
                // successful connection it is retried (like discord.py reconnect).
                if handshake.is_some() {
                    ctx.set_error(&code);
                    if let Some(tx) = handshake.take() {
                        let _ = tx.send(Err(code));
                    }
                    return;
                }
                tracing::warn!("Discord gateway resolve failed: {code}");
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(30.0);
                continue;
            }
        };

        let sep = if url.contains('?') {
            if url.ends_with('?') || url.ends_with('&') {
                ""
            } else {
                "&"
            }
        } else {
            "/?"
        };
        let ws_url = format!("{url}{sep}v=10&encoding=json");

        // 2. Connect (honoring `http_proxy`, mirroring discord.py's gateway).
        let connect_result: std::result::Result<_, String> = if ctx.config.http_proxy.is_empty() {
            tokio_tungstenite::connect_async(&ws_url)
                .await
                .map(|(ws, _)| ws)
                .map_err(|e| e.to_string())
        } else {
            let auth = if ctx.config.http_proxy_auth.is_empty() {
                None
            } else {
                Some(ctx.config.http_proxy_auth.as_str())
            };
            crate::utils::ws_connect_proxied(&ws_url, &ctx.config.http_proxy, auth).await
        };
        let ws = match connect_result {
            Ok(ws) => ws,
            Err(e) => {
                if handshake.is_some() {
                    ctx.set_error("discord_connection_failed");
                    if let Some(tx) = handshake.take() {
                        let _ = tx.send(Err("discord_connection_failed".to_string()));
                    }
                    return;
                }
                tracing::warn!("Discord gateway connect failed: {e}");
                tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                backoff = (backoff * 2.0).min(30.0);
                continue;
            }
        };

        // 3. Run one Gateway session.
        match run_session(&ctx, ws, &mut session_id, &mut last_seq, &mut handshake).await {
            Outcome::Fatal(code) => {
                ctx.set_error(&code);
                if let Some(tx) = handshake.take() {
                    let _ = tx.send(Err(code));
                }
                return;
            }
            Outcome::InvalidSession(resumable) => {
                if !resumable {
                    session_id = None;
                    last_seq = None;
                }
            }
            Outcome::Reconnect => {}
        }

        // Disconnected: mirror Python `on_disconnect`.
        let was_ready = ctx.ready.swap(false, Ordering::SeqCst);
        *ctx.session.lock().unwrap() = None;
        if was_ready {
            backoff = 1.0;
        }
        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
        backoff = (backoff * 2.0).min(30.0);
    }
}

/// Drive a single Gateway session: Hello → IDENTIFY/RESUME → dispatch loop.
async fn run_session(
    ctx: &GatewayCtx,
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    session_id: &mut Option<String>,
    last_seq: &mut Option<i64>,
    handshake: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
) -> Outcome {
    let (mut write, mut read) = ws.split();

    // --- Hello (op10) ------------------------------------------------------
    let mut hb_interval = Duration::from_secs_f64(41.25);
    loop {
        match read.next().await {
            Some(Ok(Message::Text(t))) => {
                if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                    if v.get("op").and_then(|x| x.as_i64()) == Some(10) {
                        if let Some(iv) = v.pointer("/d/heartbeat_interval").and_then(|x| x.as_u64()) {
                            hb_interval = Duration::from_millis(iv.max(1));
                        }
                        break;
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
            Some(Ok(Message::Close(frame))) => {
                if let Some(f) = frame {
                    let code = u16::from(f.code);
                    if code == 4004 {
                        return Outcome::Fatal("discord_invalid_token".to_string());
                    }
                    if code == 4014 {
                        return Outcome::Fatal("discord_intents_required".to_string());
                    }
                }
                return Outcome::Reconnect;
            }
            Some(Err(_)) | None => return Outcome::Reconnect,
        }
    }

    // --- IDENTIFY (op2) or RESUME (op6) ------------------------------------
    let identify = if let (Some(sid), Some(seq)) = (session_id.as_ref(), last_seq.as_ref()) {
        json!({
            "op": 6,
            "d": { "token": ctx.config.bot_token, "session_id": sid, "seq": seq }
        })
    } else {
        json!({
            "op": 2,
            "d": {
                "token": ctx.config.bot_token,
                "intents": DISCORD_INTENTS,
                "properties": {
                    "os": std::env::consts::OS,
                    "browser": "buddy-gateway",
                    "device": "buddy-gateway",
                }
            }
        })
    };
    if write
        .send(Message::Text(identify.to_string().into()))
        .await
        .is_err()
    {
        return Outcome::Reconnect;
    }

    // --- Heartbeat + dispatch loop ----------------------------------------
    let mut hb = tokio::time::interval(hb_interval);
    hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick so the first beat lands one interval out.
    hb.tick().await;
    let mut acked = true;

    loop {
        tokio::select! {
            _ = hb.tick() => {
                // A previous heartbeat that was never acked means a zombie link.
                if !acked {
                    tracing::warn!("Discord heartbeat not acknowledged; reconnecting");
                    return Outcome::Reconnect;
                }
                acked = false;
                let payload = json!({ "op": 1, "d": last_seq });
                if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                    return Outcome::Reconnect;
                }
            }
            incoming = read.next() => {
                let msg = match incoming {
                    Some(Ok(m)) => m,
                    Some(Err(_)) => return Outcome::Reconnect,
                    None => return Outcome::Reconnect,
                };
                let txt = match msg {
                    Message::Text(t) => t,
                    Message::Binary(_) | Message::Frame(_) => continue,
                    Message::Ping(p) => {
                        let _ = write.send(Message::Pong(p)).await;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(frame) => {
                        if let Some(f) = frame {
                            let code = u16::from(f.code);
                            if code == 4004 {
                                return Outcome::Fatal("discord_invalid_token".to_string());
                            }
                            if code == 4014 {
                                return Outcome::Fatal("discord_intents_required".to_string());
                            }
                        }
                        return Outcome::Reconnect;
                    }
                };
                let v: Value = match serde_json::from_str(txt.as_str()) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let op = v.get("op").and_then(|x| x.as_i64()).unwrap_or(-1);
                match op {
                    // Late Hello (unusual within a session) — ignored.
                    10 => {}
                    // Heartbeat ACK.
                    11 => acked = true,
                    // Server-requested heartbeat.
                    1 => {
                        let payload = json!({ "op": 1, "d": last_seq });
                        if write.send(Message::Text(payload.to_string().into())).await.is_err() {
                            return Outcome::Reconnect;
                        }
                    }
                    // RECONNECT.
                    7 => return Outcome::Reconnect,
                    // INVALID_SESSION (d = resumable).
                    9 => {
                        let resumable = v.get("d").and_then(|x| x.as_bool()).unwrap_or(false);
                        return Outcome::InvalidSession(resumable);
                    }
                    // DISPATCH.
                    0 => {
                        if let Some(s) = v.get("s").and_then(|x| x.as_i64()) {
                            *last_seq = Some(s);
                        }
                        let t = v.get("t").and_then(|x| x.as_str()).unwrap_or("");
                        let d = v.get("d").cloned().unwrap_or(Value::Null);
                        match t {
                            "READY" => {
                                if let Some(sid) = d.get("session_id").and_then(|x| x.as_str()) {
                                    if !sid.is_empty() {
                                        *session_id = Some(sid.to_string());
                                    }
                                }
                                if let Some(uid) = d.pointer("/user/id") {
                                    let uid = uid
                                        .as_str()
                                        .map(|s| s.to_string())
                                        .or_else(|| uid.as_u64().map(|n| n.to_string()));
                                    if let Some(uid) = uid {
                                        *ctx.bot_id.lock().unwrap() = Some(uid);
                                    }
                                }
                                tracing::info!("Discord gateway READY");
                                ctx.mark_connected();
                                if let Some(tx) = handshake.take() {
                                    let _ = tx.send(Ok(()));
                                }
                            }
                            "RESUMED" => {
                                tracing::info!("Discord gateway RESUMED");
                                ctx.mark_connected();
                            }
                            // Mirrors Python `DiscordChannel.on_message`.
                            "MESSAGE_CREATE" => handle_dispatch(ctx, &d),
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Access control / inbound parsing (mirrors Python `on_message` + `parse_inbound`)
// ---------------------------------------------------------------------------

/// JSON id → `String`, tolerating numeric ids.
fn id_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Unwrap a full Gateway frame (`{op,t,d}`) down to its `d`, else identity.
fn event_data(v: &Value) -> &Value {
    if v.get("op").is_some() && v.get("t").is_some() {
        v.get("d").unwrap_or(v)
    } else {
        v
    }
}

fn is_guild_msg(d: &Value) -> bool {
    d.get("guild_id").map(|g| !g.is_null()).unwrap_or(false)
        || d.get("guild").map(|g| !g.is_null()).unwrap_or(false)
}

fn channel_id_of(d: &Value) -> String {
    d.get("channel_id")
        .and_then(|x| x.as_str())
        .or_else(|| d.pointer("/channel/id").and_then(|x| x.as_str()))
        .unwrap_or("")
        .to_string()
}

fn parent_id_of(d: &Value) -> Option<String> {
    d.get("parent_id")
        .and_then(|x| x.as_str())
        .or_else(|| d.pointer("/channel/parent_id").and_then(|x| x.as_str()))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn channel_name_of(d: &Value) -> String {
    d.pointer("/channel/name")
        .and_then(|x| x.as_str())
        .or_else(|| d.get("channel_name").and_then(|x| x.as_str()))
        .unwrap_or("")
        .to_string()
}

fn author_id_of(d: &Value) -> String {
    d.pointer("/author/id")
        .map(id_str)
        .or_else(|| d.get("author_id").map(id_str))
        .unwrap_or_default()
}

fn author_display_name_of(d: &Value) -> String {
    d.pointer("/author/display_name")
        .and_then(|x| x.as_str())
        .or_else(|| d.pointer("/author/global_name").and_then(|x| x.as_str()))
        .or_else(|| d.pointer("/author/username").and_then(|x| x.as_str()))
        .unwrap_or("")
        .to_string()
}

fn message_content_of(d: &Value) -> String {
    d.get("content")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn has_attachments(d: &Value) -> bool {
    d.get("attachments")
        .and_then(|x| x.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false)
}

/// True when the bot's mention token appears in `text` (Python
/// `re.search(rf"<@!?{bot.id}>", text)`).
fn bot_mentioned(bot_id: Option<&str>, text: &str) -> bool {
    match bot_id {
        Some(id) if !id.is_empty() => Regex::new(&format!(r"<@!?{}>", regex::escape(id)))
            .map(|re| re.is_match(text))
            .unwrap_or(false),
        _ => false,
    }
}

/// Strip bot mentions and trim (Python `re.sub(...).strip()`); no-op without a bot id.
fn strip_bot_mention(bot_id: Option<&str>, text: &str) -> String {
    match bot_id {
        Some(id) if !id.is_empty() => Regex::new(&format!(r"<@!?{}>", regex::escape(id)))
            .map(|re| re.replace_all(text, "").trim().to_string())
            .unwrap_or_else(|_| text.to_string()),
        _ => text.to_string(),
    }
}

/// The bot/webhook/type gate + access whitelist (Python `on_message` head).
fn gate_allows(config: &DiscordConfig, d: &Value) -> bool {
    // Bot-authored messages are ignored.
    if d.pointer("/author/bot")
        .and_then(|x| x.as_bool())
        .unwrap_or(false)
    {
        return false;
    }
    // Webhook messages are ignored.
    if let Some(w) = d.get("webhook_id") {
        if !w.is_null() {
            return false;
        }
    }
    // Only DEFAULT and REPLY message types are accepted.
    let mtype = d.get("type").and_then(|x| x.as_i64()).unwrap_or(MESSAGE_TYPE_DEFAULT);
    if mtype != MESSAGE_TYPE_DEFAULT && mtype != MESSAGE_TYPE_REPLY {
        return false;
    }

    if !is_guild_msg(d) {
        // DMs always require an allowed user id.
        let uid = author_id_of(d);
        config.allowed_user_ids.iter().any(|x| x == &uid)
    } else {
        if config.allow_all_channels {
            return true;
        }
        let cid = channel_id_of(d);
        if config.allowed_channel_ids.iter().any(|x| x == &cid) {
            return true;
        }
        // Threads inherit access from their parent channel.
        if let Some(pid) = parent_id_of(d) {
            if config.allowed_channel_ids.iter().any(|x| x == &pid) {
                return true;
            }
        }
        false
    }
}

/// The full `on_message` pipeline: gate → dedup → emptiness → mention gate → enqueue.
fn handle_dispatch(ctx: &GatewayCtx, d: &Value) {
    if !gate_allows(&ctx.config, d) {
        return;
    }

    let mid = id_str(d.get("id").unwrap_or(&Value::Null));
    if !mid.is_empty() {
        let mut seen = ctx.seen.lock().unwrap();
        if seen.is_duplicate(&mid) {
            return;
        }
    }

    let content = message_content_of(d);
    if content.is_empty() && !has_attachments(d) {
        return;
    }

    // When shared group context is disabled, keep the default mention gate.
    if is_guild_msg(d) {
        let cid = channel_id_of(d);
        if !ctx.group_context.resolve(&cid).enabled {
            let bot = ctx.bot_id.lock().unwrap().clone();
            if !bot_mentioned(bot.as_deref(), &content) {
                return;
            }
        }
    }

    if let Some(cb) = &ctx.cb {
        cb(RawPayload::Json(d.clone()));
    }
}

/// Map a Discord attachment to a typed content part.
fn attachment_part(att: &Value) -> Option<ContentPart> {
    let url = att.get("url").and_then(|x| x.as_str()).unwrap_or("");
    if url.is_empty() {
        return None;
    }
    let filename = att.get("filename").and_then(|x| x.as_str()).unwrap_or("");
    let size = att.get("size").and_then(|x| x.as_u64());
    // Python: attachment.content_type or mimetypes.guess_type(filename)[0] or octet-stream.
    let mime = att
        .get("content_type")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            mime_guess::from_path(filename)
                .first()
                .map(|m| m.essence_str().to_string())
        })
        .unwrap_or_else(|| "application/octet-stream".to_string());

    if mime.starts_with("image/") {
        Some(ContentPart::Image(ImageContent {
            url: url.to_string(),
            alt_text: filename.to_string(),
            mime_type: Some(mime),
            size,
            ..Default::default()
        }))
    } else if mime.starts_with("audio/") {
        Some(ContentPart::Audio(AudioContent {
            url: url.to_string(),
            mime_type: Some(mime),
            size,
            ..Default::default()
        }))
    } else if mime.starts_with("video/") {
        Some(ContentPart::Video(VideoContent {
            url: url.to_string(),
            mime_type: Some(mime),
            size,
            ..Default::default()
        }))
    } else {
        Some(ContentPart::File(FileContent {
            url: url.to_string(),
            filename: filename.to_string(),
            mime_type: Some(mime),
            size,
            ..Default::default()
        }))
    }
}

/// Discord snowflake → unix seconds (equals `message.created_at.timestamp()`).
fn snowflake_timestamp(id: &str) -> Option<f64> {
    let n: u64 = id.parse().ok()?;
    Some(((n >> 22) + 1_420_070_400_000) as f64 / 1000.0)
}

impl DiscordChannel {
    /// True while the Gateway task is alive and the session is READY.
    fn is_connected(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
            && self
                .task
                .as_ref()
                .map(|t| !t.is_finished())
                .unwrap_or(false)
    }

    /// Python `runtime_error`: `None` when connected, else the stored code.
    fn runtime_error_code(&self) -> String {
        if self.is_connected() {
            String::new()
        } else {
            self.runtime_error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "discord_disconnected".to_string())
        }
    }

    /// Build a REST client honoring `http_proxy` / `http_proxy_auth`.
    ///
    /// The Gateway WebSocket honors the same proxy via
    /// [`crate::utils::ws_connect_proxied`] (an HTTP `CONNECT` tunnel), so the
    /// proxy applies to both REST and the gateway, matching discord.py.
    fn build_http(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
        if !self.config.http_proxy.is_empty() {
            let mut proxy = reqwest::Proxy::all(&self.config.http_proxy)
                .map_err(|e| GatewayError::Other(format!("discord_proxy_invalid: {e}")))?;
            if !self.config.http_proxy_auth.is_empty() {
                let (user, password) = self
                    .config
                    .http_proxy_auth
                    .split_once(':')
                    .ok_or_else(|| GatewayError::Other("discord_proxy_auth_invalid".to_string()))?;
                proxy = proxy.basic_auth(user, password);
            }
            builder = builder.proxy(proxy);
        }
        builder.build().map_err(GatewayError::Http)
    }

    /// Resolve the outbound channel id from a subject (Python `_target`).
    async fn resolve_target(&self, subject: &ChannelSubject) -> Result<String> {
        if !self.is_connected() {
            return Err(GatewayError::Other(self.runtime_error_code()));
        }
        let chat_id = subject
            .metadata
            .get("chat_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                subject
                    .metadata
                    .get("channel_id_native")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
            });
        if chat_id.is_none() && subject.chat_type == "dm" {
            return self.create_dm(&subject.subject_id).await;
        }
        Ok(chat_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone()))
    }

    /// Open (or reuse) a DM channel with a user (Python `user.create_dm()`).
    async fn create_dm(&self, user_id: &str) -> Result<String> {
        let http = self.build_http()?;
        let url = format!("{DISCORD_API}/users/@me/channels");
        let resp = http
            .post(&url)
            .header("Authorization", format!("Bot {}", self.config.bot_token))
            .json(&json!({ "recipient_id": user_id }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!("discord dm open failed: {body}")));
        }
        let v: Value = resp.json().await?;
        Ok(v.get("id")
            .and_then(|x| x.as_str())
            .unwrap_or(user_id)
            .to_string())
    }

    /// POST one text chunk, blocking all mentions (Python `AllowedMentions.none()`).
    async fn post_message(&self, channel_id: &str, content: &str) -> Result<()> {
        let http = self.build_http()?;
        let url = format!("{DISCORD_API}/channels/{channel_id}/messages");
        let body = json!({ "content": content, "allowed_mentions": { "parse": [] } });
        let resp = http
            .post(&url)
            .header("Authorization", format!("Bot {}", self.config.bot_token))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!("discord send failed: {body}")));
        }
        Ok(())
    }

    /// Upload attachment bytes to a channel (Python `target.send(file=...)`).
    async fn try_send_media(&self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let (data, mime) = self.load_media_bytes(media).await?;

        // Python: getattr(media, "filename", "") or getattr(media, "alt_text", "").
        let mut filename = match media {
            ContentPart::File(f) => f.filename.clone(),
            ContentPart::Image(i) => i.alt_text.clone(),
            _ => String::new(),
        };
        filename = filename.replace('\\', "/");
        filename = filename.rsplit('/').next().unwrap_or("").to_string();
        if filename.is_empty() {
            let ext = guess_extension(&mime).unwrap_or(".bin");
            filename = format!("attachment{ext}");
        }

        let target = self.resolve_target(subject).await?;
        let http = self.build_http()?;
        let url = format!("{DISCORD_API}/channels/{target}/messages");

        let clean_mime = mime.split(';').next().unwrap_or("").trim().to_string();
        let clean_mime = if clean_mime.contains('/') {
            clean_mime
        } else {
            "application/octet-stream".to_string()
        };
        let part = reqwest::multipart::Part::bytes(data)
            .file_name(filename.clone())
            .mime_str(&clean_mime)?;
        let payload = json!({
            "allowed_mentions": { "parse": [] },
            "attachments": [{ "id": "0", "filename": filename }],
        });
        let form = reqwest::multipart::Form::new()
            .text("payload_json", payload.to_string())
            .part("files[0]", part);
        let resp = http
            .post(&url)
            .header("Authorization", format!("Bot {}", self.config.bot_token))
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!("discord upload failed: {body}")));
        }
        Ok(())
    }

    /// Best-effort typing trigger (Python `target.typing()`).
    async fn try_typing(&self, subject: &ChannelSubject) -> Result<()> {
        let target = self.resolve_target(subject).await?;
        let http = self.build_http()?;
        let url = format!("{DISCORD_API}/channels/{target}/typing");
        let resp = http
            .post(&url)
            .header("Authorization", format!("Bot {}", self.config.bot_token))
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other("discord typing failed".to_string()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Text splitting (exact port of Python `split_discord_text`)
// ---------------------------------------------------------------------------

/// UTF-16 code-unit length (Python `len(value.encode("utf-16-le")) // 2`).
fn size_u16(value: &str) -> usize {
    value.encode_utf16().count()
}

/// Python `str.rstrip("\r\n")`.
fn rstrip_crlf(value: &str) -> &str {
    value.trim_end_matches(['\r', '\n'])
}

/// Python `str.splitlines(keepends=True)`.
fn split_lines_keepends(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        current.push(c);
        let is_break = matches!(
            c,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if is_break {
            // `\r\n` is a single boundary and kept intact.
            if c == '\r' && chars.peek() == Some(&'\n') {
                current.push(chars.next().unwrap());
            }
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Split text on lines, closing/reopening code fences, budgeting UTF-16 units.
///
/// Direct port of Python `split_discord_text`.
fn split_discord_text(text: &str, limit: usize) -> Result<Vec<String>> {
    if limit < 32 {
        return Err(GatewayError::Other(
            "Discord chunk limit must be at least 32".to_string(),
        ));
    }
    if size_u16(text) <= limit {
        return Ok(if text.is_empty() {
            Vec::new()
        } else {
            vec![text.to_string()]
        });
    }

    let fence_re = Regex::new(r"^(`{3,}|~{3,})([^\r\n]*)")
        .map_err(|e| GatewayError::Other(format!("discord fence regex: {e}")))?;
    let closing = |value: &str| -> String {
        if value.is_empty() {
            String::new()
        } else {
            format!("\n{value}")
        }
    };

    let mut chunks: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut opener = String::new();
    let mut marker = String::new();

    for line in split_lines_keepends(text) {
        let mut line = line;
        let mut next_opener = opener.clone();
        let mut next_marker = marker.clone();

        if let Some(caps) = fence_re.captures(&line) {
            let candidate = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let info = caps.get(2).map(|m| m.as_str()).unwrap_or("");
            let cand_first = candidate.chars().next().unwrap_or('\0');
            let mark_first = marker.chars().next().unwrap_or('\0');
            if !marker.is_empty()
                && cand_first == mark_first
                && candidate.len() >= marker.len()
                && info.trim().is_empty()
            {
                next_opener = String::new();
                next_marker = String::new();
            } else if marker.is_empty() && (cand_first != '`' || !info.contains('`')) {
                // Bound only the *recognition* of a fence, never truncate user text.
                let header = format!("{}\n", rstrip_crlf(&line));
                if size_u16(&header) + size_u16(&closing(candidate)) < limit / 2 {
                    next_opener = header;
                    next_marker = candidate.to_string();
                }
            }
        }

        while !line.is_empty() {
            let room = limit as i64 - size_u16(&current) as i64 - size_u16(&closing(&next_marker)) as i64;
            if size_u16(&line) as i64 <= room {
                current.push_str(&line);
                break;
            }
            if !current.is_empty() && current != opener {
                chunks.push(format!("{}{}", current, closing(&marker)));
                current = opener.clone();
                continue;
            }
            // A long ordinary line is split without losing whitespace or emoji.
            let room = limit as i64 - size_u16(&current) as i64 - size_u16(&closing(&marker)) as i64;
            let mut used: i64 = 0;
            let mut cut_byte = 0usize;
            let mut progressed = false;
            for (i, ch) in line.char_indices() {
                let units = ch.len_utf16() as i64;
                if used + units > room {
                    break;
                }
                used += units;
                cut_byte = i + ch.len_utf8();
                progressed = true;
            }
            if !progressed {
                // Safety valve: always take one code point to guarantee progress.
                cut_byte = line.chars().next().map(|c| c.len_utf8()).unwrap_or(0);
            }
            let taken: String = line[..cut_byte].to_string();
            current.push_str(&taken);
            line = line[cut_byte..].to_string();
            chunks.push(format!("{}{}", current, closing(&marker)));
            current = opener.clone();
        }

        opener = next_opener;
        marker = next_marker;
    }

    if !current.is_empty() {
        chunks.push(format!("{}{}", current, closing(&marker)));
    }
    Ok(chunks)
}

/// Minimal `mimetypes.guess_extension` for attachment filenames.
fn guess_extension(mime: &str) -> Option<&'static str> {
    match mime.split(';').next().unwrap_or("").trim() {
        "image/png" => Some(".png"),
        "image/jpeg" => Some(".jpg"),
        "image/gif" => Some(".gif"),
        "image/webp" => Some(".webp"),
        "image/bmp" => Some(".bmp"),
        "audio/mpeg" => Some(".mp3"),
        "audio/ogg" => Some(".ogg"),
        "audio/wav" | "audio/x-wav" => Some(".wav"),
        "video/mp4" => Some(".mp4"),
        "video/webm" => Some(".webm"),
        "application/pdf" => Some(".pdf"),
        "application/json" => Some(".json"),
        "application/zip" => Some(".zip"),
        "text/plain" => Some(".txt"),
        "text/csv" => Some(".csv"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Channel implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for DiscordChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            typing_keepalive_interval: 8.0,
            show_thinking: false,
            show_tool_hints: true,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        // Mirror Python: tear down any previous client before (re)starting.
        self.stop().await?;
        if self.config.bot_token.is_empty() {
            return Err(GatewayError::MissingCredentials {
                kind: "discord".to_string(),
                missing: vec!["bot_token".to_string()],
            });
        }
        // Validate proxy config up-front (Python `_proxy_auth()` in `start`).
        let http = self.build_http()?;

        *self.runtime_error.lock().unwrap() = None;
        self.ready.store(false, Ordering::SeqCst);
        *self.session.lock().unwrap() = None;

        let ctx = GatewayCtx {
            config: self.config.clone(),
            http,
            cb: self.core.enqueue_sender(),
            group_context: self.config.base.group_context.clone().unwrap_or_default(),
            ready: self.ready.clone(),
            session: self.session.clone(),
            runtime_error: self.runtime_error.clone(),
            seen: self.seen.clone(),
            bot_id: self.bot_id.clone(),
        };
        let (tx, rx) = oneshot::channel::<std::result::Result<(), String>>();
        self.task = Some(tokio::spawn(run_gateway(ctx, tx)));

        match tokio::time::timeout(Duration::from_secs_f64(CONNECT_TIMEOUT), rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(code))) => {
                self.stop().await.ok();
                Err(GatewayError::Other(code))
            }
            Ok(Err(_)) => {
                self.stop().await.ok();
                let code = self
                    .runtime_error
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "discord_connection_failed".to_string());
                Err(GatewayError::Other(code))
            }
            Err(_) => {
                self.stop().await.ok();
                Err(GatewayError::Other("discord_connect_timeout".to_string()))
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.ready.store(false, Ordering::SeqCst);
        *self.session.lock().unwrap() = None;
        self.core.set_connection_session(None);
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        let target = self.resolve_target(subject).await?;
        for chunk in split_discord_text(text, DISCORD_TEXT_LIMIT)? {
            self.post_message(&target, &chunk).await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        match self.try_send_media(subject, media).await {
            Ok(()) => Ok(()),
            Err(_) => {
                // Discord SDK and MediaBackend failures share this boundary.
                tracing::warn!("Discord attachment delivery failed");
                let url = media.url().unwrap_or("");
                let fallback = if url.is_empty() {
                    "[Attachment upload failed]".to_string()
                } else {
                    format!("[Attachment: {url}]")
                };
                self.send_text(subject, &fallback).await
            }
        }
    }

    async fn send_typing_indicator(&mut self, subject: &ChannelSubject) -> Result<()> {
        // Typing indicators are best-effort across arbitrary Discord targets.
        if self.try_typing(subject).await.is_err() {
            tracing::debug!("Discord typing indicator unavailable");
        }
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        // Python returns an already-parsed InboundMessage unchanged.
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let v = match raw {
            RawPayload::Json(v) => v.clone(),
            RawPayload::Text(t) => serde_json::from_str(t)
                .map_err(|e| GatewayError::Other(format!("discord: invalid JSON payload: {e}")))?,
            RawPayload::Preparsed(m) => return Ok(m.clone()),
        };
        let d = event_data(&v);

        let is_group = is_guild_msg(d);
        let chat_id = channel_id_of(d);
        let sender_id = author_id_of(d);
        let sender_name = author_display_name_of(d);
        let message_id = id_str(d.get("id").unwrap_or(&Value::Null));
        let raw_text = message_content_of(d);

        let bot_id = self.bot_id.lock().unwrap().clone();
        let mentioned = bot_mentioned(bot_id.as_deref(), &raw_text);
        let text = strip_bot_mention(bot_id.as_deref(), &raw_text);

        let mut parts: Vec<ContentPart> = Vec::new();
        if !text.is_empty() {
            parts.push(ContentPart::text(text));
        }
        if let Some(attachments) = d.get("attachments").and_then(|x| x.as_array()) {
            for att in attachments {
                if let Some(part) = attachment_part(att) {
                    parts.push(part);
                }
            }
        }

        let chat_type = if is_group { "group" } else { "dm" };
        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("chat_id".to_string(), json!(chat_id));
        metadata.insert("channel_id_native".to_string(), json!(chat_id));
        metadata.insert("chat_type".to_string(), json!(chat_type));
        metadata.insert("sender_id".to_string(), json!(sender_id));
        metadata.insert("sender_name".to_string(), json!(sender_name));
        metadata.insert("message_id".to_string(), json!(message_id));
        metadata.insert("bot_mentioned".to_string(), json!(mentioned));
        if is_group {
            let guild_id = d
                .get("guild_id")
                .map(id_str)
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            metadata.insert("guild_id".to_string(), json!(guild_id));
        }
        if let Some(parent_id) = parent_id_of(d) {
            metadata.insert("parent_channel_id".to_string(), json!(parent_id));
        }

        let subject_id = if is_group {
            chat_id.clone()
        } else {
            sender_id.clone()
        };
        let display_name = if is_group {
            channel_name_of(d)
        } else {
            sender_name.clone()
        };

        let session_id = self
            .session
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| format!("{}-unconnected", self.core.channel_id()));

        let timestamp = snowflake_timestamp(&message_id).unwrap_or_else(crate::channel::now_f64);

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "discord".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id,
                first_seen: None,
                last_seen: None,
                display_name,
                chat_type: chat_type.to_string(),
                metadata: metadata.clone(),
            }),
            channel_session_id: session_id,
            content: parts,
            group_context: None,
            metadata,
            timestamp,
        })
    }

    /// Download remote media honoring the proxy and the 25 MiB cap.
    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let url = if url.starts_with("//") {
            format!("https:{url}")
        } else {
            url.to_string()
        };
        let http = self.build_http()?;
        let mut resp = http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(GatewayError::Other(format!(
                "discord media fetch failed: {}",
                resp.status()
            )));
        }
        if let Some(len) = resp.content_length() {
            if len > MAX_DOWNLOAD_BYTES {
                return Err(GatewayError::Other(
                    "Discord attachment exceeds 25 MiB".to_string(),
                ));
            }
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or("").trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());

        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = resp.chunk().await? {
            data.extend_from_slice(&chunk);
            if data.len() as u64 > MAX_DOWNLOAD_BYTES {
                return Err(GatewayError::Other(
                    "Discord attachment exceeds 25 MiB".to_string(),
                ));
            }
        }
        Ok((data, content_type))
    }
}
