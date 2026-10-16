//! WeChat Official Account (微信公众号) channel — passive reply + 客服消息 outbound.
//!
//! Unlike the WebSocket channels, the official account pushes every user
//! message to a public HTTPS endpoint and *waits for the answer in the HTTP
//! response body*, but only for about five seconds. An agent rarely answers
//! that fast, so this channel uses the two-phase model 微信 itself recommends:
//!
//! 1. **Passive reply** — [`WechatMpWebhook`] answers the POST synchronously
//!    with either the literal `success` (empty `default_reply`) or a canned
//!    `default_reply` text. This is what keeps 微信 from retrying.
//! 2. **客服消息** — the agent's real answer is delivered later through
//!    `cgi-bin/message/custom/send`, which needs an `access_token` minted from
//!    `app_id` + `app_secret`.
//!
//! Inbound media is referenced by `MediaId` rather than inlined, so
//! [`WechatMpChannel::preprocess_inbound`] resolves the markers into bytes via
//! `cgi-bin/media/get` before the shared pipeline persists them.
//!
//! Two payload modes must both be honoured:
//! * **明文模式** (`encoding_aes_key` empty): the body is plain XML and the
//!   query string carries `signature` / `timestamp` / `nonce`.
//! * **安全模式** (`encoding_aes_key` = 43-char Base64): the body is
//!   `<xml><Encrypt>…</Encrypt></xml>`, AES-256-CBC encrypted, and the query
//!   string additionally carries `encrypt_type=aes` / `msg_signature`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::channel::{Channel, ChannelConfig, ChannelCore};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, ImageContent, InboundMessage, RawPayload,
    VideoContent,
};
use crate::webhook::{
    constant_time_eq, wechat_signature, WebhookAdapter, WebhookOutcome, WebhookRequest,
    WebhookResponse,
};

/// Public WeChat API origin.
const WECHAT_API: &str = "https://api.weixin.qq.com";
/// 客服消息 text payloads are capped near 2048 bytes; 1000 chars is safe even
/// for 3-byte CJK code points.
const TEXT_CHUNK: usize = 1000;
/// Marker URL scheme for media that can only be resolved through `media/get`.
const MEDIA_SCHEME: &str = "wechat-media://";
/// Upper bound on a single `media/get` download.
const MAX_MEDIA_BYTES: usize = 20 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// WeChat Official Account channel configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WechatMpConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// `AppID` from 公众号后台.
    pub app_id: String,
    /// `AppSecret`, used for `cgi-bin/token` and the 客服消息 endpoints.
    pub app_secret: String,
    /// Server token configured under 服务器配置 — signs every webhook request.
    pub token: String,
    /// 43-char Base64 `EncodingAESKey`; empty selects 明文模式.
    pub encoding_aes_key: String,
    /// Optional HTTP proxy for REST calls.
    pub http_proxy: String,
    /// Optional `user:pass` for the proxy.
    pub http_proxy_auth: String,
    /// Text answered passively (inside the 5 s window). Empty or the literal
    /// `success` means "no passive reply" — the right choice when the real
    /// answer is sent asynchronously via 客服消息.
    pub default_reply: String,
}

impl WechatMpConfig {
    /// True when the account is configured for 安全模式.
    fn safe_mode(&self) -> bool {
        !self.encoding_aes_key.trim().is_empty()
    }
}

crate::channel_adapter! {
    /// WeChat Official Account channel adapter.
    WechatMpChannel, WechatMpConfig, "wechat_mp",
    required = [app_id, token],
    fields = {}
}

// ---------------------------------------------------------------------------
// Process-wide access_token cache
// ---------------------------------------------------------------------------

/// `app_id -> (access_token, unix_expiry)`. 微信 allows only a limited number
/// of `cgi-bin/token` calls, so every send reuses a cached token until shortly
/// before it expires. The map lives for the process lifetime.
static TOKEN_CACHE: Lazy<StdMutex<HashMap<String, (String, f64)>>> =
    Lazy::new(|| StdMutex::new(HashMap::new()));

fn cache_get(app_id: &str, now: f64) -> Option<String> {
    let cache = TOKEN_CACHE.lock().unwrap_or_else(|p| p.into_inner());
    let (token, expiry) = cache.get(app_id)?;
    if !token.is_empty() && *expiry > now + 120.0 {
        Some(token.clone())
    } else {
        None
    }
}

fn cache_put(app_id: &str, token: &str, expiry: f64) {
    let mut cache = TOKEN_CACHE.lock().unwrap_or_else(|p| p.into_inner());
    cache.insert(app_id.to_string(), (token.to_string(), expiry));
}

fn cache_forget(app_id: &str) {
    let mut cache = TOKEN_CACHE.lock().unwrap_or_else(|p| p.into_inner());
    cache.remove(app_id);
}

// ---------------------------------------------------------------------------
// AES-256-CBC helpers (WeChat safe mode)
// ---------------------------------------------------------------------------

/// Decode a 43-char `EncodingAESKey` into its 32-byte key.
fn aes_key_bytes(encoding_aes_key: &str) -> Result<Vec<u8>> {
    let padded = format!("{}=", encoding_aes_key.trim());
    let key = base64::engine::general_purpose::STANDARD
        .decode(padded.as_bytes())
        .map_err(|e| GatewayError::Other(format!("wechat_mp: invalid encoding_aes_key: {e}")))?;
    if key.len() != 32 {
        return Err(GatewayError::Other(format!(
            "wechat_mp: encoding_aes_key must decode to 32 bytes, got {}",
            key.len()
        )));
    }
    Ok(key)
}

/// AES-256-CBC decrypt with `IV = key[..16]`, PKCS#7 unpadded by the cipher.
fn aes_cbc_decrypt(key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
    let mut k = [0u8; 32];
    k.copy_from_slice(key);
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&key[..16]);
    cbc::Decryptor::<aes::Aes256>::new(&k.into(), &iv.into())
        .decrypt_padded_vec_mut::<Pkcs7>(data)
        .ok()
}

/// AES-256-CBC encrypt with `IV = key[..16]` and PKCS#7 padding.
fn aes_cbc_encrypt(key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
    let mut k = [0u8; 32];
    k.copy_from_slice(key);
    let mut iv = [0u8; 16];
    iv.copy_from_slice(&key[..16]);
    Some(
        cbc::Encryptor::<aes::Aes256>::new(&k.into(), &iv.into())
            .encrypt_padded_vec_mut::<Pkcs7>(data),
    )
}

/// Split a decrypted bundle into `(message, trailing app_id)`.
///
/// Layout: 16 random bytes ‖ 4-byte big-endian `msg_len` ‖ message ‖ app_id.
fn aes_decrypt_bundle(key: &[u8], encrypt_b64: &str) -> Result<(String, String)> {
    let cipher = base64::engine::general_purpose::STANDARD
        .decode(encrypt_b64.as_bytes())
        .map_err(|e| GatewayError::Other(format!("wechat_mp: invalid base64 ciphertext: {e}")))?;
    let plain = aes_cbc_decrypt(key, &cipher)
        .ok_or_else(|| GatewayError::Other("wechat_mp: AES-CBC decrypt failed".into()))?;
    if plain.len() < 20 {
        return Err(GatewayError::Other(
            "wechat_mp: decrypted bundle too short".into(),
        ));
    }
    let msg_len = u32::from_be_bytes([plain[16], plain[17], plain[18], plain[19]]) as usize;
    let end = 20usize
        .checked_add(msg_len)
        .filter(|e| *e <= plain.len())
        .ok_or_else(|| GatewayError::Other("wechat_mp: invalid message length".into()))?;
    let msg = String::from_utf8(plain[20..end].to_vec())
        .map_err(|e| GatewayError::Other(format!("wechat_mp: message is not UTF-8: {e}")))?;
    let trailing = String::from_utf8_lossy(&plain[end..]).trim().to_string();
    Ok((msg, trailing))
}

/// Decrypt a safe-mode payload and require the trailing app_id to match.
fn decrypt_wechat_payload(encoding_aes_key: &str, encrypt_b64: &str, app_id: &str) -> Result<String> {
    let key = aes_key_bytes(encoding_aes_key)?;
    let (msg, trailing) = aes_decrypt_bundle(&key, encrypt_b64)?;
    if !app_id.is_empty() && trailing != app_id {
        return Err(GatewayError::Other(format!(
            "wechat_mp: app_id mismatch (expected {app_id}, got {trailing})"
        )));
    }
    Ok(msg)
}

/// Encrypt a passive-reply XML body and wrap it in the safe-mode envelope.
fn encrypt_wechat_reply(
    token: &str,
    encoding_aes_key: &str,
    app_id: &str,
    plaintext: &str,
) -> Result<String> {
    use rand::RngCore;
    let key = aes_key_bytes(encoding_aes_key)?;

    let mut buf = Vec::with_capacity(plaintext.len() + 36);
    let mut prefix = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut prefix);
    buf.extend_from_slice(&prefix);
    buf.extend_from_slice(&(plaintext.len() as u32).to_be_bytes());
    buf.extend_from_slice(plaintext.as_bytes());
    buf.extend_from_slice(app_id.as_bytes());

    let cipher = aes_cbc_encrypt(&key, &buf)
        .ok_or_else(|| GatewayError::Other("wechat_mp: AES-CBC encrypt failed".into()))?;
    let encrypt = base64::engine::general_purpose::STANDARD.encode(&cipher);
    let timestamp = crate::channel::now_secs().to_string();
    let nonce = format!("{}", rand::random::<u32>());
    let msg_signature = wechat_signature(token, &timestamp, &nonce, Some(&encrypt));

    Ok(format!(
        "<xml><Encrypt><![CDATA[{encrypt}]]></Encrypt><MsgSignature><![CDATA[{msg_signature}]]></MsgSignature><TimeStamp>{timestamp}</TimeStamp><Nonce><![CDATA[{nonce}]]></Nonce></xml>"
    ))
}

// ---------------------------------------------------------------------------
// XML helpers (no XML crate is available)
// ---------------------------------------------------------------------------

/// Extract the text of the first `<tag>…</tag>` element, accepting both
/// `<tag><![CDATA[value]]></tag>` and `<tag>value</tag>`.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let pattern = format!(r"(?s)<{tag}>\s*(?:<!\[CDATA\[(.*?)\]\]>|(.*?))\s*</{tag}>");
    let re = regex::Regex::new(&pattern).ok()?;
    let caps = re.captures(xml)?;
    if let Some(m) = caps.get(1) {
        return Some(m.as_str().to_string());
    }
    caps.get(2).map(|m| m.as_str().to_string())
}

/// Extract the first present tag among `tags` (微信 mixes `MsgId`/`MsgID`,
/// `FromUserName` casing across products and API versions).
fn xml_text_any(xml: &str, tags: &[&str]) -> Option<String> {
    tags.iter().find_map(|t| xml_text(xml, t))
}

/// Escape a string for use inside an XML text node.
///
/// Kept alongside [`cdata`] as the general-purpose reply-building toolkit;
/// exercised by the unit tests.
#[allow(dead_code)]
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Wrap a string in CDATA, splitting any embedded `]]>` terminator.
fn cdata(s: &str) -> String {
    if s.contains("]]>") {
        format!("<![CDATA[{}]]>", s.replace("]]>", "]]]]><![CDATA[>"))
    } else {
        format!("<![CDATA[{s}]]>")
    }
}

/// Build a passive text reply. `from_user` (the openid) becomes the reply's
/// `ToUserName`, and the account becomes `FromUserName`.
fn passive_text_xml(from_user: &str, to_user: &str, text: &str) -> String {
    format!(
        "<xml>\
<ToUserName>{}</ToUserName>\
<FromUserName>{}</FromUserName>\
<CreateTime>{}</CreateTime>\
<MsgType><![CDATA[text]]></MsgType>\
<Content>{}</Content>\
</xml>",
        cdata(from_user),
        cdata(to_user),
        crate::channel::now_secs(),
        cdata(text)
    )
}

/// Translate an inbound XML document into the native payload JSON shape that
/// [`WechatMpChannel::parse_inbound`] consumes.
fn parse_wechat_xml(xml: &str) -> Result<Value> {
    let msg_type = xml_text_any(xml, &["MsgType", "Msgtype", "msgtype"]).unwrap_or_default();
    let from_user =
        xml_text_any(xml, &["FromUserName", "FromUsername", "fromusername"]).unwrap_or_default();
    if from_user.is_empty() {
        return Err(GatewayError::Other(
            "wechat_mp: payload without FromUserName".into(),
        ));
    }
    let to_user = xml_text_any(xml, &["ToUserName", "ToUsername"]).unwrap_or_default();
    let create_time = xml_text_any(xml, &["CreateTime"])
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let msg_id = xml_text_any(xml, &["MsgId", "MsgID", "msgid"]).unwrap_or_default();

    let event_kind = if msg_type.eq_ignore_ascii_case("event") {
        "event"
    } else {
        "message"
    };

    let f64_of = |tags: &[&str]| -> f64 {
        xml_text_any(xml, tags)
            .and_then(|s| s.trim().parse::<f64>().ok())
            .unwrap_or(0.0)
    };

    Ok(json!({
        "event_kind": event_kind,
        "msg_type": msg_type,
        "from_user": from_user,
        "to_user": to_user,
        "create_time": create_time,
        "msg_id": msg_id,
        "content": xml_text_any(xml, &["Content"]).unwrap_or_default(),
        "media_id": xml_text_any(xml, &["MediaId", "MediaID", "mediaid"]).unwrap_or_default(),
        "pic_url": xml_text_any(xml, &["PicUrl"]).unwrap_or_default(),
        "format": xml_text_any(xml, &["Format"]).unwrap_or_default(),
        "recognition": xml_text_any(xml, &["Recognition"]).unwrap_or_default(),
        "event": xml_text_any(xml, &["Event", "event"]).unwrap_or_default(),
        "event_key": xml_text_any(xml, &["EventKey", "eventkey"]).unwrap_or_default(),
        "latitude": f64_of(&["Latitude"]),
        "longitude": f64_of(&["Longitude"]),
        "label": xml_text_any(xml, &["Label"]).unwrap_or_default(),
        "title": xml_text_any(xml, &["Title"]).unwrap_or_default(),
        "description": xml_text_any(xml, &["Description"]).unwrap_or_default(),
        "url": xml_text_any(xml, &["Url"]).unwrap_or_default(),
    }))
}

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

fn field<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(|x| x.as_str()).unwrap_or("")
}

/// Resolvable marker URL for media that only exposes a `MediaId`.
fn media_marker(media_id: &str) -> String {
    format!("{MEDIA_SCHEME}{media_id}")
}

fn voice_mime(format: &str) -> Option<String> {
    let m = match format.to_ascii_lowercase().as_str() {
        "amr" => "audio/amr",
        "speex" => "audio/speex",
        "silk" => "audio/silk",
        "mp3" => "audio/mpeg",
        "" => return None,
        _ => return None,
    };
    Some(m.to_string())
}

/// Human sentence describing an event so the agent can respond to it.
fn describe_event(event: &str, event_key: &str) -> String {
    let base = match event.to_ascii_lowercase().as_str() {
        "subscribe" => "User subscribed to the official account".to_string(),
        "unsubscribe" => "User unsubscribed from the official account".to_string(),
        "scan" => "User scanned the account QR code".to_string(),
        "click" => "User clicked a custom menu item".to_string(),
        "view" => "User opened a custom menu link".to_string(),
        "location" => "User reported a location".to_string(),
        "templatesendjobfinish" => "Template message delivery finished".to_string(),
        "" => "Event".to_string(),
        other => format!("Event: {other}"),
    };
    if event_key.is_empty() {
        base
    } else {
        format!("{base} (EventKey={event_key})")
    }
}

/// Extension for a downloaded inbound media item.
fn media_ext(msg_type: &str, mime: &str) -> String {
    if let Some(ext) = mime_guess::get_mime_extensions_str(mime).and_then(|e| e.first().copied()) {
        return ext.to_string();
    }
    match msg_type {
        "voice" => "amr",
        "video" | "shortvideo" => "mp4",
        "image" => "jpg",
        _ => "bin",
    }
    .to_string()
}

/// Stable short hash used in storage keys.
fn media_hash(media_id: &str) -> String {
    format!("{:x}", md5::compute(media_id.as_bytes()))
        .chars()
        .take(16)
        .collect()
}

/// Stamp persisted-media facts onto a content part, mirroring the base
/// pipeline's `set_media_meta`.
fn stamp_local(part: &mut ContentPart, key: String, size: u64, mime: String) {
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

/// Split text on character boundaries, preferring paragraph/line breaks.
fn chunk_text(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
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

/// Resolve the destination openid for an outbound send.
fn resolve_openid(subject: &ChannelSubject) -> Result<String> {
    let openid = subject
        .metadata
        .get("openid")
        .and_then(|v| v.as_str())
        .or_else(|| subject.metadata.get("to_handle").and_then(|v| v.as_str()))
        .filter(|s| !s.is_empty())
        .unwrap_or(subject.subject_id.as_str());
    if openid.is_empty() {
        return Err(GatewayError::Other(
            "wechat_mp: empty destination openid".into(),
        ));
    }
    Ok(openid.to_string())
}

// ---------------------------------------------------------------------------
// Dedup (parse_inbound takes &self, so ChannelCore::is_duplicate is unavailable)
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
impl Channel for WechatMpChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            // 微信 drops the socket if the passive reply misses the 5 s window,
            // so fire the placeholder well before it (the guard triggers at
            // 80% of this value).
            reply_timeout: 4.5,
            timeout_strategy: "placeholder".to_string(),
            send_rate_limit: Some((20, 60.0)),
            typing_keepalive_interval: 0.0,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        if self.config.app_secret.trim().is_empty() {
            tracing::warn!(
                "wechat_mp: app_secret is empty; only passive (5 s) replies will work \
                 (set app_secret to enable the 客服消息 API)"
            );
            return Ok(());
        }

        // Warm the token cache. A transport failure is an `Err`; a provider
        // error response is only logged, because the account may still answer
        // passively.
        let http = self.build_http()?;
        let url = format!(
            "{WECHAT_API}/cgi-bin/token?grant_type=client_credential&appid={}&secret={}",
            urlencoding::encode(self.config.app_id.trim()),
            urlencoding::encode(self.config.app_secret.trim()),
        );
        let resp = http.get(&url).send().await?;
        match resp.json::<Value>().await {
            Ok(v) => match v.get("access_token").and_then(|t| t.as_str()) {
                Some(token) if !token.is_empty() => {
                    let expires = v.get("expires_in").and_then(|e| e.as_i64()).unwrap_or(7200) as f64;
                    cache_put(&self.config.app_id, token, crate::channel::now_f64() + expires);
                    tracing::info!(
                        "wechat_mp: access_token acquired (app_id={})",
                        self.config.app_id
                    );
                }
                _ => tracing::warn!("wechat_mp: token endpoint returned {}", v),
            },
            Err(e) => tracing::warn!("wechat_mp: token response was not JSON: {e}"),
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        // `mp_passive` marks the message as having arrived through the webhook,
        // whose HTTP response already carried the passive answer (the canned
        // `default_reply` or `success`). A caller that injected its *real*
        // answer into that body sets `mp_passive_sent = true` so this async
        // duplicate is skipped.
        let passive = subject
            .metadata
            .get("mp_passive")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let passive_sent = subject
            .metadata
            .get("mp_passive_sent")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if passive && passive_sent {
            return Ok(());
        }

        let openid = resolve_openid(subject)?;
        for chunk in chunk_text(text, TEXT_CHUNK) {
            self.send_custom(
                &openid,
                json!({
                    "touser": openid,
                    "msgtype": "text",
                    "text": { "content": chunk },
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        if media.is_text() {
            return Ok(());
        }
        let openid = resolve_openid(subject)?;
        let filename = media
            .filename()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .unwrap_or_else(|| media.label().to_ascii_lowercase());

        let (bytes, mime) = self.load_media_bytes(media).await?;
        if bytes.len() > MAX_MEDIA_BYTES {
            // Oversized for the 客服消息 upload; degrade to a text notice.
            return self.file_fallback(subject, media).await;
        }
        let _ = mime;

        match media {
            ContentPart::Image(_) => {
                let media_id = self.upload_media("image", &filename, bytes).await?;
                self.send_custom(
                    &openid,
                    json!({
                        "touser": openid,
                        "msgtype": "image",
                        "image": { "media_id": media_id },
                    }),
                )
                .await
            }
            ContentPart::Audio(_) => {
                let media_id = self.upload_media("voice", &filename, bytes).await?;
                self.send_custom(
                    &openid,
                    json!({
                        "touser": openid,
                        "msgtype": "voice",
                        "voice": { "media_id": media_id },
                    }),
                )
                .await
            }
            ContentPart::Video(_) => {
                let media_id = self.upload_media("video", &filename, bytes).await?;
                self.send_custom(
                    &openid,
                    json!({
                        "touser": openid,
                        "msgtype": "video",
                        "video": {
                            "media_id": media_id,
                            "title": filename,
                            "description": filename,
                        },
                    }),
                )
                .await
            }
            // 微信 has no generic "file" type for 客服消息, so anything else
            // (documents, archives, …) is announced as text instead.
            _ => self.file_fallback(subject, media).await,
        }
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("wechat_mp expects a JSON payload".into()))?;

        let from_user = field(v, "from_user");
        if from_user.is_empty() {
            return Err(GatewayError::Other(
                "wechat_mp: payload without from_user".into(),
            ));
        }
        let to_user = field(v, "to_user").to_string();
        let msg_type = field(v, "msg_type").to_string();
        let msg_id = field(v, "msg_id").to_string();
        let media_id = field(v, "media_id");
        let pic_url = field(v, "pic_url");
        let create_time = v.get("create_time").and_then(|x| x.as_i64()).unwrap_or(0);

        let mut content: Vec<ContentPart> = Vec::new();
        match msg_type.as_str() {
            "text" => {
                let text = field(v, "content");
                if !text.is_empty() {
                    content.push(ContentPart::text(text));
                }
            }
            "voice" => {
                let recognition = field(v, "recognition");
                if !recognition.is_empty() {
                    // 微信 exposes a speech-to-text transcript when the account
                    // has 语音识别 enabled — far more useful than the audio.
                    content.push(ContentPart::text(recognition));
                } else if !media_id.is_empty() {
                    content.push(ContentPart::Audio(AudioContent {
                        url: media_marker(media_id),
                        mime_type: voice_mime(field(v, "format")),
                        ..Default::default()
                    }));
                }
            }
            "image" => {
                let url = if !pic_url.is_empty() {
                    pic_url.to_string()
                } else if !media_id.is_empty() {
                    media_marker(media_id)
                } else {
                    String::new()
                };
                if !url.is_empty() {
                    content.push(ContentPart::Image(ImageContent {
                        url,
                        ..Default::default()
                    }));
                }
            }
            "video" | "shortvideo" => {
                if !media_id.is_empty() {
                    content.push(ContentPart::Video(VideoContent {
                        url: media_marker(media_id),
                        ..Default::default()
                    }));
                }
            }
            "location" => {
                let label = field(v, "label");
                let lat = v.get("latitude").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let lon = v.get("longitude").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let text = if label.is_empty() {
                    format!("Location: ({lat}, {lon})")
                } else {
                    format!("Location: {label} ({lat}, {lon})")
                };
                content.push(ContentPart::text(text));
            }
            "link" => {
                let title = field(v, "title");
                let url = field(v, "url");
                content.push(ContentPart::text(format!("Link: {title} — {url}")));
            }
            "event" => {
                content.push(ContentPart::text(describe_event(
                    field(v, "event"),
                    field(v, "event_key"),
                )));
            }
            other => {
                let text = field(v, "content");
                if !text.is_empty() {
                    content.push(ContentPart::text(text));
                } else if !other.is_empty() {
                    content.push(ContentPart::text(format!("[{other} message]")));
                }
            }
        }
        if content.is_empty() {
            content.push(ContentPart::text(format!("[{msg_type} message]")));
        }

        let mut subject = ChannelSubject::new(from_user);
        // 微信 exposes no nickname through this API, so the openid is both the
        // id and the best available display name.
        subject.display_name = from_user.to_string();
        subject.chat_type = "user".to_string();
        subject
            .metadata
            .insert("to_handle".into(), Value::String(from_user.to_string()));
        subject
            .metadata
            .insert("openid".into(), Value::String(from_user.to_string()));
        subject
            .metadata
            .insert("message_id".into(), Value::String(msg_id.clone()));
        subject
            .metadata
            .insert("create_time".into(), json!(create_time));
        subject
            .metadata
            .insert("msg_type".into(), Value::String(msg_type.clone()));
        subject.metadata.insert("mp_passive".into(), Value::Bool(true));

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("message_id".into(), Value::String(msg_id));
        metadata.insert("to_handle".into(), Value::String(from_user.to_string()));
        metadata.insert("openid".into(), Value::String(from_user.to_string()));
        metadata.insert("mp_passive".into(), Value::Bool(true));
        metadata.insert("create_time".into(), json!(create_time));
        metadata.insert("msg_type".into(), Value::String(msg_type));
        metadata.insert("to_user".into(), Value::String(to_user));

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "wechat_mp".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(subject),
            channel_session_id: from_user.to_string(),
            content,
            group_context: None,
            metadata,
            timestamp: create_time as f64,
        })
    }

    /// Resolve every `wechat-media://<MediaId>` marker into bytes through
    /// `cgi-bin/media/get` and persist it via the configured media backend.
    async fn preprocess_inbound(&mut self, message: &mut InboundMessage) -> Result<()> {
        let markers: Vec<(usize, String)> = message
            .content
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                p.url()
                    .and_then(|u| u.strip_prefix(MEDIA_SCHEME))
                    .filter(|id| !id.is_empty())
                    .map(|id| (i, id.to_string()))
            })
            .collect();
        if markers.is_empty() {
            return Ok(());
        }

        let backend = match self.core().media_backend() {
            Some(b) => b.clone(),
            None => {
                tracing::warn!(
                    "wechat_mp: {} media marker(s) present but no media backend configured; \
                     leaving markers unresolved",
                    markers.len()
                );
                return Ok(());
            }
        };
        let channel_id = self.core().channel_id().to_string();
        let msg_type = message
            .metadata
            .get("msg_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        for (idx, media_id) in markers {
            let (data, mime) = match self.download_media(&media_id).await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("wechat_mp: media download failed for {media_id}: {e}");
                    continue;
                }
            };
            let ext = media_ext(&msg_type, &mime);
            let filename = format!(
                "{}_{}.{}",
                crate::channel::now_secs(),
                media_hash(&media_id),
                ext
            );
            let key = format!("wechat_mp/{channel_id}/{filename}");
            if let Err(e) = backend.save(&data, &key).await {
                tracing::warn!("wechat_mp: media save failed for {media_id}: {e}");
                continue;
            }
            if let Some(part) = message.content.get_mut(idx) {
                stamp_local(part, key, data.len() as u64, mime);
            }
        }
        Ok(())
    }

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        mut meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        // 客服消息 addresses the recipient by openid, which for this channel is
        // exactly the subject id.
        meta.insert(
            "to_handle".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta.insert(
            "openid".into(),
            Value::String(subject.subject_id.clone()),
        );
        meta
    }
}

impl WechatMpChannel {
    fn build_http(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(20));
        let proxy = self.config.http_proxy.trim();
        if !proxy.is_empty() {
            let mut p = reqwest::Proxy::all(proxy)?;
            let auth = self.config.http_proxy_auth.trim();
            if !auth.is_empty() {
                let (user, pass) = match auth.split_once(':') {
                    Some((u, p)) => (u, p),
                    None => (auth, ""),
                };
                p = p.basic_auth(user, pass);
            }
            builder = builder.proxy(p);
        }
        builder
            .build()
            .map_err(|e| GatewayError::Other(format!("wechat_mp http client: {e}")))
    }

    /// Return a live `access_token`, consulting the process-wide cache first.
    async fn access_token(&self) -> Result<String> {
        if self.config.app_secret.trim().is_empty() {
            return Err(GatewayError::Other(
                "wechat_mp: app_secret is required for 客服消息 / media endpoints".into(),
            ));
        }
        let app_id = self.config.app_id.clone();
        if let Some(tok) = cache_get(&app_id, crate::channel::now_f64()) {
            return Ok(tok);
        }
        let http = self.build_http()?;
        let url = format!(
            "{WECHAT_API}/cgi-bin/token?grant_type=client_credential&appid={}&secret={}",
            urlencoding::encode(app_id.trim()),
            urlencoding::encode(self.config.app_secret.trim()),
        );
        let resp = http.get(&url).send().await?;
        let v: Value = resp.json().await?;
        let token = v
            .get("access_token")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string())
            .ok_or_else(|| GatewayError::Other(format!("wechat_mp: token fetch failed: {v}")))?;
        let expires = v.get("expires_in").and_then(|e| e.as_i64()).unwrap_or(7200) as f64;
        cache_put(&app_id, &token, crate::channel::now_f64() + expires);
        Ok(token)
    }

    /// POST a 客服消息 body, refreshing an expired token once.
    async fn send_custom(&self, _openid: &str, payload: Value) -> Result<()> {
        let http = self.build_http()?;
        for attempt in 0..2 {
            let token = self.access_token().await?;
            let url = format!("{WECHAT_API}/cgi-bin/message/custom/send?access_token={token}");
            let resp = http.post(&url).json(&payload).send().await?;
            let v: Value = resp.json().await?;
            let code = v.get("errcode").and_then(|c| c.as_i64()).unwrap_or(0);
            if code == 0 {
                return Ok(());
            }
            // 40001/40014/42001 are the "access_token expired/invalid" family.
            if matches!(code, 40001 | 40014 | 42001) && attempt == 0 {
                cache_forget(&self.config.app_id);
                continue;
            }
            return Err(GatewayError::Other(format!(
                "wechat_mp: custom send failed: {v}"
            )));
        }
        Err(GatewayError::Other(
            "wechat_mp: custom send failed after token refresh".into(),
        ))
    }

    /// Upload bytes to `cgi-bin/media/upload` and return the `media_id`.
    async fn upload_media(
        &self,
        media_type: &str,
        filename: &str,
        data: Vec<u8>,
    ) -> Result<String> {
        let token = self.access_token().await?;
        let http = self.build_http()?;
        let part = reqwest::multipart::Part::bytes(data).file_name(filename.to_string());
        let form = reqwest::multipart::Form::new().part("media", part);
        let url = format!("{WECHAT_API}/cgi-bin/media/upload?access_token={token}&type={media_type}");
        let resp = http.post(&url).multipart(form).send().await?;
        let v: Value = resp.json().await?;
        if let Some(code) = v.get("errcode").and_then(|c| c.as_i64()) {
            if code != 0 {
                return Err(GatewayError::Other(format!(
                    "wechat_mp: media upload failed: {v}"
                )));
            }
        }
        v.get("media_id")
            .and_then(|m| m.as_str())
            .filter(|m| !m.is_empty())
            .map(|m| m.to_string())
            .ok_or_else(|| {
                GatewayError::Other(format!("wechat_mp: upload returned no media_id: {v}"))
            })
    }

    /// Download by `MediaId` through `cgi-bin/media/get`.
    async fn download_media(&self, media_id: &str) -> Result<(Vec<u8>, String)> {
        let token = self.access_token().await?;
        let http = self.build_http()?;
        let url = format!(
            "{WECHAT_API}/cgi-bin/media/get?access_token={token}&media_id={}",
            urlencoding::encode(media_id)
        );
        let resp = http.get(&url).send().await?;
        let mime = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let data = resp.bytes().await?;
        // Errors come back as a small JSON document with a JSON content type.
        if mime.contains("json") || mime.contains("text/plain") {
            if let Ok(v) = serde_json::from_slice::<Value>(&data) {
                let code = v.get("errcode").and_then(|c| c.as_i64()).unwrap_or(0);
                if code != 0 {
                    return Err(GatewayError::Other(format!(
                        "wechat_mp: media/get failed: {v}"
                    )));
                }
            }
        }
        if data.len() > MAX_MEDIA_BYTES {
            return Err(GatewayError::Other(format!(
                "wechat_mp: media exceeds {MAX_MEDIA_BYTES} bytes"
            )));
        }
        Ok((data.to_vec(), mime))
    }

    /// Announce an unsupported attachment as text.
    async fn file_fallback(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let name = media
            .filename()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .unwrap_or_else(|| media.label().to_string());
        // 微信's 客服消息 API has no generic file type; a document can only be
        // referenced, never attached.
        self.send_text(
            subject,
            &format!("[File: {name}]: download not available"),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Webhook adapter
// ---------------------------------------------------------------------------

/// WeChat Official Account webhook adapter: verifies signatures, decrypts safe
/// mode, produces the native payload, and answers the passive-reply window.
pub struct WechatMpWebhook {
    config: WechatMpConfig,
    dedup: StdMutex<LruSet>,
}

impl WechatMpWebhook {
    pub fn new(config: WechatMpConfig) -> Self {
        WechatMpWebhook {
            config,
            dedup: StdMutex::new(LruSet::new(2000)),
        }
    }

    /// Build the passive HTTP response. This is where the answer *must* be
    /// delivered because 微信 only reads the HTTP response body — but the agent
    /// needs longer than the 5 s window, so the real answer is sent later via
    /// the 客服消息 API ([`WechatMpChannel::send_text`]) and this body carries
    /// only the canned `default_reply`, or the literal `success` when none is
    /// configured (the recommended default).
    fn passive_reply(&self, native: &Value) -> Result<WebhookResponse> {
        let reply = self.config.default_reply.trim();
        if reply.is_empty() || reply.eq_ignore_ascii_case("success") {
            return Ok(WebhookResponse::text("success"));
        }
        let from_user = native.get("from_user").and_then(|v| v.as_str()).unwrap_or("");
        let to_user = native.get("to_user").and_then(|v| v.as_str()).unwrap_or("");
        let xml = passive_text_xml(from_user, to_user, reply);
        let body = if self.config.safe_mode() {
            encrypt_wechat_reply(
                &self.config.token,
                &self.config.encoding_aes_key,
                &self.config.app_id,
                &xml,
            )?
        } else {
            xml
        };
        Ok(WebhookResponse::raw("text/xml; charset=utf-8", body))
    }
}

impl WebhookAdapter for WechatMpWebhook {
    fn channel_type(&self) -> &'static str {
        "wechat_mp"
    }

    fn verify_challenge(&self, req: &WebhookRequest) -> Option<WebhookResponse> {
        let signature = req.query_param("signature")?;
        let timestamp = req.query_param("timestamp").unwrap_or("");
        let nonce = req.query_param("nonce").unwrap_or("");
        let echostr = req.query_param("echostr").unwrap_or("");

        let expected = wechat_signature(&self.config.token, timestamp, nonce, None);
        if signature.is_empty() || !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
            return Some(WebhookResponse::status(403, "invalid signature"));
        }
        if echostr.is_empty() {
            return Some(WebhookResponse::status(403, "missing echostr"));
        }

        if !self.config.safe_mode() {
            // 明文模式: echo the challenge verbatim.
            return Some(WebhookResponse::text(echostr.to_string()));
        }

        // 安全模式: `echostr` is Base64 of the AES-CBC bundle. The shared query
        // parser maps `+` to a space, so restore it before decoding.
        let cipher = echostr.replace(' ', "+");
        let key = match aes_key_bytes(&self.config.encoding_aes_key) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!("wechat_mp: {e}");
                return Some(WebhookResponse::status(403, "invalid encoding_aes_key"));
            }
        };
        match aes_decrypt_bundle(&key, &cipher) {
            Ok((msg, trailing)) => {
                // 微信 appends the app_id; some deployments echo the signature.
                if !trailing.is_empty()
                    && trailing != self.config.app_id
                    && !constant_time_eq(trailing.as_bytes(), expected.as_bytes())
                {
                    tracing::warn!(
                        "wechat_mp: echostr trailing token {trailing:?} matches neither app_id nor signature"
                    );
                    return Some(WebhookResponse::status(403, "invalid echostr"));
                }
                Some(WebhookResponse::text(msg))
            }
            Err(e) => {
                tracing::warn!("wechat_mp: echostr decrypt failed: {e}");
                Some(WebhookResponse::status(403, "invalid echostr"))
            }
        }
    }

    fn handle_request(&self, req: &WebhookRequest) -> Result<WebhookOutcome> {
        let body = req.body_text();

        // Safe mode is announced by an <Encrypt> element (body) plus
        // encrypt_type=aes / msg_signature (query).
        let encrypt = xml_text_any(&body, &["Encrypt", "encrypt"]).filter(|e| !e.is_empty());

        let (plain_xml, nonce) = match encrypt {
            Some(enc) => {
                if !self.config.safe_mode() {
                    return Err(GatewayError::Other(
                        "wechat_mp: encrypted body but no encoding_aes_key configured".into(),
                    ));
                }
                let timestamp = req
                    .query_param("timestamp")
                    .unwrap_or("")
                    .to_string();
                let nonce = req.query_param("nonce").unwrap_or("").to_string();
                let signature: Option<String> = req
                    .query_param("msg_signature")
                    .or_else(|| req.query_param("signature"))
                    .map(|s| s.to_string())
                    .or_else(|| {
                        // Some proxies strip the query string; the envelope may
                        // echo the signature inline.
                        xml_text_any(&body, &["MsgSignature", "msg_signature"])
                    });
                let Some(signature) = signature else {
                    return Err(GatewayError::Other(
                        "wechat_mp: missing msg_signature".into(),
                    ));
                };
                let expected =
                    wechat_signature(&self.config.token, &timestamp, &nonce, Some(&enc));
                if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
                    return Err(GatewayError::Other(
                        "wechat_mp: signature verification failed".into(),
                    ));
                }
                let plain = decrypt_wechat_payload(
                    &self.config.encoding_aes_key,
                    &enc,
                    &self.config.app_id,
                )?;
                (plain, nonce)
            }
            None => {
                let timestamp = req.query_param("timestamp").unwrap_or("");
                let nonce = req.query_param("nonce").unwrap_or("").to_string();
                let signature = req.query_param("signature").unwrap_or("");
                let expected = wechat_signature(&self.config.token, timestamp, &nonce, None);
                if signature.is_empty()
                    || !constant_time_eq(expected.as_bytes(), signature.as_bytes())
                {
                    return Err(GatewayError::Other(
                        "wechat_mp: signature verification failed".into(),
                    ));
                }
                (body.clone(), nonce)
            }
        };
        let _ = nonce;

        let native = parse_wechat_xml(&plain_xml)?;

        // 微信 retries a callback when the reply is slow; drop the duplicates.
        // `parse_inbound` is `&self`, so the channel's own dedup is unusable.
        let dedup_key = {
            let msg_id = native.get("msg_id").and_then(|v| v.as_str()).unwrap_or("");
            if !msg_id.is_empty() {
                format!("m:{msg_id}")
            } else {
                format!(
                    "e:{}:{}:{}",
                    native.get("from_user").and_then(|v| v.as_str()).unwrap_or(""),
                    native.get("create_time").and_then(|v| v.as_i64()).unwrap_or(0),
                    native.get("event").and_then(|v| v.as_str()).unwrap_or(""),
                )
            }
        };
        {
            let mut dedup = self.dedup.lock().unwrap_or_else(|p| p.into_inner());
            if !dedup.fresh(&dedup_key) {
                tracing::debug!("wechat_mp: duplicate delivery dropped ({dedup_key})");
                return Ok(WebhookOutcome::with_response(
                    WebhookResponse::text("success"),
                    Vec::new(),
                ));
            }
        }

        let response = self.passive_reply(&native)?;
        Ok(WebhookOutcome::with_response(
            response,
            vec![RawPayload::Json(native)],
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Offline helper that builds a safe-mode ciphertext bundle.
    fn encrypt_for_test(key: &[u8], msg: &str, app_id: &str) -> String {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 16]);
        buf.extend_from_slice(&(msg.len() as u32).to_be_bytes());
        buf.extend_from_slice(msg.as_bytes());
        buf.extend_from_slice(app_id.as_bytes());
        let cipher = aes_cbc_encrypt(key, &buf).expect("encrypt");
        base64::engine::general_purpose::STANDARD.encode(&cipher)
    }

    fn encoding_key_from(key: &[u8; 32]) -> String {
        let b64 = base64::engine::general_purpose::STANDARD.encode(key);
        assert!(b64.ends_with('='), "32-byte key must encode with one '='");
        b64[..43].to_string()
    }

    fn empty_processor() -> crate::channel::MessageProcessor {
        Arc::new(|_m: InboundMessage| {
            let s: std::pin::Pin<
                Box<dyn futures::Stream<Item = crate::models::MessageEvent> + Send>,
            > = Box::pin(futures::stream::empty());
            s
        })
    }

    fn test_channel() -> WechatMpChannel {
        WechatMpChannel::new(
            empty_processor(),
            WechatMpConfig::default(),
            Some("ch1".to_string()),
            None,
            0.0,
        )
    }

    #[test]
    fn wechat_signature_matches_manual_sha1() {
        use sha1::{Digest, Sha1};

        let token = "token-abc";
        let timestamp = "1700000000";
        let nonce = "nonce-xyz";

        let mut items = vec![token, timestamp, nonce];
        items.sort();
        let mut hasher = Sha1::new();
        hasher.update(items.concat().as_bytes());
        let expected = hex::encode(hasher.finalize());
        assert_eq!(wechat_signature(token, timestamp, nonce, None), expected);

        // Safe mode folds the ciphertext into the same sorted list.
        let encrypt = "ENCRYPTED-BLOB";
        let mut items = vec![token, timestamp, nonce, encrypt];
        items.sort();
        let mut hasher = Sha1::new();
        hasher.update(items.concat().as_bytes());
        let expected = hex::encode(hasher.finalize());
        assert_eq!(
            wechat_signature(token, timestamp, nonce, Some(encrypt)),
            expected
        );
    }

    #[test]
    fn xml_text_handles_cdata_and_plain() {
        let xml = "<xml>\
<ToUserName><![CDATA[gh_account]]></ToUserName>\
<FromUserName><![CDATA[openid_123]]></FromUserName>\
<MsgType>text</MsgType>\
<Content><![CDATA[hello & <world>]]></Content>\
</xml>";
        assert_eq!(xml_text(xml, "ToUserName").as_deref(), Some("gh_account"));
        assert_eq!(xml_text(xml, "MsgType").as_deref(), Some("text"));
        assert_eq!(
            xml_text(xml, "Content").as_deref(),
            Some("hello & <world>")
        );
        assert_eq!(xml_text(xml, "Absent"), None);
        assert_eq!(
            xml_text_any(xml, &["missing", "MsgType"]).as_deref(),
            Some("text")
        );
    }

    #[test]
    fn xml_escape_produces_extractable_text() {
        let escaped = xml_escape("a & b < c > d");
        assert_eq!(escaped, "a &amp; b &lt; c &gt; d");
        // The escaped value survives a plain (non-CDATA) tag verbatim.
        let xml = format!("<x>{escaped}</x>");
        assert_eq!(xml_text(&xml, "x").as_deref(), Some(escaped.as_str()));
    }

    #[test]
    fn safe_mode_payload_round_trips() {
        let key = [7u8; 32];
        let encoding_key = encoding_key_from(&key);
        assert_eq!(aes_key_bytes(&encoding_key).expect("key").len(), 32);

        let msg = "<xml><Content><![CDATA[hello 微信]]></Content></xml>";
        let encrypt = encrypt_for_test(&key, msg, "wxappid");

        let decrypted =
            decrypt_wechat_payload(&encoding_key, &encrypt, "wxappid").expect("decrypt");
        assert_eq!(decrypted, msg);

        // The trailing app_id is enforced.
        assert!(decrypt_wechat_payload(&encoding_key, &encrypt, "other").is_err());
    }

    #[test]
    fn safe_mode_reply_envelope_round_trips() {
        let key = [3u8; 32];
        let encoding_key = encoding_key_from(&key);
        let token = "tok";
        let xml = "<xml><Content><![CDATA[bye]]></Content></xml>";

        let envelope = encrypt_wechat_reply(token, &encoding_key, "wxappid", xml).expect("encrypt");
        let encrypt = xml_text(&envelope, "Encrypt").expect("Encrypt element");
        let signature = xml_text(&envelope, "MsgSignature").expect("MsgSignature element");
        let timestamp = xml_text(&envelope, "TimeStamp").expect("TimeStamp element");
        let nonce = xml_text(&envelope, "Nonce").expect("Nonce element");

        assert_eq!(
            signature,
            wechat_signature(token, &timestamp, &nonce, Some(&encrypt))
        );
        assert_eq!(
            decrypt_wechat_payload(&encoding_key, &encrypt, "wxappid").expect("decrypt"),
            xml
        );
    }

    #[test]
    fn parses_text_message() {
        let channel = test_channel();
        let payload = json!({
            "event_kind": "message",
            "msg_type": "text",
            "from_user": "openid_abc",
            "to_user": "gh_123",
            "create_time": 1_700_000_000i64,
            "msg_id": "10001",
            "content": "hello world",
        });
        let message = channel
            .parse_inbound(&RawPayload::Json(payload))
            .expect("parse");

        assert_eq!(message.channel_type, "wechat_mp");
        assert_eq!(message.channel_session_id, "openid_abc");
        assert_eq!(message.text(), "hello world");
        assert_eq!(message.timestamp, 1_700_000_000.0);
        assert!(message.group_context.is_none());

        let subject = message.channel_subject.clone().expect("subject");
        assert_eq!(subject.subject_id, "openid_abc");
        assert_eq!(subject.display_name, "openid_abc");
        assert_eq!(subject.chat_type, "user");
        assert_eq!(
            subject.metadata.get("to_handle").and_then(|v| v.as_str()),
            Some("openid_abc")
        );
        assert_eq!(
            message.metadata.get("mp_passive").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            message.metadata.get("message_id").and_then(|v| v.as_str()),
            Some("10001")
        );
    }

    #[test]
    fn parses_image_message_using_pic_url() {
        let channel = test_channel();
        let payload = json!({
            "event_kind": "message",
            "msg_type": "image",
            "from_user": "openid_abc",
            "to_user": "gh_123",
            "create_time": 1_700_000_001i64,
            "msg_id": "10002",
            "pic_url": "https://mmbiz.qpic.cn/mmbiz_jpg/abcd/0",
            "media_id": "MEDIAID_123",
        });
        let message = channel
            .parse_inbound(&RawPayload::Json(payload))
            .expect("parse");

        assert_eq!(message.content.len(), 1);
        match &message.content[0] {
            ContentPart::Image(image) => {
                assert_eq!(image.url, "https://mmbiz.qpic.cn/mmbiz_jpg/abcd/0");
            }
            other => panic!("expected image part, got {other:?}"),
        }
    }

    #[test]
    fn voice_without_recognition_becomes_media_marker() {
        let channel = test_channel();
        let payload = json!({
            "event_kind": "message",
            "msg_type": "voice",
            "from_user": "openid_abc",
            "to_user": "gh_123",
            "create_time": 1_700_000_002i64,
            "msg_id": "10003",
            "media_id": "VOICE_ID",
            "format": "amr",
        });
        let message = channel
            .parse_inbound(&RawPayload::Json(payload))
            .expect("parse");
        match &message.content[0] {
            ContentPart::Audio(audio) => {
                assert_eq!(audio.url, "wechat-media://VOICE_ID");
                assert_eq!(audio.mime_type.as_deref(), Some("audio/amr"));
            }
            other => panic!("expected audio part, got {other:?}"),
        }
    }

    #[test]
    fn webhook_verifies_dedups_and_replies_success() {
        let config = WechatMpConfig {
            app_id: "wxappid".to_string(),
            token: "tok".to_string(),
            ..Default::default()
        };
        let webhook = WechatMpWebhook::new(config);

        let timestamp = "1700000000";
        let nonce = "nonce1";
        let signature = wechat_signature("tok", timestamp, nonce, None);

        let body = "<xml>\
<ToUserName><![CDATA[gh_1]]></ToUserName>\
<FromUserName><![CDATA[openid_abc]]></FromUserName>\
<CreateTime>1700000000</CreateTime>\
<MsgType><![CDATA[text]]></MsgType>\
<Content><![CDATA[hi]]></Content>\
<MsgId>10001</MsgId>\
</xml>"
            .as_bytes()
            .to_vec();

        let mut query = HashMap::new();
        query.insert("signature".to_string(), signature);
        query.insert("timestamp".to_string(), timestamp.to_string());
        query.insert("nonce".to_string(), nonce.to_string());

        let req = WebhookRequest {
            method: "POST".to_string(),
            path: "/webhooks/wechat".to_string(),
            query: query.clone(),
            headers: HashMap::new(),
            body,
        };

        let first = webhook.handle_request(&req).expect("first");
        assert_eq!(first.payloads.len(), 1);
        // No default_reply configured -> the body is the literal "success".
        assert_eq!(first.response.body, "success");

        // The exact same delivery is dropped by the dedup cache.
        let second = webhook.handle_request(&req).expect("second");
        assert_eq!(second.payloads.len(), 0);

        // A bad signature is rejected outright.
        let mut bad = req.clone();
        bad.query
            .insert("signature".to_string(), "deadbeef".to_string());
        assert!(webhook.handle_request(&bad).is_err());
    }

    #[test]
    fn webhook_rejects_missing_signature() {
        let webhook = WechatMpWebhook::new(WechatMpConfig {
            token: "tok".to_string(),
            ..Default::default()
        });
        let req = WebhookRequest {
            method: "POST".to_string(),
            path: "/webhooks/wechat".to_string(),
            query: HashMap::new(),
            headers: HashMap::new(),
            body: b"<xml><FromUserName><![CDATA[x]]></FromUserName></xml>".to_vec(),
        };
        assert!(webhook.handle_request(&req).is_err());
    }

    #[test]
    fn lru_set_evicts_oldest() {
        let mut set = LruSet::new(2);
        assert!(set.fresh("a"));
        assert!(set.fresh("b"));
        assert!(!set.fresh("a")); // still cached
        assert!(set.fresh("c")); // evicts the oldest entry, "a"
        assert!(set.fresh("a")); // fresh again after eviction (evicts "b")
        assert!(!set.fresh("c")); // still cached
    }

    #[test]
    fn chunking_stays_within_limit() {
        let text = "a".repeat(2500);
        let chunks = chunk_text(&text, TEXT_CHUNK);
        assert!(chunks.len() >= 3);
        for c in &chunks {
            assert!(c.chars().count() <= TEXT_CHUNK);
        }
        assert_eq!(chunks.concat(), text);
    }
}
