//! Telegram channel: Bot API with HTTP long polling.
//!
//! Direct Bot API HTTP calls via `reqwest` plus a background `getUpdates`
//! long-polling loop that converts each update into a native payload and
//! enqueues it as `RawPayload::Json`.
//!
//! `format_html.py` is inlined verbatim as [`format_html`].

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

use crate::channel::{Channel, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{ChannelSubject, ContentPart, InboundMessage, RawPayload};

const TG_API: &str = "https://api.telegram.org/bot";
const TG_FILE_API: &str = "https://api.telegram.org/file/bot";
const TG_SEND_CHUNK_SIZE: usize = 4000;
const TG_MAX_FILE_SIZE_BYTES: usize = 50 * 1024 * 1024;

// ===========================================================================
// Markdown -> Telegram HTML formatter used by `send_text`.
// ===========================================================================

/// Convert standard Markdown to Telegram-compatible HTML.
///
/// Markdown -> Telegram HTML. The two italic rules use
/// lookbehind/lookahead (`(?<!\w)...(?!\w)`), which the `regex` crate does not
/// support, so they are emulated by [`replace_italic`] with identical
/// leftmost-match + minimal-content + backtracking semantics.
pub mod format_html {
    use regex::{Regex, RegexBuilder};

    fn escape_html(text: &str) -> String {
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    fn ph(placeholders: &mut Vec<String>, fragment: &str) -> String {
        let idx = placeholders.len();
        placeholders.push(fragment.to_string());
        format!("\u{0}PH{}\u{0}", idx)
    }

    fn is_word(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    /// Emulate `re.sub(r"(?<!\w)DELIM(.+?)DELIM(?!\w)", ...)`.
    fn replace_italic(text: &str, delim: u8, tag: &str) -> String {
        let bytes = text.as_bytes();
        let n = bytes.len();
        let mut out = String::with_capacity(text.len());
        let mut copied = 0usize;
        let mut i = 0usize;
        while i < n {
            if bytes[i] != delim {
                i += 1;
                continue;
            }
            // Lookbehind: previous char must not be a word char.
            let before_ok = match text[..i].chars().next_back() {
                Some(c) => !is_word(c),
                None => true,
            };
            if !before_ok {
                i += 1;
                continue;
            }
            // Backtracking search for the minimal closing delimiter.
            let mut j = i + 1;
            let mut close: Option<usize> = None;
            while j < n {
                if bytes[j] == b'\n' {
                    break;
                }
                if bytes[j] == delim && j > i + 1 {
                    let after_ok = match text[j + 1..].chars().next() {
                        Some(c) => !is_word(c),
                        None => true,
                    };
                    if after_ok {
                        close = Some(j);
                        break;
                    }
                }
                j += 1;
            }
            match close {
                Some(j) => {
                    out.push_str(&text[copied..i]);
                    out.push('<');
                    out.push_str(tag);
                    out.push('>');
                    out.push_str(&text[i + 1..j]);
                    out.push_str("</");
                    out.push_str(tag);
                    out.push('>');
                    copied = j + 1;
                    i = j + 1;
                }
                None => i += 1,
            }
        }
        out.push_str(&text[copied..]);
        out
    }

    pub fn markdown_to_telegram_html(text: &str) -> String {
        if text.is_empty() {
            return text.to_string();
        }

        let mut placeholders: Vec<String> = Vec::new();

        // ```lang\ncode```
        let re_code = RegexBuilder::new(r"```(\w*)\n?(.*?)```")
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        let text = re_code
            .replace_all(text, |caps: &regex::Captures| {
                let lang = caps.get(1).map(|m| m.as_str()).unwrap_or("").trim();
                let code = escape_html(caps.get(2).map(|m| m.as_str()).unwrap_or(""));
                if !lang.is_empty() {
                    ph(
                        &mut placeholders,
                        &format!(
                            "<pre><code class=\"language-{}\">{}</code></pre>",
                            escape_html(lang),
                            code
                        ),
                    )
                } else {
                    ph(&mut placeholders, &format!("<pre>{}</pre>", code))
                }
            })
            .to_string();

        // `inline code`
        let re_inline = Regex::new(r"`([^`\n]+)`").unwrap();
        let text = re_inline
            .replace_all(&text, |caps: &regex::Captures| {
                ph(
                    &mut placeholders,
                    &format!("<code>{}</code>", escape_html(caps.get(1).unwrap().as_str())),
                )
            })
            .to_string();

        // [label](url)
        let re_link = Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").unwrap();
        let text = re_link
            .replace_all(&text, |caps: &regex::Captures| {
                let url = caps
                    .get(2)
                    .unwrap()
                    .as_str()
                    .replace('<', "%3C")
                    .replace('>', "%3E");
                ph(
                    &mut placeholders,
                    &format!(
                        "<a href=\"{}\">{}</a>",
                        url,
                        escape_html(caps.get(1).unwrap().as_str())
                    ),
                )
            })
            .to_string();

        let mut text = escape_html(&text);

        let re_hr = RegexBuilder::new(r"^[\*\-_]{3,}\s*$")
            .multi_line(true)
            .build()
            .unwrap();
        text = re_hr.replace_all(&text, "———").to_string();

        let re_head = RegexBuilder::new(r"^#{1,6}\s+(.+?)$")
            .multi_line(true)
            .build()
            .unwrap();
        text = re_head.replace_all(&text, "<b>$1</b>").to_string();

        // Blockquote grouping ("> " lines become one <blockquote>).
        let mut result_lines: Vec<String> = Vec::new();
        let mut quote_buf: Vec<String> = Vec::new();
        for line in text.split('\n') {
            let stripped = line.trim_start();
            if let Some(rest) = stripped.strip_prefix("&gt; ") {
                quote_buf.push(rest.to_string());
            } else if stripped == "&gt;" {
                quote_buf.push(String::new());
            } else {
                if !quote_buf.is_empty() {
                    result_lines.push(format!("<blockquote>{}</blockquote>", quote_buf.join("\n")));
                    quote_buf.clear();
                }
                result_lines.push(line.to_string());
            }
        }
        if !quote_buf.is_empty() {
            result_lines.push(format!("<blockquote>{}</blockquote>", quote_buf.join("\n")));
        }
        let mut text = result_lines.join("\n");

        let re_bullet = RegexBuilder::new(r"^(\s*)[\*\-]\s+")
            .multi_line(true)
            .build()
            .unwrap();
        text = re_bullet.replace_all(&text, "$1• ").to_string();

        text = Regex::new(r"\|\|(.+?)\|\|")
            .unwrap()
            .replace_all(&text, "<tg-spoiler>$1</tg-spoiler>")
            .to_string();
        text = Regex::new(r"\*{3}(.+?)\*{3}")
            .unwrap()
            .replace_all(&text, "<b><i>$1</i></b>")
            .to_string();
        text = Regex::new(r"\*{2}(.+?)\*{2}")
            .unwrap()
            .replace_all(&text, "<b>$1</b>")
            .to_string();
        text = Regex::new(r"__(.+?)__")
            .unwrap()
            .replace_all(&text, "<b>$1</b>")
            .to_string();

        text = replace_italic(&text, b'*', "i");
        text = replace_italic(&text, b'_', "i");

        text = Regex::new(r"~~(.+?)~~")
            .unwrap()
            .replace_all(&text, "<s>$1</s>")
            .to_string();

        for (idx, content) in placeholders.iter().enumerate() {
            text = text.replace(&format!("\u{0}PH{}\u{0}", idx), content);
        }
        text
    }
}

// ===========================================================================
// Config
// ===========================================================================

fn default_true() -> bool {
    true
}

/// Deserialize `allowed_user_ids` the way `TelegramConfig.from_dict`
/// does: accept a list or a comma/whitespace-separated string, require
/// ASCII-digit IDs, and de-duplicate preserving order.
fn de_allowed_user_ids<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    let value = Option::<Value>::deserialize(deserializer)?;
    let raw: Vec<String> = match value {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => {
            let s = s.trim();
            if s.is_empty() {
                Vec::new()
            } else {
                s.split(|c: char| c == ',' || c.is_whitespace())
                    .filter(|part| !part.is_empty())
                    .map(|part| part.to_string())
                    .collect()
            }
        }
        Some(Value::Array(arr)) => arr
            .iter()
            .map(|item| match item {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect(),
        Some(_) => {
            return Err(D::Error::custom(
                "allowed_user_ids: expected Telegram user IDs separated by commas or whitespace",
            ))
        }
    };
    if raw
        .iter()
        .any(|item| !item.is_ascii() || !item.chars().all(|c| c.is_ascii_digit()))
    {
        return Err(D::Error::custom(
            "allowed_user_ids: expected Telegram user IDs separated by commas or whitespace",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(raw.len());
    for item in raw {
        if seen.insert(item.clone()) {
            out.push(item);
        }
    }
    Ok(out)
}

/// Configuration for the Telegram Bot channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelegramConfig {
    #[serde(flatten)]
    pub base: crate::channel::ChannelConfig,
    /// Telegram Bot API token from @BotFather.
    pub bot_token: String,
    /// Optional HTTP proxy URL (used for both getUpdates and API calls).
    pub http_proxy: String,
    /// Send `typing` chat action while processing.
    #[serde(default = "default_true")]
    pub show_typing: bool,
    /// Telegram user IDs allowed to talk to the bot. Empty keeps the bot open.
    #[serde(deserialize_with = "de_allowed_user_ids")]
    pub allowed_user_ids: Vec<String>,
}

impl Default for TelegramConfig {
    fn default() -> Self {
        TelegramConfig {
            base: Default::default(),
            bot_token: String::new(),
            http_proxy: String::new(),
            show_typing: true,
            allowed_user_ids: Vec::new(),
        }
    }
}

crate::channel_adapter! {
    /// Telegram Bot channel.
    TelegramChannel, TelegramConfig, "telegram",
    required = [bot_token],
    fields = {
        poll_task: Option<tokio::task::JoinHandle<()>>,
        http: Option<reqwest::Client>
    }
}

// ===========================================================================
// JSON helpers
// ===========================================================================

fn json_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// truthiness for `message_thread_id` (`None` / `0` / `""` are falsy).
fn truthy_thread(v: &Value) -> Option<Value> {
    match v {
        Value::Null => None,
        Value::String(s) if s.is_empty() => None,
        Value::Number(n) if n.as_i64() == Some(0) => None,
        Value::Bool(false) => None,
        other => Some(other.clone()),
    }
}

/// Telegram accepts `chat_id` as an integer or string. Send numeric ids as
/// integers to match the `int(chat_id)` call.
fn chat_id_value(chat_id: &str) -> Value {
    match chat_id.parse::<i64>() {
        Ok(n) => json!(n),
        Err(_) => json!(chat_id),
    }
}

/// Split `text` into `TG_SEND_CHUNK_SIZE`-character pieces (slices by
/// code point, not byte).
fn chunk_text(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= TG_SEND_CHUNK_SIZE {
        return vec![text.to_string()];
    }
    let mut chunks = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let end = (i + TG_SEND_CHUNK_SIZE).min(chars.len());
        chunks.push(chars[i..end].iter().collect());
        i = end;
    }
    chunks
}

// ===========================================================================
// Bot API helpers (free fns; used by &self / &mut self methods and the loop)
// ===========================================================================

/// Call a Bot API method with a JSON body; returns `true` when the response is
/// a success (`ok == true`).
async fn api_call(http: &reqwest::Client, token: &str, method: &str, body: &Value) -> bool {
    let url = format!("{}{}/{}", TG_API, token, method);
    match http.post(&url).json(body).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                tracing::warn!("Telegram {} failed: {}", method, text);
                return false;
            }
            match resp.json::<Value>().await {
                Ok(v) => v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false),
                Err(_) => true,
            }
        }
        Err(e) => {
            tracing::warn!("Telegram {} request failed: {}", method, e);
            false
        }
    }
}

/// Multipart upload of inline media (`sendPhoto` / `sendVideo` / ... ).
#[allow(clippy::too_many_arguments)]
async fn upload_media(
    http: &reqwest::Client,
    token: &str,
    method: &str,
    chat_id: &str,
    field: &str,
    filename: &str,
    mime: &str,
    data: Vec<u8>,
    thread_id: Option<&Value>,
) -> bool {
    let url = format!("{}{}/{}", TG_API, token, method);
    let make_part = || reqwest::multipart::Part::bytes(data.clone()).file_name(filename.to_string());
    let part = match make_part().mime_str(mime) {
        Ok(p) => p,
        Err(_) => make_part(),
    };
    let mut form = reqwest::multipart::Form::new()
        .text("chat_id", chat_id.to_string())
        .part(field.to_string(), part);
    if let Some(t) = thread_id {
        form = form.text("message_thread_id", json_to_string(t).unwrap_or_default());
    }
    match http.post(&url).multipart(form).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                tracing::warn!("Telegram {} failed: {}", method, text);
                return false;
            }
            match resp.json::<Value>().await {
                Ok(v) => v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false),
                Err(_) => true,
            }
        }
        Err(e) => {
            tracing::warn!("Telegram {} upload failed: {}", method, e);
            false
        }
    }
}

/// Resolve a Telegram `file_id` into a download URL via `getFile`.
async fn resolve_file_url(http: &reqwest::Client, token: &str, file_id: &str) -> String {
    let url = format!("{}{}/getFile", TG_API, token);
    let body = json!({ "file_id": file_id });
    let resp = match http.post(&url).json(&body).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("Telegram getFile failed: {}", e);
            return String::new();
        }
    };
    let val: Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return String::new(),
    };
    if val.get("ok").and_then(|x| x.as_bool()) != Some(true) {
        return String::new();
    }
    let file_path = val
        .get("result")
        .and_then(|r| r.get("file_path"))
        .and_then(|x| x.as_str())
        .unwrap_or("");
    if file_path.starts_with("http") {
        return file_path.to_string();
    }
    if !file_path.is_empty() {
        return format!("{}{}/{}", TG_FILE_API, token, file_path);
    }
    String::new()
}

/// `_is_allowed_sender`: empty allow-list keeps the bot open to everyone.
fn is_allowed_sender(allowed: &[String], user_id: &str) -> bool {
    allowed.is_empty() || allowed.iter().any(|u| u == user_id)
}

/// Build the native payload for one Telegram `message` object.
async fn build_native_from_message(
    http: &reqwest::Client,
    token: &str,
    message: &Value,
) -> Option<Value> {
    let mut content: Vec<Value> = Vec::new();
    let mut file_ids: Vec<String> = Vec::new();

    let text = message
        .get("text")
        .and_then(|x| x.as_str())
        .or_else(|| message.get("caption").and_then(|x| x.as_str()))
        .unwrap_or("")
        .trim()
        .to_string();
    if !text.is_empty() {
        content.push(json!({ "type": "text", "text": text }));
    }

    for attr in ["photo", "document", "video", "voice", "audio"] {
        let media = match message.get(attr) {
            Some(m) if !m.is_null() => m,
            _ => continue,
        };
        let file_obj = if attr == "photo" {
            match media.as_array().and_then(|a| a.last()) {
                Some(o) => o.clone(),
                None => continue,
            }
        } else {
            media.clone()
        };
        let file_id = file_obj
            .get("file_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let filename = file_obj
            .get("file_name")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| attr.to_string());
        let mut url = String::new();
        if !file_id.is_empty() {
            url = resolve_file_url(http, token, &file_id).await;
        }
        let part = match attr {
            "photo" => json!({ "type": "image", "url": url, "alt_text": filename }),
            "video" => json!({ "type": "video", "url": url }),
            "voice" | "audio" => json!({ "type": "audio", "url": url }),
            _ => json!({ "type": "file", "url": url, "filename": filename }),
        };
        content.push(part);
        file_ids.push(file_id);
    }

    if content.is_empty() {
        return None;
    }

    let chat = message.get("chat");
    let chat_id = chat
        .and_then(|c| c.get("id"))
        .and_then(json_to_string)
        .unwrap_or_default();
    let user_id = message
        .get("from")
        .and_then(|f| f.get("id"))
        .and_then(json_to_string)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| chat_id.clone());
    let is_group = matches!(
        chat.and_then(|c| c.get("type")).and_then(|t| t.as_str()),
        Some("group") | Some("supergroup")
    );
    let thread_id = message
        .get("message_thread_id")
        .cloned()
        .unwrap_or(Value::Null);

    Some(json!({
        "chat_id": chat_id,
        "user_id": user_id,
        "message_id": message.get("message_id").and_then(json_to_string).unwrap_or_default(),
        "is_group": is_group,
        "message_thread_id": thread_id,
        "content": content,
        "file_ids": file_ids,
    }))
}

/// Long-polling `getUpdates` loop. Long-polling updater.
async fn poll_updates(http: reqwest::Client, token: String, allowed: Vec<String>, cb: EnqueueFn) {
    let mut offset: i64 = 0;
    loop {
        let url = format!("{}{}/getUpdates", TG_API, token);
        let body = json!({
            "offset": offset,
            "timeout": 30,
            "allowed_updates": ["message", "edited_message"],
        });
        let resp = match http.post(&url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Telegram getUpdates failed: {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        let val: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Telegram getUpdates decode failed: {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if val.get("ok").and_then(|x| x.as_bool()) != Some(true) {
            tracing::warn!("Telegram getUpdates error: {}", val);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        let updates = val
            .get("result")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        for update in updates {
            if let Some(uid) = update.get("update_id").and_then(|x| x.as_i64()) {
                if uid + 1 > offset {
                    offset = uid + 1;
                }
            }
            let message = match update.get("message").or_else(|| update.get("edited_message")) {
                Some(m) if !m.is_null() => m,
                _ => continue,
            };
            let user_id = message
                .get("from")
                .and_then(|f| f.get("id"))
                .and_then(json_to_string)
                .unwrap_or_default();
            if !is_allowed_sender(&allowed, &user_id) {
                tracing::info!(
                    "Telegram message from user {} ignored: not in allowed_user_ids",
                    if user_id.is_empty() { "unknown" } else { &user_id }
                );
                continue;
            }
            if let Some(native) = build_native_from_message(&http, &token, message).await {
                cb(RawPayload::Json(native));
            }
        }
    }
}

// ===========================================================================
// Channel implementation
// ===========================================================================

impl TelegramChannel {
    /// Build an HTTP client honoring `http_proxy` (shared by polling + sends).
    fn build_http(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder();
        if !self.config.http_proxy.is_empty() {
            builder = builder.proxy(reqwest::Proxy::all(&self.config.http_proxy)?);
        }
        Ok(builder.build()?)
    }

    async fn http(&self) -> Result<reqwest::Client> {
        if let Some(c) = &self.http {
            return Ok(c.clone());
        }
        self.core.ensure_http().await
    }

    /// `chat_id = metadata["chat_id"] or subject_id`.
    fn chat_id_of(&self, subject: &ChannelSubject) -> String {
        subject
            .metadata
            .get("chat_id")
            .and_then(json_to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| subject.subject_id.clone())
    }

    fn thread_id_of(subject: &ChannelSubject) -> Option<Value> {
        subject
            .metadata
            .get("message_thread_id")
            .and_then(truthy_thread)
    }

    fn media_label(media: &ContentPart) -> String {
        match media {
            ContentPart::File(f) if !f.filename.is_empty() => format!("File: {}", f.filename),
            other => other.label().to_string(),
        }
    }

    fn media_endpoint(media: &ContentPart) -> (&'static str, &'static str) {
        match media {
            ContentPart::Image(_) => ("sendPhoto", "photo"),
            ContentPart::Video(_) => ("sendVideo", "video"),
            ContentPart::Audio(_) => ("sendAudio", "audio"),
            _ => ("sendDocument", "document"),
        }
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            send_rate_limit: Some((20, 60.0)),
            typing_keepalive_interval: 4.0,
            show_thinking: false,
            show_tool_hints: true,
            ..Default::default()
        }
    }
    fn configure_constraints(&self, c: &mut crate::constraints::ChannelConstraints) {
        // `_default_constraints(self)` reads `self._config.show_typing`.
        c.typing_keepalive_interval = if self.config.show_typing { 4.0 } else { 0.0 };
    }

    async fn start(&mut self) -> Result<()> {
        if self.config.bot_token.is_empty() {
            return Err(GatewayError::Other(
                "TelegramChannel: bot_token is required".into(),
            ));
        }

        let http = self.build_http()?;
        self.http = Some(http.clone());

        let session = format!("telegram-{}", &uuid::Uuid::new_v4().to_string()[..12]);
        self.core.set_connection_session(Some(session));

        // Probe the bot (SDK `initialize()`); non-fatal on failure.
        let me_url = format!("{}{}/getMe", TG_API, self.config.bot_token);
        if let Ok(resp) = http.post(&me_url).send().await {
            if let Ok(v) = resp.json::<Value>().await {
                let username = v
                    .get("result")
                    .and_then(|r| r.get("username"))
                    .and_then(|u| u.as_str())
                    .unwrap_or("");
                tracing::info!("Telegram getMe ok username={}", username);
            }
        }

        if let Some(cb) = self.core.enqueue_sender() {
            let token = self.config.bot_token.clone();
            let allowed = self.config.allowed_user_ids.clone();
            let handle = tokio::spawn(async move {
                poll_updates(http, token, allowed, cb).await;
            });
            self.poll_task = Some(handle);
        }

        tracing::info!("TelegramChannel started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(handle) = self.poll_task.take() {
            handle.abort();
        }
        self.http = None;
        self.core.set_connection_session(None);
        tracing::info!("TelegramChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        let http = self.http().await?;
        let token = self.config.bot_token.clone();
        let chat_id = self.chat_id_of(subject);
        let thread_id = Self::thread_id_of(subject);

        let html = format_html::markdown_to_telegram_html(text);
        for chunk in chunk_text(&html) {
            let mut body = json!({
                "chat_id": chat_id_value(&chat_id),
                "text": chunk,
            });
            if let Some(t) = &thread_id {
                body["message_thread_id"] = t.clone();
            }
            let mut html_body = body.clone();
            html_body["parse_mode"] = json!("HTML");

            if !api_call(&http, &token, "sendMessage", &html_body).await {
                // Retry without parse_mode (plain text).
                if !api_call(&http, &token, "sendMessage", &body).await {
                    tracing::error!("Telegram send_text failed chat_id={}", chat_id);
                }
            }
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let http = self.http().await?;
        let token = self.config.bot_token.clone();
        let chat_id = self.chat_id_of(subject);
        let thread_id = Self::thread_id_of(subject);
        let label = Self::media_label(media);
        let (method, field) = Self::media_endpoint(media);

        let url = media.url().unwrap_or("").to_string();
        let has_inline = media
            .data()
            .map(|s| !s.is_empty())
            .unwrap_or(false)
            || media
                .local_path()
                .map(|s| !s.is_empty())
                .unwrap_or(false);

        if url.is_empty() && has_inline {
            return match self.load_media_bytes(media).await {
                Ok((data, mime)) => {
                    if data.len() > TG_MAX_FILE_SIZE_BYTES {
                        return self.send_text(subject, &format!("[{}: file too large]", label)).await;
                    }
                    let filename = media
                        .filename()
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| {
                            let ext = mime.rsplit('/').next().unwrap_or("bin");
                            format!("file.{}", ext)
                        });
                    let ok = upload_media(
                        &http,
                        &token,
                        method,
                        &chat_id,
                        field,
                        &filename,
                        &mime,
                        data,
                        thread_id.as_ref(),
                    )
                    .await;
                    if ok {
                        Ok(())
                    } else {
                        // Upload failure shares the fail-soft boundary.
                        self.send_text(subject, &format!("[{} (upload failed)]", label))
                            .await
                    }
                }
                Err(e) => {
                    tracing::error!("Telegram local media upload failed: {}", e);
                    self.send_text(subject, &format!("[{} (upload failed)]", label))
                        .await
                }
            };
        }

        if !url.is_empty() {
            let mut body = json!({
                "chat_id": chat_id_value(&chat_id),
                field: url,
            });
            if let Some(t) = &thread_id {
                body["message_thread_id"] = t.clone();
            }
            let _ = api_call(&http, &token, method, &body).await;
        } else if has_inline {
            self.send_text(subject, &format!("[{} (not deliverable)]", label))
                .await?;
        }
        Ok(())
    }

    async fn send_typing_indicator(&mut self, subject: &ChannelSubject) -> Result<()> {
        if !self.config.show_typing || self.config.bot_token.is_empty() {
            return Ok(());
        }
        let http = match self.http().await {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        let chat_id = self.chat_id_of(subject);
        let body = json!({
            "chat_id": chat_id_value(&chat_id),
            "action": "typing",
        });
        let _ = api_call(&http, &self.config.bot_token, "sendChatAction", &body).await;
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }

        let data = match raw.as_json() {
            Some(v) => v.clone(),
            None => Value::Object(serde_json::Map::new()),
        };

        let chat_id = data
            .get("chat_id")
            .and_then(json_to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        let user_id = data
            .get("user_id")
            .and_then(json_to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| chat_id.clone());
        let is_group = data.get("is_group").and_then(|x| x.as_bool()).unwrap_or(false);

        let content: Vec<ContentPart> = match data.get("content") {
            Some(v) if v.is_array() => serde_json::from_value(v.clone()).unwrap_or_default(),
            _ => Vec::new(),
        };

        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("chat_id".to_string(), json!(chat_id));
        metadata.insert("user_id".to_string(), json!(user_id));
        metadata.insert(
            "message_id".to_string(),
            data.get("message_id").cloned().unwrap_or(json!("")),
        );
        metadata.insert(
            "message_thread_id".to_string(),
            data.get("message_thread_id").cloned().unwrap_or(json!(null)),
        );
        metadata.insert("is_group".to_string(), json!(is_group));
        // Telegram `file_id`s captured during polling (extra metadata, additive).
        if let Some(fids) = data.get("file_ids") {
            if fids.is_array() {
                metadata.insert("file_ids".to_string(), fids.clone());
            }
        }

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "telegram".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id: chat_id.clone(),
                display_name: user_id.clone(),
                chat_type: if is_group { "group" } else { "direct" }.to_string(),
                metadata: metadata.clone(),
                first_seen: None,
                last_seen: None,
            }),
            channel_session_id: self
                .core
                .connection_session()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}-unconnected", self.core.channel_id())),
            content,
            metadata,
            timestamp: crate::channel::now_f64(),
            group_context: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::format_html::markdown_to_telegram_html;
    use super::TelegramConfig;

    #[test]
    fn formatter_basic_rules() {
        assert_eq!(markdown_to_telegram_html(""), "");
        assert_eq!(markdown_to_telegram_html("a & b < c"), "a &amp; b &lt; c");
        assert_eq!(markdown_to_telegram_html("**bold**"), "<b>bold</b>");
        assert_eq!(markdown_to_telegram_html("__bold__"), "<b>bold</b>");
        assert_eq!(markdown_to_telegram_html("~~gone~~"), "<s>gone</s>");
        assert_eq!(markdown_to_telegram_html("||spoiler||"), "<tg-spoiler>spoiler</tg-spoiler>");
        assert_eq!(markdown_to_telegram_html("`x < y`"), "<code>x &lt; y</code>");
        assert_eq!(
            markdown_to_telegram_html("[a](http://e.com/<q>)"),
            "<a href=\"http://e.com/%3Cq%3E\">a</a>"
        );
        assert_eq!(
            markdown_to_telegram_html("```rust\nlet a = 1;\n```"),
            "<pre><code class=\"language-rust\">let a = 1;\n</code></pre>"
        );
        assert_eq!(markdown_to_telegram_html("# Title"), "<b>Title</b>");
        assert_eq!(markdown_to_telegram_html("---"), "———");
        assert_eq!(markdown_to_telegram_html("- item"), "• item");
        assert_eq!(markdown_to_telegram_html("> q1\n> q2"), "<blockquote>q1\nq2</blockquote>");
        // italic lookbehind/lookahead: word-adjacent asterisks are NOT italic.
        assert_eq!(markdown_to_telegram_html("*i*"), "<i>i</i>");
        assert_eq!(markdown_to_telegram_html("a*b*c"), "a*b*c");
        assert_eq!(markdown_to_telegram_html("_i_"), "<i>i</i>");
    }

    #[test]
    fn config_defaults_and_allowlist() {
        let cfg = TelegramConfig::default();
        assert!(cfg.show_typing);
        assert!(cfg.allowed_user_ids.is_empty());
        let json = serde_json::json!({ "bot_token": "t", "allowed_user_ids": "1, 2 2 3" });
        let cfg: TelegramConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.allowed_user_ids, vec!["1", "2", "3"]);
        assert!(cfg.show_typing);
        assert!(serde_json::from_value::<TelegramConfig>(
            serde_json::json!({ "bot_token": "t", "allowed_user_ids": ["12x"] })
        )
        .is_err());
    }
}
