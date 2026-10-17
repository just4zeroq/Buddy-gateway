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
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::channel::{Channel, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{ChannelSubject, ContentPart, InboundMessage, RawPayload};

pub mod delivery;
pub mod interaction;
pub mod policy;

const TG_API: &str = "https://api.telegram.org/bot";
const TG_FILE_API: &str = "https://api.telegram.org/file/bot";
const TG_MAX_FILE_SIZE_BYTES: usize = 50 * 1024 * 1024;
/// Telegram counts `sendMessage` length in UTF-16 code units.
const TG_UTF16_LIMIT: usize = 4000;

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

    use crate::errors::{GatewayError, Result};

    fn escape_html(text: &str) -> String {
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    /// `html.escape(value, quote=True)` — also escapes quotes.
    fn escape_attr(value: &str) -> String {
        escape_html(value)
            .replace('"', "&quot;")
            .replace('\'', "&#x27;")
    }

    /// Telegram counts message length in UTF-16 code units.
    fn utf16_units(text: &str) -> usize {
        text.chars()
            .map(|c| if (c as u32) > 0xFFFF { 2 } else { 1 })
            .sum()
    }

    /// Decode the named/numeric entities Telegram HTML can carry.
    fn decode_entities(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(pos) = rest.find('&') {
            out.push_str(&rest[..pos]);
            let tail = &rest[pos..];
            let end = tail.find(';');
            match end {
                Some(e) if e <= 10 => {
                    if let Some(decoded) = decode_entity(&tail[..=e]) {
                        out.push(decoded);
                        rest = &tail[e + 1..];
                        continue;
                    }
                }
                _ => {}
            }
            out.push('&');
            rest = &tail[1..];
        }
        out.push_str(rest);
        out
    }

    fn decode_entity(entity: &str) -> Option<char> {
        let body = entity.strip_prefix('&')?.strip_suffix(';')?;
        match body {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            other => {
                let digits = other.strip_prefix('#')?;
                let code = if let Some(hex) = digits.strip_prefix(|c| c == 'x' || c == 'X') {
                    u32::from_str_radix(hex, 16).ok()?
                } else {
                    digits.parse::<u32>().ok()?
                };
                char::from_u32(code)
            }
        }
    }

    /// Re-serialize tag attributes the way Telegram expects them.
    fn render_attrs(attrs: &str) -> String {
        let mut out = String::new();
        let mut rest = attrs.trim();
        while !rest.is_empty() {
            match rest.find('=') {
                Some(p) => {
                    let name = rest[..p].trim();
                    let value_part = rest[p + 1..].trim_start();
                    if let Some(value) = value_part.strip_prefix('"') {
                        match value.find('"') {
                            Some(q) => {
                                out.push_str(&format!(
                                    " {}=\"{}\"",
                                    name,
                                    escape_attr(&decode_entities(&value[..q]))
                                ));
                                rest = value_part[q + 2..].trim_start();
                            }
                            None => {
                                out.push_str(&format!(" {}", name));
                                break;
                            }
                        }
                    } else {
                        let value: String =
                            value_part.chars().take_while(|c| !c.is_whitespace()).collect();
                        out.push_str(&format!(" {}=\"{}\"", name, escape_attr(&decode_entities(&value))));
                        rest = value_part[value.len()..].trim_start();
                    }
                }
                None => {
                    out.push_str(&format!(" {}", rest.trim_end()));
                    break;
                }
            }
        }
        out
    }

    /// Collect decoded text with the formatting active at each position.
    ///
    /// Mirrors the `_HTMLRuns` parser: every run of visible text carries the
    /// open-tag/close-tag pairs that wrap it.
    pub fn html_runs(html: &str) -> Result<Vec<(String, Vec<(String, String)>)>> {
        let balanced = || GatewayError::Other("Expected balanced Telegram HTML".to_string());
        let mut runs: Vec<(String, Vec<(String, String)>)> = Vec::new();
        let mut stack: Vec<(String, String)> = Vec::new();
        let mut buf = String::new();
        let bytes = html.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'<' {
                let end = html[i..].find('>').map(|p| i + p).ok_or_else(balanced)?;
                let raw = html[i + 1..end].trim();
                if !buf.is_empty() {
                    runs.push((std::mem::take(&mut buf), stack.clone()));
                }
                if let Some(name) = raw.strip_prefix('/') {
                    let expected = format!("</{}>", name.trim());
                    match stack.last() {
                        Some((_, close)) if *close == expected => {
                            stack.pop();
                        }
                        _ => return Err(balanced()),
                    }
                } else if !raw.ends_with('/') {
                    let (name, attrs) = match raw.find(char::is_whitespace) {
                        Some(p) => (&raw[..p], raw[p..].trim()),
                        None => (raw, ""),
                    };
                    stack.push((
                        format!("<{}{}>", name, render_attrs(attrs)),
                        format!("</{}>", name),
                    ));
                }
                i = end + 1;
            } else if bytes[i] == b'&' {
                let mut pushed = false;
                if let Some(rel) = html[i..].find(';') {
                    if rel <= 10 {
                        if let Some(decoded) = decode_entity(&html[i..=i + rel]) {
                            buf.push(decoded);
                            i += rel + 1;
                            pushed = true;
                        }
                    }
                }
                if !pushed {
                    buf.push('&');
                    i += 1;
                }
            } else {
                let c = html[i..].chars().next().unwrap_or('&');
                buf.push(c);
                i += c.len_utf8();
            }
        }
        if !buf.is_empty() {
            runs.push((buf, stack.clone()));
        }
        if !stack.is_empty() {
            return Err(balanced());
        }
        Ok(runs)
    }

    /// Split formatter-produced HTML into balanced HTML / plain-text pairs.
    ///
    /// Both the serialized HTML (including reopened tags and escaped entities)
    /// and the decoded text fit within `limit` UTF-16 code units. Characters and
    /// entities stay intact. Formatting whose wrappers cannot fit alongside a
    /// character is omitted for that character; its visible text is always
    /// retained. The input must be balanced Telegram HTML, as emitted by
    /// [`markdown_to_telegram_html`]. Empty input produces no messages.
    pub fn split_telegram_html(html: &str, limit: usize) -> Result<Vec<(String, String)>> {
        if !(6..=4000).contains(&limit) {
            return Err(GatewayError::Other(
                "limit must be between 6 and 4000 UTF-16 code units".to_string(),
            ));
        }
        let runs = html_runs(html)?;
        let mut chunks: Vec<(String, String)> = Vec::new();

        let flush = |parts: &mut Vec<String>,
                     plain: &mut String,
                     active: &[(String, String)]|
         -> Option<(String, String)> {
            if plain.is_empty() {
                return None;
            }
            let mut serialized = parts.concat();
            for (_, close) in active.iter().rev() {
                serialized.push_str(close);
            }
            Some((serialized, plain.clone()))
        };

        let mut parts: Vec<String> = Vec::new();
        let mut plain = String::new();
        let mut active: Vec<(String, String)> = Vec::new();
        let mut size = 0usize;

        for (text, tags) in runs {
            let wrapper_size: usize = tags
                .iter()
                .map(|(start, close)| utf16_units(start) + utf16_units(close))
                .sum();
            for ch in text.chars() {
                let encoded = escape_html(&ch.to_string());
                let char_size = utf16_units(&encoded);
                let target: Vec<(String, String)> =
                    if wrapper_size + char_size <= limit { tags.clone() } else { Vec::new() };
                let mut common = 0usize;
                while common < active.len().min(target.len()) && active[common] == target[common] {
                    common += 1;
                }
                let mut transition = String::new();
                for (_, close) in active[common..].iter().rev() {
                    transition.push_str(close);
                }
                for (start, _) in target[common..].iter() {
                    transition.push_str(start);
                }
                let closing_size: usize = target.iter().map(|(_, close)| utf16_units(close)).sum();
                if size + utf16_units(&transition) + char_size + closing_size > limit {
                    if let Some(chunk) = flush(&mut parts, &mut plain, &active) {
                        chunks.push(chunk);
                    }
                    parts.clear();
                    plain.clear();
                    active.clear();
                    size = 0;
                    transition = target.iter().map(|(start, _)| start.clone()).collect::<String>();
                }
                parts.push(transition.clone());
                parts.push(encoded);
                plain.push(ch);
                size += utf16_units(&transition) + char_size;
                active = target;
            }
        }
        if let Some(chunk) = flush(&mut parts, &mut plain, &active) {
            chunks.push(chunk);
        }
        Ok(chunks)
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

        let original = text;
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
                        escape_attr(&url),
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
        // Regex Markdown parsing can create crossed tags for overlapping
        // markers. Such input must still be deliverable without an exception
        // or lost text, so fall back to the fully escaped original.
        if html_runs(&text).is_err() {
            return escape_html(original);
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

/// Deserialize `allowed_chat_ids`: accept a list or a comma/whitespace-separated
/// string. Strict validation happens in [`TelegramConfig::normalize`], so a
/// negative (group) ID is accepted here and rejected only for user lists.
fn de_allowed_chat_ids<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
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
                "allowed_chat_ids: expected Telegram chat IDs separated by commas or whitespace",
            ))
        }
    };
    Ok(raw)
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
    /// Chat IDs allowed to talk to the bot. Empty keeps every chat open.
    #[serde(deserialize_with = "de_allowed_chat_ids")]
    pub allowed_chat_ids: Vec<String>,
    /// `always` | `mention` | `mention_or_reply` — how a group activates the bot.
    pub group_activation: String,
    /// Accept a bare command (no `@bot`) in restricted group modes.
    pub allow_unaddressed_commands: bool,
    /// Keep forum topics as separate subjects.
    pub topic_isolation: bool,
    /// Quote the inbound message when replying.
    pub reply_to_message: bool,
    /// Process edited messages (never commands).
    pub edited_messages: bool,
    /// Send without a notification sound.
    pub disable_notification: bool,
    /// Forbid forwarding/saving of sent content.
    pub protect_content: bool,
    /// Let Telegram render link previews.
    pub link_preview: bool,
    /// Send audio parts as voice messages.
    pub audio_as_voice: bool,
    /// Merge the members of one inbound `media_group` into a single turn, and
    /// send consecutive same-kind attachments back out as one album.
    pub albums: bool,
}

impl TelegramConfig {
    /// Normalize + validate the ID lists and enum-like fields.
    ///
    /// Mirrors `TelegramConfig.__post_init__`: bad values fail loudly at
    /// construction time rather than silently disabling the bot.
    pub fn normalize(&mut self) -> Result<()> {
        self.allowed_user_ids = policy::parse_id_list(&serde_json::json!(self.allowed_user_ids), false)?;
        self.allowed_chat_ids = policy::parse_id_list(&serde_json::json!(self.allowed_chat_ids), true)?;
        if !matches!(self.group_activation.as_str(), "always" | "mention" | "mention_or_reply") {
            return Err(GatewayError::Other("Invalid Telegram group_activation".to_string()));
        }
        Ok(())
    }
}

impl Default for TelegramConfig {
    fn default() -> Self {
        TelegramConfig {
            base: Default::default(),
            bot_token: String::new(),
            http_proxy: String::new(),
            show_typing: true,
            allowed_user_ids: Vec::new(),
            allowed_chat_ids: Vec::new(),
            group_activation: "always".to_string(),
            allow_unaddressed_commands: true,
            topic_isolation: false,
            reply_to_message: true,
            edited_messages: false,
            disable_notification: false,
            protect_content: false,
            link_preview: true,
            audio_as_voice: false,
            albums: false,
        }
    }
}

crate::channel_adapter! {
    /// Telegram Bot channel.
    TelegramChannel, TelegramConfig, "telegram",
    required = [bot_token],
    fields = {
        poll_task: Option<tokio::task::JoinHandle<()>>,
        http: Option<reqwest::Client>,
        interactions: interaction::Interactions,
        requests: std::sync::Arc<delivery::RequestScheduler>
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

// ===========================================================================
// Bot API helpers (free fns; used by &self / &mut self methods and the loop)
// ===========================================================================

/// Call a Bot API method with a JSON body; returns `true` when the response is
/// a success (`ok == true`).
async fn api_call(http: &reqwest::Client, token: &str, method: &str, body: &Value) -> bool {
    api_call_detail(http, token, method, body).await.is_ok()
}

/// Same as [`api_call`] but keeps the redacted error description, so a caller
/// can tell an entity-parsing failure (safe to resend as plain text) from any
/// other rejection.
async fn api_call_detail(
    http: &reqwest::Client,
    token: &str,
    method: &str,
    body: &Value,
) -> std::result::Result<Value, (Option<u16>, String)> {
    let url = format!("{}{}/{}", TG_API, token, method);
    match http.post(&url).json(body).send().await {
        Ok(resp) => {
            let status = Some(resp.status().as_u16());
            if !resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                let description = serde_json::from_str::<Value>(&text)
                    .ok()
                    .and_then(|v| v.get("description").and_then(|d| d.as_str()).map(|s| s.to_string()))
                    .unwrap_or(text);
                let message = delivery::sanitize_error_message(&description);
                tracing::warn!("Telegram {} failed: {}", method, message);
                return Err((status, message));
            }
            match resp.json::<Value>().await {
                Ok(v) => {
                    if v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
                        Ok(v)
                    } else {
                        let message = delivery::sanitize_error_message(
                            v.get("description").and_then(|d| d.as_str()).unwrap_or("unknown error"),
                        );
                        Err((status, message))
                    }
                }
                Err(e) => Err((status, format!("decode failed: {}", e))),
            }
        }
        Err(e) => {
            tracing::warn!("Telegram {} request failed: {}", method, e);
            Err((None, format!("request failed: {}", e)))
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
    let mut media_meta: Vec<Value> = Vec::new();

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
        // Telegram `PhotoSize` carries no `mime_type`; Bot API photos are JPEG.
        // Preserving it keeps the stored file from being renamed to `.png`.
        let mime = if attr == "photo" {
            "image/jpeg".to_string()
        } else {
            file_obj
                .get("mime_type")
                .and_then(|m| m.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "application/octet-stream".to_string())
        };
        if !file_id.is_empty() {
            media_meta.push(json!({
                "kind": attr,
                "file_id": file_id,
                "file_unique_id": file_obj.get("file_unique_id").and_then(json_to_string).unwrap_or_default(),
                "filename": filename,
                "mime_type": mime,
                "size": file_obj.get("file_size").and_then(|s| s.as_u64()),
            }));
        }
        let part = match attr {
            "photo" => json!({ "type": "image", "url": url, "alt_text": filename, "mime_type": mime }),
            "video" => json!({ "type": "video", "url": url, "mime_type": mime }),
            "voice" | "audio" => json!({ "type": "audio", "url": url, "mime_type": mime }),
            _ => json!({ "type": "file", "url": url, "filename": filename, "mime_type": mime }),
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
    // An anonymous group sender (sender_chat) has no user identity to bind.
    let user_id = if message.get("sender_chat").is_some() {
        String::new()
    } else {
        message
            .get("from")
            .and_then(|f| f.get("id"))
            .and_then(json_to_string)
            .unwrap_or_default()
    };
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
        "source_message_id": message.get("message_id").and_then(json_to_string).unwrap_or_default(),
        "message_id": message.get("message_id").and_then(json_to_string).unwrap_or_default(),
        "reply_to_message_id": message.get("reply_to_message").and_then(|r| r.get("message_id")).and_then(json_to_string),
        "is_group": is_group,
        "message_thread_id": thread_id,
        "content": content,
        "media": media_meta,
        "file_ids": file_ids,
    }))
}

/// Bounded FIFO set used for update/message de-duplication.
///
/// Independent windows: update churn cannot evict source message IDs.
struct BoundedSet {
    seen: std::collections::VecDeque<String>,
    cap: usize,
}

impl BoundedSet {
    fn new(cap: usize) -> Self {
        BoundedSet {
            seen: std::collections::VecDeque::new(),
            cap,
        }
    }

    /// Record `key`; returns `true` when it was already present.
    fn is_duplicate(&mut self, key: &str) -> bool {
        if self.seen.iter().any(|k| k == key) {
            return true;
        }
        self.seen.push_back(key.to_string());
        while self.seen.len() > self.cap {
            self.seen.pop_front();
        }
        false
    }

    /// Membership test that does not extend the window (edit suppression).
    fn contains(&self, key: &str) -> bool {
        self.seen.iter().any(|k| k == key)
    }
}

/// One inbound album: chat + topic + media group + sender.
///
/// The sender is part of the identity so activation can come from any member's
/// caption but never merges members from two different users.
type AlbumKey = (String, String, String, String);

/// Members buffered while waiting for one that activates the bot.
///
/// FIFO with a deadline per key, so a group that never activates is dropped
/// instead of being held forever.
struct PassiveAlbums {
    entries: VecDeque<(AlbumKey, Instant, Vec<Value>)>,
}

impl PassiveAlbums {
    fn new() -> Self {
        PassiveAlbums {
            entries: VecDeque::new(),
        }
    }

    fn expire(&mut self, now: Instant) {
        while self.entries.front().map(|e| e.1 <= now).unwrap_or(false) {
            self.entries.pop_front();
        }
    }

    /// Entry for `key`, created with a fresh deadline when absent.
    fn entry(&mut self, key: &AlbumKey, now: Instant) -> &mut (AlbumKey, Instant, Vec<Value>) {
        if let Some(idx) = self.entries.iter().position(|e| e.0 == *key) {
            return &mut self.entries[idx];
        }
        self.entries
            .push_back((key.clone(), now + Duration::from_millis(350), Vec::new()));
        while self.entries.len() > 100 {
            self.entries.pop_front();
        }
        let last = self.entries.len() - 1;
        &mut self.entries[last]
    }

    fn take(&mut self, key: &AlbumKey) -> Option<(Instant, Vec<Value>)> {
        let idx = self.entries.iter().position(|e| e.0 == *key)?;
        self.entries.remove(idx).map(|(_, expires, buffered)| (expires, buffered))
    }
}

fn album_key(message: &Value, group_id: &str) -> AlbumKey {
    let chat_id = message
        .get("chat")
        .and_then(|c| c.get("id"))
        .and_then(json_to_string)
        .unwrap_or_default();
    let thread_id = message
        .get("message_thread_id")
        .and_then(json_to_string)
        .unwrap_or_default();
    let sender = if message.get("sender_chat").is_some() {
        message.get("sender_chat")
    } else {
        message.get("from")
    };
    let sender_id = sender
        .and_then(|s| s.get("id"))
        .and_then(json_to_string)
        .unwrap_or_default();
    (chat_id, thread_id, group_id.to_string(), sender_id)
}

/// Append a member, dropping it once an album hits Telegram's 10-item cap.
fn push_album_member(active: &mut HashMap<AlbumKey, Vec<Value>>, key: &AlbumKey, message: Value) {
    let entry = active.entry(key.clone()).or_default();
    if entry.len() < 10 {
        entry.push(message);
    } else {
        tracing::warn!("Telegram album member limit reached; dropping member");
    }
}

/// Deliver one collected album as a single inbound turn.
///
/// Members are ordered by `message_id` and each one is admitted on its own;
/// activation can come from any caption, but a rejected sender is never
/// carried along by an accepted one.
#[allow(clippy::too_many_arguments)]
async fn flush_album(
    http: reqwest::Client,
    token: String,
    admission: Admission,
    bot_id: Option<i64>,
    bot_username: Option<String>,
    seen_messages: Arc<tokio::sync::Mutex<BoundedSet>>,
    active: Arc<tokio::sync::Mutex<HashMap<AlbumKey, Vec<Value>>>>,
    cb: EnqueueFn,
    key: AlbumKey,
    delay: Duration,
) {
    tokio::time::sleep(delay).await;
    let mut members = active.lock().await.remove(&key).unwrap_or_default();
    members.sort_by_key(|m| m.get("message_id").and_then(|x| x.as_i64()).unwrap_or(0));
    if !members
        .iter()
        .any(|m| admission.accepts(m, bot_id, bot_username.as_deref()))
    {
        return;
    }

    let mut result: Option<Value> = None;
    for item in &members {
        if !policy::should_accept_message(
            item,
            bot_id,
            bot_username.as_deref(),
            "always",
            &admission.allowed_user_ids,
            &admission.allowed_chat_ids,
            admission.allow_unaddressed_commands,
        ) {
            continue;
        }
        let chat_id = item
            .get("chat")
            .and_then(|c| c.get("id"))
            .and_then(json_to_string)
            .unwrap_or_default();
        let message_id = item
            .get("message_id")
            .and_then(json_to_string)
            .unwrap_or_default();
        if seen_messages
            .lock()
            .await
            .is_duplicate(&format!("message:{}:{}", chat_id, message_id))
        {
            continue;
        }
        let Some(mut native) = build_native_from_message(&http, &token, item).await else {
            continue;
        };
        native["edited"] = json!(false);
        native["topic_isolation"] = json!(admission.topic_isolation);
        match result.as_mut() {
            None => result = Some(native),
            Some(acc) => {
                for field in ["content", "media", "file_ids"] {
                    let incoming = native.get(field).cloned().unwrap_or_else(|| json!([]));
                    if let (Some(dst), Value::Array(src)) = (
                        acc.get_mut(field).and_then(|v| v.as_array_mut()),
                        incoming,
                    ) {
                        dst.extend(src);
                    }
                }
            }
        }
    }
    if let Some(native) = result {
        cb(RawPayload::Json(native));
    }
}

/// Buffer, collect, and schedule one `media_group` member.
///
/// Returns `true` when the update was consumed as an album member (the caller
/// must then skip the normal single-message path).
#[allow(clippy::too_many_arguments)]
async fn collect_album_member(
    http: &reqwest::Client,
    token: &str,
    admission: &Admission,
    bot_id: Option<i64>,
    bot_username: Option<&str>,
    seen_messages: Arc<tokio::sync::Mutex<BoundedSet>>,
    active: Arc<tokio::sync::Mutex<HashMap<AlbumKey, Vec<Value>>>>,
    cb: &EnqueueFn,
    passive: &mut PassiveAlbums,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    message: &Value,
) -> bool {
    let Some(group_id) = message.get("media_group_id").and_then(json_to_string) else {
        return false;
    };
    if !admission.albums {
        return false;
    }
    // Identity, allowlists and command ownership are checked album-wide, so an
    // uncaptioned first member still survives until a caption arrives.
    if !policy::should_accept_message(
        message,
        bot_id,
        bot_username,
        "always",
        &admission.allowed_user_ids,
        &admission.allowed_chat_ids,
        admission.allow_unaddressed_commands,
    ) {
        return true;
    }
    let key = album_key(message, &group_id);
    let now = Instant::now();
    passive.expire(now);

    {
        let mut guard = active.lock().await;
        if !guard.contains_key(&key) {
            if !admission.accepts(message, bot_id, bot_username) {
                let entry = passive.entry(&key, now);
                if entry.2.len() < 10 {
                    entry.2.push(message.clone());
                }
                return true;
            }
            tasks.retain(|t| !t.is_finished());
            if tasks.len() >= 100 {
                tracing::warn!("Telegram pending album limit reached; dropping member");
                return true;
            }
            let (expires, buffered) = passive
                .take(&key)
                .unwrap_or((now + Duration::from_millis(350), Vec::new()));
            // Keep room for the activating caption even at the member cap.
            guard.insert(key.clone(), buffered.into_iter().take(9).collect());
            drop(guard);
            let handle = tokio::spawn(flush_album(
                http.clone(),
                token.to_string(),
                admission.clone(),
                bot_id,
                bot_username.map(|s| s.to_string()),
                seen_messages.clone(),
                active.clone(),
                cb.clone(),
                key.clone(),
                expires.saturating_duration_since(now),
            ));
            tasks.push(handle);
        }
    }

    let mut guard = active.lock().await;
    push_album_member(&mut guard, &key, message.clone());
    true
}

/// Admission + activation settings cloned into the polling task.
#[derive(Clone)]
struct Admission {
    allowed_user_ids: Vec<String>,
    allowed_chat_ids: Vec<String>,
    group_activation: String,
    allow_unaddressed_commands: bool,
    edited_messages: bool,
    topic_isolation: bool,
    /// Merge `media_group` members into one inbound turn.
    albums: bool,
}

impl Admission {
    fn from_config(cfg: &TelegramConfig) -> Self {
        Admission {
            allowed_user_ids: cfg.allowed_user_ids.clone(),
            allowed_chat_ids: cfg.allowed_chat_ids.clone(),
            group_activation: cfg.group_activation.clone(),
            allow_unaddressed_commands: cfg.allow_unaddressed_commands,
            edited_messages: cfg.edited_messages,
            topic_isolation: cfg.topic_isolation,
            albums: cfg.albums,
        }
    }

    /// Allowlists + command ownership + group activation, without I/O.
    fn accepts(&self, message: &Value, bot_id: Option<i64>, bot_username: Option<&str>) -> bool {
        let user_id = message
            .get("from")
            .and_then(|f| f.get("id"))
            .and_then(json_to_string)
            .unwrap_or_default();
        if !is_allowed_sender(&self.allowed_user_ids, &user_id) {
            tracing::info!(
                "Telegram message from user {} ignored: not in allowed_user_ids",
                if user_id.is_empty() { "unknown" } else { &user_id }
            );
            return false;
        }
        policy::should_accept_message(
            message,
            bot_id,
            bot_username,
            &self.group_activation,
            &self.allowed_user_ids,
            &self.allowed_chat_ids,
            self.allow_unaddressed_commands,
        )
    }
}

/// Long-polling `getUpdates` loop. Long-polling updater.
async fn poll_updates(
    http: reqwest::Client,
    token: String,
    admission: Admission,
    bot_id: Option<i64>,
    bot_username: Option<String>,
    interactions: interaction::Interactions,
    cb: EnqueueFn,
) {
    let mut offset: i64 = 0;
    // Bounded, independent de-duplication windows so update churn cannot evict
    // the source message IDs that edits are matched against. The message window
    // is shared with album flushes, which resolve members after a delay.
    let mut seen_updates = BoundedSet::new(1000);
    let seen_messages = Arc::new(tokio::sync::Mutex::new(BoundedSet::new(1000)));
    let active_albums: Arc<tokio::sync::Mutex<HashMap<AlbumKey, Vec<Value>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let mut passive_albums = PassiveAlbums::new();
    let mut album_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        let url = format!("{}{}/getUpdates", TG_API, token);
        let mut allowed_updates = vec![json!("message"), json!("callback_query")];
        if admission.edited_messages {
            allowed_updates.push(json!("edited_message"));
        }
        let body = json!({
            "offset": offset,
            "timeout": 30,
            "allowed_updates": allowed_updates,
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
                if seen_updates.is_duplicate(&format!("update:{}", uid)) {
                    continue;
                }
            }
            // Native button press: identity-bound, one-shot, never privileged.
            if let Some(query) = update.get("callback_query") {
                let query_id = query
                    .get("id")
                    .and_then(json_to_string)
                    .unwrap_or_default();
                let data = query
                    .get("data")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string();
                let user = query
                    .get("from")
                    .and_then(|f| f.get("id"))
                    .and_then(json_to_string)
                    .unwrap_or_default();
                let source = query.get("message").cloned().unwrap_or(Value::Null);
                let chat = source
                    .get("chat")
                    .and_then(|c| c.get("id"))
                    .and_then(json_to_string)
                    .unwrap_or_default();
                let topic = source.get("message_thread_id").and_then(|t| t.as_i64());
                let handled = interactions.invoke(&data, &user, &chat, topic).await;
                if !handled {
                    let _ = api_call(
                        &http,
                        &token,
                        "answerCallbackQuery",
                        &json!({"callback_query_id": query_id}),
                    )
                    .await;
                }
                continue;
            }

            let edited = update.get("edited_message").is_some();
            let message = match update.get("message").or_else(|| update.get("edited_message")) {
                Some(m) if !m.is_null() => m,
                _ => continue,
            };
            // Suppress edits while the source ID stays in its bounded cache.
            // Commands on previously unseen edited messages are rejected too.
            if edited {
                if !admission.edited_messages {
                    continue;
                }
                let text = message
                    .get("text")
                    .and_then(|t| t.as_str())
                    .or_else(|| message.get("caption").and_then(|t| t.as_str()))
                    .unwrap_or("");
                if text.trim_start().starts_with('/') {
                    continue;
                }
                let chat_id = message
                    .get("chat")
                    .and_then(|c| c.get("id"))
                    .and_then(json_to_string)
                    .unwrap_or_default();
                let message_id = message
                    .get("message_id")
                    .and_then(json_to_string)
                    .unwrap_or_default();
                if seen_messages
                    .lock()
                    .await
                    .contains(&format!("message:{}:{}", chat_id, message_id))
                {
                    continue;
                }
            }
            // Album members are collected first; the group is delivered as one
            // turn once its activation window closes.
            if collect_album_member(
                &http,
                &token,
                &admission,
                bot_id,
                bot_username.as_deref(),
                seen_messages.clone(),
                active_albums.clone(),
                &cb,
                &mut passive_albums,
                &mut album_tasks,
                message,
            )
            .await
            {
                continue;
            }
            if !admission.accepts(message, bot_id, bot_username.as_deref()) {
                continue;
            }
            // De-duplicate on the source IDs before any media retrieval.
            let chat_id = message
                .get("chat")
                .and_then(|c| c.get("id"))
                .and_then(json_to_string)
                .unwrap_or_default();
            let message_id = message
                .get("message_id")
                .and_then(json_to_string)
                .unwrap_or_default();
            if seen_messages
                .lock()
                .await
                .is_duplicate(&format!("message:{}:{}", chat_id, message_id))
            {
                continue;
            }
            if let Some(mut native) = build_native_from_message(&http, &token, message).await {
                native["edited"] = json!(edited);
                native["topic_isolation"] = json!(admission.topic_isolation);
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

    /// Host hook for authorized model/agent options or private approvals.
    ///
    /// The host must enforce its own application permissions before registering
    /// an action. Telegram identity or administrator status grants no
    /// privilege. Actions run inline in the serial update handler: apply short
    /// state changes or enqueue work and return — never await a whole agent
    /// turn here, or later updates (including Stop) stop being responsive.
    pub fn register_callback<F, Fut>(
        &self,
        subject: &ChannelSubject,
        action: F,
        ttl: Option<Duration>,
    ) -> String
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let user = subject
            .metadata
            .get("user_id")
            .and_then(json_to_string)
            .unwrap_or_default();
        let chat = self.chat_id_of(subject);
        let topic = Self::thread_id_of(subject).and_then(|t| t.as_i64());
        self.interactions.register(
            &user,
            &chat,
            topic,
            action,
            ttl.unwrap_or(interaction::DEFAULT_TTL),
        )
    }

    /// Render host-authorized model, agent, or approval choices as native buttons.
    pub async fn send_buttons(
        &mut self,
        subject: &ChannelSubject,
        text: &str,
        actions: Vec<(String, interaction::ActionFn)>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let http = self.http().await?;
        let token = self.config.bot_token.clone();
        let chat_id = self.chat_id_of(subject);
        let ttl = ttl.unwrap_or(interaction::DEFAULT_TTL);
        let mut tokens = Vec::with_capacity(actions.len());
        let mut keyboard = Vec::with_capacity(actions.len());
        for (label, action) in actions {
            let user = subject
                .metadata
                .get("user_id")
                .and_then(json_to_string)
                .unwrap_or_default();
            let topic = Self::thread_id_of(subject).and_then(|t| t.as_i64());
            let token_id =
                self.interactions
                    .register_boxed(&user, &chat_id, topic, action, ttl);
            keyboard.push(json!({"text": label, "callback_data": token_id.clone()}));
            tokens.push(token_id);
        }
        self.requests.acquire(Some(&chat_id)).await;
        let mut body = json!({
            "chat_id": chat_id_value(&chat_id),
            "text": text,
            "reply_markup": {"inline_keyboard": keyboard.iter().map(|b| vec![b.clone()]).collect::<Vec<_>>()},
        });
        for (key, value) in self.send_options(subject, true) {
            body[key] = value;
        }
        if api_call(&http, &token, "sendMessage", &body).await {
            return Ok(());
        }
        // The buttons never reached the user: drop their pending actions.
        for token_id in tokens {
            self.interactions.discard(&token_id);
        }
        Err(GatewayError::Other("Telegram send_buttons failed".to_string()))
    }

    /// Per-message delivery options shared by text and media sends.
    ///
    /// `text` adds the link-preview flag, which only `sendMessage` accepts.
    fn send_options(
        &self,
        subject: &ChannelSubject,
        text: bool,
    ) -> serde_json::Map<String, Value> {
        let mut options = serde_json::Map::new();
        options.insert(
            "disable_notification".to_string(),
            json!(self.config.disable_notification),
        );
        options.insert("protect_content".to_string(), json!(self.config.protect_content));
        if let Some(thread) = Self::thread_id_of(subject) {
            options.insert("message_thread_id".to_string(), thread);
        }
        if self.config.reply_to_message {
            let source = subject
                .metadata
                .get("source_message_id")
                .or_else(|| subject.metadata.get("message_id"))
                .and_then(json_to_string)
                .filter(|s| !s.is_empty());
            if let Some(id) = source.and_then(|s| s.parse::<i64>().ok()) {
                options.insert(
                    "reply_parameters".to_string(),
                    json!({"message_id": id, "allow_sending_without_reply": true}),
                );
            }
        }
        if text {
            options.insert(
                "link_preview_options".to_string(),
                json!({"is_disabled": !self.config.link_preview}),
            );
        }
        options
    }

    fn media_label(media: &ContentPart) -> String {
        match media {
            ContentPart::File(f) if !f.filename.is_empty() => format!("File: {}", f.filename),
            other => other.label().to_string(),
        }
    }

    fn media_endpoint(&self, media: &ContentPart) -> (&'static str, &'static str) {
        match media {
            ContentPart::Image(_) => ("sendPhoto", "photo"),
            ContentPart::Video(_) => ("sendVideo", "video"),
            // `audio_as_voice` sends audio parts as voice messages.
            ContentPart::Audio(_) if self.config.audio_as_voice => ("sendVoice", "voice"),
            ContentPart::Audio(_) => ("sendAudio", "audio"),
            _ => ("sendDocument", "document"),
        }
    }

    fn album_member_type(part: &ContentPart) -> &'static str {
        match part {
            ContentPart::Image(_) => "photo",
            ContentPart::Video(_) => "video",
            ContentPart::Audio(_) => "audio",
            _ => "document",
        }
    }

    /// Resolve an album attachment.
    ///
    /// `Err` means the part cannot be delivered as part of a group; the caller
    /// then degrades to individual sends, which preserve each attachment or
    /// emit its own unavailable marker.
    async fn album_source(&self, media: &ContentPart) -> Result<AlbumSource> {
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
            let (data, mime) = self.load_media_bytes(media).await?;
            if data.len() > TG_MAX_FILE_SIZE_BYTES {
                return Err(GatewayError::Other("Telegram upload exceeds 50 MB".into()));
            }
            let filename = media
                .filename()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    let ext = mime.rsplit('/').next().unwrap_or("bin");
                    format!("file.{}", ext)
                });
            return Ok(AlbumSource::Upload { data, mime, filename });
        }
        if !url.is_empty() {
            let resolved = if url.starts_with("//") {
                format!("https:{}", url)
            } else {
                url
            };
            return Ok(AlbumSource::Url(resolved));
        }
        Err(GatewayError::Other("Telegram attachment has no source".into()))
    }

    /// Drain the pending album in Telegram-sized batches (10 per group).
    async fn flush_album_batch(
        &mut self,
        subject: &ChannelSubject,
        pending: &mut Vec<ContentPart>,
    ) -> Result<()> {
        while !pending.is_empty() {
            let take = std::cmp::min(10, pending.len());
            let batch: Vec<ContentPart> = pending.drain(..take).collect();
            if batch.len() == 1 {
                self.send_media(subject, &batch[0]).await?;
                continue;
            }
            self.send_media_group(subject, &batch).await?;
        }
        Ok(())
    }

    /// Send same-kind attachments as one `sendMediaGroup` album.
    ///
    /// Uploads are attached as `attach://` parts; URL members are inlined in
    /// the `media` array. There is no fallback once a group request has been
    /// attempted, because its result is ambiguous.
    async fn send_media_group(
        &mut self,
        subject: &ChannelSubject,
        parts: &[ContentPart],
    ) -> Result<()> {
        let http = self.http().await?;
        let token = self.config.bot_token.clone();
        let chat_id = self.chat_id_of(subject);

        let mut media: Vec<Value> = Vec::new();
        let mut form = reqwest::multipart::Form::new().text("chat_id", chat_id_value(&chat_id).to_string());
        for (idx, part) in parts.iter().enumerate() {
            let kind = Self::album_member_type(part);
            match self.album_source(part).await {
                Ok(AlbumSource::Url(url)) => media.push(json!({ "type": kind, "media": url })),
                Ok(AlbumSource::Upload { data, mime, filename }) => {
                    let name = format!("attach{}", idx);
                    media.push(json!({ "type": kind, "media": format!("attach://{}", name) }));
                    let make = || {
                        reqwest::multipart::Part::bytes(data.clone()).file_name(filename.clone())
                    };
                    let field = match make().mime_str(&mime) {
                        Ok(p) => p,
                        Err(_) => make(),
                    };
                    form = form.part(name, field);
                }
                Err(e) => {
                    // Preflight failed before a group request: individual sends
                    // preserve each attachment or emit its unavailable marker.
                    tracing::warn!("Telegram album preflight failed: {}", e);
                    for item in parts {
                        self.send_media(subject, item).await?;
                    }
                    return Ok(());
                }
            }
        }
        form = form.text(
            "media",
            serde_json::to_string(&media).unwrap_or_else(|_| "[]".to_string()),
        );
        for (key, value) in self.send_options(subject, false) {
            form = form.text(
                key,
                json_to_string(&value).unwrap_or_else(|| value.to_string()),
            );
        }

        let url = format!("{}{}/sendMediaGroup", TG_API, token);
        self.requests.acquire(Some(&chat_id)).await;
        match http.post(&url).multipart(form).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    tracing::warn!(
                        "Telegram sendMediaGroup failed: {}",
                        delivery::sanitize_error_message(&text)
                    );
                }
            }
            Err(e) => tracing::warn!("Telegram sendMediaGroup failed: {}", e),
        }
        Ok(())
    }
}

/// Album bucket for a part; `""` means it can never join an album.
///
/// Text is always sent on its own, and voice messages cannot be grouped by the
/// Bot API, so both fall back to the single-attachment path.
fn album_kind(audio_as_voice: bool, part: &ContentPart) -> &'static str {
    match part {
        ContentPart::Image(_) | ContentPart::Video(_) => "visual",
        ContentPart::Audio(_) if !audio_as_voice => "audio",
        ContentPart::File(_) => "document",
        _ => "",
    }
}

/// How one album member reaches the Bot API.
enum AlbumSource {
    /// A publicly reachable URL Telegram can fetch itself.
    Url(String),
    /// Bytes to upload under an `attach://` reference.
    Upload {
        data: Vec<u8>,
        mime: String,
        filename: String,
    },
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

        // Probe the bot (SDK `initialize()`); non-fatal on failure. The
        // identity is what restricted group modes match mentions against.
        let result = {
            let me_url = format!("{}{}/getMe", TG_API, self.config.bot_token);
            match http.post(&me_url).send().await {
                Ok(resp) => resp
                    .json::<Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("result").cloned()),
                Err(_) => None,
            }
        };
        let bot_id = result.as_ref().and_then(|r| r.get("id")).and_then(|i| i.as_i64());
        let bot_username = result
            .as_ref()
            .and_then(|r| r.get("username"))
            .and_then(|u| u.as_str())
            .map(|s| s.to_string());
        tracing::info!(
            "Telegram getMe ok username={}",
            bot_username.as_deref().unwrap_or("")
        );

        if let Some(cb) = self.core.enqueue_sender() {
            let token = self.config.bot_token.clone();
            let admission = Admission::from_config(&self.config);
            let interactions = self.interactions.clone();
            let handle = tokio::spawn(async move {
                poll_updates(http, token, admission, bot_id, bot_username, interactions, cb).await;
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
        self.interactions.clear();
        self.http = None;
        self.core.set_connection_session(None);
        tracing::info!("TelegramChannel stopped");
        Ok(())
    }

    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        if !self.config.albums {
            for p in parts {
                if let Some(t) = p.text_value() {
                    self.send_text(subject, t).await?;
                } else {
                    self.send_media(subject, p).await?;
                }
            }
            return Ok(());
        }
        // Consecutive same-kind attachments become one album; anything that
        // cannot be grouped flushes the batch and goes out on its own.
        let mut kind: &'static str = "";
        let mut pending: Vec<ContentPart> = Vec::new();
        for part in parts {
            let next = album_kind(self.config.audio_as_voice, part);
            if next.is_empty() {
                self.flush_album_batch(subject, &mut pending).await?;
                if let Some(t) = part.text_value() {
                    self.send_text(subject, t).await?;
                } else {
                    self.send_media(subject, part).await?;
                }
                continue;
            }
            if kind != next {
                self.flush_album_batch(subject, &mut pending).await?;
            }
            kind = next;
            pending.push(part.clone());
        }
        self.flush_album_batch(subject, &mut pending).await?;
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        let http = self.http().await?;
        let token = self.config.bot_token.clone();
        let chat_id = self.chat_id_of(subject);

        let options = self.send_options(subject, true);
        let html = format_html::markdown_to_telegram_html(text);
        let chunks = match format_html::split_telegram_html(&html, TG_UTF16_LIMIT) {
            Ok(chunks) => chunks,
            Err(e) => {
                // Never lose a message because the formatter produced HTML the
                // splitter cannot balance; fall back to the escaped original.
                tracing::warn!("Telegram HTML split failed: {}", e);
                vec![(html.clone(), text.to_string())]
            }
        };
        for (html_chunk, plain) in chunks {
            let mut html_body = json!({
                "chat_id": chat_id_value(&chat_id),
                "text": html_chunk,
                "parse_mode": "HTML",
            });
            for (key, value) in &options {
                html_body[key] = value.clone();
            }
            self.requests.acquire(Some(&chat_id)).await;
            if let Err((status, message)) =
                api_call_detail(&http, &token, "sendMessage", &html_body).await
            {
                match delivery::classify_failure(status, &message) {
                    // Only entity-parsing failures permit a plaintext resend.
                    delivery::SendFailure::Formatting => {
                        let mut plain_body = json!({
                            "chat_id": chat_id_value(&chat_id),
                            "text": plain,
                        });
                        for (key, value) in &options {
                            plain_body[key] = value.clone();
                        }
                        self.requests.acquire(Some(&chat_id)).await;
                        if api_call(&http, &token, "sendMessage", &plain_body).await {
                            continue;
                        }
                        tracing::error!("Telegram send_text failed chat_id={}", chat_id);
                    }
                    delivery::SendFailure::RateLimited(_) | delivery::SendFailure::Fatal => {
                        tracing::error!(
                            "Telegram send_text rejected chat_id={} error={}",
                            chat_id,
                            message
                        );
                    }
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
        let (method, field) = self.media_endpoint(media);

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
        let mut body = json!({
            "chat_id": chat_id_value(&chat_id),
            "action": "typing",
        });
        if let Some(thread) = Self::thread_id_of(subject) {
            body["message_thread_id"] = thread;
        }
        self.requests.acquire(Some(&chat_id)).await;
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
        // `sender_id` is the identity the native button callbacks bind to.
        metadata.insert("sender_id".to_string(), json!(user_id));
        metadata.insert(
            "message_id".to_string(),
            data.get("message_id").cloned().unwrap_or(json!("")),
        );
        metadata.insert(
            "source_message_id".to_string(),
            data.get("source_message_id").cloned().unwrap_or(json!("")),
        );
        metadata.insert(
            "reply_to_message_id".to_string(),
            data.get("reply_to_message_id").cloned().unwrap_or(json!(null)),
        );
        metadata.insert(
            "message_thread_id".to_string(),
            data.get("message_thread_id").cloned().unwrap_or(json!(null)),
        );
        metadata.insert("is_group".to_string(), json!(is_group));
        metadata.insert(
            "edited".to_string(),
            data.get("edited").cloned().unwrap_or(json!(false)),
        );
        metadata.insert(
            "topic_isolation".to_string(),
            data.get("topic_isolation").cloned().unwrap_or(json!(false)),
        );
        // Telegram `file_id`s captured during polling (extra metadata, additive).
        if let Some(fids) = data.get("file_ids") {
            if fids.is_array() {
                metadata.insert("file_ids".to_string(), fids.clone());
            }
        }
        if let Some(list) = data.get("media") {
            if list.is_array() {
                metadata.insert("media".to_string(), list.clone());
            }
        }

        // Forum topics can be isolated into their own subjects.
        let topic_isolation = data
            .get("topic_isolation")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let subject_id = match data.get("message_thread_id").and_then(truthy_thread) {
            Some(thread) if topic_isolation => format!(
                "{}:topic:{}",
                chat_id,
                thread.as_i64().unwrap_or_default()
            ),
            _ => chat_id.clone(),
        };

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "telegram".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id,
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
    use super::{build_native_from_message, TelegramConfig};

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
    fn splitter_keeps_html_balanced_and_bounded() {
        use super::format_html::{html_runs, split_telegram_html};

        assert!(split_telegram_html("<b>a</b>", 3).is_err());
        assert!(html_runs("<b>a").is_err());
        assert_eq!(split_telegram_html("", 4000).unwrap().len(), 0);

        let (html, plain) = &split_telegram_html("<b>hi</b>", 4000).unwrap()[0];
        assert_eq!(html, "<b>hi</b>");
        assert_eq!(plain, "hi");

        // A wrapper that cannot fit alongside a character is dropped, but the
        // visible text is always retained.
        let long = format!("<b>{}</b>", "x".repeat(50));
        let chunks = split_telegram_html(&long, 10).unwrap();
        assert!(chunks.len() > 1);
        let joined: String = chunks.iter().map(|(_, plain)| plain.clone()).collect();
        assert_eq!(joined, "x".repeat(50));
        for (html, plain) in &chunks {
            assert!(html_runs(html).is_ok(), "unbalanced chunk: {}", html);
            assert!(!plain.is_empty());
            assert!(plain.chars().count() <= 10);
        }
    }

    #[test]
    fn overlapping_markers_stay_deliverable() {
        use super::format_html::html_runs;
        // Overlapping markers must still be deliverable, without lost text.
        let out = markdown_to_telegram_html("*a _b* c_");
        assert!(out.contains("a"));
        assert!(out.contains("c"));
        assert!(
            html_runs(&out).is_ok(),
            "formatter output must stay balanced: {}",
            out
        );
        // Genuinely crossed HTML is rejected by the splitter instead of being
        // delivered as unbalanced markup.
        assert!(html_runs("<b>a<i>b</b></i>").is_err());
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

    #[test]
    fn config_scoped_activation_defaults_and_validation() {
        let cfg = TelegramConfig::default();
        assert_eq!(cfg.group_activation, "always");
        assert!(cfg.allow_unaddressed_commands);
        assert!(cfg.reply_to_message);
        assert!(cfg.link_preview);
        assert!(cfg.allowed_chat_ids.is_empty());
        assert!(!cfg.topic_isolation);

        let mut cfg: TelegramConfig =
            serde_json::from_value(serde_json::json!({ "bot_token": "t", "group_activation": "mention" }))
                .unwrap();
        cfg.normalize().unwrap();
        assert_eq!(cfg.group_activation, "mention");

        let mut bad: TelegramConfig =
            serde_json::from_value(serde_json::json!({ "bot_token": "t", "group_activation": "sometimes" }))
                .unwrap();
        assert!(bad.normalize().is_err());

        // Chat allowlists accept negative IDs; user allowlists do not.
        let mut chats: TelegramConfig = serde_json::from_value(serde_json::json!({
            "bot_token": "t",
            "allowed_chat_ids": "-100123, 5"
        }))
        .unwrap();
        chats.normalize().unwrap();
        assert_eq!(chats.allowed_chat_ids, vec!["-100123", "5"]);

        // A user allowlist rejects negative IDs (only chat lists allow them).
        assert!(super::policy::parse_id_list(&serde_json::json!([-1]), false).is_err());
        assert!(super::policy::parse_id_list(&serde_json::json!([-1]), true).is_ok());
    }

    #[tokio::test]
    async fn anonymous_sender_has_no_user_identity() {
        let native = build_native_from_message(
            &reqwest::Client::new(),
            "token",
            &serde_json::json!({
                "message_id": 8,
                "chat": {"id": -100, "type": "supergroup"},
                "sender_chat": {"id": -100},
                "text": "hi",
            }),
        )
        .await
        .expect("native payload");
        assert_eq!(native.get("user_id").unwrap().as_str().unwrap(), "");
    }
}

#[cfg(test)]
mod album_tests {
    use super::*;
    use crate::models::{AudioContent, FileContent, ImageContent, VideoContent};

    /// A caption-only member: no `file_id`, so no `getFile` round-trip.
    fn member(chat_id: i64, message_id: i64, caption: &str, chat_type: &str) -> Value {
        json!({
            "message_id": message_id,
            "chat": {"id": chat_id, "type": chat_type},
            "from": {"id": 7},
            "media_group_id": "g1",
            "caption": caption,
        })
    }

    fn admission_with(cfg: TelegramConfig) -> Admission {
        Admission::from_config(&cfg)
    }

    fn spy() -> (EnqueueFn, Arc<std::sync::Mutex<Vec<Value>>>) {
        let delivered: Arc<std::sync::Mutex<Vec<Value>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = delivered.clone();
        let cb: EnqueueFn = Arc::new(move |payload| {
            if let RawPayload::Json(v) = payload {
                sink.lock().expect("delivery sink").push(v);
            }
        });
        (cb, delivered)
    }

    fn texts(payload: &Value) -> Vec<String> {
        payload
            .get("content")
            .and_then(|c| c.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|i| i.get("text").and_then(|t| t.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn album_key_separates_chat_topic_group_and_sender() {
        let a = member(-100, 1, "a", "supergroup");
        let b = member(-100, 2, "b", "supergroup");
        let mut c = member(-100, 3, "c", "supergroup");
        c["message_thread_id"] = json!(5);
        let mut d = member(-100, 4, "d", "supergroup");
        d["from"]["id"] = json!(9);

        assert_eq!(album_key(&a, "g1"), album_key(&b, "g1"));
        assert_ne!(album_key(&a, "g1"), album_key(&c, "g1"));
        assert_ne!(album_key(&a, "g1"), album_key(&d, "g1"));
        assert_ne!(album_key(&a, "g1"), album_key(&a, "g2"));
    }

    #[test]
    fn passive_albums_expire_and_stay_bounded() {
        let mut passive = PassiveAlbums::new();
        let now = Instant::now();
        let key = album_key(&member(-100, 1, "a", "supergroup"), "g1");
        passive.entry(&key, now).2.push(json!(1));
        assert_eq!(passive.entries.len(), 1);
        passive.expire(now + Duration::from_secs(1));
        assert!(passive.entries.is_empty());

        for i in 0..120 {
            let k = (format!("chat{}", i), String::new(), "g".to_string(), String::new());
            passive.entry(&k, now).2.push(json!(i));
        }
        assert_eq!(passive.entries.len(), 100);
    }

    #[test]
    fn album_members_are_capped_at_ten() {
        let mut active: HashMap<AlbumKey, Vec<Value>> = HashMap::new();
        let key = album_key(&member(-100, 1, "a", "supergroup"), "g1");
        for i in 0..12 {
            push_album_member(&mut active, &key, member(-100, i, "x", "supergroup"));
        }
        assert_eq!(active[&key].len(), 10);
    }

    #[test]
    fn album_kind_classifies_every_part() {
        let image = ContentPart::Image(ImageContent::default());
        let video = ContentPart::Video(VideoContent::default());
        let audio = ContentPart::Audio(AudioContent::default());
        let file = ContentPart::File(FileContent::default());
        let text = ContentPart::text("hi");

        assert_eq!(album_kind(false, &image), "visual");
        assert_eq!(album_kind(false, &video), "visual");
        assert_eq!(album_kind(false, &audio), "audio");
        assert_eq!(album_kind(false, &file), "document");
        assert_eq!(album_kind(false, &text), "");
        // Voice messages cannot be grouped, so they take the single path.
        assert_eq!(album_kind(true, &audio), "");
    }

    #[tokio::test]
    async fn flush_album_merges_members_in_message_order() {
        let key = album_key(&member(-100, 1, "a", "supergroup"), "g1");
        let active = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        active.lock().await.insert(
            key.clone(),
            vec![
                member(-100, 3, "third", "supergroup"),
                member(-100, 1, "first", "supergroup"),
                member(-100, 2, "second", "supergroup"),
            ],
        );
        let seen = Arc::new(tokio::sync::Mutex::new(BoundedSet::new(1000)));
        let (cb, delivered) = spy();

        flush_album(
            reqwest::Client::new(),
            "token".to_string(),
            admission_with(TelegramConfig::default()),
            Some(1),
            Some("bot".to_string()),
            seen,
            active,
            cb,
            key,
            Duration::from_millis(0),
        )
        .await;

        let out = delivered.lock().expect("sink").clone();
        assert_eq!(out.len(), 1, "an album is one inbound turn");
        assert_eq!(texts(&out[0]), vec!["first", "second", "third"]);
        // The earliest member stays the reply target.
        assert_eq!(out[0].get("source_message_id").unwrap().as_str().unwrap(), "1");
    }

    #[tokio::test]
    async fn flush_album_skips_members_that_fail_admission() {
        let key = album_key(&member(-100, 1, "a", "supergroup"), "g1");
        let active = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        active.lock().await.insert(
            key.clone(),
            vec![
                member(-100, 1, "kept", "supergroup"),
                member(-100, 2, "dropped", "channel"),
            ],
        );
        let seen = Arc::new(tokio::sync::Mutex::new(BoundedSet::new(1000)));
        let (cb, delivered) = spy();

        flush_album(
            reqwest::Client::new(),
            "token".to_string(),
            admission_with(TelegramConfig::default()),
            Some(1),
            Some("bot".to_string()),
            seen,
            active,
            cb,
            key,
            Duration::from_millis(0),
        )
        .await;

        let out = delivered.lock().expect("sink").clone();
        assert_eq!(texts(&out[0]), vec!["kept"]);
    }

    #[tokio::test]
    async fn album_members_wait_for_one_that_activates_the_bot() {
        let mut cfg = TelegramConfig::default();
        cfg.albums = true;
        cfg.group_activation = "mention".to_string();
        let admission = admission_with(cfg);

        let active = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let seen = Arc::new(tokio::sync::Mutex::new(BoundedSet::new(1000)));
        let (cb, delivered) = spy();
        let mut passive = PassiveAlbums::new();
        let mut tasks = Vec::new();
        let http = reqwest::Client::new();

        // Uncaptioned-in-activation-terms member: buffered, not delivered.
        let first = member(-100, 1, "first", "supergroup");
        assert!(
            collect_album_member(
                &http,
                "token",
                &admission,
                Some(1),
                Some("bot"),
                seen.clone(),
                active.clone(),
                &cb,
                &mut passive,
                &mut tasks,
                &first,
            )
            .await
        );
        assert_eq!(passive.entries.len(), 1);
        assert!(active.lock().await.is_empty());
        assert!(delivered.lock().expect("sink").is_empty());

        // The mentioning member activates the whole album.
        let mut second = member(-100, 2, "hey @bot", "supergroup");
        second["caption_entities"] = json!([{ "type": "mention", "offset": 4, "length": 4 }]);
        assert!(
            collect_album_member(
                &http,
                "token",
                &admission,
                Some(1),
                Some("bot"),
                seen.clone(),
                active.clone(),
                &cb,
                &mut passive,
                &mut tasks,
                &second,
            )
            .await
        );
        assert_eq!(tasks.len(), 1);

        tokio::time::sleep(Duration::from_millis(700)).await;
        let out = delivered.lock().expect("sink").clone();
        assert_eq!(out.len(), 1, "both members arrive as one turn");
        assert_eq!(texts(&out[0]), vec!["first", "hey @bot"]);
    }

    #[tokio::test]
    async fn album_collection_is_inert_when_disabled() {
        let admission = admission_with(TelegramConfig::default());
        let active = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let seen = Arc::new(tokio::sync::Mutex::new(BoundedSet::new(1000)));
        let (cb, delivered) = spy();
        let mut passive = PassiveAlbums::new();
        let mut tasks = Vec::new();
        let msg = member(-100, 1, "hi", "supergroup");

        assert!(
            !collect_album_member(
                &reqwest::Client::new(),
                "token",
                &admission,
                Some(1),
                Some("bot"),
                seen,
                active,
                &cb,
                &mut passive,
                &mut tasks,
                &msg,
            )
            .await
        );
        assert!(passive.entries.is_empty());
        assert!(delivered.lock().expect("sink").is_empty());
    }
}
