//! Yuanbao (元宝) channel over binary WebSocket.
//!
//! `protocol`, `utils` and `constants` are inlined as `mod` blocks below.
//! The channel:
//!
//! * signs a short-lived token (`POST /api/v5/robotLogic/sign-token`,
//! HMAC-SHA256 over `nonce+timestamp+app_key+app_secret`),
//! * opens a binary WebSocket (`wss://bot-wss.yuanbao.tencent.com/...`),
//! * auth-binds a hand-rolled protobuf `ConnMsg`,
//! * keeps the link alive with a ping loop and reconnects with backoff,
//! * decodes `inbound_message` push frames (TIM message elements),
//! * sends `send_c2c_message` / `send_group_message` protobuf frames and
//! heartbeats, uploading media to COS directly when required.
//!
//! Everything lives in this single file; `constants`, `utils` and `protocol`
//! are inlined as private `mod` blocks for the same
//! name.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::channel::{Channel, ChannelConfigLike, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage, RawPayload,
    TextContent, VideoContent,
};

// ===========================================================================
// constants.py
// ===========================================================================

#[allow(dead_code)]
mod constants {
    pub const DEFAULT_API_DOMAIN: &str = "https://bot.yuanbao.tencent.com";
    pub const DEFAULT_WS_URL: &str = "wss://bot-wss.yuanbao.tencent.com/wss/connection";
    pub const SIGN_TOKEN_PATH: &str = "/api/v5/robotLogic/sign-token";

    pub const MODULE_CONN_ACCESS: &str = "conn_access";
    pub const MODULE_BIZ: &str = "yuanbao_openclaw_proxy";

    pub const CMD_AUTH_BIND: &str = "auth-bind";
    pub const CMD_PING: &str = "ping";
    pub const CMD_KICKOUT: &str = "kickout";
    pub const CMD_UPDATE_META: &str = "update-meta";
    pub const CMD_INBOUND_MESSAGE: &str = "inbound_message";
    pub const CMD_SEND_C2C_MESSAGE: &str = "send_c2c_message";
    pub const CMD_SEND_GROUP_MESSAGE: &str = "send_group_message";
    pub const CMD_SEND_PRIVATE_HEARTBEAT: &str = "send_private_heartbeat";
    pub const CMD_SEND_GROUP_HEARTBEAT: &str = "send_group_heartbeat";

    pub const CALLBACK_C2C_SEND_MSG: &str = "C2C.CallbackAfterSendMsg";
    pub const CALLBACK_GROUP_SEND_MSG: &str = "Group.CallbackAfterSendMsg";

    pub const MSG_TYPE_TEXT: &str = "TIMTextElem";
    pub const MSG_TYPE_IMAGE: &str = "TIMImageElem";
    pub const MSG_TYPE_FILE: &str = "TIMFileElem";
    pub const MSG_TYPE_SOUND: &str = "TIMSoundElem";
    pub const MSG_TYPE_VIDEO: &str = "TIMVideoFileElem";

    pub const CMD_TYPE_REQUEST: i64 = 0;
    pub const CMD_TYPE_RESPONSE: i64 = 1;
    pub const CMD_TYPE_PUSH: i64 = 2;
    pub const CMD_TYPE_PUSH_ACK: i64 = 3;

    pub const RET_SUCCESS: i64 = 0;
    pub const RET_ALREADY_AUTH: i64 = 41101;
    pub const TOKEN_EXPIRED_CODES: [i64; 3] = [41103, 41104, 41108];

    pub const HEARTBEAT_RUNNING: i64 = 1;
    pub const HEARTBEAT_FINISH: i64 = 2;
    pub const HERMES_INSTANCE_ID: i64 = 17;

    pub const WT_VARINT: u32 = 0;
    pub const WT_LEN: u32 = 2;

    pub const TOKEN_REFRESH_MARGIN_SECONDS: i64 = 300;
    pub const INBOUND_DEDUPE_TTL_SECONDS: f64 = 300.0;
    pub const UPLOAD_INFO_PATH: &str = "/api/resource/genUploadInfo";
    pub const RESOURCE_DOWNLOAD_PATH: &str = "/api/resource/v1/download";
    pub const MAX_MEDIA_SIZE_BYTES: usize = 50 * 1024 * 1024;

    /// `_MIME_TO_IMAGE_FORMAT` (constants.py).
    pub fn mime_to_image_format(mime: &str) -> i64 {
        match mime.to_lowercase().as_str() {
            "image/jpeg" | "image/jpg" => 1,
            "image/gif" => 2,
            "image/png" => 3,
            "image/bmp" => 4,
            _ => 255,
        }
    }
}

use constants::*;

// ===========================================================================
// utils.py
// ===========================================================================

mod utils {
    use hmac::{Hmac, Mac};
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    use serde_json::Value;
    use sha1::{Digest, Sha1};
    use sha2::Sha256;

    use crate::models::ContentPart;
    use super::constants::{DEFAULT_API_DOMAIN, MSG_TYPE_TEXT};

    /// `urllib.parse.quote(value, safe="")`.
    const QUOTE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'_')
        .remove(b'.')
        .remove(b'-')
        .remove(b'~');

    pub fn quote(value: &str) -> String {
        utf8_percent_encode(value, QUOTE_SET).to_string()
    }

    pub fn percent_decode(value: &str) -> String {
        percent_encoding::percent_decode_str(value)
            .decode_utf8_lossy()
            .to_string()
    }

    /// `_compute_signature` — HMAC-SHA256(app_secret) over
    /// `nonce + timestamp + app_key + app_secret`.
    pub fn compute_signature(app_secret: &str, nonce: &str, timestamp: &str, app_key: &str) -> String {
        let plain = format!("{nonce}{timestamp}{app_key}{app_secret}");
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(app_secret.as_bytes())
            .expect("hmac accepts any key length");
        mac.update(plain.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// `_normalize_http_origin`.
    pub fn normalize_http_origin(value: &str) -> String {
        let value = value.trim().trim_end_matches('/');
        if value.is_empty() {
            return DEFAULT_API_DOMAIN.to_string();
        }
        if !value.starts_with("http://") && !value.starts_with("https://") {
            return format!("https://{value}");
        }
        value.to_string()
    }

    /// `_normalize_ws_url`.
    pub fn normalize_ws_url(value: &str) -> String {
        let value = value.trim();
        if value.is_empty() {
            return super::constants::DEFAULT_WS_URL.to_string();
        }
        if let Some(rest) = value.strip_prefix("https://") {
            return format!("wss://{rest}");
        }
        if let Some(rest) = value.strip_prefix("http://") {
            return format!("ws://{rest}");
        }
        if !value.starts_with("ws://") && !value.starts_with("wss://") {
            return format!("wss://{value}");
        }
        value.to_string()
    }

    fn has_scheme(value: &str) -> bool {
        // `urlparse(value).scheme` is non-empty.
        if let Some(idx) = value.find(':') {
            let scheme = &value[..idx];
            if scheme.is_empty() {
                return false;
            }
            if !scheme.chars().next().unwrap().is_ascii_alphabetic() {
                return false;
            }
            return scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.');
        }
        false
    }

    /// `_normalize_media_url`.
    pub fn normalize_media_url(value: &str, api_domain: &str) -> String {
        let value = value.trim();
        if value.is_empty() {
            return String::new();
        }
        if let Some(rest) = value.strip_prefix("//") {
            return format!("https://{rest}");
        }
        if has_scheme(value) {
            return value.to_string();
        }
        if let Some(rest) = value.strip_prefix('/') {
            return format!("{}/{}", api_domain.trim_end_matches('/'), rest);
        }
        format!("https://{value}")
    }

    /// `_first_media_url`.
    pub fn first_media_url(content: &Value, api_domain: &str) -> String {
        let url = normalize_media_url(str_at(content, "url").as_str(), api_domain);
        if !url.is_empty() {
            return url;
        }
        if let Some(images) = content.get("image_info_array").and_then(|v| v.as_array()) {
            for item in images {
                if item.is_object() {
                    let u = str_at(item, "url");
                    if !u.is_empty() {
                        return normalize_media_url(&u, api_domain);
                    }
                }
            }
        }
        let sound = normalize_media_url(str_at(content, "sound").as_str(), api_domain);
        if !sound.is_empty() {
            return sound;
        }
        String::new()
    }

    /// `_first_image_info` — returns `{}` (Null) when none.
    pub fn first_image_info(content: &Value) -> Value {
        let images = match content.get("image_info_array").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return Value::Null,
        };
        if images.len() > 1 && images[1].is_object() {
            return images[1].clone();
        }
        if !images.is_empty() && images[0].is_object() {
            return images[0].clone();
        }
        Value::Null
    }

    /// `_content_filename`.
    pub fn content_filename(content: &Value) -> String {
        for key in ["file_name", "fileName", "filename"] {
            let v = str_at(content, key);
            if !v.is_empty() {
                return v;
            }
        }
        String::new()
    }

    /// `_resource_id_from_url`.
    pub fn resource_id_from_url(url: &str) -> String {
        let query = match url.split_once('?') {
            Some((_, q)) => q,
            None => return String::new(),
        };
        let query = query.split('#').next().unwrap_or("");
        for pair in query.split('&') {
            let (k, v) = match pair.split_once('=') {
                Some(x) => x,
                None => continue,
            };
            if k == "resourceId" || k == "resourceid" {
                let v = percent_decode(v).trim().to_string();
                if !v.is_empty() {
                    return v;
                }
            }
        }
        String::new()
    }

    /// `_resolve_media_filename`.
    pub fn resolve_media_filename(part: &ContentPart, mime_type: &str) -> String {
        match part {
            ContentPart::File(f) if !f.filename.is_empty() => f.filename.clone(),
            ContentPart::Image(x) => {
                let base = basename_from_url(&x.url);
                if !base.is_empty() {
                    base
                } else {
                    format!("image{}", mime_to_extension(mime_type, ".jpg"))
                }
            }
            ContentPart::Video(x) => {
                let base = basename_from_url(&x.url);
                if !base.is_empty() {
                    base
                } else {
                    format!("video{}", mime_to_extension(mime_type, ".mp4"))
                }
            }
            ContentPart::Audio(x) => {
                let base = basename_from_url(&x.url);
                if !base.is_empty() {
                    base
                } else {
                    format!("audio{}", mime_to_extension(mime_type, ".mp3"))
                }
            }
            other => {
                let url = other.url().unwrap_or("");
                let base = if url.is_empty() {
                    String::new()
                } else {
                    basename_from_url(url)
                };
                if !base.is_empty() {
                    base
                } else {
                    format!("file{}", mime_to_extension(mime_type, ".bin"))
                }
            }
        }
    }

    /// `_basename_from_url`.
    pub fn basename_from_url(url: &str) -> String {
        let path = url.split(['?', '#']).next().unwrap_or("");
        let base = path.rsplit('/').next().unwrap_or("");
        percent_decode(base)
    }

    /// `_guess_mime_type`.
    pub fn guess_mime_type(filename_or_url: &str) -> String {
        let path = filename_or_url
            .split(['?', '#'])
            .next()
            .unwrap_or(filename_or_url);
        mime_guess::from_path(path)
            .first()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "application/octet-stream".to_string())
    }

    /// `_mime_to_extension`.
    pub fn mime_to_extension(mime_type: &str, default: &str) -> String {
        if mime_type.is_empty() {
            return default.to_string();
        }
        match mime_guess::get_mime_extensions_str(mime_type) {
            Some(exts) if !exts.is_empty() => format!(".{}", exts[0]),
            _ => default.to_string(),
        }
    }

    /// `_parse_image_size`.
    pub fn parse_image_size(data: &[u8]) -> (u32, u32) {
        parse_png_size(data)
            .or_else(|| parse_gif_size(data))
            .or_else(|| parse_jpeg_size(data))
            .or_else(|| parse_webp_size(data))
            .unwrap_or((0, 0))
    }

    fn be_u32(data: &[u8]) -> u32 {
        u32::from_be_bytes([data[0], data[1], data[2], data[3]])
    }
    fn be_u16(data: &[u8]) -> u16 {
        u16::from_be_bytes([data[0], data[1]])
    }
    fn le_u16(data: &[u8]) -> u16 {
        u16::from_le_bytes([data[0], data[1]])
    }
    fn le_u32(data: &[u8]) -> u32 {
        u32::from_le_bytes([data[0], data[1], data[2], data[3]])
    }

    fn parse_png_size(data: &[u8]) -> Option<(u32, u32)> {
        if data.len() >= 24 && data[..8] == *b"\x89PNG\r\n\x1a\n" {
            return Some((be_u32(&data[16..20]), be_u32(&data[20..24])));
        }
        None
    }

    fn parse_gif_size(data: &[u8]) -> Option<(u32, u32)> {
        if data.len() >= 10 && (&data[..6] == b"GIF87a" || &data[..6] == b"GIF89a") {
            return Some((le_u16(&data[6..8]) as u32, le_u16(&data[8..10]) as u32));
        }
        None
    }

    fn parse_jpeg_size(data: &[u8]) -> Option<(u32, u32)> {
        if data.len() < 4 || data[..2] != *b"\xff\xd8" {
            return None;
        }
        let mut pos = 2usize;
        while pos + 9 < data.len() {
            if data[pos] != 0xFF {
                pos += 1;
                continue;
            }
            let marker = data[pos + 1];
            if marker == 0xC0 || marker == 0xC2 {
                let height = be_u16(&data[pos + 5..pos + 7]);
                let width = be_u16(&data[pos + 7..pos + 9]);
                return Some((width as u32, height as u32));
            }
            if pos + 4 > data.len() {
                return None;
            }
            pos += 2 + be_u16(&data[pos + 2..pos + 4]) as usize;
        }
        None
    }

    fn parse_webp_size(data: &[u8]) -> Option<(u32, u32)> {
        if data.len() < 30 || &data[..4] != b"RIFF" || &data[8..12] != b"WEBP" {
            return None;
        }
        let chunk = &data[12..16];
        if chunk == b"VP8 " && &data[23..26] == b"\x9d\x01\x2a" {
            let width = (le_u16(&data[26..28]) & 0x3FFF) as u32;
            let height = (le_u16(&data[28..30]) & 0x3FFF) as u32;
            return Some((width, height));
        }
        if chunk == b"VP8L" && data.len() >= 25 && data[20] == 0x2F {
            let bits = le_u32(&data[21..25]);
            return Some(((bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1));
        }
        if chunk == b"VP8X" {
            let width = (data[24] as u32) | ((data[25] as u32) << 8) | ((data[26] as u32) << 16);
            let height = (data[27] as u32) | ((data[28] as u32) << 8) | ((data[29] as u32) << 16);
            return Some((width + 1, height + 1));
        }
        None
    }

    /// `_cos_sign` — Tencent COS request signature (HMAC-SHA1).
    #[allow(clippy::too_many_arguments)]
    pub fn cos_sign(
        method: &str,
        path: &str,
        params: &[(String, String)],
        headers: &[(&str, &str)],
        secret_id: &str,
        secret_key: &str,
        start_time: i64,
        expire_seconds: i64,
    ) -> String {
        let sign_time = format!("{start_time};{}", start_time + expire_seconds);
        let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(secret_key.as_bytes())
            .expect("hmac accepts any key length");
        mac.update(sign_time.as_bytes());
        let sign_key = hex::encode(mac.finalize().into_bytes());

        let mut sorted_params: Vec<(String, String)> = params
            .iter()
            .map(|(k, v)| (k.to_lowercase(), quote(v)))
            .collect();
        sorted_params.sort();
        let mut sorted_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_lowercase(), quote(v)))
            .collect();
        sorted_headers.sort();

        let param_keys = sorted_params
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>()
            .join(";");
        let param_string = sorted_params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let header_keys = sorted_headers
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>()
            .join(";");
        let header_string = sorted_headers
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let http_string = [
            method.to_lowercase(),
            path.to_string(),
            param_string,
            header_string,
            String::new(),
        ]
        .join("\n");

        let mut hasher = Sha1::new();
        hasher.update(http_string.as_bytes());
        let http_hash = hex::encode(hasher.finalize());

        let string_to_sign = [
            "sha1".to_string(),
            sign_time.clone(),
            http_hash,
            String::new(),
        ]
        .join("\n");

        let mut mac2 = <Hmac<Sha1> as Mac>::new_from_slice(sign_key.as_bytes())
            .expect("hmac accepts any key length");
        mac2.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac2.finalize().into_bytes());

        format!(
            "q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={sign_time}&q-key-time={sign_time}\
&q-header-list={header_keys}&q-url-param-list={param_keys}&q-signature={signature}"
        )
    }

    /// `_single_text_body_text` — Some(text) only for a lone TIMTextElem.
    pub fn single_text_body_text(msg_body: &[Value]) -> Option<String> {
        if msg_body.len() != 1 {
            return None;
        }
        let element = &msg_body[0];
        if element.get("msg_type").and_then(|v| v.as_str()) != Some(MSG_TYPE_TEXT) {
            return None;
        }
        let content = element.get("msg_content")?;
        if !content.is_object() {
            return None;
        }
        match content.get("text") {
            Some(v) if !v.is_null() => Some(py_str(v)),
            _ => None,
        }
    }

    /// `_optional_int`.
    pub fn optional_int(value: &Value) -> Option<i64> {
        match value {
            Value::Null => None,
            Value::String(s) if s.is_empty() => None,
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64)),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .ok()
                .or_else(|| s.trim().parse::<f64>().ok().map(|f| f as i64)),
            Value::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }

    /// `_optional_int` on an `Option<&Value>`.
    pub fn optional_int_opt(value: Option<&Value>) -> Option<i64> {
        value.and_then(optional_int)
    }

    /// `_redact_account`.
    pub fn redact_account(value: &str) -> String {
        if value.is_empty() {
            return String::new();
        }
        let chars: Vec<char> = value.chars().collect();
        if chars.len() <= 12 {
            return value.to_string();
        }
        let head: String = chars[..8].iter().collect();
        let tail: String = chars[chars.len() - 4..].iter().collect();
        format!("{head}...{tail}")
    }

    /// `str(value)` for the subset of JSON values we emit.
    pub fn py_str(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Bool(b) => {
                if *b {
                    "True".to_string()
                } else {
                    "False".to_string()
                }
            }
            Value::Number(n) => n.to_string(),
            Value::Null => "None".to_string(),
            other => other.to_string(),
        }
    }

    /// `int(value)` for encoding varint fields.
    pub fn py_int(value: &Value) -> i64 {
        match value {
            Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_f64().map(|f| f as i64))
                .unwrap_or(0),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .ok()
                .or_else(|| s.trim().parse::<f64>().ok().map(|f| f as i64))
                .unwrap_or(0),
            Value::Bool(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    /// truthiness.
    pub fn truthy(value: &Value) -> bool {
        match value {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
        }
    }

    /// `str(obj.get(key) or "")` semantics for object values.
    pub fn str_at(obj: &Value, key: &str) -> String {
        match obj.get(key) {
            Some(Value::Null) | None => String::new(),
            Some(v) => py_str(v),
        }
    }

    /// `datetime.now(CST).isoformat(timespec="seconds")` (UTC+08:00, no chrono).
    pub fn cst_timestamp() -> String {
        let secs = crate::channel::now_f64() as i64 + 8 * 3600;
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        let (y, m, d) = civil_from_days(days);
        let hh = rem / 3600;
        let mm = (rem % 3600) / 60;
        let ss = rem % 60;
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}+08:00")
    }

    /// Howard Hinnant's `civil_from_days`.
    fn civil_from_days(z: i64) -> (i64, u32, u32) {
        let z = z + 719468;
        let era = if z >= 0 { z } else { z - 146096 } / 146097;
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        (if m <= 2 { y + 1 } else { y }, m, d)
    }
}

// ===========================================================================
// protocol.py
// ===========================================================================

#[allow(dead_code)]
mod protocol {
    use serde_json::{json, Map, Value};

    use super::constants::*;
    use super::utils::{py_int, py_str, truthy};

    // ---- protobuf-lite primitives -------------------------------------

    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                out.push(byte | 0x80);
            } else {
                out.push(byte);
                return out;
            }
        }
    }

    fn field(field_number: u32, wire_type: u32, value: &[u8]) -> Vec<u8> {
        let tag = ((field_number as u64) << 3) | wire_type as u64;
        let mut out = varint(tag);
        out.extend_from_slice(value);
        out
    }

    fn pb_string(value: &str) -> Vec<u8> {
        let bytes = value.as_bytes();
        let mut out = varint(bytes.len() as u64);
        out.extend_from_slice(bytes);
        out
    }

    fn pb_bytes(value: &[u8]) -> Vec<u8> {
        let mut out = varint(value.len() as u64);
        out.extend_from_slice(value);
        out
    }

    fn read_varint(data: &[u8], mut pos: usize) -> Result<(u64, usize), &'static str> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        while pos < data.len() {
            let b = data[pos];
            pos += 1;
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok((result, pos));
            }
            shift += 7;
            if shift >= 64 {
                return Err("protobuf varint too long");
            }
        }
        Err("truncated protobuf varint")
    }

    #[derive(Clone, Debug)]
    pub enum FieldVal {
        Varint(u64),
        Bytes(Vec<u8>),
    }

    fn parse_fields(data: &[u8]) -> Result<Vec<(u32, u32, FieldVal)>, &'static str> {
        let mut fields = Vec::new();
        let mut pos = 0usize;
        while pos < data.len() {
            let (tag, next) = read_varint(data, pos)?;
            pos = next;
            let field_number = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u32;
            match wire_type {
                WT_VARINT => {
                    let (value, next) = read_varint(data, pos)?;
                    pos = next;
                    fields.push((field_number, wire_type, FieldVal::Varint(value)));
                }
                WT_LEN => {
                    let (len, next) = read_varint(data, pos)?;
                    pos = next;
                    let end = pos.saturating_add(len as usize);
                    let value = if pos <= data.len() {
                        data[pos..end.min(data.len())].to_vec()
                    } else {
                        Vec::new()
                    };
                    pos = end;
                    fields.push((field_number, wire_type, FieldVal::Bytes(value)));
                }
                _ => return Err("unsupported protobuf wire type"),
            }
        }
        Ok(fields)
    }

    type Fields = std::collections::HashMap<u32, Vec<(u32, FieldVal)>>;

    fn fields_to_dict(fields: Vec<(u32, u32, FieldVal)>) -> Fields {
        let mut out: Fields = std::collections::HashMap::new();
        for (n, wt, v) in fields {
            out.entry(n).or_default().push((wt, v));
        }
        out
    }

    fn get_string(fields: &Fields, field_number: u32) -> String {
        match fields.get(&field_number).and_then(|v| v.first()) {
            Some((wt, FieldVal::Bytes(b))) if *wt == WT_LEN => {
                String::from_utf8_lossy(b).to_string()
            }
            _ => String::new(),
        }
    }

    fn get_varint(fields: &Fields, field_number: u32) -> u64 {
        match fields.get(&field_number).and_then(|v| v.first()) {
            Some((wt, FieldVal::Varint(n))) if *wt == WT_VARINT => *n,
            _ => 0,
        }
    }

    fn get_bytes(fields: &Fields, field_number: u32) -> Vec<u8> {
        match fields.get(&field_number).and_then(|v| v.first()) {
            Some((wt, FieldVal::Bytes(b))) if *wt == WT_LEN => b.clone(),
            _ => Vec::new(),
        }
    }

    // ---- decoded types -------------------------------------------------

    #[derive(Clone, Debug, Default)]
    pub struct Head {
        pub cmd_type: i64,
        pub cmd: String,
        pub seq_no: u64,
        pub msg_id: String,
        pub module: String,
        pub need_ack: bool,
        pub status: u64,
    }

    #[derive(Clone, Debug, Default)]
    pub struct ConnMsg {
        pub head: Head,
        pub data: Vec<u8>,
    }

    // ---- encoders ------------------------------------------------------

    pub fn encode_request(cmd: &str, module: &str, msg_id: &str, seq_no: u64, payload: &[u8]) -> Vec<u8> {
        encode_conn_msg(CMD_TYPE_REQUEST, cmd, seq_no, msg_id, module, payload)
    }

    pub fn encode_push_ack(head: &Head, seq_no: u64) -> Vec<u8> {
        encode_conn_msg(CMD_TYPE_PUSH_ACK, &head.cmd, seq_no, &head.msg_id, &head.module, &[])
    }

    fn encode_conn_msg(
        cmd_type: i64,
        cmd: &str,
        seq_no: u64,
        msg_id: &str,
        module: &str,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut head = Vec::new();
        if cmd_type != 0 {
            head.extend(field(1, WT_VARINT, &varint(cmd_type as u64)));
        }
        if !cmd.is_empty() {
            head.extend(field(2, WT_LEN, &pb_string(cmd)));
        }
        if seq_no != 0 {
            head.extend(field(3, WT_VARINT, &varint(seq_no)));
        }
        if !msg_id.is_empty() {
            head.extend(field(4, WT_LEN, &pb_string(msg_id)));
        }
        if !module.is_empty() {
            head.extend(field(5, WT_LEN, &pb_string(module)));
        }

        let mut conn = field(1, WT_LEN, &pb_bytes(&head));
        if !payload.is_empty() {
            conn.extend(field(2, WT_LEN, &pb_bytes(payload)));
        }
        conn
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_auth_bind_payload(
        bot_id: &str,
        source: &str,
        token: &str,
        route_env: &str,
        app_version: &str,
        app_operation_system: &str,
        bot_version: &str,
        instance_id: &str,
    ) -> Vec<u8> {
        let mut auth_info = Vec::new();
        auth_info.extend(field(1, WT_LEN, &pb_string(bot_id)));
        auth_info.extend(field(2, WT_LEN, &pb_string(source)));
        auth_info.extend(field(3, WT_LEN, &pb_string(token)));

        let mut device_info = Vec::new();
        if !app_version.is_empty() {
            device_info.extend(field(1, WT_LEN, &pb_string(app_version)));
        }
        if !app_operation_system.is_empty() {
            device_info.extend(field(2, WT_LEN, &pb_string(app_operation_system)));
        }
        if !instance_id.is_empty() {
            device_info.extend(field(10, WT_LEN, &pb_string(instance_id)));
        }
        if !bot_version.is_empty() {
            device_info.extend(field(24, WT_LEN, &pb_string(bot_version)));
        }

        let mut payload = Vec::new();
        payload.extend(field(1, WT_LEN, &pb_string("ybBot")));
        payload.extend(field(2, WT_LEN, &pb_bytes(&auth_info)));
        payload.extend(field(3, WT_LEN, &pb_bytes(&device_info)));
        if !route_env.is_empty() {
            payload.extend(field(5, WT_LEN, &pb_string(route_env)));
        }
        payload
    }

    pub fn encode_text_body(text: &str) -> Vec<Value> {
        vec![json_value_element(MSG_TYPE_TEXT, json!({ "text": text }))]
    }

    fn json_value_element(msg_type: &str, content: Value) -> Value {
        json!({ "msg_type": msg_type, "msg_content": content })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_image_msg_body(
        url: &str,
        uuid: &str,
        filename: &str,
        size: i64,
        width: i64,
        height: i64,
        mime_type: &str,
    ) -> Vec<Value> {
        let image_uuid = if !uuid.is_empty() {
            uuid.to_string()
        } else if !filename.is_empty() {
            filename.to_string()
        } else {
            "image".to_string()
        };
        let image_format = if mime_type.is_empty() {
            255
        } else {
            mime_to_image_format(mime_type)
        };
        vec![json!({
            "msg_type": MSG_TYPE_IMAGE,
            "msg_content": {
                "uuid": image_uuid,
                "image_format": image_format,
                "image_info_array": [
                    { "type": 1, "size": size, "width": width, "height": height, "url": url }
                ],
            },
        })]
    }

    pub fn build_file_msg_body(url: &str, filename: &str, uuid: &str, size: i64) -> Vec<Value> {
        vec![json!({
            "msg_type": MSG_TYPE_FILE,
            "msg_content": {
                "uuid": if uuid.is_empty() { filename } else { uuid },
                "file_name": filename,
                "file_size": size,
                "url": url,
            },
        })]
    }

    fn encode_log_ext(trace_id: &str) -> Vec<u8> {
        if trace_id.is_empty() {
            Vec::new()
        } else {
            field(1, WT_LEN, &pb_string(trace_id))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_c2c_message_payload(
        to_account: &str,
        from_account: &str,
        msg_body: &[Value],
        msg_id: &str,
        msg_random: i64,
        msg_seq: Option<i64>,
        group_code: &str,
        trace_id: &str,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        if !msg_id.is_empty() {
            payload.extend(field(1, WT_LEN, &pb_string(msg_id)));
        }
        payload.extend(field(2, WT_LEN, &pb_string(to_account)));
        if !from_account.is_empty() {
            payload.extend(field(3, WT_LEN, &pb_string(from_account)));
        }
        if msg_random != 0 {
            payload.extend(field(4, WT_VARINT, &varint(msg_random as u64)));
        }
        for element in msg_body {
            payload.extend(field(5, WT_LEN, &pb_bytes(&encode_msg_body_element(element))));
        }
        if !group_code.is_empty() {
            payload.extend(field(6, WT_LEN, &pb_string(group_code)));
        }
        if let Some(seq) = msg_seq {
            payload.extend(field(7, WT_VARINT, &varint(seq as u64)));
        }
        if !trace_id.is_empty() {
            payload.extend(field(8, WT_LEN, &pb_bytes(&encode_log_ext(trace_id))));
        }
        payload
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_group_message_payload(
        group_code: &str,
        from_account: &str,
        msg_body: &[Value],
        msg_id: &str,
        to_account: &str,
        random: &str,
        msg_seq: Option<i64>,
        ref_msg_id: &str,
        trace_id: &str,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        if !msg_id.is_empty() {
            payload.extend(field(1, WT_LEN, &pb_string(msg_id)));
        }
        payload.extend(field(2, WT_LEN, &pb_string(group_code)));
        if !from_account.is_empty() {
            payload.extend(field(3, WT_LEN, &pb_string(from_account)));
        }
        if !to_account.is_empty() {
            payload.extend(field(4, WT_LEN, &pb_string(to_account)));
        }
        if !random.is_empty() {
            payload.extend(field(5, WT_LEN, &pb_string(random)));
        }
        for element in msg_body {
            payload.extend(field(6, WT_LEN, &pb_bytes(&encode_msg_body_element(element))));
        }
        if !ref_msg_id.is_empty() {
            payload.extend(field(7, WT_LEN, &pb_string(ref_msg_id)));
        }
        if let Some(seq) = msg_seq {
            payload.extend(field(8, WT_VARINT, &varint(seq as u64)));
        }
        if !trace_id.is_empty() {
            payload.extend(field(9, WT_LEN, &pb_bytes(&encode_log_ext(trace_id))));
        }
        payload
    }

    pub fn encode_private_heartbeat_payload(from_account: &str, to_account: &str, heartbeat: i64) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend(field(1, WT_LEN, &pb_string(from_account)));
        payload.extend(field(2, WT_LEN, &pb_string(to_account)));
        payload.extend(field(3, WT_VARINT, &varint(heartbeat as u64)));
        payload
    }

    pub fn encode_group_heartbeat_payload(
        from_account: &str,
        group_code: &str,
        send_time: i64,
        heartbeat: i64,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend(field(1, WT_LEN, &pb_string(from_account)));
        payload.extend(field(2, WT_LEN, &pb_string("")));
        payload.extend(field(3, WT_LEN, &pb_string(group_code)));
        payload.extend(field(4, WT_VARINT, &varint(send_time as u64)));
        payload.extend(field(5, WT_VARINT, &varint(heartbeat as u64)));
        payload
    }

    fn encode_msg_body_element(element: &Value) -> Vec<u8> {
        let mut payload = Vec::new();
        let msg_type = element
            .get("msg_type")
            .map(py_str)
            .unwrap_or_default();
        if !msg_type.is_empty() {
            payload.extend(field(1, WT_LEN, &pb_string(&msg_type)));
        }
        if let Some(content) = element.get("msg_content") {
            if content.is_object() {
                payload.extend(field(2, WT_LEN, &pb_bytes(&encode_msg_content(content))));
            }
        }
        payload
    }

    fn encode_msg_content(content: &Value) -> Vec<u8> {
        let mut payload = Vec::new();
        for (field_no, key) in [
            (1u32, "text"),
            (2, "uuid"),
            (4, "data"),
            (5, "desc"),
            (6, "ext"),
            (7, "sound"),
            (10, "url"),
            (12, "file_name"),
        ] {
            if let Some(value) = content.get(key) {
                if truthy(value) {
                    payload.extend(field(field_no, WT_LEN, &pb_string(&py_str(value))));
                }
            }
        }
        for (field_no, key) in [(3u32, "image_format"), (9, "index"), (11, "file_size")] {
            if let Some(value) = content.get(key) {
                if truthy(value) {
                    payload.extend(field(field_no, WT_VARINT, &varint(py_int(value) as u64)));
                }
            }
        }
        if let Some(infos) = content.get("image_info_array").and_then(|v| v.as_array()) {
            for image_info in infos {
                if !image_info.is_object() {
                    continue;
                }
                let mut image_payload = Vec::new();
                for (field_no, key) in [(1u32, "type"), (2, "size"), (3, "width"), (4, "height")] {
                    if let Some(value) = image_info.get(key) {
                        if truthy(value) {
                            image_payload
                                .extend(field(field_no, WT_VARINT, &varint(py_int(value) as u64)));
                        }
                    }
                }
                if let Some(url) = image_info.get("url") {
                    if truthy(url) {
                        image_payload.extend(field(5, WT_LEN, &pb_string(&py_str(url))));
                    }
                }
                if !image_payload.is_empty() {
                    payload.extend(field(8, WT_LEN, &pb_bytes(&image_payload)));
                }
            }
        }
        if let Some(ext_map) = content.get("ext_map").and_then(|v| v.as_object()) {
            for (key, value) in ext_map {
                let mut entry = field(1, WT_LEN, &pb_string(key));
                entry.extend(field(2, WT_LEN, &pb_string(&py_str(value))));
                payload.extend(field(999, WT_LEN, &pb_bytes(&entry)));
            }
        }
        payload
    }

    // ---- decoders ------------------------------------------------------

    pub fn decode_conn_msg(frame: &[u8]) -> Result<ConnMsg, &'static str> {
        let fields = fields_to_dict(parse_fields(frame)?);
        let head_bytes = get_bytes(&fields, 1);
        let data = get_bytes(&fields, 2);
        let head = if head_bytes.is_empty() {
            Head::default()
        } else {
            decode_head(&head_bytes)
        };
        Ok(ConnMsg { head, data })
    }

    fn decode_head(payload: &[u8]) -> Head {
        let fields = fields_to_dict(parse_fields(payload).unwrap_or_default());
        Head {
            cmd_type: get_varint(&fields, 1) as i64,
            cmd: get_string(&fields, 2),
            seq_no: get_varint(&fields, 3),
            msg_id: get_string(&fields, 4),
            module: get_string(&fields, 5),
            need_ack: get_varint(&fields, 6) != 0,
            status: get_varint(&fields, 10),
        }
    }

    pub fn decode_status_response(payload: &[u8]) -> (i64, String) {
        if payload.is_empty() {
            return (RET_SUCCESS, String::new());
        }
        let fields = match parse_fields(payload) {
            Ok(f) => fields_to_dict(f),
            Err(_) => return (RET_SUCCESS, String::new()),
        };
        (get_varint(&fields, 1) as i64, get_string(&fields, 2))
    }

    /// Only used by tests / future encoders (kept for completeness).
    #[allow(dead_code)]
    pub fn encode_msg_content_only(content: &Value) -> Vec<u8> {
        encode_msg_content(content)
    }

    #[allow(dead_code)]
    pub fn empty_object() -> Map<String, Value> {
        Map::new()
    }
}

// ===========================================================================
// config.py
// ===========================================================================

/// `_YuanbaoToken`.
#[derive(Clone, Debug, Default)]
struct YuanbaoToken {
    token: String,
    bot_id: String,
    source: String,
    expires_at: f64,
    #[allow(dead_code)]
    duration: i64,
}

/// Configuration for the Tencent Yuanbao channel (`YuanbaoConfig`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct YuanbaoConfig {
    #[serde(flatten)]
    pub base: crate::channel::ChannelConfig,

    pub app_key: String,
    pub app_secret: String,
    pub api_domain: String,
    pub ws_url: String,
    pub route_env: String,

    // Legacy/compat fields (retained so old stored configs still load).
    pub api_base: String,
    pub token: String,
    pub bot_id: String,
    pub identifier: String,
    pub source: String,
    pub extra: HashMap<String, Value>,

    pub app_version: String,
    pub app_operation_system: String,
    pub operation_system: String,
    pub bot_version: String,
    pub instance_id: String,
    pub request_timeout: f64,
    pub heartbeat_interval: f64,
    pub connect_timeout: f64,
    pub probe_mode: String,
}

impl Default for YuanbaoConfig {
    fn default() -> Self {
        YuanbaoConfig {
            base: crate::channel::ChannelConfig::default(),
            app_key: String::new(),
            app_secret: String::new(),
            api_domain: DEFAULT_API_DOMAIN.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            route_env: String::new(),
            api_base: String::new(),
            token: String::new(),
            bot_id: String::new(),
            identifier: String::new(),
            source: String::new(),
            extra: HashMap::new(),
            app_version: "1.0.0".to_string(),
            app_operation_system: String::new(),
            operation_system: String::new(),
            bot_version: "1.0.0".to_string(),
            instance_id: HERMES_INSTANCE_ID.to_string(),
            request_timeout: 30.0,
            heartbeat_interval: 30.0,
            connect_timeout: 15.0,
            probe_mode: "full".to_string(),
        }
    }
}

/// `sys.platform` approximation.
fn default_platform() -> &'static str {
    if cfg!(target_os = "windows") {
        "win32"
    } else if cfg!(target_os = "macos") {
        "darwin"
    } else {
        std::env::consts::OS
    }
}

impl YuanbaoConfig {
    /// `__post_init__`.
    fn normalize(&mut self) {
        let source_domain = if self.api_domain.trim().is_empty() {
            self.api_base.clone()
        } else {
            self.api_domain.clone()
        };
        self.api_domain = utils::normalize_http_origin(&source_domain);
        self.api_base = self.api_domain.clone();

        self.ws_url = utils::normalize_ws_url(&self.ws_url);

        let selected = if !self.app_operation_system.is_empty() {
            self.app_operation_system.clone()
        } else if !self.operation_system.is_empty() {
            self.operation_system.clone()
        } else {
            default_platform().to_string()
        };
        self.app_operation_system = selected.clone();
        self.operation_system = selected;

        if self.instance_id.is_empty() {
            self.instance_id = HERMES_INSTANCE_ID.to_string();
        }
        if self.bot_id.is_empty() && !self.identifier.is_empty() {
            self.bot_id = self.identifier.clone();
        }
        if self.identifier.is_empty() && !self.bot_id.is_empty() {
            self.identifier = self.bot_id.clone();
        }
    }

    fn effective_api_domain(&self) -> String {
        let source = if self.api_domain.trim().is_empty() {
            self.api_base.clone()
        } else {
            self.api_domain.clone()
        };
        utils::normalize_http_origin(&source)
    }

    fn effective_ws_url(&self) -> String {
        utils::normalize_ws_url(&self.ws_url)
    }

    fn effective_operation_system(&self) -> String {
        if !self.app_operation_system.is_empty() {
            self.app_operation_system.clone()
        } else if !self.operation_system.is_empty() {
            self.operation_system.clone()
        } else {
            default_platform().to_string()
        }
    }
}

// ===========================================================================
// Shared connection state
// ===========================================================================

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = futures::stream::SplitSink<WsStream, Message>;
type WsReader = futures::stream::SplitStream<WsStream>;

/// State shared between the `Channel` methods (`&self`) and the background
/// WebSocket tasks. Holds the channel's mutable shared state.
struct Shared {
    config: YuanbaoConfig,
    http: reqwest::Client,
    enqueue: Option<EnqueueFn>,
    channel_id: String,
    tenant_id: Option<String>,

    sink: AsyncMutex<Option<WsSink>>,
    receive_task: StdMutex<Option<JoinHandle<()>>>,
    pending: StdMutex<HashMap<String, oneshot::Sender<Vec<u8>>>>,
    disconnect_tx: mpsc::UnboundedSender<()>,
    disconnect_rx: StdMutex<Option<mpsc::UnboundedReceiver<()>>>,

    connected: AtomicBool,
    stopping: AtomicBool,
    should_reconnect: AtomicBool,
    seq: AtomicU32,

    bot_id: StdMutex<String>,
    token: StdMutex<Option<YuanbaoToken>>,
    seen: StdMutex<HashMap<String, std::time::Instant>>,
}

impl Shared {
    fn new(
        config: YuanbaoConfig,
        http: reqwest::Client,
        enqueue: Option<EnqueueFn>,
        channel_id: String,
        tenant_id: Option<String>,
    ) -> Shared {
        let (disconnect_tx, disconnect_rx) = mpsc::unbounded_channel();
        Shared {
            config,
            http,
            enqueue,
            channel_id,
            tenant_id,
            sink: AsyncMutex::new(None),
            receive_task: StdMutex::new(None),
            pending: StdMutex::new(HashMap::new()),
            disconnect_tx,
            disconnect_rx: StdMutex::new(Some(disconnect_rx)),
            connected: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            should_reconnect: AtomicBool::new(false),
            seq: AtomicU32::new(0),
            bot_id: StdMutex::new(String::new()),
            token: StdMutex::new(None),
            seen: StdMutex::new(HashMap::new()),
        }
    }

    fn bot_id(&self) -> String {
        self.bot_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_bot_id(&self, value: String) {
        *self.bot_id.lock().unwrap_or_else(|e| e.into_inner()) = value;
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst).wrapping_add(1) as u64
    }

    fn next_business_msg_id(&self, prefix: &str) -> String {
        format!("{prefix}_{}", self.next_seq())
    }

    /// `_remember_message_id` — monotonic TTL dedup.
    fn remember_message_id(&self, msg_id: &str) -> bool {
        let now = std::time::Instant::now();
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.retain(|_, expires_at| *expires_at > now);
        if let Some(expires_at) = seen.get(msg_id) {
            if *expires_at > now {
                return false;
            }
        }
        seen.insert(
            msg_id.to_string(),
            now + Duration::from_secs_f64(INBOUND_DEDUPE_TTL_SECONDS),
        );
        true
    }

    async fn send_raw(&self, packet: &[u8]) -> Result<()> {
        let mut guard = self.sink.lock().await;
        match guard.as_mut() {
            Some(sink) => sink
                .send(Message::Binary(packet.to_vec().into()))
                .await
                .map_err(|e| GatewayError::Other(e.to_string())),
            None => Err(GatewayError::Other(
                "YuanbaoChannel is not connected".to_string(),
            )),
        }
    }
}

/// Response-bearing error used to detect COS/resource HTTP 401 retries.
struct RespErr {
    status: Option<u16>,
    message: String,
}

impl RespErr {
    fn into_gateway(self) -> GatewayError {
        GatewayError::Other(self.message)
    }
}

fn random_hex(nbytes: usize) -> String {
    use rand::RngCore;
    let mut buf = vec![0u8; nbytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

// ===========================================================================
// channel_adapter! scaffolding
// ===========================================================================

crate::channel_adapter! {
    /// Tencent Yuanbao channel over binary WebSocket.
    YuanbaoChannel, YuanbaoConfig, "yuanbao",
    required = [app_key, app_secret],
    fields = {
        shared: Option<Arc<Shared>>,
        supervisor: Option<JoinHandle<()>>,
    }
}

// ===========================================================================
// Token / HTTP helpers
// ===========================================================================

/// `_sign_token` (HMAC-SHA256 signature + token cache).
async fn sign_token(shared: &Shared, force: bool) -> Result<YuanbaoToken> {
    let cfg = &shared.config;
    let now = crate::channel::now_f64();
    if !force {
        if let Some(cached) = shared.token.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            if now < cached.expires_at {
                return Ok(cached);
            }
        }
    }

    let nonce = random_hex(16);
    let timestamp = utils::cst_timestamp();
    let signature = utils::compute_signature(&cfg.app_secret, &nonce, &timestamp, &cfg.app_key);
    let payload = json!({
        "app_key": cfg.app_key,
        "nonce": nonce,
        "signature": signature,
        "timestamp": timestamp,
    });

    let url = format!("{}{}", cfg.effective_api_domain(), SIGN_TOKEN_PATH);
    let timeout = Duration::from_secs_f64(cfg.request_timeout.max(0.001));
    let mut req = shared
        .http
        .post(&url)
        .timeout(timeout)
        .header("Content-Type", "application/json")
        .header("X-AppVersion", cfg.app_version.clone())
        .header("X-OperationSystem", cfg.effective_operation_system())
        .header("X-Instance-Id", cfg.instance_id.clone())
        .header("X-Bot-Version", cfg.bot_version.clone());
    if !cfg.route_env.is_empty() {
        req = req.header("X-Route-Env", cfg.route_env.clone());
    }
    let resp = req.json(&payload).send().await?;
    let status = resp.status();
    let body: Value = resp.json().await.map_err(|_| {
        GatewayError::Other(format!(
            "Yuanbao sign-token returned non-JSON response: HTTP {status}"
        ))
    })?;

    if !body.is_object() {
        return Err(GatewayError::Other(
            "Yuanbao sign-token returned malformed response".to_string(),
        ));
    }
    let code = body
        .get("code")
        .and_then(utils::optional_int)
        .unwrap_or(0);
    if code != 0 {
        return Err(GatewayError::Other(format!(
            "Yuanbao sign-token failed: code={code}, msg={}",
            body.get("msg").map(utils::py_str).unwrap_or_default()
        )));
    }
    let data = match body.get("data") {
        Some(d) if d.is_object() => d,
        _ => {
            return Err(GatewayError::Other(
                "Yuanbao sign-token response missing data".to_string(),
            ))
        }
    };
    let token = utils::str_at(data, "token");
    let bot_id = {
        let v = utils::str_at(data, "bot_id");
        if v.is_empty() {
            cfg.bot_id.clone()
        } else {
            v
        }
    };
    let source = {
        let v = utils::str_at(data, "source");
        if v.is_empty() {
            cfg.source.clone()
        } else {
            v
        }
    };
    let duration = data
        .get("duration")
        .and_then(utils::optional_int)
        .unwrap_or(3600);
    if token.is_empty() || bot_id.is_empty() {
        return Err(GatewayError::Other(
            "Yuanbao sign-token response missing token or bot_id".to_string(),
        ));
    }

    let expires_at = crate::channel::now_f64() + (duration - TOKEN_REFRESH_MARGIN_SECONDS).max(60) as f64;
    let tok = YuanbaoToken {
        token,
        bot_id: bot_id.clone(),
        source,
        expires_at,
        duration,
    };
    *shared.token.lock().unwrap_or_else(|e| e.into_inner()) = Some(tok.clone());
    shared.set_bot_id(bot_id);
    Ok(tok)
}

/// `_auth_bind`.
async fn auth_bind(shared: &Shared, token: &YuanbaoToken) -> Result<()> {
    let cfg = &shared.config;
    let payload = protocol::encode_auth_bind_payload(
        &token.bot_id,
        &token.source,
        &token.token,
        &cfg.route_env,
        &cfg.app_version,
        &cfg.effective_operation_system(),
        &cfg.bot_version,
        &cfg.instance_id,
    );
    let response = request(
        shared,
        CMD_AUTH_BIND,
        MODULE_CONN_ACCESS,
        &payload,
        None,
        true,
    )
    .await?;
    let (code, message) = protocol::decode_status_response(&response);
    tracing::info!("Yuanbao auth-bind response: code={code} message={message}");
    if code == RET_SUCCESS || code == RET_ALREADY_AUTH {
        return Ok(());
    }
    if TOKEN_EXPIRED_CODES.contains(&code) {
        *shared.token.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
    Err(GatewayError::Other(format!(
        "Yuanbao auth-bind failed: code={code}, message={message}"
    )))
}

/// `_request` — encode + send a `ConnMsg`, optionally awaiting the response.
async fn request(
    shared: &Shared,
    cmd: &str,
    module: &str,
    payload: &[u8],
    msg_id: Option<String>,
    wait_response: bool,
) -> Result<Vec<u8>> {
    if !shared.connected.load(Ordering::SeqCst) {
        return Err(GatewayError::Other(
            "YuanbaoChannel is not connected".to_string(),
        ));
    }

    let request_id = msg_id.unwrap_or_else(|| format!("{cmd}_{}", shared.next_seq()));
    let receiver = if wait_response {
        let (tx, rx) = oneshot::channel();
        shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(request_id.clone(), tx);
        Some(rx)
    } else {
        None
    };

    let seq_no = shared.next_seq();
    let packet = protocol::encode_request(cmd, module, &request_id, seq_no, payload);
    if cmd != CMD_PING {
        tracing::debug!("Yuanbao send request: cmd={cmd} module={module} msg_id={request_id}");
    }
    shared.send_raw(&packet).await?;

    let receiver = match receiver {
        Some(rx) => rx,
        None => return Ok(Vec::new()),
    };

    let timeout = Duration::from_secs_f64(shared.config.request_timeout.max(0.001));
    let result = tokio::time::timeout(timeout, receiver).await;
    shared
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&request_id);
    match result {
        Ok(Ok(data)) => Ok(data),
        Ok(Err(_)) => Err(GatewayError::Other(
            "Yuanbao request failed: WebSocket closed".to_string(),
        )),
        Err(_) => Err(GatewayError::Other("Yuanbao request timeout".to_string())),
    }
}

fn fail_pending(shared: &Shared) {
    // Dropping the senders makes awaiting callers observe a closed channel,
    shared
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

// ===========================================================================
// Outbound: heartbeat + message body
// ===========================================================================

/// `_send_reply_heartbeat`.
async fn send_reply_heartbeat(shared: &Shared, subject: &ChannelSubject, heartbeat: i64) -> Result<()> {
    let meta = &subject.metadata;
    let from_account = {
        let from_meta = meta
            .get("bot_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        match from_meta {
            Some(v) => v,
            None => {
                let bot = shared.bot_id();
                if !bot.is_empty() {
                    bot
                } else {
                    shared.config.bot_id.clone()
                }
            }
        }
    };
    if from_account.is_empty() {
        return Ok(());
    }

    let group_code = meta
        .get("group_code")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !group_code.is_empty() || subject.chat_type == "group" {
        let gc = if group_code.is_empty() {
            subject.subject_id.clone()
        } else {
            group_code
        };
        let send_time = (crate::channel::now_f64() * 1000.0) as i64;
        let payload = protocol::encode_group_heartbeat_payload(&from_account, &gc, send_time, heartbeat);
        request(
            shared,
            CMD_SEND_GROUP_HEARTBEAT,
            MODULE_BIZ,
            &payload,
            None,
            false,
        )
        .await?;
        return Ok(());
    }

    let to_account = meta
        .get("reply_to_account")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| subject.subject_id.clone());
    if !to_account.is_empty() {
        let payload = protocol::encode_private_heartbeat_payload(&from_account, &to_account, heartbeat);
        request(
            shared,
            CMD_SEND_PRIVATE_HEARTBEAT,
            MODULE_BIZ,
            &payload,
            None,
            false,
        )
        .await?;
    }
    Ok(())
}

/// `_send_msg_body`.
async fn send_msg_body(shared: &Shared, subject: &ChannelSubject, msg_body: &[Value]) -> Result<()> {
    let meta = subject.metadata.clone();
    let from_account = {
        let from_meta = meta
            .get("bot_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        match from_meta {
            Some(v) => v,
            None => {
                let bot = shared.bot_id();
                if !bot.is_empty() {
                    bot
                } else {
                    shared.config.bot_id.clone()
                }
            }
        }
    };
    if from_account.is_empty() {
        return Err(GatewayError::Other(
            "YuanbaoChannel is not authenticated".to_string(),
        ));
    }

    let is_group = meta
        .get("group_code")
        .and_then(|v| v.as_str())
        .map(|s| !s.is_empty())
        .unwrap_or(false)
        || subject.chat_type == "group";
    let prefix = if is_group { "grp" } else { "c2c" };
    let outbound_msg_id = shared.next_business_msg_id(prefix);

    let result = send_msg_body_once(shared, subject, msg_body, &meta, &from_account, &outbound_msg_id).await;
    let _ = send_reply_heartbeat(shared, subject, HEARTBEAT_FINISH).await;

    let (code, message) = result?;
    if code != RET_SUCCESS {
        return Err(GatewayError::Other(format!(
            "Yuanbao send failed: code={code}, message={message}"
        )));
    }
    Ok(())
}

/// `_send_msg_body_once`.
async fn send_msg_body_once(
    shared: &Shared,
    subject: &ChannelSubject,
    msg_body: &[Value],
    meta: &HashMap<String, Value>,
    from_account: &str,
    outbound_msg_id: &str,
) -> Result<(i64, String)> {
    let group_code = meta
        .get("group_code")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let response = if !group_code.is_empty() || subject.chat_type == "group" {
        let gc = if group_code.is_empty() {
            subject.subject_id.clone()
        } else {
            group_code
        };
        let ref_msg_id = meta
            .get("msg_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let payload = protocol::encode_group_message_payload(
            &gc,
            from_account,
            msg_body,
            outbound_msg_id,
            "",
            "",
            None,
            &ref_msg_id,
            "",
        );
        request(
            shared,
            CMD_SEND_GROUP_MESSAGE,
            MODULE_BIZ,
            &payload,
            Some(outbound_msg_id.to_string()),
            true,
        )
        .await?
    } else {
        let to_account = meta
            .get("reply_to_account")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone());
        let private_from_group_code = meta
            .get("private_from_group_code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let payload = protocol::encode_c2c_message_payload(
            &to_account,
            from_account,
            msg_body,
            outbound_msg_id,
            0,
            None,
            &private_from_group_code,
            "",
        );
        request(
            shared,
            CMD_SEND_C2C_MESSAGE,
            MODULE_BIZ,
            &payload,
            Some(outbound_msg_id.to_string()),
            true,
        )
        .await?
    };

    let (code, message) = protocol::decode_status_response(&response);
    tracing::info!(
        "Yuanbao send response: code={code} message={message} business_msg_id={outbound_msg_id} from={}",
        utils::redact_account(from_account)
    );
    Ok((code, message))
}

// ===========================================================================
// Outbound: media upload (COS direct)
// ===========================================================================

/// `_upload_media_to_yuanbao`.
async fn upload_media_to_yuanbao(
    shared: &Shared,
    data: &[u8],
    filename: &str,
    mime_type: &str,
) -> Result<Value> {
    if data.is_empty() {
        return Err(GatewayError::Other("Yuanbao media is empty".to_string()));
    }
    if data.len() > MAX_MEDIA_SIZE_BYTES {
        let size_mb = data.len() as f64 / 1024.0 / 1024.0;
        return Err(GatewayError::Other(format!(
            "Yuanbao media is too large: {size_mb:.1} MB"
        )));
    }

    let token = sign_token(shared, false).await?;
    let credentials = get_cos_upload_credentials(shared, &token, filename).await?;
    let mut upload = put_media_to_cos(shared, data, mime_type, &credentials).await?;
    if mime_type.starts_with("image/") {
        let (width, height) = utils::parse_image_size(data);
        if let Some(obj) = upload.as_object_mut() {
            if width != 0 {
                obj.insert("width".to_string(), json!(width));
            }
            if height != 0 {
                obj.insert("height".to_string(), json!(height));
            }
        }
    }
    Ok(upload)
}

/// `_get_cos_upload_credentials`.
async fn get_cos_upload_credentials(
    shared: &Shared,
    token: &YuanbaoToken,
    filename: &str,
) -> Result<Value> {
    let cfg = &shared.config;
    let x_id = if !token.bot_id.is_empty() {
        token.bot_id.clone()
    } else {
        let bot = shared.bot_id();
        if !bot.is_empty() {
            bot
        } else {
            cfg.app_key.clone()
        }
    };
    let mut req = shared
        .http
        .post(format!("{}{}", cfg.effective_api_domain(), UPLOAD_INFO_PATH))
        .timeout(Duration::from_secs_f64(cfg.request_timeout.max(0.001)))
        .header("Content-Type", "application/json")
        .header("X-Token", token.token.clone())
        .header("X-ID", x_id)
        .header("X-Source", "web");
    if !cfg.route_env.is_empty() {
        req = req.header("X-Route-Env", cfg.route_env.clone());
    }
    let body = json!({
        "fileName": filename,
        "fileId": random_hex(16),
        "docFrom": "localDoc",
        "docOpenId": "",
    });
    let resp = req.json(&body).send().await?;
    if !resp.status().is_success() {
        return Err(GatewayError::Other(format!(
            "Yuanbao genUploadInfo HTTP {}",
            resp.status()
        )));
    }
    let result: Value = resp.json().await.map_err(|e| GatewayError::Other(e.to_string()))?;
    if !result.is_object() {
        return Err(GatewayError::Other(
            "Yuanbao genUploadInfo returned malformed response".to_string(),
        ));
    }
    let code = result.get("code").and_then(utils::optional_int).unwrap_or(0);
    if code != 0 {
        return Err(GatewayError::Other(format!(
            "Yuanbao genUploadInfo failed: code={code}, msg={}",
            result.get("msg").map(utils::py_str).unwrap_or_default()
        )));
    }
    let data = match result.get("data") {
        Some(d) if d.is_object() => d.clone(),
        _ => {
            if result.is_object() {
                result.clone()
            } else {
                return Err(GatewayError::Other(
                    "Yuanbao genUploadInfo response missing data".to_string(),
                ));
            }
        }
    };
    let mut missing = Vec::new();
    for key in ["bucketName", "location", "encryptTmpSecretId", "encryptTmpSecretKey"] {
        if !truthy_str(data.get(key)) {
            missing.push(key.to_string());
        }
    }
    if !missing.is_empty() {
        return Err(GatewayError::Other(format!(
            "Yuanbao genUploadInfo response missing fields: {}",
            missing.join(", ")
        )));
    }
    Ok(data)
}

fn truthy_str(value: Option<&Value>) -> bool {
    matches!(value, Some(v) if !v.is_null() && utils::py_str(v) != "")
}

/// `_put_media_to_cos`.
async fn put_media_to_cos(
    shared: &Shared,
    data: &[u8],
    mime_type: &str,
    credentials: &Value,
) -> Result<Value> {
    let bucket = utils::str_at(credentials, "bucketName");
    let region = utils::str_at(credentials, "region");
    let cos_key = utils::str_at(credentials, "location");
    let resource_url = utils::str_at(credentials, "resourceUrl");
    let secret_id = utils::str_at(credentials, "encryptTmpSecretId");
    let secret_key = utils::str_at(credentials, "encryptTmpSecretKey");
    let session_token = utils::str_at(credentials, "encryptToken");
    if bucket.is_empty() || cos_key.is_empty() || secret_id.is_empty() || secret_key.is_empty() {
        return Err(GatewayError::Other(
            "Yuanbao COS credentials are incomplete".to_string(),
        ));
    }

    let mut cos_host = String::new();
    if !bucket.is_empty() {
        cos_host = format!("{bucket}.cos.accelerate.myqcloud.com");
    }
    if cos_host.is_empty() && !region.is_empty() {
        cos_host = format!("{bucket}.cos.{region}.myqcloud.com");
    }
    let encoded_key = cos_encode_path(&cos_key);
    let clean_key = encoded_key.trim_start_matches('/');
    let cos_url = format!("https://{cos_host}/{clean_key}");

    let headers_to_sign: [(&str, &str); 3] = [
        ("host", cos_host.as_str()),
        ("content-type", mime_type),
        ("x-cos-security-token", session_token.as_str()),
    ];
    let now = crate::channel::now_secs() as i64;
    let start_time = credentials
        .get("startTime")
        .and_then(utils::optional_int)
        .filter(|v| *v != 0)
        .unwrap_or(now);
    let expired_time = credentials
        .get("expiredTime")
        .and_then(utils::optional_int)
        .filter(|v| *v != 0)
        .unwrap_or(now + 3600);
    let authorization = utils::cos_sign(
        "put",
        &format!("/{clean_key}"),
        &[],
        &headers_to_sign,
        &secret_id,
        &secret_key,
        start_time,
        (expired_time - now).max(60),
    );

    let timeout = Duration::from_secs_f64(shared.config.request_timeout.max(120.0));
    let resp = shared
        .http
        .put(&cos_url)
        .timeout(timeout)
        .header("Authorization", authorization)
        .header("Content-Type", mime_type)
        .header("x-cos-security-token", session_token)
        .body(data.to_vec())
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(GatewayError::Other(format!(
            "Yuanbao COS upload HTTP {}",
            resp.status()
        )));
    }

    let url = if resource_url.is_empty() {
        cos_url
    } else {
        resource_url
    };
    Ok(json!({
        "url": url,
        "uuid": format!("{:x}", md5::compute(data)),
        "size": data.len(),
    }))
}

/// `urllib.parse.quote(cos_key, safe="/")`.
fn cos_encode_path(key: &str) -> String {
    key.split('/')
        .map(utils::quote)
        .collect::<Vec<_>>()
        .join("/")
}

// ===========================================================================
// Inbound: resource download (fetch_remote_media)
// ===========================================================================

/// `_resolve_resource_download_url`.
async fn resolve_resource_download_url(shared: &Shared, url: &str) -> Result<String> {
    let resource_id = utils::resource_id_from_url(url);
    if resource_id.is_empty() {
        return Ok(url.to_string());
    }

    let mut token = sign_token(shared, false).await?;
    for attempt in 0..2 {
        match fetch_resource_download_url(shared, &resource_id, &token).await {
            Ok(found) => return Ok(found),
            Err(err) => {
                if err.status != Some(401) || attempt > 0 {
                    return Err(err.into_gateway());
                }
                *shared.token.lock().unwrap_or_else(|e| e.into_inner()) = None;
                token = sign_token(shared, true).await?;
            }
        }
    }
    Ok(url.to_string())
}

/// `_fetch_resource_download_url`.
async fn fetch_resource_download_url(
    shared: &Shared,
    resource_id: &str,
    token: &YuanbaoToken,
) -> std::result::Result<String, RespErr> {
    let cfg = &shared.config;
    let x_id = if !token.bot_id.is_empty() {
        token.bot_id.clone()
    } else {
        let bot = shared.bot_id();
        if !bot.is_empty() {
            bot
        } else {
            cfg.app_key.clone()
        }
    };
    let x_source = if !token.source.is_empty() {
        token.source.clone()
    } else if !cfg.source.is_empty() {
        cfg.source.clone()
    } else {
        "web".to_string()
    };

    let api_url = format!("{}{}", cfg.effective_api_domain(), RESOURCE_DOWNLOAD_PATH);
    let mut req = shared
        .http
        .get(&api_url)
        .timeout(Duration::from_secs_f64(cfg.request_timeout.max(0.001)))
        .query(&[("resourceId", resource_id)])
        .header("Content-Type", "application/json")
        .header("X-ID", x_id)
        .header("X-Token", token.token.clone())
        .header("X-Source", x_source);
    if !cfg.route_env.is_empty() {
        req = req.header("X-Route-Env", cfg.route_env.clone());
    }
    let resp = req.send().await.map_err(|e| RespErr {
        status: None,
        message: e.to_string(),
    })?;
    if !resp.status().is_success() {
        return Err(RespErr {
            status: Some(resp.status().as_u16()),
            message: format!("Yuanbao resource download HTTP {}", resp.status()),
        });
    }
    let payload: Value = resp.json().await.map_err(|e| RespErr {
        status: None,
        message: e.to_string(),
    })?;
    if !payload.is_object() {
        return Err(RespErr {
            status: None,
            message: "Yuanbao resource download returned malformed response".to_string(),
        });
    }
    let code = payload.get("code").and_then(utils::optional_int).unwrap_or(0);
    if code != 0 {
        return Err(RespErr {
            status: None,
            message: format!(
                "Yuanbao resource download failed: code={code}, msg={}",
                payload.get("msg").map(utils::py_str).unwrap_or_default()
            ),
        });
    }
    let data = match payload.get("data") {
        Some(d) if d.is_object() => d.clone(),
        _ => payload.clone(),
    };
    let real_url = {
        let a = utils::str_at(&data, "url");
        let b = utils::str_at(&data, "realUrl");
        if !a.trim().is_empty() {
            a.trim().to_string()
        } else {
            b.trim().to_string()
        }
    };
    if real_url.is_empty() {
        return Err(RespErr {
            status: None,
            message: "Yuanbao resource download response missing url".to_string(),
        });
    }
    Ok(utils::normalize_media_url(&real_url, &cfg.effective_api_domain()))
}

/// `fetch_remote_media` implementation.
async fn fetch_remote_media_impl(shared: &Shared, url: &str) -> Result<(Vec<u8>, String)> {
    let api_domain = shared.config.effective_api_domain();
    let url = utils::normalize_media_url(url, &api_domain);
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(GatewayError::Other(format!(
            "Yuanbao media URL is not HTTP(S): {url}"
        )));
    }
    let original_url = url.clone();
    let url = resolve_resource_download_url(shared, &url).await?;

    let mut headers: Vec<(String, String)> = Vec::new();
    if url == original_url {
        if let Ok(token) = sign_token(shared, false).await {
            headers.push(("X-Token".to_string(), token.token.clone()));
            headers.push(("Authorization".to_string(), format!("Bearer {}", token.token)));
        }
    }

    let mut req = shared.http.get(&url);
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        return Err(GatewayError::Other(format!(
            "Yuanbao media fetch HTTP {}",
            resp.status()
        )));
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let data = resp.bytes().await?.to_vec();
    Ok((data, content_type))
}

// ===========================================================================
// Inbound: frames + parsing
// ===========================================================================

async fn handle_binary_frame(shared: &Arc<Shared>, frame: &[u8]) {
    let message = match protocol::decode_conn_msg(frame) {
        Ok(m) => m,
        Err(_) => {
            tracing::warn!("Failed to decode Yuanbao ConnMsg");
            return;
        }
    };

    let head = message.head;
    let cmd_type = head.cmd_type;
    let cmd = head.cmd.clone();
    let msg_id = head.msg_id.clone();

    if cmd_type == CMD_TYPE_RESPONSE {
        let waiter = shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&msg_id);
        if let Some(tx) = waiter {
            let _ = tx.send(message.data);
        } else if cmd != CMD_PING {
            tracing::debug!("Yuanbao response without waiter: cmd={cmd} msg_id={msg_id}");
        }
        return;
    }

    if cmd_type != CMD_TYPE_PUSH {
        return;
    }

    if head.need_ack {
        let packet = protocol::encode_push_ack(&head, shared.next_seq());
        let _ = shared.send_raw(&packet).await;
    }

    if cmd == CMD_INBOUND_MESSAGE {
        let raw = String::from_utf8_lossy(&message.data).to_string();
        tracing::info!("Yuanbao inbound push received: msg_id={msg_id}");
        handle_text_frame(shared, &raw).await;
    } else if cmd == CMD_KICKOUT {
        tracing::warn!("Yuanbao channel received kickout push");
    } else if cmd == CMD_UPDATE_META {
        tracing::debug!("Yuanbao channel received update-meta push");
    } else {
        tracing::debug!("Yuanbao channel ignored push cmd={cmd}");
    }
}

async fn handle_text_frame(shared: &Arc<Shared>, frame: &str) {
    let payload: Value = match serde_json::from_str(frame) {
        Ok(v) => v,
        Err(_) => {
            tracing::debug!("Yuanbao text frame is not JSON");
            return;
        }
    };
    if !payload.is_object() {
        return;
    }

    if payload.get("msg_body").is_some() {
        let inbound_msg_id = first_nonempty_str(&payload, &["msg_id", "msg_key", "MsgKey"]);
        if !inbound_msg_id.is_empty() && !shared.remember_message_id(&inbound_msg_id) {
            tracing::info!("Yuanbao duplicate inbound skipped: msg_id={inbound_msg_id}");
            return;
        }
        tracing::info!(
            "Yuanbao inbound message: callback={} from={} group={} msg_id={}",
            value_display(payload.get("callback_command")),
            utils::redact_account(&utils::str_at(&payload, "from_account")),
            value_display(payload.get("group_code")),
            inbound_msg_id
        );
        let inbound = parse_inbound_value(
            &payload,
            &shared.config,
            &shared.channel_id,
            "yuanbao",
            shared.tenant_id.as_deref(),
            &shared.bot_id(),
        );
        if let Some(subject) = &inbound.channel_subject {
            let _ = send_reply_heartbeat(shared, subject, HEARTBEAT_RUNNING).await;
        }
    }

    if let Some(cb) = &shared.enqueue {
        cb(RawPayload::Json(payload));
    } else {
        tracing::warn!("Yuanbao inbound payload dropped: enqueue callback is not set");
    }
}

fn value_display(value: Option<&Value>) -> String {
    match value {
        Some(v) => utils::py_str(v),
        None => String::new(),
    }
}

fn first_nonempty_str(obj: &Value, keys: &[&str]) -> String {
    for key in keys {
        let v = utils::str_at(obj, key);
        if !v.is_empty() {
            return v;
        }
    }
    String::new()
}

/// Parse a raw Yuanbao payload into an `InboundMessage`.
fn parse_inbound_value(
    data: &Value,
    cfg: &YuanbaoConfig,
    channel_id: &str,
    channel_type: &str,
    tenant_id: Option<&str>,
    bot_id: &str,
) -> InboundMessage {
    let empty = Value::Object(serde_json::Map::new());
    let data = if data.is_object() { data } else { &empty };
    if data.get("msg_body").is_some() {
        parse_yuanbao_message(data, cfg, channel_id, channel_type, tenant_id, bot_id)
    } else {
        parse_legacy_message(data, cfg, channel_id, channel_type, tenant_id)
    }
}

fn parse_yuanbao_message(
    data: &Value,
    cfg: &YuanbaoConfig,
    channel_id: &str,
    channel_type: &str,
    tenant_id: Option<&str>,
    bot_id: &str,
) -> InboundMessage {
    let api_domain = cfg.effective_api_domain();
    let group_code = utils::str_at(data, "group_code");
    let from_account = {
        let v = utils::str_at(data, "from_account");
        if v.is_empty() {
            "unknown".to_string()
        } else {
            v
        }
    };
    let msg_id = first_nonempty_str(data, &["msg_id", "msg_key", "MsgKey"]);
    let subject_id = if !group_code.is_empty() {
        group_code.clone()
    } else {
        from_account.clone()
    };
    let is_group = !group_code.is_empty()
        || utils::str_at(data, "callback_command").starts_with("Group.");

    let mut parts: Vec<ContentPart> = Vec::new();
    if let Some(body) = data.get("msg_body").and_then(|v| v.as_array()) {
        for item in body {
            if !item.is_object() {
                continue;
            }
            let msg_type = utils::str_at(item, "msg_type");
            let content = match item.get("msg_content") {
                Some(c) if c.is_object() => c.clone(),
                _ => Value::Object(serde_json::Map::new()),
            };
            match msg_type.as_str() {
                v if v == MSG_TYPE_TEXT => {
                    let text = utils::str_at(&content, "text");
                    if !text.is_empty() {
                        parts.push(ContentPart::Text(TextContent { text }));
                    }
                }
                v if v == MSG_TYPE_IMAGE => {
                    let image_info = utils::first_image_info(&content);
                    let url = utils::first_media_url(&content, &api_domain);
                    if !url.is_empty() {
                        let info_ok = image_info.is_object()
                            && image_info.as_object().map(|m| !m.is_empty()).unwrap_or(false);
                        let (width, height, size) = if info_ok {
                            (
                                utils::optional_int_opt(image_info.get("width")),
                                utils::optional_int_opt(image_info.get("height")),
                                utils::optional_int_opt(image_info.get("size")),
                            )
                        } else {
                            (None, None, None)
                        };
                        parts.push(ContentPart::Image(ImageContent {
                            url,
                            width: width.map(|x| x.max(0) as u64),
                            height: height.map(|x| x.max(0) as u64),
                            size: size.map(|x| x.max(0) as u64),
                            ..Default::default()
                        }));
                    }
                }
                v if v == MSG_TYPE_FILE => {
                    let url = utils::normalize_media_url(&utils::str_at(&content, "url"), &api_domain);
                    parts.push(ContentPart::File(FileContent {
                        url,
                        filename: utils::content_filename(&content),
                        size: utils::optional_int_opt(content.get("file_size")).map(|x| x.max(0) as u64),
                        ..Default::default()
                    }));
                }
                v if v == MSG_TYPE_SOUND => {
                    let url = utils::first_media_url(&content, &api_domain);
                    if !url.is_empty() {
                        parts.push(ContentPart::Audio(AudioContent {
                            url,
                            size: utils::optional_int_opt(content.get("file_size")).map(|x| x.max(0) as u64),
                            ..Default::default()
                        }));
                    }
                }
                v if v == MSG_TYPE_VIDEO => {
                    let url = utils::first_media_url(&content, &api_domain);
                    if !url.is_empty() {
                        let thumb = utils::normalize_media_url(&utils::str_at(&content, "thumb_url"), &api_domain);
                        parts.push(ContentPart::Video(VideoContent {
                            url,
                            size: utils::optional_int_opt(content.get("file_size")).map(|x| x.max(0) as u64),
                            thumbnail_url: if thumb.is_empty() { None } else { Some(thumb) },
                            ..Default::default()
                        }));
                    }
                }
                _ => {
                    let mut text = utils::str_at(&content, "text");
                    if text.is_empty() {
                        text = utils::str_at(&content, "desc");
                    }
                    if !text.is_empty() {
                        parts.push(ContentPart::Text(TextContent { text }));
                    }
                }
            }
        }
    }
    if parts.is_empty() {
        parts.push(ContentPart::Text(TextContent { text: String::new() }));
    }

    let trace_id = {
        let mut t = String::new();
        if let Some(log_ext) = data.get("log_ext") {
            if log_ext.is_object() {
                t = utils::str_at(log_ext, "trace_id");
            }
        }
        let direct = utils::str_at(data, "trace_id");
        if !direct.is_empty() {
            direct
        } else {
            t
        }
    };

    let mut metadata: HashMap<String, Value> = HashMap::new();
    metadata.insert(
        "callback_command".to_string(),
        data.get("callback_command").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert("from_account".to_string(), json!(from_account.clone()));
    metadata.insert("reply_to_account".to_string(), json!(from_account.clone()));
    metadata.insert(
        "to_account".to_string(),
        data.get("to_account").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert(
        "group_id".to_string(),
        data.get("group_id").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert("group_code".to_string(), json!(group_code.clone()));
    metadata.insert(
        "group_name".to_string(),
        data.get("group_name").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert("msg_id".to_string(), json!(msg_id));
    let msg_key = {
        let a = utils::str_at(data, "msg_key");
        if !a.is_empty() {
            a
        } else {
            utils::str_at(data, "MsgKey")
        }
    };
    metadata.insert("msg_key".to_string(), json!(msg_key));
    metadata.insert(
        "msg_seq".to_string(),
        data.get("msg_seq").cloned().unwrap_or_else(|| json!(0)),
    );
    metadata.insert(
        "msg_random".to_string(),
        data.get("msg_random").cloned().unwrap_or_else(|| json!(0)),
    );
    metadata.insert(
        "msg_time".to_string(),
        data.get("msg_time").cloned().unwrap_or_else(|| json!(0)),
    );
    metadata.insert(
        "bot_owner_id".to_string(),
        data.get("bot_owner_id").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert(
        "private_from_group_code".to_string(),
        data.get("private_from_group_code")
            .cloned()
            .unwrap_or_else(|| json!("")),
    );
    metadata.insert("trace_id".to_string(), json!(trace_id));
    metadata.insert("bot_id".to_string(), json!(bot_id));

    InboundMessage {
        channel_id: channel_id.to_string(),
        channel_type: channel_type.to_string(),
        tenant_id: tenant_id.map(|s| s.to_string()),
        channel_subject: Some(ChannelSubject {
            subject_id: subject_id.clone(),
            first_seen: None,
            last_seen: None,
            display_name: utils::str_at(data, "sender_nickname"),
            chat_type: if is_group {
                "group".to_string()
            } else {
                "direct".to_string()
            },
            metadata: metadata.clone(),
        }),
        channel_session_id: if !group_code.is_empty() {
            group_code
        } else {
            from_account
        },
        content: parts,
        group_context: None,
        metadata,
        timestamp: crate::channel::now_f64(),
    }
}

fn parse_legacy_message(
    data: &Value,
    cfg: &YuanbaoConfig,
    channel_id: &str,
    channel_type: &str,
    tenant_id: Option<&str>,
) -> InboundMessage {
    let api_domain = cfg.effective_api_domain();
    let sender_id = {
        let v = utils::str_at(data, "from_user");
        if v.is_empty() {
            "unknown".to_string()
        } else {
            v
        }
    };
    let conversation_id = utils::str_at(data, "conversation_id");
    let msg_type = {
        let v = utils::str_at(data, "message_type");
        if v.is_empty() {
            "text".to_string()
        } else {
            v
        }
    };
    let content = match data.get("content") {
        Some(c) if c.is_object() => c.clone(),
        _ => Value::Object(serde_json::Map::new()),
    };

    let mut parts: Vec<ContentPart> = Vec::new();
    match msg_type.as_str() {
        "text" => parts.push(ContentPart::Text(TextContent {
            text: utils::str_at(&content, "text"),
        })),
        "image" => parts.push(ContentPart::Image(ImageContent {
            url: utils::normalize_media_url(&utils::str_at(&content, "url"), &api_domain),
            ..Default::default()
        })),
        "file" => parts.push(ContentPart::File(FileContent {
            url: utils::normalize_media_url(&utils::str_at(&content, "url"), &api_domain),
            filename: utils::str_at(&content, "filename"),
            ..Default::default()
        })),
        "audio" => parts.push(ContentPart::Audio(AudioContent {
            url: utils::normalize_media_url(&utils::str_at(&content, "url"), &api_domain),
            ..Default::default()
        })),
        "video" => parts.push(ContentPart::Video(VideoContent {
            url: utils::normalize_media_url(&utils::str_at(&content, "url"), &api_domain),
            ..Default::default()
        })),
        _ => {
            let text = utils::str_at(&content, "text");
            let text = if text.is_empty() {
                content.to_string()
            } else {
                text
            };
            parts.push(ContentPart::Text(TextContent { text }));
        }
    }

    let mut metadata: HashMap<String, Value> = HashMap::new();
    metadata.insert(
        "message_id".to_string(),
        data.get("message_id").cloned().unwrap_or_else(|| json!("")),
    );
    metadata.insert("conversation_id".to_string(), json!(conversation_id.clone()));
    metadata.insert("raw_type".to_string(), json!(msg_type));

    InboundMessage {
        channel_id: channel_id.to_string(),
        channel_type: channel_type.to_string(),
        tenant_id: tenant_id.map(|s| s.to_string()),
        channel_subject: Some(ChannelSubject {
            subject_id: sender_id.clone(),
            first_seen: None,
            last_seen: None,
            display_name: sender_id,
            chat_type: "direct".to_string(),
            metadata: metadata.clone(),
        }),
        channel_session_id: conversation_id,
        content: parts,
        group_context: None,
        metadata,
        timestamp: crate::channel::now_f64(),
    }
}

// ===========================================================================
// Connection lifecycle: connect / receive / ping / reconnect
// ===========================================================================

/// `_connect_once`.
async fn connect_once(shared: &Arc<Shared>, force_token: bool) -> Result<()> {
    if shared.stopping.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Abandon any previous receive task and socket.
    if let Some(handle) = shared
        .receive_task
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        handle.abort();
    }
    {
        let mut guard = shared.sink.lock().await;
        if let Some(mut sink) = guard.take() {
            let _ = sink.close().await;
        }
    }

    let token = sign_token(shared, force_token).await?;

    let ws_url = shared.config.effective_ws_url();
    let timeout = Duration::from_secs_f64(shared.config.connect_timeout.max(0.001));
    let connect = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(ws_url.as_str())).await;
    let (ws, _resp) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            return Err(GatewayError::Other(format!(
                "Yuanbao WebSocket connect failed: {e}"
            )))
        }
        Err(_) => {
            return Err(GatewayError::Other(
                "Yuanbao WebSocket connect timeout".to_string(),
            ))
        }
    };

    let (sink, reader) = ws.split();
    *shared.sink.lock().await = Some(sink);
    shared.connected.store(true, Ordering::SeqCst);

    let receive_handle = tokio::spawn(run_receive(shared.clone(), reader));
    *shared
        .receive_task
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(receive_handle);

    if let Err(e) = auth_bind(shared, &token).await {
        shared.connected.store(false, Ordering::SeqCst);
        {
            let mut guard = shared.sink.lock().await;
            if let Some(mut sink) = guard.take() {
                let _ = sink.close().await;
            }
        }
        if let Some(handle) = shared
            .receive_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
        return Err(e);
    }
    Ok(())
}

/// Per-connection receive loop + ping timer.
async fn run_receive(shared: Arc<Shared>, mut reader: WsReader) {
    let interval_secs = shared.config.heartbeat_interval.max(0.001);
    let mut interval = tokio::time::interval(Duration::from_secs_f64(interval_secs));
    // tokio fires the first tick immediately; consume it to match the
    // "sleep first" ping loop.
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !shared.connected.load(Ordering::SeqCst) {
                    continue;
                }
                let _ = request(&shared, CMD_PING, MODULE_CONN_ACCESS, &[], None, false).await;
            }
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(Message::Binary(data))) => {
                        handle_binary_frame(&shared, &data).await;
                    }
                    Some(Ok(Message::Text(text))) => {
                        handle_text_frame(&shared, text.as_str()).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::warn!("Yuanbao receive loop error: {e}");
                        break;
                    }
                }
            }
        }
    }

    shared.connected.store(false, Ordering::SeqCst);
    {
        let mut guard = shared.sink.lock().await;
        *guard = None;
    }
    fail_pending(&shared);
    let _ = shared.disconnect_tx.send(());
}

/// Reconnect supervisor: waits for disconnects and re-dials with backoff.
async fn supervisor(shared: Arc<Shared>) {
    let mut rx = match shared
        .disconnect_rx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        Some(rx) => rx,
        None => return,
    };
    let delays = [1.0f64, 2.0, 5.0, 10.0, 30.0, 60.0];

    while !shared.stopping.load(Ordering::SeqCst) {
        if rx.recv().await.is_none() {
            return;
        }
        if shared.stopping.load(Ordering::SeqCst) || !shared.should_reconnect.load(Ordering::SeqCst) {
            return;
        }
        while rx.try_recv().is_ok() {}

        let mut attempt = 0usize;
        loop {
            if shared.stopping.load(Ordering::SeqCst) || !shared.should_reconnect.load(Ordering::SeqCst) {
                return;
            }
            let delay = delays[attempt.min(delays.len() - 1)];
            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
            if shared.stopping.load(Ordering::SeqCst) || !shared.should_reconnect.load(Ordering::SeqCst) {
                return;
            }
            match connect_once(&shared, false).await {
                Ok(()) => {
                    tracing::info!("YuanbaoChannel reconnected (bot_id={})", shared.bot_id());
                    break;
                }
                Err(_) => {
                    attempt += 1;
                    tracing::warn!("Yuanbao reconnect failed; retrying");
                }
            }
        }
    }
}

// ===========================================================================
// Channel impl
// ===========================================================================

impl YuanbaoChannel {
    fn shared_ref(&self) -> Result<Arc<Shared>> {
        self.shared
            .clone()
            .ok_or_else(|| GatewayError::Other("YuanbaoChannel is not connected".to_string()))
    }

    async fn upload_media_to_yuanbao(
        &self,
        data: &[u8],
        filename: &str,
        mime_type: &str,
    ) -> Result<Value> {
        let shared = self.shared_ref()?;
        upload_media_to_yuanbao(&shared, data, filename, mime_type).await
    }

    /// `_media_url_msg_body` (URL-only fallback used when upload fails).
    fn media_url_msg_body(&self, part: &ContentPart) -> Vec<Value> {
        let api_domain = self.config.effective_api_domain();
        let url = utils::normalize_media_url(part.url().unwrap_or(""), &api_domain);
        if url.is_empty() {
            return Vec::new();
        }
        match part {
            ContentPart::Image(img) => {
                let alt = if img.alt_text.is_empty() {
                    let base = utils::basename_from_url(&url);
                    if base.is_empty() {
                        "image".to_string()
                    } else {
                        base
                    }
                } else {
                    img.alt_text.clone()
                };
                let mime = img
                    .mime_type
                    .clone()
                    .filter(|m| !m.is_empty())
                    .unwrap_or_else(|| utils::guess_mime_type(&url));
                protocol::build_image_msg_body(
                    &url,
                    &alt,
                    &alt,
                    img.size.unwrap_or(0) as i64,
                    img.width.unwrap_or(0) as i64,
                    img.height.unwrap_or(0) as i64,
                    &mime,
                )
            }
            ContentPart::File(_) | ContentPart::Audio(_) | ContentPart::Video(_) => {
                let base_mime = part
                    .mime_type()
                    .filter(|m| !m.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| utils::guess_mime_type(&url));
                let filename = utils::resolve_media_filename(part, &base_mime);
                protocol::build_file_msg_body(&url, &filename, &filename, part.size().unwrap_or(0) as i64)
            }
            _ => Vec::new(),
        }
    }

    /// `_media_part_to_msg_body` — errors are swallowed into text fallbacks.
    async fn media_part_to_msg_body(&self, part: &ContentPart) -> Vec<Value> {
        let label = part.label_with_filename();
        if let ContentPart::Text(t) = part {
            return protocol::encode_text_body(&t.text);
        }
        if !matches!(
            part,
            ContentPart::Image(_) | ContentPart::File(_) | ContentPart::Audio(_) | ContentPart::Video(_)
        ) {
            return protocol::encode_text_body(&format!("[{label}]"));
        }

        match self.load_media_bytes(part).await {
            Ok((data, mut mime_type)) => {
                let filename = utils::resolve_media_filename(part, &mime_type);
                if mime_type.is_empty() || mime_type == "application/octet-stream" {
                    mime_type = utils::guess_mime_type(&filename);
                }
                match self.upload_media_to_yuanbao(&data, &filename, &mime_type).await {
                    Ok(upload) => {
                        let url = upload.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let uuid = upload
                            .get("uuid")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let size = upload
                            .get("size")
                            .and_then(utils::optional_int)
                            .filter(|v| *v != 0)
                            .unwrap_or(data.len() as i64);
                        if let ContentPart::Image(img) = part {
                            let up_width = upload.get("width").and_then(utils::optional_int).unwrap_or(0);
                            let up_height = upload.get("height").and_then(utils::optional_int).unwrap_or(0);
                            let width = img.width.filter(|w| *w != 0).map(|w| w as i64).unwrap_or(up_width);
                            let height = img.height.filter(|h| *h != 0).map(|h| h as i64).unwrap_or(up_height);
                            return protocol::build_image_msg_body(
                                &url, &uuid, &filename, size, width, height, &mime_type,
                            );
                        }
                        return protocol::build_file_msg_body(&url, &filename, &uuid, size);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to upload Yuanbao media: {e}");
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to load Yuanbao media: {e}");
            }
        }

        let url_body = self.media_url_msg_body(part);
        if !url_body.is_empty() {
            return url_body;
        }
        if part.data().is_some() || part.local_path().is_some() {
            return protocol::encode_text_body(&format!("[{label} (local file upload failed)]"));
        }
        protocol::encode_text_body(&format!("[{label} (not deliverable on Yuanbao)]"))
    }
}

#[async_trait]
impl Channel for YuanbaoChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }

    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            typing_keepalive_interval: 2.0,
            send_rate_limit: Some((2, 5.0)),
            show_thinking: false,
            show_tool_hints: true,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        let missing = self.config.missing_credentials();
        if !missing.is_empty() {
            return Err(GatewayError::MissingCredentials {
                kind: "yuanbao".to_string(),
                missing,
            });
        }

        self.config.normalize();

        let http = self.core.ensure_http().await?;
        let shared = Arc::new(Shared::new(
            self.config.clone(),
            http,
            self.core.enqueue_sender(),
            self.core.channel_id().to_string(),
            self.core.tenant_id().map(|s| s.to_string()),
        ));
        shared.set_bot_id(self.config.bot_id.clone());
        self.shared = Some(shared.clone());

        if self.config.probe_mode == "sign_token" {
            let token = sign_token(&shared, true).await?;
            let short: String = token.bot_id.chars().take(12).collect();
            tracing::info!(
                "YuanbaoChannel probe ok (bot_id={}, api_domain={})",
                short,
                self.config.api_domain
            );
            return Ok(());
        }

        if let Err(e) = connect_once(&shared, true).await {
            self.shared = None;
            return Err(e);
        }
        shared.should_reconnect.store(true, Ordering::SeqCst);

        if let Some(task) = self.supervisor.take() {
            task.abort();
        }
        self.supervisor = Some(tokio::spawn(supervisor(shared.clone())));

        let short: String = shared.bot_id().chars().take(12).collect();
        tracing::info!(
            "YuanbaoChannel started (bot_id={}, api_domain={})",
            short,
            self.config.api_domain
        );
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(shared) = self.shared.clone() {
            shared.stopping.store(true, Ordering::SeqCst);
            shared.should_reconnect.store(false, Ordering::SeqCst);
            shared.connected.store(false, Ordering::SeqCst);

            if let Some(task) = self.supervisor.take() {
                task.abort();
            }
            if let Some(handle) = shared
                .receive_task
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                handle.abort();
            }
            {
                let mut guard = shared.sink.lock().await;
                if let Some(mut sink) = guard.take() {
                    let _ = sink.close().await;
                }
            }
            fail_pending(&shared);
            self.shared = None;
        }
        self.core.set_connection_session(None);
        tracing::info!("YuanbaoChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let shared = self.shared_ref()?;
        let body = protocol::encode_text_body(text);
        send_msg_body(&shared, subject, &body).await
    }

    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()> {
        let mut msg_body: Vec<Value> = Vec::new();
        for part in parts {
            if let ContentPart::Text(t) = part {
                if !t.text.is_empty() {
                    msg_body.extend(protocol::encode_text_body(&t.text));
                }
            } else {
                msg_body.extend(self.media_part_to_msg_body(part).await);
            }
        }
        if msg_body.is_empty() {
            return Ok(());
        }
        let shared = self.shared_ref()?;
        send_msg_body(&shared, subject, &msg_body).await
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let msg_body = self.media_part_to_msg_body(media).await;
        if let Some(text) = utils::single_text_body_text(&msg_body) {
            return self.send_text(subject, &text).await;
        }
        if msg_body.is_empty() {
            return Ok(());
        }
        let shared = self.shared_ref()?;
        send_msg_body(&shared, subject, &msg_body).await
    }

    async fn send_typing_indicator(&mut self, subject: &ChannelSubject) -> Result<()> {
        let shared = self.shared_ref()?;
        send_reply_heartbeat(&shared, subject, HEARTBEAT_RUNNING).await
    }

    async fn fetch_remote_media(&self, url: &str) -> Result<(Vec<u8>, String)> {
        let shared = self.shared_ref()?;
        fetch_remote_media_impl(&shared, url).await
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let value = match raw {
            RawPayload::Json(v) => v.clone(),
            RawPayload::Text(s) => serde_json::from_str(s)?,
            RawPayload::Preparsed(m) => serde_json::to_value(m)?,
        };
        let bot_id = self
            .shared
            .as_ref()
            .map(|s| s.bot_id())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.config.bot_id.clone());
        Ok(parse_inbound_value(
            &value,
            &self.config,
            self.core.channel_id(),
            "yuanbao",
            self.core.tenant_id(),
            &bot_id,
        ))
    }
}
