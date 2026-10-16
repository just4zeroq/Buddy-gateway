//! Channel adapter layer.
//!
//! Each submodule implements the [`crate::channel::Channel`] contract for one
//! IM platform. This module provides:
//!   - [`ChannelKind`] — canonical channel kinds (mirrors the Python enum).
//!   - [`build_channel`] — the JSON-driven construction dispatcher.
//!   - the `channel_adapter!` macro that generates the repetitive struct /
//!     `new` / `ChannelConfigLike` scaffolding for every adapter.

use serde::Deserialize;
use serde_json::Value;

use crate::channel::{Channel, MessageProcessor};
use crate::errors::{GatewayError, Result};

pub mod dingtalk;
pub mod discord;
pub mod feishu;
pub mod loopback;
pub mod mqtt;
pub mod qq;
pub mod telegram;
pub mod wecom;
pub mod weixin;
pub mod xiaoyi;
pub mod yuanbao;

// ===========================================================================
// Canonical channel kinds (parity with Python ChannelKind / _CHANNEL_MAP).
// ===========================================================================

/// Built-in channel kinds that [`build_channel`] can construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    Discord,
    Feishu,
    DingTalk,
    Qq,
    WeCom,
    Weixin,
    Yuanbao,
    Xiaoyi,
    Mqtt,
    Telegram,
}

impl ChannelKind {
    /// Parse a kind string (case-sensitive, lower-cased internally).
    pub fn from_str(s: &str) -> Option<ChannelKind> {
        match s.to_ascii_lowercase().as_str() {
            "discord" => Some(ChannelKind::Discord),
            "feishu" => Some(ChannelKind::Feishu),
            "dingtalk" => Some(ChannelKind::DingTalk),
            "qq" => Some(ChannelKind::Qq),
            "wecom" => Some(ChannelKind::WeCom),
            "weixin" => Some(ChannelKind::Weixin),
            "yuanbao" => Some(ChannelKind::Yuanbao),
            "xiaoyi" => Some(ChannelKind::Xiaoyi),
            "mqtt" => Some(ChannelKind::Mqtt),
            "telegram" => Some(ChannelKind::Telegram),
            _ => None,
        }
    }

    /// The canonical string key for this kind.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelKind::Discord => "discord",
            ChannelKind::Feishu => "feishu",
            ChannelKind::DingTalk => "dingtalk",
            ChannelKind::Qq => "qq",
            ChannelKind::WeCom => "wecom",
            ChannelKind::Weixin => "weixin",
            ChannelKind::Yuanbao => "yuanbao",
            ChannelKind::Xiaoyi => "xiaoyi",
            ChannelKind::Mqtt => "mqtt",
            ChannelKind::Telegram => "telegram",
        }
    }
}

/// All supported channel kind keys.
pub const SUPPORTED_CHANNEL_KINDS: &[&str] = &[
    "discord", "feishu", "dingtalk", "qq", "wecom", "weixin", "yuanbao", "xiaoyi", "mqtt",
    "telegram",
];

// ===========================================================================
// DRY scaffolding macro.
//
// Generates, for a platform adapter:
//   - the channel struct holding a `ChannelCore` + the platform `Config`;
//   - `new(processor, config, channel_id, tenant_id, debounce_seconds)` which
//     resolves a concrete channel id (config > arg > random uuid), applies the
//     config's display flags to the constraints, and stamps the channel type;
//   - the `ChannelConfigLike` impl (base accessor, required creds, missing creds).
//
// The concrete `impl Channel` (start/stop/send_*/parse_inbound) is written per
// platform because those are the platform-specific bits.
// ===========================================================================

#[macro_export]
macro_rules! channel_adapter {
    (
        $(#[$struct_meta:meta])*
        $chan:ident, $cfg:ident, $type:literal,
        required = [$($req:ident),* $(,)?],
        fields = { $($fname:ident : $fty:ty),* $(,)? }
    ) => {
        $(#[$struct_meta])*
        pub struct $chan {
            core: $crate::channel::ChannelCore,
            config: $cfg,
            $(#[allow(dead_code)] $fname: $fty,)*
        }

        impl $chan {
            pub fn new(
                processor: $crate::channel::MessageProcessor,
                config: $cfg,
                channel_id: Option<String>,
                tenant_id: Option<String>,
                debounce_seconds: f64,
            ) -> Self {
                use $crate::channel::ChannelConfigLike;
                let cid = channel_id
                    .or_else(|| config.base().channel_id.clone())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let tid = tenant_id.or_else(|| config.base().tenant_id.clone());
                let mut constraints =
                    <$chan as $crate::channel::Channel>::default_constraints();
                constraints.show_thinking = config.base().show_thinking;
                constraints.show_tool_hints = config.base().show_tool_hints;
                let group_config = config.base().group_context.clone();
                let mut core = $crate::channel::ChannelCore::new(
                    cid,
                    tid,
                    debounce_seconds,
                    constraints,
                    group_config,
                );
                core.set_channel_type($type);
                core.set_processor(processor);
                let mut chan = $chan {
                    core,
                    config,
                    $($fname: ::std::default::Default::default(),)*
                };
                {
                    // Python `_default_constraints(self)` may read the config
                    // (e.g. Telegram's typing keepalive depends on show_typing).
                    let mut c = chan.core().constraints().clone();
                    chan.configure_constraints(&mut c);
                    chan.core_mut().apply_constraints(c);
                }
                chan
            }

            pub fn config(&self) -> &$cfg {
                &self.config
            }

            pub fn core_ref(&self) -> &$crate::channel::ChannelCore {
                &self.core
            }
        }

        impl $crate::channel::ChannelConfigLike for $cfg {
            fn base(&self) -> &$crate::channel::ChannelConfig {
                &self.base
            }
            fn required_credentials(&self) -> &'static [&'static str] {
                &[$(stringify!($req)),*]
            }
            fn missing_credentials(&self) -> Vec<String> {
                #[allow(unused_mut)]
                let mut v = Vec::new();
                $(
                    if self.$req.is_empty() {
                        v.push(stringify!($req).to_string());
                    }
                )*
                v
            }
        }
    };
}

// ===========================================================================
// JSON-driven construction dispatcher (mirrors Python `_load_channel`).
// ===========================================================================

/// Field aliases per channel kind, mirroring the Python `field_aliases`.
/// Applied only when the canonical key is absent.
fn aliases_for(channel_type: &str) -> &'static [(&'static str, &'static str)] {
    match channel_type {
        "discord" => &[("token", "bot_token")],
        "dingtalk" => &[("client_id", "app_key"), ("client_secret", "app_secret")],
        "qq" => &[("client_secret", "secret")],
        "yuanbao" => &[
            ("appId", "app_key"),
            ("appKey", "app_key"),
            ("client_id", "app_key"),
            ("appSecret", "app_secret"),
            ("client_secret", "app_secret"),
            ("api_base", "api_domain"),
            ("apiBase", "api_domain"),
            ("apiDomain", "api_domain"),
            ("websocket_url", "ws_url"),
            ("webSocketURL", "ws_url"),
            ("wsUrl", "ws_url"),
            ("routeEnv", "route_env"),
            ("probeMode", "probe_mode"),
            ("botId", "bot_id"),
            ("identifier", "bot_id"),
            ("operationSystem", "app_operation_system"),
            ("appOperationSystem", "app_operation_system"),
        ],
        _ => &[],
    }
}

/// Python-truthiness for a JSON value (used for `c2c_streaming` / `enabled`).
fn as_bool_like(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Number(n) => Some(n.as_i64().map(|i| i != 0).unwrap_or(true)),
        Value::String(s) => Some(!matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off" | ""
        )),
        Value::Null => None,
        _ => Some(true),
    }
}

/// Split a comma/whitespace-delimited ID string (or list) into a validated,
/// de-duplicated list of ASCII-digit IDs. Mirrors Discord/Telegram `from_dict`
/// (`re.split(r"[,\s]+", ...)` + `str(item).isascii() and isdigit()` + dedup).
/// Returns `None` when validation fails (caller keeps the original value so the
/// typed deserializer surfaces the error).
fn split_ids(value: &Value) -> Option<Value> {
    let items: Vec<String> = match value {
        Value::String(s) => {
            if s.trim().is_empty() {
                Vec::new()
            } else {
                s.split(|c: char| c == ',' || c.is_whitespace())
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect()
            }
        }
        Value::Array(a) => a
            .iter()
            .map(|v| v.as_str().unwrap_or("").to_string())
            .collect(),
        _ => return None,
    };
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for item in items {
        if item.is_empty() {
            continue;
        }
        if !item.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if seen.insert(item.clone()) {
            out.push(Value::String(item));
        }
    }
    Some(Value::Array(out))
}

/// Per-channel config coercions mirroring the Python `from_dict` overrides.
fn coerce_config(channel_type: &str, obj: &mut serde_json::Map<String, Value>) {
    // group_context: coerce numeric/bool strings so serde can deserialize
    // (Python `from_dict` does int(str(..)) / bool(..)).
    if let Some(Value::Object(gc)) = obj.get_mut("group_context") {
        if let Some(Some(n)) = gc.get("history_limit").and_then(|v| v.as_str()).map(|s| s.parse::<u64>().ok()) {
            gc.insert("history_limit".to_string(), Value::from(n));
        }
        if let Some(Some(n)) = gc
            .get("history_ttl_seconds")
            .and_then(|v| v.as_str())
            .map(|s| s.parse::<f64>().ok())
        {
            gc.insert("history_ttl_seconds".to_string(), Value::from(n));
        }
        for k in ["enabled", "clear_after_reply"] {
            if let Some(v) = gc.get(k) {
                if let Some(b) = as_bool_like(v) {
                    gc.insert(k.to_string(), Value::Bool(b));
                }
            }
        }
    }

    match channel_type {
        "discord" => {
            for k in ["allowed_user_ids", "allowed_channel_ids"] {
                if let Some(v) = obj.get(k).cloned() {
                    if let Some(list) = split_ids(&v) {
                        obj.insert(k.to_string(), list);
                    }
                }
            }
        }
        "telegram" => {
            if let Some(v) = obj.get("allowed_user_ids").cloned() {
                if let Some(list) = split_ids(&v) {
                    obj.insert("allowed_user_ids".to_string(), list);
                }
            }
        }
        "qq" => {
            // `c2c_streaming`: string/bool coercion; absent (or legacy
            // `streaming`) defaults ON (mirrors QQConfig.from_dict).
            match obj.get("c2c_streaming").cloned() {
                Some(v) => {
                    if let Some(b) = as_bool_like(&v) {
                        obj.insert("c2c_streaming".to_string(), Value::Bool(b));
                    }
                }
                None => {
                    obj.insert("c2c_streaming".to_string(), Value::Bool(true));
                }
            }
            // A supplied `group_context` dict without `enabled` implies enabled.
            if let Some(Value::Object(gc)) = obj.get_mut("group_context") {
                if !gc.contains_key("enabled") {
                    gc.insert("enabled".to_string(), Value::Bool(true));
                }
            }
        }
        _ => {}
    }
}

/// Normalize a raw JSON config the way Python `ChannelConfig.from_dict` does:
/// apply field aliases (canonical wins), strip surrounding whitespace from
/// string values, and run per-channel coercions. Unknown keys are ignored by
/// `serde` automatically.
pub fn normalize_config(channel_type: &str, config: &Value) -> Value {
    let mut v = config.clone();
    if let Some(obj) = v.as_object_mut() {
        for (alias, canonical) in aliases_for(channel_type) {
            if !obj.contains_key(*canonical) {
                if let Some(val) = obj.get(*alias).cloned() {
                    obj.insert((*canonical).to_string(), val);
                }
            }
        }
        for val in obj.values_mut() {
            if let Value::String(s) = val {
                *s = s.trim().to_string();
            }
        }
        coerce_config(channel_type, obj);
    }
    v
}

/// Fail fast when required credentials are missing (mirrors `missing_credentials`).
fn ensure_credentials<C: crate::channel::ChannelConfigLike>(
    kind: &str,
    cfg: &C,
) -> Result<()> {
    let missing = cfg.missing_credentials();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(GatewayError::MissingCredentials {
            kind: kind.to_string(),
            missing,
        })
    }
}

/// Build a channel object from a kind string + JSON config.
///
/// `channel_id` / `tenant_id` supplied here override the values embedded in the
/// config (consistent with the Python manager: explicit > config > generated).
/// The config is normalized (aliases + string stripping) and its required
/// credentials are validated before construction.
pub fn build_channel(
    channel_type: &str,
    config: &Value,
    processor: MessageProcessor,
    channel_id: Option<String>,
    tenant_id: Option<String>,
) -> Result<Box<dyn Channel>> {
    let kind = ChannelKind::from_str(channel_type)
        .ok_or_else(|| GatewayError::UnknownChannel(channel_type.to_string()))?;
    let config = normalize_config(channel_type, config);
    match kind {
        ChannelKind::Discord => {
            let cfg = discord::DiscordConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(discord::DiscordChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Feishu => {
            let cfg = feishu::FeishuConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(feishu::FeishuChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::DingTalk => {
            let cfg = dingtalk::DingTalkConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(dingtalk::DingTalkChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Qq => {
            let cfg = qq::QQConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(qq::QQChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::WeCom => {
            let cfg = wecom::WeComConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(wecom::WeComChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Weixin => {
            let cfg = weixin::WeixinConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(weixin::WeixinChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Yuanbao => {
            let cfg = yuanbao::YuanbaoConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(yuanbao::YuanbaoChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Xiaoyi => {
            let cfg = xiaoyi::XiaoyiConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(xiaoyi::XiaoyiChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Mqtt => {
            let cfg = mqtt::MQTTConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(mqtt::MQTTChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Telegram => {
            let cfg = telegram::TelegramConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(telegram::TelegramChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
    }
}
