//! Channel adapter layer.
//!
//! Each submodule implements the [`crate::channel::Channel`] contract for one
//! IM platform. This module provides:
//! - [`ChannelKind`] — canonical channel kinds.
//! - [`build_channel`] — the JSON-driven construction dispatcher.
//! - the `channel_adapter!` macro that generates the repetitive struct /
//! `new` / `ChannelConfigLike` scaffolding for every adapter.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Value;

use crate::channel::{Channel, MessageProcessor};
use crate::errors::{GatewayError, Result};
use crate::webhook::WebhookAdapter;

pub mod dingtalk;
pub mod discord;
pub mod feishu;
pub mod googlechat;
pub mod line;
pub mod loopback;
pub mod mqtt;
pub mod qq;
pub mod slack;
pub mod telegram;
pub mod wechat_mp;
pub mod wecom;
pub mod weixin;
pub mod whatsapp;
pub mod xiaoyi;
pub mod yuanbao;

// ===========================================================================
// Canonical channel kinds.
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
    Slack,
    WhatsApp,
    Line,
    WechatMp,
    GoogleChat,
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
            "slack" => Some(ChannelKind::Slack),
            "whatsapp" => Some(ChannelKind::WhatsApp),
            "line" => Some(ChannelKind::Line),
            "wechat_mp" => Some(ChannelKind::WechatMp),
            "googlechat" => Some(ChannelKind::GoogleChat),
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
            ChannelKind::Slack => "slack",
            ChannelKind::WhatsApp => "whatsapp",
            ChannelKind::Line => "line",
            ChannelKind::WechatMp => "wechat_mp",
            ChannelKind::GoogleChat => "googlechat",
        }
    }
}

/// All supported channel kind keys.
pub const SUPPORTED_CHANNEL_KINDS: &[&str] = &[
    "discord", "feishu", "dingtalk", "qq", "wecom", "weixin", "yuanbao", "xiaoyi", "mqtt",
    "telegram", "slack", "whatsapp", "line", "wechat_mp", "googlechat",
];

/// Channel kinds that receive their events over HTTP webhooks rather than a
/// long-lived connection.
pub const WEBHOOK_CHANNEL_KINDS: &[&str] = &["whatsapp", "line", "wechat_mp", "googlechat"];

// ===========================================================================
// Runtime (plugin) channel-kind registration
// ===========================================================================

/// Constructor for a runtime-registered channel kind.
///
/// Receives the normalized JSON config, the processor, and the explicit
/// `channel_id` / `tenant_id` overrides, exactly like [`build_channel`].
pub type ChannelBuilder = Arc<
    dyn Fn(&Value, MessageProcessor, Option<String>, Option<String>) -> Result<Box<dyn Channel>>
        + Send
        + Sync,
>;

type PluginRegistry = Mutex<HashMap<String, (String, ChannelBuilder)>>;

fn plugin_registry() -> &'static PluginRegistry {
    static REGISTRY: std::sync::OnceLock<PluginRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn normalize_kind(kind: &str) -> String {
    kind.trim().to_ascii_lowercase()
}

/// Builtin and currently registered plugin kinds.
pub fn registered_channel_kinds() -> Vec<String> {
    let mut kinds: Vec<String> = SUPPORTED_CHANNEL_KINDS.iter().map(|s| s.to_string()).collect();
    let registry = plugin_registry().lock().unwrap_or_else(|e| e.into_inner());
    for kind in registry.keys() {
        if !kinds.contains(kind) {
            kinds.push(kind.clone());
        }
    }
    kinds
}

/// Register a channel builder for a new kind owned by `plugin_id`.
///
/// The same owner may replace its registration (plugin reload); builtins and
/// kinds owned by another plugin cannot be replaced. Hosts must stop affected
/// live instances before replacing a registration.
pub fn register_channel_kind(kind: &str, builder: ChannelBuilder, plugin_id: &str) -> Result<()> {
    let owner = plugin_id.trim();
    if owner.is_empty() {
        return Err(GatewayError::Other("plugin_id must not be empty".into()));
    }
    let normalized = normalize_kind(kind);
    if normalized.is_empty() {
        return Err(GatewayError::Other("channel kind must not be empty".into()));
    }
    if ChannelKind::from_str(&normalized).is_some() {
        return Err(GatewayError::Other(format!(
            "channel kind '{}' is a builtin kind and cannot be overridden",
            normalized
        )));
    }
    let mut registry = plugin_registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some((existing, _)) = registry.get(&normalized) {
        if existing != owner {
            return Err(GatewayError::Other(format!(
                "channel kind '{}' is owned by another plugin",
                normalized
            )));
        }
    }
    registry.insert(normalized, (owner.to_string(), builder));
    Ok(())
}

/// Remove the registration owned by `plugin_id`; builtin kinds are preserved.
///
/// Returns `true` when a registration was removed.
pub fn unregister_channel_kind(kind: &str, plugin_id: &str) -> Result<bool> {
    let owner = plugin_id.trim();
    if owner.is_empty() {
        return Err(GatewayError::Other("plugin_id must not be empty".into()));
    }
    let normalized = normalize_kind(kind);
    if ChannelKind::from_str(&normalized).is_some() {
        return Err(GatewayError::Other(
            "cannot unregister a builtin channel kind".to_string(),
        ));
    }
    let mut registry = plugin_registry().lock().unwrap_or_else(|e| e.into_inner());
    match registry.get(&normalized) {
        None => Ok(false),
        Some((existing, _)) if existing != owner => Err(GatewayError::Other(format!(
            "channel kind '{}' is owned by another plugin",
            normalized
        ))),
        Some(_) => {
            registry.remove(&normalized);
            Ok(true)
        }
    }
}

/// Look up a runtime-registered builder without holding the registry lock.
fn plugin_builder(kind: &str) -> Option<ChannelBuilder> {
    let registry = plugin_registry().lock().unwrap_or_else(|e| e.into_inner());
    registry.get(kind).map(|(_, builder)| Arc::clone(builder))
}

// ===========================================================================
// DRY scaffolding macro.
//
// Generates, for a platform adapter:
// - the channel struct holding a `ChannelCore` + the platform `Config`;
// - `new(processor, config, channel_id, tenant_id, debounce_seconds)` which
// resolves a concrete channel id (config > arg > random uuid), applies the
// config's display flags to the constraints, and stamps the channel type;
// - the `ChannelConfigLike` impl (base accessor, required creds, missing creds).
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
                    // `_default_constraints(self)` may read the config
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
// JSON-driven construction dispatcher.
// ===========================================================================

/// Field aliases per channel kind.
/// Applied only when the canonical key is absent.
fn aliases_for(channel_type: &str) -> &'static [(&'static str, &'static str)] {
    match channel_type {
        "discord" => &[("token", "bot_token")],
        "dingtalk" => &[("client_id", "app_key"), ("client_secret", "app_secret")],
        "qq" => &[("client_secret", "secret")],
        "slack" => &[("token", "bot_token"), ("app_level_token", "app_token")],
        "whatsapp" => &[
            ("phone_id", "phone_number_id"),
            ("permanent_token", "access_token"),
            ("graph_api_version", "api_version"),
        ],
        "line" => &[
            ("channel_token", "channel_access_token"),
            ("access_token", "channel_access_token"),
            ("secret", "channel_secret"),
        ],
        "wechat_mp" => &[
            ("aes_key", "encoding_aes_key"),
            ("encoding_key", "encoding_aes_key"),
            ("server_token", "token"),
            ("secret", "app_secret"),
        ],
        "googlechat" => &[
            ("audience", "expected_audience"),
            ("aud", "expected_audience"),
            ("token", "access_token"),
            ("bot_name", "bot_user_name"),
        ],
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

/// Deserialize a whitelist field that may be a JSON array of strings or a
/// single comma/whitespace separated string (config-file friendly).
pub fn string_or_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(match value {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => s
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_string())
            .collect(),
        Some(Value::Array(items)) => items
            .into_iter()
            .filter_map(|v| match v {
                Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .collect(),
        Some(other) => vec![other.to_string()],
    })
}

/// Truthiness for a JSON value (used for `c2c_streaming` / `enabled`).
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
/// De-duplicated list of ASCII-digit IDs, parsed leniently from `from_dict`-style
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

/// Per-channel config coercions.
fn coerce_config(channel_type: &str, obj: &mut serde_json::Map<String, Value>) {
    // group_context: coerce numeric/bool strings so serde can deserialize
    // (`from_dict` does int(str(..)) / bool(..)).
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
            // `streaming`) defaults ON.
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

/// Normalize a raw JSON config the way `ChannelConfig.from_dict` does:
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

/// Fail fast when required credentials are missing.
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
/// config (consistent with the manager: explicit > config > generated).
/// The config is normalized (aliases + string stripping) and its required
/// credentials are validated before construction.
pub fn build_channel(
    channel_type: &str,
    config: &Value,
    processor: MessageProcessor,
    channel_id: Option<String>,
    tenant_id: Option<String>,
) -> Result<Box<dyn Channel>> {
    let config = normalize_config(channel_type, config);
    let kind = match ChannelKind::from_str(channel_type) {
        Some(k) => k,
        // Fall back to runtime-registered (plugin) kinds before rejecting.
        None => {
            let builder = plugin_builder(&normalize_kind(channel_type))
                .ok_or_else(|| GatewayError::UnknownChannel(channel_type.to_string()))?;
            return builder(&config, processor, channel_id, tenant_id);
        }
    };
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
            let mut cfg = telegram::TelegramConfig::deserialize(&config)?;
            cfg.normalize()?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(telegram::TelegramChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Slack => {
            let cfg = slack::SlackConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(slack::SlackChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::WhatsApp => {
            let cfg = whatsapp::WhatsAppConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(whatsapp::WhatsAppChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::Line => {
            let cfg = line::LineConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(line::LineChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::WechatMp => {
            let cfg = wechat_mp::WechatMpConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(wechat_mp::WechatMpChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
        ChannelKind::GoogleChat => {
            let cfg = googlechat::GoogleChatConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Box::new(googlechat::GoogleChatChannel::new(processor, cfg, channel_id, tenant_id, 0.0)))
        }
    }
}

// ===========================================================================
// Webhook-based channels
// ===========================================================================

/// Default mount path for a webhook-based channel kind.
///
/// Slack is deliberately absent: Socket Mode needs no public endpoint. Use
/// [`crate::webhook::WebhookServer`] with an explicit path if you deploy
/// Slack's HTTP Events API instead.
pub fn webhook_path_for(channel_type: &str) -> Result<&'static str> {
    match ChannelKind::from_str(channel_type) {
        Some(ChannelKind::WhatsApp) => Ok("/webhooks/whatsapp"),
        Some(ChannelKind::Line) => Ok("/webhooks/line"),
        Some(ChannelKind::WechatMp) => Ok("/webhooks/wechat"),
        Some(ChannelKind::GoogleChat) => Ok("/webhooks/googlechat"),
        Some(other) => Err(GatewayError::Other(format!(
            "channel kind `{}` does not deliver events over webhooks",
            other.as_str()
        ))),
        None => Err(GatewayError::UnknownChannel(channel_type.to_string())),
    }
}

/// Build the [`WebhookAdapter`] for a webhook-based channel kind from its JSON
/// config (normalized + credential-checked, exactly like [`build_channel`]).
pub fn build_webhook_adapter(channel_type: &str, config: &Value) -> Result<Arc<dyn WebhookAdapter>> {
    let kind = ChannelKind::from_str(channel_type)
        .ok_or_else(|| GatewayError::UnknownChannel(channel_type.to_string()))?;
    let config = normalize_config(channel_type, config);
    match kind {
        ChannelKind::WhatsApp => {
            let cfg = whatsapp::WhatsAppConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Arc::new(whatsapp::WhatsAppWebhook::new(cfg)))
        }
        ChannelKind::Line => {
            let cfg = line::LineConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Arc::new(line::LineWebhook::new(cfg)))
        }
        ChannelKind::WechatMp => {
            let cfg = wechat_mp::WechatMpConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Arc::new(wechat_mp::WechatMpWebhook::new(cfg)))
        }
        ChannelKind::GoogleChat => {
            let cfg = googlechat::GoogleChatConfig::deserialize(&config)?;
            ensure_credentials(channel_type, &cfg)?;
            Ok(Arc::new(googlechat::GoogleChatWebhook::new(cfg)))
        }
        other => Err(GatewayError::Other(format!(
            "channel kind `{}` does not deliver events over webhooks",
            other.as_str()
        ))),
    }
}
