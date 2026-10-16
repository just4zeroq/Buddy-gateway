//! Tests for channel config + kind mapping.

use std::collections::HashSet;

use serde_json::json;

use buddy_gateway::channel::{ChannelConfig, ChannelConfigLike, ChannelCredentialsError};
use buddy_gateway::channels::{
    build_channel, normalize_config, ChannelKind, SUPPORTED_CHANNEL_KINDS,
    WEBHOOK_CHANNEL_KINDS,
};
use buddy_gateway::channels::dingtalk::DingTalkConfig;
use buddy_gateway::channels::feishu::FeishuConfig;
use buddy_gateway::channels::loopback::LoopbackConfig;
use buddy_gateway::channels::qq::QQConfig;
use buddy_gateway::channels::telegram::TelegramConfig;
use buddy_gateway::channels::wecom::WeComConfig;
use buddy_gateway::channels::weixin::WeixinConfig;
use buddy_gateway::channels::xiaoyi::XiaoyiConfig;
use buddy_gateway::channels::yuanbao::YuanbaoConfig;

mod common;

#[test]
fn channel_config_defaults() {
    let cfg = ChannelConfig::default();
    assert!(cfg.channel_id.is_none());
    assert!(cfg.tenant_id.is_none());
    assert!(!cfg.show_thinking);
    assert!(!cfg.show_tool_hints);
    assert!(cfg.group_context.is_none());
}

#[test]
fn channel_config_explicit_values() {
    let cfg = ChannelConfig {
        channel_id: Some("abc".to_string()),
        tenant_id: Some("acme".to_string()),
        ..Default::default()
    };
    assert_eq!(cfg.channel_id.as_deref(), Some("abc"));
    assert_eq!(cfg.tenant_id.as_deref(), Some("acme"));
}

#[test]
fn channel_kind_matches_supported_set() {
    let kinds: HashSet<&str> = [
        ChannelKind::Discord,
        ChannelKind::Feishu,
        ChannelKind::DingTalk,
        ChannelKind::Qq,
        ChannelKind::WeCom,
        ChannelKind::Weixin,
        ChannelKind::Yuanbao,
        ChannelKind::Xiaoyi,
        ChannelKind::Mqtt,
        ChannelKind::Telegram,
        ChannelKind::Slack,
        ChannelKind::WhatsApp,
        ChannelKind::Line,
        ChannelKind::WechatMp,
        ChannelKind::GoogleChat,
    ]
    .iter()
    .map(|k| k.as_str())
    .collect();
    let supported: HashSet<&str> = SUPPORTED_CHANNEL_KINDS.iter().copied().collect();
    assert_eq!(kinds, supported);
    assert_eq!(SUPPORTED_CHANNEL_KINDS.len(), 15);
}

#[test]
fn webhook_kinds_are_a_subset_of_supported() {
    for kind in WEBHOOK_CHANNEL_KINDS {
        assert!(
            SUPPORTED_CHANNEL_KINDS.contains(kind),
            "webhook kind {kind} is not in SUPPORTED_CHANNEL_KINDS"
        );
        assert!(ChannelKind::from_str(kind).is_some());
    }
}

#[test]
fn channel_kind_from_str() {
    assert_eq!(ChannelKind::from_str("discord"), Some(ChannelKind::Discord));
    assert_eq!(ChannelKind::from_str("MQTT"), Some(ChannelKind::Mqtt));
    assert_eq!(ChannelKind::from_str("nope"), None);
}

#[test]
fn feishu_missing_credentials_reports_empty_fields() {
    let mut cfg = FeishuConfig::default();
    assert_eq!(
        cfg.missing_credentials(),
        vec!["app_id".to_string(), "app_secret".to_string()]
    );
    cfg.app_id = "cli_x".to_string();
    cfg.app_secret = "sec".to_string();
    assert!(cfg.missing_credentials().is_empty());
}

#[test]
fn dingtalk_missing_credentials() {
    let cfg = DingTalkConfig::default();
    assert_eq!(
        cfg.missing_credentials(),
        vec!["app_key".to_string(), "app_secret".to_string()]
    );
}

#[test]
fn qq_missing_credentials_reports_both() {
    let mut cfg = QQConfig::default();
    assert_eq!(
        cfg.missing_credentials(),
        vec!["app_id".to_string(), "secret".to_string()]
    );
    cfg.app_id = "qq".to_string();
    assert_eq!(cfg.missing_credentials(), vec!["secret".to_string()]);
}

#[test]
fn weixin_missing_credentials() {
    // `accounts` is validated in start(); the macro list is empty.
    let cfg = WeixinConfig::default();
    assert!(cfg.missing_credentials().is_empty());
}

#[test]
fn wecom_missing_credentials() {
    let cfg = WeComConfig::default();
    assert_eq!(
        cfg.missing_credentials(),
        vec!["bot_id".to_string(), "secret".to_string()]
    );
}

#[test]
fn telegram_missing_credentials() {
    let cfg = TelegramConfig::default();
    assert_eq!(cfg.missing_credentials(), vec!["bot_token".to_string()]);
}

#[test]
fn xiaoyi_yuanbao_missing_credentials() {
    assert_eq!(
        XiaoyiConfig::default().missing_credentials(),
        vec!["ak".to_string(), "sk".to_string(), "agent_id".to_string()]
    );
    assert_eq!(
        YuanbaoConfig::default().missing_credentials(),
        vec!["app_key".to_string(), "app_secret".to_string()]
    );
}

#[test]
fn loopback_has_no_required_credentials() {
    let cfg = LoopbackConfig::default();
    assert!(cfg.required_credentials().is_empty());
    assert!(cfg.missing_credentials().is_empty());
}

#[test]
fn required_credentials_reflects_macro_list() {
    let cfg = QQConfig::default();
    assert_eq!(cfg.required_credentials(), &["app_id", "secret"]);
}

#[test]
fn channel_credentials_error_carries_structured_data() {
    let err = ChannelCredentialsError {
        kind: "feishu".to_string(),
        missing: vec!["app_id".to_string(), "app_secret".to_string()],
    };
    assert_eq!(err.kind, "feishu");
    assert_eq!(err.missing, vec!["app_id", "app_secret"]);
}

#[test]
fn default_constraints_match_python() {
    use buddy_gateway::channel::Channel;
    use buddy_gateway::channels::qq::QQChannel;
    use buddy_gateway::channels::wecom::WeComChannel;

    let qq = QQChannel::default_constraints();
    assert_eq!(qq.send_rate_limit, Some((20, 60.0)));
    assert_eq!(qq.typing_keepalive_interval, 5.0);
    assert!(!qq.show_tool_hints);

    let wecom = WeComChannel::default_constraints();
    assert_eq!(wecom.reply_timeout, 10.0);
    assert_eq!(wecom.timeout_strategy, "placeholder");
    assert_eq!(wecom.send_rate_limit, Some((2, 5.0)));
    assert_eq!(wecom.placeholder_texts.len(), 3);
}

#[test]
fn channel_config_like_base_accessor() {
    let cfg = FeishuConfig {
        base: ChannelConfig {
            tenant_id: Some("acme".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(cfg.base().tenant_id.as_deref(), Some("acme"));
    assert_eq!(cfg.tenant_id(), Some("acme"));
}

#[test]
fn build_channel_telegram_ok() {
    let proc = common::echo_processor("Reply");
    let cfg = json!({ "bot_token": "123:abc" });
    let ch = build_channel("telegram", &cfg, proc, None, None).unwrap();
    assert_eq!(ch.channel_type(), "telegram");
}

#[test]
fn build_channel_loopback_is_not_a_kind() {
    // Loopback is a test harness, not a buildable channel kind (the registry's
    // _CHANNEL_MAP which excludes it).
    let proc = common::echo_processor("Reply");
    assert!(build_channel("loopback", &json!({}), proc, None, None).is_err());
}

#[test]
fn build_channel_unknown_kind_errors() {
    let proc = common::echo_processor("Reply");
    let result = build_channel("does-not-exist", &json!({}), proc, None, None);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// normalize_config: config normalization (aliases, trim, coercions)
// ---------------------------------------------------------------------------

#[test]
fn normalize_applies_aliases_per_channel() {
    let d = normalize_config("discord", &json!({ "token": "  abc  " }));
    assert_eq!(d["bot_token"], json!("abc"));

    let dt = normalize_config("dingtalk", &json!({ "client_id": "k", "client_secret": "s" }));
    assert_eq!(dt["app_key"], json!("k"));
    assert_eq!(dt["app_secret"], json!("s"));

    let qq = normalize_config("qq", &json!({ "client_secret": "sec" }));
    assert_eq!(qq["secret"], json!("sec"));

    let yb = normalize_config(
        "yuanbao",
        &json!({ "appId": "a", "client_secret": "b", "apiBase": "https://x", "wsUrl": "wss://y" }),
    );
    assert_eq!(yb["app_key"], json!("a"));
    assert_eq!(yb["app_secret"], json!("b"));
    assert_eq!(yb["api_domain"], json!("https://x"));
    assert_eq!(yb["ws_url"], json!("wss://y"));
}

#[test]
fn normalize_canonical_key_wins_over_alias() {
    let d = normalize_config("discord", &json!({ "token": "alias", "bot_token": "canonical" }));
    assert_eq!(d["bot_token"], json!("canonical"));
}

#[test]
fn normalize_qq_coerces_c2c_streaming() {
    // Missing key → defaults on.
    let n = normalize_config("qq", &json!({ "app_id": "1", "secret": "s" }));
    assert_eq!(n["c2c_streaming"], json!(true));

    for (raw, expected) in [
        (json!("false"), false),
        (json!("0"), false),
        (json!("no"), false),
        (json!("OFF"), false),
        (json!("true"), true),
        (json!(false), false),
        (json!(true), true),
    ] {
        let n = normalize_config("qq", &json!({ "c2c_streaming": raw }));
        assert_eq!(n["c2c_streaming"], json!(expected));
    }
}

#[test]
fn normalize_qq_forces_group_context_enabled() {
    let n = normalize_config("qq", &json!({ "group_context": {} }));
    assert_eq!(n["group_context"]["enabled"], json!(true));

    // Explicit enabled is preserved.
    let n = normalize_config("qq", &json!({ "group_context": { "enabled": false } }));
    assert_eq!(n["group_context"]["enabled"], json!(false));
}

#[test]
fn normalize_group_context_coerces_string_numbers() {
    let n = normalize_config(
        "feishu",
        &json!({ "group_context": { "history_limit": "7", "history_ttl_seconds": "12.5" } }),
    );
    assert_eq!(n["group_context"]["history_limit"], json!(7));
    assert_eq!(n["group_context"]["history_ttl_seconds"], json!(12.5));
}

#[test]
fn normalize_discord_splits_and_dedups_ids() {
    let n = normalize_config("discord", &json!({ "allowed_user_ids": "1, 2 2  3" }));
    assert_eq!(n["allowed_user_ids"], json!(["1", "2", "3"]));

    let n = normalize_config("discord", &json!({ "allowed_channel_ids": ["10", "10"] }));
    assert_eq!(n["allowed_channel_ids"], json!(["10"]));
}

#[test]
fn normalize_telegram_splits_allowed_ids() {
    let n = normalize_config("telegram", &json!({ "allowed_user_ids": "11,22" }));
    assert_eq!(n["allowed_user_ids"], json!(["11", "22"]));
}

#[test]
fn telegram_typing_interval_follows_show_typing() {
    use buddy_gateway::channels::telegram::TelegramChannel;

    let proc = common::echo_processor("Reply");
    let on = TelegramChannel::new(
        proc.clone(),
        TelegramConfig {
            bot_token: "123:abc".to_string(),
            show_typing: true,
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    assert_eq!(on.core_ref().constraints().typing_keepalive_interval, 4.0);

    let off = TelegramChannel::new(
        proc,
        TelegramConfig {
            bot_token: "123:abc".to_string(),
            show_typing: false,
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    assert_eq!(off.core_ref().constraints().typing_keepalive_interval, 0.0);
}
