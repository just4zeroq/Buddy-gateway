//! Example: one HTTP port serving every webhook-based channel.
//!
//! WhatsApp Cloud API, LINE Messaging API, the WeChat Official Account platform
//! and Google Chat all deliver events by *posting* them to a public HTTPS URL
//! you register with the provider. This example registers the matching channel
//! for each platform that has credentials configured, then mounts all of them on
//! a single port.
//!
//! Run:
//! ```text
//! cargo run --example webhook_server
//! ```
//!
//! Environment variables (each platform is skipped when its credentials are
//! absent, so you can start with just one):
//!
//! | Platform | Variables |
//! |----------|-----------|
//! | WhatsApp | `WHATSAPP_PHONE_NUMBER_ID`, `WHATSAPP_ACCESS_TOKEN`, `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET` |
//! | LINE | `LINE_CHANNEL_ACCESS_TOKEN`, `LINE_CHANNEL_SECRET` |
//! | WeChat MP | `WECHAT_MP_APP_ID`, `WECHAT_MP_TOKEN` (plus `WECHAT_MP_APP_SECRET`, `WECHAT_MP_ENCODING_AES_KEY`) |
//! | Google Chat | `GOOGLE_CHAT_ACCESS_TOKEN`, `GOOGLE_CHAT_AUDIENCE`, `GOOGLE_CHAT_BOT_USER` |
//! | Server | `WEBHOOK_ADDR` (default `0.0.0.0:8080`), `WEBHOOK_MEDIA_DIR` |
//!
//! Deployment notes:
//! - Every provider requires **HTTPS** on a public host. Terminate TLS in front
//!   of this process (nginx/Caddy/Cloud LB) and forward the raw body — signature
//!   checks run over the exact bytes received, so nothing may rewrite the body.
//! - Register these callback URLs with each provider:
//!   `/webhooks/whatsapp`, `/webhooks/line`, `/webhooks/wechat`, `/webhooks/googlechat`.
//! - The WeChat platform only accepts a passive reply inside a 5 second window;
//!   the adapter answers `success` immediately and the real answer is delivered
//!   later through the customer-service API.

mod common;

use std::sync::Arc;

use buddy_gateway::channel::ChannelConfig;
use buddy_gateway::channels::googlechat::{GoogleChatConfig, GoogleChatWebhook};
use buddy_gateway::channels::line::{LineConfig, LineWebhook};
use buddy_gateway::channels::wechat_mp::{WechatMpConfig, WechatMpWebhook};
use buddy_gateway::channels::whatsapp::{WhatsAppConfig, WhatsAppWebhook};
use buddy_gateway::manager::{ChannelManager, ChannelOptions};
use buddy_gateway::media::FileSystemMediaBackend;
use buddy_gateway::webhook::{WebhookAdapter, WebhookServer};

/// One platform ready to be mounted after the manager starts.
struct Mount {
    path: &'static str,
    channel_id: String,
    adapter: Arc<dyn WebhookAdapter>,
    label: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager = ChannelManager::new(Some(common::echo_processor()))
        .with_workers_per_channel(4)
        .with_media_backend(Arc::new(FileSystemMediaBackend::new(common::env_or(
            "WEBHOOK_MEDIA_DIR",
            "/tmp/buddy-gateway/media",
        ))));

    let mut mounts: Vec<Mount> = Vec::new();
    let base = ChannelConfig {
        show_thinking: common::env_bool("WEBHOOK_SHOW_THINKING", false),
        show_tool_hints: common::env_bool("WEBHOOK_SHOW_TOOL_HINTS", true),
        ..Default::default()
    };

    // ---- WhatsApp Cloud API -------------------------------------------------
    let wa_phone = common::env("WHATSAPP_PHONE_NUMBER_ID");
    if !wa_phone.is_empty() {
        let cfg = WhatsAppConfig {
            base: base.clone(),
            phone_number_id: wa_phone,
            access_token: common::env("WHATSAPP_ACCESS_TOKEN"),
            verify_token: common::env("WHATSAPP_VERIFY_TOKEN"),
            app_secret: common::env("WHATSAPP_APP_SECRET"),
            api_version: Some(common::env_or("WHATSAPP_API_VERSION", "v22.0")),
            http_proxy: common::env("WHATSAPP_HTTP_PROXY"),
            http_proxy_auth: common::env("WHATSAPP_HTTP_PROXY_AUTH"),
            allowed_user_ids: common::env_list("WHATSAPP_ALLOWED_USER_IDS"),
            ..Default::default()
        };
        let channel_id = manager.add_whatsapp_channel(cfg.clone(), ChannelOptions::new()).await?;
        mounts.push(Mount {
            path: "/webhooks/whatsapp",
            channel_id,
            adapter: Arc::new(WhatsAppWebhook::new(cfg)),
            label: "WhatsApp Cloud API",
        });
    }

    // ---- LINE Messaging API -------------------------------------------------
    let line_token = common::env("LINE_CHANNEL_ACCESS_TOKEN");
    if !line_token.is_empty() {
        let cfg = LineConfig {
            base: base.clone(),
            channel_access_token: line_token,
            channel_secret: common::env("LINE_CHANNEL_SECRET"),
            http_proxy: common::env("LINE_HTTP_PROXY"),
            http_proxy_auth: common::env("LINE_HTTP_PROXY_AUTH"),
            allowed_user_ids: common::env_list("LINE_ALLOWED_USER_IDS"),
            allowed_group_ids: common::env_list("LINE_ALLOWED_GROUP_IDS"),
            use_reply_token: Some(common::env_bool("LINE_USE_REPLY_TOKEN", true)),
        };
        let channel_id = manager.add_line_channel(cfg.clone(), ChannelOptions::new()).await?;
        mounts.push(Mount {
            path: "/webhooks/line",
            channel_id,
            adapter: Arc::new(LineWebhook::new(cfg)),
            label: "LINE Messaging API",
        });
    }

    // ---- WeChat Official Account -------------------------------------------
    let mp_app_id = common::env("WECHAT_MP_APP_ID");
    if !mp_app_id.is_empty() {
        let cfg = WechatMpConfig {
            base: base.clone(),
            app_id: mp_app_id,
            app_secret: common::env("WECHAT_MP_APP_SECRET"),
            token: common::env("WECHAT_MP_TOKEN"),
            encoding_aes_key: common::env("WECHAT_MP_ENCODING_AES_KEY"),
            http_proxy: common::env("WECHAT_MP_HTTP_PROXY"),
            http_proxy_auth: common::env("WECHAT_MP_HTTP_PROXY_AUTH"),
            default_reply: common::env_or("WECHAT_MP_DEFAULT_REPLY", "success"),
        };
        let channel_id = manager.add_wechat_mp_channel(cfg.clone(), ChannelOptions::new()).await?;
        mounts.push(Mount {
            path: "/webhooks/wechat",
            channel_id,
            adapter: Arc::new(WechatMpWebhook::new(cfg)),
            label: "WeChat Official Account",
        });
    }

    // ---- Google Chat --------------------------------------------------------
    let gc_token = common::env("GOOGLE_CHAT_ACCESS_TOKEN");
    if !gc_token.is_empty() {
        let cfg = GoogleChatConfig {
            base: base.clone(),
            access_token: gc_token,
            service_account_json: common::env("GOOGLE_CHAT_SERVICE_ACCOUNT_JSON"),
            expected_audience: common::env("GOOGLE_CHAT_AUDIENCE"),
            verify_jwt: Some(common::env_bool("GOOGLE_CHAT_VERIFY_JWT", true)),
            http_proxy: common::env("GOOGLE_CHAT_HTTP_PROXY"),
            http_proxy_auth: common::env("GOOGLE_CHAT_HTTP_PROXY_AUTH"),
            bot_user_name: common::env("GOOGLE_CHAT_BOT_USER"),
            include_sender_in_text: Some(common::env_bool("GOOGLE_CHAT_INCLUDE_SENDER", false)),
        };
        let channel_id = manager.add_googlechat_channel(cfg.clone(), ChannelOptions::new()).await?;
        mounts.push(Mount {
            path: "/webhooks/googlechat",
            channel_id,
            adapter: Arc::new(GoogleChatWebhook::new(cfg)),
            label: "Google Chat",
        });
    }

    if mounts.is_empty() {
        eprintln!("没有配置任何平台。请至少设置一组凭据，例如 LINE_CHANNEL_ACCESS_TOKEN + LINE_CHANNEL_SECRET。");
        return Ok(());
    }

    manager.start().await?;

    // The channel must be registered before its webhook can be mounted: mounting
    // captures the channel's enqueue callback, so requests never need a lock.
    let mut server = WebhookServer::new();
    for m in &mounts {
        server
            .mount_channel(&manager, m.path, m.adapter.clone(), &m.channel_id)
            .await?;
    }

    let addr = common::env_or("WEBHOOK_ADDR", "0.0.0.0:8080");
    println!("已挂载 {} 个平台:", mounts.len());
    for m in &mounts {
        println!("  {}  {}  ->  {}", m.path, m.label, m.channel_id);
    }
    println!("Webhook 服务监听 {addr}; 每平台一个回调地址; Ctrl+C 退出");

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    server.serve_with_shutdown(&addr, shutdown).await?;
    manager.stop().await;
    println!("已停止");
    Ok(())
}
