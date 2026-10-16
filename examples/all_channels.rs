//! 全通道 AI Bot 示例 — 企业微信 / QQ / 飞书 / 微信 同时运行.
//!
//!
//! Run:
//! ```text
//! WECOM_BOT_ID=... WECOM_SECRET=... \
//! QQ_APP_ID=... QQ_SECRET=... \
//! FEISHU_APP_ID=... FEISHU_APP_SECRET=... \
//! WEIXIN_ACCOUNT_ID=... WEIXIN_TOKEN=... \
//! cargo run --example all_channels
//! ```

mod common;

use std::sync::Arc;

use serde_json::json;

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::feishu::FeishuConfig;
use buddy_gateway::channels::qq::{QQChannel, QQConfig};
use buddy_gateway::channels::wecom::{WeComChannel, WeComConfig};
use buddy_gateway::channels::weixin::{WeixinChannel, WeixinConfig};
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::media::FileSystemMediaBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let processor = common::echo_processor();
    let mut manager = ChannelManager::new(Some(processor.clone()))
        .with_workers_per_channel(4)
        .with_media_backend(Arc::new(FileSystemMediaBackend::new("/tmp/buddy-gateway/media")));
    manager.start().await?;

    // 企业微信
    let mut wecom = WeComChannel::new(
        processor.clone(),
        WeComConfig {
            bot_id: common::env("WECOM_BOT_ID"),
            secret: common::env("WECOM_SECRET"),
            ws_url: common::env("WECOM_WS_URL"),
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    wecom.set_constraints(&common::constraints(&[
        ("reply_timeout", json!(10.0)),
        ("timeout_strategy", json!("placeholder")),
        ("send_rate_limit", json!([2, 5.0])),
        ("show_thinking", json!(false)),
        ("show_tool_hints", json!(false)),
    ]));
    let wecom_id = manager.add_channel_object(Box::new(wecom)).await?;

    // QQ
    let mut qq = QQChannel::new(
        processor.clone(),
        QQConfig {
            app_id: common::env("QQ_APP_ID"),
            token: common::env("QQ_TOKEN"),
            secret: {
                let s = common::env("QQ_SECRET");
                if s.is_empty() {
                    common::env("QQ_TOKEN")
                } else {
                    s
                }
            },
            sandbox: common::env_bool("QQ_SANDBOX", false),
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    qq.set_constraints(&common::constraints(&[
        ("show_thinking", json!(false)),
        ("show_tool_hints", json!(false)),
    ]));
    let qq_id = manager.add_channel_object(Box::new(qq)).await?;

    // 飞书
    let feishu_id = manager
        .add_feishu_channel(FeishuConfig {
            app_id: common::env("FEISHU_APP_ID"),
            app_secret: common::env("FEISHU_APP_SECRET"),
            verification_token: common::env("FEISHU_VERIFICATION_TOKEN"),
            encrypt_key: common::env("FEISHU_ENCRYPT_KEY"),
            ..Default::default()
        }, Default::default())
        .await?;

    // 微信 iLink
    let mut weixin = WeixinChannel::new(
        processor.clone(),
        WeixinConfig {
            accounts: vec![buddy_gateway::channels::weixin::WeixinAccountConfig {
                account_id: common::env("WEIXIN_ACCOUNT_ID"),
                token: common::env("WEIXIN_TOKEN"),
                base_url: common::env("WEIXIN_BASE_URL"),
                ..Default::default()
            }],
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    weixin.set_constraints(&common::constraints(&[
        ("reply_timeout", json!(5.0)),
        ("timeout_strategy", json!("placeholder")),
        ("send_rate_limit", json!([5, 60.0])),
        ("show_thinking", json!(false)),
    ]));
    let weixin_id = manager.add_channel_object(Box::new(weixin)).await?;

    println!("{}", "=".repeat(60));
    println!("🟢 全通道 AI Bot 在线!");
    println!("   wecom:  {wecom_id}");
    println!("   qq:     {qq_id}");
    println!("   feishu: {feishu_id}");
    println!("   weixin: {weixin_id}");
    println!("   Ctrl+C 退出");
    println!("{}", "=".repeat(60));

    common::wait_forever().await;
    manager.stop().await;
    println!("\n⏹️  全部停止");
    Ok(())
}
