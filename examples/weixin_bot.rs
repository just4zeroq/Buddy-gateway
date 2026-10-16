//! 微信 (WeChat iLink) AI Bot 示例 — 打字保活 + 占位符超时.
//!
//! Mirrors `examples/weixin_bot.py`.
//!
//! Run:
//! ```text
//! WEIXIN_ACCOUNT_ID=... WEIXIN_TOKEN=... cargo run --example weixin_bot
//! ```

mod common;

use serde_json::json;

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::weixin::{WeixinChannel, WeixinConfig};
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let processor = common::echo_processor();
    let mut manager = ChannelManager::new(Some(processor.clone())).with_workers_per_channel(4);
    manager.start().await?;

    let mut channel = WeixinChannel::new(
        processor,
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
    channel.set_constraints(&common::constraints(&[
        ("reply_timeout", json!(5.0)),
        ("timeout_strategy", json!("placeholder")),
        ("send_rate_limit", json!([5, 60.0])),
        ("typing_keepalive_interval", json!(5.0)),
        ("show_thinking", json!(false)),
        ("show_tool_hints", json!(false)),
        (
            "placeholder_texts",
            json!(["⏳ 思考中...", "🤔 让我想想...", "💭 正在处理..."]),
        ),
    ]));

    let channel_id = manager.add_channel_object(Box::new(channel)).await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 微信 iLink Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
