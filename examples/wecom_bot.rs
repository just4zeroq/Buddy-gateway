//! 企业微信 (WeCom) AI Bot 示例 — 占位符超时策略 + 限流.
//!
//!
//! Run:
//! ```text
//! WECOM_BOT_ID=... WECOM_SECRET=... cargo run --example wecom_bot
//! ```

mod common;

use serde_json::json;

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::wecom::{WeComChannel, WeComConfig};
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let processor = common::echo_processor();
    let mut manager = ChannelManager::new(Some(processor.clone())).with_workers_per_channel(4);
    manager.start().await?;

    let mut channel = WeComChannel::new(
        processor,
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
    channel.set_constraints(&common::constraints(&[
        ("reply_timeout", json!(10.0)),
        ("timeout_strategy", json!("placeholder")),
        ("send_rate_limit", json!([2, 5.0])),
        ("show_thinking", json!(false)),
        ("show_tool_hints", json!(false)),
        (
            "placeholder_texts",
            json!(["⏳ 思考中...", "🤔 处理中...", "💭 让我想想..."]),
        ),
    ]));

    let channel_id = manager.add_channel_object(Box::new(channel)).await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 企业微信 Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
