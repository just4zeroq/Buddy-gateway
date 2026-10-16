//! 飞书 Bot 高级示例：命令处理 (`/help`) + 多模态.
//!
//! Mirrors `examples/advanced/feishu_advanced.py`.
//!
//! Run:
//! ```text
//! FEISHU_APP_ID=... FEISHU_APP_SECRET=... cargo run --example feishu_advanced
//! ```

#[path = "../common/mod.rs"]
mod common;

use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{self, Stream};

use buddy_gateway::channel::{ChannelConfig, MessageProcessor};
use buddy_gateway::channels::feishu::FeishuConfig;
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::models::{InboundMessage, MessageEvent};

/// A processor with commands and a simple text reply.
fn advanced_processor() -> MessageProcessor {
    Arc::new(|msg: InboundMessage| {
        let text = msg.text();
        let trimmed = text.trim().to_string();
        let events = if trimmed == "/help" {
            vec![
                MessageEvent::text(
                    "**飞书高级 Bot 命令:**\n- /help — 显示帮助\n- 发送图片 — AI 描述图片内容\n",
                ),
                MessageEvent::completed(),
            ]
        } else {
            vec![
                MessageEvent::text(format!("飞书助手: 收到「{trimmed}」")),
                MessageEvent::completed(),
            ]
        };
        Box::pin(stream::iter(events)) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager = ChannelManager::new(Some(advanced_processor())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_feishu_channel(FeishuConfig {
            base: ChannelConfig {
                show_thinking: false,
                show_tool_hints: false,
                ..Default::default()
            },
            app_id: common::env("FEISHU_APP_ID"),
            app_secret: common::env("FEISHU_APP_SECRET"),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 飞书 Advanced Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️ 已停止");
    Ok(())
}
