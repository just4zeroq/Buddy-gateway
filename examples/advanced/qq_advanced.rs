//! QQ Bot 高级示例：`/image` 发送示例图片 + 周期性打印已知通道.
//!
//! Mirrors `examples/advanced/qq_advanced.py`.
//!
//! Run:
//! ```text
//! QQ_APP_ID=... QQ_TOKEN=... cargo run --example qq_advanced
//! ```

#[path = "../common/mod.rs"]
mod common;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, Stream};
use serde_json::json;

use buddy_gateway::channel::{Channel, MessageProcessor};
use buddy_gateway::channels::qq::{QQChannel, QQConfig};
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::models::{ContentPart, ImageContent, InboundMessage, MessageEvent, MessageEventType};

/// A processor that answers `/image` with a sample picture, else echoes.
fn advanced_processor() -> MessageProcessor {
    Arc::new(|msg: InboundMessage| {
        let text = msg.text().trim().to_lowercase();
        let events = if text == "/image" {
            vec![
                MessageEvent {
                    event_type: MessageEventType::Message,
                    content: vec![ContentPart::Image(ImageContent {
                        url: "https://picsum.photos/400/300".to_string(),
                        ..Default::default()
                    })],
                    metadata: Default::default(),
                    error: None,
                },
                MessageEvent::completed(),
            ]
        } else {
            vec![
                MessageEvent::text(format!("QQ 助手: 收到「{}」", msg.text())),
                MessageEvent::completed(),
            ]
        };
        Box::pin(stream::iter(events)) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let processor = advanced_processor();
    let mut manager = ChannelManager::new(Some(processor.clone())).with_workers_per_channel(4);
    manager.start().await?;

    let mut channel = QQChannel::new(
        processor,
        QQConfig {
            app_id: common::env("QQ_APP_ID"),
            token: common::env("QQ_TOKEN"),
            ..Default::default()
        },
        None,
        None,
        0.0,
    );
    channel.set_constraints(&common::constraints(&[
        ("send_rate_limit", json!([20, 60.0])),
        ("show_thinking", json!(false)),
        ("show_tool_hints", json!(false)),
    ]));
    let channel_id = manager.add_channel_object(Box::new(channel)).await?;

    println!("   channel_id: {channel_id}");
    println!("🟢 QQ Advanced Bot 在线! 发送 /image 获取示例图片, Ctrl+C 退出");

    // 周期性地打印已知通道（对应原示例的 show_users 后台任务）。
    let mut ticker = tokio::time::interval(Duration::from_secs(60));
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let ids = manager.channel_ids();
                println!("📋 已知通道 ({}): {ids:?}", ids.len());
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    manager.stop().await;
    println!("\n⏹️ 停止中...");
    Ok(())
}
