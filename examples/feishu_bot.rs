//! 飞书 (Feishu/Lark) AI Bot 示例.
//!
//! Mirrors `examples/feishu_bot.py`.
//!
//! Run:
//! ```text
//! FEISHU_APP_ID=... FEISHU_APP_SECRET=... cargo run --example feishu_bot
//! (optional: FEISHU_VERIFICATION_TOKEN / FEISHU_ENCRYPT_KEY)
//! ```

mod common;

use std::sync::Arc;

use buddy_gateway::channels::feishu::FeishuConfig;
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::media::FileSystemMediaBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let media_backend = Arc::new(FileSystemMediaBackend::new("/tmp/octop-gateway/media"));

    let mut manager = ChannelManager::new(Some(common::echo_processor()))
        .with_workers_per_channel(4)
        .with_media_backend(media_backend);

    manager.start().await?;

    let channel_id = manager
        .add_feishu_channel(FeishuConfig {
            app_id: common::env("FEISHU_APP_ID"),
            app_secret: common::env("FEISHU_APP_SECRET"),
            verification_token: common::env("FEISHU_VERIFICATION_TOKEN"),
            encrypt_key: common::env("FEISHU_ENCRYPT_KEY"),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 飞书 Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
