//! Example: Discord echo bot with a filesystem media backend.
//!
//!
//! Run:
//! ```text
//! DISCORD_BOT_TOKEN=... cargo run --example discord_bot
//! ```

mod common;

use std::sync::Arc;

use buddy_gateway::channels::discord::DiscordConfig;
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::media::FileSystemMediaBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager = ChannelManager::new(Some(common::echo_processor()))
        .with_workers_per_channel(4)
        .with_media_backend(Arc::new(FileSystemMediaBackend::new("./discord-media")));
    manager.start().await?;

    let channel_id = manager
        .add_discord_channel(DiscordConfig {
            bot_token: common::env("DISCORD_BOT_TOKEN"),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 Discord Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
