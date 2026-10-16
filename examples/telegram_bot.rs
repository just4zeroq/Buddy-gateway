//! Example: Telegram bot (echo).
//!
//!
//! Run:
//! ```text
//! TELEGRAM_BOT_TOKEN=123:abc cargo run --example telegram_bot
//! ```

mod common;

use buddy_gateway::channel::ChannelConfig;
use buddy_gateway::channels::telegram::TelegramConfig;
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_telegram_channel(TelegramConfig {
            base: ChannelConfig {
                show_thinking: common::env_bool("TELEGRAM_SHOW_THINKING", false),
                show_tool_hints: common::env_bool("TELEGRAM_SHOW_TOOL_HINTS", true),
                ..Default::default()
            },
            bot_token: common::env("TELEGRAM_BOT_TOKEN"),
            http_proxy: common::env("TELEGRAM_HTTP_PROXY"),
            show_typing: common::env_bool("TELEGRAM_SHOW_TYPING", true),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 Telegram Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
