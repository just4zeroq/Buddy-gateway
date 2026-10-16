//! Example: XiaoYi (小艺) bot.
//!
//!
//! Run:
//! ```text
//! XIAOYI_AK=... XIAOYI_SK=... XIAOYI_AGENT_ID=... cargo run --example xiaoyi_bot
//! ```

mod common;

use buddy_gateway::channel::ChannelConfig;
use buddy_gateway::channels::xiaoyi::XiaoyiConfig;
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_xiaoyi_channel(XiaoyiConfig {
            base: ChannelConfig {
                show_thinking: common::env_bool("XIAOYI_SHOW_THINKING", false),
                show_tool_hints: common::env_bool("XIAOYI_SHOW_TOOL_HINTS", true),
                ..Default::default()
            },
            ak: common::env("XIAOYI_AK"),
            sk: common::env("XIAOYI_SK"),
            agent_id: common::env("XIAOYI_AGENT_ID"),
            ws_url: common::env("XIAOYI_WS_URL"),
            ws_url_backup: common::env("XIAOYI_WS_URL_BACKUP"),
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 小艺 Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
