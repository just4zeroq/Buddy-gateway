//! QQ Bot 示例 — 真实 WebSocket 网关（收消息）+ OpenAPI（发消息）。
//!
//! Mirrors `examples/qq_bot.py`.
//!
//! 参数：
//!   QQ_APP_ID  机器人 AppID
//!   QQ_SECRET  机器人 AppSecret（若未设置则回退到 QQ_TOKEN）
//!   QQ_TOKEN   机器人 Token（旧字段，作为 secret 的回退）
//!   QQ_SANDBOX 是否用沙盒端点（默认 false）
//!
//! Run:
//! ```text
//! QQ_APP_ID=... QQ_TOKEN=... cargo run --example qq_bot
//! ```

mod common;

use buddy_gateway::channels::qq::QQConfig;
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(4);
    manager.start().await?;

    // QQ 开放平台的凭据通常给的是 AppID + Token；这里把 Token 当作 AppSecret 使用，
    // 若显式提供了 QQ_SECRET 则优先用它。
    let secret = {
        let s = common::env("QQ_SECRET");
        if s.is_empty() {
            common::env("QQ_TOKEN")
        } else {
            s
        }
    };

    let channel_id = manager
        .add_qq_channel(QQConfig {
            app_id: common::env("QQ_APP_ID"),
            secret,
            sandbox: common::env_bool("QQ_SANDBOX", false),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 QQ Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
