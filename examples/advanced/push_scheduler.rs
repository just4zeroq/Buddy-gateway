//! 定时主动推送示例 — 周期性向所有已知用户群发问候.
//!
//! Mirrors `examples/advanced/push_scheduler.py`.
//!
//! Run:
//! ```text
//! QQ_APP_ID=... QQ_TOKEN=... PUSH_INTERVAL_SECS=60 cargo run --example push_scheduler
//! ```

#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use buddy_gateway::channels::qq::QQConfig;
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let interval_secs = common::env_u16("PUSH_INTERVAL_SECS", 300) as u64;

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(2);
    manager.start().await?;

    manager
        .add_qq_channel(QQConfig {
            app_id: common::env("QQ_APP_ID"),
            token: common::env("QQ_TOKEN"),
            ..Default::default()
        }, Default::default())
        .await?;

    println!("🟢 Push Scheduler Bot 在线");
    println!("   每 {interval_secs} 秒向所有已知用户推送一条问候");

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // 跳过第一次立即触发
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                for cid in manager.channel_ids() {
                    match manager
                        .push_to_all(&cid, "👋 定时问候！有什么需要帮助的吗？")
                        .await
                    {
                        Ok(()) => println!("📤 已推送到 {cid}"),
                        Err(e) => println!("📤 推送到 {cid} 失败: {e}"),
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    manager.stop().await;
    println!("⏹️ 已停止");
    Ok(())
}
