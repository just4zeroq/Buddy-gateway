//! Example: Slack bot over Socket Mode (echo).
//!
//! Socket Mode keeps the bot behind your firewall: the app-level token buys a
//! WebSocket URL from Slack, and every event arrives over that single socket,
//! so no public HTTPS endpoint is required.
//!
//! Run:
//! ```text
//! SLACK_BOT_TOKEN=xoxb-... SLACK_APP_TOKEN=xapp-... cargo run --example slack_bot
//! ```
//!
//! Optional environment variables:
//! - `SLACK_SIGNING_SECRET` — only needed if you also deploy the HTTP Events API
//! - `SLACK_ALLOWED_CHANNEL_IDS` — comma separated conversation ids (`C…`/`D…`)
//! - `SLACK_ALLOWED_USER_IDS` — comma separated member ids (`U…`)
//! - `SLACK_ALLOW_ALL_CHANNELS=true` — serve every conversation the bot is in
//! - `SLACK_THREAD_REPLIES=false` — answer in the channel timeline instead of a thread
//! - `SLACK_HTTP_PROXY` / `SLACK_HTTP_PROXY_AUTH`
//!
//! Slack scopes and events needed: `app_mentions:read`, `chat:write`,
//! `channels:history` (or `groups:history`/`im:history`), `files:write`, and the
//! matching event subscriptions (`app_mention`, `message.channels`, …).

mod common;

use buddy_gateway::channel::ChannelConfig;
use buddy_gateway::channels::slack::SlackConfig;
use buddy_gateway::manager::ChannelManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_slack_channel(
            SlackConfig {
                base: ChannelConfig {
                    show_thinking: common::env_bool("SLACK_SHOW_THINKING", false),
                    show_tool_hints: common::env_bool("SLACK_SHOW_TOOL_HINTS", true),
                    ..Default::default()
                },
                bot_token: common::env("SLACK_BOT_TOKEN"),
                app_token: common::env("SLACK_APP_TOKEN"),
                signing_secret: common::env("SLACK_SIGNING_SECRET"),
                http_proxy: common::env("SLACK_HTTP_PROXY"),
                http_proxy_auth: common::env("SLACK_HTTP_PROXY_AUTH"),
                allow_all_channels: common::env_bool("SLACK_ALLOW_ALL_CHANNELS", false),
                allowed_channel_ids: common::env_list("SLACK_ALLOWED_CHANNEL_IDS"),
                allowed_user_ids: common::env_list("SLACK_ALLOWED_USER_IDS"),
                thread_replies: Some(common::env_bool("SLACK_THREAD_REPLIES", true)),
            },
            Default::default(),
        )
        .await?;

    println!("   channel_id: {channel_id}");
    println!("Slack Socket Mode 在线! 在 Slack 里私聊或 @ 机器人; Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("已停止");
    Ok(())
}
