//! Example: MQTT bot (e.g. EMQX Cloud TLS) for IoT devices.
//!
//!
//! Run:
//! ```text
//! MQTT_HOST=... MQTT_USERNAME=... MQTT_PASSWORD=... cargo run --example mqtt_bot
//! ```

mod common;

use buddy_gateway::channel::ChannelConfig;
use buddy_gateway::channels::mqtt::MQTTConfig;
use buddy_gateway::manager::ChannelManager;

/// Load the TLS CA PEM from `MQTT_TLS_CA_FILE` when present.
fn load_ca_pem() -> String {
    let ca_file = common::env("MQTT_TLS_CA_FILE");
    if !ca_file.is_empty() {
        if let Ok(text) = std::fs::read_to_string(&ca_file) {
            return text;
        }
    }
    String::new()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::load_dotenv();

    let mut manager =
        ChannelManager::new(Some(common::echo_processor())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_mqtt_channel(MQTTConfig {
            base: ChannelConfig {
                show_thinking: common::env_bool("MQTT_SHOW_THINKING", false),
                show_tool_hints: common::env_bool("MQTT_SHOW_TOOL_HINTS", true),
                ..Default::default()
            },
            host: common::env_or("MQTT_HOST", "c14633a9.ala.cn-shenzhen.emqxsl.cn"),
            port: common::env_u16("MQTT_PORT", 8883),
            username: common::env("MQTT_USERNAME"),
            password: common::env("MQTT_PASSWORD"),
            subscribe_topic: common::env_or("MQTT_SUBSCRIBE_TOPIC", "devices/+/in"),
            publish_topic: common::env_or("MQTT_PUBLISH_TOPIC", "devices/{client_id}/out"),
            tls_enabled: common::env_bool("MQTT_TLS_ENABLED", true),
            tls_ca_pem: load_ca_pem(),
            ..Default::default()
        }, Default::default())
        .await?;
    println!("   channel_id: {channel_id}");
    println!("🟢 MQTT Bot 在线! Ctrl+C 退出");

    common::wait_forever().await;
    manager.stop().await;
    println!("⏹️  已停止");
    Ok(())
}
