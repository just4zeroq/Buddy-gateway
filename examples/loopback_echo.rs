//! In-process loopback demo — runs the full message pipeline with no network.
//!
//! Uses the `loopback` test channel: inbound payloads are parsed and fed through
//! the shared pipeline (preprocess → group policy → process → finalize), and
//! outbound replies are captured in memory for inspection.
//!
//! Run:
//! ```text
//! cargo run --example loopback_echo
//! ```

mod common;

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::loopback::{LoopbackChannel, LoopbackConfig};
use buddy_gateway::models::RawPayload;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::init_tracing();

    let mut channel = LoopbackChannel::new(
        common::echo_processor(),
        LoopbackConfig::default(),
        Some("demo".to_string()),
        None,
        0.0,
    );
    channel.start().await?;

    let inputs = ["你好，介绍一下你自己", "ping", "/help"];
    for text in inputs {
        println!("→ 收到: {text}");
        channel
            .handle_inbound(&RawPayload::Text(text.to_string()))
            .await?;
    }

    println!("\n← 管道捕获的回复 ({} 条):", channel.captured().len());
    for part in channel.captured() {
        if let Some(text) = part.text_value() {
            println!("  ← {text}");
        }
    }

    channel.stop().await?;
    Ok(())
}
