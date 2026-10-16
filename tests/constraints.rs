//! Tests for constraints (mirrors tests/test_constraints.py).

use std::collections::HashMap;
use std::time::Instant;

use serde_json::json;

use buddy_gateway::constraints::{
    tool_hint_message, ChannelConstraints, RateLimiter, ReplyTimeoutGuard, TypingKeepalive,
};

// ---------------------------------------------------------------------------
// ChannelConstraints
// ---------------------------------------------------------------------------

#[test]
fn default_reply_timeout() {
    let c = ChannelConstraints::default();
    assert_eq!(c.reply_timeout, 0.0);
}

#[test]
fn default_send_rate_limit_none() {
    let c = ChannelConstraints::default();
    assert!(c.send_rate_limit.is_none());
}

#[test]
fn default_typing_keepalive_interval() {
    let c = ChannelConstraints::default();
    assert_eq!(c.typing_keepalive_interval, 0.0);
}

#[test]
fn default_timeout_strategy() {
    let c = ChannelConstraints::default();
    assert_eq!(c.timeout_strategy, "none");
}

#[test]
fn default_placeholder_texts_is_three() {
    let c = ChannelConstraints::default();
    assert_eq!(c.placeholder_texts.len(), 3);
}

#[test]
fn default_tool_hint_throttle() {
    let c = ChannelConstraints::default();
    assert_eq!(c.tool_hint_throttle, 6.0);
}

#[test]
fn default_show_tool_hints_true() {
    let c = ChannelConstraints::default();
    assert!(c.show_tool_hints);
}

#[test]
fn default_tool_hint_template() {
    let c = ChannelConstraints::default();
    assert_eq!(c.tool_hint_template, "🔧 Calling tool: {tool_name}");
}

#[test]
fn default_show_thinking_false() {
    let c = ChannelConstraints::default();
    assert!(!c.show_thinking);
}

#[test]
fn custom_values_round_trip() {
    let mut c = ChannelConstraints::default();
    c.reply_timeout = 10.0;
    c.send_rate_limit = Some((5, 60.0));
    c.typing_keepalive_interval = 3.0;
    c.timeout_strategy = "placeholder".to_string();
    c.placeholder_texts = vec!["Working...".to_string(), "Hold on...".to_string()];
    c.tool_hint_throttle = 10.0;
    c.show_tool_hints = false;
    c.tool_hint_template = "Using {tool_name} now...".to_string();
    c.show_thinking = true;

    assert_eq!(c.reply_timeout, 10.0);
    assert_eq!(c.send_rate_limit, Some((5, 60.0)));
    assert_eq!(c.typing_keepalive_interval, 3.0);
    assert_eq!(c.timeout_strategy, "placeholder");
    assert_eq!(c.placeholder_texts.len(), 2);
    assert_eq!(c.tool_hint_throttle, 10.0);
    assert!(!c.show_tool_hints);
    assert_eq!(c.tool_hint_template, "Using {tool_name} now...");
    assert!(c.show_thinking);
}

#[test]
fn placeholder_text_from_list() {
    let mut c = ChannelConstraints::default();
    c.placeholder_texts = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    for _ in 0..50 {
        assert!(c.placeholder_texts.contains(&c.placeholder_text()));
    }
}

#[test]
fn placeholder_text_single_item() {
    let mut c = ChannelConstraints::default();
    c.placeholder_texts = vec!["Only one".to_string()];
    assert_eq!(c.placeholder_text(), "Only one");
}

#[test]
fn placeholder_text_empty_fallback() {
    let mut c = ChannelConstraints::default();
    c.placeholder_texts = vec![];
    assert_eq!(c.placeholder_text(), "⏳ ...");
}

#[test]
fn placeholder_text_covers_multiple() {
    let mut c = ChannelConstraints::default();
    c.placeholder_texts = vec!["X".to_string(), "Y".to_string(), "Z".to_string()];
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        seen.insert(c.placeholder_text());
    }
    assert!(seen.len() >= 2);
}

// ---------------------------------------------------------------------------
// tool_hint_message
// ---------------------------------------------------------------------------

#[test]
fn tool_hint_prefers_metadata_text() {
    let c = ChannelConstraints::default();
    let mut meta = HashMap::new();
    meta.insert("tool_name".to_string(), json!("read_file"));
    meta.insert("tool_hint_text".to_string(), json!("🔧 正在调用工具：读取文件"));
    let text = tool_hint_message(&meta, &c, "start");
    assert_eq!(text, "🔧 正在调用工具：读取文件");
}

#[test]
fn tool_hint_falls_back_to_template() {
    let c = ChannelConstraints::default();
    let mut meta = HashMap::new();
    meta.insert("tool_name".to_string(), json!("grep"));
    let text = tool_hint_message(&meta, &c, "start");
    assert_eq!(text, "🔧 Calling tool: grep");
}

#[test]
fn tool_hint_end_phase_template() {
    let c = ChannelConstraints::default();
    let mut meta = HashMap::new();
    meta.insert("tool_name".to_string(), json!("grep"));
    let text = tool_hint_message(&meta, &c, "end");
    assert_eq!(text, "✅ grep done");
}

// ---------------------------------------------------------------------------
// RateLimiter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rate_limiter_allows_within_limit() {
    let limiter = RateLimiter::new(3, 10.0);
    let start = Instant::now();
    for _ in 0..3 {
        limiter.acquire().await;
    }
    assert!(start.elapsed().as_secs_f64() < 0.1);
}

#[tokio::test]
async fn rate_limiter_blocks_when_exceeded() {
    let limiter = RateLimiter::new(2, 0.2);
    limiter.acquire().await;
    limiter.acquire().await;
    let start = Instant::now();
    limiter.acquire().await;
    assert!(start.elapsed().as_secs_f64() >= 0.15);
}

#[tokio::test]
async fn rate_limiter_available_property() {
    let limiter = RateLimiter::new(3, 10.0);
    assert_eq!(limiter.available().await, 3);
    limiter.acquire().await;
    assert_eq!(limiter.available().await, 2);
    limiter.acquire().await;
    assert_eq!(limiter.available().await, 1);
    limiter.acquire().await;
    assert_eq!(limiter.available().await, 0);
}

#[tokio::test]
async fn rate_limiter_reset() {
    let limiter = RateLimiter::new(2, 10.0);
    limiter.acquire().await;
    limiter.acquire().await;
    assert_eq!(limiter.available().await, 0);
    limiter.reset().await;
    assert_eq!(limiter.available().await, 2);
}

// ---------------------------------------------------------------------------
// ReplyTimeoutGuard
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn timeout_guard_fires_at_80_percent() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let _guard = ReplyTimeoutGuard::spawn(0.1, tx);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(rx.try_recv().is_ok());
}

#[tokio::test(start_paused = true)]
async fn timeout_guard_zero_does_not_fire() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let _guard = ReplyTimeoutGuard::spawn(0.0, tx);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn timeout_guard_cancel_prevents_firing() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut guard = ReplyTimeoutGuard::spawn(0.5, tx);
    guard.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(rx.try_recv().is_err());
}

// ---------------------------------------------------------------------------
// TypingKeepalive
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn typing_keepalive_signals_periodically() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut ka = TypingKeepalive::spawn(0.05, tx);
    tokio::time::sleep(std::time::Duration::from_millis(180)).await;
    ka.stop();
    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    assert!(count >= 3, "expected >= 3 signals, got {count}");
}

#[tokio::test(start_paused = true)]
async fn typing_keepalive_zero_interval_no_signal() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let _ka = TypingKeepalive::spawn(0.0, tx);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn typing_keepalive_stop_halts_signals() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut ka = TypingKeepalive::spawn(0.05, tx);
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    ka.stop();
    // Drain whatever was produced.
    while rx.try_recv().is_ok() {}
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    assert!(rx.try_recv().is_err());
}
