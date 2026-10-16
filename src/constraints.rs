//! Platform constraints and rate limiting utilities.
//! Mirrors `octop_gateway.constraints`.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Channel Constraints Configuration
// ---------------------------------------------------------------------------

/// Platform-specific constraints that affect message delivery behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelConstraints {
    /// Max seconds allowed before first reply. 0 = no constraint.
    pub reply_timeout: f64,
    /// (max_messages, window_seconds). None = no limit.
    pub send_rate_limit: Option<(i64, f64)>,
    /// How often (seconds) to refresh typing indicator during processing. 0 = disabled.
    pub typing_keepalive_interval: f64,
    /// Strategy when reply_timeout is about to expire: "placeholder" | "none".
    pub timeout_strategy: String,
    /// Placeholder texts randomly selected when timeout fires.
    pub placeholder_texts: Vec<String>,
    /// Max seconds to wait for tool execution before sending intermediate status.
    pub tool_hint_throttle: f64,
    /// Display control: whether to show tool-call status to user.
    pub show_tool_hints: bool,
    /// Tool hint prefix template.
    pub tool_hint_template: String,
    /// Tool completion template.
    pub tool_end_template: String,
    /// Display control: whether to forward thinking/reasoning content to user.
    pub show_thinking: bool,
    /// Thinking content template.
    pub thinking_template: String,
}

impl Default for ChannelConstraints {
    fn default() -> Self {
        ChannelConstraints {
            reply_timeout: 0.0,
            send_rate_limit: None,
            typing_keepalive_interval: 0.0,
            timeout_strategy: "none".to_string(),
            placeholder_texts: vec![
                "⏳ 思考中...".to_string(),
                "🤔 让我想想...".to_string(),
                "💭 正在处理你的问题...".to_string(),
            ],
            tool_hint_throttle: 6.0,
            show_tool_hints: true,
            tool_hint_template: "🔧 Calling tool: {tool_name}".to_string(),
            tool_end_template: "✅ {tool_name} done".to_string(),
            show_thinking: false,
            thinking_template: "💭 Thinking: {content}".to_string(),
        }
    }
}

impl ChannelConstraints {
    /// Randomly select one placeholder text.
    pub fn placeholder_text(&self) -> String {
        if self.placeholder_texts.is_empty() {
            return "⏳ ...".to_string();
        }
        use rand::seq::SliceRandom;
        self.placeholder_texts
            .choose(&mut rand::thread_rng())
            .cloned()
            .unwrap_or_else(|| "⏳ ...".to_string())
    }
}

/// Format a tool status line, preferring pre-localized `tool_hint_text` in metadata.
pub fn tool_hint_message(
    metadata: &std::collections::HashMap<String, serde_json::Value>,
    constraints: &ChannelConstraints,
    phase: &str,
) -> String {
    if let Some(serde_json::Value::String(raw)) = metadata.get("tool_hint_text") {
        if !raw.trim().is_empty() {
            return raw.clone();
        }
    }
    let tool_name = metadata
        .get("tool_name")
        .and_then(|v| v.as_str())
        .unwrap_or("tool")
        .to_string();
    let template = if phase == "end" {
        &constraints.tool_end_template
    } else {
        &constraints.tool_hint_template
    };
    template.replace("{tool_name}", &tool_name)
}

// ---------------------------------------------------------------------------
// Rate Limiter (Sliding Window)
// ---------------------------------------------------------------------------

/// Sliding window rate limiter for outbound messages.
pub struct RateLimiter {
    max_calls: i64,
    window: Duration,
    timestamps: Mutex<VecDeque<Instant>>,
}

impl RateLimiter {
    pub fn new(max_calls: i64, window_seconds: f64) -> Self {
        RateLimiter {
            max_calls,
            window: Duration::from_secs_f64(window_seconds),
            timestamps: Mutex::new(VecDeque::new()),
        }
    }

    /// Acquire a send slot. Blocks if rate limit would be exceeded.
    pub async fn acquire(&self) {
        let mut guard = self.timestamps.lock().await;
        let now = Instant::now();
        while let Some(front) = guard.front() {
            if now.duration_since(*front) >= self.window {
                guard.pop_front();
            } else {
                break;
            }
        }
        if guard.len() as i64 >= self.max_calls {
            let wait = self.window.saturating_sub(now.duration_since(*guard.front().unwrap()));
            if !wait.is_zero() {
                drop(guard);
                tokio::time::sleep(wait).await;
                let mut g = self.timestamps.lock().await;
                let now2 = Instant::now();
                while let Some(front) = g.front() {
                    if now2.duration_since(*front) >= self.window {
                        g.pop_front();
                    } else {
                        break;
                    }
                }
                g.push_back(Instant::now());
                return;
            }
        }
        guard.push_back(Instant::now());
    }

    /// Number of available slots right now (without waiting).
    pub async fn available(&self) -> i64 {
        let guard = self.timestamps.lock().await;
        let now = Instant::now();
        let active = guard
            .iter()
            .filter(|t| now.duration_since(**t) < self.window)
            .count() as i64;
        (self.max_calls - active).max(0)
    }

    pub async fn reset(&self) {
        self.timestamps.lock().await.clear();
    }
}

// ---------------------------------------------------------------------------
// Reply Timeout Guard
// ---------------------------------------------------------------------------

/// Manages reply timeout by signalling on a channel when the deadline nears.
///
/// Mirrors `ReplyTimeoutGuard`. In the Rust port the guard does not call the
/// channel directly (that would require capturing `&mut self` in a `'static`
/// closure); instead it pushes a unit signal on `tx`, and the channel's
/// processing loop consumes the signal and emits the placeholder itself.
pub struct ReplyTimeoutGuard {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ReplyTimeoutGuard {
    pub fn spawn(timeout: f64, tx: UnboundedSender<()>) -> Self {
        if timeout <= 0.0 {
            return ReplyTimeoutGuard { task: None };
        }
        let dur = Duration::from_secs_f64(timeout * 0.8);
        let task = tokio::spawn(async move {
            tokio::time::sleep(dur).await;
            let _ = tx.send(());
        });
        ReplyTimeoutGuard { task: Some(task) }
    }

    pub fn cancel(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Drop for ReplyTimeoutGuard {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Typing Keepalive
// ---------------------------------------------------------------------------

/// Periodically signals during long processing so the channel can refresh its
/// typing indicator. Mirrors `TypingKeepalive` (signal-based, see above).
pub struct TypingKeepalive {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl TypingKeepalive {
    pub fn spawn(interval: f64, tx: UnboundedSender<()>) -> Self {
        if interval <= 0.0 {
            return TypingKeepalive { task: None };
        }
        let dur = Duration::from_secs_f64(interval);
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(dur).await;
                if tx.send(()).is_err() {
                    break;
                }
            }
        });
        TypingKeepalive { task: Some(task) }
    }

    pub fn stop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Drop for TypingKeepalive {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}
