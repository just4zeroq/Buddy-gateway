//! Telegram delivery rules: explicit rejection retries, never ambiguous resends.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use regex::Regex;
use tokio::sync::Mutex;

/// Bot-wide request budget: 30 requests/second.
const BOT_INTERVAL: Duration = Duration::from_nanos(33_333_333);
/// Per-chat request budget: one request/second.
const CHAT_INTERVAL: Duration = Duration::from_secs(1);
/// Default bound on the per-chat reservation table.
pub const MAX_CHATS: usize = 1000;

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Monotonic clock used by [`RequestScheduler`] (seconds since first use).
pub type ClockFn = Arc<dyn Fn() -> Duration + Send + Sync>;
/// Awaitable sleep used by [`RequestScheduler`].
pub type SleepFn =
    Arc<dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

fn default_clock() -> ClockFn {
    EPOCH.get_or_init(Instant::now);
    Arc::new(|| Instant::now().duration_since(*EPOCH.get().expect("epoch set")))
}

fn default_sleep() -> SleepFn {
    Arc::new(|d| Box::pin(tokio::time::sleep(d)))
}

struct Inner {
    bot_next: Duration,
    chats: VecDeque<(String, Duration)>,
}

/// Schedule every API attempt independently of the channel's logical slots.
///
/// Serializes admission at 30 requests/second per bot and one per second per
/// chat. A full chat table waits for its oldest reservation to expire rather
/// than evicting a live limit. Only one caller holds the admission lock.
pub struct RequestScheduler {
    clock: ClockFn,
    sleep: SleepFn,
    max_chats: usize,
    lock: Mutex<Inner>,
}

impl RequestScheduler {
    pub fn new() -> Self {
        RequestScheduler {
            clock: default_clock(),
            sleep: default_sleep(),
            max_chats: MAX_CHATS,
            lock: Mutex::new(Inner {
                bot_next: Duration::ZERO,
                chats: VecDeque::new(),
            }),
        }
    }

    /// Build a scheduler with an injectable clock/sleep (tests).
    pub fn with(clock: ClockFn, sleep: SleepFn, max_chats: usize) -> Self {
        RequestScheduler {
            clock,
            sleep,
            max_chats,
            lock: Mutex::new(Inner {
                bot_next: Duration::ZERO,
                chats: VecDeque::new(),
            }),
        }
    }

    /// Reserve one request; cancellation never reserves a future slot.
    pub async fn acquire(&self, chat: Option<&str>) {
        let mut inner = self.lock.lock().await;
        let key = chat.map(|c| c.to_string());
        loop {
            let now = (self.clock)();
            while inner
                .chats
                .front()
                .map(|(_, t)| *t <= now)
                .unwrap_or(false)
            {
                inner.chats.pop_front();
            }
            let mut ready = inner.bot_next;
            let known = key
                .as_ref()
                .and_then(|k| inner.chats.iter().find(|(c, _)| c == k).map(|(_, t)| *t));
            if let Some(t) = known {
                ready = ready.max(t);
            }
            if key.is_some() && known.is_none() && inner.chats.len() >= self.max_chats {
                if let Some((_, oldest)) = inner.chats.front() {
                    ready = ready.max(*oldest);
                }
            }
            if ready <= now {
                break;
            }
            (self.sleep)(ready - now).await;
        }
        let now = (self.clock)();
        inner.bot_next = now + BOT_INTERVAL;
        if let Some(k) = key {
            inner.chats.retain(|(c, _)| c != &k);
            inner.chats.push_back((k, now + CHAT_INTERVAL));
        }
    }
}

impl Default for RequestScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Redact credential-bearing URLs and bot tokens from an SDK/API message.
///
/// Keeps the error type and its meaning while removing anything that could
/// leak the bot token into logs.
pub fn sanitize_error_message(message: &str) -> String {
    static URL: OnceLock<Regex> = OnceLock::new();
    static TOKEN: OnceLock<Regex> = OnceLock::new();
    let url = URL.get_or_init(|| Regex::new(r#"https?://[^\s'"<>]+"#).expect("valid regex"));
    let token =
        TOKEN.get_or_init(|| Regex::new(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b").expect("valid regex"));
    let redacted = url.replace_all(message, "[redacted URL]");
    token.replace_all(&redacted, "[redacted token]").to_string()
}

/// Only entity parsing failures permit a plaintext resend.
pub fn formatting_error(message: &str) -> bool {
    let text = message.to_lowercase();
    [
        "parse entities",
        "unsupported start tag",
        "can't find end",
        "entity byte offset",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

/// How a failed Bot API call should be treated.
#[derive(Debug, Clone, PartialEq)]
pub enum SendFailure {
    /// Entity parsing failed: the same text may be resent as plain text.
    Formatting,
    /// Telegram explicitly rejected with a 429; `f64` is the retry delay.
    RateLimited(f64),
    /// Anything else: never resend, the result would be ambiguous.
    Fatal,
}

/// Classify a Bot API error description.
pub fn classify_failure(status: Option<u16>, description: &str) -> SendFailure {
    if status == Some(429) {
        return SendFailure::RateLimited(parse_retry_after(description));
    }
    if formatting_error(description) {
        return SendFailure::Formatting;
    }
    SendFailure::Fatal
}

/// Extract `retry_after` from a Bot API 429 description.
pub fn parse_retry_after(description: &str) -> f64 {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"retry[ _]after[^0-9]*([0-9]+(?:\.[0-9]+)?)").expect("valid regex")
    });
    re.captures(description)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok())
        .unwrap_or(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn redacts_urls_and_tokens() {
        let msg = "failed https://api.telegram.org/bot123456:AAA-token-value-here/x";
        let out = sanitize_error_message(msg);
        assert!(!out.contains("123456:AAA"));
        assert!(out.contains("[redacted URL]"));
    }

    #[test]
    fn detects_formatting_failures() {
        assert!(formatting_error("Bad Request: can't parse entities"));
        assert!(formatting_error("unsupported start tag"));
        assert!(!formatting_error("Bad Request: message is too long"));
    }

    #[test]
    fn classifies_failures() {
        assert_eq!(
            classify_failure(Some(429), "Too Many Requests: retry after 3"),
            SendFailure::RateLimited(3.0)
        );
        assert_eq!(
            classify_failure(Some(400), "can't find end tag"),
            SendFailure::Formatting
        );
        assert_eq!(classify_failure(Some(400), "chat not found"), SendFailure::Fatal);
    }

    /// Nanosecond-resolution fake clock: the bot budget is 33_333_333 ns, so
    /// the clock must advance in the same unit or the wait never converges.
    fn fake_parts() -> (Arc<AtomicU64>, Arc<AtomicU64>, ClockFn, SleepFn) {
        let now = Arc::new(AtomicU64::new(0));
        let slept = Arc::new(AtomicU64::new(0));
        let clock_now = Arc::clone(&now);
        let clock: ClockFn = Arc::new(move || Duration::from_nanos(clock_now.load(Ordering::SeqCst)));
        let sleep_slept = Arc::clone(&slept);
        let sleep_now = Arc::clone(&now);
        let sleep: SleepFn = Arc::new(move |d| {
            sleep_slept.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
            sleep_now.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
            Box::pin(async {})
        });
        (now, slept, clock, sleep)
    }

    const SEC: u64 = 1_000_000_000;

    #[tokio::test]
    async fn chat_slot_waits_one_second() {
        let (now, slept, clock, sleep) = fake_parts();
        let scheduler = RequestScheduler::with(clock, sleep, MAX_CHATS);

        scheduler.acquire(Some("-1")).await;
        assert_eq!(slept.load(Ordering::SeqCst), 0);
        // Same chat one nanosecond later: wait out the rest of the second.
        now.store(1, Ordering::SeqCst);
        scheduler.acquire(Some("-1")).await;
        assert_eq!(slept.load(Ordering::SeqCst), SEC - 1);

        // A different chat only waits for the bot-wide budget.
        slept.store(0, Ordering::SeqCst);
        now.store(SEC, Ordering::SeqCst);
        scheduler.acquire(Some("-2")).await;
        let waited = slept.load(Ordering::SeqCst);
        assert!(waited > 0 && waited < SEC, "waited={}", waited);
    }

    #[tokio::test]
    async fn full_chat_table_waits_for_oldest_reservation() {
        let (now, _slept, clock, sleep) = fake_parts();
        let scheduler = RequestScheduler::with(clock, sleep, 2);

        scheduler.acquire(Some("a")).await;
        scheduler.acquire(Some("b")).await;
        // Table full: a new chat must wait for the oldest reservation.
        scheduler.acquire(Some("c")).await;
        assert!(now.load(Ordering::SeqCst) >= SEC);
    }
}
