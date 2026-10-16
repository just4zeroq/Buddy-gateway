//! Bounded, expiring, identity-bound native callback capabilities.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Maximum number of pending callbacks; the oldest is dropped beyond this.
pub const MAX_PENDING: usize = 1000;
/// Default lifetime of a registered callback.
pub const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// A boxed, shareable host action.
pub type ActionFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

type Action = ActionFn;

/// A one-shot host action tied to one Telegram conversation and user.
#[derive(Clone)]
struct Callback {
    user: String,
    chat: String,
    topic: Option<i64>,
    expires: Instant,
    action: Action,
}

/// Keep opaque callback data free of privileges or user-controlled actions.
#[derive(Clone, Default)]
pub struct Interactions {
    pending: Arc<Mutex<VecDeque<(String, Callback)>>>,
}

impl Interactions {
    pub fn new() -> Self {
        Interactions {
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Register a short host action; long work must be queued by the host.
    ///
    /// The token is the only thing that reaches the wire, so a callback can
    /// never carry a privilege by itself: it also has to match the user, chat,
    /// and topic that press it, and it expires.
    pub fn register<F, Fut>(
        &self,
        user: &str,
        chat: &str,
        topic: Option<i64>,
        action: F,
        ttl: Duration,
    ) -> String
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register_boxed(
            user,
            chat,
            topic,
            Arc::new(move || Box::pin(action()) as Pin<Box<dyn Future<Output = ()> + Send>>),
            ttl,
        )
    }

    pub fn register_boxed(
        &self,
        user: &str,
        chat: &str,
        topic: Option<i64>,
        action: Action,
        ttl: Duration,
    ) -> String {
        let token = random_token();
        let item = Callback {
            user: user.to_string(),
            chat: chat.to_string(),
            topic,
            expires: Instant::now() + ttl,
            action,
        };
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.push_back((token.clone(), item));
        while pending.len() > MAX_PENDING {
            pending.pop_front();
        }
        token
    }

    /// Invoke a registered action; other users cannot invalidate a button.
    pub async fn invoke(
        &self,
        token: &str,
        user: &str,
        chat: &str,
        topic: Option<i64>,
    ) -> bool {
        let matched = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            let position = pending.iter().position(|(t, _)| t == token);
            let Some(position) = position else {
                return false;
            };
            let item = &pending[position].1;
            if Instant::now() >= item.expires {
                pending.remove(position);
                return false;
            }
            // A mismatched press must not invalidate someone else's button.
            if (item.user.as_str(), item.chat.as_str(), item.topic) != (user, chat, topic) {
                return false;
            }
            let (_, item) = pending.remove(position).expect("position is valid");
            item.action
        };
        matched().await;
        true
    }

    /// Drop one pending callback without running it (delivery failed).
    pub fn discard(&self, token: &str) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(position) = pending.iter().position(|(t, _)| t == token) {
            pending.remove(position);
        }
    }

    /// Drop every pending callback (used when a channel stops).
    pub fn clear(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.clear();
    }

    /// Number of pending callbacks (tests / diagnostics).
    pub fn len(&self) -> usize {
        self.pending.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn random_token() -> String {
    use base64::Engine as _;
    let mut bytes = [0u8; 18];
    for b in bytes.iter_mut() {
        *b = rand::random::<u8>();
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn invoke_runs_only_for_the_matching_identity() {
        let interactions = Interactions::new();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let token = interactions.register("7", "-1", Some(3), move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }, DEFAULT_TTL);

        assert!(!interactions.invoke(&token, "8", "-1", Some(3)).await);
        assert!(!interactions.invoke(&token, "7", "-2", Some(3)).await);
        assert!(!interactions.invoke(&token, "7", "-1", None).await);
        assert!(interactions.invoke(&token, "7", "-1", Some(3)).await);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // One-shot: a second press does nothing.
        assert!(!interactions.invoke(&token, "7", "-1", Some(3)).await);
    }

    #[tokio::test]
    async fn expired_callbacks_are_dropped() {
        let interactions = Interactions::new();
        let token = interactions.register("7", "-1", None, || async {}, Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!interactions.invoke(&token, "7", "-1", None).await);
    }

    #[test]
    fn pending_is_bounded() {
        let interactions = Interactions::new();
        for i in 0..MAX_PENDING + 20 {
            interactions.register("7", "-1", None, move || {
                let _ = i;
                async {}
            }, DEFAULT_TTL);
        }
        assert_eq!(interactions.len(), MAX_PENDING);
        interactions.clear();
        assert!(interactions.is_empty());
    }

    #[test]
    fn tokens_are_distinct() {
        let interactions = Interactions::new();
        let a = interactions.register("7", "-1", None, || async {}, DEFAULT_TTL);
        let b = interactions.register("7", "-1", None, || async {}, DEFAULT_TTL);
        assert_ne!(a, b);
    }
}
