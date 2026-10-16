//! Tests for the channel manager (mirrors tests/test_manager.py).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use buddy_gateway::channel::{Channel, MessageProcessor};
use buddy_gateway::manager::ChannelManager;
use buddy_gateway::models::{ChannelSubject, ContentPart, RawPayload};

mod common;

use common::CaptureChannel;

struct Harness {
    channel: Box<dyn Channel>,
    sent: Arc<Mutex<Vec<(String, String)>>>,
    started: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    media_backend_set: Arc<AtomicBool>,
}

fn make_channel(id: &str, prefix: &'static str) -> Harness {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let ch = CaptureChannel::new(common::echo_processor(prefix), sent.clone(), id);
    let started = ch.started.clone();
    let stopped = ch.stopped.clone();
    let media_backend_set = ch.media_backend_set.clone();
    Harness {
        channel: Box::new(ch),
        sent,
        started,
        stopped,
        media_backend_set,
    }
}

fn manager(proc: MessageProcessor) -> ChannelManager {
    ChannelManager::new(Some(proc)).with_workers_per_channel(1)
}

#[tokio::test]
async fn start_stop_lifecycle() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-1", "Reply");
    let id = mgr.add_channel_object(h.channel).await.unwrap();
    assert_eq!(id, "cap-1");
    assert!(!h.started.load(Ordering::SeqCst));

    mgr.start().await.unwrap();
    assert!(h.started.load(Ordering::SeqCst));

    mgr.stop().await;
    assert!(h.stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn channel_ids_contains_registered() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-ids", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    assert!(mgr.channel_ids().contains(&"cap-ids".to_string()));
}

#[tokio::test]
async fn enqueue_and_process() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-e", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    mgr.enqueue("cap-e", RawPayload::Text("hello".to_string()));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let sent = h.sent.lock().unwrap().clone();
    assert!(sent.iter().any(|(_, msg)| msg.contains("Reply: hello")));
    mgr.stop().await;
}

#[tokio::test]
async fn enqueue_nonexistent_channel_does_not_panic() {
    let mgr = manager(common::echo_processor("Reply"));
    // Must not panic.
    mgr.enqueue("missing", RawPayload::Text("x".to_string()));
}

#[tokio::test]
async fn push_text() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-push", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    let subject = ChannelSubject::new("user1");
    mgr.push_text("cap-push", &subject, "pushed message")
        .await
        .unwrap();
    let sent = h.sent.lock().unwrap().clone();
    assert!(sent.contains(&("user1".to_string(), "pushed message".to_string())));
    mgr.stop().await;
}

#[tokio::test]
async fn push_content() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-pc", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    let subject = ChannelSubject::new("user1");
    mgr.push_content("cap-pc", &subject, &[ContentPart::text("content push")])
        .await
        .unwrap();
    let sent = h.sent.lock().unwrap().clone();
    assert!(sent.contains(&("user1".to_string(), "content push".to_string())));
    mgr.stop().await;
}

#[tokio::test]
async fn push_nonexistent_channel_errors() {
    let mgr = manager(common::echo_processor("Reply"));
    let subject = ChannelSubject::new("user1");
    assert!(mgr.push_text("missing", &subject, "text").await.is_err());
}

#[tokio::test]
async fn add_duplicate_errors() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h1 = make_channel("dup", "Reply");
    mgr.add_channel_object(h1.channel).await.unwrap();

    let h2 = make_channel("dup", "Reply");
    assert!(mgr.add_channel_object(h2.channel).await.is_err());
}

#[tokio::test]
async fn remove_channel() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-rm", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    mgr.remove_channel("cap-rm").await.unwrap();
    assert!(!mgr.channel_ids().contains(&"cap-rm".to_string()));
    assert!(h.stopped.load(Ordering::SeqCst));
    mgr.stop().await;
}

#[tokio::test]
async fn media_backend_propagated_on_start() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-mb", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();

    mgr.start().await.unwrap();
    assert!(h.media_backend_set.load(Ordering::SeqCst));
    mgr.stop().await;
}

#[tokio::test]
async fn probe_channel_ok() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let res = mgr
        .probe_channel(
            "telegram",
            serde_json::json!({ "bot_token": "123:abc" }),
            None,
            None,
        )
        .await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn probe_channel_missing_credentials_errors() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let res = mgr
        .probe_channel("telegram", serde_json::json!({}), None, None)
        .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn list_subjects_and_all_users() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-ls", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();

    assert!(mgr.list_subjects("cap-ls").await.is_empty());
    assert!(mgr.list_subjects("missing").await.is_empty());
    let all = mgr.list_all_users().await;
    assert!(all.contains_key("cap-ls"));
}

#[tokio::test]
async fn run_in_session_returns_value() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-session", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();

    let result = mgr
        .run_in_session("cap-session", "user1", async { 42 })
        .await;
    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// v2 manager features: get_channel, ChannelOptions, predicates, pre-lock hook
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_channel_returns_registered_handle() {
    let mut mgr = manager(common::echo_processor("Reply"));
    let h = make_channel("cap-get", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    assert!(mgr.get_channel("cap-get").is_some());
    assert!(mgr.get_channel("nope").is_none());
}

#[tokio::test]
async fn add_channel_full_applies_options_constraints() {
    use serde_json::json;
    use std::collections::HashMap;

    use buddy_gateway::manager::ChannelOptions;

    let mut mgr = manager(common::echo_processor("Reply"));
    let mut constraints = HashMap::new();
    constraints.insert("show_thinking".to_string(), json!(true));

    let id = mgr
        .add_channel_full(
            "telegram",
            json!({ "bot_token": "123:abc" }),
            ChannelOptions {
                constraints: Some(constraints),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let arc = mgr.get_channel(&id).unwrap();
    let guard = arc.lock().await;
    assert!(guard.core().constraints().show_thinking);
}

#[tokio::test]
async fn add_channel_full_honors_explicit_channel_id() {
    use serde_json::json;

    use buddy_gateway::manager::ChannelOptions;

    let mut mgr = manager(common::echo_processor("Reply"));
    let id = mgr
        .add_channel_full(
            "telegram",
            json!({ "bot_token": "123:abc" }),
            ChannelOptions {
                channel_id: Some("fixed-id".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(id, "fixed-id");
}

#[tokio::test]
async fn interrupt_predicate_bypasses_queue() {
    use buddy_gateway::manager::MessagePredicate;
    use buddy_gateway::models::InboundMessage;

    let predicate: MessagePredicate = Arc::new(|m: &InboundMessage| m.text().contains("STOP"));
    let mut mgr = ChannelManager::new(Some(common::echo_processor("Reply")))
        .with_workers_per_channel(1)
        .with_interrupt_message_predicate(predicate);

    let h = make_channel("cap-int", "Reply");
    let sent = h.sent.clone();
    mgr.add_channel_object(h.channel).await.unwrap();

    // No `start()` → no worker loops. An interrupt must still be handled.
    mgr.enqueue("cap-int", RawPayload::Text("STOP now".to_string()));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn control_message_is_not_batched() {
    use buddy_gateway::manager::MessagePredicate;
    use buddy_gateway::models::InboundMessage;

    let predicate: MessagePredicate = Arc::new(|m: &InboundMessage| m.text().starts_with("CTRL"));
    let mut mgr = ChannelManager::new(Some(common::echo_processor("Reply")))
        .with_workers_per_channel(1)
        .with_control_message_predicate(predicate);

    let h = make_channel("cap-ctl", "Reply");
    let sent = h.sent.clone();
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    mgr.enqueue("cap-ctl", RawPayload::Text("CTRL stop".to_string()));
    mgr.enqueue("cap-ctl", RawPayload::Text("hello".to_string()));
    tokio::time::sleep(Duration::from_millis(400)).await;

    // A control message is processed on its own (not merged with the next
    // same-session message), so two separate replies are produced.
    let count = sent.lock().unwrap().len();
    assert!(count >= 2, "expected 2 sends, got {count}");
    mgr.stop().await;
}

#[tokio::test]
async fn pre_lock_handler_runs_before_processing() {
    use buddy_gateway::manager::PreLockHandler;

    let hit = Arc::new(AtomicBool::new(false));
    let hit2 = hit.clone();
    let handler: PreLockHandler = Arc::new(move |_cid, _msg| {
        let h = hit2.clone();
        Box::pin(async move {
            h.store(true, Ordering::SeqCst);
        })
    });

    let mut mgr = ChannelManager::new(Some(common::echo_processor("Reply")))
        .with_workers_per_channel(1)
        .with_on_pre_lock(handler);
    let h = make_channel("cap-pre", "Reply");
    mgr.add_channel_object(h.channel).await.unwrap();
    mgr.start().await.unwrap();

    mgr.enqueue("cap-pre", RawPayload::Text("hi".to_string()));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(hit.load(Ordering::SeqCst));
    mgr.stop().await;
}
