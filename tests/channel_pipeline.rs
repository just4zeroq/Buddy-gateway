//! Tests for the shared channel pipeline.

use std::collections::HashMap;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::loopback::{LoopbackChannel, LoopbackConfig};
use buddy_gateway::media::{FileSystemMediaBackend, MediaBackend};
use buddy_gateway::models::{
    ChannelSubject, ContentPart, ImageContent, MessageEvent, RawPayload,
};

mod common;

fn loopback(processor: buddy_gateway::channel::MessageProcessor) -> LoopbackChannel {
    LoopbackChannel::new(processor, LoopbackConfig::default(), None, None, 0.0)
}

fn captured_texts(ch: &LoopbackChannel) -> Vec<String> {
    ch.captured()
        .iter()
        .map(|p| p.text_value().unwrap_or("").to_string())
        .collect()
}

fn enable_thinking(ch: &mut LoopbackChannel) {
    let overrides: HashMap<String, serde_json::Value> =
        [("show_thinking".to_string(), json!(true))].into_iter().collect();
    ch.set_constraints(&overrides);
}

#[test]
fn channel_id_is_generated_uuid() {
    let ch = loopback(common::echo_processor("Echo"));
    assert_eq!(ch.channel_type(), "loopback");
    assert_eq!(ch.channel_id().len(), 36);
}

#[test]
fn parse_inbound_text() {
    let ch = loopback(common::echo_processor("Echo"));
    let msg = ch.parse_inbound(&RawPayload::Text("hello world".to_string())).unwrap();
    assert_eq!(msg.channel_type, "loopback");
    assert_eq!(msg.channel_subject.as_ref().unwrap().subject_id, "loopback-user");
    assert_eq!(msg.text(), "hello world");
}

#[tokio::test]
async fn handle_inbound_echo_reply() {
    let mut ch = loopback(common::echo_processor("Echo"));
    ch.handle_inbound(&RawPayload::Text("hi there".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("Echo: hi there"));
}

#[tokio::test]
async fn delta_events_accumulate_into_single_message() {
    let proc = common::static_processor(vec![
        MessageEvent::delta("Hello "),
        MessageEvent::delta("World"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts, vec!["Hello World".to_string()]);
}

#[tokio::test]
async fn flush_sends_accumulated_delta() {
    let proc = common::static_processor(vec![
        MessageEvent::delta("First "),
        MessageEvent::delta("part"),
        MessageEvent::flush(),
        MessageEvent::delta("Second "),
        MessageEvent::delta("part"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts, vec!["First part".to_string(), "Second part".to_string()]);
}

#[tokio::test]
async fn flush_sends_thinking_then_delta() {
    let proc = common::static_processor(vec![
        MessageEvent::thinking_delta("Reasoning: "),
        MessageEvent::thinking_delta("step by step"),
        MessageEvent::flush(),
        MessageEvent::delta("Answer: "),
        MessageEvent::delta("complete"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    enable_thinking(&mut ch);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts.len(), 2);
    assert!(texts[0].contains("Reasoning"));
    assert!(texts[1].contains("Answer"));
}

#[tokio::test]
async fn flush_on_empty_buffer_no_extra_message() {
    let proc = common::static_processor(vec![
        MessageEvent::flush(),
        MessageEvent::delta("Content"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts, vec!["Content".to_string()]);
}

#[tokio::test]
async fn thinking_shown_when_enabled() {
    let proc = common::static_processor(vec![
        MessageEvent::thinking("Let me think about this..."),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    enable_thinking(&mut ch);
    ch.handle_inbound(&RawPayload::Text("question".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("Let me think"));
}

#[tokio::test]
async fn thinking_discarded_when_disabled() {
    let proc = common::static_processor(vec![
        MessageEvent::thinking("Let me think about this..."),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    // show_thinking default false
    ch.handle_inbound(&RawPayload::Text("question".to_string()))
        .await
        .unwrap();
    assert!(captured_texts(&ch).is_empty());
}

#[tokio::test]
async fn thinking_delta_accumulates_when_enabled() {
    let proc = common::static_processor(vec![
        MessageEvent::thinking_delta("Step 1: "),
        MessageEvent::thinking_delta("analyze the problem"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    enable_thinking(&mut ch);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts.len(), 1);
    assert!(texts[0].contains("Step 1:"));
    assert!(texts[0].contains("analyze"));
}

#[tokio::test]
async fn thinking_delta_discarded_when_disabled() {
    let proc = common::static_processor(vec![
        MessageEvent::thinking_delta("Step 1: "),
        MessageEvent::thinking_delta("analyze the problem"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    assert!(captured_texts(&ch).is_empty());
}

#[tokio::test]
async fn clean_output_strips_raw_think_tags() {
    let proc = common::static_processor(vec![
        MessageEvent::delta("<think>internal reasoning</think>Final answer"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(proc);
    ch.handle_inbound(&RawPayload::Text("test".to_string()))
        .await
        .unwrap();
    let texts = captured_texts(&ch);
    assert_eq!(texts.len(), 1);
    assert!(!texts[0].contains("think"));
    assert!(!texts[0].contains("internal reasoning"));
    assert!(texts[0].contains("Final answer"));
}

#[tokio::test]
async fn push_text_routes_to_send_text() {
    let mut ch = loopback(common::echo_processor("Echo"));
    let subject = ChannelSubject::new("dest");
    ch.push_text(&subject, "notification").await.unwrap();
    assert_eq!(captured_texts(&ch), vec!["notification".to_string()]);
}

// ---------------------------------------------------------------------------
// load_media_bytes priority: data > local_path > url
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_media_bytes_data_wins() {
    let mut ch = loopback(common::echo_processor("Echo"));
    let root = std::env::temp_dir().join(format!("buddy-gw-lmb-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let backend = Arc::new(FileSystemMediaBackend::new(&root));
    backend.save(b"WRONG_FROM_BACKEND", "img/from_backend.png").await.unwrap();
    ch.set_media_backend(backend);

    let part = ContentPart::Image(ImageContent {
        data: Some(STANDARD.encode(b"INLINE")),
        local_path: Some("img/from_backend.png".to_string()),
        url: "https://example.com/x.png".to_string(),
        mime_type: Some("image/png".to_string()),
        ..Default::default()
    });

    let (raw, mime) = ch.load_media_bytes(&part).await.unwrap();
    assert_eq!(raw, b"INLINE");
    assert_eq!(mime, "image/png");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn load_media_bytes_local_path_reads_backend() {
    let mut ch = loopback(common::echo_processor("Echo"));
    let root = std::env::temp_dir().join(format!("buddy-gw-lmb2-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let backend = Arc::new(FileSystemMediaBackend::new(&root));
    backend.save(b"BACKEND_BYTES", "x/y.png").await.unwrap();
    ch.set_media_backend(backend);

    let part = ContentPart::Image(ImageContent {
        local_path: Some("x/y.png".to_string()),
        mime_type: Some("image/png".to_string()),
        ..Default::default()
    });

    let (raw, mime) = ch.load_media_bytes(&part).await.unwrap();
    assert_eq!(raw, b"BACKEND_BYTES");
    assert_eq!(mime, "image/png");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn load_media_bytes_local_path_without_backend_errors() {
    let ch = loopback(common::echo_processor("Echo"));
    let part = ContentPart::Image(ImageContent {
        local_path: Some("should/exist.png".to_string()),
        url: "https://x/y".to_string(),
        ..Default::default()
    });
    assert!(ch.load_media_bytes(&part).await.is_err());
}

#[tokio::test]
async fn load_media_bytes_invalid_base64_errors() {
    let ch = loopback(common::echo_processor("Echo"));
    let part = ContentPart::Image(ImageContent {
        data: Some("!!!not-valid-base64!!!".to_string()),
        mime_type: Some("image/png".to_string()),
        ..Default::default()
    });
    assert!(ch.load_media_bytes(&part).await.is_err());
}
