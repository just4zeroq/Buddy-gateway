//! Tests for utils (mirrors tests/test_utils.py).

use std::collections::HashMap;

use serde_json::json;

use buddy_gateway::models::{ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage, VideoContent};
use buddy_gateway::utils::{extract_media, extract_text, has_media, has_text, merge_messages};

fn msg(text: &str) -> InboundMessage {
    InboundMessage {
        channel_id: "test".to_string(),
        channel_type: String::new(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject::new("u1")),
        channel_session_id: String::new(),
        content: vec![ContentPart::text(text)],
        group_context: None,
        metadata: Default::default(),
        timestamp: 0.0,
    }
}

#[test]
fn has_text_true() {
    let parts = vec![
        ContentPart::text("hello"),
        ContentPart::Image(ImageContent {
            url: "http://x.png".to_string(),
            ..Default::default()
        }),
    ];
    assert!(has_text(&parts));
}

#[test]
fn has_text_false_empty() {
    let parts = vec![ContentPart::text(""), ContentPart::text("   ")];
    assert!(!has_text(&parts));
}

#[test]
fn has_text_false_no_text() {
    let parts = vec![ContentPart::Image(ImageContent {
        url: "http://x.png".to_string(),
        ..Default::default()
    })];
    assert!(!has_text(&parts));
}

#[test]
fn has_media_true() {
    let parts = vec![
        ContentPart::text("hi"),
        ContentPart::Image(ImageContent {
            url: "http://x.png".to_string(),
            ..Default::default()
        }),
    ];
    assert!(has_media(&parts));
}

#[test]
fn has_media_false() {
    let parts = vec![ContentPart::text("hi")];
    assert!(!has_media(&parts));
}

#[test]
fn has_media_all_types() {
    assert!(has_media(&[ContentPart::Video(VideoContent {
        url: "http://v.mp4".to_string(),
        ..Default::default()
    })]));
    assert!(has_media(&[ContentPart::File(FileContent {
        url: "http://f.pdf".to_string(),
        filename: "f.pdf".to_string(),
        ..Default::default()
    })]));
}

#[test]
fn extract_text_joins() {
    let parts = vec![
        ContentPart::text("line 1"),
        ContentPart::Image(ImageContent {
            url: "http://x.png".to_string(),
            ..Default::default()
        }),
        ContentPart::text("line 2"),
    ];
    assert_eq!(extract_text(&parts), "line 1\nline 2");
}

#[test]
fn extract_text_empty() {
    let parts = vec![ContentPart::Image(ImageContent {
        url: "http://x.png".to_string(),
        ..Default::default()
    })];
    assert_eq!(extract_text(&parts), "");
}

#[test]
fn extract_media_returns_non_text() {
    let parts = vec![
        ContentPart::text("hi"),
        ContentPart::Image(ImageContent {
            url: "http://img.png".to_string(),
            ..Default::default()
        }),
        ContentPart::File(FileContent {
            url: "http://f.pdf".to_string(),
            filename: "f.pdf".to_string(),
            ..Default::default()
        }),
    ];
    let media = extract_media(&parts);
    assert_eq!(media.len(), 2);
    assert!(media.iter().all(|m| !m.is_text()));
}

#[test]
fn merge_single_returns_clone() {
    let m = msg("hi");
    let result = merge_messages(&[m.clone()]);
    assert_eq!(result.text(), "hi");
    assert_eq!(result.channel_id, "test");
}

#[test]
fn merge_multiple_concatenates_and_merges_metadata() {
    let mut meta1 = HashMap::new();
    meta1.insert("first".to_string(), json!(true));
    let msg1 = InboundMessage {
        channel_id: "test".to_string(),
        channel_type: "weixin".to_string(),
        tenant_id: Some("agent-1".to_string()),
        channel_subject: Some(ChannelSubject::new("u1")),
        channel_session_id: "session-1".to_string(),
        content: vec![ContentPart::text("hello")],
        group_context: None,
        metadata: meta1,
        timestamp: 100.0,
    };
    let mut meta2 = HashMap::new();
    meta2.insert("second".to_string(), json!(true));
    let msg2 = InboundMessage {
        channel_id: "test".to_string(),
        channel_type: "weixin".to_string(),
        tenant_id: Some("agent-1".to_string()),
        channel_subject: Some(ChannelSubject::new("u1")),
        channel_session_id: "session-1".to_string(),
        content: vec![ContentPart::Image(ImageContent {
            url: "http://img.png".to_string(),
            ..Default::default()
        })],
        group_context: None,
        metadata: meta2,
        timestamp: 101.0,
    };

    let result = merge_messages(&[msg1, msg2]);
    assert_eq!(result.content.len(), 2);
    assert_eq!(result.timestamp, 100.0);
    assert_eq!(result.channel_subject.as_ref().unwrap().subject_id, "u1");
    assert_eq!(result.channel_type, "weixin");
    assert_eq!(result.tenant_id.as_deref(), Some("agent-1"));
    assert_eq!(result.channel_session_id, "session-1");
    assert_eq!(result.metadata.get("first"), Some(&json!(true)));
    assert_eq!(result.metadata.get("second"), Some(&json!(true)));
}

#[test]
#[should_panic]
fn merge_empty_panics() {
    let _ = merge_messages(&[]);
}
