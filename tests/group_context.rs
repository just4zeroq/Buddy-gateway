//! Tests for group context policy.

use std::collections::HashMap;

use serde_json::{json, Value};

use buddy_gateway::group_context::{GroupContextConfig, GroupContextManager};
use buddy_gateway::models::{ChannelSubject, ContentPart, FileContent, InboundMessage};

fn group_message(
    text: &str,
    message_id: &str,
    sender_id: &str,
    mentioned: bool,
    timestamp: f64,
) -> InboundMessage {
    let mut metadata: HashMap<String, Value> = HashMap::new();
    metadata.insert("chat_type".to_string(), json!("group"));
    metadata.insert("conversation_id".to_string(), json!("group-1"));
    metadata.insert("sender_id".to_string(), json!(sender_id));
    metadata.insert("sender_name".to_string(), json!(sender_id.to_uppercase()));
    metadata.insert("message_id".to_string(), json!(message_id));
    metadata.insert("bot_mentioned".to_string(), json!(mentioned));

    InboundMessage {
        channel_id: "channel-1".to_string(),
        channel_type: "test".to_string(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject {
            subject_id: "group-1".to_string(),
            first_seen: None,
            last_seen: None,
            display_name: String::new(),
            chat_type: "group".to_string(),
            metadata: metadata.clone(),
        }),
        channel_session_id: String::new(),
        content: vec![ContentPart::text(text)],
        group_context: None,
        metadata,
        timestamp,
    }
}

#[test]
fn mention_recent_buffers_passive_and_attaches_to_mention() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "mention_recent".to_string(),
        history_limit: 10,
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    assert!(manager
        .prepare(&group_message("first", "m1", "alice", false, 100.0))
        .is_none());
    assert!(manager
        .prepare(&group_message("second", "m2", "bob", false, 100.0))
        .is_none());

    let current = manager
        .prepare(&group_message("question", "m3", "carol", true, 100.0))
        .expect("mention should process");
    let gc = current.group_context.expect("group_context present");
    let texts: Vec<&str> = gc.messages.iter().map(|m| m.text.as_str()).collect();
    let senders: Vec<&str> = gc.messages.iter().map(|m| m.sender_id.as_str()).collect();
    assert_eq!(texts, vec!["first", "second"]);
    assert_eq!(senders, vec!["alice", "bob"]);
}

#[test]
fn mention_recent_retains_passive_file_only_message() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "mention_recent".to_string(),
        history_limit: 10,
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    let mut passive = group_message("", "m-file", "alice", false, 100.0);
    passive.content.push(ContentPart::File(FileContent {
        filename: "report.pdf".to_string(),
        mime_type: Some("application/pdf".to_string()),
        local_path: Some("qq/channel/report.pdf".to_string()),
        ..Default::default()
    }));

    assert!(manager.should_persist_media(&passive));
    assert!(manager.prepare(&passive).is_none());

    let current = manager
        .prepare(&group_message("summarize it", "m2", "bob", true, 100.0))
        .expect("mention should process");
    let gc = current.group_context.expect("group_context present");
    assert_eq!(gc.messages.len(), 1);
    assert_eq!(gc.messages[0].text, "");
    assert_eq!(gc.messages[0].content.len(), 1);
    assert_eq!(
        gc.messages[0].content[0].local_path(),
        Some("qq/channel/report.pdf")
    );
}

#[test]
fn mention_only_never_retains_passive_chatter() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "mention_only".to_string(),
        history_limit: 10,
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    assert!(manager
        .prepare(&group_message("hidden", "m1", "alice", false, 100.0))
        .is_none());
    let current = manager
        .prepare(&group_message("question", "m2", "bob", true, 100.0))
        .expect("mention should process");
    let gc = current.group_context.expect("group_context present");
    assert!(gc.messages.is_empty());
}

#[test]
fn always_activation_requires_full_visibility() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "auto".to_string(),
        activation: "always".to_string(),
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    assert!(manager
        .prepare(&group_message("ambient", "m1", "alice", false, 100.0))
        .is_none());
    let current = manager
        .prepare(&group_message("question", "m2", "bob", true, 100.0))
        .expect("mention should process");
    let gc = current.group_context.expect("group_context present");
    assert!(gc.capability_degraded);
}

#[test]
fn full_visibility_can_activate_every_message() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "all".to_string(),
        activation: "always".to_string(),
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    let current = manager
        .prepare(&group_message("ambient", "m1", "alice", false, 100.0))
        .expect("always+all should process");
    let gc = current.group_context.expect("group_context present");
    assert_eq!(gc.activation, "always");
}

#[test]
fn context_expires_and_clears_after_reply() {
    let config = GroupContextConfig {
        enabled: true,
        visibility: "mention_recent".to_string(),
        history_ttl_seconds: 5.0,
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    assert!(manager
        .prepare(&group_message("expired", "m1", "alice", false, 100.0))
        .is_none());
    let current = manager
        .prepare(&group_message("question", "m2", "bob", true, 110.0))
        .expect("mention should process");
    let gc = current.group_context.clone().expect("group_context present");
    assert!(gc.messages.is_empty(), "expired buffer should be dropped");

    assert!(manager
        .prepare(&group_message("fresh", "m3", "alice", false, 111.0))
        .is_none());
    manager.mark_replied(&current);
    let next_turn = manager
        .prepare(&group_message("next", "m4", "bob", true, 112.0))
        .expect("mention should process");
    let gc2 = next_turn.group_context.expect("group_context present");
    assert!(gc2.messages.is_empty());
}

#[test]
fn per_group_override_can_disable_history() {
    let mut groups: HashMap<String, HashMap<String, Value>> = HashMap::new();
    let mut override_map = HashMap::new();
    override_map.insert("history".to_string(), json!("none"));
    groups.insert("group-1".to_string(), override_map);

    let config = GroupContextConfig {
        enabled: true,
        groups,
        ..Default::default()
    };
    let mut manager = GroupContextManager::new(Some(config));

    assert!(manager
        .prepare(&group_message("ambient", "m1", "alice", false, 100.0))
        .is_none());
    let current = manager
        .prepare(&group_message("question", "m2", "bob", true, 100.0))
        .expect("mention should process");
    let gc = current.group_context.expect("group_context present");
    assert!(gc.messages.is_empty());
}

#[test]
fn disabled_config_passes_through_without_context() {
    let config = GroupContextConfig::default(); // enabled = false
    let mut manager = GroupContextManager::new(Some(config));
    let prepared = manager
        .prepare(&group_message("hi", "m1", "alice", false, 100.0))
        .expect("disabled config passes through");
    assert!(prepared.group_context.is_none());
}
