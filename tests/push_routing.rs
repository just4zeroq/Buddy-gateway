//! Tests for push routing helpers.

use std::collections::HashMap;

use serde_json::{json, Value};

use buddy_gateway::push_routing::{
    alias_subject_fields, strip_ephemeral_push_meta, EPHEMERAL_PUSH_META,
};

#[test]
fn alias_backfills_missing_keys() {
    let out = alias_subject_fields(HashMap::new(), "openid_1", &["user_openid", "chat_id"]);
    assert_eq!(out.get("user_openid"), Some(&json!("openid_1")));
    assert_eq!(out.get("chat_id"), Some(&json!("openid_1")));
}

#[test]
fn alias_does_not_overwrite() {
    let mut meta = HashMap::new();
    meta.insert("chat_id".to_string(), json!("existing"));
    let out = alias_subject_fields(meta, "openid_1", &["chat_id", "user_openid"]);
    assert_eq!(out.get("chat_id"), Some(&json!("existing")));
    assert_eq!(out.get("user_openid"), Some(&json!("openid_1")));
}

#[test]
fn strip_removes_webhook_and_reply_context() {
    let mut meta: HashMap<String, Value> = HashMap::new();
    meta.insert("chat_id".to_string(), json!("c1"));
    meta.insert("msg_id".to_string(), json!("m1"));
    meta.insert("webhook_url".to_string(), json!("https://example.com/hook"));
    meta.insert("context_token".to_string(), json!("ctx"));

    strip_ephemeral_push_meta(&mut meta);

    let expected: HashMap<String, Value> =
        [("chat_id".to_string(), json!("c1"))].into_iter().collect();
    assert_eq!(meta, expected);
    assert!(EPHEMERAL_PUSH_META.contains(&"webhook_url"));
}
