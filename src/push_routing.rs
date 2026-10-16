//! Shared helpers for proactive (bot-initiated) push routing.

use std::collections::HashMap;
use serde_json::Value;

/// Fields that tie a send to a single inbound turn — must not survive a cron/webhook push.
pub const EPHEMERAL_PUSH_META: &[&str] = &[
    "msg_id",
    "message_id",
    "_frame",
    "_ws_client",
    "response_url",
    "context_token",
    "webhook_url",
];

/// Durable copy of the latest iLink context token (ephemeral `context_token` is stripped on push).
pub const WEIXIN_CONTEXT_TOKEN_KEY: &str = "weixin_context_token";
pub const WEIXIN_CONTEXT_TOKEN_MS_KEY: &str = "weixin_context_token_ms";

/// Backfill routing metadata keys from `subject_id` when absent.
pub fn alias_subject_fields(
    mut meta: HashMap<String, Value>,
    subject_id: &str,
    field_names: &[&str],
) -> HashMap<String, Value> {
    if subject_id.is_empty() {
        return meta;
    }
    for name in field_names {
        meta.entry((*name).to_string())
            .or_insert_with(|| Value::String(subject_id.to_string()));
    }
    meta
}

/// Remove passive-reply-only fields so proactive sends use the right path.
pub fn strip_ephemeral_push_meta(meta: &mut HashMap<String, Value>) -> &mut HashMap<String, Value> {
    for key in EPHEMERAL_PUSH_META {
        meta.remove(*key);
    }
    meta
}
