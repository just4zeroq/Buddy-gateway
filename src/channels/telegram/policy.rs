//! Pure Telegram admission and activation policy, evaluated before media retrieval.

use serde_json::Value;

use crate::errors::{GatewayError, Result};

/// Normalize a CSV string or array of nonzero integer IDs.
///
/// Empty strings and null mean no restriction. User IDs must be positive; chat
/// allowlists can opt into negative IDs. Invalid values are rejected, including
/// booleans, floats, and empty entries. Duplicates retain first order.
pub fn parse_id_list(value: &Value, allow_negative: bool) -> Result<Vec<String>> {
    let items: Vec<Value> = match value {
        Value::Null => return Ok(Vec::new()),
        Value::String(s) => {
            if s.trim().is_empty() {
                return Ok(Vec::new());
            }
            s.split(',')
                .map(|part| Value::String(part.to_string()))
                .collect()
        }
        Value::Array(arr) => arr.clone(),
        _ => {
            return Err(GatewayError::Other(
                "Telegram IDs must be a CSV string or a list of integers".to_string(),
            ))
        }
    };

    let mut out: Vec<String> = Vec::new();
    for item in items {
        let text = match &item {
            Value::Bool(_) => {
                return Err(GatewayError::Other(
                    "Telegram IDs must be integers, not booleans or floats".to_string(),
                ))
            }
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    n.to_string()
                } else {
                    return Err(GatewayError::Other(
                        "Telegram IDs must be integers, not booleans or floats".to_string(),
                    ));
                }
            }
            Value::String(s) => s.trim().to_string(),
            _ => {
                return Err(GatewayError::Other(
                    "Telegram IDs must be a CSV string or a list of integers".to_string(),
                ))
            }
        };
        let digits = text.strip_prefix('-').unwrap_or(&text);
        if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
            return Err(GatewayError::Other(format!("Invalid Telegram ID: {}", item)));
        }
        if !allow_negative && text.starts_with('-') {
            return Err(GatewayError::Other(format!("Invalid Telegram ID: {}", item)));
        }
        let parsed: i64 = text.parse().map_err(|_| {
            GatewayError::Other(format!("Invalid Telegram ID: {}", item))
        })?;
        if parsed == 0 {
            return Err(GatewayError::Other(format!("Invalid Telegram ID: {}", item)));
        }
        let normalized = parsed.to_string();
        if !out.contains(&normalized) {
            out.push(normalized);
        }
    }
    Ok(out)
}

/// Slice an entity out of `text` using Telegram's UTF-16 offsets.
fn parse_entity(text: &str, offset: usize, length: usize) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let end = (offset + length).min(units.len());
    if offset >= units.len() {
        return String::new();
    }
    String::from_utf16_lossy(&units[offset..end])
}

/// Apply allowlists, command ownership, and group activation without I/O.
///
/// Private chats bypass group activation. A command addressed to another bot is
/// rejected in every mode. Restricted group modes require a known bot identity;
/// each trigger uses the corresponding ID or username when available. Anonymous
/// group senders cannot satisfy a user allowlist.
#[allow(clippy::too_many_arguments)]
pub fn should_accept_message(
    message: &Value,
    bot_id: Option<i64>,
    bot_username: Option<&str>,
    activation: &str,
    allowed_user_ids: &[String],
    allowed_chat_ids: &[String],
    allow_unaddressed_commands: bool,
) -> bool {
    let chat = message.get("chat");
    let chat_type = chat
        .and_then(|c| c.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    if !matches!(chat_type, "private" | "group" | "supergroup") {
        return false;
    }
    let chat_id = chat
        .and_then(|c| c.get("id"))
        .map(|v| match v {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default();
    if !allowed_chat_ids.is_empty() && !allowed_chat_ids.contains(&chat_id) {
        return false;
    }
    if !allowed_user_ids.is_empty() {
        if message.get("sender_chat").is_some() || message.get("from").is_none() {
            return false;
        }
        let user_id = id_to_string(message.get("from").and_then(|f| f.get("id")));
        if !allowed_user_ids.contains(&user_id) {
            return false;
        }
    }

    let text = message
        .get("text")
        .and_then(|t| t.as_str())
        .or_else(|| message.get("caption").and_then(|t| t.as_str()))
        .unwrap_or("")
        .trim()
        .to_string();
    let has_media = ["photo", "document", "video", "voice", "audio"]
        .iter()
        .any(|k| message.get(k).map(|v| !v.is_null()).unwrap_or(false));
    if text.is_empty() && !has_media {
        return false;
    }

    let username = bot_username.unwrap_or("").trim_start_matches('@').to_lowercase();
    let mut mentioned = false;
    let mut own_command = false;
    let mut bare_command = false;

    for (entities_key, text_key) in [("entities", "text"), ("caption_entities", "caption")] {
        let source = message
            .get(text_key)
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let entities = match message.get(entities_key).and_then(|e| e.as_array()) {
            Some(e) => e,
            None => continue,
        };
        for entity in entities {
            let kind = entity.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let offset = entity.get("offset").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let length = entity.get("length").and_then(|o| o.as_u64()).unwrap_or(0) as usize;
            let token = parse_entity(&source, offset, length);
            match kind {
                "text_mention" => {
                    let uid = entity.get("user").and_then(|u| u.get("id")).and_then(|i| i.as_i64());
                    if bot_id.is_some() && uid == bot_id {
                        mentioned = true;
                    }
                }
                "mention" => {
                    if !username.is_empty() && token.to_lowercase() == format!("@{}", username) {
                        mentioned = true;
                    }
                }
                "bot_command" => match token.find('@') {
                    Some(pos) => {
                        let target = token[pos + 1..].to_lowercase();
                        if username.is_empty() || target != username {
                            return false;
                        }
                        own_command = true;
                    }
                    None => bare_command = true,
                },
                _ => {}
            }
        }
    }

    if chat_type == "private" {
        return true;
    }
    if activation == "always" {
        return true;
    }
    if !matches!(activation, "mention" | "mention_or_reply") || (bot_id.is_none() && username.is_empty()) {
        return false;
    }
    if mentioned || own_command || (bare_command && allow_unaddressed_commands) {
        return true;
    }
    // Reply-to-bot trigger.
    let reply = message.get("reply_to_message");
    activation == "mention_or_reply"
        && bot_id.is_some()
        && reply.is_some()
        && reply.and_then(|r| r.get("sender_chat")).is_none()
        && reply
            .and_then(|r| r.get("from"))
            .and_then(|f| f.get("id"))
            .and_then(|i| i.as_i64())
            == bot_id
}

fn id_to_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_id_list_normalizes_and_rejects() {
        assert_eq!(parse_id_list(&json!("1, 02 ,1"), false).unwrap(), vec!["1", "2"]);
        assert_eq!(parse_id_list(&json!([1, "2"]), false).unwrap(), vec!["1", "2"]);
        assert_eq!(parse_id_list(&json!(""), false).unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_id_list(&json!([-100, 5]), true).unwrap(),
            vec!["-100", "5"]
        );
        assert!(parse_id_list(&json!("0"), false).is_err());
        assert!(parse_id_list(&json!("abc"), false).is_err());
        assert!(parse_id_list(&json!(true), false).is_err());
        assert!(parse_id_list(&json!(1.5), false).is_err());
        assert!(parse_id_list(&json!([-1]), false).is_err());
    }

    fn private() -> Value {
        json!({"chat": {"id": 1, "type": "private"}, "from": {"id": 7}, "text": "hi"})
    }

    #[test]
    fn private_chat_bypasses_activation() {
        assert!(should_accept_message(
            &private(),
            None,
            None,
            "mention",
            &[],
            &[],
            false
        ));
    }

    #[test]
    fn group_requires_trigger_in_mention_mode() {
        let msg = json!({"chat": {"id": -1, "type": "group"}, "from": {"id": 7}, "text": "hello"});
        assert!(should_accept_message(&msg, Some(9), Some("bot"), "always", &[], &[], false));
        assert!(!should_accept_message(&msg, Some(9), Some("bot"), "mention", &[], &[], false));

        let mentioned = json!({
            "chat": {"id": -1, "type": "group"},
            "from": {"id": 7},
            "text": "hi @bot",
            "entities": [{"type": "mention", "offset": 3, "length": 4}],
        });
        assert!(should_accept_message(
            &mentioned,
            Some(9),
            Some("Bot"),
            "mention",
            &[],
            &[],
            false
        ));

        let other_bot = json!({
            "chat": {"id": -1, "type": "group"},
            "from": {"id": 7},
            "text": "/start@other",
            "entities": [{"type": "bot_command", "offset": 0, "length": 12}],
        });
        assert!(!should_accept_message(
            &other_bot,
            Some(9),
            Some("bot"),
            "always",
            &[],
            &[],
            false
        ));

        let own = json!({
            "chat": {"id": -1, "type": "group"},
            "from": {"id": 7},
            "text": "/start@bot",
            "entities": [{"type": "bot_command", "offset": 0, "length": 10}],
        });
        assert!(should_accept_message(&own, Some(9), Some("bot"), "mention", &[], &[], false));

        let bare = json!({
            "chat": {"id": -1, "type": "group"},
            "from": {"id": 7},
            "text": "/start",
            "entities": [{"type": "bot_command", "offset": 0, "length": 6}],
        });
        assert!(!should_accept_message(&bare, Some(9), Some("bot"), "mention", &[], &[], false));
        assert!(should_accept_message(&bare, Some(9), Some("bot"), "mention", &[], &[], true));
    }

    #[test]
    fn reply_trigger_needs_mention_or_reply() {
        let msg = json!({
            "chat": {"id": -1, "type": "supergroup"},
            "from": {"id": 7},
            "text": "yes",
            "reply_to_message": {"from": {"id": 9}, "text": "want?"},
        });
        assert!(should_accept_message(
            &msg,
            Some(9),
            Some("bot"),
            "mention_or_reply",
            &[],
            &[],
            false
        ));
        assert!(!should_accept_message(&msg, Some(9), Some("bot"), "mention", &[], &[], false));
    }

    #[test]
    fn allowlists_are_enforced() {
        let msg = json!({"chat": {"id": -1, "type": "group"}, "from": {"id": 7}, "text": "hi"});
        assert!(!should_accept_message(
            &msg,
            Some(9),
            Some("bot"),
            "always",
            &["8".into()],
            &[],
            false
        ));
        assert!(!should_accept_message(
            &msg,
            Some(9),
            Some("bot"),
            "always",
            &[],
            &["-2".into()],
            false
        ));
        let anon = json!({"chat": {"id": -1, "type": "group"}, "sender_chat": {"id": -3}, "text": "hi"});
        assert!(!should_accept_message(
            &anon,
            Some(9),
            Some("bot"),
            "always",
            &["7".into()],
            &[],
            false
        ));
    }

    #[test]
    fn empty_message_without_media_is_rejected() {
        let msg = json!({"chat": {"id": 1, "type": "private"}, "from": {"id": 7}});
        assert!(!should_accept_message(&msg, Some(9), Some("bot"), "always", &[], &[], false));
        let photo = json!({"chat": {"id": 1, "type": "private"}, "from": {"id": 7}, "photo": [{"file_id": "x"}]});
        assert!(should_accept_message(&photo, Some(9), Some("bot"), "always", &[], &[], false));
    }

    #[test]
    fn restricted_mode_needs_bot_identity() {
        let msg = json!({"chat": {"id": -1, "type": "group"}, "from": {"id": 7}, "text": "hi"});
        assert!(!should_accept_message(&msg, None, None, "mention", &[], &[], false));
    }
}
