//! Shared group-conversation policy and recent-message buffering.
//! Mirrors `octop_gateway.group_context`.

use std::collections::{HashMap, VecDeque};

use crate::models::{ContentPart, GroupContext, GroupContextMessage, InboundMessage};
use serde::{Deserialize, Serialize};

/// Group messages that the platform is expected to expose to the bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GroupVisibility {
    #[default]
    Auto,
    All,
    MentionRecent,
    MentionOnly,
}

/// When a visible group message starts an agent turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GroupActivation {
    #[default]
    Mention,
    Always,
}

/// Whether passive group chatter is attached to the next agent turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GroupHistoryMode {
    #[default]
    Recent,
    None,
}

fn parse_enum<E: serde::de::DeserializeOwned + Default>(value: &str) -> E {
    serde_json::from_value(serde_json::Value::String(value.to_string())).unwrap_or_default()
}

/// Effective policy for one group conversation.
#[derive(Debug, Clone)]
pub struct GroupContextPolicy {
    pub enabled: bool,
    pub visibility: GroupVisibility,
    pub activation: GroupActivation,
    pub history: GroupHistoryMode,
    pub history_limit: usize,
    pub history_ttl_seconds: f64,
    pub clear_after_reply: bool,
}

impl Default for GroupContextPolicy {
    fn default() -> Self {
        GroupContextPolicy {
            enabled: false,
            visibility: GroupVisibility::Auto,
            activation: GroupActivation::Mention,
            history: GroupHistoryMode::Recent,
            history_limit: 10,
            history_ttl_seconds: 300.0,
            clear_after_reply: true,
        }
    }
}

/// Serializable default policy with optional per-group overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GroupContextConfig {
    pub enabled: bool,
    pub visibility: String,
    pub activation: String,
    pub history: String,
    pub history_limit: usize,
    pub history_ttl_seconds: f64,
    pub clear_after_reply: bool,
    #[serde(default)]
    pub groups: HashMap<String, HashMap<String, serde_json::Value>>,
}

impl Default for GroupContextConfig {
    fn default() -> Self {
        GroupContextConfig {
            enabled: false,
            visibility: "auto".to_string(),
            activation: "mention".to_string(),
            history: "recent".to_string(),
            history_limit: 10,
            history_ttl_seconds: 300.0,
            clear_after_reply: true,
            groups: HashMap::new(),
        }
    }
}

impl GroupContextConfig {
    pub fn from_dict(data: &HashMap<String, serde_json::Value>) -> Self {
        let get_str = |k: &str, d: &str| -> String {
            data.get(k)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| d.to_string())
        };
        let get_bool = |k: &str, d: bool| -> bool {
            data.get(k).and_then(|v| v.as_bool()).unwrap_or(d)
        };
        let get_usize = |k: &str, d: usize| -> usize {
            data.get(k)
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .unwrap_or(d)
        };
        let get_f64 = |k: &str, d: f64| -> f64 {
            data.get(k)
                .and_then(|v| v.as_f64())
                .unwrap_or(d)
        };

        let mut groups = HashMap::new();
        if let Some(raw) = data.get("groups").and_then(|v| v.as_object()) {
            for (cid, override_val) in raw {
                if let Some(ov) = override_val.as_object() {
                    let map = ov
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<HashMap<String, serde_json::Value>>();
                    groups.insert(cid.clone(), map);
                }
            }
        }

        GroupContextConfig {
            enabled: get_bool("enabled", false),
            visibility: get_str("visibility", "auto"),
            activation: get_str("activation", "mention"),
            history: get_str("history", "recent"),
            history_limit: get_usize("history_limit", 10),
            history_ttl_seconds: get_f64("history_ttl_seconds", 300.0),
            clear_after_reply: get_bool("clear_after_reply", true),
            groups,
        }
    }

    fn policy_from_map(
        enabled: bool,
        visibility: &str,
        activation: &str,
        history: &str,
        history_limit: usize,
        history_ttl_seconds: f64,
        clear_after_reply: bool,
    ) -> GroupContextPolicy {
        GroupContextPolicy {
            enabled,
            visibility: parse_enum(visibility),
            activation: parse_enum(activation),
            history: parse_enum(history),
            history_limit,
            history_ttl_seconds,
            clear_after_reply,
        }
    }

    fn overlay(base: &GroupContextPolicy, values: &HashMap<String, serde_json::Value>) -> GroupContextPolicy {
        let enabled = values
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(base.enabled);
        let visibility = values
            .get("visibility")
            .and_then(|v| v.as_str())
            .unwrap_or("auto");
        let activation = values
            .get("activation")
            .and_then(|v| v.as_str())
            .unwrap_or("mention");
        let history = values.get("history").and_then(|v| v.as_str()).unwrap_or("recent");
        let history_limit = values
            .get("history_limit")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(base.history_limit);
        let history_ttl_seconds = values
            .get("history_ttl_seconds")
            .and_then(|v| v.as_f64())
            .unwrap_or(base.history_ttl_seconds);
        let clear_after_reply = values
            .get("clear_after_reply")
            .and_then(|v| v.as_bool())
            .unwrap_or(base.clear_after_reply);
        GroupContextConfig::policy_from_map(
            enabled,
            visibility,
            activation,
            history,
            history_limit,
            history_ttl_seconds,
            clear_after_reply,
        )
    }

    pub fn resolve(&self, conversation_id: &str) -> GroupContextPolicy {
        let base = GroupContextConfig::policy_from_map(
            self.enabled,
            &self.visibility,
            &self.activation,
            &self.history,
            self.history_limit,
            self.history_ttl_seconds,
            self.clear_after_reply,
        );
        let mut policy = base;
        if let Some(wildcard) = self.groups.get("*") {
            policy = GroupContextConfig::overlay(&policy, wildcard);
        }
        if let Some(specific) = self.groups.get(conversation_id) {
            policy = GroupContextConfig::overlay(&policy, specific);
        }
        policy
    }
}

/// Apply group activation rules and keep bounded passive-message history.
pub struct GroupContextManager {
    config: GroupContextConfig,
    buffers: HashMap<String, VecDeque<GroupContextMessage>>,
}

impl GroupContextManager {
    pub fn new(config: Option<GroupContextConfig>) -> Self {
        GroupContextManager {
            config: config.unwrap_or_default(),
            buffers: HashMap::new(),
        }
    }

    pub fn handles(&self, message: &InboundMessage) -> bool {
        let cid = Self::conversation_id(message);
        cid.is_some() && self.config.resolve(cid.unwrap()).enabled
    }

    pub fn should_persist_media(&self, message: &InboundMessage) -> bool {
        let context_has_media = message.group_context.as_ref().map(|gc| {
            gc.messages.iter().any(|item| {
                item.content
                    .iter()
                    .any(|p| !matches!(p, ContentPart::Text(_)))
            })
        });
        if !message.has_media() && !context_has_media.unwrap_or(false) {
            return false;
        }
        match Self::conversation_id(message) {
            None => true,
            Some(cid) => {
                let policy = self.config.resolve(cid);
                if !policy.enabled {
                    return true;
                }
                if Self::should_process(message, &policy) {
                    return true;
                }
                policy.visibility != GroupVisibility::MentionOnly
                    && policy.history == GroupHistoryMode::Recent
                    && policy.history_limit > 0
            }
        }
    }

    /// Record passive chatter or enrich a message that should reach the agent.
    ///
    /// A message with no conversation id (e.g. a direct chat) always reaches the
    /// agent unchanged. Group messages are buffered/enriched per policy.
    pub fn prepare(&mut self, message: &InboundMessage) -> Option<InboundMessage> {
        let cid = match Self::conversation_id(message) {
            Some(c) => c.to_string(),
            // No conversation id → not a group; pass through untouched.
            None => return Some(message.clone()),
        };
        let policy = self.config.resolve(&cid);
        if !policy.enabled {
            let mut m = message.clone();
            m.group_context = None;
            return Some(m);
        }

        self.expire(&cid, &policy, message.timestamp);
        let (activation, degraded) = Self::effective_activation(&policy);
        let should_process = activation == GroupActivation::Always || Self::is_mentioned(message);

        if !should_process {
            if policy.visibility != GroupVisibility::MentionOnly
                && policy.history == GroupHistoryMode::Recent
            {
                self.append(&cid, message, &policy);
            }
            return None;
        }

        let platform_messages = message
            .group_context
            .as_ref()
            .map(|gc| gc.messages.clone())
            .unwrap_or_default();
        let buffered: Vec<GroupContextMessage> = self
            .buffers
            .get(&cid)
            .map(|d| d.iter().cloned().collect())
            .unwrap_or_default();
        let merged = Self::merge_messages(&platform_messages, &buffered);

        let buffered_trimmed = if policy.history == GroupHistoryMode::None
            || policy.visibility == GroupVisibility::MentionOnly
        {
            Vec::new()
        } else {
            let limit = policy.history_limit;
            if limit > 0 && merged.len() > limit {
                merged[merged.len() - limit..].to_vec()
            } else {
                merged
            }
        };

        let mut m = message.clone();
        m.group_context = Some(GroupContext {
            conversation_id: cid.clone(),
            visibility: format!("{:?}", policy.visibility).to_lowercase(),
            activation: format!("{:?}", activation).to_lowercase(),
            messages: buffered_trimmed,
            capability_degraded: degraded,
        });
        Some(m)
    }

    fn merge_messages(
        platform_messages: &[GroupContextMessage],
        buffered_messages: &[GroupContextMessage],
    ) -> Vec<GroupContextMessage> {
        let mut merged = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for item in platform_messages.iter().chain(buffered_messages.iter()) {
            let media = item
                .content
                .iter()
                .map(|p| {
                    p.url()
                        .map(|u| u.to_string())
                        .or_else(|| p.local_path().map(|l| l.to_string()))
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(",");
            let key = if !item.message_id.is_empty() {
                item.message_id.clone()
            } else {
                format!("{}\0{}\0{}", item.sender_id, item.text, media)
            };
            if seen.insert(key) {
                merged.push(item.clone());
            }
        }
        merged
    }

    fn effective_activation(policy: &GroupContextPolicy) -> (GroupActivation, bool) {
        let activation = policy.activation;
        if activation == GroupActivation::Always && policy.visibility != GroupVisibility::All {
            return (GroupActivation::Mention, true);
        }
        (activation, false)
    }

    /// Truthiness of the `bot_mentioned` flag (mirrors Python `bool(...)`).
    fn is_mentioned(message: &InboundMessage) -> bool {
        matches!(
            message.metadata.get("bot_mentioned"),
            Some(serde_json::Value::Bool(true))
        )
    }

    fn should_process(message: &InboundMessage, policy: &GroupContextPolicy) -> bool {
        let (activation, _) = Self::effective_activation(policy);
        activation == GroupActivation::Always || Self::is_mentioned(message)
    }

    /// Clear one-shot passive context after a successful agent turn.
    pub fn mark_replied(&mut self, message: &InboundMessage) {
        if let Some(cid) = Self::conversation_id(message) {
            let policy = self.config.resolve(cid);
            if policy.enabled && policy.clear_after_reply {
                self.buffers.remove(cid);
            }
        }
    }

    pub fn clear(&mut self, conversation_id: &str) {
        self.buffers.remove(conversation_id);
    }

    fn conversation_id(message: &InboundMessage) -> Option<&str> {
        if let Some(serde_json::Value::String(v)) = message.metadata.get("conversation_id") {
            if !v.is_empty() {
                return Some(v.as_str());
            }
        }
        if let Some(subject) = &message.channel_subject {
            if subject.chat_type == "group" && !subject.subject_id.is_empty() {
                return Some(subject.subject_id.as_str());
            }
        }
        None
    }

    fn append(&mut self, conversation_id: &str, message: &InboundMessage, policy: &GroupContextPolicy) {
        if policy.history_limit == 0 {
            return;
        }
        let text = message.text().trim().to_string();
        let content: Vec<ContentPart> = message
            .content
            .iter()
            .filter(|p| !matches!(p, ContentPart::Text(_)))
            .cloned()
            .collect();
        if text.is_empty() && content.is_empty() {
            return;
        }
        let entry = GroupContextMessage {
            message_id: {
                let v = message
                    .metadata
                    .get("msg_id")
                    .or_else(|| message.metadata.get("message_id"));
                v.and_then(|x| x.as_str()).unwrap_or("").to_string()
            },
            sender_id: message
                .metadata
                .get("sender_id")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string(),
            sender_name: message
                .metadata
                .get("sender_name")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            text,
            content,
            timestamp: message.timestamp,
        };
        let buffer = self.buffers.entry(conversation_id.to_string()).or_default();
        if !entry.message_id.is_empty() && buffer.iter().any(|x| x.message_id == entry.message_id) {
            return;
        }
        buffer.push_back(entry);
        while buffer.len() > policy.history_limit {
            buffer.pop_front();
        }
    }

    fn expire(&mut self, conversation_id: &str, policy: &GroupContextPolicy, now: f64) {
        if policy.history_ttl_seconds <= 0.0 {
            self.buffers.remove(conversation_id);
            return;
        }
        if let Some(buffer) = self.buffers.get_mut(conversation_id) {
            let cutoff = now - policy.history_ttl_seconds;
            while let Some(front) = buffer.front() {
                if front.timestamp < cutoff {
                    buffer.pop_front();
                } else {
                    break;
                }
            }
            if buffer.is_empty() {
                self.buffers.remove(conversation_id);
            }
        }
    }
}
