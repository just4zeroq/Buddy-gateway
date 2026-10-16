//! Data models for buddy-gateway.
//!
//! Unified content type system, message models, and routing types used across
//! all channel implementations.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Content types
// ---------------------------------------------------------------------------

/// Supported content types for message parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    Text,
    Image,
    Video,
    Audio,
    File,
}

/// Plain text content part.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextContent {
    pub text: String,
}

/// Image content part.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageContent {
    pub url: String,
    pub alt_text: String,
    pub width: Option<u64>,
    pub height: Option<u64>,
    pub size: Option<u64>,
    pub mime_type: Option<String>,
    pub local_path: Option<String>,
    pub data: Option<String>,
}

/// Video content part.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VideoContent {
    pub url: String,
    pub duration: Option<u64>,
    pub thumbnail_url: Option<String>,
    pub size: Option<u64>,
    pub mime_type: Option<String>,
    pub local_path: Option<String>,
    pub data: Option<String>,
}

/// Audio content part.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AudioContent {
    pub url: String,
    pub duration: Option<u64>,
    pub size: Option<u64>,
    pub mime_type: Option<String>,
    pub local_path: Option<String>,
    pub data: Option<String>,
}

/// File attachment content part.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FileContent {
    pub url: String,
    pub filename: String,
    pub size: Option<u64>,
    pub mime_type: Option<String>,
    pub local_path: Option<String>,
    pub data: Option<String>,
}

/// Unified content part — discriminated union on the `type` tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ContentPart {
    Text(TextContent),
    Image(ImageContent),
    Video(VideoContent),
    Audio(AudioContent),
    File(FileContent),
}

impl ContentPart {
    /// Build a text content part.
    pub fn text<S: Into<String>>(text: S) -> Self {
        ContentPart::Text(TextContent { text: text.into() })
    }

    pub fn content_type(&self) -> ContentType {
        match self {
            ContentPart::Text(_) => ContentType::Text,
            ContentPart::Image(_) => ContentType::Image,
            ContentPart::Video(_) => ContentType::Video,
            ContentPart::Audio(_) => ContentType::Audio,
            ContentPart::File(_) => ContentType::File,
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(self, ContentPart::Text(_))
    }

    pub fn text_value(&self) -> Option<&str> {
        match self {
            ContentPart::Text(t) => Some(t.text.as_str()),
            _ => None,
        }
    }

    pub fn url(&self) -> Option<&str> {
        match self {
            ContentPart::Image(x) => opt(&x.url),
            ContentPart::Video(x) => opt(&x.url),
            ContentPart::Audio(x) => opt(&x.url),
            ContentPart::File(x) => opt(&x.url),
            ContentPart::Text(_) => None,
        }
    }

    pub fn data(&self) -> Option<&str> {
        match self {
            ContentPart::Image(x) => x.data.as_deref(),
            ContentPart::Video(x) => x.data.as_deref(),
            ContentPart::Audio(x) => x.data.as_deref(),
            ContentPart::File(x) => x.data.as_deref(),
            ContentPart::Text(_) => None,
        }
    }

    pub fn local_path(&self) -> Option<&str> {
        match self {
            ContentPart::Image(x) => x.local_path.as_deref(),
            ContentPart::Video(x) => x.local_path.as_deref(),
            ContentPart::Audio(x) => x.local_path.as_deref(),
            ContentPart::File(x) => x.local_path.as_deref(),
            ContentPart::Text(_) => None,
        }
    }

    pub fn mime_type(&self) -> Option<&str> {
        match self {
            ContentPart::Image(x) => x.mime_type.as_deref(),
            ContentPart::Video(x) => x.mime_type.as_deref(),
            ContentPart::Audio(x) => x.mime_type.as_deref(),
            ContentPart::File(x) => x.mime_type.as_deref(),
            ContentPart::Text(_) => None,
        }
    }

    pub fn filename(&self) -> Option<&str> {
        match self {
            ContentPart::File(x) => opt(&x.filename),
            _ => None,
        }
    }

    pub fn size(&self) -> Option<u64> {
        match self {
            ContentPart::Image(x) => x.size,
            ContentPart::Video(x) => x.size,
            ContentPart::Audio(x) => x.size,
            ContentPart::File(x) => x.size,
            ContentPart::Text(_) => None,
        }
    }

    /// Human-readable label for text-fallback markers.
    pub fn label(&self) -> &'static str {
        match self {
            ContentPart::Text(_) => "Text",
            ContentPart::Image(_) => "Image",
            ContentPart::Video(_) => "Video",
            ContentPart::Audio(_) => "Audio",
            ContentPart::File(_) => "File",
        }
    }

    /// Label including the filename for file parts
    /// `BaseChannel._get_media_label` (`"File: {filename}"`).
    pub fn label_with_filename(&self) -> String {
        match self {
            ContentPart::File(f) if !f.filename.is_empty() => format!("File: {}", f.filename),
            other => other.label().to_string(),
        }
    }
}

fn opt(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

// ---------------------------------------------------------------------------
// Message event types
// ---------------------------------------------------------------------------

/// Types of events emitted by the message processor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageEventType {
    Message,
    Delta,
    Thinking,
    ThinkingDelta,
    Flush,
    Typing,
    ToolStart,
    ToolEnd,
    Error,
    Completed,
}

/// Event emitted by the message processor back to the channel for delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEvent {
    #[serde(rename = "type")]
    pub event_type: MessageEventType,
    #[serde(default)]
    pub content: Vec<ContentPart>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    #[serde(default)]
    pub error: Option<String>,
}

impl MessageEvent {
    pub fn text<S: Into<String>>(text: S) -> Self {
        MessageEvent {
            event_type: MessageEventType::Message,
            content: vec![ContentPart::text(text)],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn delta<S: Into<String>>(text: S) -> Self {
        MessageEvent {
            event_type: MessageEventType::Delta,
            content: vec![ContentPart::text(text)],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn typing() -> Self {
        MessageEvent {
            event_type: MessageEventType::Typing,
            content: vec![],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn flush() -> Self {
        MessageEvent {
            event_type: MessageEventType::Flush,
            content: vec![],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn thinking<S: Into<String>>(text: S) -> Self {
        MessageEvent {
            event_type: MessageEventType::Thinking,
            content: vec![ContentPart::text(text)],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn thinking_delta<S: Into<String>>(text: S) -> Self {
        MessageEvent {
            event_type: MessageEventType::ThinkingDelta,
            content: vec![ContentPart::text(text)],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn tool_start<S: Into<String>>(tool_name: S) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("tool_name".to_string(), Value::String(tool_name.into()));
        MessageEvent {
            event_type: MessageEventType::ToolStart,
            content: vec![],
            metadata,
            error: None,
        }
    }

    pub fn tool_end<S: Into<String>>(tool_name: S) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("tool_name".to_string(), Value::String(tool_name.into()));
        MessageEvent {
            event_type: MessageEventType::ToolEnd,
            content: vec![],
            metadata,
            error: None,
        }
    }

    pub fn completed() -> Self {
        MessageEvent {
            event_type: MessageEventType::Completed,
            content: vec![],
            metadata: HashMap::new(),
            error: None,
        }
    }

    pub fn error_event<S: Into<String>>(message: S) -> Self {
        MessageEvent {
            event_type: MessageEventType::Error,
            content: vec![],
            metadata: HashMap::new(),
            error: Some(message.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Subject registry model
// ---------------------------------------------------------------------------

/// Known outbound routing subject (a direct user or group conversation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelSubject {
    pub subject_id: String,
    #[serde(default)]
    pub first_seen: Option<f64>,
    #[serde(default)]
    pub last_seen: Option<f64>,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub chat_type: String,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

impl ChannelSubject {
    pub fn new<S: Into<String>>(subject_id: S) -> Self {
        ChannelSubject {
            subject_id: subject_id.into(),
            first_seen: None,
            last_seen: None,
            display_name: String::new(),
            chat_type: String::new(),
            metadata: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Group context
// ---------------------------------------------------------------------------

/// One passive group message supplied as background for a later turn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupContextMessage {
    #[serde(default)]
    pub message_id: String,
    pub sender_id: String,
    #[serde(default)]
    pub sender_name: String,
    pub text: String,
    #[serde(default)]
    pub content: Vec<ContentPart>,
    #[serde(default)]
    pub timestamp: f64,
}

/// Structured, short-lived group context attached to the current message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupContext {
    pub conversation_id: String,
    pub visibility: String,
    pub activation: String,
    #[serde(default)]
    pub messages: Vec<GroupContextMessage>,
    #[serde(default)]
    pub capability_degraded: bool,
}

// ---------------------------------------------------------------------------
// Inbound message
// ---------------------------------------------------------------------------

/// Inbound message from user to bot (normalized from platform-native format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundMessage {
    pub channel_id: String,
    #[serde(default)]
    pub channel_type: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub channel_subject: Option<ChannelSubject>,
    #[serde(default)]
    pub channel_session_id: String,
    pub content: Vec<ContentPart>,
    #[serde(default)]
    pub group_context: Option<GroupContext>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
    #[serde(default)]
    pub timestamp: f64,
}

impl InboundMessage {
    /// Extract concatenated text from all `TextContent` parts.
    pub fn text(&self) -> String {
        let mut parts = Vec::new();
        for c in &self.content {
            if let Some(t) = c.text_value() {
                if !t.is_empty() {
                    parts.push(t);
                }
            }
        }
        parts.join("\n")
    }

    /// True if message contains any media (image/video/audio/file).
    pub fn has_media(&self) -> bool {
        self.content.iter().any(|c| !c.is_text())
    }
}

// ---------------------------------------------------------------------------
// Raw payload (the input to `parse_inbound`)
// ---------------------------------------------------------------------------

/// Untyped payload handed to `parse_inbound`.
#[derive(Debug, Clone)]
pub enum RawPayload {
    Json(Value),
    Text(String),
    Preparsed(InboundMessage),
}

impl RawPayload {
    pub fn into_json(self) -> Result<Value, serde_json::Error> {
        match self {
            RawPayload::Json(v) => Ok(v),
            RawPayload::Text(s) => serde_json::from_str(&s),
            RawPayload::Preparsed(m) => serde_json::to_value(m),
        }
    }

    pub fn as_json(&self) -> Option<&Value> {
        match self {
            RawPayload::Json(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            RawPayload::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
}
