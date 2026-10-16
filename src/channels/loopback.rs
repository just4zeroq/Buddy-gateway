//! Loopback channel — a test/in-process harness.
//!
//! Does not connect to any external platform. Inbound payloads are parsed
//! (optionally echoing), and outbound replies are captured into an in-memory
//! buffer so tests can assert on what the pipeline produced. Faithful to the
//! common contract; useful as the default channel in integration tests.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::channel::Channel;
use crate::errors::Result;
use crate::models::{ChannelSubject, ContentPart, InboundMessage, RawPayload};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LoopbackConfig {
    #[serde(flatten)]
    pub base: crate::channel::ChannelConfig,
    /// When true, inbound text is echoed back as an outbound reply.
    pub echo: bool,
}

crate::channel_adapter! {
    /// In-process loopback channel for tests and harnesses.
    LoopbackChannel, LoopbackConfig, "loopback",
    required = [],
    fields = {
        captured: Arc<Mutex<Vec<ContentPart>>>,
    }
}

impl LoopbackChannel {
    /// Snapshot of everything captured by `send_text` / `send_media`.
    pub fn captured(&self) -> Vec<ContentPart> {
        self.captured.lock().unwrap().clone()
    }

    /// Clear the capture buffer.
    pub fn clear_captured(&self) {
        self.captured.lock().unwrap().clear();
    }
}

#[async_trait]
impl Channel for LoopbackChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }
    async fn start(&mut self) -> Result<()> {
        self.core
            .set_connection_session(Some(format!("loopback-{}", self.core.channel_id())));
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.core.set_connection_session(None);
        Ok(())
    }

    async fn send_text(&mut self, _subject: &ChannelSubject, text: &str) -> Result<()> {
        self.captured
            .lock()
            .unwrap()
            .push(ContentPart::text(text.to_string()));
        Ok(())
    }

    async fn send_media(&mut self, _subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        self.captured.lock().unwrap().push(media.clone());
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let text = match raw {
            RawPayload::Preparsed(m) => return Ok(m.clone()),
            RawPayload::Json(v) => v
                .get("text")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            RawPayload::Text(s) => s.clone(),
        };
        let subject_id = match raw {
            RawPayload::Json(v) => v
                .get("subject_id")
                .and_then(|x| x.as_str())
                .unwrap_or("loopback-user")
                .to_string(),
            _ => "loopback-user".to_string(),
        };

        let mut metadata = HashMap::new();
        metadata.insert("subject_id".to_string(), serde_json::Value::String(subject_id.clone()));

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "loopback".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id: subject_id.clone(),
                display_name: subject_id.clone(),
                chat_type: "direct".to_string(),
                metadata: metadata.clone(),
                first_seen: None,
                last_seen: None,
            }),
            channel_session_id: self
                .core
                .connection_session()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}-unconnected", self.core.channel_id())),
            content: vec![ContentPart::text(text)],
            metadata,
            timestamp: crate::channel::now_f64(),
            group_context: None,
        })
    }
}
