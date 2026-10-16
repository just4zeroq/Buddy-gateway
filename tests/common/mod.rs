//! Shared test helpers (not itself a test target).

#![allow(dead_code)]

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::{self, Stream};

use buddy_gateway::channel::{Channel, ChannelCore, MessageProcessor};
use buddy_gateway::constraints::ChannelConstraints;
use buddy_gateway::errors::Result;
use buddy_gateway::media::DynMediaBackend;
use buddy_gateway::models::{
    ChannelSubject, ContentPart, InboundMessage, MessageEvent, RawPayload,
};

/// Build a minimal inbound message with a single text part.
pub fn text_message(text: &str) -> InboundMessage {
    InboundMessage {
        channel_id: "test".to_string(),
        channel_type: "test".to_string(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject::new("user1")),
        channel_session_id: "sess".to_string(),
        content: vec![ContentPart::text(text)],
        group_context: None,
        metadata: Default::default(),
        timestamp: 100.0,
    }
}

/// A processor that always emits the same, pre-canned event sequence.
pub fn static_processor(events: Vec<MessageEvent>) -> MessageProcessor {
    let events = Arc::new(events);
    Arc::new(move |_msg: InboundMessage| {
        let ev = events.as_ref().clone();
        let s = stream::iter(ev);
        Box::pin(s) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

/// A processor that echoes the inbound text as `"{prefix}: {text}"`.
pub fn echo_processor(prefix: &'static str) -> MessageProcessor {
    Arc::new(move |msg: InboundMessage| {
        let s = stream::iter(vec![
            MessageEvent::text(format!("{}: {}", prefix, msg.text())),
            MessageEvent::completed(),
        ]);
        Box::pin(s) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
    })
}

/// A test channel that records `send_text` calls into a shared buffer, so a
/// manager (which takes ownership) can still be observed by the test.
pub struct CaptureChannel {
    core: ChannelCore,
    pub sent: Arc<Mutex<Vec<(String, String)>>>,
    pub started: Arc<AtomicBool>,
    pub stopped: Arc<AtomicBool>,
    pub media_backend_set: Arc<AtomicBool>,
}

impl CaptureChannel {
    pub fn new(
        processor: MessageProcessor,
        sent: Arc<Mutex<Vec<(String, String)>>>,
        id: &str,
    ) -> Self {
        let mut core = ChannelCore::new(
            id.to_string(),
            None,
            0.0,
            ChannelConstraints::default(),
            None,
        );
        core.set_channel_type("capture");
        core.set_processor(processor);
        CaptureChannel {
            core,
            sent,
            started: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            media_backend_set: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl Channel for CaptureChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }
    fn set_media_backend(&mut self, backend: DynMediaBackend) {
        self.media_backend_set.store(true, Ordering::SeqCst);
        self.core.set_media_backend(backend);
    }
    async fn start(&mut self) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        self.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        self.sent
            .lock()
            .unwrap()
            .push((subject.subject_id.clone(), text.to_string()));
        Ok(())
    }
    async fn send_media(&mut self, _subject: &ChannelSubject, _media: &ContentPart) -> Result<()> {
        Ok(())
    }
    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        match raw {
            RawPayload::Preparsed(m) => Ok(m.clone()),
            RawPayload::Text(s) => Ok(text_message(s)),
            RawPayload::Json(_) => Ok(text_message("json")),
        }
    }
}
