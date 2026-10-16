//! Runtime channel-kind registration (plugin extension point).

use std::sync::{Arc, Mutex};

use buddy_gateway::channel::{Channel, ChannelCore, MessageProcessor};
use buddy_gateway::channels::{
    build_channel, register_channel_kind, registered_channel_kinds, unregister_channel_kind,
};
use buddy_gateway::errors::Result;
use buddy_gateway::models::{
    ChannelSubject, ContentPart, InboundMessage, MessageEvent, RawPayload,
};

/// Minimal channel used to prove a plugin kind can be built and driven.
struct AcmeChannel {
    sent: Arc<Mutex<Vec<String>>>,
    core: ChannelCore,
}

#[async_trait::async_trait]
impl Channel for AcmeChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
    async fn send_text(&mut self, _subject: &ChannelSubject, text: &str) -> Result<()> {
        self.sent.lock().unwrap().push(text.to_string());
        Ok(())
    }
    async fn send_media(&mut self, _subject: &ChannelSubject, _m: &ContentPart) -> Result<()> {
        Ok(())
    }
    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let text = raw.as_text().unwrap_or("").to_string();
        Ok(InboundMessage {
            channel_id: "acme".to_string(),
            channel_type: "acme".to_string(),
            tenant_id: None,
            channel_subject: Some(ChannelSubject::new("acme-user")),
            channel_session_id: "acme".to_string(),
            content: vec![ContentPart::text(text)],
            group_context: None,
            metadata: Default::default(),
            timestamp: 0.0,
        })
    }
}

fn acme_builder(sent: Arc<Mutex<Vec<String>>>) -> buddy_gateway::channels::ChannelBuilder {
    Arc::new(
        move |_config: &serde_json::Value,
              processor: MessageProcessor,
              channel_id: Option<String>,
              _tenant_id: Option<String>|
              -> Result<Box<dyn Channel>> {
            let mut core = ChannelCore::new(
                channel_id.unwrap_or_else(|| "acme-1".to_string()),
                None,
                0.0,
                Default::default(),
                None,
            );
            core.set_channel_type("acme");
            core.set_processor(processor);
            Ok(Box::new(AcmeChannel {
                sent: Arc::clone(&sent),
                core,
            }))
        },
    )
}

fn echo_processor(prefix: &'static str) -> MessageProcessor {
    Arc::new(move |msg: InboundMessage| {
        let text = format!("{}: {}", prefix, msg.text());
        Box::pin(futures::stream::iter(vec![
            MessageEvent::text(text),
            MessageEvent::completed(),
        ])) as std::pin::Pin<
            Box<dyn futures::Stream<Item = MessageEvent> + Send>,
        >
    })
}

#[test]
fn register_build_and_resolve() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    register_channel_kind("acme-one", acme_builder(Arc::clone(&sent)), "test").unwrap();
    assert!(registered_channel_kinds().contains(&"acme-one".to_string()));

    let ch = build_channel("acme-one", &serde_json::json!({}), echo_processor("Echo"), None, None)
        .expect("plugin kind resolves");
    assert_eq!(ch.channel_type(), "acme");
    assert!(registered_channel_kinds().contains(&"telegram".to_string()));
    assert!(unregister_channel_kind("acme-one", "test").unwrap());
}

#[test]
fn register_rejects_builtin_override() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let err = register_channel_kind("telegram", acme_builder(sent), "test").unwrap_err();
    assert!(err.to_string().contains("builtin"));
}

#[test]
fn register_rejects_empty_kind_and_owner() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    assert!(register_channel_kind("  ", acme_builder(Arc::clone(&sent)), "test").is_err());
    assert!(register_channel_kind("acme-two", acme_builder(sent), "  ").is_err());
    assert!(unregister_channel_kind("acme-two", "").is_err());
}

#[test]
fn same_owner_may_replace_but_others_cannot() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    register_channel_kind("acme-three", acme_builder(Arc::clone(&sent)), "first").unwrap();
    assert!(registered_channel_kinds().contains(&"acme-three".to_string()));

    // Another plugin may not take it over.
    assert!(register_channel_kind("acme-three", acme_builder(Arc::clone(&sent)), "second").is_err());
    assert!(unregister_channel_kind("acme-three", "second").is_err());

    // The owner can replace its registration on reload.
    register_channel_kind("acme-three", acme_builder(Arc::clone(&sent)), "first").unwrap();

    assert!(unregister_channel_kind("acme-three", "first").unwrap());
    assert!(!registered_channel_kinds().contains(&"acme-three".to_string()));
    // Removing it twice is a no-op, not an error.
    assert!(!unregister_channel_kind("acme-three", "first").unwrap());
}

#[test]
fn builtin_kinds_cannot_be_unregistered() {
    assert!(unregister_channel_kind("telegram", "test").is_err());
}

#[test]
fn unknown_kind_still_rejected() {
    assert!(build_channel("definitely-not-a-channel", &serde_json::json!({}), echo_processor("x"), None, None).is_err());
}
