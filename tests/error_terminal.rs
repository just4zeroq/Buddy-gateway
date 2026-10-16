//! ERROR is a terminal event: the failed turn stops, the host producer is
//! released with bounded cleanup, and the next turn still recovers.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};

use buddy_gateway::channel::Channel;
use buddy_gateway::channels::loopback::{LoopbackChannel, LoopbackConfig};
use buddy_gateway::models::{MessageEvent, RawPayload};

mod common;

fn loopback(processor: buddy_gateway::channel::MessageProcessor) -> LoopbackChannel {
    LoopbackChannel::new(processor, LoopbackConfig::default(), None, None, 0.0)
}

fn captured(ch: &LoopbackChannel) -> Vec<String> {
    ch.captured()
        .iter()
        .map(|p| p.text_value().unwrap_or("").to_string())
        .collect()
}

/// A stream that flags when the producer is released (host cleanup).
struct Finalizing {
    inner: stream::Iter<std::vec::IntoIter<MessageEvent>>,
    released: Arc<AtomicBool>,
}

impl Stream for Finalizing {
    type Item = MessageEvent;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<MessageEvent>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Drop for Finalizing {
    fn drop(&mut self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

fn finalizing_processor(events: Vec<MessageEvent>) -> (buddy_gateway::channel::MessageProcessor, Arc<AtomicBool>) {
    let released = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&released);
    let events = Arc::new(events);
    let processor: buddy_gateway::channel::MessageProcessor =
        Arc::new(move |_msg: buddy_gateway::models::InboundMessage| {
            let ev = events.as_ref().clone();
            let stream = Finalizing {
                inner: stream::iter(ev),
                released: Arc::clone(&flag),
            };
            Box::pin(stream) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
        });
    (processor, released)
}

#[tokio::test]
async fn error_is_delivered_and_terminal() {
    let (processor, released) = finalizing_processor(vec![
        MessageEvent::delta("half "),
        MessageEvent::delta("done"),
        MessageEvent::error_event("boom"),
        MessageEvent::text("never delivered"),
        MessageEvent::completed(),
    ]);
    let mut ch = loopback(processor);
    ch.handle_inbound(&RawPayload::Text("hi".to_string()))
        .await
        .unwrap();

    let texts = captured(&ch);
    // The buffered delta is flushed, then the error; nothing after ERROR.
    assert_eq!(texts, vec!["half done".to_string(), "boom".to_string()]);
    // The producer is released even though the consumer stopped at ERROR.
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn error_without_partial_output_still_delivers() {
    let mut ch = loopback(common::static_processor(vec![
        MessageEvent::error_event("auth failed"),
        MessageEvent::completed(),
    ]));
    ch.handle_inbound(&RawPayload::Text("hi".to_string()))
        .await
        .unwrap();
    assert_eq!(captured(&ch), vec!["auth failed".to_string()]);
}

#[tokio::test]
async fn next_turn_recovers_after_error() {
    let events = Arc::new(std::sync::Mutex::new(vec![
        vec![MessageEvent::error_event("boom"), MessageEvent::completed()],
        vec![MessageEvent::text("recovered"), MessageEvent::completed()],
    ]));
    let processor: buddy_gateway::channel::MessageProcessor =
        Arc::new(move |_msg: buddy_gateway::models::InboundMessage| {
            let next = {
                let mut guard = events.lock().unwrap();
                if guard.is_empty() {
                    Vec::new()
                } else {
                    guard.remove(0)
                }
            };
            let s = stream::iter(next);
            Box::pin(s) as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
        });
    let mut ch = loopback(processor);
    ch.handle_inbound(&RawPayload::Text("first".to_string()))
        .await
        .unwrap();
    assert_eq!(captured(&ch), vec!["boom".to_string()]);

    ch.handle_inbound(&RawPayload::Text("second".to_string()))
        .await
        .unwrap();
    assert_eq!(
        captured(&ch),
        vec!["boom".to_string(), "recovered".to_string()]
    );
}

#[tokio::test]
async fn completed_still_marks_success() {
    let mut ch = loopback(common::static_processor(vec![
        MessageEvent::delta("ok"),
        MessageEvent::completed(),
    ]));
    ch.handle_inbound(&RawPayload::Text("hi".to_string()))
        .await
        .unwrap();
    assert_eq!(captured(&ch), vec!["ok".to_string()]);
}
