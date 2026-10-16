//! Tests for models.

use buddy_gateway::models::{
    AudioContent, ChannelSubject, ContentPart, ContentType, FileContent, ImageContent,
    InboundMessage, MessageEvent, MessageEventType, TextContent, VideoContent,
};

#[test]
fn text_content_type() {
    let tc = TextContent {
        text: "hello".to_string(),
    };
    let part = ContentPart::Text(tc);
    assert_eq!(part.content_type(), ContentType::Text);
    assert_eq!(part.text_value(), Some("hello"));
}

#[test]
fn image_content_type() {
    let part = ContentPart::Image(ImageContent {
        url: "https://example.com/img.png".to_string(),
        alt_text: "cat".to_string(),
        ..Default::default()
    });
    assert_eq!(part.content_type(), ContentType::Image);
    assert_eq!(part.url(), Some("https://example.com/img.png"));
    assert!(part.local_path().is_none());
}

#[test]
fn video_content_type() {
    let part = ContentPart::Video(VideoContent {
        url: "https://example.com/vid.mp4".to_string(),
        duration: Some(5000),
        ..Default::default()
    });
    assert_eq!(part.content_type(), ContentType::Video);
    assert_eq!(part.url(), Some("https://example.com/vid.mp4"));
}

#[test]
fn audio_content_type() {
    let part = ContentPart::Audio(AudioContent {
        url: "https://example.com/audio.mp3".to_string(),
        duration: Some(3000),
        ..Default::default()
    });
    assert_eq!(part.content_type(), ContentType::Audio);
}

#[test]
fn file_content_type() {
    let part = ContentPart::File(FileContent {
        url: "https://example.com/doc.pdf".to_string(),
        filename: "doc.pdf".to_string(),
        size: Some(1024),
        ..Default::default()
    });
    assert_eq!(part.content_type(), ContentType::File);
    assert_eq!(part.filename(), Some("doc.pdf"));
    assert_eq!(part.size(), Some(1024));
}

#[test]
fn inbound_basic_creation() {
    let msg = InboundMessage {
        channel_id: "test".to_string(),
        channel_type: String::new(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject::new("user1")),
        channel_session_id: String::new(),
        content: vec![ContentPart::text("hello world")],
        group_context: None,
        metadata: Default::default(),
        timestamp: 0.0,
    };
    assert_eq!(msg.channel_id, "test");
    assert_eq!(msg.channel_subject.as_ref().unwrap().subject_id, "user1");
    assert_eq!(msg.text(), "hello world");
    assert!(!msg.has_media());
}

#[test]
fn inbound_text_concatenates() {
    let msg = InboundMessage {
        channel_id: "test".to_string(),
        channel_type: String::new(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject::new("user1")),
        channel_session_id: String::new(),
        content: vec![ContentPart::text("line 1"), ContentPart::text("line 2")],
        group_context: None,
        metadata: Default::default(),
        timestamp: 0.0,
    };
    assert_eq!(msg.text(), "line 1\nline 2");
}

#[test]
fn inbound_has_media_with_image() {
    let msg = InboundMessage {
        channel_id: "test".to_string(),
        channel_type: String::new(),
        tenant_id: None,
        channel_subject: Some(ChannelSubject::new("user1")),
        channel_session_id: String::new(),
        content: vec![
            ContentPart::text("check this"),
            ContentPart::Image(ImageContent {
                url: "https://example.com/img.png".to_string(),
                ..Default::default()
            }),
        ],
        group_context: None,
        metadata: Default::default(),
        timestamp: 0.0,
    };
    assert!(msg.has_media());
}

#[test]
fn message_event_text_factory() {
    let evt = MessageEvent::text("hello");
    assert_eq!(evt.event_type, MessageEventType::Message);
    assert_eq!(evt.content.len(), 1);
    assert_eq!(evt.content[0].text_value(), Some("hello"));
}

#[test]
fn message_event_typing_factory() {
    let evt = MessageEvent::typing();
    assert_eq!(evt.event_type, MessageEventType::Typing);
    assert!(evt.content.is_empty());
}

#[test]
fn message_event_completed_factory() {
    let evt = MessageEvent::completed();
    assert_eq!(evt.event_type, MessageEventType::Completed);
}

#[test]
fn message_event_error_factory() {
    let evt = MessageEvent::error_event("something broke");
    assert_eq!(evt.event_type, MessageEventType::Error);
    assert_eq!(evt.error.as_deref(), Some("something broke"));
}

#[test]
fn image_local_path_default_none() {
    let part = ContentPart::Image(ImageContent {
        url: "https://example.com/img.png".to_string(),
        ..Default::default()
    });
    assert!(part.local_path().is_none());
}

#[test]
fn image_local_path_set() {
    let part = ContentPart::Image(ImageContent {
        url: "https://example.com/img.png".to_string(),
        local_path: Some("images/abc.png".to_string()),
        ..Default::default()
    });
    assert_eq!(part.local_path(), Some("images/abc.png"));
}

#[test]
fn image_url_optional() {
    // A part can carry only local_path (no remote URL).
    let part = ContentPart::Image(ImageContent {
        local_path: Some("images/abc.png".to_string()),
        ..Default::default()
    });
    assert!(part.url().is_none());
    assert_eq!(part.local_path(), Some("images/abc.png"));
}

#[test]
fn file_label_includes_filename() {
    let part = ContentPart::File(FileContent {
        filename: "doc.pdf".to_string(),
        ..Default::default()
    });
    assert_eq!(part.label_with_filename(), "File: doc.pdf");

    let img = ContentPart::Image(ImageContent {
        url: "x".to_string(),
        ..Default::default()
    });
    assert_eq!(img.label_with_filename(), "Image");
}

#[test]
fn file_local_path_set() {
    let part = ContentPart::File(FileContent {
        filename: "doc.pdf".to_string(),
        local_path: Some("files/doc.pdf".to_string()),
        ..Default::default()
    });
    assert_eq!(part.local_path(), Some("files/doc.pdf"));
}
