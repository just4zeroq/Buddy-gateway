//! Webhook service + webhook-channel integration tests.
//!
//! These exercise the real HTTP path: a mounted [`WebhookServer`] router is
//! served on an ephemeral port and driven with signed requests, so routing,
//! signature verification and response shaping are all covered end to end.

use std::sync::Arc;

use serde_json::json;

use buddy_gateway::channels::{
    ChannelKind, SUPPORTED_CHANNEL_KINDS, WEBHOOK_CHANNEL_KINDS, build_webhook_adapter,
    webhook_path_for,
};
use buddy_gateway::channel::Channel;
use buddy_gateway::channels::line::{LineChannel, LineConfig, LineWebhook};
use buddy_gateway::channels::whatsapp::{WhatsAppConfig, WhatsAppWebhook};
use buddy_gateway::manager::{ChannelManager, ChannelOptions};
use buddy_gateway::models::RawPayload;
use buddy_gateway::webhook::{
    WebhookResponse, WebhookServer, constant_time_eq, hmac_sha1_hex, hmac_sha256_base64,
    hmac_sha256_hex, verify_sha256_base64_signature, verify_sha256_hex_signature,
    wechat_signature,
};

fn echo_processor() -> buddy_gateway::channel::MessageProcessor {
    use futures::stream;
    use std::pin::Pin;
    Arc::new(|msg: buddy_gateway::models::InboundMessage| {
        let s = stream::iter(vec![
            buddy_gateway::models::MessageEvent::text(format!("Echo: {}", msg.text())),
            buddy_gateway::models::MessageEvent::completed(),
        ]);
        Box::pin(s) as Pin<Box<dyn stream::Stream<Item = buddy_gateway::models::MessageEvent> + Send>>
    })
}

// ---------------------------------------------------------------------------
// Signature primitives
// ---------------------------------------------------------------------------

#[test]
fn hmac_sha256_matches_known_vector() {
    // RFC 4231 test case 2.
    let mac = hmac_sha256_hex(b"Jefe", b"what do ya want for nothing?");
    assert_eq!(
        mac,
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn hmac_sha1_hex_is_stable() {
    let mac = hmac_sha1_hex(b"key", b"The quick brown fox jumps over the lazy dog");
    assert_eq!(mac, "de7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9");
}

#[test]
fn constant_time_eq_semantics() {
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"ab"));
    assert!(constant_time_eq(b"", b""));
}

#[test]
fn hub_signature_verification() {
    let body = br#"{"hello":"world"}"#;
    let header = format!("sha256={}", hmac_sha256_hex(b"secret", body));
    assert!(verify_sha256_hex_signature("secret", body, &header));
    assert!(!verify_sha256_hex_signature("other", body, &header));
    // A bare hex digest without the `sha256=` prefix must not pass.
    assert!(!verify_sha256_hex_signature(
        "secret",
        body,
        &header["sha256=".len()..]
    ));
}

#[test]
fn line_signature_verification() {
    let body = br#"{"events":[]}"#;
    let sig = hmac_sha256_base64(b"secret", body);
    assert!(verify_sha256_base64_signature("secret", body, &sig));
    assert!(!verify_sha256_base64_signature("secret", body, "AAAA"));
}

#[test]
fn wechat_signature_sorts_its_inputs() {
    // SHA1 of the concatenated, sorted [token, timestamp, nonce].
    let a = wechat_signature("tok", "111", "222", None);
    let b = wechat_signature("tok", "222", "111", None);
    assert_eq!(a, b);
    assert_ne!(a, wechat_signature("other", "111", "222", None));
    // Safe mode folds the ciphertext into the digest.
    assert_ne!(
        wechat_signature("tok", "111", "222", Some("enc")),
        wechat_signature("tok", "111", "222", None)
    );
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[test]
fn new_kinds_are_registered() {
    for kind in ["slack", "whatsapp", "line", "wechat_mp", "googlechat"] {
        assert!(
            SUPPORTED_CHANNEL_KINDS.contains(&kind),
            "{kind} missing from SUPPORTED_CHANNEL_KINDS"
        );
        let parsed = ChannelKind::from_str(kind).expect("kind parses");
        assert_eq!(parsed.as_str(), kind);
    }
    assert_eq!(WEBHOOK_CHANNEL_KINDS.len(), 4);
}

#[test]
fn webhook_paths_are_stable() {
    assert_eq!(webhook_path_for("whatsapp").unwrap(), "/webhooks/whatsapp");
    assert_eq!(webhook_path_for("line").unwrap(), "/webhooks/line");
    assert_eq!(webhook_path_for("wechat_mp").unwrap(), "/webhooks/wechat");
    assert_eq!(webhook_path_for("googlechat").unwrap(), "/webhooks/googlechat");
    // Slack uses Socket Mode and has no webhook route by default.
    assert!(webhook_path_for("slack").is_err());
    assert!(webhook_path_for("nope").is_err());
}

#[test]
fn adapters_require_credentials() {
    let err = build_webhook_adapter("line", &json!({ "channel_secret": "s" }))
        .err()
        .expect("missing token should fail");
    let msg = err.to_string();
    assert!(msg.contains("channel_access_token"), "unexpected: {msg}");

    let err = build_webhook_adapter("whatsapp", &json!({}))
        .err()
        .expect("missing credentials should fail");
    assert!(err.to_string().contains("phone_number_id"));

    // Google Chat needs no static credential for the inbound path.
    assert!(build_webhook_adapter("googlechat", &json!({})).is_ok());

    assert!(build_webhook_adapter("telegram", &json!({})).is_err());
}

#[test]
fn adapter_aliases_are_normalised() {
    // `access_token` -> `channel_access_token`, `secret` -> `channel_secret`.
    let adapter = build_webhook_adapter("line", &json!({ "access_token": "tok", "secret": "sec" }));
    assert!(
        adapter.is_ok(),
        "alias normalisation failed: {}",
        adapter.err().map(|e| e.to_string()).unwrap_or_default()
    );
}

// ---------------------------------------------------------------------------
// Server routing
// ---------------------------------------------------------------------------

fn line_config() -> LineConfig {
    LineConfig {
        channel_access_token: "token".into(),
        channel_secret: "secret".into(),
        ..Default::default()
    }
}

fn whatsapp_config() -> WhatsAppConfig {
    WhatsAppConfig {
        phone_number_id: "123".into(),
        access_token: "token".into(),
        verify_token: "verify-me".into(),
        app_secret: "secret".into(),
        ..Default::default()
    }
}

/// Register the two channels used by the routing tests (no network: the manager
/// is never started, so `Channel::start` is not called).
async fn manager_with_channels() -> ChannelManager {
    let mut manager = ChannelManager::new(Some(echo_processor())).with_workers_per_channel(1);
    manager
        .add_line_channel(
            line_config(),
            ChannelOptions {
                channel_id: Some("line-test".into()),
                ..Default::default()
            },
        )
        .await
        .expect("register line");
    manager
        .add_whatsapp_channel(
            whatsapp_config(),
            ChannelOptions {
                channel_id: Some("wa-test".into()),
                ..Default::default()
            },
        )
        .await
        .expect("register whatsapp");
    manager
}

async fn server_with_routes() -> (WebhookServer, ChannelManager) {
    let manager = manager_with_channels().await;
    let mut server = WebhookServer::new();
    server
        .mount_channel(
            &manager,
            "/webhooks/line",
            Arc::new(LineWebhook::new(line_config())),
            "line-test",
        )
        .await
        .expect("mount line");
    server
        .mount_channel(
            &manager,
            "/webhooks/whatsapp",
            Arc::new(WhatsAppWebhook::new(whatsapp_config())),
            "wa-test",
        )
        .await
        .expect("mount whatsapp");
    (server, manager)
}

/// Serve `router` on an ephemeral port and return its base URL.
async fn spawn(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn mount_reports_paths_and_rejects_duplicates() {
    let (mut server, _manager) = server_with_routes().await;
    let paths = server.paths();
    assert!(paths.contains(&"/webhooks/line".to_string()));
    assert!(paths.contains(&"/webhooks/whatsapp".to_string()));

    // Mounting the same path twice is a configuration error.
    let dup = server.mount(
        "/webhooks/line/",
        buddy_gateway::webhook::WebhookRoute {
            path: String::new(),
            channel_id: "x".into(),
            channel_type: "line".into(),
            adapter: Arc::new(LineWebhook::new(line_config())),
            enqueue: Arc::new(|_| {}),
        },
    );
    assert!(dup.is_err(), "duplicate mount should fail");

    // Trailing slashes normalise onto the same route.
    assert!(server.get("/webhooks/line").is_some());
}

#[tokio::test]
async fn unknown_path_is_404_and_get_without_handshake_is_405() {
    let (server, _manager) = server_with_routes().await;
    let base = spawn(server.into_router()).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/nope"))
        .body("{}")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 404);

    // LINE has no GET handshake.
    let resp = client
        .get(format!("{base}/webhooks/line"))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 405);
}

#[tokio::test]
async fn line_post_requires_a_valid_signature() {
    let (server, _manager) = server_with_routes().await;
    let base = spawn(server.into_router()).await;
    let client = reqwest::Client::new();
    let body = json!({
        "destination": "Ubot",
        "events": [{
            "type": "message",
            "webhookEventId": "evt-1",
            "replyToken": "reply-1",
            "timestamp": 1729000000000u64,
            "source": { "type": "user", "userId": "U1" },
            "message": { "id": "msg-1", "type": "text", "text": "hello" }
        }]
    })
    .to_string();

    // No signature header.
    let resp = client
        .post(format!("{base}/webhooks/line"))
        .body(body.clone())
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);

    // Wrong signature.
    let resp = client
        .post(format!("{base}/webhooks/line"))
        .header("X-Line-Signature", "AAAA")
        .body(body.clone())
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);

    // Correct signature.
    let sig = hmac_sha256_base64(b"secret", body.as_bytes());
    let resp = client
        .post(format!("{base}/webhooks/line"))
        .header("X-Line-Signature", sig)
        .header("Content-Type", "application/json")
        .body(body.clone())
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "{\"status\":\"ok\"}");
}

#[tokio::test]
async fn whatsapp_handshake_and_signed_post() {
    let (server, _manager) = server_with_routes().await;
    let base = spawn(server.into_router()).await;
    let client = reqwest::Client::new();

    // Handshake with the right verify token echoes the challenge.
    let resp = client
        .get(format!(
            "{base}/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=verify-me&hub.challenge=abc123"
        ))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "abc123");

    // Handshake with the wrong token is refused.
    let resp = client
        .get(format!(
            "{base}/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=nope&hub.challenge=abc123"
        ))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 403);

    let body = json!({
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "WABA",
            "changes": [{
                "field": "messages",
                "value": {
                    "metadata": { "phone_number_id": "123" },
                    "contacts": [{ "profile": { "name": "Alice" }, "wa_id": "16505551234" }],
                    "messages": [{
                        "from": "16505551234",
                        "id": "wamid.1",
                        "timestamp": "1729000000",
                        "type": "text",
                        "text": { "body": "hi" }
                    }]
                }
            }]
        }]
    })
    .to_string();

    let sig = format!("sha256={}", hmac_sha256_hex(b"secret", body.as_bytes()));
    let resp = client
        .post(format!("{base}/webhooks/whatsapp"))
        .header("X-Hub-Signature-256", sig)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "EVENT_RECEIVED");
}

// ---------------------------------------------------------------------------
// Pipeline handoff
// ---------------------------------------------------------------------------

/// The worker parses each queued item once (to derive the session key and to
/// decide what to batch) and then feeds the merged message back through the
/// pipeline as `RawPayload::Preparsed`. A channel that only understands its own
/// native JSON shape must still accept that second hand-off — this used to fail
/// with "expects a JSON payload".
#[tokio::test]
async fn preparsed_payload_is_not_reparsed() {
    let mut channel = LineChannel::new(
        echo_processor(),
        line_config(),
        Some("line-preparsed".into()),
        None,
        0.0,
    );

    let native = json!({
        "_line": true,
        "event_id": "e1",
        "event_type": "message",
        "source_type": "user",
        "user_id": "U1",
        "reply_token": "rt",
        "message_id": "m1",
        "message_type": "text",
        "text": "hi",
        "timestamp": 1729000000000u64,
        "files": [],
    });
    let parsed = channel
        .parse_inbound(&RawPayload::Json(native))
        .expect("native parse");
    assert_eq!(
        parsed.channel_subject.as_ref().unwrap().subject_id,
        "U1"
    );
    assert_eq!(parsed.text(), "hi");

    // A foreign payload is still rejected by `parse_inbound`...
    assert!(channel
        .parse_inbound(&RawPayload::Text("nope".into()))
        .is_err());
    // ...but an already-parsed message passes through untouched.
    let (prepared, subject) = channel
        .prepare_inbound(&RawPayload::Preparsed(parsed))
        .await
        .expect("preparsed must not be re-parsed")
        .expect("preparsed must survive group context");
    assert_eq!(prepared.text(), "hi");
    assert_eq!(subject.subject_id, "U1");
}

// ---------------------------------------------------------------------------
// Response / outcome helpers
// ---------------------------------------------------------------------------

#[test]
fn response_constructors() {
    let r = WebhookResponse::text("ok");
    assert_eq!(r.status, 200);
    assert!(r.content_type.starts_with("text/plain"));

    let r = WebhookResponse::json(&json!({ "status": "ok" }));
    assert_eq!(r.content_type, "application/json");
    assert_eq!(r.body, "{\"status\":\"ok\"}");

    let r = WebhookResponse::raw("application/xml", "<xml/>");
    assert_eq!(r.content_type, "application/xml");

    let r = WebhookResponse::status(403, "nope");
    assert_eq!(r.status, 403);
}
