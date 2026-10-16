//! Inbound HTTP webhook service.
//!
//! Some platforms deliver bot events over a long-lived connection the adapter
//! owns directly (WebSocket, long polling, MQTT). A second family instead
//! *posts* every event to a public HTTPS endpoint registered with the provider
//! (WhatsApp Cloud API, LINE Messaging API, WeChat official accounts, Google
//! Chat, ...).
//!
//! [`WebhookServer`] is that missing half. It is a small HTTP server which
//!
//! 1. routes requests by path to the matching platform,
//! 2. delegates signature verification and payload translation to a
//!    [`WebhookAdapter`],
//! 3. hands the resulting [`RawPayload`]s to the channel's enqueue callback, so
//!    the standard pipeline (group context, media persistence, debounce,
//!    session locks, worker pool) runs completely unchanged.
//!
//! ```no_run
//! # use buddy_gateway::manager::ChannelManager;
//! # use buddy_gateway::webhook::WebhookServer;
//! # async fn demo(manager: &ChannelManager) -> buddy_gateway::Result<()> {
//! let mut server = WebhookServer::new();
//! server
//!     .mount_kind(
//!         manager,
//!         "line",
//!         &serde_json::json!({ "channel_access_token": "t", "channel_secret": "s" }),
//!         "line-bot",
//!         None,
//!     )
//!     .await?;
//! server.serve("0.0.0.0:8080").await
//! # }
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::Router;
use serde_json::Value;

use crate::channel::EnqueueFn;
use crate::errors::{GatewayError, Result};
use crate::manager::ChannelManager;
use crate::models::RawPayload;

/// Upper bound for an accepted request body.
///
/// Platform event callbacks are small — media is always referenced by URL or
/// id, never inlined — so this only guards against abusive requests.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Normalized request / response
// ---------------------------------------------------------------------------

/// A platform-agnostic view of one inbound HTTP request.
#[derive(Debug, Clone)]
pub struct WebhookRequest {
    /// Uppercase HTTP method (`GET`, `POST`, ...).
    pub method: String,
    /// Request path, always starting with `/`.
    pub path: String,
    /// Decoded query parameters (last value wins).
    pub query: HashMap<String, String>,
    /// Response headers with lower-cased names.
    pub headers: HashMap<String, String>,
    /// Raw request body.
    pub body: Vec<u8>,
}

impl WebhookRequest {
    /// Look up a header by case-insensitive name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(|s| s.as_str())
    }

    /// Look up a query parameter.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query.get(name).map(|s| s.as_str())
    }

    /// The body decoded as UTF-8 (lossy).
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// Parse the body as JSON.
    pub fn body_json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body)
            .map_err(|e| GatewayError::Other(format!("invalid JSON body: {e}")))
    }

    /// True when the request is a `GET` (provider verification handshake).
    pub fn is_get(&self) -> bool {
        self.method.eq_ignore_ascii_case("GET")
    }
}

/// A platform-agnostic HTTP response.
#[derive(Debug, Clone)]
pub struct WebhookResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

impl WebhookResponse {
    /// `200 text/plain` with a plain body (the default acknowledgement).
    pub fn text<S: Into<String>>(body: S) -> Self {
        WebhookResponse {
            status: 200,
            content_type: "text/plain; charset=utf-8".to_string(),
            body: body.into(),
        }
    }

    /// `200 application/json`.
    pub fn json(value: &Value) -> Self {
        WebhookResponse {
            status: 200,
            content_type: "application/json".to_string(),
            body: value.to_string(),
        }
    }

    /// A `200` carrying raw markup (WeChat passive replies are XML).
    pub fn raw<S: Into<String>>(content_type: &str, body: S) -> Self {
        WebhookResponse {
            status: 200,
            content_type: content_type.to_string(),
            body: body.into(),
        }
    }

    /// Any status with a plain-text body.
    pub fn status<S: Into<String>>(status: u16, body: S) -> Self {
        WebhookResponse {
            status,
            content_type: "text/plain; charset=utf-8".to_string(),
            body: body.into(),
        }
    }
}

impl IntoResponse for WebhookResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::OK);
        let ct = HeaderValue::from_str(&self.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("text/plain; charset=utf-8"));
        (status, [(header::CONTENT_TYPE, ct)], self.body).into_response()
    }
}

/// What an adapter produces for one request: the HTTP reply plus the inbound
/// payloads to feed into the channel queue.
#[derive(Debug, Clone)]
pub struct WebhookOutcome {
    pub response: WebhookResponse,
    pub payloads: Vec<RawPayload>,
}

impl WebhookOutcome {
    /// Acknowledge with no payloads (provider pings, unsupported event types).
    pub fn ack() -> Self {
        WebhookOutcome {
            response: WebhookResponse::text("ok"),
            payloads: Vec::new(),
        }
    }

    /// Acknowledge while enqueueing inbound payloads.
    pub fn with_payloads(payloads: Vec<RawPayload>) -> Self {
        WebhookOutcome {
            response: WebhookResponse::text("ok"),
            payloads,
        }
    }

    /// Acknowledge with an explicit response body.
    pub fn with_response(response: WebhookResponse, payloads: Vec<RawPayload>) -> Self {
        WebhookOutcome { response, payloads }
    }
}

// ---------------------------------------------------------------------------
// Adapter contract
// ---------------------------------------------------------------------------

/// Platform-specific verification and translation for webhook-delivered events.
///
/// Implementations are stateless with respect to the pipeline: they only verify
/// the caller, translate the provider's JSON/XML into the platform-native
/// payload shape the matching [`crate::channel::Channel::parse_inbound`]
/// expects, and say what to reply.
///
/// Heavy work (fetching media by id, decrypting attachments) is intentionally
/// deferred to `Channel::preprocess_inbound`, which is async and has the
/// channel's HTTP client and media backend.
pub trait WebhookAdapter: Send + Sync {
    /// Canonical channel kind this adapter serves.
    fn channel_type(&self) -> &'static str;

    /// Answer the provider's `GET` verification handshake.
    ///
    /// Returns `None` when the path has no GET handshake, in which case the
    /// server replies `405`.
    fn verify_challenge(&self, _req: &WebhookRequest) -> Option<WebhookResponse> {
        None
    }

    /// Verify the request and translate it into zero or more raw payloads.
    ///
    /// Returning `Err` makes the server reply `400`, which is what providers
    /// expect for a failed signature or a malformed body.
    fn handle_request(&self, req: &WebhookRequest) -> Result<WebhookOutcome>;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// One mounted endpoint: which adapter serves it and where payloads go.
#[derive(Clone)]
pub struct WebhookRoute {
    pub path: String,
    pub channel_id: String,
    pub channel_type: String,
    pub adapter: Arc<dyn WebhookAdapter>,
    /// The registered channel's enqueue callback.
    pub enqueue: EnqueueFn,
}

impl std::fmt::Debug for WebhookRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookRoute")
            .field("path", &self.path)
            .field("channel_id", &self.channel_id)
            .field("channel_type", &self.channel_type)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// An HTTP server that fans provider callbacks into registered channels.
///
/// The server holds no reference to the manager: mounting captures the target
/// channel's `EnqueueFn` once, so request handling never needs a lock and stays
/// safe under concurrency.
#[derive(Default)]
pub struct WebhookServer {
    routes: HashMap<String, WebhookRoute>,
}

impl WebhookServer {
    pub fn new() -> Self {
        WebhookServer {
            routes: HashMap::new(),
        }
    }

    /// Mount a pre-built adapter at `path`.
    pub fn mount(&mut self, path: &str, route: WebhookRoute) -> Result<()> {
        let path = normalize_path(path)?;
        if self.routes.contains_key(&path) {
            return Err(GatewayError::Other(format!(
                "webhook path already mounted: {path}"
            )));
        }
        let mut route = route;
        route.path = path.clone();
        self.routes.insert(path, route);
        Ok(())
    }

    /// Mount `channel_id` (already registered with `manager`) at `path`.
    pub async fn mount_channel(
        &mut self,
        manager: &ChannelManager,
        path: &str,
        adapter: Arc<dyn WebhookAdapter>,
        channel_id: &str,
    ) -> Result<()> {
        let enqueue = manager.enqueue_handle(channel_id).await.ok_or_else(|| {
            GatewayError::ChannelNotFound(format!(
                "{channel_id} (channel must be registered and running before mounting its webhook)"
            ))
        })?;
        let channel_type = adapter.channel_type().to_string();
        self.mount(
            path,
            WebhookRoute {
                path: String::new(),
                channel_id: channel_id.to_string(),
                channel_type,
                adapter,
                enqueue,
            },
        )
    }

    /// Build the adapter for `channel_type` from its JSON config, mount it, and
    /// return the path it was mounted at.
    ///
    /// `path_override` wins over the platform default
    /// ([`crate::channels::webhook_path_for`]).
    pub async fn mount_kind(
        &mut self,
        manager: &ChannelManager,
        channel_type: &str,
        config: &Value,
        channel_id: &str,
        path_override: Option<&str>,
    ) -> Result<String> {
        let adapter = crate::channels::build_webhook_adapter(channel_type, config)?;
        let default_path = crate::channels::webhook_path_for(channel_type)?;
        let path = path_override.unwrap_or(default_path);
        self.mount_channel(manager, path, adapter, channel_id).await?;
        Ok(normalize_path(path)?)
    }

    /// The mounted paths.
    pub fn paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.routes.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn get(&self, path: &str) -> Option<&WebhookRoute> {
        self.routes.get(path)
    }

    /// Build the axum router (useful to nest this under an existing app).
    pub fn into_router(self) -> Router {
        let state = Arc::new(self.routes);
        Router::new().fallback(dispatch).with_state(state)
    }

    /// Serve until the process is terminated.
    pub async fn serve(self, addr: &str) -> Result<()> {
        self.serve_with_shutdown(addr, std::future::pending::<()>()).await
    }

    /// Serve until `shutdown` resolves.
    pub async fn serve_with_shutdown<F>(self, addr: &str, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        for path in self.paths() {
            if let Some(r) = self.get(&path) {
                tracing::info!(
                    "Webhook mounted: {} -> {} ({})",
                    path,
                    r.channel_id,
                    r.channel_type
                );
            }
        }
        let app = self.into_router();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| GatewayError::Other(format!("webhook bind {addr} failed: {e}")))?;
        tracing::info!("Webhook server listening on {}", addr);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| GatewayError::Other(format!("webhook server error: {e}")))
    }
}

/// axum handler: route by path, verify, translate, enqueue.
async fn dispatch(
    State(routes): State<Arc<HashMap<String, WebhookRoute>>>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();

    let route = match routes.get(&path) {
        Some(r) => r.clone(),
        None => {
            return WebhookResponse::status(404, "no webhook mounted at this path")
                .into_response();
        }
    };

    let bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return WebhookResponse::status(413, "request body too large").into_response();
        }
    };

    let request = WebhookRequest {
        method: parts.method.as_str().to_string(),
        path: path.clone(),
        query: parse_query(parts.uri.query()),
        headers: lower_headers(&parts.headers),
        body: bytes.to_vec(),
    };

    // Provider verification handshake.
    if request.is_get() {
        return match route.adapter.verify_challenge(&request) {
            Some(resp) => resp.into_response(),
            None => WebhookResponse::status(405, "GET not supported on this endpoint")
                .into_response(),
        };
    }

    if !request.method.eq_ignore_ascii_case("POST") {
        return WebhookResponse::status(405, "method not allowed").into_response();
    }

    match route.adapter.handle_request(&request) {
        Ok(outcome) => {
            for payload in outcome.payloads {
                (route.enqueue)(payload);
            }
            outcome.response.into_response()
        }
        Err(e) => {
            tracing::warn!(
                "Webhook rejected: path={} channel={} error={}",
                path,
                route.channel_id,
                e
            );
            WebhookResponse::status(400, format!("rejected: {e}")).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Normalize a mount path: leading slash required, no trailing slash, no query.
fn normalize_path(path: &str) -> Result<String> {
    let path = path.trim();
    let path = path.split('?').next().unwrap_or(path);
    if path.is_empty() {
        return Err(GatewayError::Other("webhook path must not be empty".into()));
    }
    let mut p = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    while p.len() > 1 && p.ends_with('/') {
        p.pop();
    }
    Ok(p)
}

fn parse_query(q: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(q) = q else { return out };
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = decode_component(k);
        let val = decode_component(&v.replace('+', " "));
        out.insert(key, val);
    }
    out
}

fn decode_component(s: &str) -> String {
    urlencoding::decode(s)
        .map(|c| c.into_owned())
        .unwrap_or_else(|_| s.to_string())
}

fn lower_headers(h: &axum::http::HeaderMap) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (name, value) in h.iter() {
        if let Ok(v) = value.to_str() {
            out.insert(name.as_str().to_ascii_lowercase(), v.to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Signature primitives (constant-time)
// ---------------------------------------------------------------------------

/// Constant-time byte comparison (avoids leaking a shared secret via timing).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// `HMAC-SHA256(secret, message)` rendered as lowercase hex.
pub fn hmac_sha256_hex(secret: &[u8], message: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

/// `HMAC-SHA1(secret, message)` rendered as lowercase hex.
pub fn hmac_sha1_hex(secret: &[u8], message: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha1::Sha1;
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(message);
    hex::encode(mac.finalize().into_bytes())
}

/// `HMAC-SHA256(secret, message)` rendered as standard base64 (LINE's scheme).
pub fn hmac_sha256_base64(secret: &[u8], message: &[u8]) -> String {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(message);
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Verify a `sha256=<hex>` style header (Meta's `X-Hub-Signature-256`).
pub fn verify_sha256_hex_signature(secret: &str, body: &[u8], header: &str) -> bool {
    let Some(expected) = header.strip_prefix("sha256=") else {
        return false;
    };
    let actual = hmac_sha256_hex(secret.as_bytes(), body);
    constant_time_eq(actual.as_bytes(), expected.trim().as_bytes())
}

/// Verify a base64 HMAC-SHA256 signature (LINE's `X-Line-Signature`).
pub fn verify_sha256_base64_signature(secret: &str, body: &[u8], header: &str) -> bool {
    let actual = hmac_sha256_base64(secret.as_bytes(), body);
    constant_time_eq(actual.as_bytes(), header.trim().as_bytes())
}

/// WeChat's signature scheme: `SHA1` over the lexicographically sorted
/// `[token, timestamp, nonce]` (plus `encrypt` in safe mode).
pub fn wechat_signature(token: &str, timestamp: &str, nonce: &str, encrypt: Option<&str>) -> String {
    use sha1::{Digest, Sha1};
    let mut items = vec![token, timestamp, nonce];
    if let Some(e) = encrypt {
        items.push(e);
    }
    items.sort();
    let joined = items.concat();
    let mut hasher = Sha1::new();
    hasher.update(joined.as_bytes());
    hex::encode(hasher.finalize())
}
