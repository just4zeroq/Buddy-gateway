//! Google Chat channel — inbound webhook + Chat API outbound.
//!
//! Google Chat delivers bot events by *posting* a single event object to a
//! public HTTPS endpoint registered on the Chat app:
//!
//! ```text
//! { "type": "MESSAGE", "message": {...}, "space": {...}, "user": {...} }
//! ```
//!
//! The [`crate::webhook::WebhookServer`] routes the POST here, so this module
//! ships two halves:
//!
//! * [`GoogleChatWebhook`] — authenticates the caller, translates the event
//!   into the platform-native payload [`GoogleChatChannel::parse_inbound`]
//!   reads, and says what to reply (Chat accepts an empty JSON body as "no
//!   reply").
//! * [`GoogleChatChannel`] — outbound only. Text goes to
//!   `POST /v1/{space}/messages`; media goes through the Chat
//!   `media.upload` multipart endpoint, then an attachment message.
//!
//! Authentication uses a bot OAuth2 bearer token. The webhook path itself needs
//! no credential: Google signs the callback with a JWT that we validate.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex as StdMutex, Once};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::channel::{Channel, ChannelConfig, ChannelCore};
use crate::constraints::ChannelConstraints;
use crate::errors::{GatewayError, Result};
use crate::models::{
    AudioContent, ChannelSubject, ContentPart, FileContent, ImageContent, InboundMessage,
    RawPayload, VideoContent,
};
use crate::webhook::{WebhookAdapter, WebhookOutcome, WebhookRequest, WebhookResponse};

/// Chat API base (messages, spaces).
const CHAT_API: &str = "https://chat.googleapis.com/v1";
/// Chat media upload base (`upload/v1`).
const CHAT_UPLOAD_API: &str = "https://chat.googleapis.com/upload/v1";
/// OAuth2 scope minted for bot calls.
const CHAT_BOT_SCOPE: &str = "https://www.googleapis.com/auth/chat.bot";
/// Fixed issuer of the JWT Google attaches to every callback.
const CHAT_JWT_ISSUER: &str = "chat@system.gserviceaccount.com";
/// Google Chat's message length limit.
const TEXT_CHUNK: usize = 4000;
/// Cap on downloaded media handed to `media.upload`.
const MAX_MEDIA_BYTES: usize = 25 * 1024 * 1024;
/// Dedup window for inbound message ids.
const DEDUP_CAP: usize = 2000;

/// The `iss`/`exp`/`aud` check is done once per process; log the
/// "expected_audience is empty" warning only once so a busy bot stays quiet.
static WARN_NO_AUDIENCE: Once = Once::new();
/// Likewise for the service-account/RS256 refusal.
static WARN_NO_SIGNER: Once = Once::new();

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Google Chat channel configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GoogleChatConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    /// Full service-account JSON (as a string) used to mint the OAuth2 access
    /// token for outbound calls. May be empty, in which case `access_token`
    /// must be supplied.
    pub service_account_json: String,
    /// Static OAuth2 bearer used when `service_account_json` is empty.
    pub access_token: String,
    /// The JWT `aud` registered with Google (the Cloud project number). When
    /// set, the adapter validates it.
    pub expected_audience: String,
    /// Validate the inbound JWT claims. Defaults to `true` when omitted.
    pub verify_jwt: Option<bool>,
    /// Optional HTTP proxy for REST calls.
    pub http_proxy: String,
    /// Optional `user:pass` for the proxy.
    pub http_proxy_auth: String,
    /// The bot's own resource name (e.g. `users/1234567890`); messages from
    /// this user are ignored to prevent reply loops.
    pub bot_user_name: String,
    /// Prefix inbound text with `"{sender_name}: "`. Defaults to false.
    pub include_sender_in_text: Option<bool>,
}

impl GoogleChatConfig {
    /// JWT verification is on unless explicitly disabled.
    fn verify_jwt_enabled(&self) -> bool {
        self.verify_jwt.unwrap_or(true)
    }

    /// Sender prefixing is off unless explicitly enabled.
    fn include_sender(&self) -> bool {
        self.include_sender_in_text.unwrap_or(false)
    }
}

crate::channel_adapter! {
    /// Google Chat channel adapter.
    GoogleChatChannel, GoogleChatConfig, "googlechat",
    required = [],
    fields = {}
}

// ---------------------------------------------------------------------------
// Inbound dedup
// ---------------------------------------------------------------------------

/// Small private LRU of seen message ids.
///
/// `parse_inbound` takes `&self`, so the core's `is_duplicate` (which needs
/// `&mut self`) is unavailable on the adapter and a local set is kept instead.
struct LruSet {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl LruSet {
    fn new(cap: usize) -> Self {
        LruSet {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    /// Returns true when `id` was not present (i.e. it is fresh).
    fn fresh(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Outbound channel
// ---------------------------------------------------------------------------

#[async_trait]
impl Channel for GoogleChatChannel {
    fn core(&self) -> &ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut ChannelCore {
        &mut self.core
    }

    fn default_constraints() -> ChannelConstraints {
        ChannelConstraints {
            // Chat API writes are fine with a modest sustained rate.
            send_rate_limit: Some((20, 60.0)),
            reply_timeout: 0.0,
            typing_keepalive_interval: 0.0,
            ..Default::default()
        }
    }

    async fn start(&mut self) -> Result<()> {
        // No network work is needed here: the webhook endpoint is mounted by the
        // server, and outbound tokens are resolved lazily per send.
        if !self.config.service_account_json.trim().is_empty() {
            tracing::info!(
                "Google Chat: service account JSON configured; access tokens are resolved per call"
            );
        }
        if self.config.service_account_json.trim().is_empty()
            && self.config.access_token.trim().is_empty()
        {
            tracing::warn!(
                "Google Chat: neither service_account_json nor access_token is set — outbound messages will fail"
            );
        }
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let space = subject_space(subject)?;
        let token = self.access_token().await?;
        let http = self.build_http()?;
        let converted = markdown_to_chat_text(text);
        for chunk in chunk_text(&converted, TEXT_CHUNK) {
            let url = format!(
                "{CHAT_API}/{space}/messages?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
            );
            let resp = http
                .post(&url)
                .header("Authorization", format!("Bearer {token}"))
                .json(&json!({ "text": chunk }))
                .send()
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(GatewayError::Other(format!(
                    "googlechat send failed: {status}: {body}"
                )));
            }
        }
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        if media.is_text() {
            return Ok(());
        }
        let label = media.label_with_filename();
        // Uploading is a two-step, multi-request dance; any failure along the
        // way degrades to a text marker so the user still learns a part existed.
        if let Err(e) = self.send_media_inner(subject, media).await {
            tracing::warn!("Google Chat media upload failed: {}", e);
            return self
                .send_text(subject, &format!("[{label}: upload failed]"))
                .await;
        }
        Ok(())
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("googlechat expects a JSON payload".into()))?;
        let space = v.get("space").and_then(|x| x.as_str()).unwrap_or_default();
        if space.is_empty() {
            return Err(GatewayError::Other("googlechat payload without space".into()));
        }

        // Google reports ROOM / DIRECT_MESSAGE / GROUP_CHAT; only the first two
        // map onto our chat-type vocabulary.
        let chat_type = match v
            .get("space_type")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "DIRECT_MESSAGE" | "DM" => "dm",
            _ => "room",
        }
        .to_string();

        let display_name = v
            .get("space_display_name")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| space.to_string());

        let message_id = v
            .get("message_id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let thread = v
            .get("thread")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let sender_id = v
            .get("sender_id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let sender_name = v
            .get("sender_name")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let argument_text = v
            .get("argument_text")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();

        // Optionally attribute the speaker, since a Chat space has many voices.
        let raw_text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
        let text = if self.config.include_sender() && !sender_name.is_empty() && !raw_text.is_empty()
        {
            format!("{sender_name}: {raw_text}")
        } else {
            raw_text.to_string()
        };

        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(ContentPart::text(text));
        }
        if let Some(files) = v.get("files").and_then(|f| f.as_array()) {
            for f in files {
                if let Some(part) = file_to_part(f) {
                    content.push(part);
                }
            }
        }

        let mut subject = ChannelSubject::new(space);
        subject.display_name = display_name;
        subject.chat_type = chat_type;
        subject
            .metadata
            .insert("to_handle".into(), Value::String(space.to_string()));
        subject
            .metadata
            .insert("space".into(), Value::String(space.to_string()));
        if let Some(t) = &thread {
            subject
                .metadata
                .insert("thread".into(), Value::String(t.clone()));
        }

        let mut metadata = HashMap::new();
        metadata.insert("message_id".into(), Value::String(message_id));
        metadata.insert("space".into(), Value::String(space.to_string()));
        metadata.insert("to_handle".into(), Value::String(space.to_string()));
        if let Some(t) = &thread {
            metadata.insert("thread".into(), Value::String(t.clone()));
        }
        metadata.insert("sender_id".into(), Value::String(sender_id));
        metadata.insert("sender_name".into(), Value::String(sender_name));
        metadata.insert("argument_text".into(), Value::String(argument_text));

        // A thread is the natural conversation session; a plain room is not.
        let session = thread.clone().unwrap_or_else(|| space.to_string());
        let timestamp = v
            .get("create_time")
            .and_then(|x| x.as_str())
            .and_then(parse_rfc3339_secs)
            .map(|s| s as f64)
            .unwrap_or(0.0);

        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "googlechat".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(subject),
            channel_session_id: session,
            content,
            group_context: None,
            metadata,
            timestamp,
        })
    }

    fn enrich_push_metadata(
        &self,
        subject: &ChannelSubject,
        mut meta: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        let space = subject
            .metadata
            .get("space")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(subject.subject_id.as_str())
            .to_string();
        meta.insert("to_handle".into(), Value::String(space.clone()));
        meta.insert("space".into(), Value::String(space));
        meta
    }
}

impl GoogleChatChannel {
    /// Resolve the OAuth2 bearer used for outbound calls.
    async fn access_token(&self) -> Result<String> {
        // 1. A static token needs neither network nor signing.
        let static_token = self.config.access_token.trim();
        if !static_token.is_empty() {
            return Ok(static_token.to_string());
        }

        // 2. A service account would require a signed JWT assertion. Parse it so
        //    the failure is specific, then refuse honestly instead of faking it.
        let raw = self.config.service_account_json.trim();
        if raw.is_empty() {
            return Err(GatewayError::Other("googlechat: no credentials".into()));
        }
        let sa: Value = serde_json::from_str(raw).map_err(|e| {
            GatewayError::Other(format!("googlechat: invalid service_account_json: {e}"))
        })?;
        let client_email = sa
            .get("client_email")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let token_uri = sa
            .get("token_uri")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("https://oauth2.googleapis.com/token");
        // The claim set a real signer would encode onto the RS256 assertion
        // (kept explicit so the required shape is documented, not implied).
        let _claims = json!({
            "iss": client_email,
            "scope": CHAT_BOT_SCOPE,
            "aud": token_uri,
            "iat": crate::channel::now_secs(),
            "exp": crate::channel::now_secs() + 3600,
        });

        WARN_NO_SIGNER.call_once(|| {
            tracing::warn!(
                "Google Chat: service_account_json is configured but this build cannot sign RS256 \
                 assertions; set `access_token` to a pre-minted OAuth2 bearer instead"
            );
        });

        // NOTE: A complete implementation would (a) base64url-encode the claim
        // set above, (b) sign it with RS256 using the private key from
        // `private_key` — needing an RSA crate (`ring`/`rsa`/`jsonwebtoken`) that
        // is not in this manifest — (c) exchange the assertion at `token_uri` for
        // an access token, and (d) cache it until `exp` minus skew, optionally
        // refreshing Google's public certs via `x509-parser`/JWKS. None of that
        // is possible in this build, so the supported path today is a pre-minted
        // static `access_token` (mint one out-of-band with
        // `gcloud auth print-access-token --impersonate-service-account` or the
        // equivalent and pass it as `access_token`).
        Err(GatewayError::Other(format!(
            "googlechat: service_account_json (client_email={client_email:?}) requires a \
             pre-minted `access_token` in this build — no RSA signer is linked"
        )))
    }

    /// Step two of media send: upload, then attach. Factored out so
    /// [`Channel::send_media`] can uniformly fall back to a text marker.
    async fn send_media_inner(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let space = subject_space(subject)?;
        let token = self.access_token().await?;
        let http = self.build_http()?;

        let (bytes, mime) = self.load_media_bytes(media).await?;
        if bytes.len() > MAX_MEDIA_BYTES {
            return Err(GatewayError::Other(
                "googlechat media exceeds 25 MiB".to_string(),
            ));
        }
        let filename = media
            .filename()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .unwrap_or_else(|| default_filename(&mime, media.label()));

        // 1. Upload the bytes. Chat's user-auth `media.upload` is a multipart
        //    request: a metadata part describing the attachment plus the bytes.
        let metadata = json!({ "filename": filename });
        let meta_part = reqwest::multipart::Part::text(metadata.to_string())
            .mime_str("application/json")
            .unwrap_or_else(|_| reqwest::multipart::Part::text(metadata.to_string()));
        let file_part = reqwest::multipart::Part::bytes(bytes)
            .file_name(filename.clone())
            .mime_str(&mime)
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(Vec::new()));
        let form = reqwest::multipart::Form::new()
            .part("metadata", meta_part)
            .part("file", file_part);

        let upload_url = format!("{CHAT_UPLOAD_API}/{space}/attachments?uploadType=multipart");
        let resp = http
            .post(&upload_url)
            .header("Authorization", format!("Bearer {token}"))
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!(
                "googlechat upload failed: {status}: {body}"
            )));
        }
        let uploaded: Value = resp.json().await.map_err(|e| {
            GatewayError::Other(format!("googlechat upload: bad response: {e}"))
        })?;
        let resource_name = uploaded
            .get("attachmentDataRef")
            .and_then(|a| a.get("resourceName"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                GatewayError::Other("googlechat upload without attachmentDataRef".into())
            })?
            .to_string();

        // 2. Send a message that references the uploaded attachment.
        let msg_url = format!(
            "{CHAT_API}/{space}/messages?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
        );
        let resp = http
            .post(&msg_url)
            .header("Authorization", format!("Bearer {token}"))
            .json(&json!({
                "attachment": [{ "attachmentDataRef": { "resourceName": resource_name } }]
            }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Other(format!(
                "googlechat attachment message failed: {status}: {body}"
            )));
        }
        Ok(())
    }

    fn build_http(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder();
        let proxy = self.config.http_proxy.trim();
        if !proxy.is_empty() {
            let mut p = reqwest::Proxy::all(proxy)?;
            let auth = self.config.http_proxy_auth.trim();
            if !auth.is_empty() {
                let (user, pass) = auth.split_once(':').unwrap_or((auth, ""));
                p = p.basic_auth(user, pass);
            }
            builder = builder.proxy(p);
        }
        builder
            .build()
            .map_err(|e| GatewayError::Other(format!("googlechat http client: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Webhook adapter
// ---------------------------------------------------------------------------

/// Inbound webhook verifier + translator for Google Chat.
pub struct GoogleChatWebhook {
    config: GoogleChatConfig,
    dedup: StdMutex<LruSet>,
}

impl GoogleChatWebhook {
    pub fn new(config: GoogleChatConfig) -> Self {
        GoogleChatWebhook {
            config,
            dedup: StdMutex::new(LruSet::new(DEDUP_CAP)),
        }
    }
}

impl WebhookAdapter for GoogleChatWebhook {
    fn channel_type(&self) -> &'static str {
        "googlechat"
    }

    fn verify_challenge(&self, _req: &WebhookRequest) -> Option<WebhookResponse> {
        // Google Chat has no GET verification handshake on the callback URL.
        None
    }

    fn handle_request(&self, req: &WebhookRequest) -> Result<WebhookOutcome> {
        // 1. Authenticate the caller.
        if self.config.verify_jwt_enabled() {
            let header = req
                .header("authorization")
                .ok_or_else(|| {
                    GatewayError::Other("googlechat: missing Authorization header".into())
                })?
                .to_string();
            let token = header
                .strip_prefix("Bearer ")
                .or_else(|| header.strip_prefix("bearer "))
                .unwrap_or(&header)
                .trim();
            let claims = decode_jwt_payload(token)?;
            validate_jwt_claims(
                &claims,
                self.config.expected_audience.trim(),
                crate::channel::now_secs() as i64,
            )?;

            // NOTE: The RS256 signature is deliberately NOT verified here. This
            // build has no RSA dependency, so only the claims (iss/exp/aud) are
            // checked — a forged token with a plausible payload would pass.
            // Terminate signature verification at the ingress (Cloud Run, an
            // HTTPS load balancer, or IAP) so an unverified body never reaches
            // this process, and set `expected_audience` to the Cloud project
            // number so at least audience pinning is enforced here.
            if self.config.expected_audience.trim().is_empty() {
                WARN_NO_AUDIENCE.call_once(|| {
                    tracing::warn!(
                        "googlechat: expected_audience is empty — the JWT audience is not pinned \
                         and the RS256 signature is not verified by this adapter; terminate both \
                         at the ingress"
                    );
                });
            }
        }

        // 2. Parse the single event object.
        let event = req.body_json()?;
        let event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");

        match event_type {
            "MESSAGE" => {
                let message = event.get("message").cloned().unwrap_or(Value::Null);

                // Skip anything the bot itself produced (loop prevention). Chat
                // marks bot authors with `sender.type == "BOT"` and/or gives the
                // sender the bot's own resource name.
                let sender = message.get("sender").cloned().unwrap_or(Value::Null);
                let sender_name = sender
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let sender_type = sender
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let bot = self.config.bot_user_name.trim();
                if sender_type.eq_ignore_ascii_case("BOT")
                    || (!bot.is_empty() && sender_name == bot)
                {
                    tracing::debug!(
                        "Google Chat: ignoring bot-authored message from {}",
                        if sender_name.is_empty() { "-" } else { &sender_name }
                    );
                    return Ok(ack_json());
                }

                // Deduplicate: Chat retries a callback until it gets a 2xx.
                let message_name = message
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if message_name.is_empty() {
                    return Err(GatewayError::Other(
                        "googlechat: MESSAGE event without message.name".into(),
                    ));
                }
                {
                    let mut dedup = match self.dedup.lock() {
                        Ok(d) => d,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if !dedup.fresh(&message_name) {
                        tracing::debug!("Google Chat: duplicate message {} ignored", message_name);
                        return Ok(ack_json());
                    }
                }

                let payload = build_native_payload(&event, None).ok_or_else(|| {
                    GatewayError::Other("googlechat: MESSAGE event without space".into())
                })?;
                Ok(WebhookOutcome::with_response(
                    WebhookResponse::json(&json!({})),
                    vec![RawPayload::Json(payload)],
                ))
            }

            // Greet option: enqueue a synthetic text payload so the agent can
            // greet the space it was added to. The empty JSON reply tells Chat
            // we are not answering inline (which would double up the greeting).
            "ADDED_TO_SPACE" => match build_native_payload(&event, Some("(bot added to this space)"))
            {
                Some(payload) => Ok(WebhookOutcome::with_response(
                    WebhookResponse::json(&json!({})),
                    vec![RawPayload::Json(payload)],
                )),
                None => Ok(ack_json()),
            },

            // Nothing actionable — acknowledge so Chat stops retrying.
            "REMOVED_FROM_SPACE" | "CARD_CLICKED" => Ok(ack_json()),

            other => {
                tracing::debug!("Google Chat: ignored event type {:?}", other);
                Ok(ack_json())
            }
        }
    }
}

/// An empty JSON body ack, which Chat renders as "no reply".
fn ack_json() -> WebhookOutcome {
    WebhookOutcome::with_response(WebhookResponse::json(&json!({})), Vec::new())
}

// ---------------------------------------------------------------------------
// JWT verification
// ---------------------------------------------------------------------------

/// Split `header.payload.signature`, Base64URL-decode the payload, and parse it.
///
/// Tolerates padding on the encoded segments.
fn decode_jwt_payload(token: &str) -> Result<Value> {
    let mut parts = token.split('.');
    let header = parts
        .next()
        .ok_or_else(|| GatewayError::Other("googlechat: malformed JWT".into()))?;
    let payload = parts
        .next()
        .ok_or_else(|| GatewayError::Other("googlechat: malformed JWT".into()))?;
    let signature = parts
        .next()
        .ok_or_else(|| GatewayError::Other("googlechat: malformed JWT".into()))?;
    if header.is_empty() || payload.is_empty() || signature.is_empty() {
        return Err(GatewayError::Other("googlechat: malformed JWT".into()));
    }
    let bytes = decode_b64url(payload)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| GatewayError::Other(format!("googlechat: bad JWT payload: {e}")))
}

/// Base64URL decode tolerating padded input.
fn decode_b64url(s: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    let trimmed = s.trim_end_matches('=');
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(trimmed)
        .map_err(|e| GatewayError::Other(format!("googlechat: invalid base64url: {e}")))
}

/// Validate the claims we *can* check without an RSA dependency.
///
/// `now` is passed in so the check is deterministic under test.
fn validate_jwt_claims(payload: &Value, expected_audience: &str, now: i64) -> Result<()> {
    let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or("");
    if iss != CHAT_JWT_ISSUER {
        return Err(GatewayError::Other(format!(
            "googlechat: unexpected JWT issuer {iss:?}"
        )));
    }
    let exp = payload.get("exp").and_then(|v| v.as_i64()).unwrap_or(0);
    if exp <= now {
        return Err(GatewayError::Other(
            "googlechat: JWT is missing or expired".into(),
        ));
    }
    if !expected_audience.is_empty() {
        let aud = payload.get("aud");
        let matches = match aud {
            Some(Value::String(s)) => s == expected_audience,
            Some(Value::Array(items)) => {
                items.iter().any(|i| i.as_str() == Some(expected_audience))
            }
            _ => false,
        };
        if !matches {
            return Err(GatewayError::Other(
                "googlechat: JWT audience mismatch".into(),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Payload helpers
// ---------------------------------------------------------------------------

/// Translate one Google Chat event into the native payload `parse_inbound` reads.
///
/// `text_override` replaces the message text and is used for the synthetic
/// `ADDED_TO_SPACE` greeting event.
fn build_native_payload(event: &Value, text_override: Option<&str>) -> Option<Value> {
    let space = event.get("space")?;
    let space_name = space.get("name").and_then(|v| v.as_str()).unwrap_or("");
    if space_name.is_empty() {
        return None;
    }
    let message = event.get("message").cloned().unwrap_or(Value::Null);
    // `ADDED_TO_SPACE` carries no `message`; fall back to the top-level `user`.
    let sender = message
        .get("sender")
        .or_else(|| event.get("user"))
        .cloned()
        .unwrap_or(Value::Null);

    let text = match text_override {
        Some(t) => t.to_string(),
        None => message
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    };
    let create_time = message
        .get("createTime")
        .and_then(|v| v.as_str())
        .or_else(|| event.get("eventTime").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    Some(json!({
        "_googlechat": true,
        "type": event.get("type").cloned().unwrap_or(Value::String("MESSAGE".into())),
        "message_id": message.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "space": space_name,
        "space_type": space.get("type").and_then(|v| v.as_str()).unwrap_or("ROOM"),
        "space_display_name": space.get("displayName").and_then(|v| v.as_str()).unwrap_or(""),
        "thread": message.get("thread").and_then(|t| t.get("name")).and_then(|v| v.as_str()).unwrap_or(""),
        "text": text,
        "argument_text": message.get("argumentText").and_then(|v| v.as_str()).unwrap_or(""),
        "sender_id": sender.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "sender_name": sender.get("displayName").and_then(|v| v.as_str()).unwrap_or(""),
        "sender_type": sender.get("type").and_then(|v| v.as_str()).unwrap_or("HUMAN"),
        "create_time": create_time,
        "files": normalize_attachments(message.get("attachment")),
    }))
}

/// Keep only image/video/audio attachments, flattened to the shape
/// `parse_inbound` reads.
fn normalize_attachments(attachments: Option<&Value>) -> Value {
    let Some(items) = attachments.and_then(|a| a.as_array()) else {
        return Value::Array(Vec::new());
    };
    let out: Vec<Value> = items
        .iter()
        .filter_map(|a| {
            let mime = a.get("contentType").and_then(|v| v.as_str()).unwrap_or("");
            if !(mime.starts_with("image/")
                || mime.starts_with("video/")
                || mime.starts_with("audio/"))
            {
                return None;
            }
            let url = a.get("downloadUri").and_then(|v| v.as_str())?;
            Some(json!({
                "name": a.get("contentName").and_then(|v| v.as_str()).unwrap_or(""),
                "mime": mime,
                "url": url,
            }))
        })
        .collect();
    Value::Array(out)
}

/// Destination space for outbound calls: metadata `space` wins over `subject_id`.
fn subject_space(subject: &ChannelSubject) -> Result<String> {
    let space = subject
        .metadata
        .get("space")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(subject.subject_id.as_str());
    if space.is_empty() {
        return Err(GatewayError::Other(
            "googlechat: empty destination space".into(),
        ));
    }
    Ok(space.to_string())
}

fn file_to_part(f: &Value) -> Option<ContentPart> {
    let url = f
        .get("url")
        .and_then(|u| u.as_str())
        .filter(|s| !s.is_empty())?;
    let mime = f
        .get("mime")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let name = f
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();

    let kind = if mime.starts_with("image/") {
        "image"
    } else if mime.starts_with("video/") {
        "video"
    } else if mime.starts_with("audio/") {
        "audio"
    } else {
        "file"
    };

    Some(match kind {
        "image" => ContentPart::Image(ImageContent {
            url: url.to_string(),
            alt_text: name.clone(),
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        "video" => ContentPart::Video(VideoContent {
            url: url.to_string(),
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        "audio" => ContentPart::Audio(AudioContent {
            url: url.to_string(),
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
        _ => ContentPart::File(FileContent {
            url: url.to_string(),
            filename: name,
            mime_type: non_empty(&mime),
            ..Default::default()
        }),
    })
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

// ---------------------------------------------------------------------------
// Text conversion
// ---------------------------------------------------------------------------

/// Convert the common Markdown a model emits into the Google Chat subset.
///
/// Chat renders `*bold*`, `_italic_`, `~strikethrough~`, `` `code` `` and
/// ``` ```fences``` ``` — the same shapes as Markdown except links, which use
/// the `<url|label>` form, and double-marker emphasis, which must be collapsed.
fn markdown_to_chat_text(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    // 1. Protect fenced blocks and inline code behind placeholders so their
    //    content is never rewritten.
    let fence_re = regex::Regex::new(r"(?s)```.*?```").expect("static regex");
    let code_re = regex::Regex::new(r"`[^`\n]+`").expect("static regex");

    let mut protected: Vec<String> = Vec::new();
    let mut stage1 = String::new();
    let mut last = 0usize;
    for m in fence_re.find_iter(text) {
        stage1.push_str(&text[last..m.start()]);
        protected.push(m.as_str().to_string());
        stage1.push_str(&format!("\u{0}{}\u{0}", protected.len() - 1));
        last = m.end();
    }
    stage1.push_str(&text[last..]);

    let mut stage2 = String::new();
    let mut last = 0usize;
    for m in code_re.find_iter(&stage1) {
        stage2.push_str(&stage1[last..m.start()]);
        protected.push(m.as_str().to_string());
        stage2.push_str(&format!("\u{0}{}\u{0}", protected.len() - 1));
        last = m.end();
    }
    stage2.push_str(&stage1[last..]);

    // 2. Rewrite the prose into Chat's markup.
    let link = regex::Regex::new(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)").expect("static regex");
    let bold = regex::Regex::new(r"\*\*([^*\n]+)\*\*").expect("static regex");
    let bold_under = regex::Regex::new(r"__([^_\n]+)__").expect("static regex");
    let strike = regex::Regex::new(r"~~([^~\n]+)~~").expect("static regex");

    let s = link.replace_all(&stage2, "<$2|$1>").to_string();
    let s = bold.replace_all(&s, "*$1*").to_string();
    let s = bold_under.replace_all(&s, "*$1*").to_string();
    // Chat's strikethrough marker is a single tilde.
    let s = strike.replace_all(&s, "~$1~").to_string();

    // 3. Restore protected spans.
    restore_placeholders(&s, &protected)
}

/// Swap `\u{0}<index>\u{0}` placeholders back for their originals.
fn restore_placeholders(text: &str, protected: &[String]) -> String {
    let mut out = String::with_capacity(text.len());
    for (i, part) in text.split('\u{0}').enumerate() {
        if i % 2 == 1 {
            if let Ok(idx) = part.parse::<usize>() {
                if let Some(original) = protected.get(idx) {
                    out.push_str(original);
                    continue;
                }
            }
        }
        out.push_str(part);
    }
    out
}

/// Split text on character boundaries, preferring paragraph/line breaks.
fn chunk_text(text: &str, limit: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= limit {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < chars.len() {
        let end = (start + limit).min(chars.len());
        let slice: String = chars[start..end].iter().collect();
        let cut = slice
            .rfind("\n\n")
            .or_else(|| slice.rfind('\n'))
            .or_else(|| slice.rfind(' '))
            .filter(|i| *i > limit / 3)
            .unwrap_or(slice.len());
        let piece: String = slice.chars().take(cut).collect();
        out.push(piece.trim_end().to_string());
        let consumed = slice[..cut].chars().count().max(1);
        start += consumed;
    }
    out.retain(|s| !s.is_empty());
    if out.is_empty() {
        out.push(text.to_string());
    }
    out
}

fn default_filename(mime: &str, label: &str) -> String {
    let ext = mime_guess::get_mime_extensions_str(mime)
        .and_then(|exts| exts.first().copied())
        .unwrap_or("bin");
    format!("{}.{}", label.to_ascii_lowercase(), ext)
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/// Howard Hinnant's `days_from_civil`: days since 1970-01-01 for a proleptic
/// Gregorian date. Pure integer math, no leap-second or timezone tables.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fff]Z` (a numeric `±hh:mm` offset also works)
/// into epoch seconds. Seconds precision: the fraction is truncated.
fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time_part) = s.split_once('T')?;

    // Split the timezone designator off, then the fraction.
    let (time, offset_secs) = split_tz_offset(time_part);
    let time = time.split('.').next().unwrap_or(time);

    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    let mut tp = time.split(':');
    let h: i64 = tp.next()?.parse().ok()?;
    let mi: i64 = tp.next()?.parse().ok()?;
    let sec: i64 = tp.next().and_then(|s| s.parse().ok()).unwrap_or(0);

    Some(days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + sec - offset_secs)
}

/// Split a trailing `Z` / `±hh:mm` off `HH:MM:SS[.fff]`, returning the bare
/// time and the offset in seconds east of UTC.
fn split_tz_offset(time_part: &str) -> (&str, i64) {
    if let Some(stripped) = time_part.strip_suffix(['Z', 'z']) {
        return (stripped, 0);
    }
    if let Some(i) = time_part.find(['+', '-']) {
        let (time, sign_part) = time_part.split_at(i);
        let sign: i64 = if sign_part.starts_with('-') { -1 } else { 1 };
        let body = &sign_part[1..];
        let (hh, mm) = match body.split_once(':') {
            Some((a, b)) => (a, b),
            None => (body, "0"),
        };
        let hh: i64 = hh.parse().unwrap_or(0);
        let mm: i64 = mm.parse().unwrap_or(0);
        return (time, sign * (hh * 3600 + mm * 60));
    }
    (time_part, 0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;

    use futures::Stream;

    use super::*;
    use crate::channel::MessageProcessor;
    use crate::models::MessageEvent;

    // --- RFC3339 -> epoch -------------------------------------------------

    #[test]
    fn rfc3339_to_epoch_vectors() {
        assert_eq!(parse_rfc3339_secs("1970-01-01T00:00:00Z"), Some(0));
        // The Unix billennium.
        assert_eq!(parse_rfc3339_secs("2001-09-09T01:46:40Z"), Some(1_000_000_000));
        assert_eq!(parse_rfc3339_secs("2000-01-01T00:00:00Z"), Some(946_684_800));
        // Leap day.
        assert_eq!(parse_rfc3339_secs("2024-02-29T00:00:00Z"), Some(1_709_164_800));
        // Non-leap February: the 28th -> 1st gap is one day.
        assert_eq!(parse_rfc3339_secs("2023-02-28T00:00:00Z"), Some(1_677_542_400));
        assert_eq!(parse_rfc3339_secs("2023-03-01T00:00:00Z"), Some(1_677_628_800));
        // Fractional seconds are truncated (seconds precision).
        assert_eq!(
            parse_rfc3339_secs("2026-10-16T06:00:00.000000Z"),
            Some(1_792_130_400)
        );
        assert_eq!(
            parse_rfc3339_secs("2026-10-16T06:00:00.999Z"),
            Some(1_792_130_400)
        );
        // A numeric offset shifts the instant.
        assert_eq!(parse_rfc3339_secs("2024-02-29T02:00:00+02:00"), Some(1_709_164_800));
        assert_eq!(parse_rfc3339_secs("2024-02-29T00:00:00-02:00"), Some(1_709_172_000));
        // Garbage is rejected rather than defaulted.
        assert_eq!(parse_rfc3339_secs("not-a-timestamp"), None);
    }

    // --- JWT --------------------------------------------------------------

    fn b64url(bytes: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// A fake, unsigned JWT: the signature segment is never checked.
    fn fake_jwt(payload: &Value) -> String {
        let header = b64url(br#"{"alg":"RS256","typ":"JWT"}"#);
        let body = b64url(payload.to_string().as_bytes());
        let sig = b64url(b"not-a-real-signature");
        format!("{header}.{body}.{sig}")
    }

    #[test]
    fn jwt_payload_decode_and_claim_validation() {
        let payload = json!({
            "iss": "chat@system.gserviceaccount.com",
            "aud": "1234567890",
            "exp": 2_000_000_000i64,
            "iat": 1_999_999_000i64,
        });
        let token = fake_jwt(&payload);

        // Payload round-trips through Base64URL.
        let decoded = decode_jwt_payload(&token).expect("payload decodes");
        assert_eq!(decoded.get("iss").and_then(|v| v.as_str()), Some("chat@system.gserviceaccount.com"));

        // Padded input is tolerated.
        let padded_payload = format!("{}==", token.split('.').nth(1).unwrap_or(""));
        assert!(decode_b64url(&padded_payload).is_ok());

        // Valid claims.
        assert!(validate_jwt_claims(&decoded, "1234567890", 1_999_999_500).is_ok());
        // Audience pinning is skipped when unset.
        assert!(validate_jwt_claims(&decoded, "", 1_999_999_500).is_ok());
        // Wrong audience.
        assert!(validate_jwt_claims(&decoded, "999", 1_999_999_500).is_err());
        // Array `aud` accepted.
        let multi = json!({
            "iss": "chat@system.gserviceaccount.com",
            "aud": ["other", "1234567890"],
            "exp": 2_000_000_000i64,
        });
        assert!(validate_jwt_claims(&multi, "1234567890", 0).is_ok());
        // Expired.
        assert!(validate_jwt_claims(&decoded, "1234567890", 2_000_000_001).is_err());
        // Wrong issuer.
        let bad_iss = json!({
            "iss": "evil@example.com",
            "exp": 2_000_000_000i64,
        });
        assert!(validate_jwt_claims(&bad_iss, "", 0).is_err());
        // Missing `exp` is treated as expired.
        assert!(validate_jwt_claims(&json!({ "iss": "chat@system.gserviceaccount.com" }), "", 0).is_err());

        // A malformed token is rejected.
        assert!(decode_jwt_payload("only.two").is_err());
    }

    // --- Markdown ---------------------------------------------------------

    #[test]
    fn markdown_conversions() {
        assert_eq!(markdown_to_chat_text(""), "");
        assert_eq!(markdown_to_chat_text("**bold**"), "*bold*");
        assert_eq!(markdown_to_chat_text("__bold__"), "*bold*");
        assert_eq!(markdown_to_chat_text("~~gone~~"), "~gone~");
        assert_eq!(
            markdown_to_chat_text("see [the docs](https://example.com/a)"),
            "see <https://example.com/a|the docs>"
        );
        // Code spans and fences are left untouched.
        assert_eq!(markdown_to_chat_text("`**keep**`"), "`**keep**`");
        assert_eq!(
            markdown_to_chat_text("```\n**keep**\n```"),
            "```\n**keep**\n```"
        );
        assert_eq!(markdown_to_chat_text("plain text"), "plain text");
    }

    #[test]
    fn text_chunking_respects_limit() {
        let short = "hello";
        assert_eq!(chunk_text(short, 4000), vec!["hello".to_string()]);
        let long: String = "a".repeat(9000);
        let chunks = chunk_text(&long, 4000);
        assert!(chunks.len() >= 3);
        assert!(chunks.iter().all(|c| c.chars().count() <= 4000));
    }

    // --- Webhook adapter --------------------------------------------------

    fn request_with(headers: &[(&str, &str)], body: &Value) -> WebhookRequest {
        let mut h = HashMap::new();
        for (k, v) in headers {
            h.insert((*k).to_string(), (*v).to_string());
        }
        WebhookRequest {
            method: "POST".to_string(),
            path: "/webhooks/googlechat".to_string(),
            query: HashMap::new(),
            headers: h,
            body: body.to_string().into_bytes(),
        }
    }

    fn adapter(verify_jwt: bool) -> GoogleChatWebhook {
        GoogleChatWebhook::new(GoogleChatConfig {
            verify_jwt: Some(verify_jwt),
            ..Default::default()
        })
    }

    fn message_event(name: &str, text: &str, space_type: &str) -> Value {
        json!({
            "type": "MESSAGE",
            "message": {
                "name": name,
                "sender": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
                "text": text,
                "argumentText": text,
                "thread": { "name": "spaces/X/threads/Z" },
                "createTime": "2026-10-16T06:00:00.000000Z",
                "attachment": [
                    { "contentType": "image/png", "contentName": "a.png", "downloadUri": "https://example.com/a.png" },
                    { "contentType": "application/pdf", "contentName": "b.pdf", "downloadUri": "https://example.com/b.pdf" }
                ]
            },
            "space": { "name": "spaces/X", "type": space_type, "displayName": "Team Room" },
            "user": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "eventTime": "2026-10-16T06:00:00.000000Z"
        })
    }

    fn test_channel(config: GoogleChatConfig) -> GoogleChatChannel {
        let processor: MessageProcessor = Arc::new(|_m: InboundMessage| {
            Box::pin(futures::stream::empty())
                as Pin<Box<dyn Stream<Item = MessageEvent> + Send>>
        });
        GoogleChatChannel::new(processor, config, Some("gc1".to_string()), None, 0.0)
    }

    #[test]
    fn message_event_translates_and_parses() {
        let webhook = adapter(false);
        let req = request_with(&[], &message_event("spaces/X/messages/Y", "hi there", "ROOM"));
        let out = webhook.handle_request(&req).expect("accepted");
        assert_eq!(out.payloads.len(), 1);

        let channel = test_channel(GoogleChatConfig::default());
        let msg = channel.parse_inbound(&out.payloads[0]).expect("parsed");

        assert_eq!(msg.channel_type, "googlechat");
        let subject = msg.channel_subject.as_ref().expect("subject");
        assert_eq!(subject.subject_id, "spaces/X");
        assert_eq!(subject.display_name, "Team Room");
        assert_eq!(subject.chat_type, "room");
        // A thread is the session.
        assert_eq!(msg.channel_session_id, "spaces/X/threads/Z");
        assert_eq!(msg.timestamp, 1_792_130_400.0);

        assert_eq!(
            msg.metadata.get("message_id").and_then(|v| v.as_str()),
            Some("spaces/X/messages/Y")
        );
        assert_eq!(
            msg.metadata.get("space").and_then(|v| v.as_str()),
            Some("spaces/X")
        );
        assert_eq!(
            msg.metadata.get("to_handle").and_then(|v| v.as_str()),
            Some("spaces/X")
        );
        assert_eq!(
            msg.metadata.get("sender_id").and_then(|v| v.as_str()),
            Some("users/1")
        );
        assert_eq!(
            msg.metadata.get("sender_name").and_then(|v| v.as_str()),
            Some("Alice")
        );
        assert_eq!(
            msg.metadata.get("thread").and_then(|v| v.as_str()),
            Some("spaces/X/threads/Z")
        );

        // Text plus the single supported (image) attachment; the PDF is dropped.
        assert_eq!(msg.text(), "hi there");
        assert_eq!(msg.content.len(), 2);
        assert_eq!(
            msg.content[1].url(),
            Some("https://example.com/a.png")
        );
        assert!(msg.group_context.is_none());
    }

    #[test]
    fn direct_message_maps_to_dm() {
        let webhook = adapter(false);
        let req = request_with(
            &[],
            &message_event("spaces/D/messages/1", "yo", "DIRECT_MESSAGE"),
        );
        let out = webhook.handle_request(&req).expect("accepted");
        let channel = test_channel(GoogleChatConfig::default());
        let msg = channel.parse_inbound(&out.payloads[0]).expect("parsed");
        assert_eq!(msg.channel_subject.as_ref().unwrap().chat_type, "dm");
    }

    #[test]
    fn include_sender_prefixes_text() {
        let webhook = adapter(false);
        let req = request_with(&[], &message_event("spaces/X/messages/Y", "hi", "ROOM"));
        let out = webhook.handle_request(&req).expect("accepted");

        let channel = test_channel(GoogleChatConfig {
            include_sender_in_text: Some(true),
            ..Default::default()
        });
        let msg = channel.parse_inbound(&out.payloads[0]).expect("parsed");
        assert_eq!(msg.text(), "Alice: hi");
    }

    #[test]
    fn bot_self_messages_are_ignored() {
        // Sender flagged as a bot.
        let mut ev = message_event("spaces/X/messages/1", "loop", "ROOM");
        ev["message"]["sender"]["type"] = json!("BOT");
        let out = adapter(false)
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert!(out.payloads.is_empty());

        // Sender matching the configured bot resource name.
        let webhook = GoogleChatWebhook::new(GoogleChatConfig {
            verify_jwt: Some(false),
            bot_user_name: "users/1234567890".to_string(),
            ..Default::default()
        });
        let mut ev = message_event("spaces/X/messages/2", "loop", "ROOM");
        ev["message"]["sender"]["name"] = json!("users/1234567890");
        ev["message"]["sender"]["type"] = json!("HUMAN");
        let out = webhook
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert!(out.payloads.is_empty());
    }

    #[test]
    fn duplicate_messages_are_dropped() {
        let webhook = adapter(false);
        let ev = message_event("spaces/X/messages/same", "hi", "ROOM");
        let first = webhook
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert_eq!(first.payloads.len(), 1);
        let second = webhook
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert!(second.payloads.is_empty());
    }

    #[test]
    fn missing_authorization_is_rejected_when_verifying() {
        let webhook = adapter(true);
        let ev = message_event("spaces/X/messages/1", "hi", "ROOM");
        // No header at all.
        assert!(webhook.handle_request(&request_with(&[], &ev)).is_err());

        // A properly formed but claim-invalid token is rejected too.
        let bad = fake_jwt(&json!({ "iss": "evil@example.com", "exp": 2_000_000_000i64 }));
        let req = request_with(&[("authorization", &format!("Bearer {bad}"))], &ev);
        assert!(webhook.handle_request(&req).is_err());

        // A valid token is accepted.
        let good = fake_jwt(&json!({
            "iss": "chat@system.gserviceaccount.com",
            "exp": 2_000_000_000i64,
        }));
        let req = request_with(&[("authorization", &format!("Bearer {good}"))], &ev);
        assert!(webhook.handle_request(&req).is_ok());
    }

    #[test]
    fn added_to_space_enqueues_greeting() {
        let ev = json!({
            "type": "ADDED_TO_SPACE",
            "space": { "name": "spaces/X", "type": "ROOM", "displayName": "Team Room" },
            "user": { "name": "users/1", "displayName": "Alice", "type": "HUMAN" },
            "eventTime": "2026-10-16T06:00:00.000000Z"
        });
        let out = adapter(false)
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert_eq!(out.payloads.len(), 1);

        let channel = test_channel(GoogleChatConfig::default());
        let msg = channel.parse_inbound(&out.payloads[0]).expect("parsed");
        assert_eq!(msg.channel_subject.as_ref().unwrap().subject_id, "spaces/X");
        assert_eq!(msg.channel_session_id, "spaces/X");
        assert!(msg.text().contains("added to this space"));
    }

    #[test]
    fn removed_from_space_is_ack_only() {
        let ev = json!({
            "type": "REMOVED_FROM_SPACE",
            "space": { "name": "spaces/X", "type": "ROOM" },
            "user": { "name": "users/1" }
        });
        let out = adapter(false)
            .handle_request(&request_with(&[], &ev))
            .expect("accepted");
        assert!(out.payloads.is_empty());
    }

    // --- Credentials ------------------------------------------------------

    #[tokio::test]
    async fn access_token_prefers_static_bearer() {
        let channel = test_channel(GoogleChatConfig {
            access_token: "ya29.static".to_string(),
            ..Default::default()
        });
        assert_eq!(channel.access_token().await.unwrap(), "ya29.static");
    }

    #[tokio::test]
    async fn access_token_refuses_service_account_honestly() {
        let channel = test_channel(GoogleChatConfig {
            service_account_json: json!({
                "client_email": "bot@proj.iam.gserviceaccount.com",
                "token_uri": "https://oauth2.googleapis.com/token"
            })
            .to_string(),
            ..Default::default()
        });
        let err = channel.access_token().await.expect_err("must refuse");
        let text = err.to_string();
        assert!(text.contains("access_token"), "unexpected error: {text}");
        assert!(text.contains("RSA"), "unexpected error: {text}");
    }

    #[tokio::test]
    async fn access_token_without_credentials_errors() {
        let channel = test_channel(GoogleChatConfig::default());
        assert!(channel.access_token().await.is_err());
    }
}
