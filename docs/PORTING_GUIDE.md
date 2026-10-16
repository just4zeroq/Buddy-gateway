# Channel porting guide (Rust) — shared interface contract

Goal: port each Python channel in `D:\code\github\octop-gateway\src\octop_gateway\channels\`
to Rust in `D:\code\buddy-gateway\src\channels\` **1:1**, losing no behavior.

## Hard rules

1. **Do NOT modify shared files**: `src/channel.rs`, `src/models.rs`, `src/manager.rs`,
   `src/channels/mod.rs`, `src/constraints.rs`, `src/errors.rs`, `Cargo.toml`.
   If you need a change there, write it as a NOTE in your final report instead.
2. Edit only your own channel file(s) under `src/channels/`. Prefer a single file
   (`src/channels/NAME.rs`); put helper logic in inline `mod` blocks or free fns.
   If Python has submodules (e.g. `weixin/api.py`), inline them.
3. Keep the crate compiling. Rust edition 2021. No `unsafe`.
4. `required = [...]` entries MUST be `String` fields on the config (macro calls `.is_empty()`).

## The `Channel` trait (`crate::channel::Channel`, `#[async_trait]`)

```rust
#[async_trait]
pub trait Channel: Send + Sync {
    // abstract (must implement):
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()>;
    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()>;
    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage>;
    // access:
    fn core(&self) -> &ChannelCore;
    fn core_mut(&mut self) -> &mut ChannelCore;
    // overridable hooks (defaults provided — override to match Python):
    async fn preprocess_inbound(&mut self, _m: &mut InboundMessage) -> Result<()> { Ok(()) }
    async fn send_typing_indicator(&mut self, _s: &ChannelSubject) -> Result<()> { Ok(()) }
    fn default_constraints() -> ChannelConstraints where Self: Sized { Default::default() }
    fn enrich_push_metadata(&self, _s: &ChannelSubject, meta: HashMap<String,Value>)
        -> HashMap<String,Value> { meta }
    fn get_debounce_key(&self, m: &InboundMessage) -> String { /* subject_id */ }
    fn should_batch_inbound(&self, m: &InboundMessage) -> bool { /* !group handles */ }
    fn merge_inbound(&self, ms: &[InboundMessage]) -> Result<InboundMessage> { /* merge */ }
    // provided: handle_inbound / process_inbound / prepare_inbound / finalize_inbound /
    //           send_content / reply_text|content|media / push_text|message /
    //           load_media_bytes / persist_media / fetch_remote_media /
    //           rate_limited_send / clean_output / set_constraints / acquire_rate_slot
}
```

## `ChannelCore` (via `self.core` / `self.core_mut`)

`channel_id() -> &str`, `channel_type() -> &str`, `tenant_id() -> Option<&str>`,
`set_channel_type(&str)`, `set_processor(MessageProcessor)`, `processor() -> Option<MessageProcessor>`,
`set_enqueue_callback(EnqueueFn)`, `enqueue_sender() -> Option<EnqueueFn>`, `enqueue(RawPayload)`,
`ensure_http() -> Result<reqwest::Client>`, `set_connection_session(Option<String>)`,
`connection_session() -> Option<&str>`, `media_backend() -> Option<&DynMediaBackend>`,
`set_media_backend(DynMediaBackend)`, `constraints() -> &ChannelConstraints`,
`constraints_mut() -> &mut ChannelConstraints`, `is_duplicate(&mut self, id) -> bool`,
`track_subject(&mut self, &InboundMessage)`, `get_subject(id) -> Option<ChannelSubject>`,
`list_subjects() -> Vec<ChannelSubject>`, `set_on_new_subject(cb)`,
`group_context_manager()` / `group_context_manager_mut()`.

`EnqueueFn = Arc<dyn Fn(RawPayload) + Send + Sync>`.
Background loops: in `start()`, clone what you need, then
`let cb = self.core.enqueue_sender(); tokio::spawn(async move { /* cb(RawPayload::Json(v)) */ });`
Store `tokio::task::JoinHandle` (or `AbortHandle`) in the macro `fields` so `stop()` can abort it.

## Macro

```rust
crate::channel_adapter! {
    /// Doc comment.
    XChannel, XConfig, "kind",
    required = [field_a, field_b],
    fields = { task: Option<tokio::task::JoinHandle<()>> }   // extra state, Default-constructible
}
```
Generates `pub struct XChannel { core, config, <fields> }` with
`new(processor, config, channel_id, tenant_id, debounce_seconds)`,
`config()`, `core_ref()`, and `impl ChannelConfigLike for XConfig`
(base accessor + required_credentials + missing_credentials).
`required` fields must be `String`; the macro checks `self.<field>.is_empty()`.

Then YOU write `#[async_trait] impl Channel for XChannel { fn core(..); fn core_mut(..); ...platform methods... }`.

## Types

`RawPayload` = `Json(serde_json::Value) | Text(String) | Preparsed(InboundMessage)`.
`InboundMessage { channel_id:String, channel_type:String, tenant_id:Option<String>,
  channel_subject:Option<ChannelSubject>, channel_session_id:String, content:Vec<ContentPart>,
  group_context:Option<GroupContext>, metadata:HashMap<String,Value>, timestamp:f64 }`.
`ChannelSubject { subject_id, first_seen:Option<f64>, last_seen:Option<f64>, display_name, chat_type, metadata }`.
`ContentPart` = `Text(TextContent{text}) | Image(ImageContent{url,alt_text,width,height,size,mime_type,local_path,data})`
`| Video(VideoContent{url,duration,thumbnail_url,size,mime_type,local_path,data})`
`| Audio(AudioContent{url,duration,size,mime_type,local_path,data})`
`| File(FileContent{url,filename,size,mime_type,local_path,data})`.
Build content with `ContentPart::text("..")`. Read with `.text_value()`, `.url()`, `.data()`,
`.local_path()`, `.mime_type()`, `.filename()`, `.is_text()`, `.label_with_filename()`.

Always set `group_context: None` in a hand-built `InboundMessage` unless you fill it.

## Available crates

tokio (full), serde/serde_json, async-trait, reqwest (json, multipart, rustls-tls),
tokio-tungstenite (rustls), rumqttc, uuid(v4), base64(0.22, `use base64::Engine`),
sha1, sha2, hmac, md5, aes, cbc, ecb, ctr, aes-gcm, cipher, hex, urlencoding,
percent-encoding, byteorder, once_cell, rand, bytes, regex, futures, async-stream,
mime_guess, thiserror, tracing.

## Report back

List: config fields (name/type/default), start() mechanism, inbound parse fields,
outbound endpoints/payloads, hooks overridden, submodules inlined, and any NOTE for shared files.
