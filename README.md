<div align="center">
  <h1>Buddy Gateway</h1>
  <p><strong>Multi-platform IM channel bridge for AI agents and bots — one abstraction layer, fifteen IM platforms.</strong></p>
</div>

<p align="center">
  <a href="https://www.rust-lang.org/"><img alt="Rust 2021" src="https://img.shields.io/badge/rust-2021%20edition-orange?logo=rust&logoColor=white" /></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green" /></a>
  <a href="https://tokio.rs/"><img alt="Async: Tokio" src="https://img.shields.io/badge/async-tokio-blue" /></a>
</p>

<p align="center">
  <b>English</b> · <a href="README_CN.md">中文</a>
</p>

---

**buddy-gateway** is a multi-platform IM channel bridge with a unified message abstraction for AI agents and bots. It connects to mainstream IM platforms and normalizes every inbound message into one processing pipeline, so your agent logic is written **once** and runs across Feishu, DingTalk, QQ, WeCom, WeChat iLink, Yuanbao, Xiaoyi, MQTT, Telegram, Discord, Slack, WhatsApp, LINE, WeChat Official Accounts, and Google Chat.

> Every channel ships with its real transport, inbound media parsing, outbound media upload, and platform-specific hooks behind one unified interface — see [`docs/CHANNELS.md`](docs/CHANNELS.md) for the per-channel coverage matrix.

## What is Buddy Gateway?

| | Feature | Description |
|---|---------|-------------|
| 🔌 | **15 platforms, one processor** | Feishu, DingTalk, QQ, WeCom, WeChat iLink, Yuanbao, Xiaoyi, MQTT, Telegram, Discord, Slack, WhatsApp, LINE, WeChat Official Accounts, Google Chat — all behind one interface |
| 🧩 | **Unified abstraction** | `Channel` + `ChannelCore` turn each platform's quirks into a common `InboundMessage` / `MessageEvent` model |
| 📨 | **Streaming events** | Your processor returns a `Stream<Item = MessageEvent>` — first-class token streaming |
| 💾 | **Pluggable media** | `MediaBackend` stores attachments; `FileSystemMediaBackend` ships by default |
| 🚦 | **Constraints** | Per-channel rate limit, response timeout, placeholder, and typing indicator |
| 📡 | **Push routing** | `push_text` / `push_content` / `push_to_all` for proactive messages |
| 🏢 | **Multi-tenant** | Run multiple channels of the same kind, each isolated by `channel_id` |
| 🦀 | **Idiomatic Rust** | Pure `tokio` + `async-trait`, strongly-typed models, no hidden magic |

## Why Buddy Gateway?

The gateway sits between your agent and the outside world. Each platform is implemented as a `Channel` that knows how to connect, parse inbound traffic, and send replies. A `ChannelManager` orchestrates them through async queues and hands your agent a normalized stream of `MessageEvent`s. You never write platform-specific code in your bot — just one processor.

> Because the abstraction lives in the gateway, swapping IM platforms is a configuration change, not a rewrite.

### Key abstractions

| Abstraction | Role |
|---|---|
| `Channel` trait + `ChannelCore` | Platform contract (start / stop / parse_inbound / send_*) split from shared state |
| `default_constraints()` + `configure_constraints(&self, …)` | Per-channel defaults, plus config-dependent adjustments (e.g. typing interval) |
| `process_inbound` + `default_process_inbound_body` | Overridable inbound pipeline that can still delegate the standard loop |
| `MessageProcessor` | `Arc<dyn Fn(InboundMessage) -> Pin<Box<dyn Stream<Item = MessageEvent> + Send>> + Send + Sync>` |
| `add_channel_full(..., ChannelOptions { … })` | Full channel construction with processor / ids / debounce / constraint overrides |
| `channels::normalize_config` | Config normalization: field aliases, trimming, and per-channel coercions |
| `tokio` | Async runtime driving every channel and worker pool |

### Core Technology

| Layer | Technology |
|-------|-----------|
| **Language** | Rust 2021 (MSRV: stable, built with 1.95) |
| **Async runtime** | `tokio` |
| **Channel model** | `Channel` trait + per-platform adapter (15 built-in) + `WebhookAdapter` for callback platforms |
| **Messaging model** | `InboundMessage` / `MessageEvent` / `ContentPart` |
| **Orchestration** | `ChannelManager` (async queues, worker pools, session locks) |
| **Media** | `MediaBackend` (`FileSystemMediaBackend` default) |
| **Constraints** | Rate limit / timeout / placeholder / typing keepalive |
| **Transport** | `reqwest` (REST) · `tokio-tungstenite` (WS) · `rumqttc` (MQTT) · `axum` (inbound webhooks) |
| **Crypto** | `rustls` · `sha1`/`sha2`/`hmac` · `aes`/`aes-gcm`/`cbc`/`ecb` |

### Features

#### Supported platforms

| Platform | Channel kind | Transport | Text | Media |
|----------|--------------|-----------|------|-------|
| Feishu (Lark) | `feishu` | WebSocket + REST | ✅ | ✅ |
| DingTalk | `dingtalk` | Stream (WS) + REST | ✅ | ✅ |
| QQ | `qq` | WebSocket gateway + OpenAPI | ✅ | ✅ |
| WeCom (Enterprise WeChat) | `wecom` | WebSocket + streaming reply | ✅ | ✅ |
| WeChat iLink | `weixin` | HTTP long-polling + CDN | ✅ | ✅ |
| Yuanbao (元宝) | `yuanbao` | Binary WebSocket + protobuf + COS | ✅ | ✅ |
| Xiaoyi (小艺) | `xiaoyi` | Primary/backup WebSocket + A2A | ✅ | ✅ |
| MQTT | `mqtt` | MQTT (TCP / TLS / WebSocket) | ✅ | ✅ |
| Telegram | `telegram` | HTTP long-polling | ✅ | ✅ |
| Discord | `discord` | Gateway WebSocket + REST (proxy-aware) | ✅ | ✅ |
| Slack | `slack` | Socket Mode WebSocket + Web API | ✅ | ✅ |
| WhatsApp | `whatsapp` | Inbound webhook + Graph API | ✅ | ✅ |
| LINE | `line` | Inbound webhook + Messaging API | ✅ | ✅ |
| WeChat Official Account | `wechat_mp` | Inbound webhook (XML / AES) + 客服消息 | ✅ | 🟡 |
| Google Chat | `googlechat` | Inbound webhook (JWT) + Chat API | ✅ | ✅ |

Every channel ships with its real transport, inbound media parsing, outbound platform media upload, overridden hooks, config field aliases, and (where applicable) crypto/login submodules. See [`docs/CHANNELS.md`](docs/CHANNELS.md).

#### Inbound webhooks

Eleven channels own a long-lived connection. The other four receive events as
provider-initiated `POST`s, which [`WebhookServer`](src/webhook.rs) serves:

```rust
use std::sync::Arc;
use buddy_gateway::channels::line::{LineConfig, LineWebhook};
use buddy_gateway::manager::{ChannelManager, ChannelOptions};
use buddy_gateway::webhook::{WebhookAdapter, WebhookServer};

let cfg = LineConfig { channel_access_token: token, channel_secret: secret, ..Default::default() };
let channel_id = manager.add_line_channel(cfg.clone(), ChannelOptions::new()).await?;

let mut server = WebhookServer::new();
server.mount_channel(&manager, "/webhooks/line", Arc::new(LineWebhook::new(cfg)), &channel_id).await?;
server.serve("0.0.0.0:8080").await?;
```

Mounting captures the channel's enqueue callback, so request handling needs no
manager reference and no lock. Signature verification is constant-time and runs
over the raw body — terminate TLS in front and forward the bytes untouched.


#### Unified message abstraction
- `InboundMessage` carries the text, structured `ContentPart`s (text / image / video / audio / file), and a `ChannelSubject`.
- Your processor is a closure returning a `Stream<Item = MessageEvent>` — emit `Message` for complete text, `Delta` for token streaming, and `Completed` to flush.
- `ChannelConfig` is a per-platform struct; the `Channel` trait defines `start` / `stop` / `parse_inbound` / `send_*`.

#### Custom channels
Implement the `Channel` trait, register it with `ChannelManager::add_channel_object(...)`, and the rest of the pipeline (media, constraints, push) works unchanged.

#### Constraints, media & push
- **Constraints** — rate limit, reply timeout, placeholder strategy, and typing keepalive per channel.
- **Media** — pluggable `MediaBackend`; persist attachments wherever you like.
- **Push** — `push_text` / `push_content` to one subject, or `push_to_all` for broadcasts.

#### Multi-tenant
Run several channels of the same kind (e.g. two Feishu apps for two teams) — each is isolated by `channel_id`.

#### Manager hooks
`on_pre_lock`, `control_message_predicate`, and `interrupt_message_predicate` let hosts bypass the session lock for `/stop`-style control messages.

## How to Use

### Prerequisites
- **Rust** (stable; built with 1.95) and `cargo`
- Credentials for the platforms you connect to

### 1. Add as a dependency

```toml
[dependencies]
buddy-gateway = { git = "https://github.com/just4zeroq/Buddy-gateway.git" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

### 2. Minimal echo bot (Telegram)

```rust
use std::sync::Arc;

use buddy_gateway::channel::MessageProcessor;
use buddy_gateway::channels::telegram::TelegramConfig;
use buddy_gateway::manager::{ChannelManager, ChannelOptions};
use buddy_gateway::models::{InboundMessage, MessageEvent};

/// A minimal processor: emit `"Echo: {text}"`, then complete.
fn echo() -> MessageProcessor {
    Arc::new(|msg: InboundMessage| {
        Box::pin(futures::stream::iter(vec![
            MessageEvent::text(format!("Echo: {}", msg.text())),
            MessageEvent::completed(),
        ]))
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut manager = ChannelManager::new(Some(echo())).with_workers_per_channel(4);
    manager.start().await?;

    let channel_id = manager
        .add_telegram_channel(
            TelegramConfig {
                bot_token: std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default(),
                ..Default::default()
            },
            ChannelOptions::default(),
        )
        .await?;
    println!("channel_id: {channel_id}");

    futures::future::pending::<()>().await; // run until Ctrl-C
    Ok(())
}
```

> The `examples/common` module provides `.env` loading, `echo_processor`, and other helpers used by all bundled examples.

### 3. Add more platforms

```rust
// Reusing `echo()` from the previous section.
use buddy_gateway::channels::{dingtalk::DingTalkConfig, feishu::FeishuConfig, qq::QQConfig};
use buddy_gateway::manager::{ChannelManager, ChannelOptions};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let mut manager = ChannelManager::new(Some(echo())).with_workers_per_channel(4);
manager.start().await?;

manager.add_feishu_channel(
    FeishuConfig {
        app_id: std::env::var("FEISHU_APP_ID").unwrap_or_default(),
        app_secret: std::env::var("FEISHU_APP_SECRET").unwrap_or_default(),
        ..Default::default()
    },
    ChannelOptions::default(),
).await?;

manager.add_qq_channel(
    QQConfig {
        app_id: std::env::var("QQ_APP_ID").unwrap_or_default(),
        token: std::env::var("QQ_TOKEN").unwrap_or_default(),
        secret: std::env::var("QQ_SECRET").unwrap_or_default(),
        ..Default::default()
    },
    ChannelOptions::default(),
).await?;

manager.add_dingtalk_channel(
    DingTalkConfig {
        app_key: std::env::var("DINGTALK_APP_KEY").unwrap_or_default(),
        app_secret: std::env::var("DINGTALK_APP_SECRET").unwrap_or_default(),
        ..Default::default()
    },
    ChannelOptions::default(),
).await?;
# Ok(()) }
```

Copy `.env.example` to `.env` for environment-based configuration.

### 4. Run a bundled example

```bash
# List all examples
ls examples

# Run one (credentials via env or a sibling .env)
TELEGRAM_BOT_TOKEN=123:abc cargo run --example telegram_bot

# No network needed — exercises the full inbound→process→outbound pipeline
cargo run --example loopback_echo
```

| Example | What it shows |
|---|---|
| `telegram_bot` · `discord_bot` · `feishu_bot` · `qq_bot` · `wecom_bot` · `weixin_bot` · `xiaoyi_bot` · `mqtt_bot` | A single-platform echo bot |
| `all_channels` | Four channels running in one manager |
| `advanced/feishu_advanced` · `advanced/qq_advanced` · `advanced/push_scheduler` | Per-channel constraints and proactive push |
| `loopback_echo` | The full pipeline with a built-in loopback channel (no network) |

## Documentation

- [What is Buddy Gateway?](#what-is-buddy-gateway)
- [Why Buddy Gateway?](#why-buddy-gateway)
- [How to Use](#how-to-use)
- **Reference**
  - [Architecture](#architecture)
  - [Registering a channel kind at runtime](#registering-a-channel-kind-at-runtime)
  - [QQ Bot QR binding](#qq-bot-qr-binding)
  - [Group conversation policy](#group-conversation-policy)
  - [Development](#development)
- **Project Info**
  - [Channel coverage](#channel-coverage)
  - [License](#license)

### Architecture

```
ChannelManager
 ├─ async queues + worker pools (per channel)
 ├─ session locks (per channel + session key)
 ├─ add_channel_object(Box<dyn Channel>) / add_*_channel(config, ChannelOptions)
 ├─ push_text / push_content / push_to_all
 └─ per-channel Channel (trait) + ChannelCore (shared state)
      ├─ start / stop
      ├─ parse_inbound → InboundMessage
      └─ send_text / send_content / send_media

MessageProcessor: InboundMessage → Stream<Item = MessageEvent>
```

```
src/
 ├─ channel.rs        Channel trait + ChannelCore + shared pipeline
 ├─ manager.rs        ChannelManager (+ ChannelOptions, predicates, pre-lock hook)
 ├─ models.rs         InboundMessage / MessageEvent / ContentPart / ChannelSubject
 ├─ constraints.rs    ChannelConstraints / RateLimiter / ReplyTimeoutGuard / TypingKeepalive
 ├─ group_context.rs  GroupContextManager (visibility / activation / history)
 ├─ media.rs          MediaBackend / FileSystemMediaBackend
 ├─ push_routing.rs   push metadata helpers
 ├─ utils.rs          text/media helpers, Debouncer, crypto provider, WS-over-proxy
 └─ channels/         discord feishu dingtalk qq wecom weixin yuanbao xiaoyi mqtt telegram (+ loopback)
```

Each `Channel` owns its transport; the manager owns scheduling, media, constraints, and fan-out. Your processor only sees the normalized stream.

### Registering a channel kind at runtime

Plugins can add their own platform without patching the crate. A kind is owned by the `plugin_id` that
registered it: the same owner may replace it on reload, another owner (and every builtin kind) cannot.

```rust
use buddy_gateway::channels::{register_channel_kind, unregister_channel_kind, ChannelBuilder};

let builder: ChannelBuilder = std::sync::Arc::new(
    |config, processor, channel_id, tenant_id| -> buddy_gateway::errors::Result<Box<dyn Channel>> {
        // build your own Channel here
        todo!()
    },
);
register_channel_kind("acme", builder, "my-plugin")?;
// ... later
unregister_channel_kind("acme", "my-plugin")?;
```

`build_channel("acme", ...)` then resolves the registration, so the manager and every existing entry
point keep working unchanged.

### QQ Bot QR binding

QQ Bot credentials can be obtained without manually copying an AppID and AppSecret. Render the returned URL as a QR code, then wait for confirmation:

```rust
use buddy_gateway::channels::qq::{login_qr::QQBotQRLogin, QQConfig};

let mut login = QQBotQRLogin::new("buddy", 2.0)?;
let qr = login.fetch_qr_code().await?;
println!("{}", qr.qrcode_url); // render this URL as a QR code

let result = login.wait_for_login(&qr.task_id, 480.0).await?;
if !result.connected {
    return Err(result.message.into());
}
let config = QQConfig::from_qr_credentials(&result.credentials[0]);
```

The one-time decryption key remains in memory and is discarded after success, expiry, cancellation, or timeout. Persisting the returned channel config is the caller's responsibility.

### Group conversation policy

`group_context` separates platform visibility from agent activation. QQ enables the shared manager by default with a conservative `auto + mention + recent(10)` policy. It can be configured globally and overridden by native group ID:

```json
{
  "group_context": {
    "enabled": true,
    "visibility": "auto",
    "activation": "mention",
    "history": "recent",
    "history_limit": 10,
    "history_ttl_seconds": 300,
    "clear_after_reply": true,
    "groups": {
      "GROUP_OPENID_WITH_FULL_ACCESS": { "visibility": "all", "activation": "always" },
      "GROUP_OPENID_MENTION_ONLY": { "visibility": "mention_only", "history": "none" }
    }
  }
}
```

- `visibility`: `auto`, `all`, `mention_recent`, or `mention_only`.
- `activation`: `mention` or `always`. `always` is accepted only with explicit `all` visibility, so replayed recent messages cannot trigger multiple replies.
- `history`: `recent` or `none`. Passive messages are bounded, expire by TTL, and are cleared after a successful reply.

### Development

**Prerequisites:** Rust (stable) + `cargo`

```bash
cargo check --all-targets   # compile lib + tests + examples
cargo test --lib --tests    # run the test suite
cargo build --release       # optimized build
```

> **Tip:** this project sets `[profile.dev] incremental = false` for deterministic builds — see `Cargo.toml`.

## Channel coverage

Every channel ships its real transport, inbound media parsing, outbound media upload, overridden hooks, and config aliases. The per-channel matrix lives in [`docs/CHANNELS.md`](docs/CHANNELS.md); the design overview is in [`docs/DESIGN.md`](docs/DESIGN.md).

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run `cargo check --all-targets` and `cargo test` before submitting
4. Open a Pull Request against `main`

## 📄 License

This project is licensed under the [MIT License](LICENSE).
