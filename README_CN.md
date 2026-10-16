# Buddy Gateway（Rust 版）

<p align="center">
  <b><a href="README.md">English</a></b> · <b>中文</b>
</p>

> **多平台 IM 通道桥**：为 AI Agent / 机器人提供统一的即时通讯抽象层。
> 本项目是 Python 版 [octop-gateway](https://github.com/TencentCloud/octop-gateway) 的 **Rust 1:1 复刻**。

`buddy-gateway` 接入主流 IM 平台，把每条入站消息归一化到同一条处理管线，因此你的 Agent 逻辑**只写一次**，就能同时跑在飞书、钉钉、QQ、企业微信、微信 iLink、元宝、小艺、MQTT、Telegram、Discord 上。

---

## 它是什么？

| | 特性 | 说明 |
|---|------|------|
| 🔌 | **10 个平台，一套处理器** | 飞书 / 钉钉 / QQ / 企业微信 / 微信 iLink / 元宝 / 小艺 / MQTT / Telegram / Discord，统一接口 |
| 🧩 | **统一抽象** | `Channel` + `ChannelCore` 把各平台差异收敛为统一的 `InboundMessage` / `MessageEvent` 模型 |
| 📨 | **流式事件** | 处理器返回 `Stream<Item = MessageEvent>`，天然支持 token 级流式输出 |
| 💾 | **可插拔媒体** | `MediaBackend` 负责附件落盘，默认提供 `FileSystemMediaBackend` |
| 🚦 | **约束** | 每通道的限流、回复超时、占位文案、打字保活 |
| 📡 | **主动推送** | `push_text` / `push_content` / `push_to_all` |
| 🏢 | **多租户** | 同类型通道可多实例并存，按 `channel_id` 隔离 |
| 🦀 | **地道 Rust** | 纯 `tokio` + `async-trait`，强类型模型，无隐式魔法 |

## 为什么用它？

网关位于你的 Agent 与外部世界之间。每个平台实现为一个 `Channel`，负责建连、解析入站流量、发送回复；`ChannelManager` 通过异步队列编排它们，并把归一化的 `MessageEvent` 流交给你的 Agent。你的机器人里**不会出现平台专用代码**，只有一套处理器。

> 因为抽象收敛在网关里，切换 IM 平台只是改配置，而不是重写业务。

### Python → Rust 设计映射

| Python（`octop-gateway`） | Rust（`buddy-gateway`） |
|---|---|
| `BaseChannel`（抽象基类） | `Channel` trait + `ChannelCore`（共享状态） |
| `_default_constraints(self)` | `default_constraints()` + `configure_constraints(&self, …)` |
| `_process_inbound`（可覆写，`super()._process_inbound`） | `process_inbound` + `default_process_inbound_body` |
| `MessageProcessor`（异步生成器） | `Arc<dyn Fn(InboundMessage) -> Pin<Box<dyn Stream<Item = MessageEvent> + Send>> + Send + Sync>` |
| `add_channel(..., **kwargs)` | `add_channel_full(..., ChannelOptions { … })` |
| `ChannelConfig.from_dict` | `channels::normalize_config`（别名 + strip + 强制转换） |
| `asyncio` | `tokio` |

### 技术栈

| 层 | 技术 |
|---|---|
| **语言** | Rust 2021（stable，构建于 1.95） |
| **异步运行时** | `tokio` |
| **通道模型** | `Channel` trait + 各平台适配器（内置 10 个） |
| **消息模型** | `InboundMessage` / `MessageEvent` / `ContentPart` |
| **编排** | `ChannelManager`（异步队列 / worker 池 / 会话锁） |
| **媒体** | `MediaBackend`（默认 `FileSystemMediaBackend`） |
| **约束** | 限流 / 超时 / 占位 / 打字保活 |
| **传输** | `reqwest`（REST）· `tokio-tungstenite`（WS）· `rumqttc`（MQTT） |
| **加密** | `rustls` · `sha1`/`sha2`/`hmac` · `aes`/`aes-gcm`/`cbc`/`ecb` |

### 支持的平台

| 平台 | 通道类型 | 传输 | 文本 | 媒体 |
|---|---|---|---|---|
| 飞书 (Lark) | `feishu` | WebSocket + REST | ✅ | ✅ |
| 钉钉 | `dingtalk` | Stream(WS) + REST | ✅ | ✅ |
| QQ | `qq` | WebSocket 网关 + OpenAPI | ✅ | ✅ |
| 企业微信 | `wecom` | WebSocket + 流式回复 | ✅ | ✅ |
| 微信 iLink | `weixin` | HTTP 长轮询 + CDN | ✅ | ✅ |
| 元宝 | `yuanbao` | 二进制 WS + protobuf + COS | ✅ | ✅ |
| 小艺 | `xiaoyi` | 主备 WebSocket + A2A | ✅ | ✅ |
| MQTT | `mqtt` | MQTT（TCP / TLS / WebSocket） | ✅ | ✅ |
| Telegram | `telegram` | HTTP 长轮询 | ✅ | ✅ |
| Discord | `discord` | Gateway WebSocket + REST（支持代理） | ✅ | ✅ |

每个通道都按原版实现了：真实长连接、入站媒体解析、出站平台媒体上传、覆写钩子、配置字段别名与（如适用）加密 / 扫码登录子模块。详见 [`docs/PARITY.md`](docs/PARITY.md)。

## 快速开始

### 前置条件
- **Rust**（stable，构建于 1.95）与 `cargo`
- 目标平台的凭据

### 1. 作为依赖引入

```toml
[dependencies]
buddy-gateway = { git = "https://github.com/just4zeroq/Buddy-gateway.git" }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

### 2. 最小回声机器人（Telegram）

```rust
use std::sync::Arc;

use buddy_gateway::channel::MessageProcessor;
use buddy_gateway::channels::telegram::TelegramConfig;
use buddy_gateway::manager::{ChannelManager, ChannelOptions};
use buddy_gateway::models::{InboundMessage, MessageEvent};

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

    futures::future::pending::<()>().await; // Ctrl+C 退出
    Ok(())
}
```

### 3. 运行内置示例

```bash
TELEGRAM_BOT_TOKEN=123:abc cargo run --example telegram_bot

# 无需网络：用内置 loopback 通道跑通「入站 → 处理 → 出站」全链路
cargo run --example loopback_echo
```

示例一览（与 Python 原版一一对应）：`telegram_bot`、`discord_bot`、`feishu_bot`、`qq_bot`、`wecom_bot`、`weixin_bot`、`xiaoyi_bot`、`mqtt_bot`、`all_channels`、`advanced/{feishu_advanced,qq_advanced,push_scheduler}`，以及新增的 `loopback_echo`。

环境变量可复制 `.env.example` 为 `.env`（示例会自动加载）。

## 架构

```
ChannelManager
 ├─ 异步队列 + worker 池（每通道）
 ├─ 会话锁（通道 + 会话键）
 ├─ add_channel_object(Box<dyn Channel>) / add_*_channel(config, ChannelOptions)
 ├─ push_text / push_content / push_to_all
 └─ 每通道 Channel(trait) + ChannelCore(共享状态)
      ├─ start / stop
      ├─ parse_inbound → InboundMessage
      └─ send_text / send_content / send_media

MessageProcessor: InboundMessage → Stream<Item = MessageEvent>
```

```
src/
 ├─ channel.rs        Channel trait + ChannelCore + 共享管线
 ├─ manager.rs        ChannelManager（+ ChannelOptions / 谓词 / pre-lock 钩子）
 ├─ models.rs         InboundMessage / MessageEvent / ContentPart / ChannelSubject
 ├─ constraints.rs    ChannelConstraints / RateLimiter / ReplyTimeoutGuard / TypingKeepalive
 ├─ group_context.rs  GroupContextManager（可见性 / 触发 / 历史）
 ├─ media.rs          MediaBackend / FileSystemMediaBackend
 ├─ push_routing.rs   推送元数据处理
 ├─ utils.rs          文本/媒体助手、Debouncer、crypto provider、WS-over-proxy
 └─ channels/         discord feishu dingtalk qq wecom weixin yuanbao xiaoyi mqtt telegram（+ loopback）
```

## 群聊策略（`group_context`）

`group_context` 把「平台可见性」与「Agent 触发」分离。QQ 默认保守策略 `auto + mention + recent(10)`，支持按原生群 ID 覆盖：

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

- `visibility`：`auto` / `all` / `mention_recent` / `mention_only`。
- `activation`：`mention` / `always`（`always` 仅在显式 `all` 可见性下生效，避免历史回放重复触发）。
- `history`：`recent` / `none`（被动消息有上限、按 TTL 过期、回复成功后清空）。

## 开发

```bash
cargo check --all-targets   # 编译 lib + tests + examples
cargo test --lib --tests    # 运行测试套件
cargo build --release       # 优化构建
```

> 项目在 `Cargo.toml` 中设置了 `[profile.dev] incremental = false`，以获得确定性构建。

## 与原版的对照

每个通道都做了 1:1 移植：真实传输、入站媒体解析、出站平台媒体上传、覆写钩子、配置别名与子模块。逐项对照矩阵（含少数「Rust 惯用法映射，非功能缺口」的说明）见 [`docs/PARITY.md`](docs/PARITY.md)；架构映射见 [`docs/DESIGN.md`](docs/DESIGN.md)。

## 🔗 相关项目

| 项目 | 说明 |
|------|------|
| [octop-gateway](https://github.com/TencentCloud/octop-gateway) | 本项目的 Python 原版实现 |
| [octop-harness](https://github.com/TencentCloud/octop-harness) | 驱动网关处理器的 Agent 运行时 |
| [Octop](https://github.com/TencentCloud/Octop) | 组合 Harness 栈的自托管助手 |

## 📄 许可证

本项目基于 [MIT License](LICENSE) 开源。
