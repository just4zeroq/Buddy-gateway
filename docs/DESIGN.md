# octop-gateway 开发设计文档

> 本文档基于 `octop-gateway`（Python 版）源码整理，描述其架构、核心模块、
> 数据流、约束系统与 Channel 规范，并给出对应的 Rust 复刻方案。
> Rust 复刻工程位于 `buddy-gateway/`（`D:\code\buddy-gateway`）。

---

## 1. 项目概述

`octop-gateway` 是一个**多平台 IM 渠道桥接框架**，为 AI Agent / Bot 提供统一的消息抽象。
它把来自不同 IM 平台（飞书、钉钉、企业微信、微信 iLink、QQ、Discord、Telegram、
元宝、小艺、MQTT）的原始事件，归一化为统一的 `InboundMessage`，交给用户提供的
`MessageProcessor`（异步生成器）处理，再把处理结果（`MessageEvent` 流）按各平台协议回送。

核心设计目标：

- **统一抽象**：所有平台共享同一套消息模型与处理流水线。
- **单处理器**：用户只需编写一个 `MessageProcessor`，即可接入全部平台。
- **平台解耦**：核心层（`core`）不依赖任何平台 SDK；平台适配器（`channels/*`）互不依赖。
- **运行稳健**：每个 channel 的 worker 独立失败域；单条消息处理失败不影响整体。
- **约束感知**：统一的限流、回复超时占位、打字状态保持逻辑。

---

## 2. 整体架构

```
平台 SDK / Webhook ─► parse_inbound() ─► InboundMessage ─► _preprocess_inbound()
                                                                      │
                                      _persist_media + GroupContextManager.prepare
                                                                      ▼
_prepare_inbound ─► _process_inbound ─► MessageProcessor ─► MessageEvent 流
       ▲                                                              │
       └── _finalize_inbound ◄────────────── _send_text / _send_content / _send_media
```

### 2.1 两层模块划分

| 层级 | 模块 | 规则 |
|------|------|------|
| **核心层 Core** | `models` / `channel` / `manager` / `constraints` / `media` / `utils` / `group_context` / `push_routing` | 平台无关；**禁止** import 任何 `channels/*` |
| **适配器层 Channel Adapters** | `channels/*` | 可自由 import 核心层；适配器之间**互不 import** |

`channels/__init__.py`（Rust 中为 `channels/mod.rs`）是唯一的注册点
（`BUILTIN_CHANNELS` 映射 + `ChannelKind` 枚举），并以懒加载方式导入，
确保 `import octop_gateway` 不会拉起 Discord / Telegram 等全部 SDK。

### 2.2 媒体流架构

```
ChannelManager(media_backend=FileSystemMediaBackend("/tmp/media"))
    │ set_media_backend() before start()
    ▼
BaseChannel._persist_media(msg):                 # 入站自动落盘
    data = self.fetch_remote_media(url)          ← 平台鉴权 GET（可覆写）
    backend.save(data, key)                      ← 可插拔存储
    part.local_path = key                        ← MediaBackend key

BaseChannel.load_media_bytes(part):              # 出站读取（_send_media 内部使用）
    1. part.data       → base64 解码（零 I/O）
    2. part.local_path → backend.read(local_path)（需要 backend）
    3. part.url        → fetch_remote_media(url) + 缓存（backend 可选）
```

---

## 3. 数据模型（`models`）

### 3.1 内容类型系统（判别联合）

| 类型 | 类名 | 关键字段 |
|------|------|----------|
| `text` | `TextContent` | `text` |
| `image` | `ImageContent` | `url` / `alt_text` / `width` / `height` / `size` / `mime_type` / `local_path` / `data` |
| `video` | `VideoContent` | 同上 + `duration` / `thumbnail_url` |
| `audio` | `AudioContent` | 同上 + `duration` |
| `file` | `FileContent` | 同上 + `filename` |

每个媒体部件携带**最多三类字节来源**（优先级从高到低）：

1. `data` — base64 编码原始字节（最高优先级，Agent 已持有内存字节时最省）。
2. `local_path` — `MediaBackend` 的 key（**不是**文件系统路径）。
3. `url` — 远端来源，经 `fetch_remote_media()` 拉取（可能需要平台鉴权）。

> 出站读取**必须**走 `load_media_bytes()`，绝不直接解 `data` 或开 `local_path`。

### 3.2 `MessageEvent`

处理器回吐给渠道的事件类型（`MessageEventType`）：

`MESSAGE`（完整消息，立即发）/ `DELTA`（流式文本增量，累积至 `COMPLETED` 发送）/
`THINKING`（完整思考块）/ `THINKING_DELTA`（流式思考片段）/ `FLUSH`（把累积内容当一条立即发）/
`TYPING`（打字指示）/ `TOOL_START` / `TOOL_END` / `ERROR` / `COMPLETED`（流结束）。

`MessageEvent` 提供工厂方法：`text()` / `delta()` / `thinking()` / `thinking_delta()` /
`flush()` / `typing()` / `tool_start()` / `tool_end()` / `completed()` / `error_event()`。

### 3.3 `ChannelSubject`

会话的**出站路由句柄**：私聊时是用户，群聊时是会话 ID。

| 字段 | 含义 |
|------|------|
| `subject_id` | 唯一标识（openid / user_id / conversation id） |
| `chat_type` | `"direct"` / `"group"` |
| `metadata` | 平台路由附加上下文（`msg_id` / `webhook_url` / `chat_id` …），每次入站都会刷新 |
| `first_seen` / `last_seen` / `display_name` | 跟踪信息 |

> **重要**：`InboundMessage` 已移除 `sender_id` 字段。群消息的个体作者保留在
> `metadata.sender_id` / `metadata.sender_name`，使群内所有成员共享一个 thread 而不丢失归属。

### 3.4 `InboundMessage` / `GroupContext`

- `InboundMessage.channel_subject` 是**回复/会话目标**（群聊时为 conversation ID）。
- `GroupContext`：挂载在触发消息上的短期群聊上下文（`conversation_id` / `visibility` /
  `activation` / `messages` / `capability_degraded`）。

---

## 4. 入站处理流水线（`channel`）

`BaseChannel.handle_inbound()` 是共享模板，适配器**不应覆写它**（流式协议如企业微信除外），
而应通过 `_preprocess_inbound` / `_process_inbound` 两个钩子定制：

1. `parse_inbound(raw)` → `InboundMessage`
2. `_preprocess_inbound(message)`（平台解密/归一化）
3. `_track_subject()` + （若启用）`_persist_media()`
4. `GroupContextManager.prepare()` — 应用群可见性/激活策略；非触发消息被缓冲为被动上下文
5. `_process_inbound()` — 跑默认缓冲投递策略（delta 累积、thinking、flush、timeout、typing）
6. `_finalize_inbound()` — 成功后提交一次性群上下文状态

默认 `_process_inbound` 处理所有事件类型：
- `DELTA` 累积进文本缓冲；`THINKING_DELTA` 单独累积（受 `show_thinking` 控制）。
- `FLUSH` / `COMPLETED` / `MESSAGE` 时把缓冲按 `thinking_template` / 原文发送。
- `ERROR` / `TYPING` / `TOOL_START` / `TOOL_END` 各有对应投递逻辑。
- 全程由 `ReplyTimeoutGuard`（占位策略）与 `TypingKeepalive` 守护。

### 4.1 出站 API 三层

| 层 | 方法 | 调用者 | 限流 |
|----|------|--------|------|
| **公开回复** | `reply_text` / `reply_content` / `reply_media` | 入站上下文 | 每次调用恰好获取 1 个限流槽 |
| **公开推送** | `push_text` / `push_message` | Bot 主动（定时/Webhook/管理） | 同上，但直达 `_send_*` |
| **平台原语** | `_send_text` / `_send_content` / `_send_media`（抽象） | 子类实现，唯一对接平台 SDK 之处 | **不**获取（公共层已获取） |

**限流卫生规则**：子类方法内部只能调用兄弟 `_send_*`，**绝不可**调用 `reply_*` / `push_*`，
否则同一条用户可见消息会获取两次槽位，高负载下可能自锁。

---

## 5. 约束系统（`constraints`）

`ChannelConstraints` 由渠道声明，影响投递行为：

| 字段 | 含义 |
|------|------|
| `reply_timeout` | 首次回复时限（秒），0 = 无 |
| `send_rate_limit` | `(max_calls, window_seconds)` 滑动窗口限流 |
| `typing_keepalive_interval` | 打字保持间隔，0 = 关闭 |
| `timeout_strategy` | `"placeholder"`（超时先发占位）/ `"none"` |
| `placeholder_texts` | 占位文案（随机选） |
| `show_thinking` / `show_tool_hints` | 是否展示思考/工具状态 |
| `thinking_template` / `tool_hint_template` / `tool_end_template` | 模板 |

配套工具：
- `RateLimiter` — 滑动窗口限流，`acquire()` 在超限时阻塞等待。
- `ReplyTimeoutGuard` — 在 `reply_timeout * 0.8` 触发 `on_timeout`（发占位）。
- `TypingKeepalive` — 每隔 `interval` 调用一次 `_send_typing_indicator`。

---

## 6. 媒体流（`media`）

单一 `MediaBackend` 抽象是所有文件 I/O 的契约：

- `MediaBackend: save(data, key)` / `read(key)` / `exists(key)` / `get_local_path(key)`。
- `FileSystemMediaBackend` — 存于 `{root}/{key}`。

入站 `_persist_media` 下载远端媒体并落盘，给 `part.local_path` 盖章；
出站 `_send_media` 经 `load_media_bytes()` 读取，优先级 `data → local_path → url`。

**硬规则**：当 `data` / `local_path` 存在但平台无法投递时，**必须**发出可见文本标记
（如 `[Image (本地文件，<平台> 不可投递)]`），绝不允许静默丢弃媒体。

---

## 7. 群聊上下文（`group_context`）

`GroupContextConfig`（可序列化，支持 per-group 覆盖）区分两个正交维度：

- **visibility**（平台让 Bot 看到多少）：`AUTO` / `ALL` / `MENTION_RECENT` / `MENTION_ONLY`
- **activation**（Bot 何时真正开 agent 轮）：`MENTION` / `ALWAYS`

`GroupContextManager` 负责：
- 非触发群消息缓冲为短期被动上下文（`deque`，受 `history_limit` / `history_ttl_seconds` 限制）；
- 触发消息（被 @ 或 `activation=ALWAYS`）带上最近的被动上下文；
- 回复成功后按 `clear_after_reply` 清理一次性上下文。

---

## 8. 推送路由（`push_routing`）

`resolve_push_subject()` 为**主动推送**准备 subject：合并内存入站路由状态、平台回填、
并剥离仅被动回复使用的临时字段（`EPHEMERAL_PUSH_META`：
`msg_id` / `message_id` / `_frame` / `_ws_client` / `response_url` / `context_token` / `webhook_url`）。

---

## 9. Channel 规范（`channels/*`）

每个渠道必须遵循：

1. 继承 `BaseChannel`（Rust 中为 `impl Channel for XxxChannel`，持有 `ChannelCore`）。
2. 实现抽象方法：`start()` / `stop()` / `send_text()` / `send_content()` / `send_media()` / `parse_inbound()`。
3. 构造签名一致：`(processor, config, channel_id?, tenant_id?, debounce_seconds?, constraints?)`。
4. `Config` 继承 `ChannelConfig`，自带平台字段；`ChannelConfig` 提供 `channel_id` / `tenant_id` / `from_dict()`。
5. 若平台 SDK 用回调线程（`lark-oapi` / `wecom-sdk` / `discord.py`），用 `enqueue()` 经事件循环回派。
6. 日志：`tracing` INFO 记录生命周期/消息，DEBUG 记录协议细节。
7. 错误：worker 内绝不崩溃，记录日志后继续。
8. 去重：用 `OrderedDict` LRU（max 1000）实现 `_is_duplicate(msg_id)`。
9. 媒体 URL：处理 `//` 前缀（补 `https:`）与缺失 scheme。
10. 限流卫生：子类内只调 `_send_*`，不调 `reply_*` / `push_*`。
11. 不要覆写 `handle_inbound`，用 `_preprocess_inbound` / `_process_inbound` 定制。

### 9.1 连接模型

| 平台 | 传输 |
|------|------|
| Discord | Gateway WebSocket + REST |
| QQ | WebSocket |
| 飞书 | WebSocket + REST |
| 钉钉 | Stream |
| 企业微信 | WebSocket（aibot SDK） |
| 微信 iLink | HTTP 长轮询 |
| 元宝 | HTTP REST / WebSocket |
| 小艺 | WebSocket（主+备） |
| MQTT | MQTT broker |
| Telegram | 长轮询 / Webhook |

`channel_session_id` 表示**平台连接会话**，而非用户身份；每次重连会变。

---

## 10. ChannelManager（`manager`）

编排多个渠道：

- **每渠道队列**：`asyncio.Queue`（`tokio::sync::mpsc`）。
- **每渠道 worker 池**：`workers_per_channel`（默认 4），消费队列。
- **会话锁**：同一 `session_key` 的消息串行处理，跨会话并行。
- **批处理**：同会话短时间内多条消息可合并（`merge_inbound`）。
- **主动推送 API**：`push_text` / `push_content` / `push_to_all` / `run_in_session`。
- **动态增删**：`add_channel(channel_type, config)` / `remove_channel` / `probe_channel`。
- **线程安全 enqueue**：`call_soon_threadsafe`（`tokio` 中用 `Handle::block_on` / `spawn`）。

内置便捷方法：`add_feishu_channel` / `add_wecom_channel` / `add_qq_channel` / …
（Rust 中对应 `ChannelManager::add_*_channel`）。

---

## 11. 注册机制（`channels/__init__.py` → `channels/mod.rs`）

- `_CHANNEL_MAP`（Python）→ `BUILTIN_CHANNELS`（Rust 中 `HashMap<&str, fn ...>` 或枚举分发）。
- `ChannelKind` 枚举 + `SUPPORTED_CHANNEL_KINDS`。
- 懒加载：仅当实际构造某渠道时才 import 其模块 / SDK。
- 一致性测试（parity test）：`_CHANNEL_MAP` 与 `ChannelKind` 必须同步。

---

## 12. Rust 复刻方案（`buddy-gateway`）

### 12.1 设计映射

| Python | Rust |
|--------|------|
| `pydantic.BaseModel` + `StrEnum` | `#[derive(serde::Serialize/Deserialize)]` + `enum` + `strum`/`serde` |
| `BaseChannel` 抽象类（状态 + 行为） | 拆分为 **`ChannelCore`**（共享状态 + 共享流水线泛型方法）+ **`Channel` trait**（平台契约） |
| `asyncio` | `tokio` |
| `aiohttp` | `reqwest` |
| `paho-mqtt` | `rumqttc` |
| `websockets` / `python-socketio` | `tokio-tungstenite` |
| 异步生成器 `AsyncIterator[MessageEvent]` | `MessageProcessor = Box<dyn Fn(InboundMessage) -> Pin<Box<dyn Stream<Item=MessageEvent> + Send>>>`（用 `async_stream` / `futures`） |
| `aiohttp.ClientSession` | `reqwest::Client`（含连接池） |
| `logging` | `tracing` |
| `base64` | `base64` crate |
| `uuid4().hex` | `uuid::Uuid::new_v4().simple().to_string()` |

### 12.2 关键 Rust 设计：`ChannelCore` + `Channel` trait

Python 的 `BaseChannel` 同时持有状态与定义抽象方法。Rust 没有抽象类字段，
因此把"共享状态 + 共享流水线逻辑"抽成 `ChannelCore` 结构体（含 `media_backend`、
`rate_limiter`、`known_subjects`、`constraints`、`processor`、`group_context_manager`、
`connection_session`、`enqueue` 回调等），并把"平台契约"定义为 `Channel` trait：

```rust
pub trait Channel: Send + Sync {
    // —— 平台抽象/可覆写方法 ——
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()>;
    async fn send_content(&mut self, subject: &ChannelSubject, parts: &[ContentPart]) -> Result<()>;
    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()>;
    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage>;

    // —— 共享提供的默认方法（访问 self.core_mut()） ——
    async fn handle_inbound(&mut self, raw: &RawPayload) -> Result<()> { ... }
    async fn reply_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> { ... }
    async fn push_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> { ... }
    async fn load_media_bytes(&self, part: &ContentPart) -> Result<(Vec<u8>, String)> { ... }
    // ... 其余流水线与 channel.py 一一对应 ...

    // 每个具体渠道实现：
    fn core(&self) -> &ChannelCore;
    fn core_mut(&mut self) -> &mut ChannelCore;
}
```

每个平台适配器形如：

```rust
pub struct TelegramChannel {
    core: ChannelCore,
    config: TelegramConfig,
    http: Option<reqwest::Client>,
    connection_session: Option<String>,
}
impl Channel for TelegramChannel { /* 实现平台方法与 core() 访问 */ }
```

这样**共享流水线只写一次**（在 `Channel` trait 的默认方法里），每个渠道只需实现平台相关部分，
与 Python "子类只写平台逻辑、其余继承" 的语义完全一致。

### 12.3 目录结构

```
buddy-gateway/
├── Cargo.toml
├── README.md
├── docs/DESIGN.md
├── src/
│   ├── lib.rs
│   ├── errors.rs
│   ├── models.rs
│   ├── constraints.rs
│   ├── media.rs
│   ├── utils.rs
│   ├── group_context.rs
│   ├── push_routing.rs
│   ├── channel.rs          # ChannelCore + Channel trait + 共享流水线
│   ├── manager.rs          # ChannelManager
│   └── channels/
│       ├── mod.rs          # BUILTIN_CHANNELS + ChannelKind
│       ├── loopback.rs     # 内置回环/测试渠道（完整可跑）
│       ├── mqtt.rs
│       ├── telegram.rs
│       ├── feishu.rs
│       ├── dingtalk.rs
│       ├── wecom.rs
│       ├── weixin.rs
│       ├── yuanbao.rs
│       ├── xiaoyi.rs
│       ├── qq.rs
│       └── discord.rs
└── tests/
    ├── core_tests.rs
    └── manager_tests.rs
```

### 12.4 复刻范围与说明

- **核心层**（`models` / `constraints` / `media` / `utils` / `group_context` / `push_routing` /
  `channel` / `manager`）：**完整复刻**语义，含单元测试。
- **channel 适配器**：10 个平台全部按 `Channel` trait 契约实现 `parse_inbound` / `start` / `stop` /
  `send_text` / `send_content` / `send_media`，HTTP/WS 协议使用 `reqwest` / `tokio-tungstenite` /
  `rumqttc` 在公开 API 层面实现。其中 `loopback` / `mqtt` / `telegram` 为**可直接运行与测试**的参考实现；
  其余平台因依赖专有 SDK 或需线上凭据，按公开协议结构实现并保证**编译通过**，线上联调需对应凭据。
- 由于各平台 SDK 为闭源或需鉴权，复刻侧重**框架契约与协议结构**的忠实还原，而非逐字节兼容私有 SDK。

---

## 13. 测试策略

| 测试对象 | 说明 |
|----------|------|
| `models` | `ContentPart` 判别联合解析、工厂方法、属性 |
| `constraints` | `RateLimiter` 滑动窗口、`ReplyTimeoutGuard`、`TypingKeepalive` |
| `group_context` | visibility/activation 策略、`prepare` 缓冲/触发、`mark_replied` 清理 |
| `utils` | `extract_text` / `extract_media` / `merge_messages` / `Debouncer` |
| `media` | `FileSystemMediaBackend` save/read/exists/get_local_path |
| `channel` | 用 `loopback` 跑完整入站流水线（delta 累积、thinking、flush、占位超时） |
| `manager` | 队列/worker/会话锁/批处理/push |
| `channels` | 各渠道 `parse_inbound` 的 mock 解析单测（无网络） |

---

## 14. 约束与禁止项（复刻时同样遵守）

- 不要覆写 `handle_inbound`（流式协议除外），用 `_preprocess_inbound` / `_process_inbound` 定制。
- 子类内部不要调用 `reply_*` / `push_*`。
- 不要直接读 `part.data` / `part.local_path` / `part.url` 取出站字节 —— 用 `load_media_bytes()`。
- 当 `data` / `local_path` 存在时绝不允许静默丢弃媒体 —— 必须发可见文本标记。
- `InboundMessage` 不设 `sender_id`（已移除）；群作者放 `metadata.sender_id`。
- 核心层不 import 任何 channel；适配器之间互不 import。
- 不要阻塞事件循环（无同步 HTTP、`sleep`、重 CPU）。
- 不提交凭据（`.env` 忽略）。
