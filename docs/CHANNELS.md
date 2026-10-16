# Buddy Gateway — 通道覆盖与实现说明

> 本文档给出 10 个通道的能力矩阵，以及核心层 / 管理器的实现说明与若干 Rust 特有设计取舍。
> 图例：🟢 已实现 · 🟡 部分 / 演进中

---

## 0. 速览

- **10 个通道 + 核心层 + 管理器均已实现**：每个通道都能**真实建连收消息**，具备**入站解析（含富媒体）/ 出站发送（含平台媒体上传）/ 覆写钩子 / 配置字段别名与归一化**。
- **核心层**：统一消息模型、约束系统、群聊上下文、媒体后端、推送路由、共享消息管线。
- **管理器**：队列 / worker 池 / 会话锁 / 批处理、`on_pre_lock`、control / interrupt 谓词、`ChannelOptions`、`probe_channel`、`list_subjects` / `list_all_users`。
- **验证**：`cargo check --all-targets` → **0 error / 0 warning**；`cargo test --lib --tests` → **137 passed**。

---

## 1. 核心层

| 功能项 | 状态 | 说明 |
|---|---|---|
| `models`（ContentPart / InboundMessage / MessageEvent / ChannelSubject / GroupContext） | 🟢 | 统一内容与消息模型 |
| `constraints`（默认值 / RateLimiter / ReplyTimeoutGuard / TypingKeepalive / tool hint） | 🟢 | 限流、超时（signal + mpsc 实现）、打字保活 |
| `group_context`（policy resolve/overlay、prepare/append/expire/merge/mark_replied） | 🟢 | 可见性 / 触发 / 历史缓冲 |
| `media`（MediaBackend / FileSystemMediaBackend） | 🟢 | 附件落盘 |
| `push_routing`（alias / strip ephemeral） | 🟢 | 主动推送元数据 |
| `utils`（has_text / extract_text / extract_media / merge_messages / Debouncer） | 🟢 | 文本与媒体工具 |
| `Channel::handle_inbound` 模板（prepare → process → finalize） | 🟢 | 入站主流程 |
| `load_media_bytes`（data → local_path → url，URL 结果缓存到后端） | 🟢 | 含 `derive_storage_key` |
| `label_with_filename()`（File 类型回退文案带文件名） | 🟢 | 文本回退标记 |
| `default_constraints()` + `configure_constraints(&self, …)` | 🟢 | 静态默认 + 依赖 config 的动态调整 |
| `process_inbound` + `default_process_inbound_body` | 🟢 | 可覆写管线 + 可复用的标准循环 |
| `set_constraints` 覆盖全部 `ChannelConstraints` 字段 | 🟢 | 运行时调优 |
| rustls crypto provider 一次性安装（`utils::ensure_crypto_provider`） | 🟢 | 保证 WSS 握手不 panic |

---

## 2. 通道矩阵

| 通道 | 传输层 | 入站（含富媒体） | 出站（含平台媒体） | 覆写钩子 | 子模块 |
|---|---|---|---|---|---|
| **Discord** | gateway WS(op10/2/6/1/7/9) + **代理** | 门控 / 白名单 / 去重 1000 / 附件→Image/Audio/Video/File | `split_discord_text`(2000 围栏感知) + multipart + typing | `default_constraints`(typing 8s) / `send_typing_indicator` / `fetch_remote_media`(25 MiB) | 内联 |
| **Telegram** | `getUpdates` 长轮询 | text/caption / photo[-1] / doc/video/voice/audio（`getFile`） | `markdown_to_telegram_html` + 4000 分块 + HTML→纯文本回退 + multipart | `default_constraints`((20,60)，typing 依 `show_typing`) / `send_typing_indicator` | `format_html` 内联 |
| **Feishu** | tenant_access_token + WS + watchdog | `im.message.receive_v1`（多类消息、mentions、dedup 2000） | post(markdown) + `/im/v1/images`、`/im/v1/files` + reply_in_thread | `fetch_remote_media`(Bearer) / `enrich_push_metadata` | 内联 |
| **DingTalk** | OAuth2 + Stream WS | text/richText/picture/file/audio | sessionWebhook / `oToMessages/batchSend`（>3500 文本否则 markdown）+ `/media/upload` | `fetch_remote_media`(downloadCode) | 内联 |
| **QQ** | `bots.qq.com` token + `/gateway/bot` + WS opcode | 6 类事件 + 富媒体标签(`qqimg/qqvideo/qqaudio/qqfile`) + attachments + 引用 + 群上下文 | markdown 回退 + 媒体三级上传 + **C2C `stream_messages` 流式** + `msg_seq` + 401 重试 | `process_inbound`(C2C 流式) / `send_typing_indicator`(input_notify) / `default_constraints` / `fetch_remote_media` / `enrich_push_metadata` | `stream` / `stream_blocks` / `login_qr` 内联 |
| **WeCom** | WS(bot_id+secret 认证 / 心跳 30s / 无限重连) | msgtype 白名单 + `aeskey` + 媒体解密下载 | `reply_stream`→`response_url` + `upload_media` / reply_media | `handle_inbound`(流式) / `default_constraints` / `enrich_push_metadata` | 内联 |
| **Weixin** | 多账号 `getUpdates` 长轮询(`X-WECHAT-UIN`，`-14` 暂停 300s) | message_type==1 + item_list + **CDN AES-128-ECB 解密** | `split_weixin_text`(2000) + `context_token` + CDN AES 加密上传 + `send_items` + typing ticket | **`process_inbound`(typing 灯 + 占位气泡 + status=2 收尾)** / `preprocess_inbound` / `send_typing_indicator` / `default_constraints` / `fetch_remote_media` / `enrich_push_metadata` | `api` / `media` / `text` / `types` / `login_qr` 内联 |
| **Yuanbao** | sign-token(HMAC-SHA256) + 二进制 WS + auth-bind | BINARY `CMD_INBOUND_MESSAGE` → TIM 元素（dedup TTL 300） | protobuf ConnMsg（手写）+ send_c2c/group + **COS 直传**(HMAC-SHA1) + heartbeat + typing | `default_constraints`(typing 2.0,(2,5)) / `send_typing_indicator` / `fetch_remote_media` | `constants` / `utils` / `protocol` 内联 |
| **XiaoYi** | 主备 WS + HMAC-SHA256 签名头 | A2A `message/stream`；parts(text/file→image/file) | A2A artifact(json-rpc artifact-update) + status-update final + reasoning chunk | `process_inbound`(A2A 流式) / `default_constraints`((20,60)) | `auth` / `constants` 内联 |
| **MQTT** | rumqttc + 订阅 + 重连 + **TLS(自定义 CA / 客户端证书)** | Publish → JSON / 纯文本 → enqueue | `publish_topic.format({client_id})` + 媒体文本回退 | `default_constraints`((30,60)) | 内联 |

---

## 3. 管理器

| 功能项 | 状态 | 说明 |
|---|---|---|
| 队列 / worker 池 / 会话锁 / 批处理 / 合并 | 🟢 | 每通道独立队列与 worker |
| drain 遇不同 session / control 消息放回队列 | 🟢 | 不丢消息 |
| `on_pre_lock`（`set_pre_lock_handler` / `with_on_pre_lock`） | 🟢 | 会话锁前调用，可用于 `/stop` 之类取消信号 |
| `control_message_predicate` | 🟢 | 控制消息不参与批处理 |
| `interrupt_message_predicate` | 🟢 | 旁路会话锁，立即处理 |
| `probe_channel`（含 yuanbao `probe_mode="sign_token"`） | 🟢 | 临时建连校验凭据 |
| `list_subjects` / `list_all_users` | 🟢 | 已知会话枚举 |
| `get_channel` | 🟢 | 公开 |
| `add_channel_full` / `add_*_channel(config, opts)` | 🟢 | `ChannelOptions { processor, channel_id, tenant_id, debounce_seconds, constraints }` |
| `run_in_session` 校验 | 🟢 | channel / session_key 校验 |
| 全局 `set_constraints` 任意字段 | 🟢 | |

---

## 4. 实现说明（Rust 特有取舍）

1. **可选构造参数 → `ChannelOptions`**：`add_channel_full` / `add_*_channel(config, opts)` 用结构体承载 processor / id / debounce / constraints 覆盖，能力与动态关键字参数等价。
2. **默认约束分两处**：静态 `default_constraints()` 给出每通道默认值；依赖 config 的调整放在 `configure_constraints(&self, &mut ChannelConstraints)`（例如 Telegram 的打字间隔随 `show_typing` 在 4.0 / 0.0 间切换）。
3. **可覆写入站管线**：`process_inbound` 可被通道覆写；标准循环体以 `default_process_inbound_body` 暴露，便于覆写后复用。QQ / WeCom / Weixin 在覆写中内联了等价的 `select` 循环（typing、占位、流式增量），行为与钩子语义一致。
4. **去重是每通道行为**：各通道使用自有的 LRU / TTL 去重缓存（`ChannelCore::is_duplicate` 亦提供通用 1000 条 LRU）。
5. **代理**：`ensure_http()` 返回无代理的共享 client；需要代理的通道自建 client。WebSocket 通过 `utils::ws_connect_proxied`（HTTP CONNECT 隧道 + rustls）支持 `http_proxy`，Discord 网关已接入。
6. **配置归一化**：`channels::normalize_config` 负责字段别名、字符串去空白，以及按通道的强制转换（如 QQ 的 `c2c_streaming` 字符串/布尔、`group_context.enabled` 补默认、Discord/Telegram 的 ID 串拆分与去重）。

---

## 5. 验证

- `cargo check --all-targets` → **0 error / 0 warning**
- `cargo test --lib --tests` → **137 passed**
  （lib 2、channel_config 27、channel_pipeline 17、constraints 27、group_context 8、manager 20、media 4、models 17、push_routing 3、utils 12）
- QQ bot 端到端：`QQ gateway connecting: wss://api.sgroup.qq.com/websocket` → `QQ gateway READY`
