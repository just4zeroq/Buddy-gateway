# Buddy Gateway — 通道覆盖与实现说明

> 本文档给出 15 个通道的能力矩阵，以及核心层 / 管理器 / Webhook 服务的实现说明与若干 Rust 特有设计取舍。
> 图例：🟢 已实现 · 🟡 部分 / 演进中

---

## 0. 速览

- **15 个通道 + 核心层 + 管理器 + Webhook 服务均已实现**：每个通道都能**真实收消息**，具备**入站解析（含富媒体）/ 出站发送（含平台媒体上传）/ 覆写钩子 / 配置字段别名与归一化**。
- **两类入站传输**：11 个通道用长连接（WebSocket / 长轮询 / MQTT）；4 个平台（WhatsApp / LINE / 微信公众号 / Google Chat）由 provider **主动 POST** 到你的公网回调，统一由 `webhook::WebhookServer` 承载。
- **核心层**：统一消息模型、约束系统、群聊上下文、媒体后端、推送路由、共享消息管线。
- **管理器**：队列 / worker 池 / 会话锁 / 批处理、`on_pre_lock`、control / interrupt 谓词、`ChannelOptions`、`probe_channel`、`list_subjects` / `list_all_users`、`enqueue_handle`。
- **验证**：`cargo check --all-targets` → **0 error / 0 warning**；11 个测试目标 → **195 passed**。

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
| **Slack** | `apps.connections.open`(xapp-) → **Socket Mode WS**（3s 内 ack envelope；`disconnect` 后刷新重连 + backoff） | `events_api` → message / app_mention；门控 + 白名单 + 去重 2000 + 附件→Image/Video/Audio/File；`<@U…>` 剥离与 `bot_mentioned` | `chat.postMessage`（mrkdwn 转换 + 4000 分块 + `thread_ts`）+ `files.getUploadURLExternal` → 上传 → `files.completeUploadExternal` | `default_constraints`((20,60)) / `fetch_remote_media`(Bearer, 25 MiB) / `enrich_push_metadata`(thread_ts) | 内联 |
| **WhatsApp** | **Webhook**（`GET` hub 挑战 + `POST` `X-Hub-Signature-256` HMAC-SHA256） | `entry[].changes[].value.messages[]`（text/button/interactive/reaction/location/image/video/audio/document/sticker）+ 去重 + 白名单 | `POST /{v}/{phone_number_id}/messages`（4096 分块）+ `POST /{v}/{phone_number_id}/media` 上传后按 image/video/audio/document 发送 | **`preprocess_inbound`（两步媒体：media_id → 短时 URL → 下载落盘）** / `default_constraints`((20,60)) / `enrich_push_metadata` | 内联 |
| **LINE** | **Webhook**（`X-Line-Signature` base64 HMAC-SHA256） | `events[]`（text/image/video/audio/file/sticker/location/postback）+ 白名单 + 去重（`webhookEventId`） | `POST /v2/bot/message/reply`（优先，`replyToken` 一次性）→ 失败回退 `/push`（5000 分块）；媒体需公开 HTTPS URL | `default_constraints`((20,60)) / `fetch_remote_media`(仅 `*.line.me`/`*.line-scdn.net` 加 Bearer) / `enrich_push_metadata` | 内联 |
| **微信公众号** | **Webhook**（GET `echostr` 握手 + POST `signature`/`msg_signature`；SHA1 排序校验） | XML（明文或 **AES-256-CBC 安全模式**，IV=key[..16]）→ text/image/voice(识别文本)/video/location/link/event；去重（MsgId / From+CreateTime+Event） | 被动回复（5s 窗口内 XML，安全模式加密包裹）/ 客服消息 `cgi-bin/message/custom/send` + `cgi-bin/media/upload` | **`preprocess_inbound`（`wechat-media://` 标记 → `media/get` 落盘）** / `default_constraints`(timeout 4.5) / `enrich_push_metadata` | 内联 |
| **Google Chat** | **Webhook**（`Authorization: Bearer <JWT>` claims 校验：`iss`/`exp`/`aud`） | `MESSAGE` / `ADDED_TO_SPACE` / `REMOVED_FROM_SPACE` / `CARD_CLICKED`；`attachment[]`→Image/Video/Audio；自身消息过滤 + 去重 | `POST /v1/{space}/messages`（Markdown 子集转换 + 4000 分块）+ `upload/v1/{space}/attachments?uploadType=multipart` | `default_constraints`((20,60)) / `enrich_push_metadata` | 内联 |

---

## 2.1 Webhook 服务（`webhook::WebhookServer`）

长连接类通道自己持有连接；**POST 回调类平台需要的是一个 HTTP 服务**，这就是 `WebhookServer`：

- **路由**：按路径 `mount`；同一路径重复挂载报错；路径自动归一化（补前导 `/`、去尾 `/`）。
- **适配器契约**：`WebhookAdapter { channel_type, verify_challenge(GET), handle_request(POST) }`。适配器只做**验签 + 翻译**，重活（按 id 拉媒体、解密）留给 `Channel::preprocess_inbound`（它有 HTTP client 和 media backend，且是 async）。
- **零锁路径**：`mount_channel` 在挂载时一次性抓取该通道的 `EnqueueFn`（`ChannelManager::enqueue_handle`），请求处理阶段**不需要 manager 引用、也不需要拿锁**。
- **入队即复用整套管线**：payload 进入通道队列后，群聊上下文、媒体持久化、debility、会话锁、worker 池全部照常生效。
- **默认路径**：`/webhooks/whatsapp`、`/webhooks/line`、`/webhooks/wechat`、`/webhooks/googlechat`（`channels::webhook_path_for`）。
- **常量时间验签助手**：`hmac_sha256_hex` / `hmac_sha256_base64` / `hmac_sha1_hex` / `verify_sha256_hex_signature` / `verify_sha256_base64_signature` / `wechat_signature` / `constant_time_eq`。
- **请求体上限** 8 MiB；未知路径 404、无 GET 握手的 `GET` 405、验签失败 400。

> 部署注意：各 provider 都要求**公网 HTTPS**，请在前置（nginx / Caddy / LB / Cloud Run）终止 TLS 并**原样转发请求体**——签名是对收到的原始字节计算的，任何重写都会导致验签失败。

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
6. **配置归一化**：`channels::normalize_config` 负责字段别名、字符串去空白，以及按通道的强制转换（如 QQ 的 `c2c_streaming` 字符串/布尔、`group_context.enabled` 补默认、Discord/Telegram 的 ID 串拆分与去重）。白名单类字段统一用 `channels::string_or_vec`，既能吃 JSON 数组，也能吃 `a,b c` 这样的串。
7. **两类入站共用一个出口**：无论通道自己建连，还是由 webhook 服务代收，最终都调用同一个 `EnqueueFn`。因此 webhook 通道不需要任何特殊管线逻辑，`start()` 只做凭据预校验、`stop()` 为空。
8. **去重位置**：`parse_inbound(&self, …)` 是只读方法，无法调用 `&mut self` 的 `ChannelCore::is_duplicate`，所以 webhook 适配器各自持有一个 `Mutex<LruSet>`（上限 2000）按事件 id 去重——与长连接通道自持去重缓存的做法一致。
9. **必须声明的能力差异**：Google Chat 只校验 JWT 的 **claims**（`iss`/`exp`/`aud`），**不校验 RS256 签名**（本 build 未链接 RSA 库），请在上游入口终止签名校验；`service_account_json` 路径同样需要 RSA 签名器，当前支持的是预先生成的静态 `access_token`。LINE 的图片/视频消息只接受**公开 HTTPS URL**，本地字节无法上传，故退化为文本标记。

---

## 5. 验证

- `cargo check --all-targets` → **0 error / 0 warning**
- 测试 11 个目标 → **195 passed**
  （lib 44、channel_config 28、channel_pipeline 17、constraints 27、group_context 8、manager 20、media 4、models 17、push_routing 3、utils 12、webhook 15）
  - `tests/webhook.rs` 是端到端测试：把 `WebhookServer` 的 router 绑到临时端口，用**真实签名请求**验证路由（404/405）、握手（WhatsApp hub 挑战）与验签（LINE 接受/拒绝、WhatsApp 接受）。
- QQ bot 端到端：`QQ gateway connecting: wss://api.sgroup.qq.com/websocket` → `QQ gateway READY`
- Slack 端到端：`Slack gateway connecting: wss://wss-primary.slack.com/link/?<redacted>` → `Slack gateway READY`

---

## 6. 示例

| 示例 | 说明 |
|---|---|
| `slack_bot` | Socket Mode 收 + Web API 发（echo） |
| `webhook_server` | 一个端口挂载 WhatsApp / LINE / 微信公众号 / Google Chat，按配置的凭据自动启用 |
| 其余 11 个 | 每个长连接通道一个，另有 `all_channels` 与 `loopback_echo`（全流程闭环，无需网络） |

