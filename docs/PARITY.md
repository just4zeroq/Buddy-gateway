# buddy-gateway × octop-gateway 复刻对照清单（v3 — 全量对齐）

> 目标：Python 原版（`D:\code\github\octop-gateway`）的**每个渠道、每项功能**在 Rust 版（`D:\code\buddy-gateway`）1:1 落位。
> 图例：🟢 已复刻 · 🟡 表述差异（Rust 惯用法映射，能力不缺失） · ❌ 缺失
> 规模：Python ~14,878 行 / 43 文件；Rust ~16,000+ 行 / 22 源文件（+ 13 examples）。

---

## 0. 结论速览（v3）

**10 个渠道 + 核心层 + 管理器已全部对齐，无功能缺口。** 每个通道都能**真实建连收消息**，具备**入站解析（含富媒体）/ 出站发送（含平台媒体上传）/ 覆写钩子（含 `process_inbound` 流式）/ 配置字段别名与 `from_dict` 语义**。

- **核心层**：1:1（含 `default_constraints` 与 config 相关的 `configure_constraints` 拆分、`process_inbound` / `default_process_inbound_body` 拆分以支持 `super()._process_inbound`、`load_media_bytes` URL 缓存、`label_with_filename`）。
- **管理器**：1:1（含 `on_pre_lock`、`control_message_predicate`、`interrupt_message_predicate`、`get_channel`、`ChannelOptions`、`probe_channel` 的 yuanbao 特例、drain 放回）。
- **配置归一化**：`normalize_config` 覆盖全部 Python `field_aliases` + 各通道 `from_dict` 强制（QQ `c2c_streaming`/`group_context.enabled`、Discord/Telegram ID 串拆分去重、group_context 字符串数值强制）。
- **WS 代理**：新增 `utils::ws_connect_proxied`（HTTP CONNECT 隧道 + rustls），Discord 网关按 `http_proxy` 走代理（对齐 discord.py）。
- **验证**：`cargo check --all-targets` **0 error / 0 warning**；`cargo test --lib --tests` **137 passed**；QQ bot 实测 `QQ gateway connecting → QQ gateway READY`。

---

## 1. 核心层对照

| 功能项 | Python 位置 | Rust 现状 |
|---|---|---|
| models / constraints / group_context / media / push_routing / utils | — | 🟢 1:1 |
| `handle_inbound` 模板 + `_process_inbound` 默认缓冲 + `_clean_output` | channel.py | 🟢 |
| `_process_inbound` 可被覆写（`super()._process_inbound` 语义） | channel.py | 🟢 `process_inbound` + `default_process_inbound_body` |
| `_default_constraints(self)`（可读 config） | channel.py:267 | 🟢 `default_constraints()`（静态）+ `configure_constraints(&self)`（config 相关） |
| `load_media_bytes` URL 分支缓存 + `_derive_storage_key` | channel.py:491-549,614-623 | 🟢 |
| `ChannelConfig.from_dict`（别名/strip/忽略未知/group_context） | channel.py:96-136 | 🟢 `normalize_config`（见 §4） |
| `_get_media_label` File 带文件名 | channel.py:655-666 | 🟢 `label_with_filename()` |
| `set_constraints` 任意字段 | channel.py | 🟢 覆盖全部 ChannelConstraints 字段 |
| rustls provider 安装（WSS 必需） | — | 🟢 `utils::ensure_crypto_provider()`（`ChannelCore::new` 内一次性安装） |

---

## 2. 逐渠道对照（全部 🟢）

| 渠道 | 传输层 | 入站（含富媒体） | 出站（含平台媒体） | 覆写钩子 | 子模块 |
|---|---|---|---|---|---|
| **Discord** | gateway WS(op10/2/6/1/7/9) + **代理** | 门控/白名单/去重1000/附件→Image/Audio/Video/File | `split_discord_text`(2000 围栏感知) + multipart + typing | `default_constraints`(typing 8s)/`send_typing_indicator`/`fetch_remote_media`(25MiB) | 内联 |
| **Telegram** | `getUpdates` 长轮询 | text/caption/photo[-1]/doc/video/voice/audio(`getFile`) | `markdown_to_telegram_html` + 4000 分块 + HTML→纯文本回退 + multipart | `default_constraints`((20,60),typing 依 `show_typing`)/`send_typing_indicator` | `format_html` 内联 |
| **Feishu** | tenant_access_token + WS + watchdog | `im.message.receive_v1`（6 类消息、mentions、dedup2000） | post(markdown) + `/im/v1/images`、`/im/v1/files` + reply_in_thread | `fetch_remote_media`(Bearer)/`enrich_push_metadata` | 内联 |
| **DingTalk** | OAuth2 + Stream WS | text/richText/picture/file/audio | sessionWebhook / `oToMessages/batchSend`（>3500 文本否则 markdown）+ `/media/upload` | `fetch_remote_media`(downloadCode) | 内联 |
| **QQ** | `bots.qq.com` token + `/gateway/bot` + WS opcode | 6 类事件 + 富媒体标签(`qqimg/qqvideo/qqaudio/qqfile`)+attachments+引用+群上下文 | markdown 回退 + 媒体三级上传 + **C2C `stream_messages` 流式** + `msg_seq` + 401 重试 | `process_inbound`(C2C 流式)/`send_typing_indicator`(input_notify)/`default_constraints`/`fetch_remote_media`/`enrich_push_metadata` | `stream`/`stream_blocks`/`login_qr` 内联 |
| **WeCom** | WS(bot_id+secret 认证/心跳 30s/无限重连) | msgtype 白名单 + `aeskey` + 媒体解密下载 | `reply_stream`→`response_url` + `upload_media`/reply_media | `handle_inbound`(流式)/`default_constraints`/`enrich_push_metadata` | 内联 |
| **Weixin** | 多账号 `getUpdates` 长轮询(`X-WECHAT-UIN`,`-14` 暂停 300s) | message_type==1 + item_list + **CDN AES-128-ECB 解密** | `split_weixin_text`(2000)+`context_token` + CDN AES 加密上传 + `send_items` + typing ticket | **`process_inbound`(typing 灯 + 4s 占位气泡 + status=2 收尾)**/`preprocess_inbound`/`send_typing_indicator`/`default_constraints`/`fetch_remote_media`/`enrich_push_metadata` | `api`/`media`/`text`/`types`/`login_qr` 内联 |
| **Yuanbao** | sign-token(HMAC-SHA256) + 二进制 WS + auth-bind | BINARY `CMD_INBOUND_MESSAGE` → TIM 元素（dedup TTL 300） | protobuf ConnMsg（手写）+ send_c2c/group + **COS 直传**(HMAC-SHA1) + heartbeat + typing | `default_constraints`(typing 2.0,(2,5))/`send_typing_indicator`/`fetch_remote_media` | `constants`/`utils`/`protocol` 内联 |
| **XiaoYi** | 主备 WS + HMAC-SHA256 签名头 | A2A `message/stream` + parts(text/file→image/file) | A2A artifact(json-rpc artifact-update) + status-update final + reasoning chunk | `process_inbound`(A2A 流式)/`default_constraints`((20,60)) | `auth`/`constants` 内联 |
| **MQTT** | rumqttc + 订阅 + 重连 + **TLS(自定义 CA/客户端证书)** | Publish → JSON/纯文本 → enqueue | `publish_topic.format({client_id})` + 媒体文本回退 | `default_constraints`((30,60)) | 内联 |

---

## 3. 管理器对照（全部 🟢）

| 功能项 | Rust 现状 |
|---|---|
| 队列 / worker 池 / 会话锁 / 批处理 / 合并 | 🟢 |
| drain 遇不同 session / control 消息放回队列 | 🟢 |
| `on_pre_lock`（`set_pre_lock_handler` / `with_on_pre_lock`） | 🟢 会话锁前调用 |
| `control_message_predicate`（不批处理 + drain 放回） | 🟢 |
| `interrupt_message_predicate`（旁路会话锁，立即处理） | 🟢 `enqueue` 内 `_run_interrupt` 等价 |
| `probe_channel`（含 yuanbao `probe_mode="sign_token"`） | 🟢 |
| `list_subjects` / `list_all_users` | 🟢 |
| `get_channel` | 🟢 公开 |
| `add_channel` / `add_*_channel(..., processor=, **kwargs)` | 🟢 `ChannelOptions { processor, channel_id, tenant_id, debounce_seconds, constraints }` |
| `run_in_session` 校验 | 🟢 |
| 全局 `set_constraints` 任意字段 | 🟢 |

---

## 4. 表述差异（Rust 惯用法映射，能力不缺失）

1. **`**kwargs` → `ChannelOptions`**：Python 的动态关键字参数在 Rust 用 `ChannelOptions` 结构体表达（`add_channel_full` / `add_*_channel(config, opts)`）。能力完全等价。
2. **`_default_constraints(self)` → 两个钩子**：静态 `default_constraints()` + config 相关的 `configure_constraints(&self, &mut ChannelConstraints)`。取值与 Python 逐项一致（如 Telegram `show_typing` 决定 typing 间隔）。
3. **`super()._process_inbound()` → `default_process_inbound_body()`**：Python 覆写时调用父类实现；Rust 因 `&mut self` 无法并发借用（typing ticker + 管线需同时持锁），WeCom/QQ/Weixin 在各自覆写中内联等价的 select 循环（如 QQ 的 `base_process_inbound`、Weixin 的 typing/placeholder 循环）。**行为对齐**。
4. **去重位置**：Python 去重是**每通道**自行维护（`self._seen` / `_is_duplicate`），Rust 各通道同样使用自有 LRU/TTL 缓存（非 `ChannelCore`），**完全一致**。
5. **`ensure_http()` 代理**：Python `BaseChannel._ensure_http` 也是无代理的 `aiohttp.ClientSession()`；需要代理的通道自建 client（Rust 同）。**一致**。
6. **WS 代理**：Python 各 SDK 的 WS 代理由其库实现；Rust 以 `ws_connect_proxied`（CONNECT 隧道）复刻，Discord 网关已接入。

> 以上均为「同一语义、不同实现形态」，非功能缺口。

## 5. 验证

- `cargo check --all-targets` → **0 error / 0 warning**。
- `cargo test --lib --tests` → **137 passed**
  （lib 2、channel_config 27、channel_pipeline 17、constraints 27、group_context 8、manager 20、media 4、models 17、push_routing 3、utils 12）。
- QQ bot 端到端：`QQ gateway connecting: wss://api.sgroup.qq.com/websocket` → `QQ gateway READY`。
