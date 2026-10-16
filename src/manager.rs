//! Channel manager: orchestrates multiple channels with session-aware batching.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::channel::{enqueue_callback, Channel, MessageProcessor};
use crate::channels;
use crate::errors::{GatewayError, Result};
use crate::media::{DynMediaBackend, FileSystemMediaBackend};
use crate::models::{ChannelSubject, ContentPart, InboundMessage, RawPayload};

type SessionLocks = StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>;

/// Host hook invoked before the session lock is taken.
/// Used to signal cancellation for `/stop` before the in-flight
/// turn releases the lock).
pub type PreLockHandler = Arc<
    dyn Fn(String, InboundMessage) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

/// Predicate over a parsed inbound message.
/// Used for control / interrupt classification.
pub type MessagePredicate = Arc<dyn Fn(&InboundMessage) -> bool + Send + Sync>;

/// Optional construction options for [`ChannelManager::add_channel_full`] and the
/// `add_*_channel` convenience methods. Carries the optional per-channel
/// `channel_id=` / `tenant_id=` / `**kwargs` (e.g. `debounce_seconds`,
/// `constraints`) forwarded to the channel constructor.
#[derive(Clone, Default)]
pub struct ChannelOptions {
    /// Processor override; falls back to the manager default.
    pub processor: Option<MessageProcessor>,
    /// Explicit channel id (overrides the config's `channel_id`).
    pub channel_id: Option<String>,
    /// Tenant id (overrides the config's `tenant_id`).
    pub tenant_id: Option<String>,
    /// Debounce window forwarded to the channel constructor.
    pub debounce_seconds: f64,
    /// Constraint overrides applied to the new channel (`set_constraints`).
    pub constraints: Option<HashMap<String, Value>>,
}

impl ChannelOptions {
    pub fn new() -> Self {
        ChannelOptions::default()
    }
}

/// Orchestrates multiple IM channels: per-channel queues, worker pools, session
/// locks, proactive push, and dynamic channel registration.
pub struct ChannelManager {
    channels: HashMap<String, Arc<AsyncMutex<Box<dyn Channel>>>>,
    workers_per_channel: usize,
    queue_maxsize: usize,
    media_backend: Option<DynMediaBackend>,
    global_constraints: HashMap<String, Value>,
    default_processor: Option<MessageProcessor>,
    on_pre_lock: Option<PreLockHandler>,
    control_message_predicate: Option<MessagePredicate>,
    interrupt_message_predicate: Option<MessagePredicate>,
    queues: HashMap<String, mpsc::Sender<RawPayload>>,
    receivers: HashMap<String, mpsc::Receiver<RawPayload>>,
    workers: Vec<JoinHandle<()>>,
    interrupt_tasks: Arc<StdMutex<Vec<JoinHandle<()>>>>,
    session_locks: Arc<SessionLocks>,
    running: bool,
}

impl ChannelManager {
    pub fn new(default_processor: Option<MessageProcessor>) -> Self {
        ChannelManager {
            channels: HashMap::new(),
            workers_per_channel: 4,
            queue_maxsize: 1000,
            media_backend: None,
            global_constraints: HashMap::new(),
            default_processor,
            on_pre_lock: None,
            control_message_predicate: None,
            interrupt_message_predicate: None,
            queues: HashMap::new(),
            receivers: HashMap::new(),
            workers: Vec::new(),
            interrupt_tasks: Arc::new(StdMutex::new(Vec::new())),
            session_locks: Arc::new(StdMutex::new(HashMap::new())),
            running: false,
        }
    }

    pub fn with_workers_per_channel(mut self, n: usize) -> Self {
        self.workers_per_channel = n;
        self
    }

    pub fn with_queue_maxsize(mut self, n: usize) -> Self {
        self.queue_maxsize = n;
        self
    }

    pub fn with_media_backend(mut self, backend: DynMediaBackend) -> Self {
        self.media_backend = Some(backend);
        self
    }

    /// Install or clear the pre-session-lock inbound hook
    /// `ChannelManager.set_pre_lock_handler`).
    pub fn set_pre_lock_handler(&mut self, handler: Option<PreLockHandler>) {
        self.on_pre_lock = handler;
    }

    pub fn with_on_pre_lock(mut self, handler: PreLockHandler) -> Self {
        self.on_pre_lock = Some(handler);
        self
    }

    pub fn with_control_message_predicate(mut self, predicate: MessagePredicate) -> Self {
        self.control_message_predicate = Some(predicate);
        self
    }

    pub fn with_interrupt_message_predicate(mut self, predicate: MessagePredicate) -> Self {
        self.interrupt_message_predicate = Some(predicate);
        self
    }

    pub fn set_default_processor(&mut self, processor: MessageProcessor) {
        self.default_processor = Some(processor);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub async fn start(&mut self) -> Result<()> {
        if self.running {
            return Ok(());
        }
        self.running = true;

        if self.media_backend.is_none() {
            self.media_backend = Some(Arc::new(FileSystemMediaBackend::new("/")));
        }
        let backend = self.media_backend.clone().unwrap();

        let entries: Vec<(String, Arc<AsyncMutex<Box<dyn Channel>>>)> = self
            .channels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (channel_id, ch) in entries {
            {
                let mut guard = ch.lock().await;
                guard.set_media_backend(backend.clone());
                if !self.global_constraints.is_empty() {
                    guard.set_constraints(&self.global_constraints);
                }
            }
            let tx = self
                .queues
                .get(&channel_id)
                .cloned()
                .expect("queue must exist");
            {
                let mut guard = ch.lock().await;
                guard.set_enqueue_callback(enqueue_callback(tx.clone()));
            }
            // Start the channel.
            {
                let mut guard = ch.lock().await;
                if let Err(e) = guard.start().await {
                    tracing::error!("Failed to start channel {}: {}", channel_id, e);
                } else {
                    tracing::info!("Channel started: {}", channel_id);
                }
            }
            self.spawn_workers(&channel_id, ch.clone());
        }
        Ok(())
    }

    pub async fn stop(&mut self) {
        if !self.running {
            return;
        }
        self.running = false;
        for task in self.workers.drain(..) {
            task.abort();
        }
        {
            let mut tasks = self.interrupt_tasks.lock().unwrap();
            for t in tasks.drain(..) {
                t.abort();
            }
        }
        for (_id, ch) in self.channels.iter() {
            let mut guard = ch.lock().await;
            if let Err(e) = guard.stop().await {
                tracing::warn!("Error stopping channel: {}", e);
            }
        }
        self.queues.clear();
        self.session_locks.lock().unwrap().clear();
        tracing::info!("ChannelManager stopped");
    }

    fn spawn_workers(&mut self, channel_id: &str, ch: Arc<AsyncMutex<Box<dyn Channel>>>) {
        let rx = self
            .receivers
            .remove(channel_id)
            .expect("receiver must exist");
        let shared_rx = Arc::new(AsyncMutex::new(rx));
        let tx = self
            .queues
            .get(channel_id)
            .cloned()
            .expect("queue must exist");
        let session_locks = self.session_locks.clone();
        let running = Arc::new(AtomicBool::new(true));
        let control_pred = self.control_message_predicate.clone();
        let pre_lock = self.on_pre_lock.clone();
        for _ in 0..self.workers_per_channel {
            let ch = ch.clone();
            let shared_rx = shared_rx.clone();
            let tx = tx.clone();
            let session_locks = session_locks.clone();
            let running = running.clone();
            let control_pred = control_pred.clone();
            let pre_lock = pre_lock.clone();
            let cid = channel_id.to_string();
            let task = tokio::spawn(async move {
                worker_loop(
                    ch,
                    shared_rx,
                    session_locks,
                    running,
                    cid,
                    tx,
                    control_pred,
                    pre_lock,
                )
                .await;
            });
            self.workers.push(task);
        }
    }

    // -----------------------------------------------------------------------
    // Constraints
    // -----------------------------------------------------------------------

    pub async fn set_constraints(&mut self, overrides: HashMap<String, Value>) {
        self.global_constraints.extend(overrides.clone());
        for (_id, ch) in self.channels.iter() {
            let mut guard = ch.lock().await;
            guard.set_constraints(&overrides);
        }
    }

    // -----------------------------------------------------------------------
    // Enqueue (thread-safe)
    // -----------------------------------------------------------------------

    pub fn enqueue(&self, channel_id: &str, payload: RawPayload) {
        let tx = match self.queues.get(channel_id) {
            Some(tx) => tx.clone(),
            None => {
                tracing::warn!("No queue for channel {}; dropping payload", channel_id);
                return;
            }
        };

        // Interrupt path : urgent control
        // messages bypass the session lock and are handled immediately.
        if let Some(pred) = self.interrupt_message_predicate.clone() {
            if let Some(ch) = self.channels.get(channel_id).cloned() {
                let parsed = match ch.try_lock() {
                    Ok(guard) => guard.parse_inbound(&payload).ok(),
                    Err(_) => None,
                };
                if let Some(message) = parsed {
                    if pred(&message) {
                        let cid = channel_id.to_string();
                        let handle = tokio::spawn(async move {
                            let mut g = ch.lock().await;
                            if let Err(e) =
                                g.handle_inbound(&RawPayload::Preparsed(message)).await
                            {
                                tracing::error!("Interrupt worker error: channel={} {}", cid, e);
                            }
                        });
                        self.interrupt_tasks.lock().unwrap().push(handle);
                        return;
                    }
                    // Reuse the parsed message (enqueues the parsed object).
                    if tx.try_send(RawPayload::Preparsed(message)).is_err() {
                        tracing::error!("Queue full/closed for {}; dropping payload", channel_id);
                    }
                    return;
                }
            }
        }

        if tx.try_send(payload).is_err() {
            tracing::error!("Queue full or closed for {}; dropping payload", channel_id);
        }
    }

    // -----------------------------------------------------------------------
    // Proactive push API
    // -----------------------------------------------------------------------

    pub async fn push_text(
        &self,
        channel_id: &str,
        subject: &ChannelSubject,
        text: &str,
    ) -> Result<()> {
        let ch = self.channel_arc(channel_id)?;
        let mut guard = ch.lock().await;
        guard.push_text(subject, text).await
    }

    pub async fn push_content(
        &self,
        channel_id: &str,
        subject: &ChannelSubject,
        parts: &[ContentPart],
    ) -> Result<()> {
        let ch = self.channel_arc(channel_id)?;
        let mut guard = ch.lock().await;
        guard.push_message(subject, parts).await
    }

    pub async fn push_to_all(&self, channel_id: &str, text: &str) -> Result<()> {
        let ch = self.channel_arc(channel_id)?;
        let subjects = {
            let guard = ch.lock().await;
            guard.list_subjects()
        };
        let mut guard = ch.lock().await;
        for subject in subjects {
            if let Err(e) = guard.push_text(&subject, text).await {
                tracing::warn!("Failed to push to {}: {}", subject.subject_id, e);
            }
        }
        Ok(())
    }

    /// Known subjects for a specific channel.
    pub async fn list_subjects(&self, channel_id: &str) -> Vec<ChannelSubject> {
        match self.channels.get(channel_id) {
            Some(ch) => ch.lock().await.list_subjects(),
            None => Vec::new(),
        }
    }

    /// Known subjects across all channels.
    pub async fn list_all_users(&self) -> HashMap<String, Vec<ChannelSubject>> {
        let mut out = HashMap::new();
        for (cid, ch) in self.channels.iter() {
            let subjects = ch.lock().await.list_subjects();
            out.insert(cid.clone(), subjects);
        }
        out
    }

    pub async fn run_in_session<T, F>(&self, channel_id: &str, session_key: &str, op: F) -> Result<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if !self.channels.contains_key(channel_id) {
            return Err(GatewayError::ChannelNotFound(channel_id.to_string()));
        }
        if session_key.trim().is_empty() {
            return Err(GatewayError::Other(
                "session_key must not be empty".to_string(),
            ));
        }
        let lock_key = format!("{}:{}", channel_id, session_key);
        let slock = {
            let mut map = self.session_locks.lock().unwrap();
            map.entry(lock_key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let _g = slock.lock().await;
        Ok(op.await)
    }

    // -----------------------------------------------------------------------
    // Channel management
    // -----------------------------------------------------------------------

    /// Get a channel handle by id.
    pub fn get_channel(
        &self,
        channel_id: &str,
    ) -> Option<Arc<AsyncMutex<Box<dyn Channel>>>> {
        self.channels.get(channel_id).cloned()
    }

    fn channel_arc(&self, channel_id: &str) -> Result<Arc<AsyncMutex<Box<dyn Channel>>>> {
        self.get_channel(channel_id)
            .ok_or_else(|| GatewayError::ChannelNotFound(channel_id.to_string()))
    }

    pub fn channel_ids(&self) -> Vec<String> {
        self.channels.keys().cloned().collect()
    }

    /// Add a pre-built channel object (legacy path).
    pub async fn add_channel_object(&mut self, channel: Box<dyn Channel>) -> Result<String> {
        self.register(channel).await
    }

    /// Add a channel from a kind string + JSON config, with full options
    /// ` where kwargs = `debounce_seconds` / `constraints`).
    pub async fn add_channel_full(
        &mut self,
        channel_type: &str,
        config: Value,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = opts
            .processor
            .clone()
            .or_else(|| self.default_processor.clone())
            .ok_or(GatewayError::NoProcessor)?;
        let mut channel = channels::build_channel(
            channel_type,
            &config,
            processor,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
        )?;
        if opts.debounce_seconds > 0.0 {
            channel.core_mut().set_debounce_seconds(opts.debounce_seconds);
        }
        if let Some(c) = &opts.constraints {
            channel.set_constraints(c);
        }
        self.register(channel).await
    }

    /// Add a channel dynamically from a kind string + JSON config.
    pub async fn add_channel(
        &mut self,
        channel_type: &str,
        config: Value,
        channel_id: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.add_channel_full(
            channel_type,
            config,
            ChannelOptions {
                channel_id,
                tenant_id,
                ..Default::default()
            },
        )
        .await
    }

    /// Start/stop an ephemeral channel to verify credentials, without
    /// registering it . Credentials are
    /// validated by `build_channel` before any connection attempt.
    pub async fn probe_channel(
        &mut self,
        channel_type: &str,
        config: Value,
        channel_id: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<()> {
        let processor = self
            .default_processor
            .clone()
            .ok_or(GatewayError::NoProcessor)?;

        // Yuanbao appears to allow only one effective bot WebSocket session, so
        // a UI probe uses sign-token only.
        let mut config = config;
        if channel_type == "yuanbao" {
            if let Some(obj) = config.as_object_mut() {
                if !obj.contains_key("probe_mode") {
                    obj.insert("probe_mode".to_string(), Value::String("sign_token".into()));
                }
            }
        }

        let mut channel = channels::build_channel(
            channel_type,
            &config,
            processor,
            channel_id.or_else(|| Some("__probe__".to_string())),
            tenant_id,
        )?;
        if let Some(b) = self.media_backend.clone() {
            channel.set_media_backend(b);
        }
        let start_result = channel.start().await;
        let _ = channel.stop().await;
        start_result
    }

    async fn register(&mut self, channel: Box<dyn Channel>) -> Result<String> {
        let channel_id = channel.channel_id();
        if self.channels.contains_key(&channel_id) {
            return Err(GatewayError::ChannelExists(channel_id.clone()));
        }
        let backend = self.media_backend.clone();
        let (tx, rx) = mpsc::channel::<RawPayload>(self.queue_maxsize.max(1));
        let arc: Arc<AsyncMutex<Box<dyn Channel>>> = Arc::new(AsyncMutex::new(channel));
        {
            let mut guard = arc.lock().await;
            if let Some(b) = backend {
                guard.set_media_backend(b);
            }
            if !self.global_constraints.is_empty() {
                guard.set_constraints(&self.global_constraints);
            }
            let _ = guard.set_enqueue_callback(enqueue_callback(tx.clone()));
        }
        self.channels.insert(channel_id.clone(), arc.clone());
        self.queues.insert(channel_id.clone(), tx);
        self.receivers.insert(channel_id.clone(), rx);

        if self.running {
            {
                let mut guard = arc.lock().await;
                if let Err(e) = guard.start().await {
                    self.channels.remove(&channel_id);
                    self.queues.remove(&channel_id);
                    self.receivers.remove(&channel_id);
                    return Err(e);
                }
            }
            self.spawn_workers(&channel_id, arc);
        }
        tracing::info!("Channel registered: {}", channel_id);
        Ok(channel_id)
    }

    pub async fn remove_channel(&mut self, channel_id: &str) -> Result<()> {
        let arc = self
            .channels
            .remove(channel_id)
            .ok_or_else(|| GatewayError::ChannelNotFound(channel_id.to_string()))?;
        self.queues.remove(channel_id);
        self.receivers.remove(channel_id);
        let mut guard = arc.lock().await;
        let _ = guard.stop().await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Convenience channel builders (the `add_*_channel` shorthands and
    // accept the same optional `opts` as processor=/**kwargs).
    // -----------------------------------------------------------------------

    fn resolve_processor(&self, opts: &ChannelOptions) -> Result<MessageProcessor> {
        opts.processor
            .clone()
            .or_else(|| self.default_processor.clone())
            .ok_or(GatewayError::NoProcessor)
    }

    fn finish(
        &self,
        opts: &ChannelOptions,
        mut channel: Box<dyn Channel>,
    ) -> Box<dyn Channel> {
        if opts.debounce_seconds > 0.0 {
            channel.core_mut().set_debounce_seconds(opts.debounce_seconds);
        }
        if let Some(c) = &opts.constraints {
            channel.set_constraints(c);
        }
        channel
    }

    pub async fn add_telegram_channel(
        &mut self,
        config: channels::telegram::TelegramConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::telegram::TelegramChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_mqtt_channel(
        &mut self,
        config: channels::mqtt::MQTTConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::mqtt::MQTTChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_feishu_channel(
        &mut self,
        config: channels::feishu::FeishuConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::feishu::FeishuChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_loopback_channel(
        &mut self,
        config: channels::loopback::LoopbackConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::loopback::LoopbackChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_wecom_channel(
        &mut self,
        config: channels::wecom::WeComConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::wecom::WeComChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_weixin_channel(
        &mut self,
        config: channels::weixin::WeixinConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::weixin::WeixinChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_dingtalk_channel(
        &mut self,
        config: channels::dingtalk::DingTalkConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::dingtalk::DingTalkChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_yuanbao_channel(
        &mut self,
        config: channels::yuanbao::YuanbaoConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::yuanbao::YuanbaoChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_xiaoyi_channel(
        &mut self,
        config: channels::xiaoyi::XiaoyiConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::xiaoyi::XiaoyiChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_qq_channel(
        &mut self,
        config: channels::qq::QQConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::qq::QQChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }

    pub async fn add_discord_channel(
        &mut self,
        config: channels::discord::DiscordConfig,
        opts: ChannelOptions,
    ) -> Result<String> {
        let processor = self.resolve_processor(&opts)?;
        let channel = Box::new(channels::discord::DiscordChannel::new(
            processor,
            config,
            opts.channel_id.clone(),
            opts.tenant_id.clone(),
            opts.debounce_seconds,
        ));
        let channel = self.finish(&opts, channel);
        self.register(channel).await
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    ch: Arc<AsyncMutex<Box<dyn Channel>>>,
    shared_rx: Arc<AsyncMutex<mpsc::Receiver<RawPayload>>>,
    session_locks: Arc<SessionLocks>,
    running: Arc<AtomicBool>,
    channel_id: String,
    tx: mpsc::Sender<RawPayload>,
    control_pred: Option<MessagePredicate>,
    pre_lock: Option<PreLockHandler>,
) {
    while running.load(Ordering::SeqCst) {
        // Hold the receiver lock while receiving + batching so a single worker
        // owns a given item; the channel lock then serializes processing.
        let mut rx_guard = shared_rx.lock().await;
        let payload = match rx_guard.recv().await {
            Some(p) => p,
            None => break,
        };
        let mut guard = ch.lock().await;
        let processed = process_one(
            &mut *guard,
            &mut rx_guard,
            &channel_id,
            &session_locks,
            payload,
            &tx,
            control_pred.as_ref(),
            pre_lock.as_ref(),
        )
        .await;
        if let Err(e) = processed {
            tracing::error!("Worker error on channel {}: {}", channel_id, e);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_one(
    guard: &mut Box<dyn Channel>,
    queue: &mut mpsc::Receiver<RawPayload>,
    channel_id: &str,
    session_locks: &SessionLocks,
    payload: RawPayload,
    tx: &mpsc::Sender<RawPayload>,
    control_pred: Option<&MessagePredicate>,
    pre_lock: Option<&PreLockHandler>,
) -> Result<()> {
    let message = guard.parse_inbound(&payload)?;
    let session_key = guard.get_debounce_key(&message);

    // Control messages never batch.
    let is_control = control_pred.map(|p| p(&message)).unwrap_or(false);

    // Drain additional same-session items (non-blocking).
    let mut batch = vec![message];
    if !is_control && guard.should_batch_inbound(&batch[0]) {
        loop {
            match queue.try_recv() {
                Ok(next) => match guard.parse_inbound(&next) {
                    Ok(m) => {
                        if control_pred.map(|p| p(&m)).unwrap_or(false) {
                            if tx.try_send(next).is_err() {
                                tracing::warn!("queue full re-enqueueing control payload");
                            }
                            break;
                        }
                        if guard.get_debounce_key(&m) == session_key {
                            batch.push(m);
                        } else {
                            // Different session — put it back so another worker
                            // handles it instead of dropping it.
                            if tx.try_send(next).is_err() {
                                tracing::warn!("queue full re-enqueueing different-session payload");
                            }
                            tracing::debug!("re-enqueued different-session payload");
                            break;
                        }
                    }
                    Err(_) => break,
                },
                Err(_) => break,
            }
        }
    }

    let merged = if batch.len() > 1 {
        guard.merge_inbound(&batch)?
    } else {
        batch[0].clone()
    };

    // Host hook before the session lock (e.g. signal cancel for /stop).
    if let Some(handler) = pre_lock {
        handler(channel_id.to_string(), merged.clone()).await;
    }

    let lock_key = format!("{}:{}", channel_id, session_key);
    let slock = {
        let mut map = session_locks.lock().unwrap();
        map.entry(lock_key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    };
    let _s = slock.lock().await;
    guard.handle_inbound(&RawPayload::Preparsed(merged)).await
}
