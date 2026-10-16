//! MQTT channel for IoT devices and robots.
//!
//! Mirrors `octop_gateway.channels.mqtt`. Uses `rumqttc` async client. Inbound
//! messages on `subscribe_topic` are parsed and enqueued; outbound text is
//! published to `publish_topic` (with `{client_id}` substituted).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::task::JoinHandle;

use crate::channel::{Channel, ChannelConfig, EnqueueFn};
use crate::errors::{GatewayError, Result};
use crate::models::{ChannelSubject, ContentPart, InboundMessage, RawPayload};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MQTTConfig {
    #[serde(flatten)]
    pub base: ChannelConfig,
    pub host: String,
    pub port: u16,
    pub transport: String,
    pub username: String,
    pub password: String,
    pub subscribe_topic: String,
    pub publish_topic: String,
    pub clean_session: bool,
    pub qos: u8,
    pub tls_enabled: bool,
    pub tls_ca_pem: String,
    pub tls_ca_certs: String,
    pub tls_certfile: String,
    pub tls_keyfile: String,
}

impl Default for MQTTConfig {
    /// Mirrors the Python `MQTTConfig` dataclass defaults
    /// (port 8883, TCP transport, TLS on, QoS 2, sample topics).
    fn default() -> Self {
        MQTTConfig {
            base: ChannelConfig::default(),
            host: String::new(),
            port: 8883,
            transport: "tcp".to_string(),
            username: String::new(),
            password: String::new(),
            subscribe_topic: "devices/+/in".to_string(),
            publish_topic: "devices/{client_id}/out".to_string(),
            clean_session: true,
            qos: 2,
            tls_enabled: true,
            tls_ca_pem: String::new(),
            tls_ca_certs: String::new(),
            tls_certfile: String::new(),
            tls_keyfile: String::new(),
        }
    }
}

crate::channel_adapter! {
    /// MQTT channel.
    MQTTChannel, MQTTConfig, "mqtt",
    required = [host],
    fields = {
        client: Option<rumqttc::AsyncClient>,
        task: Option<JoinHandle<()>>,
        enqueue_cb: Option<EnqueueFn>,
    }
}

#[async_trait]
impl Channel for MQTTChannel {
    fn core(&self) -> &crate::channel::ChannelCore {
        &self.core
    }
    fn core_mut(&mut self) -> &mut crate::channel::ChannelCore {
        &mut self.core
    }
    fn default_constraints() -> crate::constraints::ChannelConstraints {
        crate::constraints::ChannelConstraints {
            send_rate_limit: Some((30, 60.0)),
            show_thinking: false,
            show_tool_hints: true,
            ..Default::default()
        }
    }
    async fn start(&mut self) -> Result<()> {
        if self.config.host.is_empty() {
            return Err(GatewayError::Other("MQTTChannel: host is required".into()));
        }
        if self.config.subscribe_topic.is_empty() || self.config.publish_topic.is_empty() {
            return Err(GatewayError::Other(
                "MQTTChannel: subscribe_topic and publish_topic are required".into(),
            ));
        }

        let client_id = format!("harness-mqtt-{}", &uuid::Uuid::new_v4().to_string()[..12]);
        self.core.set_connection_session(Some(client_id.clone()));

        let mut opts = rumqttc::MqttOptions::new(
            client_id.clone(),
            self.config.host.clone(),
            self.config.port,
        );
        opts.set_clean_session(self.config.clean_session);
        if !self.config.username.is_empty() {
            opts.set_credentials(self.config.username.clone(), self.config.password.clone());
        }
        if self.config.tls_enabled {
            // Python builds an SSLContext honoring tls_ca_pem > tls_ca_certs and
            // optional client cert/key. Reproduce that selection here.
            let ca = if !self.config.tls_ca_pem.is_empty() {
                self.config.tls_ca_pem.as_bytes().to_vec()
            } else if !self.config.tls_ca_certs.is_empty() {
                std::fs::read(&self.config.tls_ca_certs).unwrap_or_default()
            } else {
                Vec::new()
            };
            let client_auth = if !self.config.tls_certfile.is_empty() {
                let cert = std::fs::read(&self.config.tls_certfile).unwrap_or_default();
                let key = if self.config.tls_keyfile.is_empty() {
                    Vec::new()
                } else {
                    std::fs::read(&self.config.tls_keyfile).unwrap_or_default()
                };
                Some((cert, key))
            } else {
                None
            };
            if ca.is_empty() && client_auth.is_none() {
                opts.set_transport(rumqttc::Transport::tls_with_default_config());
            } else {
                opts.set_transport(rumqttc::Transport::tls(ca, client_auth, None));
            }
        } else if self.config.transport == "websockets" {
            opts.set_transport(rumqttc::Transport::Ws);
        }

        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 10);
        let subscribe_topic = self.config.subscribe_topic.clone();
        let qos = match self.config.qos {
            0 => rumqttc::QoS::AtMostOnce,
            1 => rumqttc::QoS::AtLeastOnce,
            _ => rumqttc::QoS::ExactlyOnce,
        };
        let _ = client.subscribe(subscribe_topic.clone(), qos).await;

        let cb = self.core.enqueue_sender();
        self.enqueue_cb = cb.clone();
        self.client = Some(client.clone());

        let task = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                        let topic = p.topic.clone();
                        let payload = String::from_utf8_lossy(&p.payload).to_string();
                        if let Some(cb) = &cb {
                            if let Some(native) = Self::parse_native_static(&payload, &topic) {
                                cb(RawPayload::Json(serde_json::Value::Object(
                                    native
                                        .into_iter()
                                        .map(|(k, v)| (k, serde_json::Value::String(v)))
                                        .collect(),
                                )));
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("MQTT event loop error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
        self.task = Some(task);
        tracing::info!("MQTTChannel started (host={})", self.config.host);
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.client = None;
        self.core.set_connection_session(None);
        tracing::info!("MQTTChannel stopped");
        Ok(())
    }

    async fn send_text(&mut self, subject: &ChannelSubject, text: &str) -> Result<()> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| GatewayError::Other("MQTTChannel: not connected".into()))?;
        let client_id = subject
            .metadata
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.subject_id.clone());
        let topic = self.config.publish_topic.replace("{client_id}", &client_id);
        let qos = match self.config.qos {
            0 => rumqttc::QoS::AtMostOnce,
            1 => rumqttc::QoS::AtLeastOnce,
            _ => rumqttc::QoS::ExactlyOnce,
        };
        client
            .publish(topic, qos, false, text.as_bytes().to_vec())
            .await
            .map_err(|e| GatewayError::Other(format!("MQTT publish failed: {}", e)))?;
        Ok(())
    }

    async fn send_media(&mut self, subject: &ChannelSubject, media: &ContentPart) -> Result<()> {
        let label = media.label_with_filename();
        if let Some(url) = media.url() {
            if !url.is_empty() {
                return self.send_text(subject, &format!("[{}: {}]", label, url)).await;
            }
        }
        self.send_text(subject, &format!("[{} (local file, not deliverable on MQTT)]", label))
            .await
    }

    fn parse_inbound(&self, raw: &RawPayload) -> Result<InboundMessage> {
        if let RawPayload::Preparsed(m) = raw {
            return Ok(m.clone());
        }
        let v = raw
            .as_json()
            .ok_or_else(|| GatewayError::Other("mqtt: expected JSON payload".into()))?;

        let client_id = v
            .get("client_id")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown-client")
            .to_string();
        let text = v
            .get("text")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let topic = v
            .get("topic")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();

        let mut metadata = HashMap::new();
        metadata.insert("client_id".to_string(), serde_json::Value::String(client_id.clone()));
        metadata.insert("topic".to_string(), serde_json::Value::String(topic.clone()));
        metadata.insert(
            "raw_payload".to_string(),
            v.get("raw_payload").cloned().unwrap_or(serde_json::Value::String(String::new())),
        );

        let content = vec![crate::models::ContentPart::text(text)];
        Ok(InboundMessage {
            channel_id: self.core.channel_id().to_string(),
            channel_type: "mqtt".to_string(),
            tenant_id: self.core.tenant_id().map(|s| s.to_string()),
            channel_subject: Some(ChannelSubject {
                subject_id: client_id.clone(),
                chat_type: "direct".to_string(),
                metadata: metadata.clone(),
                display_name: String::new(),
                first_seen: None,
                last_seen: None,
            }),
            channel_session_id: self
                .core
                .connection_session()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}-unconnected", self.core.channel_id())),
            content,
            metadata,
            timestamp: crate::channel::now_f64(),
            group_context: None,
        })
    }
}

impl MQTTChannel {
    /// Static helper used by the background task (no `self` required).
    fn parse_native_static(payload: &str, topic: &str) -> Option<HashMap<String, String>> {
        let parsed: serde_json::Value = serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
        let content = if parsed.is_object() {
            parsed.get("text").and_then(|x| x.as_str()).unwrap_or("").to_string()
        } else {
            payload.to_string()
        };
        if content.is_empty() {
            return None;
        }
        let mut client_id = parsed
            .get("redirect_client_id")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        if client_id.is_none() {
            let parts: Vec<&str> = topic.split('/').collect();
            if parts.len() >= 2 {
                client_id = Some(parts[1].to_string());
            }
        }
        let client_id = client_id.unwrap_or_else(|| "unknown-client".to_string());
        let mut native = HashMap::new();
        native.insert("topic".to_string(), topic.to_string());
        native.insert("client_id".to_string(), client_id);
        native.insert("text".to_string(), content);
        native.insert("raw_payload".to_string(), payload.to_string());
        Some(native)
    }
}
