use std::{env, time::Duration};
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

pub mod observer { tonic::include_proto!("com.trendyol.stove.kafka"); }
#[cfg(feature = "rdkafka-adapter")]
pub mod rdkafka_adapter;
use observer::stove_kafka_observer_service_client::StoveKafkaObserverServiceClient;
pub use observer::{AcknowledgedMessage, CommittedMessage, ConsumedMessage, PublishedMessage};

#[derive(Clone)]
pub struct Bridge { client: Option<StoveKafkaObserverServiceClient<Channel>> }

impl Bridge {
  pub fn disabled() -> Self { Self { client: None } }
  /// Drop this handle after all Kafka activity has stopped. Other clones remain usable.
  pub fn close(self) { drop(self); }
  pub fn from_env() -> Self {
    let Some(port) = env::var("STOVE_KAFKA_BRIDGE_PORT").ok().filter(|p| !p.is_empty()) else { return Self::disabled(); };
    let host = env::var("STOVE_KAFKA_BRIDGE_HOST").unwrap_or_else(|_| "localhost".into());
    Self::from_endpoint(format!("http://{host}:{port}"))
  }
  pub fn from_endpoint(endpoint: String) -> Self {
    let client = Endpoint::from_shared(endpoint)
      .ok().map(|endpoint| StoveKafkaObserverServiceClient::new(endpoint.connect_lazy()));
    Self { client }
  }
  pub async fn published(&self, mut msg: PublishedMessage) {
    msg.id = Uuid::new_v4().to_string();
    if let Some(mut client) = self.client.clone() {
      if let Err(e) = tokio::time::timeout(Duration::from_secs(1), client.on_published_message(msg)).await.unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("observer timeout"))) { eprintln!("Stove Kafka bridge published: {e}"); }
    }
  }
  pub async fn consumed(&self, mut msg: ConsumedMessage) {
    msg.id = Uuid::new_v4().to_string();
    if let Some(mut client) = self.client.clone() {
      if let Err(e) = tokio::time::timeout(Duration::from_secs(1), client.on_consumed_message(msg)).await.unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("observer timeout"))) { eprintln!("Stove Kafka bridge consumed: {e}"); }
    }
  }
  pub async fn committed(&self, mut msg: CommittedMessage) {
    msg.id = Uuid::new_v4().to_string();
    if let Some(mut client) = self.client.clone() {
      if let Err(e) = tokio::time::timeout(Duration::from_secs(1), client.on_committed_message(msg)).await.unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("observer timeout"))) { eprintln!("Stove Kafka bridge committed: {e}"); }
    }
  }
  pub async fn acknowledged(&self, mut msg: AcknowledgedMessage) {
    msg.id = Uuid::new_v4().to_string();
    if let Some(mut client) = self.client.clone() {
      if let Err(e) = tokio::time::timeout(Duration::from_secs(1), client.on_acknowledged_message(msg)).await.unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("observer timeout"))) { eprintln!("Stove Kafka bridge acknowledged: {e}"); }
    }
  }
}
