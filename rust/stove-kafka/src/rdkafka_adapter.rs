//! Kafka-specific reporting. The observer transport itself has no Kafka dependency.
use std::{collections::HashMap, time::Duration};
use rdkafka::{consumer::{CommitMode, Consumer}, error::KafkaError, message::{BorrowedMessage, Header, Headers, Message, OwnedHeaders}, producer::{FutureProducer, FutureRecord}};
use crate::{AcknowledgedMessage, Bridge, CommittedMessage, ConsumedMessage, PublishedMessage};

pub fn headers<M: Message>(message: &M) -> HashMap<String, String> {
  message.headers().map(|headers| (0..headers.count()).map(|i| {
    let header = headers.get(i);
    (header.key.to_owned(), String::from_utf8_lossy(header.value.unwrap_or_default()).into_owned())
  }).collect()).unwrap_or_default()
}

pub async fn received<M: Message>(bridge: &Bridge, message: &M) {
  bridge.consumed(ConsumedMessage {
    id: String::new(), topic: message.topic().into(), partition: message.partition(), offset: message.offset(),
    key: String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned(),
    message: message.payload().unwrap_or_default().to_vec(), headers: headers(message),
  }).await;
}

pub async fn commit<C: Consumer>(bridge: &Bridge, consumer: &C, message: &BorrowedMessage<'_>) -> Result<(), KafkaError> {
  consumer.commit_message(message, CommitMode::Sync)?;
  bridge.committed(CommittedMessage { id: String::new(), topic: message.topic().into(), partition: message.partition(), offset: message.offset() + 1, metadata: String::new() }).await;
  Ok(())
}

pub async fn send(bridge: &Bridge, producer: &FutureProducer, topic: &str, key: &str, payload: &[u8], headers: &HashMap<String, String>, timeout: Duration) -> Result<(i32, i64), KafkaError> {
  let mut kafka_headers = OwnedHeaders::new();
  for (key, value) in headers { kafka_headers = kafka_headers.insert(Header { key, value: Some(value.as_str()) }); }
  let record = FutureRecord::to(topic).key(key).payload(payload).headers(kafka_headers);
  match producer.send(record, timeout).await {
    Ok((partition, offset)) => {
      bridge.published(PublishedMessage { id: String::new(), topic: topic.into(), key: key.into(), message: payload.to_vec(), headers: headers.clone() }).await;
      bridge.acknowledged(AcknowledgedMessage { id: String::new(), topic: topic.into(), partition, offset, exception: String::new() }).await;
      Ok((partition, offset))
    }
    Err((error, _)) => {
      bridge.acknowledged(AcknowledgedMessage { id: String::new(), topic: topic.into(), partition: -1, offset: -1, exception: error.to_string() }).await;
      Err(error)
    }
  }
}
