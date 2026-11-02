use std::{collections::HashMap, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant}};
use stove_kafka::{Bridge, PublishedMessage, ConsumedMessage, CommittedMessage, AcknowledgedMessage};
use stove_kafka::observer::{HealthCheckRequest, HealthCheckResponse, Reply, stove_kafka_observer_service_server::{StoveKafkaObserverService, StoveKafkaObserverServiceServer}};
use tokio::sync::{Mutex, oneshot};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct Recorder { calls: Mutex<Vec<String>>, slow: AtomicBool }

#[tonic::async_trait]
impl StoveKafkaObserverService for Recorder {
  async fn health_check(&self, _: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> { Ok(Response::new(HealthCheckResponse { status: 1 })) }
  async fn on_published_message(&self, req: Request<PublishedMessage>) -> Result<Response<Reply>, Status> {
    if self.slow.load(Ordering::Relaxed) { tokio::time::sleep(Duration::from_secs(5)).await; }
    let m = req.into_inner();
    assert!(!m.id.is_empty()); assert_eq!(m.topic, "products"); assert_eq!(m.key, "id");
    assert_eq!(m.message, vec![0, 255]); assert_eq!(m.headers.get("testCase").unwrap(), "case");
    self.calls.lock().await.push("published".into()); Ok(Response::new(Reply { status: 200 }))
  }
  async fn on_consumed_message(&self, req: Request<ConsumedMessage>) -> Result<Response<Reply>, Status> {
    let m = req.into_inner(); assert_eq!(m.offset, 9_007_199_254_740_993); assert!(m.message.is_empty());
    assert_eq!(m.headers.get("null").unwrap(), "");
    self.calls.lock().await.push("consumed".into()); Ok(Response::new(Reply { status: 200 }))
  }
  async fn on_committed_message(&self, req: Request<CommittedMessage>) -> Result<Response<Reply>, Status> {
    self.calls.lock().await.push("committed".into());
    assert_eq!(req.into_inner().offset, 9_007_199_254_740_994);
    Ok(Response::new(Reply { status: 200 }))
  }
  async fn on_acknowledged_message(&self, req: Request<AcknowledgedMessage>) -> Result<Response<Reply>, Status> {
    let message = req.into_inner();
    if message.exception.is_empty() {
      assert_eq!(message.offset, 9_007_199_254_740_993);
      self.calls.lock().await.push("acknowledged".into());
    } else {
      assert_eq!(message.offset, -1);
      self.calls.lock().await.push("failed".into());
    }
    Ok(Response::new(Reply { status: 200 }))
  }
}

#[tokio::test]
async fn records_wire_values_deadline_and_shutdown() {
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let address = listener.local_addr().unwrap();
  let recorder = Arc::new(Recorder::default());
  let (shutdown, done) = oneshot::channel();
  let service = StoveKafkaObserverServiceServer::from_arc(recorder.clone());
  let server = tokio::spawn(async move { tonic::transport::Server::builder().add_service(service).serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async { let _ = done.await; }).await.unwrap(); });
  let bridge = Bridge::from_endpoint(format!("http://{address}"));
  let published = PublishedMessage { id: "".into(), topic: "products".into(), key: "id".into(), message: vec![0, 255], headers: HashMap::from([("testCase".into(), "case".into())]) };
  bridge.published(published.clone()).await;
  bridge.consumed(ConsumedMessage { id: "".into(), topic: "products".into(), key: "".into(), partition: 2, offset: 9_007_199_254_740_993, message: vec![], headers: HashMap::from([("null".into(), "".into())]) }).await;
  bridge.committed(CommittedMessage { id: "".into(), topic: "products".into(), partition: 2, offset: 9_007_199_254_740_994, metadata: "".into() }).await;
  bridge.acknowledged(AcknowledgedMessage { id: "".into(), topic: "products".into(), partition: 2, offset: 9_007_199_254_740_993, exception: "".into() }).await;
  assert_eq!(*recorder.calls.lock().await, ["published", "consumed", "committed", "acknowledged"]);
  #[cfg(feature = "rdkafka-adapter")]
  {
    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
      .set("bootstrap.servers", "127.0.0.1:1").set("message.timeout.ms", "200").create().unwrap();
    let result = stove_kafka::rdkafka_adapter::send(&bridge, &producer, "products", "id", b"body", &HashMap::new(), Duration::from_millis(200)).await;
    assert!(result.is_err());
    assert_eq!(*recorder.calls.lock().await, ["published", "consumed", "committed", "acknowledged", "failed"]);

    // Exercise a real synchronous commit against librdkafka's mock broker, with
    // a non-retryable broker error injected into the OffsetCommit response.
    use rdkafka::{consumer::{Consumer, StreamConsumer}, mocking::MockCluster, types::{RDKafkaApiKey, RDKafkaRespErr}, Offset, TopicPartitionList};
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("commit-test", 1, 1).unwrap();
    let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
      .set("bootstrap.servers", cluster.bootstrap_servers()).create().unwrap();
    let unavailable = Bridge::from_endpoint("http://127.0.0.1:1".into());
    stove_kafka::rdkafka_adapter::send(&unavailable, &producer, "commit-test", "id", b"body", &HashMap::new(), Duration::from_secs(5)).await.unwrap();
    unavailable.close();
    let consumer: StreamConsumer = rdkafka::ClientConfig::new()
      .set("bootstrap.servers", cluster.bootstrap_servers()).set("group.id", "commit-test")
      .set("enable.auto.commit", "false").create().unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset("commit-test", 0, Offset::Beginning).unwrap();
    consumer.assign(&assignment).unwrap();
    let message = tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await.unwrap().unwrap();
    cluster.request_errors(RDKafkaApiKey::OffsetCommit, &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED]);
    assert!(stove_kafka::rdkafka_adapter::commit(&bridge, &consumer, &message).await.is_err());
    assert_eq!(*recorder.calls.lock().await, ["published", "consumed", "committed", "acknowledged", "failed"]);
  }
  recorder.slow.store(true, Ordering::Relaxed);
  let start = Instant::now(); bridge.published(published.clone()).await;
  assert!(start.elapsed() >= Duration::from_millis(900) && start.elapsed() < Duration::from_secs(3));
  bridge.close();
  let disabled = Bridge::disabled(); let start = Instant::now(); disabled.published(published).await; disabled.close();
  assert!(start.elapsed() < Duration::from_millis(200));
  shutdown.send(()).unwrap(); server.await.unwrap();
}
