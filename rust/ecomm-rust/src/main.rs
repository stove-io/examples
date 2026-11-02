use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer};
use opentelemetry::{global, Context};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::{ClientConfig, Offset};
use serde::{Deserialize, Serialize};
use stove_kafka::{rdkafka_adapter, Bridge};
use tokio_postgres::NoTls;
use uuid::Uuid;

mod checkout;
mod confirmation;
mod infrastructure;
mod messaging;
mod order_query;
mod orders;
mod product;
mod statistics;

use infrastructure::{connect_db, init_telemetry};
use messaging::{ensure_consumer_topics, kafka_producer, spawn_consumer};
use product::{create_product, get_product, list_products};

const TOPIC_CREATED: &str = "product.created";
const TOPIC_UPDATE: &str = "product.update";

#[derive(Clone)]
struct AppState {
    db: Arc<tokio_postgres::Client>,
    producer: Option<FutureProducer>,
    telemetry: Option<SdkTracerProvider>,
    bridge: Bridge,
    confirmations: riverqueue::Client,
}

fn sql_number(value: f64) -> String {
    value.to_string()
}

struct HeaderExtractor<'a>(&'a HeaderMap);

struct HeaderInjector<'a>(&'a mut std::collections::HashMap<String, String>);
impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), riverqueue::BoxError> {
    let port = env::var("APP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let telemetry = init_telemetry();
    let db = connect_db().await?;
    let query_port = env::var("ORDER_GRPC_PORT")
        .unwrap_or_else(|_| "9091".into())
        .parse::<u16>()?;
    let (query_shutdown, query_task) = order_query::start(db.clone(), query_port).await?;
    let producer = kafka_producer();
    let bridge = Bridge::from_env();
    let brokers = env::var("KAFKA_BROKERS")
        .ok()
        .filter(|value| !value.is_empty());
    if let Some(brokers) = &brokers {
        ensure_consumer_topics(brokers)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
    }
    let consumer_task = brokers
        .map(|brokers| spawn_consumer(brokers, db.clone(), bridge.clone(), producer.clone()));

    let (confirmations, mut confirmation_run) =
        confirmation::start(producer.clone(), bridge.clone()).await?;
    let state = AppState {
        db,
        producer: producer.clone(),
        telemetry,
        bridge: bridge.clone(),
        confirmations,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/orders/stream", get(orders::stream_orders))
        .route("/api/orders", post(checkout::create))
        .route("/api/orders/{id}", get(checkout::get))
        .route("/api/products", post(create_product).get(list_products))
        .route("/api/products/{id}", get(get_product))
        .with_state(state);

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("ecomm-rust listening on {port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    confirmation_run.stopper().stop();
    if let Err(error) = confirmation_run.wait().await {
        eprintln!("confirmation worker stopped: {error}");
    }
    let _ = query_shutdown.send(());
    let _ = query_task.await;
    if let Some(task) = consumer_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(producer) = producer {
        let _ = producer.flush(Duration::from_secs(5));
    }
    bridge.close();
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
      _ = ctrl_c => {}
      _ = terminate => {}
    }
}

async fn traced<T>(
    telemetry: Option<&SdkTracerProvider>,
    headers: &HeaderMap,
    name: &'static str,
    future: impl std::future::Future<Output = T>,
) -> T {
    let parent: Context =
        global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)));
    let tracer = global::tracer("ecomm-rust");
    let span = tracer.start_with_context(name, &parent);
    let active = parent.with_span(span);
    let result = future.with_context(active.clone()).await;
    active.span().end();
    if let Some(provider) = telemetry {
        let _ = provider.force_flush();
    }
    result
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "UP" }))
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}
