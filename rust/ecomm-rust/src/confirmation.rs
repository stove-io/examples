use rdkafka::producer::FutureProducer;
use riverqueue::migrate::PostgresMigrator;
use riverqueue::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use riverqueue::{BoxError, Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, Workers};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, time::Duration};
use stove_kafka::{rdkafka_adapter, Bridge};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[serde(rename_all = "camelCase")]
#[river(kind = "order_confirmation")]
pub struct Confirmation {
    pub order_id: String,
    pub user_id: String,
    pub product_id: String,
    pub amount: f64,
    pub correlation: HashMap<String, String>,
}

pub async fn start(
    producer: Option<FutureProducer>,
    bridge: Bridge,
) -> Result<(Client, riverqueue::RunHandle), BoxError> {
    let options = PgConnectOptions::new()
        .host(&env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            env::var("DB_PORT")
                .unwrap_or_else(|_| "5432".into())
                .parse()?,
        )
        .database(&env::var("DB_NAME").unwrap_or_else(|_| "stove".into()))
        .username(&env::var("DB_USER").unwrap_or_else(|_| "stove".into()))
        .password(&env::var("DB_PASS").unwrap_or_else(|_| "stove".into()));
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;
    let mut workers = Workers::new();
    workers.add_fn(move |_ctx: WorkContext, job: Job<Confirmation>| {
        let producer = producer.clone();
        let bridge = bridge.clone();
        async move {
            let args = job.args;
            println!("Order confirmation for {}: {}", args.user_id, args.order_id);
            if let Some(producer) = producer {
                let payload = serde_json::json!({"orderId": args.order_id, "userId": args.user_id, "productId": args.product_id, "amount": args.amount}).to_string();
                rdkafka_adapter::send(&bridge, &producer, "order.confirmation.sent", &args.order_id, payload.as_bytes(), &args.correlation, Duration::from_secs(5)).await.map_err(|e| std::io::Error::other(e.to_string()))?;
            }
            Ok::<_, BoxError>(WorkOutcome::Complete)
        }
    })?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(4))
        .build()?;
    let mut run = client.start()?;
    run.wait_ready().await?;
    Ok((client, run))
}
