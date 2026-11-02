use super::*;
use crate::product::ProductUpdate;

pub(super) fn kafka_producer() -> Option<FutureProducer> {
    let brokers = env::var("KAFKA_BROKERS")
        .ok()
        .filter(|value| !value.is_empty())?;
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .ok()
}

pub(super) async fn ensure_consumer_topics(
    brokers: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    let topics = [
        NewTopic::new(TOPIC_UPDATE, 1, TopicReplication::Fixed(1)),
        NewTopic::new("showcase.orders.created", 1, TopicReplication::Fixed(1)),
    ];
    for result in admin
        .create_topics(
            &topics,
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(10))),
        )
        .await?
    {
        if let Err((name, error)) = result {
            if error != rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists {
                return Err(format!("creating topic {name}: {error}").into());
            }
        }
    }
    Ok(())
}

pub(super) fn spawn_consumer(
    brokers: String,
    db: Arc<tokio_postgres::Client>,
    bridge: Bridge,
    producer: Option<FutureProducer>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "ecomm-rust")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .create()
        {
            Ok(consumer) => consumer,
            Err(error) => {
                eprintln!("kafka consumer failed: {error}");
                return;
            }
        };
        if let Err(error) = consumer.subscribe(&[TOPIC_UPDATE, "showcase.orders.created"]) {
            eprintln!("kafka subscribe failed: {error}");
            return;
        }
        loop {
            let message = match consumer.recv().await {
                Ok(message) => message,
                Err(error) => {
                    eprintln!("kafka recv failed: {error}");
                    continue;
                }
            };
            let headers = rdkafka_adapter::headers(&message);
            rdkafka_adapter::received(&bridge, &message).await;
            if message.topic() == "showcase.orders.created" {
                match statistics::update_statistics(&db, message.payload().unwrap_or_default())
                    .await
                {
                    Ok(Some(completion)) => {
                        if let Some(producer) = &producer {
                            let key = std::str::from_utf8(message.key().unwrap_or_default())
                                .unwrap_or_default();
                            if let Err(error) = rdkafka_adapter::send(
                                &bridge,
                                producer,
                                "customer.statistics.updated",
                                key,
                                &completion,
                                &headers,
                                Duration::from_secs(5),
                            )
                            .await
                            {
                                eprintln!("statistics publication failed: {error}");
                                let _ = consumer.seek(
                                    message.topic(),
                                    message.partition(),
                                    Offset::Offset(message.offset()),
                                    Duration::from_secs(5),
                                );
                                continue;
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("statistics update failed: {error}");
                        let _ = consumer.seek(
                            message.topic(),
                            message.partition(),
                            Offset::Offset(message.offset()),
                            Duration::from_secs(5),
                        );
                        continue;
                    }
                }
            } else if let Some(event) = message
                .payload()
                .and_then(|payload| serde_json::from_slice::<ProductUpdate>(payload).ok())
            {
                let price = sql_number(event.price);
                match db
                    .execute(
                        "UPDATE products SET name = $1, price = $2::text::numeric WHERE id = $3",
                        &[&event.name, &price, &event.id],
                    )
                    .await
                {
                    Ok(count) if count > 0 => {
                        if let Some(producer) = &producer {
                            let payload = message.payload().unwrap_or_default();
                            if let Err(error) = rdkafka_adapter::send(
                                &bridge,
                                producer,
                                "product.updated",
                                &event.id,
                                payload,
                                &headers,
                                Duration::from_secs(5),
                            )
                            .await
                            {
                                eprintln!("failed to publish product.updated: {error}");
                                let _ = consumer.seek(
                                    message.topic(),
                                    message.partition(),
                                    Offset::Offset(message.offset()),
                                    Duration::from_secs(5),
                                );
                                continue;
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("product update failed: {error}");
                        let _ = consumer.seek(
                            message.topic(),
                            message.partition(),
                            Offset::Offset(message.offset()),
                            Duration::from_secs(5),
                        );
                        continue;
                    }
                }
            } else {
                eprintln!("discarding malformed product.update");
            }
            if let Err(error) = rdkafka_adapter::commit(&bridge, &consumer, &message).await {
                eprintln!("product update commit failed: {error}");
                let _ = consumer.seek(
                    message.topic(),
                    message.partition(),
                    Offset::Offset(message.offset()),
                    Duration::from_secs(5),
                );
                continue;
            }
        }
    })
}
