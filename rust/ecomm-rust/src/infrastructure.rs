use super::*;

pub(super) fn init_telemetry() -> Option<SdkTracerProvider> {
    let endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())?;
    let url = if endpoint.starts_with("http") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(url)
        .build()
        .ok()?;
    let provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_service_name("ecomm-rust").build())
        .with_batch_exporter(exporter)
        .build();
    global::set_tracer_provider(provider.clone());
    Some(provider)
}

pub(super) async fn connect_db() -> Result<Arc<tokio_postgres::Client>, tokio_postgres::Error> {
    let host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string());
    let name = env::var("DB_NAME").unwrap_or_else(|_| "stove".to_string());
    let user = env::var("DB_USER").unwrap_or_else(|_| "stove".to_string());
    let password = env::var("DB_PASS").unwrap_or_else(|_| "stove".to_string());
    let config = format!("host={host} port={port} dbname={name} user={user} password={password}");
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection closed: {error}");
        }
    });
    Ok(Arc::new(client))
}
