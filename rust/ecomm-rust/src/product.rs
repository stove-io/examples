use super::*;

#[derive(Deserialize)]
pub(super) struct CreateProductRequest {
    name: String,
    price: f64,
}

#[derive(Serialize)]
struct Product {
    id: String,
    name: String,
    price: f64,
}

#[derive(Deserialize)]
pub(super) struct ProductUpdate {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) price: f64,
}

pub(super) async fn create_product(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<CreateProductRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let db = state.db.clone();
    let producer = state.producer.clone();
    let bridge = state.bridge.clone();
    let incoming_headers = headers.clone();
    traced(
        state.telemetry.as_ref(),
        &headers,
        "POST /api/products",
        async move {
            let Json(request) = match body {
                Ok(body) => body,
                Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid request body"),
            };
            if request.name.trim().is_empty() {
                return json_error(StatusCode::BAD_REQUEST, "invalid request body");
            }
            let product = Product {
                id: Uuid::new_v4().to_string(),
                name: request.name,
                price: request.price,
            };
            let price = sql_number(product.price);
            if let Err(error) = db
                .execute(
                    "INSERT INTO products (id, name, price) VALUES ($1, $2, $3::text::numeric)",
                    &[&product.id, &product.name, &price],
                )
                .await
            {
                eprintln!("insert failed: {error}");
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to create product",
                );
            }
            if let Some(producer) = producer {
                let payload = serde_json::to_string(&product).unwrap_or_default();
                let mut observed_headers = std::collections::HashMap::new();
                for name in ["traceparent", "tracestate", "baggage", "testcase"] {
                    if let Some(value) = incoming_headers.get(name).and_then(|v| v.to_str().ok()) {
                        let key = if name == "testcase" { "testCase" } else { name };
                        observed_headers.insert(key.to_string(), value.to_string());
                    }
                }
                global::get_text_map_propagator(|propagator| {
                    propagator.inject(&mut HeaderInjector(&mut observed_headers))
                });
                if let Err(error) = rdkafka_adapter::send(
                    &bridge,
                    &producer,
                    TOPIC_CREATED,
                    &product.id,
                    payload.as_bytes(),
                    &observed_headers,
                    Duration::from_secs(5),
                )
                .await
                {
                    eprintln!("failed to publish product.created: {error}");
                }
            }
            (StatusCode::CREATED, Json(product)).into_response()
        },
    )
    .await
}

pub(super) async fn get_product(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let db = state.db.clone();
    traced(
        state.telemetry.as_ref(),
        &headers,
        "GET /api/products/{id}",
        async move {
            match db
                .query_opt(
                    "SELECT id, name, price::float8 FROM products WHERE id = $1",
                    &[&id],
                )
                .await
            {
                Ok(Some(row)) => {
                    let product = Product {
                        id: row.get(0),
                        name: row.get(1),
                        price: row.get(2),
                    };
                    (StatusCode::OK, Json(product)).into_response()
                }
                Ok(None) => json_error(StatusCode::NOT_FOUND, "not found"),
                Err(error) => {
                    eprintln!("select failed: {error}");
                    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
                }
            }
        },
    )
    .await
}

pub(super) async fn list_products(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let db = state.db.clone();
    traced(
        state.telemetry.as_ref(),
        &headers,
        "GET /api/products",
        async move {
            match db
                .query("SELECT id, name, price::float8 FROM products", &[])
                .await
            {
                Ok(rows) => {
                    let products: Vec<Product> = rows
                        .into_iter()
                        .map(|row| Product {
                            id: row.get(0),
                            name: row.get(1),
                            price: row.get(2),
                        })
                        .collect();
                    (StatusCode::OK, Json(products)).into_response()
                }
                Err(error) => {
                    eprintln!("list failed: {error}");
                    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
                }
            }
        },
    )
    .await
}
