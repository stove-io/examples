use crate::{sql_number, traced, AppState, HeaderInjector};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use opentelemetry::global;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, time::Duration};
use stove_kafka::rdkafka_adapter;
use uuid::Uuid;

mod fraud {
    tonic::include_proto!("frauddetection");
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutRequest {
    user_id: String,
    product_id: String,
    amount: f64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: String,
    user_id: String,
    product_id: String,
    amount: f64,
    status: &'static str,
}
#[derive(Deserialize)]
struct Inventory {
    available: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Payment {
    success: bool,
    transaction_id: Option<String>,
}
fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({"errorCode": code, "message": message})),
    )
        .into_response()
}

pub async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CheckoutRequest>,
) -> Response {
    let telemetry = state.telemetry.clone();
    let incoming = headers.clone();
    traced(
        telemetry.as_ref(),
        &headers,
        "POST /api/orders",
        async move { create_order(state, incoming, request).await },
    )
    .await
}

async fn create_order(state: AppState, headers: HeaderMap, request: CheckoutRequest) -> Response {
    let id = Uuid::new_v4().to_string();
    let target = env::var("FRAUD_GRPC_TARGET").unwrap_or_else(|_| "localhost:9090".into());
    let mut client =
        match fraud::fraud_detection_service_client::FraudDetectionServiceClient::connect(format!(
            "http://{target}"
        ))
        .await
        {
            Ok(client) => client,
            Err(e) => {
                eprintln!("fraud connection failed: {e}");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Fraud check failed",
                );
            }
        };
    let mut fraud_request = tonic::Request::new(fraud::CheckFraudRequest {
        order_id: id.clone(),
        user_id: request.user_id.clone(),
        product_id: request.product_id.clone(),
        amount: request.amount,
    });
    fraud_request.set_timeout(Duration::from_secs(5));
    match client.check_fraud(fraud_request).await {
        Ok(response) if !response.get_ref().is_fraudulent => {}
        _ => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Fraud check failed",
            )
        }
    }
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "HTTP client failed",
            )
        }
    };
    let inventory_url =
        env::var("INVENTORY_URL").unwrap_or_else(|_| "http://localhost:8081".into());
    let payment_url = env::var("PAYMENT_URL").unwrap_or_else(|_| "http://localhost:8081".into());
    let inventory = match http
        .get(format!("{inventory_url}/inventory/{}", request.product_id))
        .send()
        .await
    {
        Ok(r) => r.error_for_status().and_then(|r| Ok(r)),
        Err(e) => Err(e),
    };
    let inventory: Inventory = match inventory {
        Ok(r) => match r.json().await {
            Ok(v) => v,
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Inventory response invalid",
                )
            }
        },
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Inventory request failed",
            )
        }
    };
    if !inventory.available {
        return error(
            StatusCode::CONFLICT,
            "INVENTORY_NOT_AVAILABLE",
            "Inventory not available",
        );
    }
    let payment = match http
        .post(format!("{payment_url}/payments/charge"))
        .json(&request)
        .send()
        .await
    {
        Ok(r) => match r.error_for_status() {
            Ok(r) => match r.json::<Payment>().await {
                Ok(p) => p,
                Err(_) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "INTERNAL_ERROR",
                        "Payment response invalid",
                    )
                }
            },
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Payment request failed",
                )
            }
        },
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Payment request failed",
            )
        }
    };
    if !payment.success {
        return error(StatusCode::BAD_GATEWAY, "PAYMENT_FAILED", "Payment failed");
    }
    let transaction = match payment.transaction_id {
        Some(v) => v,
        None => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "Payment transaction missing",
            )
        }
    };
    let amount = sql_number(request.amount);
    if let Err(e) = state.db.execute("INSERT INTO orders (id,user_id,product_id,amount,status,payment_transaction_id) VALUES ($1,$2,$3,$4::text::numeric,'CONFIRMED',$5)", &[&id, &request.user_id, &request.product_id, &amount, &transaction]).await {
    eprintln!("order insert failed: {e}"); return error(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Order persistence failed");
  }
    let mut event_headers = HashMap::new();
    for name in ["traceparent", "tracestate", "baggage", "testcase"] {
        if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
            event_headers.insert(
                if name == "testcase" {
                    "testCase".into()
                } else {
                    name.into()
                },
                value.into(),
            );
        }
    }
    global::get_text_map_propagator(|propagator| {
        propagator.inject(&mut HeaderInjector(&mut event_headers))
    });
    if let Err(e) = state
        .confirmations
        .insert(crate::confirmation::Confirmation {
            order_id: id.clone(),
            user_id: request.user_id.clone(),
            product_id: request.product_id.clone(),
            amount: request.amount,
            correlation: event_headers.clone(),
        })
        .await
    {
        eprintln!("confirmation scheduling failed: {e}");
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Confirmation scheduling failed",
        );
    }
    if let Some(producer) = &state.producer {
        let order_event = serde_json::json!({"orderId": id, "userId": request.user_id, "productId": request.product_id, "amount": request.amount});
        let payment_event = serde_json::json!({"orderId": id, "transactionId": transaction, "amount": request.amount, "success": true});
        for (topic, event) in [
            ("showcase.orders.created", order_event),
            ("showcase.payments.processed", payment_event),
        ] {
            if let Err(e) = rdkafka_adapter::send(
                &state.bridge,
                producer,
                topic,
                &id,
                event.to_string().as_bytes(),
                &event_headers,
                Duration::from_secs(5),
            )
            .await
            {
                eprintln!("event publication failed: {e}");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Event publication failed",
                );
            }
        }
    }
    (
        StatusCode::CREATED,
        Json(Order {
            order_id: id,
            user_id: request.user_id,
            product_id: request.product_id,
            amount: request.amount,
            status: "CONFIRMED",
        }),
    )
        .into_response()
}

pub async fn get(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.db.query_opt("SELECT id,user_id,product_id,amount::float8,status FROM orders WHERE id=$1", &[&id]).await {
    Ok(Some(row)) => (StatusCode::OK, Json(serde_json::json!({"orderId": row.get::<_,String>(0), "userId": row.get::<_,String>(1), "productId": row.get::<_,String>(2), "amount": row.get::<_,f64>(3), "status": row.get::<_,String>(4)}))).into_response(),
    Ok(None) => error(StatusCode::NOT_FOUND, "NOT_FOUND", "Order not found"),
    Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Order lookup failed"),
  }
}
