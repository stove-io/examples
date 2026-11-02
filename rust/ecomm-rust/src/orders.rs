use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::{json_error, AppState};

#[derive(Deserialize)]
pub(super) struct OrderStreamQuery {
    #[serde(rename = "userId")]
    user_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamedOrder {
    order_id: String,
    user_id: String,
    product_id: String,
    amount: f64,
    status: String,
}

pub(super) async fn stream_orders(
    State(state): State<AppState>,
    Query(query): Query<OrderStreamQuery>,
) -> Response {
    match state.db.query("SELECT id, user_id, product_id, amount::float8, status FROM orders WHERE user_id = $1 ORDER BY created_at, id", &[&query.user_id]).await {
    Ok(rows) => {
      let mut records = Vec::with_capacity(rows.len());
      for row in rows {
        let order = StreamedOrder { order_id: row.get(0), user_id: row.get(1), product_id: row.get(2), amount: row.get(3), status: row.get(4) };
        match serde_json::to_string(&order) {
          Ok(value) => records.push(Ok::<_, std::convert::Infallible>(format!("{value}\n"))),
          Err(error) => { eprintln!("order serialization failed: {error}"); return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"); }
        }
      }
      let body = Body::from_stream(futures_util::stream::iter(records));
      ([(axum::http::header::CONTENT_TYPE, "application/x-ndjson")], body).into_response()
    }
    Err(error) => { eprintln!("order query failed: {error}"); json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error") }
  }
}
