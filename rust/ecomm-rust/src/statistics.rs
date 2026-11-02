use crate::sql_number;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderEvent {
    order_id: String,
    user_id: String,
    amount: f64,
}

pub async fn update_statistics(
    db: &tokio_postgres::Client,
    payload: &[u8],
) -> Result<Option<Vec<u8>>, tokio_postgres::Error> {
    let event: OrderEvent = match serde_json::from_slice(payload) {
        Ok(event) => event,
        Err(_) => return Ok(None),
    };
    let amount = sql_number(event.amount);
    db.execute("WITH inserted AS (INSERT INTO processed_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING order_id) INSERT INTO user_order_statistics (user_id,total_orders,total_amount,last_order_at) SELECT $2,1,$3::text::numeric,NOW() FROM inserted ON CONFLICT (user_id) DO UPDATE SET total_orders=user_order_statistics.total_orders+1,total_amount=user_order_statistics.total_amount+EXCLUDED.total_amount,last_order_at=NOW()", &[&event.order_id, &event.user_id, &amount]).await?;
    Ok(Some(
        serde_json::json!({"orderId":event.order_id,"userId":event.user_id})
            .to_string()
            .into_bytes(),
    ))
}
