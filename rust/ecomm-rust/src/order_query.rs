use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_postgres::Client;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("orderquery");
}
use proto::order_query_service_server::{OrderQueryService, OrderQueryServiceServer};
use proto::{
    GetOrderRequest, GetOrderResponse, GetOrdersByUserRequest, GetOrdersResponse, OrderProto,
};

const COLUMNS: &str = "SELECT id, user_id, product_id, amount::float8, status, COALESCE(payment_transaction_id, ''), (EXTRACT(EPOCH FROM created_at) * 1000)::bigint FROM orders ";

fn to_proto(row: tokio_postgres::Row) -> OrderProto {
    OrderProto {
        id: row.get(0),
        user_id: row.get(1),
        product_id: row.get(2),
        amount: row.get(3),
        status: row.get(4),
        payment_transaction_id: row.get(5),
        created_at: row.get(6),
    }
}

struct Service {
    db: Arc<Client>,
}

#[tonic::async_trait]
impl OrderQueryService for Service {
    async fn get_order(
        &self,
        request: Request<GetOrderRequest>,
    ) -> Result<Response<GetOrderResponse>, Status> {
        let row = self
            .db
            .query_opt(
                &(COLUMNS.to_owned() + "WHERE id = $1"),
                &[&request.into_inner().order_id],
            )
            .await
            .map_err(query_error)?;
        Ok(Response::new(GetOrderResponse {
            found: row.is_some(),
            order: row.map(to_proto),
        }))
    }

    async fn get_orders_by_user(
        &self,
        request: Request<GetOrdersByUserRequest>,
    ) -> Result<Response<GetOrdersResponse>, Status> {
        let rows = self
            .db
            .query(
                &(COLUMNS.to_owned() + "WHERE user_id = $1 ORDER BY created_at, id"),
                &[&request.into_inner().user_id],
            )
            .await
            .map_err(query_error)?;
        Ok(Response::new(GetOrdersResponse {
            orders: rows.into_iter().map(to_proto).collect(),
        }))
    }
}

fn query_error(error: tokio_postgres::Error) -> Status {
    eprintln!("order query failed: {error}");
    Status::internal("order query failed")
}

pub async fn start(
    db: Arc<Client>,
    port: u16,
) -> Result<(oneshot::Sender<()>, tokio::task::JoinHandle<()>), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let result = tonic::transport::Server::builder()
            .add_service(OrderQueryServiceServer::new(Service { db }))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await;
        if let Err(error) = result {
            eprintln!("order query server failed: {error}");
        }
    });
    Ok((shutdown, task))
}
