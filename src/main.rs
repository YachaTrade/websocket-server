use std::env;

use tokio::task::JoinSet;
use tracing::{info, warn};
use websocket_server::{
    client,
    db::{cache::CacheManager, postgres::PostgresDatabase, redis::RedisDatabase},
    event::{self},
    metrics, server,
    stream::{v1::dex, v2::curve as v2_curve, handler::run_event_handler, price},
    types::stream::EventType,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 서버 인스턴스 ID 초기화 (UUID 생성)
    websocket_server::config::init_instance_id();

    //DB INIT
    {
        PostgresDatabase::init().await.unwrap();
        RedisDatabase::init().await.unwrap();

        // Redis 전체 초기화 (FLUSHALL)
        info!("Flushing Redis...");
        if let Err(e) = RedisDatabase::instance()?.flush_all().await {
            warn!("Failed to flush Redis: {}", e);
        }

        CacheManager::init().await.unwrap();
    }

    // Quote token 등록 (DB에서 로드 → Pyth feed 자동 등록)
    {
        let db = PostgresDatabase::instance()?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT quote_id, pyth_feed_id FROM quote_token",
        )
        .fetch_all(&db.pool)
        .await?;

        for (quote_id, pyth_feed_id) in &rows {
            price::register_quote_token(quote_id, pyth_feed_id);
        }
        info!("Registered {} quote tokens from DB", rows.len());
    }

    // RPC Client INIT (must precede price stream — price uses chain block
    // timestamps for Pyth queries, which requires RpcClient to be ready).
    {
        let main_rpc_url = env::var("MAIN_RPC_URL").expect("MAIN_RPC_URL must be set");
        let sub_rpc_url_1 = env::var("SUB_RPC_URL_1").expect("SUB_RPC_URL_1 must be set");
        let sub_rpc_url_2 = env::var("SUB_RPC_URL_2").expect("SUB_RPC_URL_2 must be set");

        let rpc_urls = vec![main_rpc_url, sub_rpc_url_1, sub_rpc_url_2];
        let rpc_retry_count = rpc_urls.len();
        client::RpcClient::init(rpc_urls, Some(rpc_retry_count)).await?;
    }

    // Native + Quote Price Update INIT
    {
        price::start_update_price().await?;
    }

    let mut set = JoinSet::new();
    set.spawn(run_event_handler::<v2_curve::V2CurveEventHandler>(
        EventType::Curve,
    ));
    set.spawn(run_event_handler::<dex::DexEventHandler>(EventType::Dex));
    set.spawn(server::main());
    set.spawn(event::main());

    set.spawn(metrics::run_metrics_logging());
    set.spawn(client::RpcClient::start_health_check_loop());
    while let Some(res) = set.join_next().await {
        match res {
            Ok(_) => info!("Task completed successfully"),
            Err(e) => warn!("Task panicked: {:?}", e),
        }
    }
    info!("main start");
    Ok(())
}
