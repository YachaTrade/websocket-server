pub mod chart;
pub mod market;
pub mod metrics;
pub mod order;
pub mod swap;

use anyhow::Result;

/// 모든 이벤트 프로듀서의 main 함수를 실행
/// 각 모듈의 main()이 내부적으로 receive_curve_event와 receive_dex_event를 실행함
pub async fn main() -> Result<()> {
    // 각 이벤트 프로듀서의 main 함수 실행
    // 이미 각 모듈의 init() 함수에서 receive 태스크들이 spawn됨

    // 모든 EventProducer 초기화
    swap::SwapEventProducer::init()?;
    chart::ChartEventProducer::init()?;
    order::OrderEventProducer::init()?;
    metrics::MetricsEventProducer::init()?;
    market::MarketEventProducer::init()?;

    // 모든 EventProducer의 main 함수 실행 (receive_curve_event, receive_dex_event 태스크 시작)
    // 각 main()은 태스크를 spawn하고 즉시 반환되므로 동시에 실행됨
    swap::SwapEventProducer::main()
        .await
        .expect("Failed to start SwapEventProducer");
    chart::ChartEventProducer::main()
        .await
        .expect("Failed to start ChartEventProducer");
    order::OrderEventProducer::main()
        .await
        .expect("Failed to start OrderEventProducer");
    metrics::MetricsEventProducer::main()
        .await
        .expect("Failed to start MetricsEventProducer");
    market::MarketEventProducer::main()
        .await
        .expect("Failed to start MarketEventProducer");

    tracing::info!("All event producers initialized and started");

    Ok(())
}
