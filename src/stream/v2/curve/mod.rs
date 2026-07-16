pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use stream::stream_v2_curve_events;

use crate::types::stream::EventType;

use crate::stream::handler::{run_event_handler, EventHandler};

/// V2 BondingCurve 이벤트 핸들러
/// V2 BondingCurve 컨트랙트에서 Create, Buy, Sell, Sync, Graduate 이벤트를 수신
pub struct V2CurveEventHandler;

impl EventHandler for V2CurveEventHandler {
    type Event = ();

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream_v2_curve_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<V2CurveEventHandler>(event_type).await
}
