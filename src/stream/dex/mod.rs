pub mod receive;
pub mod stream;

use std::{future::Future, pin::Pin};

use anyhow::Result;

use stream::stream_dex_events;

use crate::types::stream::{DexEvent, EventType};

use crate::stream::handler::{run_event_handler, EventHandler};

pub struct DexEventHandler;

impl EventHandler for DexEventHandler {
    type Event = Vec<Box<dyn DexEvent>>;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(stream_dex_events(event_type))
    }
}

pub async fn main(event_type: EventType) -> Result<()> {
    run_event_handler::<DexEventHandler>(event_type).await
}
