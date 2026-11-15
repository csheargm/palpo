//! Client endpoints for MSC4140 delayed events.

use salvo::oapi::extract::*;
use salvo::prelude::*;

use crate::core::client::delayed_events::{
    DelayedEventData, SendDelayedEventReqArgs, SendDelayedEventReqBody, SendDelayedEventResBody,
    UpdateDelayedEventReqArgs,
};
use crate::routing::prelude::*;

pub(super) fn authed_router() -> Router {
    Router::with_path("org.matrix.msc4140")
        .push(
            Router::with_path("rooms/{room_id}/delayed_event/{event_type}/{txn_id}")
                .put(send_delayed_event),
        )
        .push(
            Router::with_path("delayed_events").push(
                Router::with_path("{delay_id}")
                    .get(get_delayed_event)
                    .push(Router::with_path("{action}").post(update_delayed_event)),
            ),
        )
}

/// `PUT /_matrix/client/unstable/org.matrix.msc4140/rooms/{room_id}/delayed_event/{event_type}/
/// {txn_id}`
///
/// Schedule a message or state event to be sent into the room after a delay.
#[endpoint]
async fn send_delayed_event(
    _aa: AuthArgs,
    args: SendDelayedEventReqArgs,
    body: JsonBody<SendDelayedEventReqBody>,
    depot: &mut Depot,
) -> JsonResult<SendDelayedEventResBody> {
    let authed = depot.authed_info()?;
    let delay_id = crate::delayed_event::schedule(
        authed.user_id(),
        authed.device_id(),
        authed.appservice().is_some(),
        &args.room_id,
        &args.event_type,
        &args.txn_id,
        args.timestamp,
        sticky_duration(body.sticky_duration_ms, args.sticky_duration_ms)?,
        body.delay,
        body.state_key.clone(),
        body.content.clone(),
    )
    .await?;
    json_ok(SendDelayedEventResBody::new(delay_id))
}

/// `GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}`
///
/// Get the details of one of the requesting user's delayed events, whether
/// still scheduled or already finalized.
#[endpoint]
async fn get_delayed_event(
    _aa: AuthArgs,
    delay_id: PathParam<String>,
    depot: &mut Depot,
) -> JsonResult<DelayedEventData> {
    let authed = depot.authed_info()?;
    let delayed_event = crate::delayed_event::get(authed.user_id(), &delay_id.into_inner()).await?;
    json_ok(delayed_event)
}

/// `POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/{action}`
///
/// Restart, send, or cancel a scheduled delayed event.
#[endpoint]
async fn update_delayed_event(
    _aa: AuthArgs,
    args: UpdateDelayedEventReqArgs,
    depot: &mut Depot,
) -> EmptyResult {
    let authed = depot.authed_info()?;
    crate::delayed_event::update(authed.user_id(), &args.delay_id, &args.action).await?;
    empty_ok()
}

/// MSC4140's spec form: `PUT /send` or `PUT /state` with `?org.matrix.msc4140.delay=<ms>`
/// schedules the event through the same machinery, limits and `[delayed_events]`
/// switch as `send_delayed_event`, instead of sending it at once.
/// `txn_id` is `None` for state, whose route has no transaction ID; a fresh one is used.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn schedule_from_query(
    authed: &crate::AuthedInfo,
    room_id: &crate::core::RoomId,
    event_type: crate::core::events::TimelineEventType,
    txn_id: Option<&crate::core::TransactionId>,
    timestamp: Option<crate::core::UnixMillis>,
    sticky_duration_ms: Option<crate::core::events::sticky::StickyDurationMs>,
    delay_ms: u64,
    state_key: Option<String>,
    content: serde_json::Value,
) -> AppResult<String> {
    let fresh = crate::core::TransactionId::new();
    crate::delayed_event::schedule(
        authed.user_id(),
        authed.device_id(),
        authed.appservice().is_some(),
        room_id,
        &event_type,
        txn_id.unwrap_or(&fresh),
        timestamp,
        sticky_duration_ms,
        std::time::Duration::from_millis(delay_ms),
        state_key,
        content,
    )
    .await
}

/// Retain the historical query form while rejecting ambiguous requests.
fn sticky_duration(
    body: Option<crate::core::events::sticky::StickyDurationMs>,
    query: Option<crate::core::events::sticky::StickyDurationMs>,
) -> Result<Option<crate::core::events::sticky::StickyDurationMs>, crate::core::MatrixError> {
    if let (Some(body), Some(query)) = (body, query)
        && body != query
    {
        return Err(crate::core::MatrixError::invalid_param(
            "Conflicting sticky durations in request body and query",
        ));
    }
    Ok(body.or(query))
}

#[cfg(test)]
#[path = "delayed_event_tests.rs"]
mod tests;
