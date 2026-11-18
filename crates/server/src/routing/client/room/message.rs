use std::collections::{BTreeMap, HashSet};

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use serde_json::value::to_raw_value;

use crate::core::client::message::{
    CreateMessageReqArgs, CreateMessageWithTxnReqArgs, MessagesReqArgs, MessagesResBody,
    SendMessageResBody,
};
use crate::core::events::{StateEventType, TimelineEventType};
use crate::core::serde::to_canonical_value;
use crate::core::{Direction, UnixMillis};
use crate::data::schema::*;
use crate::data::{connect, diesel_exists};
use crate::event::BatchToken;
use crate::room::timeline::{self, topolo};
use crate::routing::prelude::*;
use crate::{PduBuilder, room};

fn parse_event_content(payload: &[u8]) -> AppResult<JsonValue> {
    let content: JsonValue =
        serde_json::from_slice(payload).map_err(|_| MatrixError::bad_json("invalid json body"))?;
    if !content.is_object() {
        return Err(MatrixError::bad_json("json body is not object").into());
    }
    to_canonical_value(&content).map_err(|e| {
        MatrixError::bad_json(format!("event content is not valid canonical JSON: {e}"))
    })?;
    Ok(content)
}

fn appservice_timestamp(
    is_appservice: bool,
    requested_timestamp: Option<UnixMillis>,
) -> Option<UnixMillis> {
    if is_appservice {
        requested_timestamp
    } else {
        None
    }
}

/// #GET /_matrix/client/r0/rooms/{room_id}/messages
/// Allows paginating through room history.
#[endpoint]
pub(super) async fn get_messages(
    _aa: AuthArgs,
    args: MessagesReqArgs,
    depot: &mut Depot,
) -> JsonResult<MessagesResBody> {
    let authed = depot.authed_info()?;
    let sender_id = authed.user_id();

    let is_joined = diesel_exists!(
        room_users::table
            .filter(room_users::room_id.eq(&args.room_id))
            .filter(room_users::user_id.eq(sender_id))
            .filter(room_users::membership.eq("join")),
        &mut connect().await?
    )?;
    if !is_joined {
        let forgotten_row = room_users::table
            .filter(room_users::room_id.eq(&args.room_id))
            .filter(room_users::user_id.eq(sender_id))
            .filter(room_users::membership.eq("leave"))
            .select((room_users::event_sn, room_users::forgotten))
            .first::<(i64, bool)>(&mut connect().await?)
            .await;
        let Some((_event_sn, forgotten)) = diesel::OptionalExtension::optional(forgotten_row)?
        else {
            return Err(MatrixError::forbidden("you aren't a member of the room", None).into());
        };
        if forgotten {
            return Err(MatrixError::forbidden("you aren't a member of the room", None).into());
        }
    }
    // let until_tk = if !is_joined {
    //     let Some((event_sn, forgotten)) = room_users::table
    //         .filter(room_users::room_id.eq(&args.room_id))
    //         .filter(room_users::user_id.eq(sender_id))
    //         .filter(room_users::membership.eq("leave"))
    //         .select((room_users::event_sn, room_users::forgotten))
    //         .first::<(i64, bool)>(&mut connect()?)
    //         .optional()?
    //     else {
    //         return Err(MatrixError::forbidden("you aren't a member of the room", None).into());
    //     };
    //     if forgotten {
    //         return Err(MatrixError::forbidden("you aren't a member of the room", None).into());
    //     }
    //     get_batch_token_by_sn(event_sn).ok()
    // } else {
    //     args.to.as_ref().map(|to| to.parse()).transpose()?
    // };
    let until_tk = args.to.as_ref().map(|to| to.parse()).transpose()?;

    let from_tk: BatchToken = args
        .from
        .as_ref()
        .map(|from| from.parse())
        .transpose()?
        .unwrap_or(match args.dir {
            Direction::Forward => BatchToken::LIVE_MIN,
            Direction::Backward => BatchToken::LIVE_MAX,
        });
    // if from_tk.event_depth.is_none() {
    //     from_tk = events::table
    //         .filter(events::sn.le(from_tk.event_sn()))
    //         .order_by(events::sn.desc())
    //         .select((events::sn, events::depth))
    //         .first::<(Seqnum, i64)>(&mut connect()?)
    //         .map(|(sn, depth)| BatchToken::new(sn, Some(depth)))?
    // }

    if let Some(device_id) = authed.device_id() {
        crate::room::lazy_loading::lazy_load_confirm_delivery(
            authed.user_id(),
            device_id,
            &args.room_id,
            from_tk.event_sn(),
        )?;
    }

    // Match Synapse's `MAX_LIMIT` for `/messages?limit=`.
    let limit = args.limit.min(1000);
    let next_token;
    let mut resp = MessagesResBody::default();
    let mut lazy_loaded = HashSet::new();
    if timeline::publication::uses_publication(from_tk)
        || until_tk.is_some_and(timeline::publication::uses_publication)
    {
        let mut page = timeline::publication::page(
            sender_id,
            &args.room_id,
            from_tk,
            until_tk,
            &args.filter,
            limit,
            args.dir,
        )
        .await?;
        if args.dir == Direction::Backward {
            let filled =
                timeline::backfill_if_required(&args.room_id, &from_tk, &page.events, limit)
                    .await?;
            if !filled.is_empty() {
                page = timeline::publication::page(
                    sender_id,
                    &args.room_id,
                    from_tk,
                    until_tk,
                    &args.filter,
                    limit,
                    args.dir,
                )
                .await?;
                let events = timeline::trim_pdus_until_backfill_gap(page.events.clone()).await?;
                page.replace_events(events, args.dir);
            }
        }
        for event in page.events.values() {
            lazy_loaded.insert(event.sender.clone());
        }
        next_token = page.end;
        resp.start = from_tk.to_string();
        resp.end = next_token.map(|token| token.to_string());
        resp.chunk = page
            .events
            .values()
            .map(|pdu| pdu.to_room_event_for(sender_id, authed.device_id()))
            .collect();
    } else {
        match args.dir {
            Direction::Forward => {
                let events = topolo::load_pdus_forward(
                    Some(sender_id),
                    &args.room_id,
                    Some(from_tk),
                    until_tk,
                    Some(&args.filter),
                    limit,
                )
                .await?;

                for (_, event) in &events {
                    // TODO: Remove this when these are resolved:
                    // https://github.com/vector-im/element-android/issues/3417
                    // https://github.com/vector-im/element-web/issues/21034
                    // if !crate::room::lazy_loading.lazy_load_was_sent_before(
                    // sender_id,
                    // sender_id,
                    // &body.room_id,
                    // &event.sender,
                    // )? {
                    // lazy_loaded.insert(event.sender.clone());
                    // }
                    lazy_loaded.insert(event.sender.clone());
                }

                next_token = events.last().map(|(_, pdu)| pdu.historic_token());

                let events: Vec<_> = events
                    .into_iter()
                    .map(|(_, pdu)| pdu.to_room_event_for(sender_id, authed.device_id()))
                    .collect();

                resp.start = from_tk.to_string();
                resp.end = next_token.map(|tk| tk.to_string());
                resp.chunk = events;
            }
            Direction::Backward => {
                let mut events: indexmap::IndexMap<i64, crate::SnPduEvent> =
                    topolo::load_pdus_backward(
                        Some(sender_id),
                        &args.room_id,
                        Some(from_tk),
                        until_tk,
                        Some(&args.filter),
                        limit,
                    )
                    .await?;
                // Backfill if the local page is short on history. Backfilled events
                // may have higher depth than what we already loaded (e.g. on a fresh
                // join, locally we only have low-depth state events while the
                // backfilled timeline messages live above them), so re-run the page
                // load whenever we filled anything.
                let filled_events =
                    timeline::backfill_if_required(&args.room_id, &from_tk, &events, limit).await?;
                if !filled_events.is_empty() {
                    events = topolo::load_pdus_backward(
                        Some(sender_id),
                        &args.room_id,
                        Some(from_tk),
                        until_tk,
                        Some(&args.filter),
                        limit,
                    )
                    .await?;
                    events = timeline::trim_pdus_until_backfill_gap(events).await?;
                }

                for (_, event) in &events {
                    // TODO: Remove this when these are resolved:
                    // https://github.com/vector-im/element-android/issues/3417
                    // https://github.com/vector-im/element-web/issues/21034
                    // if !crate::room::lazy_loading.lazy_load_was_sent_before(
                    // sender_id,
                    // authed.require_device_id()?,
                    // &args.room_id,
                    // &event.sender,
                    // )? {
                    // lazy_loaded.insert(event.sender.clone());
                    // }
                    lazy_loaded.insert(event.sender.clone());
                }
                next_token = events.last().map(|(_, pdu)| pdu.prev_historic_token());
                resp.start = from_tk.to_string();
                resp.end = next_token.map(|tk| tk.to_string());
                resp.chunk = events
                    .values()
                    .map(|pdu| pdu.to_room_event_for(sender_id, authed.device_id()))
                    .collect();
            }
        }
    }
    resp.state = Vec::new();
    for ll_id in &lazy_loaded {
        if let Ok(member_event) = room::get_state(
            &args.room_id,
            &StateEventType::RoomMember,
            ll_id.as_str(),
            None,
        )
        .await
        {
            resp.state
                .push(member_event.to_state_event_for(sender_id, authed.device_id()));
        }
    }

    // TODO: enable again when we are sure clients can handle it
    // if let Some(next_token) = next_token {
    // crate::room::lazy_loading.lazy_load_mark_sent(
    // authed.user_id(),
    // authed.require_device_id()?,
    // &body.room_id,
    // lazy_loaded,
    // next_token,
    // );
    // }

    json_ok(resp)
}

/// #PUT /_matrix/client/r0/rooms/{room_id}/send/{event_type}/{txn_id}
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is allowed
#[endpoint]
pub(super) async fn send_message(
    _aa: AuthArgs,
    args: CreateMessageWithTxnReqArgs,
    req: &mut Request,
    depot: &mut Depot,
) -> JsonResult<SendMessageResBody> {
    let authed = depot.authed_info()?;

    let conf = config::get();
    // Forbid m.room.encrypted if encryption is disabled
    if TimelineEventType::RoomEncrypted == args.event_type.to_string().into()
        && !conf.allow_encryption
    {
        return Err(MatrixError::forbidden("Encryption has been disabled", None).into());
    }

    let payload = req.payload().await?;
    let content = parse_event_content(payload)?;

    let user_id = authed.user_id().to_owned();
    let device_id = authed.device_id().map(ToOwned::to_owned);
    let timestamp = appservice_timestamp(authed.appservice().is_some(), args.timestamp);
    let event_id = run_detached(async move {
        let state_lock = room::lock_state(&args.room_id).await;
        let endpoint = format!("room/send/{}", args.event_type);
        let mut unsigned = BTreeMap::new();
        unsigned.insert(
            "transaction_id".to_owned(),
            to_raw_value(&args.txn_id).expect("TxnId is valid json"),
        );
        // The transaction receipt is looked up and recorded inside the append fence,
        // and a published event whose receipt was lost is recovered on retry.
        let event_id = timeline::build_and_append_pdu_with_transaction(
            PduBuilder {
                event_type: args.event_type.to_string().into(),
                content: to_raw_value(&content)?,
                unsigned,
                timestamp,
                sticky_duration_ms: args.sticky_duration_ms,
                ..Default::default()
            },
            &user_id,
            &args.room_id,
            &crate::room::get_version(&args.room_id).await?,
            &state_lock,
            Some((&endpoint, &args.txn_id, device_id.as_deref())),
        )
        .await?
        .pdu
        .event_id;
        Ok((*event_id).to_owned())
    })
    .await?;

    json_ok(SendMessageResBody::new(event_id))
}

/// #POST /_matrix/client/r0/rooms/{room_id}/send/{event_type}
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is allowed
#[endpoint]
pub(super) async fn post_message(
    _aa: AuthArgs,
    args: CreateMessageReqArgs,
    req: &mut Request,
    depot: &mut Depot,
) -> JsonResult<SendMessageResBody> {
    let authed = depot.authed_info()?;

    let conf = config::get();
    // Forbid m.room.encrypted if encryption is disabled
    if TimelineEventType::RoomEncrypted == args.event_type.to_string().into()
        && !conf.allow_encryption
    {
        return Err(MatrixError::forbidden("Encryption has been disabled", None).into());
    }

    let payload = req.payload().await?;
    let content = parse_event_content(payload)?;

    let user_id = authed.user_id().to_owned();
    let timestamp = appservice_timestamp(authed.appservice().is_some(), args.timestamp);
    let event_id = run_detached(async move {
        let state_lock = room::lock_state(&args.room_id).await;
        let event_id = timeline::build_and_append_pdu(
            PduBuilder {
                event_type: args.event_type.to_string().into(),
                content: to_raw_value(&content)?,
                unsigned: BTreeMap::new(),
                timestamp,
                sticky_duration_ms: args.sticky_duration_ms,
                ..Default::default()
            },
            &user_id,
            &args.room_id,
            &crate::room::get_version(&args.room_id).await?,
            &state_lock,
        )
        .await?
        .pdu
        .event_id;
        Ok((*event_id).to_owned())
    })
    .await?;

    json_ok(SendMessageResBody::new(event_id))
}

/// Run the room side of a client send on its own task. A local append spans several
/// database writes and the federation enqueue; when the client drops the request,
/// salvo drops the handler future, and an append cut off halfway used to leave its
/// event an unpublished outlier that later events still cited. A spawned task runs to
/// completion, so a retry with the same transaction id finds the finished event.
/// TODO(split seam: routing/client/room/send.rs): state PUT and redact still append on
/// the request future; the extremity write now waits for publication, so a cut-off
/// append there no longer strands a cited outlier, but its tail can still be skipped.
async fn run_detached<T, F>(work: F) -> AppResult<T>
where
    F: Future<Output = AppResult<T>> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(work)
        .await
        .map_err(|e| AppError::internal(format!("room send task did not finish: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::appservice_timestamp;
    use crate::core::UnixMillis;

    #[test]
    fn timestamp_massaging_is_limited_to_appservices() {
        let timestamp = UnixMillis(123_456);

        assert_eq!(appservice_timestamp(true, Some(timestamp)), Some(timestamp));
        assert_eq!(appservice_timestamp(false, Some(timestamp)), None);
    }
}
