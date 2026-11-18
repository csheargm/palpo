//! Regression: a local append that stops before publication must not strand its event
//! as an outlier that the room's next event cites. Scar (hagency federation demo,
//! 2026-11-18): an Agent's "Thinking" edit was cut off mid-append, stayed an outlier
//! while already a forward extremity, and the next reply cited it as its only prev. The
//! other server could neither fetch it (no state frame, so "not allowed to see event")
//! nor accept the reply ("Missing predecessors could not be recovered").
use diesel::prelude::*;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use salvo::prelude::*;
use salvo::test::{RequestBuilder, ResponseExt, TestClient};
use serde_json::{Value, json};
use std::time::Duration;

use crate::core::{EventId, OwnedDeviceId, OwnedRoomId, RoomVersionId, UnixMillis, UserId};
use crate::data::schema::{event_forward_extremities, event_points, events};
use crate::{config, data, room};

const SERVER: &str = "stranded-append.example";
const REMOTE: &str = "remote.example";

async fn request(request: RequestBuilder, service: &Service) -> Value {
    let router = service.router();
    let mut response = tokio::spawn(async move { request.send(router).await })
        .await
        .unwrap();
    let body: Value = response.take_json().await.unwrap();
    assert_eq!(response.status_code, Some(StatusCode::OK), "{body}");
    body
}

async fn provision(name: &str) -> crate::core::OwnedUserId {
    let user = UserId::parse(format!("@{name}:{SERVER}")).unwrap();
    data::user::create_user(&data::user::NewDbUser {
        id: user.clone(),
        ty: None,
        is_admin: false,
        is_guest: false,
        is_local: true,
        localpart: name.into(),
        server_name: user.server_name().to_owned(),
        appservice_id: None,
        created_at: UnixMillis::now(),
    })
    .await
    .unwrap();
    data::user::device::create_device(&user, &OwnedDeviceId::from("D"), name, None, None)
        .await
        .unwrap();
    crate::user::get_push_rules(&user).await.unwrap();
    user
}

async fn extremities(room: &OwnedRoomId) -> Vec<String> {
    event_forward_extremities::table
        .filter(event_forward_extremities::room_id.eq(room))
        .select(event_forward_extremities::event_id)
        .load::<String>(&mut data::connect().await.unwrap())
        .await
        .unwrap()
}

async fn is_outlier(event_id: &EventId) -> bool {
    events::table
        .find(event_id)
        .select(events::is_outlier)
        .first::<bool>(&mut data::connect().await.unwrap())
        .await
        .unwrap()
}

/// Save a signed local message as an outlier, the first step of every local append.
async fn local_outlier(
    sender: &UserId,
    room: &OwnedRoomId,
    body: &str,
) -> (crate::SnPduEvent, crate::core::serde::CanonicalJsonObject) {
    let (pdu, json) = crate::event::PduBuilder {
        event_type: "m.room.message".into(),
        content: serde_json::value::to_raw_value(&json!({"msgtype":"m.text","body":body})).unwrap(),
        ..Default::default()
    }
    .hash_sign(sender, room, &RoomVersionId::V11)
    .await
    .unwrap();
    let (pdu, json, _guard) = crate::event::PduBuilder::save_as_outlier(pdu, json, sender)
        .await
        .unwrap();
    (pdu, json)
}

#[test]
#[ignore = "requires an empty dedicated PALPO_TEST_DATABASE_URL; run this test alone"]
fn interrupted_local_append_is_never_cited_and_stays_fetchable() {
    std::thread::Builder::new()
        .name("stranded-append".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(stranded_append());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn stranded_append() {
    crate::test_database::init();
    config::CONFIG
        .set(
            serde_json::from_value(json!({
                "server_name": SERVER, "db": {"url": "unused-test-config"},
                "ip_range_denylist": [], "auto_join_rooms": []
            }))
            .unwrap(),
        )
        .unwrap();
    let alice = provision("alice").await;
    let service = Service::new(crate::routing::root());
    let created = request(
        TestClient::post("http://localhost/_matrix/client/v3/createRoom")
            .add_header("Authorization", "Bearer alice", true)
            .json(&json!({
                "room_version": "11", "preset": "public_chat",
                "initial_state": [{"type": "m.room.history_visibility", "state_key": "",
                    "content": {"history_visibility": "world_readable"}}]
            })),
        &service,
    )
    .await;
    let room: OwnedRoomId = created["room_id"].as_str().unwrap().try_into().unwrap();
    let published_tip = extremities(&room).await;
    assert_eq!(published_tip.len(), 1);

    // 1. An append that fails after its outlier is saved, here because another
    //    transaction holds its event_points row past the statement timeout.
    let (stranded, stranded_json) = local_outlier(&alice, &room, "cut off").await;
    let mut blocker = data::connect().await.unwrap();
    blocker
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM event_points WHERE event_id = '{}' FOR UPDATE",
            stranded.event_id
        ))
        .await
        .unwrap();
    let failed =
        room::timeline::append_pdu(&stranded, stranded_json, &room::lock_state(&room).await).await;
    blocker.batch_execute("ROLLBACK").await.unwrap();
    assert!(failed.is_err(), "the blocked append was expected to fail");
    assert!(is_outlier(&stranded.event_id).await);
    assert_eq!(
        extremities(&room).await,
        published_tip,
        "an unpublished outlier became a forward extremity"
    );

    // 2. The room's next event must not cite the unpublished outlier.
    let next = request(
        TestClient::put(format!(
            "http://localhost/_matrix/client/v3/rooms/{room}/send/m.room.message/after-cut"
        ))
        .add_header("Authorization", "Bearer alice", true)
        .json(&json!({"msgtype":"m.text","body":"next"})),
        &service,
    )
    .await;
    let next_id = EventId::parse(next["event_id"].as_str().unwrap()).unwrap();
    let next_pdu = room::timeline::get_pdu(&next_id).await.unwrap();
    assert!(
        !next_pdu.prev_events.contains(&stranded.event_id),
        "the next event cited an outlier no other server can fetch"
    );
    assert_eq!(extremities(&room).await, vec![next_id.to_string()]);

    // 3. A frameless outlier whose single prev is published is judged against the
    //    state after that prev, so a remote server can fetch it through
    //    get_missing_events. Events stranded by older builds heal this way.
    let remote = crate::core::ServerName::parse(REMOTE).unwrap();
    assert!(
        room::state::server_can_see_event(&remote, &room, &stranded.event_id)
            .await
            .unwrap(),
        "a world-readable outlier with a published prev was hidden from federation"
    );
    let unpublished_frame = event_points::table
        .find(&*stranded.event_id)
        .select(event_points::frame_id)
        .first::<Option<i64>>(&mut data::connect().await.unwrap())
        .await
        .unwrap();
    assert_eq!(unpublished_frame, None, "fixture outlier gained a frame");
    // The same rule for a rejected outlier, and for one whose only prev has no
    // frame either, keeps the event hidden: its state cannot be known.
    let mut rejected = stranded.clone();
    rejected.pdu.rejection_reason = Some("fixture rejected verdict".into());
    assert_eq!(
        room::state::frame_after_only_prev(&rejected).await.unwrap(),
        None
    );
    let mut orphan = stranded.clone();
    orphan.pdu.prev_events = vec![EventId::parse("$never-stored:remote.example").unwrap()];
    assert_eq!(
        room::state::frame_after_only_prev(&orphan).await.unwrap(),
        None
    );

    // 4. A client that drops its send request mid-append must not cut the append off.
    //    Hold the room's cross-process append fence so the send is parked inside the
    //    append, drop the request, then release the fence: the send still finishes.
    let mut fence = data::connect().await.unwrap();
    fence.batch_execute("BEGIN").await.unwrap();
    crate::data::room::timeline::lock_event_append(&mut fence, &room)
        .await
        .unwrap();
    let dropped = TestClient::put(format!(
        "http://localhost/_matrix/client/v3/rooms/{room}/send/m.room.message/dropped-send"
    ))
    .add_header("Authorization", "Bearer alice", true)
    .json(&json!({"msgtype":"m.text","body":"client went away"}))
    .send(service.router());
    assert!(
        tokio::time::timeout(Duration::from_millis(300), dropped)
            .await
            .is_err(),
        "the send was expected to wait for the held append fence"
    );
    fence.batch_execute("ROLLBACK").await.unwrap();
    let txn: crate::core::OwnedTransactionId = "dropped-send".into();
    let device = OwnedDeviceId::from("D");
    let mut finished = None;
    for _ in 0..100 {
        finished = data::room::transaction_id::get_scoped_event_id(
            &txn,
            &alice,
            Some(&*device),
            Some(&room),
            "room/send/m.room.message",
        )
        .await
        .unwrap();
        if finished.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let finished = finished.expect("a dropped send request abandoned its append");
    assert!(
        !is_outlier(&finished).await,
        "the dropped send left an outlier"
    );
    assert_eq!(extremities(&room).await, vec![finished.to_string()]);
}
