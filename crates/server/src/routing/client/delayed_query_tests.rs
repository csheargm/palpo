//! HTTP/PostgreSQL coverage for the MSC4140 `?org.matrix.msc4140.delay=` form of
//! `PUT /send` and `PUT /state`. Driven from `delayed_sticky_body` (one shared DB).
use salvo::prelude::*;
use salvo::test::TestClient;
use serde_json::{Value, json};

use super::{ok, send};
use crate::core::{OwnedRoomId, UserId};
use crate::data;

const V3: &str = "http://localhost/_matrix/client/v3";

/// Element Call's delayed leave: `PUT /state/...?org.matrix.msc4140.delay=` must not
/// apply the state at once (it used to wipe the caller's own call membership).
pub(super) async fn spec_delay_query_schedules_instead_of_sending(
    service: &Service,
    room: &OwnedRoomId,
    base: &str,
) {
    let state_url = format!("{V3}/rooms/{room}/state/org.matrix.msc3401.call.member/_owner_D");
    let joined = ok(
        service,
        TestClient::put(&state_url).json(&json!({"application":"m.call"})),
    )
    .await;
    assert!(joined["event_id"].is_string());
    let delayed = ok(
        service,
        TestClient::put(format!("{state_url}?org.matrix.msc4140.delay=60000")).json(&json!({})),
    )
    .await;
    let delay_id = delayed["delay_id"].as_str().unwrap();
    assert!(delayed.get("event_id").is_none());
    let current = ok(service, TestClient::get(&state_url)).await;
    assert_eq!(
        current["application"], "m.call",
        "delayed leave applied immediately"
    );
    ok(
        service,
        TestClient::post(format!("{base}/delayed_events/{delay_id}/send")),
    )
    .await;
    assert_eq!(ok(service, TestClient::get(&state_url)).await, json!({}));
    let send_url =
        format!("{V3}/rooms/{room}/send/m.room.message/spec-delay?org.matrix.msc4140.delay=60000");
    let body = json!({"msgtype":"m.text","body":"later"});
    let scheduled = ok(service, TestClient::put(&send_url).json(&body)).await;
    assert_eq!(
        ok(service, TestClient::put(&send_url).json(&body)).await,
        scheduled
    );
    let (status, _) = send(
        service,
        TestClient::put(format!("{state_url}?org.matrix.msc4140.delay=0")).json(&json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A delayed `/send` txn_id is scoped by room and event type like the immediate
/// path, is distinct from the unstable endpoint's txn_ids, and cannot be reused
/// across delayed and immediate modes.
pub(super) async fn delayed_send_txn_scope_and_mode(
    service: &Service,
    room: &OwnedRoomId,
    base: &str,
) {
    let other: OwnedRoomId = ok(
        service,
        TestClient::post(format!("{V3}/createRoom")).json(&json!({"preset":"public_chat"})),
    )
    .await["room_id"]
        .as_str()
        .unwrap()
        .try_into()
        .unwrap();
    let body = json!({"msgtype":"m.text","body":"scoped"});
    let delayed = |room: &OwnedRoomId, ty: &str| {
        format!("{V3}/rooms/{room}/send/{ty}/scope-txn?org.matrix.msc4140.delay=60000")
    };
    let mut ids = Vec::new();
    for url in [
        delayed(room, "m.room.message"),
        delayed(&other, "m.room.message"),
        delayed(room, "org.example.custom"),
        format!("{base}/rooms/{room}/delayed_event/m.room.message/scope-txn"),
    ] {
        let req = if url.contains("/delayed_event/") {
            json!({"delay_ms":60000, "content":body})
        } else {
            body.clone()
        };
        let res = ok(service, TestClient::put(&url).json(&req)).await;
        ids.push(res["delay_id"].as_str().unwrap().to_owned());
    }
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        4,
        "a txn_id in another scope returned an unrelated delay_id"
    );

    let mode_conflict = |status: StatusCode, body: Value| {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["errcode"], "M_INVALID_PARAM");
    };
    let immediate = format!("{V3}/rooms/{room}/send/m.room.message/scope-txn");
    let (status, res) = send(service, TestClient::put(&immediate).json(&body)).await;
    mode_conflict(status, res);
    let first = format!("{V3}/rooms/{room}/send/m.room.message/immediate-first");
    assert!(ok(service, TestClient::put(&first).json(&body)).await["event_id"].is_string());
    let (status, res) = send(
        service,
        TestClient::put(format!("{first}?org.matrix.msc4140.delay=60000")).json(&body),
    )
    .await;
    mode_conflict(status, res);
}

/// A joined user's delayed member update carrying `join_authorised_via_users_server`
/// is accepted, stored unchanged, and stripped at fire time like immediate `/state`.
pub(super) async fn delayed_member_strips_redundant_authoriser(
    service: &Service,
    room: &OwnedRoomId,
    base: &str,
    user: &UserId,
) {
    let url = format!("{V3}/rooms/{room}/state/m.room.member/{user}");
    let content = json!({"membership":"join","displayname":"later",
        "join_authorised_via_users_server":user});
    let res = ok(
        service,
        TestClient::put(format!("{url}?org.matrix.msc4140.delay=60000")).json(&content),
    )
    .await;
    let delay_id = res["delay_id"].as_str().unwrap();
    let row = data::room::delayed_event::get_by_delay_id(user, delay_id)
        .await
        .unwrap()
        .unwrap();
    // Stored as sent: the target may leave before firing and need the authorizer.
    assert_eq!(row.content, content);
    ok(
        service,
        TestClient::post(format!("{base}/delayed_events/{delay_id}/send")),
    )
    .await;
    let current = ok(service, TestClient::get(&url)).await;
    assert_eq!(current["displayname"], "later");
    assert!(
        current.get("join_authorised_via_users_server").is_none(),
        "{current}"
    );
}
