use std::time::Duration;

use crate::core::UnixMillis;
use crate::core::events::SyncEphemeralRoomEvent;
use crate::core::events::typing::{TypingContent, TypingEventContent};
use crate::core::federation::transaction::Edu;
use crate::core::identifiers::*;
use crate::{AppResult, IsRemoteOrLocal, data, sending};

/// How often passed typing timeouts are published to sync.
const TYPING_EXPIRY_SWEEP: Duration = Duration::from_secs(1);

/// Sets a user as typing until the timeout timestamp is reached or remove_typing is called.
/// State is persisted to the database for cross-instance visibility.
pub async fn add_typing(
    user_id: &UserId,
    room_id: &RoomId,
    timeout: u64,
    broadcast: bool,
) -> AppResult<()> {
    let event_sn = data::next_sn().await?;
    data::room::typing::upsert_typing(room_id, user_id, timeout as i64, event_sn).await?;

    data::sync_notify::notify_room(room_id);

    if broadcast && user_id.is_local() {
        federation_send(room_id, user_id, true).await.ok();
    }
    Ok(())
}

/// Removes a user from typing before the timeout is reached.
/// Instead of deleting the row, it is marked stopped and occur_sn is bumped
/// so that sync picks up the "typing stopped" change before cleanup.
pub async fn remove_typing(user_id: &UserId, room_id: &RoomId, broadcast: bool) -> AppResult<()> {
    let event_sn = data::next_sn().await?;
    data::room::typing::stop_typing(room_id, user_id, event_sn, UnixMillis::now().get() as i64)
        .await?;

    data::sync_notify::notify_room(room_id);

    if broadcast && user_id.is_local() {
        federation_send(room_id, user_id, false).await.ok();
    }
    Ok(())
}

/// Publish typing timeouts that have passed and wake the affected rooms' syncs.
/// Without this, a timeout (the normal end of a remote user's typing, since
/// federation rarely sends an explicit stop) never changed the typing stream,
/// so clients kept showing "X is typing…".
pub async fn publish_expired_typings() -> AppResult<()> {
    let mut rooms =
        data::room::typing::publish_expired_typings(UnixMillis::now().get() as i64).await?;
    rooms.sort_unstable();
    rooms.dedup();
    for room_id in &rooms {
        data::sync_notify::notify_room(room_id);
    }
    Ok(())
}

/// Run [`publish_expired_typings`] for the life of the process.
pub fn start_expiry_sweeper() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(TYPING_EXPIRY_SWEEP);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(e) = publish_expired_typings().await {
                // Retried on the next tick; a lost tick only delays the expiry.
                tracing::warn!(error = ?e, "failed to publish expired typing notifications");
            }
        }
    });
}

/// Removes expired typing entries from the database.
async fn maintain_typings(room_id: &RoomId) -> AppResult<()> {
    let current_timestamp = UnixMillis::now().get() as i64;
    data::room::typing::delete_expired_typings(room_id, current_timestamp).await?;
    Ok(())
}

/// Returns the sequence number of the last typing update in this room.
/// Reads from the database so it works across all instances.
/// We query max occur_sn BEFORE cleanup so that "stop typing" events
/// (rows kept, marked stopped) are still visible to sync.
pub async fn last_typing_update(room_id: &RoomId) -> AppResult<i64> {
    Ok(data::room::typing::last_typing_sn(room_id).await?)
}

/// Returns a new typing EDU with currently typing users from the database.
pub async fn all_typings(
    room_id: &RoomId,
) -> AppResult<SyncEphemeralRoomEvent<TypingEventContent>> {
    maintain_typings(room_id).await?;
    let user_ids =
        data::room::typing::typing_user_ids(room_id, UnixMillis::now().get() as i64).await?;

    Ok(SyncEphemeralRoomEvent {
        content: TypingEventContent { user_ids },
    })
}

async fn federation_send(room_id: &RoomId, user_id: &UserId, typing: bool) -> AppResult<()> {
    debug_assert!(
        user_id.is_local(),
        "tried to broadcast typing status of remote user",
    );

    if !crate::config::get().typing.allow_outgoing {
        return Ok(());
    }

    let content = TypingContent::new(room_id.to_owned(), user_id.to_owned(), typing);
    let edu = Edu::Typing(content);
    sending::send_edu_room(room_id, &edu).await?;

    Ok(())
}

#[cfg(test)]
mod database_tests {
    use std::time::Duration;

    use super::*;

    // Not skipped in CI: .github/workflows/quality-control.yml "PostgreSQL
    // regression tests" creates an empty palpo_test database and runs every
    // `database_` test with `-- --ignored`. Locally: PALPO_TEST_DATABASE_URL=<empty db>
    // cargo test -p palpo --all-features database_ -- --ignored --test-threads=1
    #[tokio::test]
    #[ignore = "requires an empty dedicated PALPO_TEST_DATABASE_URL"]
    async fn database_typing_expiry_bumps_the_sequence_once_and_wakes_sync() {
        crate::test_database::init();
        let room: OwnedRoomId = "!typing-expiry:example.org".try_into().unwrap();
        let typer: OwnedUserId = "@typer:example.org".try_into().unwrap();
        let stopper: OwnedUserId = "@stopper:example.org".try_into().unwrap();
        let now = UnixMillis::now().get() as i64;
        let watcher = data::sync_notify::register(&stopper, "W".into(), false);
        watcher.set_rooms(vec![room.clone()]);

        // Typing whose timeout has already passed: no longer listed, not yet published.
        data::room::typing::upsert_typing(&room, &typer, now - 1, 10)
            .await
            .unwrap();
        assert!(all_typings(&room).await.unwrap().content.user_ids.is_empty());
        assert_eq!(last_typing_update(&room).await.unwrap(), 10);

        publish_expired_typings().await.unwrap();
        let expired_sn = last_typing_update(&room).await.unwrap();
        assert!(expired_sn > 10, "expiry did not bump the typing sequence");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), watcher.notified())
                .await
                .is_ok(),
            "expiry did not wake the room's sync"
        );

        // Published once: later sweeps leave the sequence alone.
        publish_expired_typings().await.unwrap();
        assert_eq!(last_typing_update(&room).await.unwrap(), expired_sn);

        // An explicit stop is already published and is not re-announced as an expiry.
        add_typing(&stopper, &room, (now + 60_000) as u64, false)
            .await
            .unwrap();
        assert_eq!(
            all_typings(&room).await.unwrap().content.user_ids,
            vec![stopper.clone()]
        );
        remove_typing(&stopper, &room, false).await.unwrap();
        let stopped_sn = last_typing_update(&room).await.unwrap();
        assert!(stopped_sn > expired_sn);
        publish_expired_typings().await.unwrap();
        assert_eq!(last_typing_update(&room).await.unwrap(), stopped_sn);

        // Stopped and expired rows survive the cleanup grace period.
        assert!(all_typings(&room).await.unwrap().content.user_ids.is_empty());
        assert_eq!(last_typing_update(&room).await.unwrap(), stopped_sn);
    }
}
