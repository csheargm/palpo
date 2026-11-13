//! Long-poll wake-up for `/sync`.
//!
//! A sync that has nothing to return waits here until something it can see changes.
//! Writers wake it in-process through [`crate::data::sync_notify`]; the database
//! snapshot below stays the correctness check, so a signal only ends the sleep and
//! the snapshot decides whether to answer. A slow fallback recheck covers writers that
//! do not signal (another server process sharing the database).
//!
//! TODO(split): move `WatchSnapshot`, its SQL and `database_tests` into
//! `watcher/snapshot.rs`, leaving waiter orchestration here (file is over budget).

use std::time::Duration;

use diesel_async::RunQueryDsl;

use crate::AppResult;
use crate::core::Seqnum;
use crate::core::identifiers::*;
use crate::data::connect;
use crate::data::sync_notify::{self, SyncWaiter};

/// Recheck without a signal this often. Upstream polled at this interval for every
/// wake-up; now it only bounds the delay for writes made by another process.
const FALLBACK_RECHECK: Duration = Duration::from_secs(3);

#[derive(Debug, diesel::QueryableByName)]
struct WatchSnapshot {
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Text>)]
    room_ids: Vec<String>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    typing_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    receipt_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    inbox_id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    key_change_id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    room_key_change_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    membership_id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    event_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    account_data_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    profile_sn: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    presence_sn: i64,
}

impl WatchSnapshot {
    fn has_updates_since(&self, previous: &Self) -> bool {
        self.typing_sn > previous.typing_sn
            || self.receipt_sn > previous.receipt_sn
            || self.inbox_id > previous.inbox_id
            || self.key_change_id > previous.key_change_id
            || self.room_key_change_sn > previous.room_key_change_sn
            || self.membership_id > previous.membership_id
            || self.event_sn > previous.event_sn
            || self.account_data_sn > previous.account_data_sn
            || self.profile_sn > previous.profile_sn
            || self.presence_sn > previous.presence_sn
    }

    fn joined_rooms(&self) -> Vec<OwnedRoomId> {
        self.room_ids
            .iter()
            .filter_map(|room_id| RoomId::parse(room_id).ok())
            .collect()
    }
}

/// Which optional streams a waiting sync delivers, so it is woken only by them.
#[derive(Clone, Copy, Debug, Default)]
pub struct WatchStreams {
    /// MSC4262 profile updates (sliding sync).
    pub profiles: bool,
    /// Presence of users sharing a joined room (v3 sync with local presence on).
    pub presence: bool,
}

async fn load_watch_snapshot(
    user_id: &UserId,
    device_id: &DeviceId,
    streams: WatchStreams,
) -> AppResult<WatchSnapshot> {
    // One statement gives all streams a consistent snapshot instead of issuing
    // a round trip per stream.
    diesel::sql_query(
        "WITH joined_rooms AS (
            SELECT room_id FROM (
                SELECT DISTINCT ON (room_id) room_id, membership FROM room_users
                WHERE user_id = $1 ORDER BY room_id, id DESC
            ) AS memberships WHERE membership = 'join'
        ),
        shared_members AS (
            SELECT user_id FROM (
                SELECT DISTINCT ON (user_id, room_id) user_id, room_id, membership
                FROM room_users WHERE room_id IN (SELECT room_id FROM joined_rooms)
                ORDER BY user_id, room_id, id DESC
            ) AS members WHERE membership = 'join'
        )
        SELECT
          ARRAY(SELECT room_id FROM joined_rooms) AS room_ids,
          COALESCE((SELECT MAX(occur_sn) FROM room_typings WHERE room_id IN (SELECT room_id FROM joined_rooms)), 0) AS typing_sn,
          COALESCE((SELECT MAX(sn) FROM event_receipts WHERE room_id IN (SELECT room_id FROM joined_rooms) AND (ty <> 'm.read.private' OR user_id = $1)), 0) AS receipt_sn,
          COALESCE((SELECT MAX(id) FROM device_inboxes WHERE user_id = $1 AND device_id = $2), 0) AS inbox_id,
          COALESCE((SELECT MAX(id) FROM e2e_key_changes WHERE user_id = $1), 0) AS key_change_id,
          COALESCE((SELECT MAX(occur_sn) FROM e2e_key_changes WHERE room_id IN (SELECT room_id FROM joined_rooms)), 0) AS room_key_change_sn,
          COALESCE((SELECT MAX(id) FROM room_users WHERE user_id = $1), 0) AS membership_id,
          COALESCE((SELECT MAX(live_published_sn) FROM event_points WHERE live_published_sn IS NOT NULL AND room_id IN (SELECT room_id FROM joined_rooms)), 0) AS event_sn,
          COALESCE((SELECT MAX(occur_sn) FROM user_datas WHERE user_id = $1), 0) AS account_data_sn,
          CASE WHEN $3 THEN COALESCE((
            SELECT MAX(occur_sn) FROM user_profile_changes
            WHERE user_id = $1 OR user_id IN (SELECT user_id FROM shared_members)
          ), 0) ELSE 0 END AS profile_sn,
          CASE WHEN $4 THEN COALESCE((
            SELECT MAX(occur_sn) FROM user_presences
            WHERE user_id <> $1 AND user_id IN (SELECT user_id FROM shared_members)
          ), 0) ELSE 0 END AS presence_sn",
    )
    .bind::<diesel::sql_types::Text, _>(user_id.as_str())
    .bind::<diesel::sql_types::Text, _>(device_id.as_str())
    .bind::<diesel::sql_types::Bool, _>(streams.profiles)
    .bind::<diesel::sql_types::Bool, _>(streams.presence)
    .get_result(&mut connect().await?).await.map_err(Into::into)
}

#[cfg(test)]
pub(crate) async fn live_position_for_test(
    user_id: &UserId,
    device_id: &DeviceId,
) -> AppResult<i64> {
    Ok(load_watch_snapshot(user_id, device_id, WatchStreams::default())
        .await?
        .event_sn)
}

fn profile_change_is_ready(
    profile_updates: bool,
    profile_after_sn: Option<Seqnum>,
    latest_change_sn: Seqnum,
) -> bool {
    profile_updates && profile_after_sn.is_some_and(|after_sn| latest_change_sn >= after_sn)
}

/// A pending long-poll. Take it with [`prepare`] *before* computing the sync
/// response, so a write that lands while the response is being built is newer than
/// the baseline and ends the wait, instead of being absorbed into a later baseline.
pub struct SyncWatch {
    waiter: SyncWaiter,
    baseline: WatchSnapshot,
    streams: WatchStreams,
}

pub async fn prepare(
    user_id: &UserId,
    device_id: &DeviceId,
    streams: WatchStreams,
) -> AppResult<SyncWatch> {
    // Register first: any write committed after the baseline read signals us.
    let waiter = sync_notify::register(user_id, device_id, streams.profiles);
    let baseline = load_watch_snapshot(user_id, device_id, streams).await?;
    waiter.set_rooms(baseline.joined_rooms());
    Ok(SyncWatch {
        waiter,
        baseline,
        streams,
    })
}

/// Wait for `watch` at most `duration`. Reaching the client's timeout is the normal
/// end of an idle long-poll; a watcher failure (its snapshot query) is returned, not
/// mistaken for a wake-up that would answer with an empty response.
pub async fn wait_bounded(
    duration: Duration,
    watch: impl Future<Output = AppResult<()>>,
) -> AppResult<()> {
    match tokio::time::timeout(duration, watch).await {
        Ok(result) => result,
        Err(_elapsed) => Ok(()),
    }
}

#[derive(Debug, PartialEq)]
enum Wake {
    Signal,
    Recheck,
    Shutdown,
}

/// Sleep until a writer signals, the fallback recheck is due, or the server rotates.
async fn next_wake(
    waiter: &SyncWaiter,
    shutdown: std::pin::Pin<&mut impl Future<Output = ()>>,
    fallback: Duration,
) -> Wake {
    tokio::select! {
        _ = shutdown => Wake::Shutdown,
        _ = waiter.notified() => Wake::Signal,
        _ = tokio::time::sleep(fallback) => Wake::Recheck,
    }
}

impl SyncWatch {
    /// Return once anything visible to this device changed since [`prepare`], or on
    /// shutdown. The caller bounds the wait with the client's timeout.
    pub async fn wait(
        self,
        user_id: &UserId,
        device_id: &DeviceId,
        profile_after_sn: Option<Seqnum>,
    ) -> AppResult<()> {
        let shutdown = crate::ROTATE.watch();
        tokio::pin!(shutdown);
        loop {
            let current = load_watch_snapshot(user_id, device_id, self.streams).await?;
            if current.has_updates_since(&self.baseline)
                || profile_change_is_ready(
                    self.streams.profiles,
                    profile_after_sn,
                    current.profile_sn,
                )
            {
                return Ok(());
            }
            if next_wake(&self.waiter, shutdown.as_mut(), FALLBACK_RECHECK).await == Wake::Shutdown {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHORT: Duration = Duration::from_millis(50);

    fn never() -> impl Future<Output = ()> {
        std::future::pending()
    }

    #[tokio::test]
    async fn a_writer_signal_wakes_the_waiting_sync() {
        let user: OwnedUserId = "@wake:watcher.test".try_into().unwrap();
        let waiter = sync_notify::register(&user, "W".into(), false);
        waiter.set_rooms(vec!["!room:watcher.test".try_into().unwrap()]);
        let shutdown = never();
        tokio::pin!(shutdown);
        let (_, wake) = tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                sync_notify::notify_room("!room:watcher.test".try_into().unwrap());
            },
            next_wake(&waiter, shutdown.as_mut(), Duration::from_secs(30))
        );
        assert_eq!(wake, Wake::Signal);
    }

    #[tokio::test]
    async fn without_a_signal_the_wait_ends_only_at_the_fallback_recheck() {
        let user: OwnedUserId = "@idle:watcher.test".try_into().unwrap();
        let waiter = sync_notify::register(&user, "I".into(), false);
        let shutdown = never();
        tokio::pin!(shutdown);
        let started = std::time::Instant::now();
        assert_eq!(next_wake(&waiter, shutdown.as_mut(), SHORT).await, Wake::Recheck);
        assert!(started.elapsed() >= SHORT);
    }

    #[tokio::test]
    async fn server_rotation_ends_the_wait() {
        let user: OwnedUserId = "@rotate:watcher.test".try_into().unwrap();
        let waiter = sync_notify::register(&user, "R".into(), false);
        let rotation = crate::RotationHandler::new();
        let shutdown = rotation.watch();
        tokio::pin!(shutdown);
        rotation.fire();
        assert_eq!(
            next_wake(&waiter, shutdown.as_mut(), Duration::from_secs(30)).await,
            Wake::Shutdown
        );
    }

    #[tokio::test]
    async fn an_idle_wait_ends_quietly_at_the_client_timeout() {
        let idle = std::future::pending::<AppResult<()>>();
        assert!(wait_bounded(SHORT, idle).await.is_ok());
    }

    #[tokio::test]
    async fn a_watcher_failure_is_returned_not_treated_as_a_wakeup() {
        let failing = async { Err(crate::AppError::public("snapshot query failed")) };
        assert!(wait_bounded(Duration::from_secs(30), failing).await.is_err());
    }

    #[test]
    fn profile_changes_at_or_after_the_response_token_are_already_ready() {
        assert!(profile_change_is_ready(true, Some(10), 10));
        assert!(profile_change_is_ready(true, Some(10), 11));
        assert!(!profile_change_is_ready(true, Some(10), 9));
        assert!(!profile_change_is_ready(false, Some(10), 11));
        assert!(!profile_change_is_ready(true, None, 11));
    }
}

#[cfg(test)]
mod database_tests {
    use super::*;

    const ALL: WatchStreams = WatchStreams {
        profiles: true,
        presence: true,
    };

    #[tokio::test]
    #[ignore = "requires an empty dedicated PALPO_TEST_DATABASE_URL"]
    async fn database_watch_snapshot_covers_streams_without_unrelated_wakeups() {
        crate::test_database::init();
        let user: OwnedUserId = "@watch:example.org".try_into().unwrap();
        let device: &DeviceId = "WATCH".into();
        diesel::sql_query("INSERT INTO room_users (event_id,event_sn,room_id,user_id,user_server_id,sender_id,membership,created_at) VALUES ('$watch-member',1,'!watch:example.org','@watch:example.org','example.org','@watch:example.org','join',0),('$watch-peer',2,'!watch:example.org','@peer:example.org','example.org','@peer:example.org','join',0)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        let mut previous = load_watch_snapshot(&user, device, ALL).await.unwrap();
        assert!(!previous.has_updates_since(&previous));
        assert_eq!(previous.joined_rooms(), vec![OwnedRoomId::try_from("!watch:example.org").unwrap()]);
        // A different device's inbox and an unrelated profile are irrelevant.
        diesel::sql_query("INSERT INTO device_inboxes(user_id,device_id,json_data,created_at) VALUES ('@watch:example.org','OTHER','{}',0)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        diesel::sql_query("INSERT INTO user_profile_changes(occur_sn,user_id,field,value,removed) VALUES (900,'@stranger:example.org','displayname','\"unrelated\"',false)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        // Own presence is not delivered to oneself; a stranger's is not visible.
        diesel::sql_query("INSERT INTO user_presences(user_id,state,occur_sn,updated_at) VALUES ('@watch:example.org','online',902,0),('@stranger:example.org','online',903,0)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        // A peer's private read receipt is not visible to this user.
        diesel::sql_query("INSERT INTO event_receipts(sn,ty,room_id,user_id,event_id,event_sn,json_data,receipt_at) VALUES (901,'m.read.private','!watch:example.org','@peer:example.org','$watch-member',1,'{}',0)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        assert!(
            !load_watch_snapshot(&user, device, ALL)
                .await
                .unwrap()
                .has_updates_since(&previous)
        );
        for sql in [
            "INSERT INTO device_inboxes(user_id,device_id,json_data,created_at) VALUES ('@watch:example.org','WATCH','{}',0)",
            "INSERT INTO room_typings(room_id,user_id,timeout_at,occur_sn) VALUES ('!watch:example.org','@peer:example.org',999999,20)",
            "INSERT INTO event_receipts(sn,ty,room_id,user_id,event_id,event_sn,json_data,receipt_at) VALUES (25,'m.read','!watch:example.org','@peer:example.org','$watch-member',1,'{}',0)",
            "INSERT INTO e2e_key_changes(user_id,room_id,occur_sn,changed_at) VALUES ('@peer:example.org','!watch:example.org',35,0)",
            "INSERT INTO e2e_key_changes(user_id,occur_sn,changed_at) VALUES ('@watch:example.org',30,0)",
            "INSERT INTO event_points(event_id,event_sn,room_id,frame_id,live_published_sn,live_published_frame_id) VALUES ('$watch-event',40,'!watch:example.org',1,40,1)",
            "INSERT INTO user_datas(user_id,data_type,json_data,is_deleted,occur_sn,created_at) VALUES ('@watch:example.org','m.push_rules','{}',false,50,0)",
            "INSERT INTO user_profile_changes(occur_sn,user_id,field,value,removed) VALUES (60,'@watch:example.org','displayname','\"self\"',false)",
            "INSERT INTO user_profile_changes(occur_sn,user_id,field,value,removed) VALUES (70,'@peer:example.org','displayname','\"shared\"',false)",
            "INSERT INTO user_presences(user_id,state,occur_sn,updated_at) VALUES ('@peer:example.org','online',75,0)",
            "INSERT INTO room_users(event_id,event_sn,room_id,user_id,user_server_id,sender_id,membership,created_at) VALUES ('$watch-new',80,'!new:example.org','@watch:example.org','example.org','@watch:example.org','join',0)",
        ] {
            diesel::sql_query(sql)
                .execute(&mut connect().await.unwrap())
                .await
                .unwrap();
            let current = load_watch_snapshot(&user, device, ALL).await.unwrap();
            assert!(
                current.has_updates_since(&previous),
                "did not observe {sql}"
            );
            previous = current;
        }
        assert_eq!(previous.profile_sn, 70);
        assert_eq!(previous.presence_sn, 75);
        assert_eq!(
            load_watch_snapshot(&user, device, WatchStreams::default())
                .await
                .unwrap()
                .profile_sn,
            0
        );
        // Events in other rooms and events not yet promoted to the timeline do not wake.
        diesel::sql_query("INSERT INTO event_points(event_id,event_sn,room_id,frame_id,live_published_sn,live_published_frame_id) VALUES ('$watch-outside',1000,'!outside:example.org',1,1000,1),('$watch-outlier',1001,'!watch:example.org',NULL,NULL,NULL),('$watch-cached',1002,'!watch:example.org',1,NULL,NULL)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        assert!(
            !load_watch_snapshot(&user, device, ALL)
                .await
                .unwrap()
                .has_updates_since(&previous)
        );
    }

    #[tokio::test]
    #[ignore = "requires an empty dedicated PALPO_TEST_DATABASE_URL"]
    async fn database_presence_only_update_wakes_a_room_peer_sync() {
        crate::test_database::init();
        diesel::sql_query("INSERT INTO room_users (event_id,event_sn,room_id,user_id,user_server_id,sender_id,membership,created_at) VALUES ('$presence-peer',5001,'!presence:example.org','@presence-peer:example.org','example.org','@presence-peer:example.org','join',0)")
            .execute(&mut connect().await.unwrap()).await.unwrap();
        let watcher: OwnedUserId = "@presence-watch:example.org".try_into().unwrap();
        let waiter = sync_notify::register(&watcher, "P".into(), false);
        waiter.set_rooms(vec!["!presence:example.org".try_into().unwrap()]);
        let presence = crate::data::user::NewDbPresence {
            user_id: "@presence-peer:example.org".try_into().unwrap(),
            stream_id: None,
            state: Some("online".to_owned()),
            status_msg: None,
            last_active_at: None,
            last_federation_update_at: None,
            last_user_sync_at: None,
            currently_active: Some(true),
            occur_sn: None,
            updated_at: crate::core::UnixMillis::now(),
        };
        assert!(crate::data::user::set_presence(presence, false).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), waiter.notified())
                .await
                .is_ok(),
            "a presence-only change did not wake the peer's waiting sync"
        );
    }
}
