//! In-process wake-ups for long-polling `/sync`.
//!
//! A waiting sync registers what it can be woken by: its user, its device, the rooms the
//! user had joined when the wait began, and whether it follows profile changes. Writers
//! signal after their transaction commits. A signal only removes the waiting: the waiter
//! re-reads its database snapshot and goes back to sleep when nothing it can see changed,
//! so a spurious or over-broad signal costs one query, never a wrong response.
//!
//! Signals are per process. A writer in another server process sharing the database is
//! picked up by the waiter's fallback recheck instead.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use tokio::sync::Notify;

use crate::core::identifiers::*;

struct Waiter {
    user_id: OwnedUserId,
    device_id: OwnedDeviceId,
    rooms: Vec<OwnedRoomId>,
    profiles: bool,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct Registry {
    next_id: u64,
    waiters: HashMap<u64, Waiter>,
    by_user: HashMap<OwnedUserId, HashSet<u64>>,
    by_room: HashMap<OwnedRoomId, HashSet<u64>>,
    profile_waiters: HashSet<u64>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);

fn registry() -> MutexGuard<'static, Registry> {
    // A panic while holding the lock leaves only index sets behind; they remain
    // internally consistent enough to keep serving wake-ups, so recover the guard.
    REGISTRY.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn unindex<K: Hash + Eq>(index: &mut HashMap<K, HashSet<u64>>, key: &K, id: u64) {
    if let Some(ids) = index.get_mut(key) {
        ids.remove(&id);
        if ids.is_empty() {
            index.remove(key);
        }
    }
}

/// A registered `/sync` waiter. Dropping it unregisters, so an abandoned request
/// (client disconnect, timeout) leaves nothing behind.
pub struct SyncWaiter {
    id: u64,
    notify: Arc<Notify>,
}

impl SyncWaiter {
    /// Resolves once a signal arrived since registration or the last call. A signal
    /// sent before this is awaited is kept as a permit, so none is lost in between.
    pub async fn notified(&self) {
        self.notify.notified().await
    }

    /// Replace the rooms this waiter is woken by (the user's joined rooms).
    pub fn set_rooms(&self, rooms: Vec<OwnedRoomId>) {
        let mut registry = registry();
        let Registry { waiters, by_room, .. } = &mut *registry;
        let Some(waiter) = waiters.get_mut(&self.id) else {
            return;
        };
        for room in waiter.rooms.drain(..) {
            unindex(by_room, &room, self.id);
        }
        for room in &rooms {
            by_room.entry(room.clone()).or_default().insert(self.id);
        }
        waiter.rooms = rooms;
    }
}

impl Drop for SyncWaiter {
    fn drop(&mut self) {
        let mut registry = registry();
        let Some(waiter) = registry.waiters.remove(&self.id) else {
            return;
        };
        unindex(&mut registry.by_user, &waiter.user_id, self.id);
        for room in waiter.rooms {
            unindex(&mut registry.by_room, &room, self.id);
        }
        if waiter.profiles {
            registry.profile_waiters.remove(&self.id);
        }
    }
}

/// Register a `/sync` waiter for `user_id`/`device_id`. Rooms are added with
/// [`SyncWaiter::set_rooms`] once the caller knows them.
pub fn register(user_id: &UserId, device_id: &DeviceId, profiles: bool) -> SyncWaiter {
    let notify = Arc::new(Notify::new());
    let mut registry = registry();
    registry.next_id += 1;
    let id = registry.next_id;
    registry.waiters.insert(
        id,
        Waiter {
            user_id: user_id.to_owned(),
            device_id: device_id.to_owned(),
            rooms: Vec::new(),
            profiles,
            notify: notify.clone(),
        },
    );
    registry.by_user.entry(user_id.to_owned()).or_default().insert(id);
    if profiles {
        registry.profile_waiters.insert(id);
    }
    SyncWaiter { id, notify }
}

fn wake<'a>(registry: &Registry, ids: impl IntoIterator<Item = &'a u64>) {
    for id in ids {
        if let Some(waiter) = registry.waiters.get(id) {
            waiter.notify.notify_one();
        }
    }
}

/// Wake every waiter of `user_id` (account data, membership, key changes).
pub fn notify_user(user_id: &UserId) {
    let registry = registry();
    if let Some(ids) = registry.by_user.get(user_id) {
        wake(&registry, ids);
    }
}

/// Wake the waiters of one device (to-device messages).
pub fn notify_device(user_id: &UserId, device_id: &DeviceId) {
    let registry = registry();
    let Some(ids) = registry.by_user.get(user_id) else {
        return;
    };
    let ids = ids
        .iter()
        .filter(|id| registry.waiters.get(id).is_some_and(|w| w.device_id == device_id));
    wake(&registry, ids);
}

/// Wake every waiter whose user is joined to `room_id` (timeline events, typing,
/// receipts, room-scoped key changes).
pub fn notify_room(room_id: &RoomId) {
    let registry = registry();
    if let Some(ids) = registry.by_room.get(room_id) {
        wake(&registry, ids);
    }
}

/// Wake `user_id` and every waiter following profile changes (MSC4262). Which of
/// them share a room with the user is left to their snapshot check.
pub fn notify_profile(user_id: &UserId) {
    let registry = registry();
    if let Some(ids) = registry.by_user.get(user_id) {
        wake(&registry, ids);
    }
    wake(&registry, &registry.profile_waiters);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const SHORT: Duration = Duration::from_millis(50);

    async fn woken(waiter: &SyncWaiter) -> bool {
        tokio::time::timeout(SHORT, waiter.notified()).await.is_ok()
    }

    fn user(name: &str) -> OwnedUserId {
        UserId::parse(format!("@{name}:notify.test")).unwrap()
    }

    fn room(name: &str) -> OwnedRoomId {
        RoomId::parse(format!("!{name}:notify.test")).unwrap()
    }

    #[tokio::test]
    async fn signal_before_or_during_the_wait_wakes_the_waiter() {
        let alice = user("alice-wake");
        let waiter = register(&alice, "A".into(), false);
        // Sent before anyone awaits: kept as a permit, not lost.
        notify_user(&alice);
        assert!(woken(&waiter).await);
        // Sent while waiting.
        let (_, result) = tokio::join!(
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                notify_user(&alice);
            },
            woken(&waiter)
        );
        assert!(result);
    }

    #[tokio::test]
    async fn waiter_times_out_without_a_relevant_signal() {
        let bob = user("bob-timeout");
        let waiter = register(&bob, "B".into(), false);
        waiter.set_rooms(vec![room("bob-room")]);
        notify_user(&user("someone-else"));
        notify_room(&room("other-room"));
        notify_device(&bob, "OTHER".into());
        // `notify_profile` is not exercised here: it wakes every profile follower,
        // which would race with the routing test running in parallel.
        assert!(!woken(&waiter).await);
    }

    #[tokio::test]
    async fn room_device_and_profile_signals_reach_matching_waiters() {
        // The only test that sends profile signals: they reach every profile follower.
        let carol = user("carol-routes");
        let laptop = register(&carol, "LAPTOP".into(), false);
        let phone = register(&carol, "PHONE".into(), true);
        laptop.set_rooms(vec![room("carol-a")]);
        notify_room(&room("carol-a"));
        assert!(woken(&laptop).await);
        assert!(!woken(&phone).await);
        notify_device(&carol, "PHONE".into());
        assert!(woken(&phone).await);
        assert!(!woken(&laptop).await);
        notify_profile(&user("roommate-profile"));
        assert!(woken(&phone).await);
        assert!(!woken(&laptop).await);
        // Moving the room set drops the old room.
        laptop.set_rooms(vec![room("carol-b")]);
        notify_room(&room("carol-a"));
        assert!(!woken(&laptop).await);
        notify_room(&room("carol-b"));
        assert!(woken(&laptop).await);
    }

    #[tokio::test]
    async fn dropping_a_waiter_removes_every_index_entry() {
        let dave = user("dave-drop");
        let waiter = register(&dave, "D".into(), true);
        waiter.set_rooms(vec![room("dave-room")]);
        let id = waiter.id;
        drop(waiter);
        let registry = registry();
        assert!(!registry.waiters.contains_key(&id));
        assert!(!registry.by_user.contains_key(&dave));
        assert!(!registry.by_room.contains_key(&room("dave-room")));
        assert!(!registry.profile_waiters.contains(&id));
    }
}
