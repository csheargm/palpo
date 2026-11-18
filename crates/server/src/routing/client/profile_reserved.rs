//! Hagency's reserved custom profile fields (`io.hagency.*`, MSC4133).
//!
//! These fields say whose an Agent puppet is (`io.hagency.owner`), and other
//! readers on any server show them as fact, so only the application service
//! that exclusively owns the puppet's namespace may write them (it is the
//! Hagency server, which knows the owner from its own records).
//!
//! The puppet's own device (a token from `m.login.application_service`, so a
//! plain user token, not the appservice token) is not trusted to name an
//! owner: it is held by the owner's desktop and could name anyone. For
//! compatibility with desktops that still publish the owner themselves, the
//! puppet's write is accepted as a no-op when it repeats the stored value,
//! and refused otherwise. Fields in [`SELF_DESCRIBED`] describe what runs on
//! the puppet's device, which that device knows best, so the puppet may set
//! those on its own profile. Server-internal writes do not pass through here.
use crate::core::identifiers::UserId;
use crate::core::serde::JsonValue;
use crate::exts::IsRemoteOrLocal;
use crate::{AppResult, MatrixError, data};

/// Custom profile fields under this prefix are reserved to the appservice.
pub(super) const RESERVED_PREFIX: &str = "io.hagency.";
/// Reserved fields an appservice puppet may set on its own profile.
const SELF_DESCRIBED: &[&str] = &["io.hagency.runtime"];

/// What a permitted write does.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReservedWrite {
    /// Store the value (or delete the field).
    Apply,
    /// The stored value already equals it: answer success, change nothing.
    AlreadySet,
}

/// Who is writing, relative to the profile's user.
#[derive(Clone, Copy, Debug)]
pub(super) struct Writer {
    /// An appservice whose exclusive user namespace covers the profile's user.
    pub owning_appservice: bool,
    /// The appservice puppet itself, with its own (non-appservice) token.
    pub own_puppet: bool,
}

/// The write rule for one field. `new` is `None` for a delete.
pub(super) fn decide(
    field: &str,
    writer: Writer,
    stored: Option<&JsonValue>,
    new: Option<&JsonValue>,
) -> Result<ReservedWrite, MatrixError> {
    if !field.starts_with(RESERVED_PREFIX) || writer.owning_appservice {
        return Ok(ReservedWrite::Apply);
    }
    if writer.own_puppet && SELF_DESCRIBED.contains(&field) {
        return Ok(ReservedWrite::Apply);
    }
    if writer.own_puppet && new.is_some() && new == stored {
        return Ok(ReservedWrite::AlreadySet);
    }
    Err(MatrixError::forbidden(
        "This profile field is managed by the server.",
        None,
    ))
}

/// Applies [`decide`] to an authenticated request for `user_id`'s `field`.
pub(super) async fn check(
    authed: &crate::AuthedInfo,
    user_id: &UserId,
    field: &str,
    new: Option<&JsonValue>,
) -> AppResult<ReservedWrite> {
    if !field.starts_with(RESERVED_PREFIX) {
        return Ok(ReservedWrite::Apply);
    }
    if user_id.is_remote() {
        return Err(MatrixError::forbidden("This profile field is managed by the server.", None).into());
    }
    let owning_appservice = authed
        .appservice()
        .is_some_and(|appservice| appservice.is_exclusive_user_match(user_id));
    // A puppet is an account an appservice created inside an exclusive
    // namespace, not merely a matching MXID (an older account could match).
    let own_puppet = authed.appservice().is_none()
        && authed.user_id() == user_id
        && authed.user().appservice_id.is_some()
        && crate::appservice::is_exclusive_user_id(user_id).await?;
    let writer = Writer {
        owning_appservice,
        own_puppet,
    };
    let stored = if owning_appservice {
        None
    } else {
        data::user::profile_field(user_id, field).await?
    };
    Ok(decide(field, writer, stored.as_ref(), new)?)
}

#[cfg(test)]
#[path = "profile_reserved_tests.rs"]
mod tests;
