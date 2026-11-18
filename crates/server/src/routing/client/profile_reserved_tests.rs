use super::*;
use serde_json::json;

const OWNER: &str = "io.hagency.owner";
const APPSERVICE: Writer = Writer { owning_appservice: true, own_puppet: false };
const PUPPET: Writer = Writer { owning_appservice: false, own_puppet: true };
const PERSON: Writer = Writer { owning_appservice: false, own_puppet: false };

fn forbidden(result: Result<ReservedWrite, MatrixError>) -> bool {
    matches!(result, Err(MatrixError { kind: crate::core::error::ErrorKind::Forbidden { .. }, .. }))
}

/// Nov 18: an Agent's owner was its device's self-claim, so whoever held the
/// device could name anyone. Only the Hagency appservice names owners now.
#[test]
fn only_the_owning_appservice_names_an_agents_owner() {
    let bird = json!("@bird:beta");
    assert_eq!(decide(OWNER, APPSERVICE, None, Some(&bird)).unwrap(), ReservedWrite::Apply);
    assert_eq!(decide(OWNER, APPSERVICE, Some(&bird), None).unwrap(), ReservedWrite::Apply, "and may delete it");
    assert!(forbidden(decide(OWNER, PUPPET, None, Some(&bird))), "the puppet cannot name a first owner");
    assert!(forbidden(decide(OWNER, PUPPET, Some(&bird), Some(&json!("@eve:beta")))), "nor change it");
    assert!(forbidden(decide(OWNER, PUPPET, Some(&bird), None)), "nor delete it");
    assert!(forbidden(decide(OWNER, PERSON, None, Some(&bird))), "a person cannot claim one");
    assert!(forbidden(decide("io.hagency.anything", PERSON, None, Some(&bird))), "the whole prefix is reserved");
}

/// Compatibility: a desktop that still publishes the owner the server set
/// gets success without a write.
#[test]
fn the_puppet_repeating_the_stored_owner_is_a_no_op() {
    let bird = json!("@bird:beta");
    assert_eq!(decide(OWNER, PUPPET, Some(&bird), Some(&bird)).unwrap(), ReservedWrite::AlreadySet);
    assert!(forbidden(decide(OWNER, PERSON, Some(&bird), Some(&bird))), "only the puppet itself");
}

#[test]
fn the_puppet_describes_its_own_runtime() {
    let runtime = "io.hagency.runtime";
    assert_eq!(decide(runtime, PUPPET, None, Some(&json!("claude"))).unwrap(), ReservedWrite::Apply);
    assert_eq!(decide(runtime, PUPPET, Some(&json!("claude")), None).unwrap(), ReservedWrite::Apply);
    assert!(forbidden(decide(runtime, PERSON, None, Some(&json!("claude")))), "a person is no Agent");
}

#[test]
fn other_fields_are_untouched() {
    for field in ["m.tz", "com.example.banner", "io.hagencyx.owner", "io.hagency"] {
        assert_eq!(decide(field, PERSON, None, Some(&json!(1))).unwrap(), ReservedWrite::Apply, "{field}");
    }
}
