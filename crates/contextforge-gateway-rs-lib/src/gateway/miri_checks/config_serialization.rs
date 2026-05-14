use std::sync::Arc;

use serde::Deserialize;

use super::{SessionMapping, UserSession};

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OwnedUserSession {
    name: String,
    principal: String,
    downstream_session_id: Arc<str>,
}

fn round_trip_session_mapping(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
    let mut mapping = SessionMapping::new();
    for (backend_name, upstream_session_id) in entries {
        let upstream_session_id = upstream_session_id.map(Arc::<str>::from);
        mapping.push((*backend_name).to_owned(), upstream_session_id.as_ref());
    }

    let encoded = rmp_serde::encode::to_vec(&mapping).expect("session mapping should encode");
    let decoded: SessionMapping = rmp_serde::decode::from_slice(&encoded).expect("session mapping should decode");

    entries
        .iter()
        .map(|(backend_name, _)| {
            let upstream_session_id =
                decoded.get(backend_name).and_then(super::SessionMap::session).map(|id| id.to_string());
            ((*backend_name).to_owned(), upstream_session_id)
        })
        .collect()
}

fn user_session_msgpack_round_trip(principal: &str, downstream_session_id: &str) -> bool {
    let session = UserSession::new(principal.to_owned(), Arc::<str>::from(downstream_session_id));
    let encoded = rmp_serde::encode::to_vec(&session).expect("user session should encode");
    let decoded: OwnedUserSession = rmp_serde::decode::from_slice(&encoded).expect("user session should decode");
    decoded
        == OwnedUserSession {
            name: "UserSession".to_owned(),
            principal: principal.to_owned(),
            downstream_session_id: Arc::<str>::from(downstream_session_id),
        }
}

#[test]
fn session_mapping_msgpack_round_trip() {
    let entries = [("backend-a", Some("upstream-a")), ("backend-b", None)];
    let round_tripped = round_trip_session_mapping(&entries);

    assert_eq!(
        round_tripped,
        vec![("backend-a".to_owned(), Some("upstream-a".to_owned())), ("backend-b".to_owned(), None)]
    );
}

#[test]
fn user_session_key_msgpack_round_trip() {
    assert!(user_session_msgpack_round_trip("principal-a", "downstream-session-a"));
}
