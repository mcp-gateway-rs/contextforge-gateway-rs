#![allow(dead_code)]

#[path = "../src/config.rs"]
mod config;
#[path = "../src/error.rs"]
mod error;

use serde_json::json;

#[test]
fn decode_config_document_accepts_json_bytes() {
    let document = br#" { "version": 1, "cpex": { "plugins": [] } }"#;

    assert_eq!(
        json!({ "version": 1, "cpex": { "plugins": [] } }),
        config::decode_config_document(document).expect("JSON document decodes")
    );
}

#[test]
fn decode_config_document_accepts_messagepack_bytes() {
    let expected = json!({ "version": 1, "cpex": { "plugins": [] } });
    let document = rmp_serde::to_vec_named(&expected).expect("MessagePack document encodes");

    assert_eq!(expected, config::decode_config_document(&document).expect("MessagePack document decodes"));
}

#[test]
fn decode_config_document_rejects_invalid_json_bytes() {
    let error = config::decode_config_document(b"{not-json").expect_err("invalid JSON bytes are rejected");

    assert_eq!("runtime plugin config is in wrong format", error.to_string());
}
