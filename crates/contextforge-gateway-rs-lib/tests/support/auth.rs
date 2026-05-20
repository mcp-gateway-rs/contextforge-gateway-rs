use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;

const TEST_TOKEN_TTL_SECS: u64 = 60 * 60;

pub(crate) fn token(user_id: &str) -> String {
    let key = EncodingKey::from_rsa_pem(&fs::read("../../assets/jwt.key").expect("jwt key")).expect("encoding key");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".to_owned());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock").as_secs();
    let claims = json!({
        "iss": "mcpgateway",
        "sub": user_id,
        "aud": "mcpgateway-api",
        "exp": now + TEST_TOKEN_TTL_SECS,
        "iat": now,
        "jti": "test-token",
        "token_use": "api",
        "teams": ["team_awesome"],
        "user": {
            "email": user_id,
            "full_name": "API Token User",
            "is_admin": true,
            "auth_provider": "api_token"
        },
        "scopes": {
            "server_id": "my_id",
            "permissions": ["tools.read", "servers.use"],
            "ip_restrictions": ["192.169.1.0/24"],
            "time_restrictions": null
        },
    });
    encode(&header, &claims, &key).expect("jwt token")
}
