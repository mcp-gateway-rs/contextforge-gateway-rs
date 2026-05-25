use std::fs;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::{Router, get, post},
};
use chrono::Duration;
use contextforge_gateway_rs_apis::{User as CFUser, user_store::UserConfig};
use http::{StatusCode, header};
use uuid::Uuid;

//use tracing::debug;
use crate::{
    common::{ContextForgeClaims, ContextForgeGatewayAppState, Scopes, User},
    const_values::{CONTEXT_FORGE_GATEWAY_AUDIENCE, CONTEXT_FORGE_GATEWAY_ISSUER},
};

impl ContextForgeClaims {
    pub fn new(user_id: &str) -> Self {
        let audience = CONTEXT_FORGE_GATEWAY_AUDIENCE.to_owned();
        let start = std::time::SystemTime::now();
        let now = start.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_secs();
        Self {
            iss: CONTEXT_FORGE_GATEWAY_ISSUER.to_owned(),
            sub: user_id.to_owned(),
            aud: audience,
            exp: now + Duration::hours(1).num_seconds().cast_unsigned(),
            iat: Some(now),
            jti: Uuid::new_v4().to_string(),
            token_use: "api".to_owned(),
            teams: Some(vec!["team_awesome".to_owned()]),
            user: User::builder()
                .email(user_id.to_owned())
                .auth_provider("api_token".to_owned())
                .full_name("API Token User".to_owned())
                .is_admin(true)
                .build(),
            scopes: Scopes::builder()
                .server_id(Some("my_id".to_owned()))
                .ip_restrictions(vec!["192.169.1.0/24".to_owned()])
                .permissions(vec!["tools.read".to_owned(), "servers.use".to_owned()])
                .time_restrictions(None)
                .build(),
        }
    }
}

pub fn add_tools(router: Router<ContextForgeGatewayAppState>) -> Router<ContextForgeGatewayAppState> {
    router
        .route("/admin/tokens/{user_id}", get(get_token))
        .route("/admin/userconfigs/{user_id}", post(configure_user))
        .route("/health", get(health))
}

pub async fn health() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{\"status\": \"healthy\"}"))
        .expect("Expecting this to work")
}

pub async fn get_token(State(state): State<ContextForgeGatewayAppState>, Path(user_id): Path<String>) -> Response {
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(
        &fs::read(&state.config.token_verification_private_key).expect("Expecting this to work"),
    )
    .expect("Expecting this to work");

    let claims = ContextForgeClaims::new(&user_id);
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("test".to_owned());
    let token = jsonwebtoken::encode::<ContextForgeClaims>(&header, &claims, &key).expect("Expecting this to work");

    token.into_response()
}

//#[debug_handler]
pub async fn configure_user(
    Path(user_id): Path<String>,
    State(state): State<ContextForgeGatewayAppState>,
    Json(user_config): Json<UserConfig>,
) -> Response {
    if state.config_store.set_config(&CFUser::new(&user_id), &user_config).await.is_ok() {
        Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Added"))
            .expect("Expecting this to work")
    } else {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("Problem with encoding "))
            .expect("Expecting this to work")
    }
}
