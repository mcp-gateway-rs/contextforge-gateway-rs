use std::fs;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::{Router, get, post},
};
use http::{StatusCode, header};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

//use tracing::debug;
use crate::{
    common::{ContextForgeGatewayAppState, DefaultClaims},
    user_config_store::{User, UserConfig},
};

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
    let key = EncodingKey::from_rsa_pem(
        &fs::read(&state.config.token_verification_private_key).expect("Expecting this to work"),
    )
    .expect("Expecting this to work");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".to_owned());

    let claims = DefaultClaims::new(user_id);

    let token = encode::<DefaultClaims>(&header, &claims, &key).expect("Expecting this to work");

    token.into_response()
}

//#[debug_handler]
pub async fn configure_user(
    Path(user_id): Path<String>,
    State(state): State<ContextForgeGatewayAppState>,
    Json(user_config): Json<UserConfig>,
) -> Response {
    if state.config_store.set_config(&User::new(&user_id), &user_config).await.is_ok() {
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
