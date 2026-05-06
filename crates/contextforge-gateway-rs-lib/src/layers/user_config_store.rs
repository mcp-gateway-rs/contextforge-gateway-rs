use axum::{body::Body, extract::State, middleware::Next, response::Response};
use http::{StatusCode, header};
use openid::Claims;
use tracing::{debug, info, warn};

use crate::{
    common::{ContextForgeGatewayAppState, ContextForgeGatewayClaims},
    user_config_store::{ConfigStoreError, User},
};

pub async fn user_config_store_layer(
    State(state): State<ContextForgeGatewayAppState>,
    mut request: http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let maybe_claims = request.extensions().get::<ContextForgeGatewayClaims>();
    if let Some(claims) = maybe_claims {
        let subject = claims.standard_claims.sub();
        debug!("Getting user config for {subject}");
        match state.config_store.get_config(&User::new(subject)).await {
            Ok(user_config) => {
                info!("Got config for user {subject} {user_config:?}");
                request.extensions_mut().insert(user_config);
                next.run(request).await
            },

            Err(ConfigStoreError::NoDataForKey) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("Problem occured retrieving the configuration"))
                .expect("Expecting this to work"),

            Err(_) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from("Problem occured retrieving the configuration"))
                .expect("Expecting this to work"),
        }
    } else {
        warn!("No claims");
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from("No claims in the token"))
            .expect("Expecting this to work")
    }
}
