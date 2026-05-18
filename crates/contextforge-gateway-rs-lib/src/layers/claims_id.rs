use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use http::{StatusCode, header};
use jsonwebtoken::Validation;

use crate::{
    common::{ContextForgeClaims, ContextForgeGatewayAppState},
    const_values::{CONTEXT_FORGE_GATEWAY_AUDIENCE, CONTEXT_FORGE_GATEWAY_ISSUER},
};

fn unauthorized_response() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::empty())
        .expect("Expecting this to work")
}

pub async fn claims_layer(
    State(state): State<ContextForgeGatewayAppState>,
    request: http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let decoding_keys = state.jwt_token_decoding_keys;
    let (mut parts, body) = request.into_parts();

    let Some(authorization) = parts.headers.get("Authorization") else { return unauthorized_response() };

    let Some(token) = authorization.as_bytes().strip_prefix(b"Bearer ") else { return unauthorized_response() };

    let Ok(raw_token) = str::from_utf8(token) else { return unauthorized_response() };

    let Ok(header) = jsonwebtoken::decode_header(raw_token) else { return unauthorized_response() };

    let mut validation = Validation::new(header.alg);
    validation.set_audience(&[CONTEXT_FORGE_GATEWAY_AUDIENCE]);
    validation.set_issuer(&[CONTEXT_FORGE_GATEWAY_ISSUER]);
    validation.validate_exp = true;

    let claims = match header.alg {
        jsonwebtoken::Algorithm::RS256 | jsonwebtoken::Algorithm::RS384 | jsonwebtoken::Algorithm::RS512 => {
            let Some(decoding_key) = decoding_keys.rs.as_ref() else {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("Expecting this to work");
            };
            let maybe_valid = jsonwebtoken::decode::<ContextForgeClaims>(raw_token, decoding_key, &validation);
            let Ok(claims) = maybe_valid else { return unauthorized_response() };
            claims
        },
        jsonwebtoken::Algorithm::HS256 | jsonwebtoken::Algorithm::HS384 | jsonwebtoken::Algorithm::HS512 => {
            let Some(decoding_key) = decoding_keys.hmac_sha.as_ref() else { return unauthorized_response() };
            let maybe_valid = jsonwebtoken::decode::<ContextForgeClaims>(raw_token, decoding_key, &validation);
            let Ok(claims) = maybe_valid else { return unauthorized_response() };

            claims
        },

        _ => return unauthorized_response(),
    };

    let claims: ContextForgeClaims = claims.claims;
    parts.extensions.insert(claims.clone());
    let request = Request::from_parts(parts, body);
    next.run(request).await
}

#[cfg(test)]
mod test {

    use std::sync::{Arc, Once};

    use async_trait::async_trait;
    use axum::{Router, body::Body, middleware, response::Response, routing::get};
    use contextforge_gateway_rs_apis::{User, user_store::UserConfig};
    use http::{HeaderMap, Request, StatusCode};
    use jsonwebtoken::{DecodingKey, Validation};
    use tower::ServiceExt;

    use crate::{
        Config,
        common::{ContextForgeClaims, ContextForgeGatewayAppState, JwtTokenDecoders},
        const_values::{CONTEXT_FORGE_GATEWAY_AUDIENCE, CONTEXT_FORGE_GATEWAY_ISSUER},
        layers::claims_id::claims_layer,
        tests,
        user_config_store::{ConfigStoreError, UserConfigStore},
    };

    static CRYPTO: Once = Once::new();

    struct MockedUserConfigStore;
    #[async_trait]
    impl UserConfigStore for MockedUserConfigStore {
        async fn get_config<'a>(&self, _: &'a User) -> Result<UserConfig, ConfigStoreError> {
            Err(ConfigStoreError::InvalidConnection)
        }

        async fn set_config<'a>(&self, _: &'a User, _: &'a UserConfig) -> Result<(), ConfigStoreError> {
            Err(ConfigStoreError::InvalidConnection)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::items_after_statements)]
    async fn claim_test_valid_hmac() {
        CRYPTO.call_once(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("Failed to install rustls crypto provider");
        });

        async fn handle(_: HeaderMap) -> Response {
            Response::builder().status(StatusCode::OK).body(Body::empty()).expect("Expecting this to work")
        }

        let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbkBleGFtcGxlLmNvbSIsImp0aSI6Ijc1ZWYwZTZjLTZkZWMtNGExNy1hNzU3LWFlYmYzZjk1N2Q1NSIsInRva2VuX3VzZSI6ImFwaSIsImlhdCI6MTc3ODg2NTE2OCwiaXNzIjoibWNwZ2F0ZXdheSIsImF1ZCI6Im1jcGdhdGV3YXktYXBpIiwidXNlciI6eyJlbWFpbCI6ImFkbWluQGV4YW1wbGUuY29tIiwiZnVsbF9uYW1lIjoiQVBJIFRva2VuIFVzZXIiLCJpc19hZG1pbiI6dHJ1ZSwiYXV0aF9wcm92aWRlciI6ImFwaV90b2tlbiJ9LCJ0ZWFtcyI6bnVsbCwic2NvcGVzIjp7InNlcnZlcl9pZCI6bnVsbCwicGVybWlzc2lvbnMiOltdLCJpcF9yZXN0cmljdGlvbnMiOltdLCJ0aW1lX3Jlc3RyaWN0aW9ucyI6e319LCJleHAiOjE3ODE0NTcxNjh9.9d2-iLOHL2dJRFTSbOxHzuD6zLxupqK0ZkCG-3GZABU";

        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_audience(&[CONTEXT_FORGE_GATEWAY_AUDIENCE]);
        validation.set_issuer(&[CONTEXT_FORGE_GATEWAY_ISSUER]);
        validation.validate_exp = false;

        let decoding_key = DecodingKey::from_secret("my-test-key-but-now-longer-than-32-bytes".as_bytes());

        let state = ContextForgeGatewayAppState {
            jwt_token_decoding_keys: JwtTokenDecoders { rs: None, hmac_sha: Some(decoding_key) },
            config_store: Arc::new(MockedUserConfigStore {}),
            config: Config::default(),
        };
        let http_requst = Request::builder()
            .header("Authorization", format!("Bearer {token}"))
            .method("GET")
            .body(Body::empty())
            .expect("This should work");

        let app =
            Router::new().route("/", get(handle)).layer(middleware::from_fn_with_state(state.clone(), claims_layer));

        let res = app.oneshot(http_requst).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::items_after_statements)]
    async fn claim_test_expired_token() {
        CRYPTO.call_once(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("Failed to install rustls crypto provider");
        });

        async fn handle(_: HeaderMap) -> Response {
            Response::builder().status(StatusCode::OK).body(Body::empty()).expect("Expecting this to work")
        }

        let mut claims = ContextForgeClaims::new("blah@blah.com");
        claims.exp = 0;
        let token = tests::gateway_end_to_end::get_token_for_claims(&claims);

        let mut validation = Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_audience(&[CONTEXT_FORGE_GATEWAY_AUDIENCE]);
        validation.set_issuer(&[CONTEXT_FORGE_GATEWAY_ISSUER]);
        validation.validate_exp = true;

        let decoding_key = DecodingKey::from_secret("my-test-key-but-now-longer-than-32-bytes".as_bytes());

        let state = ContextForgeGatewayAppState {
            jwt_token_decoding_keys: JwtTokenDecoders { rs: None, hmac_sha: Some(decoding_key) },
            config_store: Arc::new(MockedUserConfigStore {}),
            config: Config::default(),
        };
        let http_requst = Request::builder()
            .header("Authorization", format!("Bearer {token}"))
            .method("GET")
            .body(Body::empty())
            .expect("This should work");

        let app =
            Router::new().route("/", get(handle)).layer(middleware::from_fn_with_state(state.clone(), claims_layer));

        let res = app.oneshot(http_requst).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
}
