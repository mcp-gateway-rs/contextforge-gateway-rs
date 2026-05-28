use std::task::{Context, Poll};

use std::sync::Arc;

use crate::{
    common::ContextForgeClaims,
    const_values::MCP_SESSION_ID,
    gateway::{SessionCleanupRegistry, UserSession, UserSessionStore, cleanup_registered_session_or_owner},
    layers::virtual_host_id::VirtualHostId,
};
use axum::{body::Body, extract::State, http::Request, middleware::Next, response::Response};
use http::{Method, StatusCode, header};
use tower::Service;
use tower_layer::Layer;

#[derive(Debug, Clone)]
pub struct SessionIdLayer;

impl<S> Layer<S> for SessionIdLayer {
    type Service = SessionIdService<S>;

    fn layer(&self, service: S) -> Self::Service {
        SessionIdService { service }
    }
}

#[derive(Debug, Clone)]
pub struct SessionIdService<S> {
    service: S,
}

#[derive(Debug, Clone)]
pub struct SessionId {
    value: String,
}
impl SessionId {
    pub fn value(&self) -> &String {
        &self.value
    }
}

impl<S, B> Service<Request<B>> for SessionIdService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        let maybe_session = request.headers().get(MCP_SESSION_ID).cloned();
        if let Some(session_id_header_value) = maybe_session
            && let Ok(session_id) = session_id_header_value.to_str()
        {
            request.extensions_mut().insert(SessionId { value: session_id.to_owned() });
        }

        self.service.call(request)
    }
}

#[derive(Clone)]
pub struct SessionOwnerState {
    pub user_session_store: Arc<dyn UserSessionStore>,
    pub cleanup_registry: SessionCleanupRegistry,
}

pub async fn session_owner_layer(
    State(state): State<SessionOwnerState>,
    request: http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(session_id) = request.extensions().get::<SessionId>() else {
        return next.run(request).await;
    };
    let Some(claims) = request.extensions().get::<ContextForgeClaims>() else {
        return not_found_response();
    };
    let Some(virtual_host_id) = request.extensions().get::<VirtualHostId>() else {
        return not_found_response();
    };

    let session =
        UserSession::new(claims.sub.clone(), virtual_host_id.value().clone(), Arc::from(session_id.value().as_str()));
    match state.user_session_store.has_session(&session).await {
        Ok(true) => {
            let remove_owner = request.method() == Method::DELETE;
            let response = next.run(request).await;
            if remove_owner
                && response.status().is_success()
                && cleanup_registered_session_or_owner(
                    &state.cleanup_registry,
                    state.user_session_store.as_ref(),
                    &session,
                )
                .await
                .is_err()
            {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(header::CONTENT_TYPE, "text/plain")
                    .body(Body::empty())
                    .expect("response should build");
            }
            response
        },
        Ok(false) => not_found_response(),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::empty())
            .expect("response should build"),
    }
}

fn not_found_response() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::empty())
        .expect("response should build")
}
