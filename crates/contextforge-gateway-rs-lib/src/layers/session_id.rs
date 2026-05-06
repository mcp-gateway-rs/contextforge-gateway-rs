use std::task::{Context, Poll};

use axum::http::Request;
use tower::Service;
use tower_layer::Layer;
use tracing::info;

use crate::const_values::MCP_SESSION_ID;

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
        if let Some(session_id_header_value) = maybe_session {
            info!("MCP Session ID {:?}", session_id_header_value.to_str());
            if let Ok(session_id) = session_id_header_value.to_str() {
                request.extensions_mut().insert(SessionId { value: session_id.to_owned() });
            }
        }

        self.service.call(request)
    }
}
