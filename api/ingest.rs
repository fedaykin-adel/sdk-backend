use http::Method;
use neo4rs::{ConfigBuilder, Graph};
use once_cell::sync::Lazy;
use shaayud_core::{EventoInput, handle_ingest};
use std::{env, sync::Arc};
use vercel_runtime::{Body, Error, Request, Response, StatusCode, run};
// Conexão global (reutilizada entre invocações)
static GRAPH: Lazy<Arc<Graph>> = Lazy::new(|| {
    let uri = env::var("NEO4J_URL").unwrap_or_default();
    let user = env::var("NEO4J_USER").unwrap_or_default();
    let pass = env::var("NEO4J_PASS").unwrap_or_default();
    let cfg = ConfigBuilder::default()
        .uri(&uri)
        .user(&user)
        .password(&pass)
        .build()
        .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    Arc::new(rt.block_on(Graph::connect(cfg)).expect("connect neo4j"))
});

async fn handler(req: Request) -> Result<Response<Body>, Error> {
    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::Empty)?);
    }

    let bytes: Vec<u8> = match req.into_body() {
        Body::Text(s) => s.into_bytes(),
        Body::Binary(b) => b,
        Body::Empty => Vec::new(),
    };

    let payload: EventoInput = serde_json::from_slice(&bytes)?;

    if let Err(e) = handle_ingest(payload, &GRAPH).await {
        return Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::Text(format!("Erro ao processar: {e}")))?);
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::Text("ok".into()))?)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // ⬅️ `run` é async → precisa de `.await`
    run(handler).await
}
