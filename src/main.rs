// pub mod eventos;

use axum::Router;
use axum::routing::{get, post};
use axum::{body::to_bytes, extract::Request, http::StatusCode, response::Json};
use dotenvy::dotenv;
use neo4rs::{ConfigBuilder, Graph};
pub use shaayud_core::{EventoInput, handle_ingest};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::fmt;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    fmt::init();

    let neo4j_url = env::var("NEO4J_URL").unwrap_or("".to_string());
    let neo4j_user = env::var("NEO4J_USER").unwrap_or("".to_string());
    let neo4j_pass = env::var("NEO4J_PASS").unwrap_or("".to_string());

    let config = ConfigBuilder::default()
        .uri(&neo4j_url)
        .user(&neo4j_user)
        .password(&neo4j_pass)
        .build()?;

    let graph = Arc::new(Graph::connect(config).await?);

    let app = Router::new()
        .route(
            "/ingest",
            post(|req: Request| async move {
                let body_bytes = to_bytes(req.into_body(), 1024 * 1024).await.unwrap();
                let body_str = String::from_utf8_lossy(&body_bytes);

                let parsed: Result<EventoInput, _> = serde_json::from_str(&body_str);
                match parsed {
                    Ok(data) => match handle_ingest(data, &graph).await {
                        Ok(_) => (StatusCode::NO_CONTENT, Json("ok".to_string())),
                        Err(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(format!("Erro ao processar: {}", e)),
                        ),
                    },
                    Err(err) => {
                        eprintln!("❌ Erro ao deserializar EventoInput: {}", err);
                        (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json("invalid payload".to_string()),
                        )
                    }
                }
            }),
        )
        .route("/", get(|| async { StatusCode::ACCEPTED }));

    let port = env::var("PORT").unwrap_or_else(|_| "6666".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    println!("🚀 Running on http://{}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}
