// pub mod eventos;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dotenvy::dotenv;
use hyper::Method;
use neo4rs::{ConfigBuilder, Graph};
use sea_orm::{Database, DatabaseConnection};
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

    let database_url = env::var("DATABASE_URL")?;
    // let db = PgPool::connect(&database_url).await?;
    let db: DatabaseConnection = Database::connect(&database_url).await?;

    let app = Router::new()
        .route(
            "/ingest",
            post({
                let graph = graph.clone();
                let db = db.clone();
                move |Json(payload): Json<EventoInput>| {
                    println!("Payload recebido: {:?}", payload);

                    let db = db.clone();
                    print!("{:?}", payload);
                    async move {
                        match handle_ingest(payload, &db, &graph).await {
                            Ok(_) => (StatusCode::NO_CONTENT, Json("ok".to_string())),
                            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())),
                        }
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
