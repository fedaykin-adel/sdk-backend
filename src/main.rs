// pub mod eventos;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dotenvy::dotenv;
use sea_orm::{Database, DatabaseConnection};
pub use shaayud_core::{EventoInput, handle_ingest};
use std::env;
use std::net::SocketAddr;
use tracing_subscriber::fmt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    fmt::init();

    let database_url = env::var("DATABASE_URL")?;
    // let db = PgPool::connect(&database_url).await?;
    let db: DatabaseConnection = Database::connect(&database_url).await?;

    let app = Router::new()
        .route(
            "/ingest",
            post({
                let db = db.clone();
                move |Json(payload): Json<EventoInput>| {
                    let db = db.clone();
                    print!("{:?}", payload);
                    async move {
                        match handle_ingest(payload, &db).await {
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
