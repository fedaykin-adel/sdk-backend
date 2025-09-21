use axum::{
    Router,
    body::to_bytes,
    extract::Request,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use dotenvy::dotenv;
use neo4rs::{ConfigBuilder, Graph};
pub use shaayud_core::{EventoInput, handle_ingest};
use std::{env, net::SocketAddr, sync::Arc};
use tokio::net::lookup_host;
use tokio::sync::{OnceCell, Semaphore};
use tracing_subscriber::fmt;
use url::Url;
// ---- globais compartilhadas (como na Vercel) ----
static GRAPH: OnceCell<Arc<Graph>> = OnceCell::const_new();
static SEM: OnceCell<Arc<Semaphore>> = OnceCell::const_new();

// -------- utilidades --------
fn err<S: ToString>(msg: S) -> anyhow::Error {
    anyhow::anyhow!(msg.to_string())
}

/// Aceita NEO4J_URL ou NEO4J_URI; devolve exatamente como veio (sem mexer em porta).
fn get_neo4j_uri_from_env() -> anyhow::Result<String> {
    env::var("NEO4J_URL")
        .or_else(|_| env::var("NEO4J_URI"))
        .map_err(|_| err("NEO4J_URL/NEO4J_URI ausente"))
}

/// Conexão global com pool pequeno (estável para Aura/Local)
async fn get_graph() -> anyhow::Result<Arc<Graph>> {
    if let Some(g) = GRAPH.get() {
        return Ok(g.clone());
    }

    let mut uri = env::var("NEO4J_URL").map_err(|_| err("NEO4J_URL ausente"))?;
    let user = env::var("NEO4J_USER").map_err(|_| err("NEO4J_USER ausente"))?;
    let pass = env::var("NEO4J_PASS").map_err(|_| err("NEO4J_PASS ausente"))?;
    uri = uri.trim().trim_matches('"').trim_matches('\'').to_string();

    // logs seguros
    eprintln!("🔌 Conectando Neo4j em {uri}");
    eprintln!("👤 NEO4J_USER = {}", user);
    eprintln!(
        "🔒 NEO4J_PASS = {}",
        if pass.is_empty() {
            "<empty>"
        } else {
            "<hidden>"
        }
    );

    // sem preflight, sem porta — passa direto pro driver
    let cfg = ConfigBuilder::default()
        .uri(&uri)
        .user(&user)
        .password(&pass)
        .db("37d88777")
        .max_connections(1) // estável para testes / evita EBUSY
        .fetch_size(1000)
        .build()
        .map_err(|e| err(format!("Config Neo4j inválida: {e:?}")))?;

    let graph = Graph::connect(cfg)
        .await
        .map_err(|e| err(format!("Falha ao conectar Neo4j: {e:?}")))?;
    let arc = Arc::new(graph);
    if let Err(e) = arc.execute(neo4rs::query("RETURN 1")).await {
        return Err(err(format!("Falha no exec inicial em {uri}: {e:?}")));
    }
    let _ = GRAPH.set(arc.clone());

    // limite de concorrência no handler
    let _ = SEM.set(Arc::new(Semaphore::new(1)));

    eprintln!("✅ Conectado ao Neo4j");
    Ok(arc)
}

// -------- handlers --------

async fn ingest_handler(req: Request) -> (StatusCode, Json<String>) {
    // pega conexão (ou erro cedo)
    let graph = match get_graph().await {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(format!("Erro ao conectar: {e}")),
            );
        }
    };

    // trava concorrência
    let sem = SEM.get().expect("SEM init");
    let _permit = sem.acquire().await.expect("SEM acquire");

    // lê corpo
    let body_bytes = match to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(format!("invalid payload: {e}")),
            );
        }
    };
    if body_bytes.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json("invalid payload".to_string()),
        );
    }

    // parse
    let parsed: Result<EventoInput, _> = serde_json::from_slice(&body_bytes);
    let data = match parsed {
        Ok(d) => d,
        Err(err) => {
            eprintln!("❌ Erro ao deserializar EventoInput: {err}");
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json("invalid payload".to_string()),
            );
        }
    };

    // chama core
    match handle_ingest(data, &graph).await {
        Ok(_) => (StatusCode::NO_CONTENT, Json("ok".to_string())),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(format!("Erro ao processar: {e}")),
        ),
    }
}

async fn health_handler() -> (StatusCode, Json<String>) {
    match get_graph().await {
        Ok(g) => {
            // tenta no banco "neo4j" (Aura costuma usar esse como default)
            let q = neo4rs::query("RETURN 1 AS ok");
            match g.execute(q).await {
                Ok(mut res) => match res.next().await {
                    Ok(Some(row)) => {
                        let ok: i64 = row.get("ok").unwrap_or(0);
                        (StatusCode::OK, Json(format!("neo4j_ok={ok}")))
                    }
                    Ok(None) => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json("neo4j_exec_failed: no row".to_string()),
                    ),
                    Err(e) => {
                        eprintln!("health row error: {:?}", e);
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(format!("neo4j_exec_failed: {:?}", e)),
                        )
                    }
                },
                Err(e) => {
                    eprintln!("health exec error: {:?}", e);
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(format!("neo4j_exec_failed: {:?}", e)),
                    )
                }
            }
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(format!("neo4j_unavailable: {e}")),
        ),
    }
}

// -------- bootstrap --------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    fmt::init();

    // logs de env (mascarados)
    let shown_url = get_neo4j_uri_from_env().unwrap_or_else(|_| "<unset>".to_string());
    let shown_user = env::var("NEO4J_USER").unwrap_or_default();
    let shown_pass = env::var("NEO4J_PASS").unwrap_or_default();
    println!("NEO4J_URL/URI = {}", shown_url);
    println!("NEO4J_USER    = {}", shown_user);
    println!(
        "NEO4J_PASS    = {}",
        if shown_pass.is_empty() {
            "<empty>"
        } else {
            "<hidden>"
        }
    );

    let app = Router::new()
        .route("/api/ingest", post(ingest_handler))
        .route("/health", get(health_handler))
        .route("/", get(|| async { StatusCode::ACCEPTED }));

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    println!("🚀 Running on http://{}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}
