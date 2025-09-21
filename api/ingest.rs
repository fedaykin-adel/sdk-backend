use http::Method;
use neo4rs::{ConfigBuilder, Graph};
use shaayud_core::{EventoInput, handle_ingest};
use std::sync::OnceLock;
use std::{env, sync::Arc};
use tokio::sync::{OnceCell, Semaphore};
use vercel_runtime::{Body, Error, Request, Response, StatusCode, run};
// Conexão global (reutilizada entre invocações)

static GRAPH: OnceCell<Arc<Graph>> = OnceCell::const_new();
// limite de concorrência para evitar "busy" transiente
static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();

async fn get_graph() -> Result<Arc<Graph>, Error> {
    if let Some(g) = GRAPH.get() {
        return Ok(g.clone());
    }
    let uri = env::var("NEO4J_URL").map_err(|_| err("NEO4J_URL ausente"))?;
    let user = env::var("NEO4J_USER").map_err(|_| err("NEO4J_USER ausente"))?;
    let pass = env::var("NEO4J_PASS").map_err(|_| err("NEO4J_PASS ausente"))?;

    eprintln!("🔌 Conectando Neo4j em {uri}");
    let cfg = ConfigBuilder::default()
        .uri(&uri)
        .user(&user)
        .password(&pass)
        // 👉 aumente o pool para lidar com bursts (ajuste conforme precisar)
        .max_connections(12) // <— importante
        .fetch_size(1000) // opcional
        .build()
        .map_err(|e| err(format!("Config Neo4j inválida: {e:?}")))?;

    let graph = Graph::connect(cfg)
        .await
        .map_err(|e| err(format!("Falha ao conectar Neo4j: {e:?}")))?;
    let arc = Arc::new(graph);
    let _ = GRAPH.set(arc.clone());
    SEM.set(Arc::new(Semaphore::new(8))).ok(); // <— limita 8 requisições simultâneas no handler
    eprintln!("✅ Conectado ao Neo4j");
    Ok(arc)
}

fn err<T: ToString>(msg: T) -> Error {
    // mantenha os detalhes (Debug) nas mensagens
    Error::from(std::io::Error::new(
        std::io::ErrorKind::Other,
        msg.to_string(),
    ))
}

async fn handler(req: Request) -> Result<Response<Body>, Error> {
    eprintln!("➡️  {} {}", req.method(), req.uri().path());

    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::Text("use POST".into()))?);
    }

    let bytes: Vec<u8> = match req.into_body() {
        Body::Text(s) => s.into_bytes(),
        Body::Binary(b) => b,
        Body::Empty => Vec::new(),
    };

    if bytes.is_empty() {
        eprintln!("⚠️  payload vazio");
        return Ok(Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(Body::Text("invalid payload".into()))?);
    }

    let payload: EventoInput = match serde_json::from_slice(&bytes) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("❌ JSON inválido: {e:?}");
            return Ok(Response::builder()
                .status(StatusCode::UNPROCESSABLE_ENTITY)
                .body(Body::Text("invalid payload".into()))?);
        }
    };

    let graph = get_graph().await?;

    // 🔒 segura a concorrência
    let sem = SEM.get().expect("sem init");
    let _permit = sem.acquire().await.unwrap();

    match handle_ingest(payload, &graph).await {
        Ok(_) => {
            eprintln!("✅ ingest OK");
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::Text("ok".into()))?)
        }
        Err(e) => {
            // LOG COMPLETO (Debug) — isso vai aparecer nos logs da Vercel
            eprintln!("💥 handle_ingest ERRO: {e:?}");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::Text(format!("ingest error: {e:?}")))?)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // ⬅️ `run` é async → precisa de `.await`
    run(handler).await
}
