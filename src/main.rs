use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use deadpool_sqlite::{Config, Pool, Runtime};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Use mimalloc for high performance on Windows
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone)]
struct AppState {
    db_pool: Pool,
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
}

#[derive(Debug, Deserialize)]
struct CreateUser {
    name: String,
    email: String,
}

#[tokio::main]
async fn main() {
    // Initialize tracing (minimal for production)
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Database configuration
    let db_path = "backend.db";
    let cfg = Config::new(db_path);
    let pool = cfg.create_pool(Runtime::Tokio1).expect("Failed to create pool");

    // Initialize Database with high-performance PRAGMAs
    setup_database(&pool).await;

    let app_state = AppState { db_pool: pool };

    // Build our application with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/users", post(create_user))
        .route("/users/:id", get(get_user))
        .with_state(app_state);

    // Run it
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn setup_database(pool: &Pool) {
    let conn = pool.get().await.expect("Failed to get connection");
    conn.interact(|conn| {
        // High-performance PRAGMAs
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA mmap_size = 30000000000;
             PRAGMA page_size = 4096;
             PRAGMA cache_size = -20000;
             PRAGMA busy_timeout = 5000;
             PRAGMA temp_store = MEMORY;",
        )
        .expect("Failed to set PRAGMAs");

        // Create initial table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )",
            [],
        )
        .expect("Failed to create users table");
    })
    .await
    .expect("Failed to interact with DB");
}

async fn health_check() -> &'static str {
    "OK"
}

async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<CreateUser>,
) -> Json<User> {
    let id = uuid::Uuid::new_v4().to_string();
    let user = User {
        id: id.clone(),
        name: payload.name,
        email: payload.email,
    };

    let user_clone = User {
        id: user.id.clone(),
        name: user.name.clone(),
        email: user.email.clone(),
    };

    let conn = state.db_pool.get().await.expect("DB connection failed");
    conn.interact(move |conn| {
        conn.execute(
            "INSERT INTO users (id, name, email) VALUES (?1, ?2, ?3)",
            params![user_clone.id, user_clone.name, user_clone.email],
        )
        .expect("Insert failed");
    })
    .await
    .expect("DB interaction failed");

    Json(user)
}

async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<Option<User>> {
    let conn = state.db_pool.get().await.expect("DB connection failed");
    let user = conn
        .interact(move |conn| {
            let mut stmt = conn
                .prepare("SELECT id, name, email FROM users WHERE id = ?1")
                .ok()?;
            stmt.query_row(params![id], |row| {
                Ok(User {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    email: row.get(2)?,
                })
            })
            .ok()
        })
        .await
        .expect("DB interaction failed");

    Json(user)
}
