pub mod db;

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::db::engine::DbEngine;
use crate::db::sql::SqlEngine;
use crate::db::types::Value;

// Use mimalloc for high performance on Windows
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone)]
struct AppState {
    sql_engine: Arc<SqlEngine>,
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
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Initialize our new Database Engine
    let db_engine = DbEngine::new("data").expect("Failed to create db engine");
    let sql_engine = Arc::new(SqlEngine::new(Arc::new(db_engine)));

    // Create initial table using our SQL engine
    sql_engine
        .execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, email TEXT)")
        .unwrap_or_else(|_| Vec::new());

    let app_state = AppState { sql_engine };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/users", post(create_user))
        .route("/users/:id", get(get_user))
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> &'static str {
    "OK"
}

async fn create_user(State(state): State<AppState>, Json(payload): Json<CreateUser>) -> Json<User> {
    let id = uuid::Uuid::new_v4().to_string();
    let user = User {
        id: id.clone(),
        name: payload.name.clone(),
        email: payload.email.clone(),
    };

    // Construct SQL INSERT for our custom engine
    let sql = format!(
        "INSERT INTO users VALUES ('{}', '{}', '{}')",
        user.id, user.name, user.email
    );

    state.sql_engine.execute(&sql).expect("Insert failed");

    Json(user)
}

async fn get_user(State(state): State<AppState>, Path(id): Path<String>) -> Json<Option<User>> {
    let sql = "SELECT id, name, email FROM users".to_string();

    // In a real implementation we would do:
    // let sql = format!("SELECT id, name, email FROM users WHERE id = '{}'", id);
    // But since filter pushdown isn't fully evaluating predicates yet, we'll scan and filter in memory:

    let rows = state.sql_engine.execute(&sql).unwrap_or_default();

    for row in rows {
        if row.values.len() >= 3 {
            if let Value::String(row_id) = &row.values[0] {
                if row_id == &id {
                    let name = match &row.values[1] {
                        Value::String(s) => s.clone(),
                        _ => String::new(),
                    };
                    let email = match &row.values[2] {
                        Value::String(s) => s.clone(),
                        _ => String::new(),
                    };

                    return Json(Some(User {
                        id: row_id.clone(),
                        name,
                        email,
                    }));
                }
            }
        }
    }

    Json(None)
}
