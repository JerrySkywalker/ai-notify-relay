mod api;
mod store;

use std::{collections::HashMap, env, net::SocketAddr};

use api::{Auth, State};
use store::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen: SocketAddr = env::var("AINOTIFY_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8787".into())
        .parse()?;
    let database = env::var("AINOTIFY_DB").unwrap_or_else(|_| "ainotify.sqlite3".into());
    let reader =
        env::var("AINOTIFY_READER_TOKEN").map_err(|_| "AINOTIFY_READER_TOKEN is required")?;
    let writers: HashMap<String, String> = serde_json::from_str(
        &env::var("AINOTIFY_WRITERS").map_err(|_| "AINOTIFY_WRITERS is required")?,
    )
    .map_err(|_| "AINOTIFY_WRITERS must be a JSON object of device IDs and tokens")?;
    let state = State::new(Store::open(database)?, Auth::new(&reader, writers)?);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("AINotify relay listening on {}", listener.local_addr()?);
    axum::serve(listener, api::router(state)).await?;
    Ok(())
}
