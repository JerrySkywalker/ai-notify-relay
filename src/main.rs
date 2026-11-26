mod api;
mod fcm;
mod mailbox;
mod store;

use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf};

use api::{Auth, State};
use clap::{Parser, Subcommand};
use store::Store;

#[derive(Parser)]
#[command(
    name = "ai-notify-relay",
    version,
    about = "AI Notify Relay Server and Management"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP relay daemon (default)
    Serve,
    /// Create a consistent online backup snapshot of the SQLite database
    Backup {
        #[arg(long, help = "Path to source SQLite database file")]
        db: PathBuf,
        #[arg(long, help = "Path to destination backup file")]
        output: PathBuf,
    },
    /// Restore an SQLite database from a backup snapshot
    Restore {
        #[arg(long, help = "Path to backup snapshot input file")]
        input: PathBuf,
        #[arg(long, help = "Path to target database file")]
        db: PathBuf,
        #[arg(long, help = "Reset stream_id to signal clients to re-synchronize")]
        reset_stream_id: bool,
    },
    /// Prune old events and orphaned push delivery records
    Prune {
        #[arg(long, help = "Path to SQLite database file")]
        db: PathBuf,
        #[arg(long, help = "Prune events older than N days")]
        older_than_days: Option<i64>,
        #[arg(long, help = "Keep only the latest N events")]
        keep_last: Option<i64>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Commands::Serve) {
        Commands::Serve => run_server().await,
        Commands::Backup { db, output } => {
            let store = Store::open(db)?;
            store.backup(output)?;
            println!("Backup completed successfully.");
            Ok(())
        }
        Commands::Restore {
            input,
            db,
            reset_stream_id,
        } => {
            let store = Store::restore(input, db, reset_stream_id)?;
            println!(
                "Database restored successfully. stream_id={}",
                store.stream_id()
            );
            Ok(())
        }
        Commands::Prune {
            db,
            older_than_days,
            keep_last,
        } => {
            let store = Store::open(db)?;
            let older_than_ms = older_than_days.map(|days| {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                now_ms - (days * 24 * 60 * 60 * 1000)
            });
            let pruned = store.prune(older_than_ms, keep_last)?;
            println!("Pruned {} events.", pruned);
            Ok(())
        }
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
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
    let inbox_token = env::var("AINOTIFY_INBOX_TOKEN");
    let inbox_token = match inbox_token {
        Ok(token) => Some(token),
        Err(env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let fcm_config = fcm::FcmConfig::from_env();
    let state = State::new(
        Store::open(database)?,
        Auth::new(&reader, writers, inbox_token.as_deref())?,
    );
    let fcm_tx = fcm::start_dispatcher(state.store_arc(), fcm_config);
    let state = state.with_fcm_notifier(fcm_tx);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("AINotify relay listening on {}", listener.local_addr()?);
    axum::serve(listener, api::router(state)).await?;
    Ok(())
}
