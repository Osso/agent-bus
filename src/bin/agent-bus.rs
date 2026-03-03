use std::path::PathBuf;

use agent_bus::Broker;
use clap::Parser;
use tokio::net::UnixListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "agent-bus", about = "Agent message bus daemon")]
struct Cli {
    /// Unix socket path
    #[arg(short, long, default_value = "/tmp/agent-bus.sock")]
    socket: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();

    // Remove stale socket file if present; ignore errors (may not exist)
    let _ = std::fs::remove_file(&cli.socket);

    let listener = match UnixListener::bind(&cli.socket) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind {}: {}", cli.socket.display(), e);
            std::process::exit(1);
        }
    };

    info!("agent-bus listening on {}", cli.socket.display());

    tokio::select! {
        result = Broker::new().run(listener) => {
            if let Err(e) = result {
                tracing::error!("broker error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C, shutting down");
        }
    }

    let _ = std::fs::remove_file(&cli.socket);
    info!("socket removed, goodbye");
}
