use anyhow::Result;
use clap::Parser;
use ripclone::server::run_server;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn default_data_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
        .join(".local/share/ripclone")
}

#[derive(Parser)]
#[command(name = "ripclone-server")]
#[command(about = "CAS-based git clone helper server")]
#[command(version)]
struct Args {
    /// Server state: artifacts in `cache/`, git mirrors in `repos/`, and the
    /// control database `control.db`. Defaults to ~/.local/share/ripclone.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value = "8000")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = Args::parse();
    ripclone::git::require_system_git()?;
    let data_dir = args.data_dir.unwrap_or_else(default_data_dir);
    run_server(&data_dir, &args.host, args.port).await
}
