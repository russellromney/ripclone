use anyhow::Result;
use clap::Parser;
use ripclone::server::run_server;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn default_cas_dir() -> PathBuf {
    default_data_dir().join("cache")
}

fn default_repo_root() -> PathBuf {
    default_data_dir().join("repos")
}

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
    /// Local CAS cache directory. Defaults to ~/.local/share/ripclone/cache.
    #[arg(long)]
    cas_dir: Option<PathBuf>,

    /// Directory for bare git mirrors. Defaults to ~/.local/share/ripclone/repos.
    #[arg(long)]
    repo_root: Option<PathBuf>,

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
    ripclone::backends::validate_database_configuration()?;
    let cas_dir = args.cas_dir.unwrap_or_else(default_cas_dir);
    let repo_root = args.repo_root.unwrap_or_else(default_repo_root);
    run_server(&cas_dir, &repo_root, &args.host, args.port).await
}
