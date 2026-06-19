use anyhow::Result;
use clap::Parser;
use ripclone::server::run_server;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ripclone-server")]
#[command(about = "CAS-based git clone helper server")]
struct Args {
    #[arg(long, default_value = "/data/cache")]
    cas_dir: PathBuf,

    #[arg(long, default_value = "/data/repos")]
    repo_root: PathBuf,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value = "8000")]
    port: u16,

    #[arg(
        long,
        default_value = "50",
        help = "Default git history depth for repo mirrors"
    )]
    default_depth: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let args = Args::parse();
    run_server(
        &args.cas_dir,
        &args.repo_root,
        &args.host,
        args.port,
        args.default_depth,
    )
    .await
}
