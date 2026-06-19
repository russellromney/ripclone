use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod cas;
mod git;
mod materialize;
mod simulate;
mod skeleton;
mod storage;

#[derive(Parser)]
#[command(name = "ripclone-spikes")]
#[command(about = "Spike tooling for CAS-based git clone")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build a skeleton pack for a single commit.
    MakeSkeletonPack {
        /// Path to bare git repository.
        bare_repo: PathBuf,
        /// Commit SHA or ref.
        commit: String,
        /// Output packfile path.
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Analyze skeleton packs for the last N commits.
    AnalyzeSkeletons {
        /// Path to bare git repository.
        bare_repo: PathBuf,
        /// Number of commits to analyze.
        #[arg(short, long, default_value = "50")]
        count: usize,
    },
    /// Simulate a lazy-blob clone.
    SimulateClone {
        /// Path to bare git repository.
        bare_repo: PathBuf,
        /// Branch or commit to clone.
        #[arg(short, long, default_value = "HEAD")]
        branch: String,
        /// File containing list of files to read (one per line).
        file_list: PathBuf,
        /// CAS directory.
        #[arg(short, long, default_value = "cas")]
        cas_dir: PathBuf,
    },
    /// Materialize a working tree from a skeleton pack + CAS.
    MaterializeTree {
        /// Skeleton packfile.
        skeleton_pack: PathBuf,
        /// CAS directory.
        cas_dir: PathBuf,
        /// File containing list of files to create.
        file_list: PathBuf,
        /// Output directory.
        #[arg(short, long, default_value = "out")]
        output: PathBuf,
    },
    /// Analyze storage models.
    AnalyzeStorage {
        /// Path to bare git repository.
        bare_repo: PathBuf,
        /// Number of commits to analyze.
        #[arg(short, long, default_value = "50")]
        count: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::MakeSkeletonPack {
            bare_repo,
            commit,
            output,
        } => {
            skeleton::make_skeleton_pack(&bare_repo, &commit, &output)?;
        }
        Commands::AnalyzeSkeletons { bare_repo, count } => {
            skeleton::analyze_skeletons(&bare_repo, count)?;
        }
        Commands::SimulateClone {
            bare_repo,
            branch,
            file_list,
            cas_dir,
        } => {
            simulate::run(&bare_repo, &branch, &file_list, &cas_dir).await?;
        }
        Commands::MaterializeTree {
            skeleton_pack,
            cas_dir,
            file_list,
            output,
        } => {
            materialize::run(&skeleton_pack, &cas_dir, &file_list, &output)?;
        }
        Commands::AnalyzeStorage { bare_repo, count } => {
            storage::analyze(&bare_repo, count)?;
        }
    }

    Ok(())
}
