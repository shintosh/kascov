mod fixture;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "kascov-bench",
    about = "Reproducible Kascov benchmark fixtures"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Process deterministic accepting-block batches and write a JSON report.
    Fixture {
        #[arg(long)]
        blocks: u64,
        #[arg(long)]
        events_per_block: u32,
        #[arg(long)]
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Fixture {
            blocks,
            events_per_block,
            output,
        } => fixture::write_fixture_report(blocks, events_per_block, &output),
    }
}
