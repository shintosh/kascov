mod fixture;
mod seed;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

const FETCH_AHEAD: [usize; 4] = [8, 16, 32, 64];
const WAL_AUTOCHECKPOINT: [u32; 3] = [1_000, 4_000, 16_000];
const READ_POOL: [u32; 3] = [4, 8, 16];
const REPLAY_PAGE: [u64; 3] = [256, 512, 1_024];
const PROFILE_VERSION: u32 = 1;
const PROFILE_STATUS: &str = "selected";

#[derive(Args, Clone, Copy, Debug)]
struct TuningArgs {
    #[arg(long, default_value_t = 16)]
    fetch_ahead: usize,
    #[arg(long, default_value_t = 1_000)]
    wal_autocheckpoint: u32,
    #[arg(long, default_value_t = 4)]
    read_pool: u32,
    #[arg(long, default_value_t = 256)]
    replay_page: u64,
}

impl TuningArgs {
    fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            FETCH_AHEAD.contains(&self.fetch_ahead),
            "invalid fetch-ahead candidate"
        );
        anyhow::ensure!(
            WAL_AUTOCHECKPOINT.contains(&self.wal_autocheckpoint),
            "invalid wal-autocheckpoint candidate"
        );
        anyhow::ensure!(
            READ_POOL.contains(&self.read_pool),
            "invalid read-pool candidate"
        );
        anyhow::ensure!(
            REPLAY_PAGE.contains(&self.replay_page),
            "invalid replay-page candidate"
        );
        Ok(self)
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "profile_version": PROFILE_VERSION,
            "profile_status": PROFILE_STATUS,
            "fetch_ahead": self.fetch_ahead,
            "wal_autocheckpoint_pages": self.wal_autocheckpoint,
            "read_pool_connections": self.read_pool,
            "replay_page_records": self.replay_page,
        })
    }
}

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
        #[command(flatten)]
        tuning: TuningArgs,
    },
    /// Seed a deterministic SQLite delivery log for replay benchmarks.
    SeedDelivery {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        network: kascov_core::Network,
        #[arg(long)]
        records: u64,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Fixture {
            blocks,
            events_per_block,
            output,
            tuning,
        } => {
            let tuning = tuning.validate()?;
            fixture::write_fixture_report(blocks, events_per_block, &output)?;
            let mut report: serde_json::Value = serde_json::from_slice(&std::fs::read(&output)?)?;
            report["tuning"] = tuning.json();
            std::fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
            Ok(())
        }
        Command::SeedDelivery {
            database,
            network,
            records,
        } => {
            let cursor = seed::seed_delivery_store(&database, network, records)?;
            println!("{cursor}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_candidates_and_selected_tuple_validate() {
        let selected = TuningArgs {
            fetch_ahead: 16,
            wal_autocheckpoint: 1_000,
            read_pool: 4,
            replay_page: 256,
        };
        assert!(selected.validate().is_ok());
        assert_eq!(1, selected.json()["profile_version"]);
        assert_eq!("selected", selected.json()["profile_status"]);

        for invalid in [
            TuningArgs {
                fetch_ahead: 7,
                ..selected
            },
            TuningArgs {
                wal_autocheckpoint: 999,
                ..selected
            },
            TuningArgs {
                read_pool: 3,
                ..selected
            },
            TuningArgs {
                replay_page: 255,
                ..selected
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
