pub mod command;
pub mod loader;
pub mod migration;

use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rusqlite::Connection;

use crate::migration::Migrations;

/// Run SQLite migration files from a given directory.
#[derive(clap::Parser, Debug, Clone)]
#[command(name = "migrator")]
#[command(bin_name = "migrator")]
struct MigrateCli {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, long, env = "MIGRATION_DIR", value_hint = clap::ValueHint::DirPath)]
    source: Option<PathBuf>,
    #[arg(short, long, env = "DATABASE_PATH", value_hint = clap::ValueHint::FilePath)]
    database: Option<PathBuf>,
}

#[derive(clap::Subcommand, Debug, Clone)]
enum Commands {
    /// Create a new migration
    Create(CreateArgs),
    /// Run migration UP to most recent or N
    Up(UpArgs),
    /// Run migration DOWN to oldest or N
    Down(DownArgs),
    // Migrate to specific version (automatically Up or Down)
    // Goto()
    // Drop()
}

#[derive(clap::Args, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct CreateArgs {
    /// Apply for N up migrations
    #[arg(required = true)]
    migration_name: String,
}

#[derive(clap::Args, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct UpArgs {
    /// Apply for N up migrations
    #[arg(short)]
    n: Option<usize>,
}

#[derive(clap::Args, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct DownArgs {
    /// Apply for N down migrations
    #[arg(short)]
    n: Option<usize>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct MigrateFileCfg {
    source_path: PathBuf,
    database_path: PathBuf,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = MigrateCli::parse();

    let current_dir = std::env::current_dir()?;

    // exit on error only in the case the file is found but couldn't be deserialiazed
    let config: Result<MigrateFileCfg> = File::open(current_dir.join(".migrate-config.yaml"))
        .map_err(|e| e.into())
        .and_then(|v| serde_yaml::from_reader(v).map_err(Into::into));

    let (source, db_path) = match (args.source.as_ref(), args.database.as_ref()) {
        (None, None) => {
            let config = config.context(
                "'source_path' and 'database_path' not found in arguments or config file.",
            )?;
            (config.source_path, config.database_path)
        }
        (None, Some(v)) => {
            let config = config.context("'source_path' not found in arguments or config file.")?;
            (config.source_path, v.clone())
        }
        (Some(v), None) => {
            let config =
                config.context("'database_path' not found in arguments or config file.")?;
            (v.clone(), config.database_path)
        }
        (Some(s), Some(d)) => (s.clone(), d.clone()),
    };

    match args.command {
        Commands::Create(ref v) => {
            if let Err(err) = command::create(&source, &v.migration_name) {
                tracing::error!("{}", err.to_string());
                anyhow::bail!(err);
            }
        }
        Commands::Up(UpArgs { n }) => {
            let migrations = Migrations::from_directory(&source)?;

            let mut conn = Connection::open(&db_path)?;
            if let Some(version) = n {
                let cur_version: usize = migrations.current_version(&conn)?.into();
                migrations.to_version(&mut conn, cur_version + version)?;
            } else {
                migrations.to_latest(&mut conn)?;
            }
        }
        Commands::Down(DownArgs { n }) => {
            let migrations = Migrations::from_directory(&source)?;

            let mut conn = Connection::open(db_path)?;
            if let Some(steps_down) = n {
                let cur_version: usize = migrations.current_version(&conn)?.into();
                let end_version = cur_version
                    .checked_sub(steps_down)
                    .ok_or(anyhow!("The number of steps down is too large."))?;
                migrations.to_version(&mut conn, end_version)?;
            } else {
                migrations.to_version(&mut conn, 0)?;
            }
        }
    }

    println!("{:#?}", args);
    Ok(())
}
