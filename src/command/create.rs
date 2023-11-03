use std::{
    fs::{self, File},
    io::Write,
    ops::ControlFlow,
    path::Path,
};

use anyhow::{Context, Result};
use chrono::Local;

pub fn create(migration_dir: &Path, migration_name: &str) -> Result<()> {
    if !migration_dir.exists() {
        fs::create_dir(migration_dir).context("Failed to create migration directory.")?;
    }

    // Determine the sequence number for the new migration folder
    let max_sequence_number = fs::read_dir(migration_dir)
        .context("Failed to read migration directory")?
        .filter_map(|res| res.map(|e| e.path()).ok())
        .filter_map(|entry| {
            let dir_name = entry.file_name()?.to_str()?;
            let parts: Vec<&str> = dir_name.split('-').collect();
            if !entry.is_dir() || parts.len() < 2 {
                return None;
            }
            parts.first().and_then(|v| v.parse::<u32>().ok())
        })
        .max()
        .unwrap_or(0);

    // Generate a new folder name with a 4-digit sequence number.
    let new_sequence_number = max_sequence_number + 1;
    let folder_name = format!(
        "{:04}-{}",
        new_sequence_number,
        migration_name
            .replace(['-', ' '], "_")
            .trim_end_matches('_')
    );

    // Create the new folder inside the source directory.
    let migration_folder = migration_dir.join(&folder_name);
    fs::create_dir(&migration_folder)
        .context("Failed to create new migration folder inside migration directory.")?;

    // Generate and write the current date as a comment in up.sql and down.sql.
    let current_date = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let comment = |up_or_down: &str| {
        format!(
            "-- {} migration `{folder_name}` generated at {current_date}.",
            up_or_down
        )
    };

    let up_sql_path = migration_folder.join("up.sql");
    let down_sql_path = migration_folder.join("down.sql");

    File::create(up_sql_path)
        .and_then(|mut file| file.write_all(comment("Up").as_bytes()))
        .context("Failed to create and write up.sql")?;

    File::create(down_sql_path)
        .and_then(|mut file| file.write_all(comment("Down").as_bytes()))
        .context("Failed to create and write down.sql")?;

    Ok(())
}
