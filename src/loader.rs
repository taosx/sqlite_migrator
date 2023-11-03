use anyhow::{format_err, Result};
use std::{
    fs::{self, DirEntry, File},
    io::Read,
    num::NonZeroUsize,
    path::Path,
};

use crate::migration::M;

#[derive(Debug, Clone)]
pub struct MigrationFile {
    pub id: NonZeroUsize,
    pub name: String,
    pub up: String,
    pub down: Option<String>,
}

fn get_name(value: &DirEntry) -> Result<String> {
    Ok(value
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(format_err!(
            "Could not extract file name from {:?}",
            value.path()
        ))?
        .to_owned())
    // .ok_or(Error::FileLoad(format!(
    //     "Could not extract file name from {:?}",
    //     value.path()
    // )))
}

fn get_migrations(name: &str, value: &DirEntry) -> Result<(String, Option<String>)> {
    let mut up = String::new();
    let mut down = None;

    for entry in std::fs::read_dir(value.path())? {
        let entry = entry?;
        let file_name = entry.file_name().into_string().unwrap();

        if file_name.ends_with("up.sql") {
            let mut file = File::open(entry.path())?;
            file.read_to_string(&mut up)?;
        } else if file_name.ends_with("down.sql") {
            let down_file = fs::read_to_string(entry.path()).ok();
            if let Some(down_file) = down_file {
                down = Some(down_file);
            }
        }
    }

    Ok((up, down))
}

fn get_id(file_name: &str) -> Result<NonZeroUsize> {
    file_name
        .split_once('-')
        .ok_or(format_err!(
            "Could not extract migration id from file name {file_name}"
        ))?
        .0
        .parse::<usize>()
        .map_err(|e| {
            format_err!("Could not parse migration id from file name {file_name} as usize: {e}")
        })
        .and_then(|v| {
            NonZeroUsize::new(v).ok_or(format_err!(
                "{file_name} has an incorrect migration id: migration id cannot be 0"
            ))
        })
}

impl<'a> TryFrom<&'a DirEntry> for MigrationFile {
    type Error = anyhow::Error;

    fn try_from(value: &DirEntry) -> std::result::Result<Self, Self::Error> {
        let name = get_name(value)?;
        let (up, down) = get_migrations(&name, value)?;
        let id = get_id(&name)?;

        Ok(MigrationFile {
            id,
            name,
            up: up.to_string(),
            down: down.map(|f| f.to_string()),
        })
    }
}

pub fn from_directory(dir: &Path) -> Result<Vec<Option<M>>> {
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, std::io::Error>>()?;
    entries.sort_by_key(|e| e.file_name());
    let entries = entries;

    let mut migrations: Vec<Option<M>> = vec![None; entries.len()];

    for dir in entries {
        let migration_file = MigrationFile::try_from(&dir)?;

        let id = usize::from(migration_file.id) - 1;
        if migrations.len() <= id {
            anyhow::bail!("Migration ids must be consecutive numbers");
        }

        if migrations[id].is_some() {
            anyhow::bail!(
                "Multiple migrations detected for migration id: {}",
                migration_file.id
            );
        }

        migrations[id] = Some((&migration_file).into());
    }

    if migrations.iter().all(|m| m.is_none()) {
        anyhow::bail!("Directory does not contain any migration files".to_string(),);
    }

    if migrations.iter().any(|m| m.is_none()) {
        anyhow::bail!("Migration ids must be consecutive numbers".to_string(),);
    }

    // The values are returned in the order of the keys, i.e. of IDs
    Ok(migrations)
}
