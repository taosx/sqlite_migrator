use std::{
    cmp::{self, Ordering},
    fmt,
    fs::DirEntry,
    num::NonZeroUsize,
    path::Path,
    ptr::addr_of,
};

use anyhow::{anyhow, Context, Result};

use rusqlite::{Connection, OptionalExtension, Transaction};
use tracing::{debug, info, trace, warn};

use crate::loader::{from_directory, MigrationFile};

pub type HookResult = Result<()>;

/// Helper trait to make hook functions clonable.
pub trait MigrationHook: Fn(&Transaction) -> HookResult + Send + Sync {
    /// Clone self.
    fn clone_box(&self) -> Box<dyn MigrationHook>;
}

impl<T> MigrationHook for T
where
    T: 'static + Clone + Send + Sync + Fn(&Transaction) -> HookResult,
{
    fn clone_box(&self) -> Box<dyn MigrationHook> {
        Box::new(self.clone())
    }
}

impl std::fmt::Debug for Box<dyn MigrationHook> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MigrationHook({:#x})", addr_of!(*self) as usize)
    }
}

impl Clone for Box<dyn MigrationHook> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}

#[derive(Debug, Clone)]
pub struct M {
    up: String,
    up_hook: Option<Box<dyn MigrationHook>>,
    down: Option<String>,
    down_hook: Option<Box<dyn MigrationHook>>,
    foreign_key_check: bool,
    comment: Option<String>,
}

impl M {
    pub const fn up(sql: String) -> Self {
        Self {
            up: sql,
            up_hook: None,
            down: None,
            down_hook: None,
            foreign_key_check: false,
            comment: None,
        }
    }

    pub fn comment(mut self, comment: String) -> Self {
        self.comment = Some(comment);
        self
    }

    pub fn down(mut self, sql: String) -> Self {
        self.down = Some(sql);
        self
    }
}

impl<'a> From<&'a MigrationFile> for M {
    fn from(value: &'a MigrationFile) -> Self {
        M::up(value.up.clone())
            .comment(value.name.clone())
            .down(value.down.clone().unwrap_or_default())
    }
}

/// Schema version, in the context of Migrations
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SchemaVersion {
    /// No schema version set
    NoneSet,
    /// The current version in the database is inside the range of defined
    /// migrations
    Inside(NonZeroUsize),
    /// The current version in the database is outside any migration defined
    Outside(NonZeroUsize),
}

impl From<&SchemaVersion> for usize {
    /// Translate schema version to db version
    fn from(schema_version: &SchemaVersion) -> usize {
        match schema_version {
            SchemaVersion::NoneSet => 0,
            SchemaVersion::Inside(v) | SchemaVersion::Outside(v) => From::from(*v),
        }
    }
}

impl From<SchemaVersion> for usize {
    fn from(schema_version: SchemaVersion) -> Self {
        From::from(&schema_version)
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaVersion::NoneSet => write!(f, "0 (no version set)"),
            SchemaVersion::Inside(v) => write!(f, "{v} (inside)"),
            SchemaVersion::Outside(v) => write!(f, "{v} (outside)"),
        }
    }
}

impl cmp::PartialOrd for SchemaVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_usize: usize = self.into();
        let other_usize: usize = other.into();

        self_usize.partial_cmp(&other_usize)
    }
}

/// Set of migrations
// PartialEq, Eq,
#[derive(Debug, Clone)]
pub struct Migrations {
    ms: Vec<M>,
}

impl Migrations {
    #[must_use]
    pub fn new(ms: Vec<M>) -> Self {
        Self { ms }
    }

    pub fn from_directory(dir: &Path) -> Result<Self> {
        let migrations = from_directory(dir)?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(anyhow::format_err!("Could not load migrations".to_string()))?;

        Ok(Self { ms: migrations })
    }

    fn db_version_to_schema(&self, db_version: usize) -> SchemaVersion {
        match db_version {
            0 => SchemaVersion::NoneSet,
            v if v > 0 && v <= self.ms.len() => SchemaVersion::Inside(
                NonZeroUsize::new(v).expect("schema version should not be equal to 0"),
            ),
            v => SchemaVersion::Outside(
                NonZeroUsize::new(v).expect("schema version should not be equal to 0"),
            ),
        }
    }

    pub fn current_version(&self, conn: &Connection) -> Result<SchemaVersion> {
        Ok(user_version(conn).map(|v| self.db_version_to_schema(v))?)
    }

    fn goto_up(
        &self,
        conn: &mut Connection,
        current_version: usize,
        target_version: usize,
    ) -> Result<()> {
        debug_assert!(current_version <= target_version);
        debug_assert!(target_version <= self.ms.len());

        trace!("start migration transaction");
        let tx = conn.transaction()?;

        for v in current_version..target_version {
            let m = &self.ms[v];
            debug!("Running: {}", m.up);

            tx.execute_batch(&m.up)
                .context(anyhow::format_err!("query: {}", m.up))?;

            if m.foreign_key_check {
                validate_foreign_keys(&tx)?;
            }

            if let Some(hook) = &m.up_hook {
                hook(&tx)?;
            }
        }

        set_user_version(&tx, target_version)?;
        tx.commit()?;
        trace!("commited migration transaction");

        Ok(())
    }

    /// Migrate downward. This is rolled back on error.
    /// All versions are db versions
    fn goto_down(
        &self,
        conn: &mut Connection,
        current_version: usize,
        target_version: usize,
    ) -> Result<()> {
        debug_assert!(current_version >= target_version);
        debug_assert!(target_version <= self.ms.len());

        // First, check if all the migrations have a "down" version
        if let Some((i, bad_m)) = self
            .ms
            .iter()
            .enumerate()
            .skip(target_version)
            .take(current_version - target_version)
            .find(|(_, m)| m.down.is_none())
        {
            warn!("Cannot revert: {:?}", bad_m);
            anyhow::bail!(
                "migration definition: down not defined migration_index: {}",
                i
            )
        }

        trace!("start migration transaction");
        let tx = conn.transaction()?;
        for v in (target_version..current_version).rev() {
            let m = &self.ms[v];
            if let Some(down) = &m.down {
                debug!("Running: {}", &down);

                if let Some(hook) = &m.down_hook {
                    hook(&tx)?;
                }

                tx.execute_batch(down)
                    .context(anyhow::format_err!("query: {}", down))?;
            } else {
                unreachable!();
            }
        }
        set_user_version(&tx, target_version)?;
        tx.commit()?;
        trace!("committed migration transaction");
        Ok(())
    }

    /// Go to a given db version
    fn goto(&self, conn: &mut Connection, target_db_version: usize) -> Result<()> {
        let current_version = user_version(conn)?;

        let res = match target_db_version.cmp(&current_version) {
            Ordering::Less => {
                if current_version > self.ms.len() {
                    anyhow::bail!("migration definition: database too far ahead")
                }
                debug!(
						"rollback to older version requested, target_db_version: {}, current_version: {}",
						target_db_version, current_version
					);
                self.goto_down(conn, current_version, target_db_version)
            }
            Ordering::Equal => {
                debug!("no migration to run, db already up to date");
                return Ok(()); // return directly, so the migration message is not printed
            }
            Ordering::Greater => {
                debug!(
						"some migrations to run, target: {target_db_version}, current: {current_version}"
					);
                self.goto_up(conn, current_version, target_db_version)
            }
        };

        if res.is_ok() {
            info!("Database migrated to version {}", target_db_version);
        }
        res
    }

    /// Maximum version defined in the migration set
    fn max_schema_version(&self) -> SchemaVersion {
        match self.ms.len() {
            0 => SchemaVersion::NoneSet,
            v => SchemaVersion::Inside(
                NonZeroUsize::new(v).expect("schema version should not be equal to 0"),
            ),
        }
    }

    pub fn to_latest(&self, conn: &mut Connection) -> Result<()> {
        let v_max = self.max_schema_version();
        match v_max {
            SchemaVersion::NoneSet => {
                warn!("no migration defined");
                anyhow::bail!("migration definition: no migration defined")
            }
            SchemaVersion::Inside(v) => {
                debug!("some migrations defined (version: {v}), try to migrate");
                self.goto(conn, v_max.into())
            }
            SchemaVersion::Outside(_) => unreachable!(),
        }
    }

    pub fn to_version(&self, conn: &mut Connection, version: usize) -> Result<()> {
        let target_version: SchemaVersion = self.db_version_to_schema(version);
        let v_max = self.max_schema_version();
        match v_max {
            SchemaVersion::NoneSet => {
                warn!("no migrations defined");
                anyhow::bail!("migration definition: no migration defined")
            }
            SchemaVersion::Inside(v) => {
                debug!("some migrations defined (version: {v}), try to migrate");
                if target_version > v_max {
                    warn!("specified version is higher than the max supported version");
                    anyhow::bail!(
                        "specified schema version {}: higher than max supported version {}",
                        target_version,
                        v_max
                    )
                }

                self.goto(conn, target_version.into())
            }
            SchemaVersion::Outside(_) => unreachable!(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        self.to_latest(&mut conn)
    }
}

// Set user version field from the SQLite db
fn set_user_version(conn: &Connection, v: usize) -> Result<()> {
    trace!("set user version to: {}", v);
    // We can’t fix this without breaking API compatibility
    #[allow(clippy::cast_possible_truncation)]
    let v = v as u32;
    conn.pragma_update(None, "user_version", v)
        .context(anyhow::format_err!(
            "query: 'PRAGMA user_version = {v}; -- Approximate query'",
        ))
}

// Validate that no foreign keys are violated
fn validate_foreign_keys(conn: &Connection) -> Result<()> {
    let pragma_fk_check = "PRAGMA foreign_key_check";
    conn.query_row(pragma_fk_check, [], |row| {
        Ok(anyhow::format_err!(
            "table: {:?}, rowid: {:?}, parent: {:?}, fkid: {:?}",
            row.get::<usize, String>(0)?,
            row.get::<usize, i64>(1)?,
            row.get::<usize, String>(2)?,
            row.get::<usize, i64>(3)?,
        ))
    })
    .optional()
    .context(anyhow::format_err!("query: {}", pragma_fk_check))
    .and_then(|o| match o {
        Some(e) => Err(anyhow::format_err!("foreign key error: {}", e)),
        None => Ok(()),
    })
}

// Read user version field from the SQLite db
fn user_version(conn: &Connection) -> Result<usize, rusqlite::Error> {
    // We can’t fix this without breaking API compatibility
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    conn.query_row("PRAGMA user_version", [], |row| row.get(0))
        .map(|v: i64| v as usize)
}
