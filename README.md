# SQLite3 Migrator CLI

SQLite3 Migrator is a tool that allows you to manage SQLite database migrations from a specified directory. You can create new migrations, run migrations to update your database, and perform other operations. (alpha, still in development)

## Usage

migrator [OPTIONS] \<COMMAND\>

### Commands

`create`: Create a new migration.

`up`: Run migrations UP to the most recent one or up to migration number N if specified.

`down`: Run migrations DOWN to the oldest one or down to migration number N if specified.

`help`: Print this message or the help of the given subcommand(s).

### Options

`-s, --source <SOURCE>` (Environment Variable: MIGRATION_DIR) - Specify the directory containing migration files.

`-d, --database <DATABASE>` (Environment Variable: DATABASE_PATH) - Specify the path to the SQLite database file.

`-h, --help` - Print help.

**Note:** You can also configure the source and database path in a `.migrate-config.yaml` file.

## Example Usage

Here's an example of how to use SQLite3 Migrator:

```bash
migrator -s ./migrations -d ./database.sqlite3 up -n 3
```

This command will run the "up" migration for the specified database from the "./migrations" directory and apply the last 3 migrations.

## TODO

Here are some improvements planned for SQLite3 Migrator:

-   Add a library for handling errors.
-   Include exit codes for better error handling.

Feel free to contribute to the project and help make it even better!
