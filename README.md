# DuckDB ADBC Extension

Use DuckDB (v1.4 or later) to query [Snowflake](https://www.snowflake.com), [Databricks](https://www.databricks.com), [BigQuery](https://cloud.google.com/bigquery), [PostgreSQL](https://www.postgresql.org), [MySQL](https://www.mysql.com), or any other system  with an [ADBC driver](https://columnar.tech/dbc)!

## What is ADBC?

ADBC (Arrow Database Connectivity) is a universal data-access API built on [Apache Arrow](https://arrow.apache.org/), an efficient, columnar data format that almost [every data system](https://arrow.apache.org/powered_by/) supports natively. 

By building on Arrow, ADBC enables:
1. Lightning fast (zero-copy) data transfer between column-oriented analytical systems, bypassing the slow column-to-row and row-to-column conversions typical of legacy row-based APIs like ODBC or JDBC.
2. Seamless interoperability with a large and growing ecosystem of Arrow-compatible data systems.

## Extension Installation

You can install the ADBC extension by running:

```sql
INSTALL adbc FROM community;
LOAD adbc;
```

If you prefer to build from source, you can install `cmake` and `ninja` and run:

```sh
# Clone the repo and its dependencies
git clone --recurse-submodules git@github.com:columnar-tech/duckdb-adbc-client.git
cd duckdb-adbc-client
# Build the extension from source
GEN=ninja make release
# Build DuckDB from source
cd duckdb
GEN=ninja make release
# Run DuckDB in unsigned mode to load untrusted extensions
./build/release/duckdb -unsigned
```

You can then load the extension by running:

```sql
LOAD './build/release/adbc/adbc.duckdb_extension'
```

## Installing ADBC Drivers

[`dbc`](https://columnar.tech/dbc/) is a command-line tool that makes it easy to install and manage ADBC drivers. 

You can install `dbc` by running one of the commands below:

```sh
# shell
curl -LsSf https://dbc.columnar.tech/install.sh | sh
# brew
brew install columnar-tech/tap/dbc
# uv
uv tool install dbc
# pipx
pipx install dbc
# powershell
powershell -ExecutionPolicy ByPass -c irm https://dbc.columnar.tech/install.ps1 | iex
# winget
winget install dbc
```

After installing `dbc`, you can run `dbc install <system>` to install a driver for a new system.

```sh
dbc install postgresql
```

To easily manage connection information for each system, you can create a [connection profile](https://arrow.apache.org/adbc/main/format/connection_profiles.html) for each driver (i.e., `postgresql.toml`):

```toml
profile_version = 1
driver = "postgresql"

[Options]
uri = "postgresql://localhost:5432/demo"
```

The last step is to save your profile in the correct location:

```sh
# Linux
mv postgresql.toml ~/.config/adbc/profiles/
# macOS
mv postgresql.toml ~/Library/Application Support/ADBC/Profiles/
# Windows
move "postgresql.toml" "%LOCALAPPDATA%\ADBC\Profiles\"
```

## Quickstart

### read_adbc

To read data through ADBC you can call the `read_adbc` table function by providing a URI to the connection profile and a SQL query. 

```sql
D SELECT * FROM read_adbc('profile://postgresql', 'SELECT * FROM games');
┌───────┬────────────┬─────────────────────┬───────┬─────────┬─────────────┬─────────────┬────────────┐
│  id   │    name    │      inventor       │ year  │ min_age │ min_players │ max_players │ list_price │
│ int32 │  varchar   │       varchar       │ int16 │  int16  │    int16    │    int16    │  varchar   │
├───────┼────────────┼─────────────────────┼───────┼─────────┼─────────────┼─────────────┼────────────┤
│     1 │ Monopoly   │ Elizabeth Magie     │  1903 │       8 │           2 │           6 │ 19.99      │
│     2 │ Scrabble   │ Alfred Mosher Butts │  1938 │       8 │           2 │           4 │ 17.99      │
│     3 │ Clue       │ Anthony E. Pratt    │  1944 │       8 │           2 │           6 │ 9.99       │
│     4 │ Candy Land │ Eleanor Abbott      │  1948 │       3 │           2 │           4 │ 7.99       │
│     5 │ Risk       │ Albert Lamorisse    │  1957 │      10 │           2 │           5 │ 29.99      │
└───────┴────────────┴─────────────────────┴───────┴─────────┴─────────────┴─────────────┴────────────┘
```

### adbc_execute

To perform arbitrarys statements on the database ADBC, you can call `adbc_execute`.

```sql
D CALL adbc_execute('profile://postgresql', 'DROP TABLE public.games');
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

### ATTACH

To create a persistent connection to an ADBC database, you can run the `ATTACH` command and then query the ADBC database as if it were a local DuckDB database. We currently support catalog lookups, as well as `SELECT`, `INSERT`, `COPY`, and `CREATE TABLE AS (SELECT ...)` (`CTAS`) statements.

```sql
D ATTACH 'profile://postgresql' AS mydb (TYPE adbc);
D USE mydb.public;
D SHOW ALL TABLES;
┌──────────┬─────────┬─────────┬──────────────────────┬──────────────────────────────────────────────────┬───────────┐
│ database │ schema  │  name   │     column_names     │                   column_types                   │ temporary │
│ varchar  │ varchar │ varchar │      varchar[]       │                    varchar[]                     │  boolean  │
├──────────┼─────────┼─────────┼──────────────────────┼──────────────────────────────────────────────────┼───────────┤
│ mydb     │ public  │ games   │ [id, name, invento…  │ [INTEGER, VARCHAR, VARCHAR, SMALLINT, SMALLINT…  │ false     │
└──────────┴─────────┴─────────┴──────────────────────┴──────────────────────────────────────────────────┴───────────┘
D SELECT * FROM games;
┌───────┬────────────┬─────────────────────┬───────┬─────────┬─────────────┬─────────────┐
│  id   │    name    │      inventor       │ year  │ min_age │ min_players │ max_players │
│ int32 │  varchar   │       varchar       │ int16 │  int16  │    int16    │    int16    │
├───────┼────────────┼─────────────────────┼───────┼─────────┼─────────────┼─────────────┤
│     1 │ Monopoly   │ Elizabeth Magie     │  1903 │       8 │           2 │           6 │
│     2 │ Scrabble   │ Alfred Mosher Butts │  1938 │       8 │           2 │           4 │
│     3 │ Clue       │ Anthony E. Pratt    │  1944 │       8 │           2 │           6 │
│     4 │ Candy Land │ Eleanor Abbott      │  1948 │       3 │           2 │           4 │
│     5 │ Risk       │ Albert Lamorisse    │  1957 │      10 │           2 │           5 │
└───────┴────────────┴─────────────────────┴───────┴─────────┴─────────────┴─────────────┘
D INSERT INTO games (SELECT 6, 'Battleship', 'Clifford Von Wickler', 1931, 7, 2, 2);
D SELECT * FROM games;
┌───────┬────────────┬──────────────────────┬───────┬─────────┬─────────────┬─────────────┐
│  id   │    name    │       inventor       │ year  │ min_age │ min_players │ max_players │
│ int32 │  varchar   │       varchar        │ int16 │  int16  │    int16    │    int16    │
├───────┼────────────┼──────────────────────┼───────┼─────────┼─────────────┼─────────────┤
│     1 │ Monopoly   │ Elizabeth Magie      │  1903 │       8 │           2 │           6 │
│     2 │ Scrabble   │ Alfred Mosher Butts  │  1938 │       8 │           2 │           4 │
│     3 │ Clue       │ Anthony E. Pratt     │  1944 │       8 │           2 │           6 │
│     4 │ Candy Land │ Eleanor Abbott       │  1948 │       3 │           2 │           4 │
│     5 │ Risk       │ Albert Lamorisse     │  1957 │      10 │           2 │           5 │
│     6 │ Battleship │ Clifford Von Wickler │  1931 │       7 │           2 │           2 │
└───────┴────────────┴──────────────────────┴───────┴─────────┴─────────────┴─────────────┘
D CREATE TABLE game_inventors(id, inventor) AS (SELECT id, inventor FROM games);
D SELECT * FROM game_inventors;
┌───────┬──────────────────────┐
│  id   │       inventor       │
│ int32 │       varchar        │
├───────┼──────────────────────┤
│     1 │ Elizabeth Magie      │
│     2 │ Alfred Mosher Butts  │
│     3 │ Anthony E. Pratt     │
│     4 │ Eleanor Abbott       │
│     5 │ Albert Lamorisse     │
│     6 │ Clifford Von Wickler │
└───────┴──────────────────────┘
```

### Custom Delimiters

By default, `ATTACH` delimits all SQL queries with double quotes (i.e., `SELECT * FROM "schema"."table"`). The `DELIMITER` option adds support for systems with different schema/table delimiters (i.e., `[schema].[table]` for SQL Server).

```sql
D ATTACH 'profile://postgresql' AS mydb (TYPE adbc, DELIMITER '[]');
```

### adbc_clear_cache

DuckDB caches schema and table metadata from ADBC databases locally. To clear the cached metadata (i.e., after a remote update), you can invoke `adbc_clear_cache`

```sql
D CALL adbc_clear_cache();
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

## Advanced Features

By default, mixing ADBC reads and writes in the same SQL statement will throw an error to prevent potential concurrency bugs. To override this warning and enable mixing ADBC reads and writes, you can set `adbc_mix_reads_writes` to `true`.

```sql
D INSERT INTO games (SELECT * FROM games);
Not implemented Error: ...
D SET adbc_mix_reads_writes = true;
D INSERT INTO games (SELECT * FROM games);
D 
```

To materialize all input rows to an `INSERT` or `CTAS` and prevent concurrency bugs when mixing ADBC reads and writes, you can set `adbc_materialize_insert_rows` to `true`.

```sql
D SET adbc_materialize_insert_rows = true;
```

## Tuning

To learn more about the internal design of the ADBC extension for tuning, you can read [DESIGN.md](./DESIGN.md).

### Connection Pool Size

Internally, the ADBC extension creates connections to perform SQL statements. To avoid repeatedly creating and destroying connections, each attached ADBC database maintains a connection pool. The pool is initially empty and grows as SQL statements create new connections, up to a default limit of 50. Once the pool is full, SQL statements create ephemeral connections, that are destroyed immediately after execution. To adjust the ADBC connection pool limit for each attached database, you can modify the value of `adbc_connection_pool_size`.

```sql
D SET adbc_connection_pool_size = 100;
```

### INSERT and CTAS Buffer Sizes

When performing `INSERT` and `CREATE TABLE AS (SELECT ...)` statements, the extension uses ADBC's bulk ingest API. To avoid materializing all input rows at once, the ADBC extension inserts batches of rows at a time. Internally, one thread appends rows to an in-memory buffer, and then another thread empties the buffer and inserts via ADBC. Increasing the buffer size may improve performance, but slow down query cancellation. The buffer is full when it exceeds 50% of the available memory or contains `adbc_insert_batch_size` chunks of 2048 rows each. The default value of `adbc_insert_buffer_size` is 1000, resulting in a buffer of 1000 x 2048 rows (i.e., ~2M rows). To adjust the insert buffer size, you can modify the value of `adbc_insert_buffer_size`.

```sql
D SET adbc_insert_buffer_size = 10000;
```
