# DuckDB ADBC Extension
The ADBC extension allows DuckDB to directly access databases through the Arrow Database Connectivity (ADBC) API. ADBC enables efficient high-performance data transfer by exchanging data in Arrow format, a columnar data representation optimized for efficient analytics.

## Usage
To use the ADBC extension, you can install it from DuckDB's community extesnsion repository by running:

```sql
INSTALL adbc FROM community;
LOAD adbc;
```

To read from an ADBC data source you can call the `read_adbc` table function, which takes a URI and SQL query as input parameters. 

For example, to query the `games` table from a PostgreSQL database `demo` running on `localhost:5432` use:

```sql
SELECT * FROM read_adbc('postgresql://localhost:5432/demo', 'SELECT * FROM games');
```

Note that you must have the corresponding ADBC driver installed in your system to read from a given ADBC data source.

The easiest way to install ADBC drivers is through the command line tool `dbc`. 

You can find more details about `dbc` at: <https://columnar.tech/dbc/>. 

## Building From Source

To build the extension from source and install it locally you can run:
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

Next, in the DuckDB shell, run:
```sql
LOAD '../build/release/adbc.duckdb_extension';
``

Now that the extension is loaded, you can run `read_adbc` to read data using ADBC.
