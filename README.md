# DuckDB ADBC Extension
The ADBC extension allows DuckDB to directly access databases through the Arrow Database Connectivity (ADBC) API. ADBC enables efficient high-performance data transfer by exchanging data in Arrow format, a columnar data representation optimized for efficient analytics.

## Usage
To use the ADBC extension, you first can install it from DuckDB's community extesnsion repository with the following commands in the DuckDB shell:

```sql
INSTALL adbc FROM community;
LOAD adbc;
```

You can then read from an ADBC data source by calling the `read_adbc` table function, which takes a URI and SQL query as input parameters. 

For example, to query the `games` table from a PostgreSQL database `demo` running on `localhost:5432` you can run the following command from the DuckDB shell:

```sql
SELECT * FROM read_adbc('postgresql://localhost:5432/demo', 'SELECT * FROM games');
```

Note that you must have the corresponding ADBC driver installed in your system to read from a given data source.

You can find more examples about how to use ADBC at: <https://github.com/columnar-tech/adbc-quickstarts/>.

The easiest way to install ADBC drivers is through the command line tool `dbc`. 

You can find more details about `dbc` at: <https://columnar.tech/dbc/>. 

## Building From Source

To build the extension from source and install it locally you can follow the steps below:
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

Next, in the DuckDB shell, run the following command to load the ADBC extension:
```sql
LOAD '../build/release/adbc.duckdb_extension';
```
