### Contributing to the DuckDB ADBC Extension

## Reporting Issues

Please file issues on the GitHub issue tracker: https://github.com/columnar-tech/duckdb-adbc-client/issues

## Building

To build the extension from source you will need to install `cmake` and `ninja` and run:

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
LOAD '../build/release/extension/adbc/adbc.duckdb_extension';
```

## Vendoring ADBC

To avoid relying on DuckDB's ADBC implementation, we decided to vendor our own ADBC implementation into the extension.

The vendoring process (run with `vendor_adbc.sh`) copies files from the `arrow-adbc` submodule and wraps them in a `namespace Private { ...}` block.

The vendoring process decouples the extension's ADBC version from DuckDB's and prevents conflicting ADBC function definitions.

To upgrade the extension to use the latest version of ADBC you can:

```sh
cd ./arrow-adbc
git fetch
git checkout <latest-tag>
cd ..
git add arrow-adbc
./vendor_adbc.sh
```

The remainder of the extension relies on DuckDB's headers including routines to convert between DuckDB and Arrow format.

## Opening a Pull Request

Before opening a pull request:

Please check if there is a corresponding issue (and if not, please make one).
Please assign the issue to yourself.
At the bottom of the PR description, add `Closes #NNNN` where `NNNN` is the issue number, so that the issue gets linked to your PR properly. ("Fixes" and other keywords that GitHub recognizes are also OK, of course.)


When committing, please follow [Conventional
Commits][conventional-commits].  This helps maintain semantic
versioning of components.

Please use the following commit types: `build`, `chore`, `ci`, `docs`,
`feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`.

[conventional-commits]: https://www.conventionalcommits.org/en/v1.0.0/
