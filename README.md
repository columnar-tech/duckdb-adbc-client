# DuckDB C/C++ extension template
This is an **experimental** template for C/C++ based extensions that link with the **C Extension API** of DuckDB. Note that this
is different from https://github.com/duckdb/extension-template, which links against the C++ API of DuckDB.

Features:
- No DuckDB build required
- CI/CD chain preconfigured
- (Coming soon) Works with community extensions

## Cloning
Clone the repo with submodules

```shell
git clone --recurse-submodules <repo>
```

## Dependencies
In principle, compiling this template only requires a C/C++ toolchain. However, this template relies on some additional
tooling to make life a little easier and to be able to share CI/CD infrastructure with extension templates for other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- CMake
- Git
- (Optional) Ninja + ccache

Installing these dependencies will vary per platform:
- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

## Building
After installing the dependencies, building is a two-step process. Firstly run:
```shell
make configure
```
This will ensure a Python venv is set up with DuckDB and DuckDB's test runner installed. Additionally, depending on configuration,
DuckDB will be used to determine the correct platform for which you are compiling.

Then, to build the extension run:
```shell
make debug
```
This delegates the build process to cargo, which will produce a shared library in `target/debug/<shared_lib_name>`. After this step, 
a script is run to transform the shared library into a loadable extension by appending a binary footer. The resulting extension is written
to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

### Faster builds
We recommend to install Ninja and Ccache for building as this can have a significant speed boost during development. After installing, ninja can be used 
by running:
```shell
make clean
GEN=ninja make debug
```

## Testing
This extension uses the DuckDB Python client for testing. This should be automatically installed in the `make configure` step.
The tests themselves are written in the SQLLogicTest format, just like most of DuckDB's tests. A sample test can be found in
`test/sql/<extension_name>.test`. To run the tests using the *debug* build:

```shell
make test_debug
```

or for the *release* build:
```shell
make test_release
```

### Version switching
Testing with different DuckDB versions is really simple:

First, run 
```
make clean_all
```
to ensure the previous `make configure` step is deleted.

Then, run 
```
DUCKDB_TEST_VERSION=v1.1.2 make configure
```
to select a different duckdb version to test with

Finally, build and test with 
```
make debug
make test_debug
```

### Using unstable Extension C API functionality
The DuckDB Extension C API has a stable part and an unstable part. By default, this template only allows usage of the stable
part of the API. To switch it to allow using the unstable part, take the following steps:

Firstly, set your `TARGET_DUCKDB_VERSION` to your desired in `./Makefile`. Then, run `make update_duckdb_headers` to ensure 
the headers in `./duckdb_capi` are set to the correct version. (FIXME: this is not yet working properly). 

Finally, set `USE_UNSTABLE_C_API` to 1 in `./Makefile`. That's all!