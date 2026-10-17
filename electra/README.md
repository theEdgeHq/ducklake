# Electra DuckLake build

## Glossary

- **DuckDB** is the embedded SQL database used by the extension.
- **DuckLake** is the catalog and table extension built in this repository.
- **ADBC** is the database driver interface used by the integration test.
- **PostgreSQL** is the metadata database used by DuckLake.
- **HTTPFS** is the DuckDB extension for HTTP and object storage access.
- **CRoaring** is the compressed bitmap library used by DuckLake.
- **AMD64** is the Linux processor target used by the release binary.

This directory builds the patched DuckLake extension for DuckDB 1.5.5.
The build uses a clean checkout and records the source commit in the output.
The DuckDB source is pinned to `d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`.

Run the build on Linux AMD64:

```sh
electra/build_linux.sh /path/to/new-build-directory
```

The build downloads and verifies the DuckDB driver, HTTPFS, PostgreSQL scanner,
and CRoaring inputs. It writes the loadable extension to
`build/extension/ducklake/ducklake.duckdb_extension`.

The GitHub Actions workflow runs only from manual dispatch. It starts a clean
PostgreSQL 16 service and runs 500 metadata checks plus 50 concurrent ADBC
`MERGE` rounds with an external writer. It uploads validation logs for every
run. When `publish_draft` is true, it creates a new draft release only after
all checks pass.

The release package contains the Linux AMD64 extension, checksums, source and
build metadata, and the raw test results. Release tags use this form:
`electra-duckdb-1.5.5-patch.1`.
