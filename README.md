# nu_plugin_bigquery

A [Nushell](https://www.nushell.sh/) plugin for querying Google BigQuery directly from your shell. Returns native Nushell tables or Arrow IPC files for seamless integration with Nushell's Polars dataframe ecosystem.

## Features

- **Native Nushell tables** — query results are real Nushell tables you can pipe to `where`, `select`, `sort-by`, etc.
- **Arrow IPC output** — `--arrow` flag writes results as Arrow IPC files for zero-copy `polars open` ingestion
- **Zero-config auth** — works out of the box with `gcloud auth application-default login`, or use explicit service account keys
- **Full BigQuery type support** — INTEGER, FLOAT, BOOLEAN, STRING, TIMESTAMP, DATE, DATETIME, BYTES, NUMERIC, RECORD, REPEATED, and more
- **Server-side filtering** — `bq read --filter` pushes WHERE clauses to BigQuery
- **Cost estimation** — `bq query --dry-run` shows bytes processed and estimated cost before running

## Requirements

- Nushell 0.110.0+
- Rust toolchain (to build from source)
- Google Cloud credentials (one of):
  - `gcloud auth application-default login` (recommended for development)
  - Service account key JSON file
  - GCE metadata server (on Google Cloud VMs)

## Installation

```bash
# Build
cargo build --release

# Register with Nushell
plugin add ./target/release/nu_plugin_bigquery
plugin use bigquery
```

## Commands

All commands are available under both `bq` and `bigquery` prefixes.

### `bq query` — Run SQL queries

```nushell
# Returns a Nushell table
bq query "SELECT name, age FROM `project.dataset.users` WHERE age > 30"

# Pipe to standard Nushell commands
bq query "SELECT * FROM dataset.sales" | where region == "US" | sort-by revenue --reverse

# Arrow IPC output for polars (returns LazyFrame, must collect)
bq query --arrow "SELECT * FROM dataset.large_table" | polars open $in | polars collect

# Check bytes processed before running
bq query --dry-run "SELECT * FROM dataset.huge_table"
# => {bytes_processed: 4500000000, bytes_processed_human: "4.19 GB"}
```

| Flag | Short | Description |
|------|-------|-------------|
| `--project` | `-p` | GCP project ID |
| `--credentials` | `-c` | Path to service account key JSON |
| `--location` | `-l` | BigQuery location (e.g., `US`, `EU`) |
| `--max-results` | `-n` | Maximum rows to return |
| `--timeout` | | Query timeout in milliseconds |
| `--arrow` | `-a` | Write Arrow IPC file, return path |
| `--dry-run` | | Show bytes processed without running |

### `bq read` — Read table data directly (no SQL)

```nushell
# Read with server-side column selection
bq read my_dataset.my_table --columns [user_id, event_type, ts]

# Server-side row filtering (SQL WHERE syntax)
bq read my_dataset.events --filter "ts > '2024-06-01'" --columns [user_id, event_type]

# Arrow output for large tables (LazyFrame, must collect)
bq read my_dataset.clickstream --arrow | polars open $in | polars collect
```

| Flag | Short | Description |
|------|-------|-------------|
| `--columns` | | Columns to read (server-side projection) |
| `--filter` | | Row filter (SQL WHERE syntax) |
| `--max-results` | `-n` | Maximum rows to return |
| `--project` | `-p` | GCP project ID |
| `--credentials` | `-c` | Path to service account key JSON |
| `--arrow` | `-a` | Write Arrow IPC file, return path |

Table reference formats: `dataset.table` or `project.dataset.table`

### `bq datasets` — List datasets

```nushell
bq datasets
bq datasets | where name =~ "prod"
```

### `bq tables` — List tables in a dataset

```nushell
bq tables my_dataset
# => table_name, table_type, num_rows, num_bytes, creation_time
```

### `bq schema` — Inspect table schema

```nushell
bq schema my_dataset.my_table
# => name, type, mode, description
```

Nested RECORD fields are flattened with dot notation (e.g., `address.city`).

## Authentication

Credentials are resolved in this order:

1. `--credentials` flag (path to service account key JSON)
2. `$env.GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. Application Default Credentials (`gcloud auth application-default login`)

Project ID is resolved in this order:

1. `--project` flag
2. `$env.BQ_PROJECT`, `$env.GOOGLE_CLOUD_PROJECT`, or `$env.GCLOUD_PROJECT`
3. Project from the credential/ADC configuration

```nushell
# Set defaults via environment
$env.GOOGLE_APPLICATION_CREDENTIALS = "/path/to/key.json"
$env.BQ_PROJECT = "my-project"
```

## Polars Integration

There are two ways to get BigQuery data into polars:

| Approach | Best for | How it works |
|----------|----------|--------------|
| `\| polars into-df` | Up to ~100K rows | BQ → Nushell table → eager DataFrame |
| `--arrow \| polars open $in \| polars collect` | 100K+ rows or memory-constrained | BQ → Arrow IPC file → zero-copy LazyFrame |

### Up to ~100K rows: pipe to `polars into-df`

The simpler path. Returns an eager DataFrame you can work with right away — no extra `collect` step needed:

```nushell
bq query "SELECT region, product, revenue FROM analytics.sales"
  | polars into-df
  | polars group-by region
  | polars agg (polars col revenue | polars sum)
  | polars into-nu
```

### 100K+ rows: `--arrow` flag (LazyFrame via Arrow IPC)

For larger datasets, the default path converts every row through Nushell's Value types, which is slow and doubles memory usage. The `--arrow` flag skips that conversion — it writes an Arrow IPC file directly and returns the path. Pipe to `polars open` for a zero-copy LazyFrame.

**You must end the pipeline with `polars collect`** to materialize results — this is by design, so polars can push down filters and projections before loading data into memory:

```nushell
# polars open returns a LazyFrame — chain operations, then collect
bq query --arrow "SELECT * FROM analytics.large_table"
  | polars open $in
  | polars filter ((polars col status) == "active")
  | polars group-by category
  | polars agg (polars col amount | polars mean)
  | polars collect

# If you just want all data as a DataFrame, still need collect at the end
bq query --arrow "SELECT * FROM analytics.large_table"
  | polars open $in
  | polars collect
```

## Type Mapping

| BigQuery Type | Nushell Value | Arrow Type |
|---------------|---------------|------------|
| INTEGER / INT64 | Int | Int64 |
| FLOAT / FLOAT64 | Float | Float64 |
| BOOLEAN / BOOL | Bool | Boolean |
| STRING | String | Utf8 |
| BYTES | Binary | Binary |
| TIMESTAMP | Date | Timestamp(us, UTC) |
| DATE | Date | Date32 |
| DATETIME | Date | Timestamp(us, None) |
| NUMERIC / BIGNUMERIC | String | Utf8 |
| GEOGRAPHY / JSON / TIME | String | Utf8 |
| RECORD / STRUCT | Record | Struct |
| REPEATED | List | List |

## License

MIT
