# bruin-fabric-lakehouse

A [Bruin](https://getbruin.com) connector for **Microsoft Fabric Lakehouses**,
with **Spark SQL** as the primary compute engine and the ability to drop into
**PySpark** when needed.

Bruin already ships a `fabric` connector, but it targets **Fabric Warehouse**
through the SQL (TDS) endpoint — T-SQL semantics, no Spark, and read-only
against lakehouse tables. This connector instead drives the **Fabric
Lakehouse Livy API**, the same interface Microsoft's
[dbt-fabricspark](https://github.com/microsoft/dbt-fabricspark) adapter is
built on, so assets execute inside a real Spark session and materialize Delta
tables natively in the lakehouse.

```
bruin asset (.sql / .py)
   │  rendered + materialized by bruin
   ▼
fabricspark.BasicOperator / PySparkOperator
   │  Livy statements (kind: sql | pyspark)
   ▼
POST {endpoint}/workspaces/{ws}/lakehouses/{lh}/livyapi/versions/2023-12-01/sessions/{id}/statements
   │
   ▼
Fabric Spark session ──► Delta tables in the lakehouse
```

## What you get

- **`fabric.spark.sql` assets** — Spark SQL with bruin materializations:
  `view`, `create+replace` (with `partition_by` / `cluster_by`), `append`,
  `merge`, `delete+insert`, `truncate+insert`, `time_interval`, `anti_join`,
  `ddl`, and full SCD2 (`scd2_by_column` / `scd2_by_time`, incremental and
  full-refresh).
- **`fabric.spark.pyspark` assets** — the asset's Python file runs as a
  PySpark statement in the *same* Spark session, with `spark` and `sc`
  predefined and the lakehouse as the default catalog.
- **`fabric.spark.seed` assets** — load a CSV into a Delta table natively
  through the Spark session (no ingestr required), with types taken from the
  asset's column definitions.
- **Sensors** — `fabric.spark.sensor.query` (poke a SQL condition) and
  `fabric.spark.sensor.table` (wait for a table to exist, via
  `SHOW TABLES` probing).
- **High-concurrency mode** — opt-in `high_concurrency: true` switches the
  connection to Fabric's `/highConcurrencySessions` API: a pool of REPLs
  packed onto one Spark application, so parallel assets execute concurrently
  instead of queueing on a single interpreter.
- **ingestr integration** — with `workspace_name` configured, the connection
  doubles as an ingestr **OneLake destination**, so ingestr assets can load
  external data into the same lakehouse this connector transforms.
- **Column quality checks** — `not_null`, `unique`, `positive`,
  `non_negative`, `negative`, `min`, `max`, `accepted_values`, `pattern`
  (via Spark `RLIKE`).
- **One session per run** — the connector lazily starts a single Livy
  session, reuses it for every statement (SQL and PySpark share temp views
  and catalog state), transparently recreates it if Fabric expires it, and
  can close it to release capacity.
- **Robust HTTP layer** — Azure AD service-principal auth with token
  caching, retries with exponential backoff, HTTP 429 `Retry-After`
  handling, and Fabric's transient-404 quirks accounted for.

## Repo layout

| Path | Purpose |
|------|---------|
| `fabricspark/` | The connector package, structured like a bruin `pkg/<platform>` package |
| `examples/fabric_lakehouse/` | A runnable example pipeline (SQL + PySpark assets, connection template) |
| `docs/DESIGN.md` | Architecture, diagrams, design decisions and trade-offs |
| `docs/INTEGRATION.md` | Exact wiring instructions to land this in bruin upstream |

The module compiles and tests standalone against
`github.com/bruin-data/bruin` as a library, so the operator, materializer and
check interfaces are the real bruin interfaces, not lookalikes:

```bash
go build ./...
go test ./...
```

## Connection configuration

```yaml
# .bruin.yml
environments:
  default:
    connections:
      fabric_spark:
        - name: fabric-spark-default
          workspace_id: "11111111-2222-3333-4444-555555555555"
          lakehouse_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
          lakehouse_name: my_lakehouse

          # Option A — service principal (recommended for CI):
          tenant_id: "99999999-8888-7777-6666-555555555555"
          client_id: "app-registration-client-id"
          client_secret: "app-registration-secret"

          # Option B — pre-acquired token (local development):
          # access_token: "$(az account get-access-token --scope 'https://analysis.windows.net/powerbi/api/.default' --query accessToken -o tsv)"
```

| Field | Required | Notes |
|-------|----------|-------|
| `workspace_id` | yes | Fabric workspace GUID |
| `workspace_name` | no | Workspace display name; required only for ingestr integration (the OneLake URI is name-based) |
| `lakehouse_id` | yes | Lakehouse item GUID |
| `lakehouse_name` | yes | Doubles as the database name for non-schema lakehouses |
| `schema` | no | Default schema for schema-enabled lakehouses (e.g. `dbo`) |
| `endpoint` | no | Defaults to `https://api.fabric.microsoft.com/v1` |
| `tenant_id` / `client_id` / `client_secret` | one of | Service-principal (client credentials) auth |
| `access_token` | one of | Static bearer token; takes precedence |
| `session_name` | no | Session name in the Fabric monitoring UI (default `bruin-fabric-spark`) |
| `environment_id` | no | Pin the session to a Fabric Environment (custom pool/libraries) |
| `spark_config` | no | Extra spark conf, e.g. `spark.sql.caseSensitive: "true"` |
| `high_concurrency` | no | Use `/highConcurrencySessions`: parallel assets run concurrently in one Spark app (default false) |
| `session_tag` | no | REPL packing key for high-concurrency mode; set explicitly to share a warm Spark app across bruin invocations (default: random per process) |
| `max_concurrent_repls` | no | REPL pool cap in high-concurrency mode (default 4) |
| `http_timeout_seconds` | no | Per-request timeout (default 120) |
| `session_start_timeout_seconds` | no | Session cold-start budget (default 600) |
| `statement_timeout_seconds` | no | Per-statement budget (default 43200; 0 = unlimited) |

The service principal needs access to the workspace (Contributor or higher)
and the tenant must allow service principals to use Fabric APIs. The token
scope is `https://analysis.windows.net/powerbi/api/.default`, matching
dbt-fabricspark.

## Asset examples

Spark SQL with a merge materialization:

```sql
/* @bruin
name: reporting.daily_event_counts
type: fabric.spark.sql
materialization:
  type: table
  strategy: merge
columns:
  - name: event_date
    type: date
    primary_key: true
  - name: event_count
    type: bigint
    update_on_merge: true
    checks:
      - name: non_negative
@bruin */

SELECT event_date, COUNT(*) AS event_count
FROM raw.events
GROUP BY event_date
```

PySpark in the same session:

```python
""" @bruin
name: transform.enrich_events
type: fabric.spark.pyspark
depends:
  - raw.events
@bruin """

from pyspark.sql import functions as F

events = spark.table("raw.events")
enriched = events.withColumn("processed_at", F.current_timestamp())
enriched.write.mode("overwrite").saveAsTable("transform.enriched_events")
print(f"enriched {enriched.count()} events")
```

See [`examples/fabric_lakehouse/`](examples/fabric_lakehouse/) for a full
pipeline.

## Materialization support

| Strategy | Supported | Implementation |
|----------|-----------|----------------|
| `view` | ✅ | `CREATE OR REPLACE VIEW` |
| `create+replace` | ✅ | Atomic `CREATE OR REPLACE TABLE ... AS` (Delta) with `partition_by` or `cluster_by` (liquid clustering, runtime 1.3+) |
| `append` | ✅ | `INSERT INTO` |
| `merge` | ✅ | `MERGE INTO` keyed on `primary_key` columns, honoring `update_on_merge` |
| `delete+insert` | ✅ | Temp view + `DELETE` + `INSERT` |
| `truncate+insert` | ✅ | `DELETE FROM` + `INSERT` (works on every Delta table) |
| `time_interval` | ✅ | Windowed `DELETE` + `INSERT` with `{{start_date}}`/`{{end_date}}` |
| `anti_join` | ✅ | Connector-specific: insert-only incremental via null-safe `LEFT ANTI JOIN` on the compound business key (`primary_key` columns); optionally window-bounded |
| `ddl` | ✅ | `CREATE TABLE IF NOT EXISTS` (no `PRIMARY KEY` constraint — Spark SQL doesn't have one) |
| `scd2_by_column` / `scd2_by_time` | ✅ | Incremental `MERGE` that closes changed/vanished rows and inserts new versions, plus full-refresh rebuild. The merge uses `WHEN NOT MATCHED BY SOURCE`, which requires **Fabric runtime 1.3+** (Delta 3.x) |

## Incremental patterns

Two incremental styles are supported, and they can be combined:

**1. Datetime-window incrementals (`time_interval`)** — classic
window-replace: delete the run's window from the target, insert the fresh
rows. Bruin renders `{{start_date}}`/`{{end_date}}` (or the `_timestamp`
variants) from the run's `--start-date`/`--end-date`:

```yaml
materialization:
  type: table
  strategy: time_interval
  incremental_key: event_date
  time_granularity: date
```

**2. Business-key incrementals (`anti_join`)** — insert-only: append the
rows from the query whose **compound business key** does not already exist
in the target. The key is whatever columns are marked `primary_key`, joined
null-safely (`<=>`), so multi-column keys and NULL-bearing keys both dedupe
correctly. Unlike `merge`, existing rows are never touched — a pure,
idempotent, re-runnable append:

```yaml
materialization:
  type: table
  strategy: anti_join
columns:
  - name: source_system
    primary_key: true
  - name: order_number
    primary_key: true
```

renders (statements per run):

```sql
CREATE OR REPLACE TEMPORARY VIEW __bruin_tmp_x AS <your query>;
INSERT INTO reporting.orders
SELECT src.* FROM __bruin_tmp_x src
LEFT ANTI JOIN (SELECT source_system, order_number FROM reporting.orders) tgt
  ON src.source_system <=> tgt.source_system AND src.order_number <=> tgt.order_number;
DROP VIEW IF EXISTS __bruin_tmp_x;
```

**Combined** — on large targets, a full-table anti join gets expensive. Add
`incremental_key` + `time_granularity` to bound the *target side* of the
anti join to the run's datetime window:

```yaml
materialization:
  type: table
  strategy: anti_join
  incremental_key: order_ts
  time_granularity: timestamp
```

The target scan becomes `WHERE order_ts BETWEEN '{{start_timestamp}}' AND
'{{end_timestamp}}'`, so the join only shuffles the window's keys. Trade-off:
deduplication is only guaranteed against rows whose `incremental_key` falls
inside the window — use the unbounded form when late-arriving duplicates
across windows are possible. `--full-refresh` rebuilds the table via
`create+replace` as usual. See
[`examples/fabric_lakehouse/assets/orders_landing.sql`](examples/fabric_lakehouse/assets/orders_landing.sql).

## Design notes (vs. dbt-fabricspark)

> For the full architecture — layer diagram, request sequence, session state
> machine, and the reasoning behind each design decision — see
> [`docs/DESIGN.md`](docs/DESIGN.md).

This connector deliberately borrows the battle-tested parts of Microsoft's
dbt adapter:

- Same Livy endpoint and API version (`livyapi/versions/2023-12-01`).
- Same Azure AD scope, service-principal flow, and token caching.
- Block comments are stripped from SQL before submission — the Fabric Livy
  SQL path interpolates statements into a server-side code block where
  `/* */` breaks parsing.
- Statement results arrive as one JSON payload
  (`output.data["application/json"]` with `schema.fields` + `data`); there is
  no server-side cursor, so results are fully materialized per statement.
- Transient 404s right after session/statement creation, HTTP 429
  throttling, and expired-session 404s are all retried/recovered the same
  way dbt handles them.

Where it differs: the default mode runs one Livy session per connection with
statements serialized — matching how bruin's other warehouse connections
behave — with dbt-style multi-REPL high concurrency available as an opt-in
(`high_concurrency: true`). PySpark support is a first-class asset type
rather than dbt's Python-model compilation, and seeds load natively through
Spark SQL instead of shelling out to ingestr.

## Seeds, sensors and high concurrency

**Seeds** load a CSV next to the asset file into a Delta table
(`CREATE OR REPLACE` + batched `INSERT`s, types from the column definitions):

```yaml
name: raw.countries
type: fabric.spark.seed
parameters:
  path: countries.csv
columns:
  - name: code
    type: string
```

**Sensors** block a pipeline until an upstream condition holds:

```yaml
name: wait_for_events
type: fabric.spark.sensor.table   # or fabric.spark.sensor.query
parameters:
  table: reporting.events          # query: SELECT COUNT(*) > 0 FROM ...
```

The table sensor probes with `SHOW TABLES IN <schema> LIKE '<table>'` rather
than an `information_schema` lookup, which Fabric Spark lakehouses don't
expose.

**High concurrency**: by default one Livy session serves the connection and
statements serialize. With `high_concurrency: true` the connector acquires up
to `max_concurrent_repls` REPLs (via Fabric's `/highConcurrencySessions`
API), all packed onto one Spark application by a shared `session_tag` —
parallel assets then genuinely execute concurrently. REPLs that die are
transparently replaced; a Spark-level statement failure keeps its REPL warm.

## Limitations / roadmap

- Statement results arrive as a single JSON payload (no server-side cursor),
  so very large `SELECT`s materialize fully in memory — fine for checks and
  metadata, not for bulk extraction (use ingestr for that).
- `Close()` isn't called automatically until the bruin connection manager
  wires it up — idle sessions are reclaimed by Fabric's idle timeout instead.
- The example pipeline only runs after the connector is wired into a bruin
  build — see [`docs/INTEGRATION.md`](docs/INTEGRATION.md).

## Development

```bash
go build ./...   # compile against real bruin interfaces
go vet ./...
go test ./...    # unit tests incl. a mock Fabric Livy server
```

The tests spin up an in-process fake of the Fabric Livy API and exercise the
full session/statement lifecycle — no Fabric capacity required.
