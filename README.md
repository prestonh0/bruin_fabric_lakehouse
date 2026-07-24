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
  `merge`, `delete+insert`, `truncate+insert`, `time_interval`, and `ddl`.
- **`fabric.spark.pyspark` assets** — the asset's Python file runs as a
  PySpark statement in the *same* Spark session, with `spark` and `sc`
  predefined and the lakehouse as the default catalog.
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
| `lakehouse_id` | yes | Lakehouse item GUID |
| `lakehouse_name` | yes | Doubles as the database name for non-schema lakehouses |
| `schema` | no | Default schema for schema-enabled lakehouses (e.g. `dbo`) |
| `endpoint` | no | Defaults to `https://api.fabric.microsoft.com/v1` |
| `tenant_id` / `client_id` / `client_secret` | one of | Service-principal (client credentials) auth |
| `access_token` | one of | Static bearer token; takes precedence |
| `session_name` | no | Session name in the Fabric monitoring UI (default `bruin-fabric-spark`) |
| `environment_id` | no | Pin the session to a Fabric Environment (custom pool/libraries) |
| `spark_config` | no | Extra spark conf, e.g. `spark.sql.caseSensitive: "true"` |
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
| `ddl` | ✅ | `CREATE TABLE IF NOT EXISTS` (no `PRIMARY KEY` constraint — Spark SQL doesn't have one) |
| `scd2_by_column` / `scd2_by_time` | ⚠️ | Full-refresh rebuild only (`--full-refresh`); incremental SCD2 not implemented yet |

## Design notes (vs. dbt-fabricspark)

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

Where it differs: bruin connectors are Go and synchronous per-connection, so
instead of dbt's high-concurrency multi-REPL sessions this v1 runs one Livy
session per connection with statements serialized — matching how bruin's
other warehouse connections behave. PySpark support is a first-class asset
type rather than dbt's Python-model compilation.

## Limitations / roadmap

- Incremental SCD2 strategies are not implemented (full-refresh works).
- No seed (`fabric.spark.seed`) or sensor asset types yet.
- No ingestr integration (`GetIngestrURI` returns empty).
- One session per connection; parallel assets queue on the session. A
  high-concurrency session pool (Fabric `/highConcurrencySessions`) is the
  natural next step.
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
