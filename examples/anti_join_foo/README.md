# Minimal example: LEFT ANTI JOIN incremental load

The smallest possible bruin + Fabric Lakehouse Spark project: **one asset**
([`assets/foo.sql`](assets/foo.sql)) that incrementally loads a table `foo`
with two columns, `id` and `value`, using the connector's `anti_join`
strategy — new ids are inserted, existing ids are never touched or
duplicated.

## What one run executes

The asset's `SELECT` produces a batch of candidate rows. The `anti_join`
materialization (keyed on the `primary_key` column `id`) turns it into:

```sql
CREATE OR REPLACE TEMPORARY VIEW __bruin_tmp_x AS
  SELECT id, CONCAT('value-', CAST(id AS STRING)) AS value
  FROM range(1, 101) WHERE rand() < 0.2;

-- first run only: bootstrap an empty `foo` from the query's schema
CREATE TABLE IF NOT EXISTS foo AS SELECT * FROM __bruin_tmp_x WHERE 1 = 0;

INSERT INTO foo
SELECT src.* FROM __bruin_tmp_x src
LEFT ANTI JOIN (SELECT id FROM foo) tgt ON src.id <=> tgt.id;

DROP VIEW IF EXISTS __bruin_tmp_x;
```

Each statement runs as a Spark SQL statement in the lakehouse's Livy
session. The join is null-safe (`<=>`) and works the same with a compound
key — mark more columns `primary_key: true` and they all become part of the
join condition.

## Run it

```bash
cp .bruin.yml.example .bruin.yml   # fill in your workspace/lakehouse/credentials
bruin run .
```

- **Run 1** — `foo` is created and receives ~20 rows (a random subset of ids
  1..100).
- **Run 2, 3, …** — each run generates another random batch; only ids not
  already in `foo` are inserted. Row count grows toward 100, and
  `SELECT id, COUNT(*) FROM foo GROUP BY id HAVING COUNT(*) > 1` stays empty
  forever — the load is idempotent by key.
- `bruin run --full-refresh .` drops the accumulated state and rebuilds
  `foo` from just that run's batch.

In a real pipeline the `SELECT` would read from a staging table or files
instead of `range()` — the pattern is unchanged.

> Requires a bruin build with this connector wired in (see
> [`docs/INTEGRATION.md`](../../docs/INTEGRATION.md)). `bruin validate` will
> flag the `anti_join` strategy name until it's added to bruin's lint
> allowlist; execution is unaffected.
