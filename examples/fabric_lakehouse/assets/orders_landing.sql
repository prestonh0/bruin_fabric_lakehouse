/* @bruin
name: reporting.orders_landing
type: fabric.spark.sql

depends:
  - raw.events

# Insert-only incremental: only rows whose compound business key
# (source_system, order_number) is not already in the target get inserted.
# The anti join is null-safe (<=>), so NULLs in key columns still dedupe.
#
# incremental_key + time_granularity bound the target-side scan to the
# pipeline run window ({{start_timestamp}}..{{end_timestamp}}), keeping the
# join cheap on large tables. Omit them both to anti-join against the full
# target table (slower, but deduplicates across all of history).
materialization:
  type: table
  strategy: anti_join
  incremental_key: order_ts
  time_granularity: timestamp

columns:
  - name: source_system
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: order_number
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: order_ts
    type: timestamp
  - name: amount
    type: double

@bruin */

SELECT
    'webshop' AS source_system,
    CAST(event_id AS STRING) AS order_number,
    CAST(event_date AS TIMESTAMP) AS order_ts,
    rand() * 100 AS amount
FROM raw.events
WHERE event_type = 'purchase'
