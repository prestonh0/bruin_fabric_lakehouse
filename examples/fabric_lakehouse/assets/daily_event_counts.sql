/* @bruin
name: reporting.daily_event_counts
type: fabric.spark.sql

depends:
  - raw.events

materialization:
  type: table
  strategy: merge

columns:
  - name: event_date
    type: date
    primary_key: true
  - name: event_type
    type: string
    primary_key: true
  - name: event_count
    type: bigint
    update_on_merge: true
    checks:
      - name: non_negative

@bruin */

SELECT
    event_date,
    event_type,
    COUNT(*) AS event_count
FROM raw.events
GROUP BY event_date, event_type
