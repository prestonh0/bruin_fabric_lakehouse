/* @bruin
name: raw.events
type: fabric.spark.sql
connection: fabric-spark-default

materialization:
  type: table
  strategy: create+replace
  partition_by: event_date

columns:
  - name: event_id
    type: string
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: event_type
    type: string
    checks:
      - name: accepted_values
        value: [click, view, purchase]
  - name: event_date
    type: date

@bruin */

SELECT
    uuid() AS event_id,
    element_at(array('click', 'view', 'purchase'), CAST(rand() * 3 AS INT) + 1) AS event_type,
    current_date() AS event_date
FROM range(100)
