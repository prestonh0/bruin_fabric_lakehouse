/* @bruin
name: foo
type: fabric.spark.sql

materialization:
  type: table
  strategy: anti_join

columns:
  - name: id
    type: bigint
    primary_key: true
  - name: value
    type: string
@bruin */

-- Each run produces a random ~20-row batch of candidate rows with ids in
-- 1..100. The anti_join strategy inserts only the ids not already in `foo`,
-- so repeated runs grow the table toward 100 rows without ever duplicating
-- an id. Replace this SELECT with your real source (a staging table, a view,
-- a files/ query) — the incremental pattern stays the same.
SELECT
    id,
    CONCAT('value-', CAST(id AS STRING)) AS value
FROM range(1, 101)
WHERE rand() < 0.2
