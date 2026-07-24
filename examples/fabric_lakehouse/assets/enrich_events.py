""" @bruin
name: transform.enrich_events
type: fabric.spark.pyspark
connection: fabric-spark-default

depends:
  - raw.events
@bruin """

# This code runs as a PySpark statement inside the lakehouse Spark session.
# `spark` (SparkSession) and `sc` (SparkContext) are predefined by Livy, and
# the lakehouse is the default catalog, so Spark SQL sees the same tables as
# the SQL assets in this pipeline.

from pyspark.sql import functions as F

events = spark.table("raw.events")

enriched = events.withColumn(
    "is_conversion", F.col("event_type") == F.lit("purchase")
).withColumn("processed_at", F.current_timestamp())

enriched.write.mode("overwrite").saveAsTable("transform.enriched_events")

print(f"enriched {enriched.count()} events")
