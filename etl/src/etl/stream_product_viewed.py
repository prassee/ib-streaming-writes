"""Stream `Product Viewed` events from `staging.user_activity` into `core.product_viewed`.

- Creates the `core` namespace and `core.product_viewed` (partitioned by event
  day) in the default catalog (spark.sql.defaultCatalog) if they don't exist.
- Streams every append snapshot on the source, keeps rows where
  t = 'Product Viewed' and appends them with the columns that apply to product
  views. Events are immutable, so this is a plain append; Iceberg's streaming
  sink commits each micro-batch once, so a replayed batch after a restart isn't
  written twice.
- On first start it processes the whole history, then picks up new appends as
  they land.

    just submit stream_product_viewed.py                  # run continuously
    just submit stream_product_viewed.py --available-now  # process what's there, then exit
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

WAREHOUSE_ROOT = "s3://warehouse"
SOURCE_TABLE = "staging.user_activity"
EVENT_TYPE = "Product Viewed"
TARGET_NAMESPACE = "core"
TARGET_NAME = "product_viewed"
TARGET_TABLE = f"{TARGET_NAMESPACE}.{TARGET_NAME}"
CHECKPOINT = f"s3a://warehouse/_checkpoints/user_activity_to_{TARGET_NAME}"

COLUMNS = [
    "insert_id",
    "distinct_id",
    "user_id",
    "device_id",
    "session_id",
    "event_time",
    "ingested_at",
    "product_id",
    "product_name",
    "category",
    "price",
    "screen_name",
    "mp_lib",
    "os",
    "os_version",
    "manufacturer",
    "model",
    "app_version",
    "app_build_number",
    "carrier",
    "wifi",
    "city",
    "region",
    "country_code",
]


def product_views(events: DataFrame) -> DataFrame:
    return events.where(F.col("t") == EVENT_TYPE).select(*COLUMNS)


def create_target_table(spark: SparkSession, catalog: str, source: str, target: str) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{TARGET_NAMESPACE} "
        f"LOCATION '{WAREHOUSE_ROOT}/{TARGET_NAMESPACE}'"
    )
    # Empty CTAS copies the column types without copying any rows.
    product_views(spark.table(source).limit(0)).createOrReplaceTempView("product_viewed_schema")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} USING iceberg "
        f"PARTITIONED BY (days(event_time)) "
        f"LOCATION '{WAREHOUSE_ROOT}/{TARGET_NAMESPACE}/{TARGET_NAME}' "
        f"AS SELECT * FROM product_viewed_schema"
    )


def stream_events(spark: SparkSession, source: str, target: str, available_now: bool):
    events = (
        spark.readStream.format("iceberg")
        # Only appends are streamed; don't fail on compaction/overwrite/delete commits.
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load(source)
    )
    writer = (
        product_views(events)
        .writeStream.format("iceberg")
        .outputMode("append")
        # A micro-batch can span several days; write each day's rows to its partition.
        .option("fanout-enabled", "true")
        .option("checkpointLocation", CHECKPOINT)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.toTable(target)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process all pending appends, then stop",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("stream-product-viewed").getOrCreate()
    catalog = (
        spark.conf.get("spark.sql.defaultCatalog") or spark.catalog.currentCatalog()
    )
    source = f"{catalog}.{SOURCE_TABLE}"
    target = f"{catalog}.{TARGET_TABLE}"

    create_target_table(spark, catalog, source, target)

    query = stream_events(spark, source, target, args.available_now)
    print(f"Streaming {source} [t = '{EVENT_TYPE}'] -> {target} (checkpoint {CHECKPOINT})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
