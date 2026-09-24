"""Stream appends from `staging.core_user` into `core.user`, one row per user.

- Creates the `core` namespace in the default catalog (spark.sql.defaultCatalog).
- Creates `core.user` with the same schema as the source if it doesn't exist.
- Streams every append snapshot on the source. `staging.core_user` is an
  append-only log with several versions per user_id; each micro-batch keeps the
  newest version per user_id and MERGEs it into `core.user`, so the target holds
  at most one row per user_id: the one with the latest `updated_at`.
- On first start it processes the whole history, then picks up new appends as
  they land.

    just submit stream_table.py                  # run continuously
    just submit stream_table.py --available-now  # process what's there, then exit
"""

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

WAREHOUSE_ROOT = "s3://warehouse"
SOURCE_TABLE = "staging.core_user"
TARGET_NAMESPACE = "core"
TARGET_TABLE = f"{TARGET_NAMESPACE}.user"
CHECKPOINT = "s3a://warehouse/_checkpoints/staging_core_user_to_core_user"


def create_core_namespace(spark: SparkSession, catalog: str) -> None:
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{TARGET_NAMESPACE} "
        f"LOCATION '{WAREHOUSE_ROOT}/{TARGET_NAMESPACE}'"
    )


def create_target_table(spark: SparkSession, source: str, target: str) -> None:
    # Empty CTAS copies the source schema without copying any rows.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} USING iceberg "
        f"LOCATION '{WAREHOUSE_ROOT}/{TARGET_NAMESPACE}/user' "
        f"AS SELECT * FROM {source} LIMIT 0"
    )


def latest_per_user(df: DataFrame) -> DataFrame:
    """Keep only the newest version (by updated_at) of each user_id."""
    newest_first = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(newest_first))
        .where("_rn = 1")
        .drop("_rn")
    )


def merge_batch(batch: DataFrame, target: str) -> None:
    # Only replace a row with a strictly newer version. That also makes a
    # replayed batch (after a failure/restart) a no-op instead of a regression.
    latest_per_user(batch).createOrReplaceTempView("core_user_updates")
    batch.sparkSession.sql(f"""
        MERGE INTO {target} t
        USING core_user_updates s
        ON t.user_id = s.user_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def stream_appends(spark: SparkSession, source: str, target: str, available_now: bool):
    df = (
        spark.readStream.format("iceberg")
        # Only appends are streamed; don't fail on compaction/overwrite/delete commits.
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load(source)
    )
    writer = (
        df.writeStream.foreachBatch(
            lambda batch, _batch_id: merge_batch(batch, target)
        )
        .option("checkpointLocation", CHECKPOINT)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.start()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process all pending appends, then stop",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("stream-core-user").getOrCreate()
    catalog = (
        spark.conf.get("spark.sql.defaultCatalog") or spark.catalog.currentCatalog()
    )
    source = f"{catalog}.{SOURCE_TABLE}"
    target = f"{catalog}.{TARGET_TABLE}"

    create_core_namespace(spark, catalog)
    create_target_table(spark, source, target)

    query = stream_appends(spark, source, target, args.available_now)
    print(f"Streaming {source} -> {target} (checkpoint {CHECKPOINT})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
