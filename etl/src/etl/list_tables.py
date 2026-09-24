"""List every table in the current Spark catalog (Lakekeeper by default).

just submit list_tables.py
"""

from pyspark.sql import SparkSession


def list_tables(spark: SparkSession, catalog: str | None = None) -> list[str]:
    catalog = catalog or spark.catalog.currentCatalog()
    tables = []
    for ns_row in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect():
        namespace = ns_row.namespace
        for t in spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect():
            tables.append(f"{catalog}.{namespace}.{t.tableName}")
    return tables


def show_first_100_rows(
    spark: SparkSession, table_name: str = "iceberg.staging.core_user"
) -> None:
    """Show the first 100 rows of a table."""
    df = spark.sql(f"SELECT * FROM {table_name} LIMIT 100")
    df.show(truncate=False)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("list-tables").getOrCreate()
    # for name in list_tables(spark):
    #     print(name)
    show_first_100_rows(spark)
    spark.stop()
