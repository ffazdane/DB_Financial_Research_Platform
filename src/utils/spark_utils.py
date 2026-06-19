"""spark_utils.py
Phase 2 — Spark Utility Functions
"""
from __future__ import annotations
from typing import List
from pyspark.sql import DataFrame, SparkSession


def get_spark() -> SparkSession:
    """Get or create the active SparkSession (works in Databricks notebooks and local tests)."""
    return SparkSession.builder.getOrCreate()


def read_delta(table_name: str) -> DataFrame:
    """Read a Unity Catalog Delta table into a Spark DataFrame.

    Args:
        table_name: Fully-qualified table name, e.g. 'fazdane_finance.bronze.stock_price_raw'
    """
    spark = get_spark()
    return spark.read.table(table_name)


def write_delta(df: DataFrame, table_name: str, mode: str = "append") -> None:
    """Write a Spark DataFrame to a Unity Catalog Delta table.

    Args:
        df:         Spark DataFrame to write.
        table_name: Fully-qualified table name.
        mode:       'append' | 'overwrite' | 'ignore' | 'error'
    """
    df.write.format("delta").mode(mode).saveAsTable(table_name)


def delta_merge(
    df: DataFrame,
    target_table: str,
    merge_keys: List[str],
) -> None:
    """Upsert a Spark DataFrame into a Delta table using MERGE.

    Rows matching on all merge_keys are updated; non-matching rows are inserted.

    Args:
        df:           Source Spark DataFrame (new/updated rows).
        target_table: Fully-qualified target Delta table name.
        merge_keys:   List of column names that form the unique key,
                      e.g. ['ticker', 'trade_date']
    """
    spark = get_spark()

    # Register the source as a temp view with a unique name
    tmp_view = f"_merge_src_{target_table.replace('.', '_')}"
    df.createOrReplaceTempView(tmp_view)

    # Build the ON clause
    on_clause = " AND ".join(
        f"target.{k} = source.{k}" for k in merge_keys
    )

    # Build SET clause (update all non-key columns)
    all_cols = df.columns
    set_clause = ", ".join(
        f"target.{c} = source.{c}" for c in all_cols if c not in merge_keys
    )

    # Build INSERT clause
    col_list = ", ".join(all_cols)
    val_list = ", ".join(f"source.{c}" for c in all_cols)

    sql = f"""
        MERGE INTO {target_table} AS target
        USING {tmp_view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
    """
    spark.sql(sql)
