def write_iceberg_fact(spark, df, db_name, table_name, coalesce_n):

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS glue_catalog.{db_name}
    """)

    full_table_name = f"glue_catalog.{db_name}.{table_name}"

    df = df.coalesce(coalesce_n)

    if not spark.catalog.tableExists(full_table_name):

        (
            df.writeTo(full_table_name)
              .tableProperty("format-version", "2")
              .partitionedBy("year", "month", "day")
              .create()
        )
    else:
        (
            df.writeTo(full_table_name)
              .append()
        )



def merge_iceberg_dim(spark, df, db_name, table_name, key_column):

    full_table_name = f"glue_catalog.{db_name}.{table_name}"

    # Ensure database exists
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS glue_catalog.{db_name}
    """)

    # First write if table does not exist
    if not spark.catalog.tableExists(full_table_name):
        (
            df.writeTo(full_table_name)
              .tableProperty("format-version", "2") 
              .create()
        )
        return

    # Otherwise perform MERGE
    df.createOrReplaceTempView("source_view")

    merge_sql = f"""
    MERGE INTO {full_table_name} t
    USING source_view s
    ON t.{key_column} = s.{key_column}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)
