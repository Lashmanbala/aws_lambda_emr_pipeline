from delta.tables import DeltaTable

def write_delta_fact(df, gold_dir, coalesce_n):
    path = f"{gold_dir.rstrip('/')}/fact_events"
    (
    df.coalesce(coalesce_n) # coalesce number = n.of executor cores
    .write.format("delta")
    .partitionBy("year", "month", "day")
    .mode("append")
    .save(path)
    )


def merge_delta_dim(spark, df, gold_dir, table_name, key_column):

    path = f"{gold_dir.rstrip('/')}/{table_name}"

    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    delta_table = DeltaTable.forPath(spark, path)

    (
        delta_table.alias("t")
        .merge(
            df.alias("s"),
            f"t.{key_column} = s.{key_column}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
