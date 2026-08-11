from delta.tables import DeltaTable


def write_delta_fact(spark, df, tgt_dir, key_column="event_id"):

    path = f"{tgt_dir.rstrip('/')}/fact_events"

    df = df.dropDuplicates([key_column])  # guard against dupes within the same batch

    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy("year", "month", "day").save(path)
        return

    delta_table = DeltaTable.forPath(spark, path)

    (
        delta_table.alias("t")
        .merge(
            df.alias("s"),
            f"t.{key_column} = s.{key_column} AND t.year = s.year AND t.month = s.month AND t.day = s.day"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def merge_delta_dim(spark, df, tgt_dir, table_name, key_column):

    path = f"{tgt_dir.rstrip('/')}/{table_name}"

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

