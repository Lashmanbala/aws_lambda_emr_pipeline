def to_files(df,tgt_dir,file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year','month','day'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)

def write_delta_fact(df, gold_dir, coalesce_n):
    path = f"{gold_dir.rstrip('/')}/fact_events"
    (
        df.coalesce(coalesce_n)
        .write.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save(path)
    )

def write_delta_dim(df, gold_dir, table_name):
    path = f"{gold_dir.rstrip('/')}/{table_name}"
    df.coalesce(1)\
        .write.format("parquet")\
        .mode("overwrite")\
        .save(path)
