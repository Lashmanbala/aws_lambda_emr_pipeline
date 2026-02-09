def read_landing(spark, file_format, data_dir, file_pattern):
    df = spark.read. \
            format(file_format). \
            load(f'{data_dir}/{file_pattern}')
    return df
# For json date spark automatically identifies schema