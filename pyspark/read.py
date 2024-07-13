def from_files(spark, file_format, data_dir, file_pattern):
    df = spark.read. \
            format(file_format). \
            load(f'{data_dir}/{file_pattern}')
    return df
