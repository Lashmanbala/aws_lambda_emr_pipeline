from pyspark.sql import SparkSession

spark = SparkSession. \
            builder. \
            master('local'). \
            appName('Github'). \
            getOrCreate()

spark.sql('SELECT current_date').show()