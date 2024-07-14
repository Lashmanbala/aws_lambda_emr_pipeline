from util import get_spark_session
import os

def validate():
    env = os.environ.get('ENVIRON')
    tgt_dir = os.environ.get('TGT_DIR')

    spark = get_spark_session(env, 'Validatation')
    
    tgt_df = spark.read.parquet(tgt_dir)
    print(tgt_df.count())
    tgt_df.printSchema()

if __name__ == '__main__':
    validate()
