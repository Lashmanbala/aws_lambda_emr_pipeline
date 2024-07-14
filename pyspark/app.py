import os
from util import get_spark_session
from read import from_files
from process import transform


def main():
    env = os.environ.get('ENVIRON')
    
    src_dir = os.environ.get('SRC_DIR')
    src_file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*" # 24 files per day
    src_file_format = os.environ.get('SRC_FILE_FORMAT')

    spark = get_spark_session(env, 'Github')

    df = from_files(spark,src_file_format,src_dir,src_file_pattern)

    df_transformed = transform(df)
    df_transformed.printSchema()
    df_transformed.select('repo.*','created_at','year','month','day').show()

if __name__ == '__main__':
    main()
