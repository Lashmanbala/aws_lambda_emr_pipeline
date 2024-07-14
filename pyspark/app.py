import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_files

def main():
    env = os.environ.get('ENVIRON')
    
    src_dir = os.environ.get('SRC_DIR')
    src_file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*" # 24 files per day
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    
    spark = get_spark_session(env, 'Github')

    df = from_files(spark,src_file_format,src_dir,src_file_pattern)

    df_transformed = transform(df)
    
    to_files(df_transformed,tgt_dir,tgt_file_format)

if __name__ == '__main__':
    main()
