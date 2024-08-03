import os
from util import get_spark_session
from read import from_files
from process import transform
from write import to_files
from bookmark import get_prev_day, get_next_day, upload_bookmark
from datetime import datetime

def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    bucket_name = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')
    bookmark_file = os.environ.get('BOOKMARK_FILE')
    baseline_file = os.environ.get('BASELINE_FILE')


    spark = get_spark_session(env, 'Github')

    while True:
        prev_day = get_prev_day(bucket_name,file_prefix,bookmark_file,baseline_file)
        nxt_day = get_next_day(prev_day)

        if datetime.strptime(nxt_day, '%Y-%m-%d').date() == datetime.today().date():
            print('its today')
            break

        src_file_pattern = f'{nxt_day}-*'

        df = from_files(spark,src_file_format,src_dir,src_file_pattern)

        df_transformed = transform(df)
        
        to_files(df_transformed,tgt_dir,tgt_file_format)

        upload_bookmark(bucket_name,file_prefix,bookmark_file,nxt_day)

if __name__ == '__main__':
    main()
