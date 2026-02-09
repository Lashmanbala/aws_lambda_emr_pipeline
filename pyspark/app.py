import os
from datetime import datetime
from read import read_landing
from util import get_spark_session
from bookmark import get_pattern, upload_bookmark
from write import write_delta_fact, write_delta_dim
from validate_schema import validate_schema_columns
from model import build_fact_events, build_dim_actor, build_dim_org, build_dim_repo, build_dim_event_type

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

    while True:   # for the first initiation if the folder hase more than 1 day's file it will read all of them in the first loop itself. The next day will have only one days data.
        pattern = get_pattern(bucket_name,file_prefix,bookmark_file,baseline_file)

        if datetime.strptime(pattern, '%Y-%m-%d').date() == datetime.today().date():
            print('its today. Let all the files get downloaded')
            break

        src_file_pattern = f'{pattern}-*'

        df = read_landing(spark,src_file_format,src_dir,src_file_pattern)
        
        df.persist() # we are writing multiple times. so each write'll trigger the same read if we don't persist. 
                     # Persisting will cache the dataframe in memory and speed up subsequent writes.
        
        validate_schema_columns(df)

        fact_df = build_fact_events(df)
        dim_actor_df = build_dim_actor(df)
        dim_org_df = build_dim_org(df)
        dim_repo_df = build_dim_repo(df)
        dim_event_type_df = build_dim_event_type(df)

        gold_dir = "s3://github-activity-bucket-123/raw/"

        write_delta_fact(fact_df, gold_dir, coalesce_n=16)
        write_delta_dim(dim_actor_df, gold_dir, "dim_actor")
        write_delta_dim(dim_org_df, gold_dir, "dim_org")
        write_delta_dim(dim_repo_df, gold_dir, "dim_repo")
        write_delta_dim(dim_event_type_df, gold_dir, "dim_event_type")

        df.unpersist() # unpersisting after all writes

        upload_bookmark(bucket_name, file_prefix, bookmark_file, pattern)


if __name__ == '__main__':
    main()
