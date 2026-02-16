import os
from datetime import datetime
from read import read_landing
from util import get_spark_session
from bookmark import get_pattern, upload_bookmark
from write import write_iceberg_fact, merge_iceberg_dim
from validate_schema import validate_schema_columns
from model import build_fact_events, build_dim_actor, build_dim_org, build_dim_repo, build_dim_event_type

def main():
    env = 'PROD'
    src_dir = 's3://github-activity-bucket-123/landing/'
    src_file_format = 'json'
    tgt_dir = 's3://github-activity-bucket-123/warehouse/'
    bucket_name = 'github-activity-bucket-123'
    file_prefix = 'warehouse'
    bookmark_file = 'bookmark'
    baseline_file = '2026-01-27-1.json.gz'
    db_name = 'github_db'

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

        write_iceberg_fact(spark, fact_df, db_name, 'fact_events', coalesce_n=16)

        merge_iceberg_dim(spark, dim_actor_df, db_name, "dim_actor", "actor_id")
        merge_iceberg_dim(spark, dim_repo_df, db_name, "dim_repo", "repo_id")
        merge_iceberg_dim(spark, dim_org_df, db_name, "dim_org", "org_id")
        merge_iceberg_dim(spark, dim_event_type_df, db_name, "dim_event_type", "event_type")

        df.unpersist() # unpersisting after all writes

        upload_bookmark(bucket_name, file_prefix, bookmark_file, pattern)


if __name__ == '__main__':
    main()
