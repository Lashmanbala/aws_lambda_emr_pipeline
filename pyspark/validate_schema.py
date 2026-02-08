# Expected root-level columns per schema.json (used for optional validation)
EXPECTED_ROOT_COLUMNS = ("id", "type", "created_at", "actor", "org", "repo", "payload", "public")


def validate_schema_columns(df):
    """Log a warning if any expected root-level columns from schema.json are missing."""
    present = set(df.columns)
    missing = [c for c in EXPECTED_ROOT_COLUMNS if c not in present]
    if missing:
        print(
            "Schema drift: expected columns missing from landing data: %s",
            missing,
        )
    else:
        print("All columns present")

# tgt_df = spark.read.parquet("s3://github-activity-bucket-123/raw/y*")
# df = spark.read.json("s3://github-activity-bucket-123/landing/")

# "s3://github-activity-bucket-123/raw/fact_events"