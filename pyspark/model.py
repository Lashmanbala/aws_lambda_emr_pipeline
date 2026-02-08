from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp, when, lit
import logger

def build_fact_events(df):
    return df.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        to_timestamp(col("created_at")).alias("created_at"),
        col("public").alias("is_public"),
        col("actor.id").alias("actor_id"),
        col("org.id").alias("org_id"),
        col("repo.id").alias("repo_id"),
        col("payload.action").alias("payload_action"),
        col("payload.ref").alias("ref"),
        col("payload.ref_type").alias("ref_type"),
        col("payload.push_id").alias("push_id"),
        col("payload.pull_request.number").alias("pr_number"),
        col("payload.issue.number").alias("issue_number"),
        col("payload.release.tag_name").alias("release_tag_name"),
        col("payload.forkee.full_name").alias("forkee_full_name"),
    ).withColumn("year", year(col("created_at"))) \
    .withColumn("month", month(col("created_at"))) \
    .withColumn("day", dayofmonth(col("created_at")))

def write_delta_fact(df, gold_dir, coalesce_n):
    path = f"{gold_dir.rstrip('/')}/fact_events"
    (
        df.coalesce(coalesce_n)
        .write.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save(path)
    )
    logger.info("Written fact_events to %s", path)

def build_dim_org(df):
    return (
        df.filter(col("org.id").isNotNull())
        .select(
            col("org.id").alias("org_id"),
            col("org.login"),
            col("org.avatar_url"),
        )
        .distinct()
        .dropDuplicates(["org_id"])
    )