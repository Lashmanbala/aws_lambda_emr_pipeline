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

def write_delta_dim(df, gold_dir, table_name):
    path = f"{gold_dir.rstrip('/')}/{table_name}"
    df.coalesce(1)\
        .write.format("parquet")\
        .mode("overwrite")\
        .save(path)
    logger.info("Written %s to %s", table_name, path)


def build_dim_actor(df):
    return (
        df.select(
            col("actor.id").alias("actor_id"),
            col("actor.login"),
            col("actor.display_login"),
            col("actor.avatar_url"),
        )
        .filter(col("actor_id").isNotNull())
        .distinct()
        .dropDuplicates(["actor_id"])
    )

def build_dim_repo(df):
    return (
        df.select(
            col("repo.id").alias("repo_id"),
            col("repo.name"),
            col("repo.url"),
        )
        .filter(col("repo_id").isNotNull())
        .distinct()
        .dropDuplicates(["repo_id"])
    )

EVENT_TYPE_CATEGORY = {
    "PushEvent": "push",
    "PullRequestEvent": "pr",
    "PullRequestReviewEvent": "pr",
    "PullRequestReviewCommentEvent": "pr",
    "IssuesEvent": "issue",
    "IssueCommentEvent": "issue",
    "ReleaseEvent": "release",
    "ForkEvent": "fork",
    "CreateEvent": "create",
    "DeleteEvent": "delete",
    "WatchEvent": "watch",
    "MemberEvent": "member",
    "PublicEvent": "public",
}

def build_dim_event_type(df):
    distinct_types = df.select(col("type").alias("event_type")).filter(col("event_type").isNotNull()).distinct()
    category_expr = lit("other")
    for event_type, category in reversed(list(EVENT_TYPE_CATEGORY.items())):
        category_expr = when(
            col("event_type") == event_type, lit(category)
        ).otherwise(category_expr)
    return distinct_types.withColumn("category", category_expr)