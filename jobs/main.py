"""
ETL Job - Raw Data to Curated Tables in Glue Catalog

Called by: run.py
"""

from datetime import datetime

import pyspark.sql.functions as F
from config import *
from pyspark.sql import Window


def dedupe(inputDF, key, order_col="updated_at"):
  """
  Deduplicate to latest per key based on order_col
  - key: string or list of string column names
  """
  return (
    inputDF.withColumn(
      "row_num",
      F.row_number().over(Window.partitionBy(key).orderBy(F.col(order_col).desc())),
    )
    .filter("row_num = 1")
    .drop("row_num")
  )


def load_raw(spark, path, date_col=None, start_date=None):
  """
  Read raw table + filter by start_date (used for incremental processing)
  """
  df = spark.read.json(path)
  if date_col and start_date:
    df = df.filter(F.col(date_col) >= start_date)
    since_start = f" new records since start_date: {start_date}"
  else:
    since_start = ""
  info(f"Reading from path: {path}\n{df.count()}{since_start}")
  return df


def build_visitors(raw_eventsDF):
  """
  Builds dim_visitors (PK: visitor_id)
  """
  RENAME_PAIRS = [
    ("visitor_key", "visitor_id"),
    ("received_at", "created_at"),
    ("ip", "ip_address"),
    ("country", "country"),
    ("region", "region"),
  ]
  TARGET_TYPES = {
    "visitor_id": "string",
    "created_at": "timestamp",
    "ip_address": "string",
    "country": "string",
    "region": "string",
  }
  exprs = []
  for old_name, new_name in RENAME_PAIRS:
    exprs.append(F.col(old_name).cast(TARGET_TYPES[new_name]).alias(new_name))

  visitors = (
    raw_eventsDF.select(*exprs)
    .filter(F.col("visitor_id").isNotNull())
    .groupBy("visitor_id")
    .agg(
      F.min("created_at").alias("created_at"),
      F.max("ip_address").alias("ip_address"),
      F.max("country").alias("country"),
      F.max("region").alias("region"),
    )
    .withColumn("updated_at", F.current_timestamp())
  )

  return visitors.select(
    [
      "visitor_id",
      "created_at",
      "ip_address",
      "country",
      "region",
      "updated_at",
    ]
  )


def build_media(raw_mediaDF, raw_eventsDF):
  """
  Builds dim_media (PK: media_id)
  """
  media_urls = raw_eventsDF.select("media_id", "media_url").distinct()

  RENAME_PAIRS = [
    ("hashed_id", "media_id"),
    ("created", "created_at"),
    ("duration", "duration"),
    ("name", "title"),
  ]
  TARGET_TYPES = {
    "media_id": "string",
    "created_at": "timestamp",
    "duration": "double",
    "title": "string",
  }
  exprs = []
  for old_name, new_name in RENAME_PAIRS:
    exprs.append(F.col(old_name).cast(TARGET_TYPES[new_name]).alias(new_name))

  mediaDF = (
    raw_mediaDF.select(*exprs)
    .filter(F.col("media_id").isNotNull())
    .groupBy("media_id")
    .agg(
      F.min("created_at").alias("created_at"),
      F.max("duration").alias("duration"),
      F.max("title").alias("title"),
    )
    .withColumn(
      "channel",
      F.when(F.lower(F.col("title")).contains("facebook"), "Facebook")
      .when(F.lower(F.col("title")).contains("youtube"), "YouTube")
      .otherwise("Other"),
    )
    .withColumn("updated_at", F.current_timestamp())
  ).join(media_urls, on="media_id", how="left")

  return mediaDF.select(
    [
      "media_id",
      "created_at",
      "duration",
      "title",
      "channel",
      "media_url",
      "updated_at",
    ]
  )


def build_events(raw_eventsDF, mediaDF):
  """
  Builds fct_events (PK: media_id, visitor_id, created_at)
  """
  RENAME_PAIRS = [
    ("received_at", "created_at"),
    ("visitor_key", "visitor_id"),
    ("media_id", "media_id"),
    ("percent_viewed", "percent_viewed"),
  ]
  TARGET_TYPES = {
    "created_at": "timestamp",
    "visitor_id": "string",
    "media_id": "string",
    "percent_viewed": "double",
  }
  exprs = []
  for old_name, new_name in RENAME_PAIRS:
    if new_name in ["created_at"]:
      exprs.append(F.to_date(F.col(old_name), "yyyy-MM-dd").alias(new_name))
    else:
      exprs.append(F.col(old_name).cast(TARGET_TYPES[new_name]).alias(new_name))

  eventsDF = (
    raw_eventsDF.select(*exprs)
    .withColumn("date", F.to_date(F.col("created_at")))
    .withColumn("p_date", F.date_trunc("year", F.col("created_at")).cast("date"))
    .withColumn("updated_at", F.current_timestamp())
  ).join(mediaDF.select("media_id", "duration"), on="media_id", how="left")

  eventsDF = eventsDF.withColumn(
    "duration_viewed", F.col("duration") * F.col("percent_viewed") / 100
  )

  return eventsDF.select(
    [
      "media_id",
      "visitor_id",
      "created_at",
      "date",
      "percent_viewed",
      "duration_viewed",
      "updated_at",
      "p_date",
    ]
  )


def build_media_engagement(eventsDF, mediaDF):
  """
  Build fct_media_engagement (PK: media_id, visitor_id, date)
  """
  dailyDF = (
    eventsDF.groupby("media_id", "visitor_id", "date")
    .agg(
      F.count("created_at").alias("play_count"),
      F.sum("duration_viewed").alias("total_watch_time"),
      F.avg("duration_viewed").alias("avg_watch_time_per_view"),
      F.max("percent_viewed").alias("max_percent_viewed"),
      F.avg("percent_viewed").alias("avg_percent_viewed"),
    )
    .withColumn("updated_at", F.current_timestamp())
  )
  return dailyDF.select(
    [
      "media_id",
      "visitor_id",
      "date",
      "play_count",
      "total_watch_time",
      "avg_watch_time_per_view",
      "max_percent_viewed",
      "avg_percent_viewed",
      "updated_at",
    ]
  )


def generate_dates(spark, start_date, end_date):
  """
  Generate dim_dates for a fixed date range + add common date attributes
  """
  days_diff = spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][
    0
  ]

  datesDF = spark.range(0, days_diff + 1).select(
    F.date_add(F.lit(start_date), F.col("id").cast("int")).alias("date")
  )
  dim_datesDF = (
    datesDF.withColumn("month_num", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week_name", F.date_format("date", "E"))
    .withColumn("day_of_week", F.weekday("date"))
    .withColumn("is_weekday", (F.col("day_of_week") <= 4).cast("boolean"))
    .withColumn("year", F.year("date"))
    .withColumn("month_name", F.date_format(F.col("date"), "MMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
  )
  return dim_datesDF


def build_dates(spark, raw_eventsDF):
  """
  Builds dim_dates (PK: date)
  """
  today = datetime.today().strftime("%Y-%m-%d")

  observed_datesDF = raw_eventsDF.select(
    F.to_date(F.col("received_at"), "dd-MM-yyyy").alias("date")
  )

  generated_datesDF = generate_dates(spark, "2024-01-29", today)

  datesDF = generated_datesDF.join(observed_datesDF, on="date", how="left").withColumn(
    "updated_at", F.current_timestamp()
  )

  return datesDF.select(
    [
      "date",
      "month_num",
      "day",
      "day_of_week_name",
      "day_of_week",
      "is_weekday",
      "year",
      "month_name",
      "week_of_year",
      "updated_at",
    ]
  )


def append_parquet(df, path):
  """
  Append data to existing Parquet files at path
  """
  info(f"Appending data to path: {path}")
  df.write.mode("append").parquet(path)


def main(spark, config):
  """
  Build curated tables from raw datasets.

  Requires config with the following attributes:
    source_path: source directory for raw data
    target_db: target database name
    target_dir: target directory for target_db
    pipeline_mode: "full" loads all data
                   "incremental" loads records since start_date
    start_date: lower bound for event timestamp filter (YYYY-MM-DD) in incremental mode
  """
  c = config

  info(f"Starting job - config: {c}")
  start_date = "1900-01-01" if c.pipeline_mode == "full" else c.start_date

  raw_mediaDF = load_raw(spark, f"{c.source_path}/media/")
  raw_eventsDF = load_raw(spark, f"{c.source_path}/events/")

  dim_datesDF = build_dates(spark, raw_eventsDF)
  dim_visitorsDF = build_visitors(raw_eventsDF)
  dim_mediaDF = build_media(raw_mediaDF, raw_eventsDF)
  fct_eventsDF = build_events(raw_eventsDF, dim_mediaDF)
  fct_media_engagementDF = build_media_engagement(fct_eventsDF, dim_mediaDF)

  SILVER_TABLES = {
    "dim_dates": dedupe(dim_datesDF, "date"),
    "dim_visitors": dedupe(dim_visitorsDF, "visitor_id"),
    "dim_media": dedupe(dim_mediaDF, "media_id"),
    "fct_events": dedupe(fct_eventsDF, ["media_id", "visitor_id", "created_at"]),
    "fct_media_engagement": dedupe(
      fct_media_engagementDF, ["media_id", "visitor_id", "date"]
    ),
  }

  counts = []
  for table, df in SILVER_TABLES.items():
    append_parquet(df, f"{c.target_dir}/{table}/")

    counts.append(f"{table}: {df.count()}")
    print(f"\n{table} columns: " + ", ".join(df.columns))

  print("\nTarget table counts:", counts)
  # table_counts(spark, schemas=[c.target_db])
  info(f"Completed job - config: {c}.")
