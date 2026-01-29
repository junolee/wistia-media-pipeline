import os

from config import *
from dotenv import load_dotenv
from main import *
from pyspark.sql import SparkSession


def get_spark():
  spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    .getOrCreate()
  )
  spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
  )
  return spark


def build_args():
  job_name = "job-tests"
  pipeline_mode = "incremental"
  start_date = "2023-01-01"
  target_db = os.environ["TARGET_TEST_DB"]
  return {
    "JOB_NAME": job_name,
    "SOURCE_DB": os.environ["SOURCE_DB"],
    "SOURCE_PATH": os.environ["SOURCE_PATH"],
    "WAREHOUSE_DIR": os.environ["WAREHOUSE_DIR"],
    "TARGET_DB": target_db,
    "PIPELINE_MODE": pipeline_mode,
    "START_DATE": start_date,
  }


def print_sql_columns(df, table):
  columns_sql = f'\n{table.upper()}_COLUMNS = """'
  for col, col_type in df.dtypes:
    columns_sql += f"\n{col}     {col_type},"
  columns_sql += '\n"""'
  print(columns_sql)


def main_tests(spark, config):
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
    # append_parquet(df, f"{c.target_dir}/{table}/")

    counts.append(f"{table}: {df.count()}")

    print(f"\n{table} columns: " + ", ".join(df.columns))

    # print_sql_columns(df, table)

  print("\nTarget table counts:", counts)

  info(f"Completed job - config: {c}.")


if __name__ == "__main__":
  load_dotenv()
  spark = get_spark()
  config = load_config(args=build_args())
  main_tests(spark, config)
