import os

import schemas as s
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_S3 = os.environ["WAREHOUSE_DIR_S3"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
RAW_PREFIX = os.environ["RAW_PREFIX"]
SOURCE_PATH = f"s3://{BUCKET_NAME}/{RAW_PREFIX}"


def create_external_json_table_raw(db_name, table_name, columns_sql, partition_by):
  partition_clause = "" if not partition_by else f"PARTITIONED BY ({partition_by} date)"
  ddl = f"""
CREATE EXTERNAL TABLE {db_name}.{table_name} ({columns_sql}
)
{partition_clause}
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{SOURCE_PATH}/{table_name}/'
;"""
  print(f"\nDROP TABLE {db_name}.{table_name};")
  print(ddl)


def create_external_parquet_table(db_name, table_name, columns_sql, partition_by):
  partition_clause = "" if not partition_by else f"PARTITIONED BY ({partition_by} date)"
  ddl = f"""
CREATE EXTERNAL TABLE {db_name}.{table_name} ({columns_sql}
)
{partition_clause}
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '{WAREHOUSE_S3}/{db_name}.db/{table_name}/'
;"""
  print(f"\nDROP TABLE {db_name}.{table_name};")
  print(ddl)
  return ddl


RAW_PARTITION_COL = "ingest_date"
RAW_TABLES = {"events": s.EVENTS_RAW_COLUMNS, "media": s.MEDIA_RAW_COLUMNS}


TARGET_TABLES_WITHOUT_PARTITIONS = {
  "dim_visitors": s.DIM_VISITORS_COLUMNS,
  "dim_dates": s.DIM_DATES_COLUMNS,
  "dim_media": s.DIM_MEDIA_COLUMNS,
  "fct_events": s.FCT_EVENTS_COLUMNS,
  "fct_media_engagement": s.FCT_MEDIA_ENGAGEMENT_COLUMNS,
}


def print_source_ddls(database_name):
  print(f"\n\n-- SOURCE TABLES FOR DATABASE: {database_name} --")
  for table, columns in RAW_TABLES.items():
    create_external_json_table_raw(
      database_name, table, columns, partition_by=RAW_PARTITION_COL
    )


def print_target_ddls(database_name):
  print(f"\n\n-- TARGET TABLES FOR DATABASE: {database_name} --")

  for table, columns in TARGET_TABLES_WITHOUT_PARTITIONS.items():
    create_external_parquet_table(database_name, table, columns, partition_by=None)


print_source_ddls(os.environ["SOURCE_DB"])
print_target_ddls(os.environ["TARGET_DB"])
