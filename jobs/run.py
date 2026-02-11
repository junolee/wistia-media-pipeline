"""
Entry point for Glue ETL job.
Calls main() from main.py.
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from config import exception, info, load_config
from main import main
from pyspark.context import SparkContext

params = [
  "JOB_NAME",
  "SOURCE_DB",
  "SOURCE_PATH",
  "WAREHOUSE_DIR",
  "TARGET_DB",
  "PIPELINE_MODE",
  "START_DATE",
  "DRY_RUN",
]
args = getResolvedOptions(sys.argv, params)
config = load_config(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


sc.setLogLevel("WARN")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


try:
  main(spark=spark, config=config)
  info("Glue job completed successfully for job: %s", config.job_name)
except Exception:
  exception("Glue job failed due to unhandled exception")
  raise
