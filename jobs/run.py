"""
Entry point for Glue ETL job.
Calls main() from main.py.
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from config import load_config
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
]
args = getResolvedOptions(sys.argv, params)
config = load_config(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

main(spark=spark, config=config)
