from dataclasses import dataclass


@dataclass
class JobConfig:
  job_name: str
  source_db: str
  source_path: str
  warehouse_dir: str
  target_db: str
  pipeline_mode: str
  start_date: str

  @property
  def target_dir(self) -> str:
    return f"{self.warehouse_dir}/{self.target_db}.db"


def load_config(args: dict):
  """
  args - dict of job arguments including:
    JOB_NAME: job name
    SOURCE_DB: source database name
    SOURCE_PATH: source path
    WAREHOUSE_DIR: Spark warehouse directory
    TARGET_DB: target database name
    PIPELINE_MODE: "full" loads all data; "incremental" loads data since start_date
    START_DATE: lower bound for created_at filter (YYYY-MM-DD) in incremental mode

  Returns JobConfig object
  """

  config = JobConfig(
    job_name=args["JOB_NAME"],
    source_db=args["SOURCE_DB"],
    source_path=args["SOURCE_PATH"],
    warehouse_dir=args["WAREHOUSE_DIR"],
    target_db=args["TARGET_DB"],
    pipeline_mode=args["PIPELINE_MODE"],
    start_date=args["START_DATE"],
  )
  return config


def info(msg):
  print("=" * 80)
  print(msg)
  print("=" * 80)


def table_counts(spark, schemas):
  table_count_msgs = []
  for db in schemas:
    tables = spark.sql(f"SHOW TABLES IN {db}")
    table_names = [f"{r.namespace}.{r.tableName}" for r in tables.collect()]

    for t in table_names:
      table_count_msgs.append(f"{spark.table(t).count():,} records  {t}")

  print("=" * 80)
  print("\nTABLE ROW COUNTS:")
  print("\n".join(table_count_msgs))
  print("=" * 80)
