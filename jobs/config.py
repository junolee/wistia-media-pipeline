import logging
from dataclasses import dataclass

# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger("app")

logging.basicConfig(
  level=logging.DEBUG,
  format="%(asctime)s %(levelname)s %(name)s:%(funcName)s:%(lineno)d | %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S",
)
for name, level in {
  "boto3": logging.WARNING,
  "botocore": logging.WARNING,
  "urllib3": logging.WARNING,
  "requests": logging.WARNING,
  "py4j": logging.ERROR,
}.items():
  logging.getLogger(name).setLevel(level)

# ============================================================
# CONFIG CLASS
# ============================================================


@dataclass
class JobConfig:
  job_name: str
  source_db: str
  source_path: str
  warehouse_dir: str
  target_db: str
  pipeline_mode: str
  start_date: str
  dry_run: bool = False

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
    DRY_RUN: if "true", run in dry run mode (default: "false")

  Returns JobConfig object
  """

  c = JobConfig(
    job_name=args["JOB_NAME"],
    source_db=args["SOURCE_DB"],
    source_path=args["SOURCE_PATH"],
    warehouse_dir=args["WAREHOUSE_DIR"],
    target_db=args["TARGET_DB"],
    pipeline_mode=args["PIPELINE_MODE"],
    start_date=args["START_DATE"],
    dry_run=args.get("DRY_RUN", "false").lower() == "true",
  )
  info(
    "Loaded config job_name=%s, source_db=%s, source_path=%s, warehouse_dir=%s, target_db=%s, pipeline_mode=%s, start_date=%s, dry_run=%s",
    c.job_name,
    c.source_db,
    c.source_path,
    c.warehouse_dir,
    c.target_db,
    c.pipeline_mode,
    c.start_date,
    c.dry_run,
  )
  return c


# ============================================================
# LOG HELPERS
# ============================================================


def debug_json(label, obj, max_length=1500):
  s = json.dumps(obj, indent=2, default=str)
  if len(s) > max_length:
    s = s[:max_length] + "\n... [truncated]"
  logger.debug("%s\n%s", label, s, stacklevel=2)


def info(msg, *args):
  logger.info(msg, *args, stacklevel=2)


def debug(msg, *args):
  logger.debug(msg, *args, stacklevel=2)


def warning(msg, *args):
  logger.warning(msg, *args, stacklevel=2)


def exception(msg, *args):
  logger.exception(msg, *args, stacklevel=2)


def table_counts(spark, schemas, cfg):
  if cfg.dry_run:
    info(f"[DRY RUN] Skip counting records in target tables in {schemas}")
    return

  info(f"Counting records in target tables in {schemas}...")
  table_count_msgs = []
  for db in schemas:
    tables = spark.sql(f"SHOW TABLES IN {db}")
    table_names = [f"{r.namespace}.{r.tableName}" for r in tables.collect()]

    for t in table_names:
      table_count_msgs.append(f"{spark.table(t).count():,} records  {t}")

  debug("\nTABLE ROW COUNTS: %s", "\n".join(table_count_msgs))
