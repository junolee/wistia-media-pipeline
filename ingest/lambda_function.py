"""
Entry point for lambda function.
Calls main() from wistia_to_s3.py.
"""

from config import debug_json, exception, info
from wistia_to_s3 import main


def lambda_handler(event, context):
  debug_json("lambda_handler received event", event)

  try:
    results = main(
      pipeline_mode=event.get("pipeline_mode"),
      start_date=event.get("start_date"),
      end_date=event.get("end_date"),
    )
    info("Lambda handler completed successfully.")
    return results
  except Exception:
    exception("Unhandled exception in Wistia ingestion Lambda")
    raise

  return results
