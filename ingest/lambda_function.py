"""
Entry point for lambda function.
Calls main() from wistia_to_s3.py.
"""

import json

from wistia_to_s3 import main


def lambda_handler(event, context):
  print(f"lambda_handler received event: {event}")

  main(
    pipeline_mode=event.get("pipeline_mode"),
    persist_state=event.get("persist_state"),
    start_date=event.get("start_date"),
    end_date=event.get("end_date"),
  )

  return {"statusCode": 200, "body": json.dumps("Lambda execution successful")}
