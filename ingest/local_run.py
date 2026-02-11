"""
Entry point for local development.
Calls lambda_handler() from lambda_function.py.
"""

from dotenv import load_dotenv
from lambda_function import lambda_handler

if __name__ == "__main__":
  load_dotenv(override=True)

  event = {
    "pipeline_mode": "incremental",
    # "start_date": "2025-05-29",
    # "end_date": "2026-01-28",
  }

  lambda_handler(event=event, context={})
