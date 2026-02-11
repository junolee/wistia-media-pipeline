"""
Commit checkpoint state to S3 after downstream processing succeeds.
"""

import os

from config import exception, get_s3_client, info, save_checkpoint


def lambda_handler(event, context):
  info("CommitCheckpoint received event: %s", event)

  try:
    new_last_run_ts = event.get("new_last_run_ts")
    persist_state = event.get("persist_state", True)
    if not new_last_run_ts:
      raise ValueError("Missing new_last_run_ts in event payload")

    bucket_name = os.environ["BUCKET_NAME"]
    checkpoint_path = os.environ["WISTIA_CHECKPOINT_PATH"]
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"

    if dry_run or not persist_state:
      info(
        "Skip checkpoint write to s3://%s/%s with last_run_ts=%s (dry_run=%s, persist_state=%s)",
        bucket_name,
        checkpoint_path,
        new_last_run_ts,
        dry_run,
        persist_state,
      )
      return {
        "status": "skipped",
        "new_last_run_ts": new_last_run_ts,
      }

    s3 = get_s3_client()
    save_checkpoint(s3, bucket_name, checkpoint_path, new_last_run_ts)

    return {
      "status": "committed",
      "new_last_run_ts": new_last_run_ts,
    }
  except Exception:
    exception("Unhandled exception in CommitCheckpoint Lambda")
    raise
