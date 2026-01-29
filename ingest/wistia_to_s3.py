"""
Incremental ingestion from Wistia API to an S3 "raw" landing zone

Called by: lambda_function.py

Run modes - set via main():
- Full: ingest all events from the Wistia Stats API (up to last 2 years are available)
- Incremental: ingest only events since the last successful run, based on state checkpoint in S3

Notes:
- If persist_state=True, an updated timestamp is saved to S3 at state_checkpoint_path after a successful run
- Both modes ingest the latest media metadata using the Wistia API
"""

import datetime

from config import *


def write_json_lines_to_s3(s3, bucket_name, key, data: list[dict]):
  # Serialize to JSON Lines
  body = "\n".join(json.dumps(record) for record in data) + ("\n" if data else "")
  s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=body,
    ContentType="application/json",
  )


def ingest_media(ws, s3, cfg):
  """
  Ingest media metadata from Wistia API and write to S3
  """
  ingest_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
  data = ws.get_media(hashed_ids=[cfg.yt_media_id, cfg.fb_media_id])

  key = f"raw/media/ingest_date={ingest_date}/media.jsonl"
  print(f"Writing media metadata to s3://{cfg.bucket_name}/{key}")
  write_json_lines_to_s3(s3, cfg.bucket_name, key, data)


def ingest_all_events(ws, s3, cfg, start_date=None, end_date=None):
  """
  Ingest events data from Wistia API and write to S3
  """
  ingest_ts = datetime.datetime.utcnow().isoformat()
  ingest_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

  events = []
  for media_id in [cfg.yt_media_id, cfg.fb_media_id]:
    media_events = ws.list_events(
      media_id=media_id, start_date=start_date, end_date=end_date
    )
    for record in media_events:
      record["media_id"] = media_id
      record["ingest_ts"] = ingest_ts
    events.extend(media_events)

  if not events:
    print(
      f"No new events to ingest for media IDs for date range {start_date} to {end_date}."
    )
    return

  key = f"{cfg.raw_prefix}/events/ingest_date={ingest_date}/events.jsonl"
  print(f"Writing events data to s3://{cfg.bucket_name}/{key}")
  write_json_lines_to_s3(s3, cfg.bucket_name, key, events)


def main(
  pipeline_mode="incremental", persist_state=False, start_date=None, end_date=None
):
  """
  Run ingestion in either Full Refresh or Incremental mode.

  Args:
    pipeline_mode: If "full_refresh", ingest all events (last 2 years). If "incremental", ingest only events since last run.
    persist_state: If True, update state checkpoint for next incremental load.

  Requires config (loaded via load_config()):
  - bucket_name: S3 bucket for raw landing zone + state
  - checkpoint_path: S3 path to JSON file storing state checkpoint
  """
  print(f"Running ingest script in mode: {pipeline_mode}")

  c = load_config()
  api_token = load_secret(c.api_creds_secret)["token"]
  ws = WistiaAPIClient(api_token)
  s3 = get_s3_client()

  new_last_run_ts = datetime.datetime.utcnow().isoformat()

  if start_date:
    print(f"Using provided start_date: {start_date} and end_date: {end_date}.")

  elif pipeline_mode == "incremental":
    last_run_date = load_checkpoint(s3, c.bucket_name, c.checkpoint_path)
    if last_run_date:
      start_date = last_run_date
      print(f"Using checkpoint start_date: {start_date}")
    else:
      print("No checkpoint available, running full refresh for last 2 years")
  else:
    print("Running full refresh for last 2 years")

  ingest_media(ws, s3, c)
  ingest_all_events(ws, s3, c, start_date=start_date, end_date=end_date)

  if persist_state:
    print("Persisting state...")
    save_checkpoint(s3, c.bucket_name, c.checkpoint_path, new_last_run_ts)
  else:
    print(f"Not persisting state... Current run timestamp: {new_last_run_ts}")

  return
