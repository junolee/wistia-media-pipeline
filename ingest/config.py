import json
import os
import time
from dataclasses import dataclass

import boto3
import requests

# ============================================================================
# CONFIG CLASS
# ============================================================================


@dataclass(frozen=True)
class AppConfig:
  yt_media_id: str
  fb_media_id: str
  api_creds_secret: str
  bucket_name: str
  raw_prefix: str
  checkpoint_path: str


def load_config() -> AppConfig:
  return AppConfig(
    yt_media_id=os.environ["YT_MEDIA_ID"],
    fb_media_id=os.environ["FB_MEDIA_ID"],
    api_creds_secret=os.environ["API_TOKEN_SECRET_NAME"],
    bucket_name=os.environ["BUCKET_NAME"],
    raw_prefix=os.environ["RAW_PREFIX"],
    checkpoint_path=os.environ["WISTIA_CHECKPOINT_PATH"],
  )


# ============================================================================
# Secrets / Auth
# ============================================================================
def load_secret(secret_name) -> dict:
  """
  Load a secret stored in Secrets Manager as SecretString and parse it as JSON
  """
  secrets = boto3.client("secretsmanager")
  secret_str = secrets.get_secret_value(SecretId=secret_name)["SecretString"]
  return json.loads(secret_str)


# ============================================================================
# S3 State Checkpoint
# ============================================================================


def load_checkpoint(s3, bucket_name, path):
  try:
    response = s3.get_object(Bucket=bucket_name, Key=path)
    last_run_ts = json.loads(response["Body"].read().decode("utf-8"))["last_run_ts"]
    print(
      f"Loaded last successful run timestamp: {last_run_ts} from s3://{bucket_name}/{path}"
    )
    last_run_date = last_run_ts.split("T")[0]
    return last_run_date

  except s3.exceptions.NoSuchKey:
    print(f"No checkpoint found at s3://{bucket_name}/{path}")
    return None


def save_checkpoint(s3, bucket_name, path, new_last_run_ts):
  data = json.dumps({"last_run_ts": new_last_run_ts})
  s3.put_object(Body=data, Bucket=bucket_name, Key=path)
  print(
    f"Saved last successful run timestamp: {new_last_run_ts} to s3://{bucket_name}/{path}"
  )


# ============================================================================
# Wistia API helper
# ============================================================================


def get_with_backoff(url, headers=None, params=None, max_retries=5, backoff_factor=1.0):
  for attempt in range(1, max_retries + 1):
    try:
      response = requests.get(url, headers=headers, params=params, timeout=10)
      if response.status_code in {500, 502, 503, 504}:
        raise requests.HTTPError(f"{response.status_code} retryable", response=response)
      response.raise_for_status()
      return response
    except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
      if attempt == max_retries:
        raise
      sleep_s = backoff_factor * (2 ** (attempt - 1))
      print(f"Retry {attempt}/{max_retries} after error {e}, sleeping {sleep_s:.1f}s")
      time.sleep(sleep_s)


class WistiaAPIClient:
  """
  Client for interacting with the Wistia Stats API
  """

  def __init__(self, api_token):
    self.api_token = api_token

  def _get_request(self, endpoint: str, params: dict = {}):
    """
    Helper function to make GET requests to the Wistia API
    """
    url = f"https://api.wistia.com/v1/{endpoint}"
    headers = {
      "accept": "application/json",
      "authorization": f"Bearer {self.api_token}",
    }

    response = get_with_backoff(url, headers=headers, params=params or {})

    return response.json()

  def get_media(self, hashed_ids: list[str]) -> dict:
    return self._get_request("medias", params=[("hashed_ids[]", h) for h in hashed_ids])

  def list_events(self, media_id=None, start_date=None, end_date=None) -> list[dict]:
    """
    List all events for media_id
    - Paginates through events from Wistia Stats API
    - Optionally filters by start_date and end_date
    """
    all_events = []
    page = 1

    while True:
      events = self._get_request(
        "stats/events",
        params={
          "media_id": media_id,
          "per_page": 100,
          "page": page,
          "start_date": start_date,
          "end_date": end_date,
        },
      )
      if not events:
        break
      all_events.extend(events)
      page += 1
      if page % 10 == 0:
        print(
          f"Fetched {900 + len(events)} events for media ID: {media_id} - page: {page} - time: {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    print(f"Fetched {len(all_events)} events for media ID: {media_id}")
    return all_events


# ============================================================================
# S3 helper functions
# ============================================================================


def get_s3_client():
  s3 = boto3.client("s3")
  return s3
