EVENTS_RAW_COLUMNS = """
  city                string,
  conversion_type     string,
  country             string,
  email               string,
  embed_url           string,
  event_key           string,
  iframe_heatmap_url  string,
  ingest_ts           string,
  ip                  string,
  lat                 string,
  lon                 string,
  media_id            string,
  media_name          string,
  media_url           string,
  org                 string,
  percent_viewed      string,
  received_at         string,
  region              string,
  thumbnail           string,
  user_agent_details  string,
  visitor_key         string
"""

MEDIA_RAW_COLUMNS = """
  archived            string,
  assets              string,
  created             string,
  description         string,
  duration            string,
  hashed_id           string,
  id                  string,
  name                string,
  progress            string,
  project             string,
  section             string,
  status              string,
  subfolder           string,
  tags                string,
  thumbnail           string,
  type                string,
  updated             string
"""


DIM_DATES_COLUMNS = """
  date              date,
  month_num         int,
  day               int,
  day_of_week_name  string,
  day_of_week       int,
  is_weekday        boolean,
  year              int,
  month_name        string,
  week_of_year      int,
  updated_at        timestamp
"""

DIM_VISITORS_COLUMNS = """
  visitor_id        string,
  created_at        timestamp,
  ip_address        string,
  country           string,
  region            string,
  updated_at        timestamp
"""

DIM_MEDIA_COLUMNS = """
  media_id          string,
  created_at        timestamp,
  duration          double,
  title             string,
  channel           string,
  media_url         string,
  updated_at        timestamp
"""

FCT_EVENTS_COLUMNS = """
  media_id          string,
  visitor_id        string,
  created_at        timestamp,
  date              date,
  percent_viewed    double,
  duration_viewed   double,
  updated_at        timestamp
"""

FCT_MEDIA_ENGAGEMENT_COLUMNS = """
  media_id                  string,
  visitor_id                string,
  date                      date,
  play_count                bigint,
  total_watch_time          double,
  avg_watch_time_per_view   double,
  max_percent_viewed        double,
  avg_percent_viewed        double,
  updated_at                timestamp
"""
