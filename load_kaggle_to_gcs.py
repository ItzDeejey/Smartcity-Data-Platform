# airflow/scripts/load_kaggle_to_gcs.py
import pandas as pd
from datetime import datetime
import os
from gcs_utils import upload_file_to_gcs

"""
Reads the Kaggle CSV, does minimal cleaning, writes a Parquet
to /opt/airflow/processed and uploads to GCS under raw/traffic/YYYY/MM/DD/.
"""

def clean_and_upload_kaggle():
    DATA_CSV = "/opt/airflow/data/uk_traffic.csv"  # host -> container mount
    PROCESSED_DIR = "/opt/airflow/processed"
    BUCKET = "smartcity-data-lake"

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    df = pd.read_csv(DATA_CSV)
    # Simple cleaning:
    df.columns = [c.strip().lower() for c in df.columns]
    # Keep only essential columns (drop the rest if present)
    keep = ["timestamp", "location_id", "vehicle_count", "avg_speed", "road_type", "coordinates"]
    df = df[[c for c in keep if c in df.columns]]

    now = datetime.utcnow()
    y, m, d = now.year, now.month, now.day
    filename = f"traffic_{y}_{m:02d}_{d:02d}.parquet"
    local_path = os.path.join(PROCESSED_DIR, filename)
    df.to_parquet(local_path, index=False)
    dest = f"raw/traffic/{y}/{m:02d}/{d:02d}/{filename}"
    upload_file_to_gcs(BUCKET, local_path, dest)
