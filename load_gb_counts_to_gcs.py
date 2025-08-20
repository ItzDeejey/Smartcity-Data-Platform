# airflow/scripts/load_gb_counts_to_gcs.py
import pandas as pd
import os
from datetime import datetime
from gcs_utils import upload_file_to_gcs

"""
Reads historical UK traffic count CSVs (AADF + Raw Counts),
cleans them, writes Parquet to /opt/airflow/processed,
and uploads to GCS under reference/traffic_counts/{dataset_type}/.
"""

BUCKET = "smartcity-data-lake"
PROCESSED_DIR = "/opt/airflow/processed"

def clean_dataset(df, dataset_type):
    """
    Transform the dataset depending on its type.
    """
    if dataset_type == "aadf":
        keep = [
            "count_point_id", "year", "region_id", "region_name",
            "local_authority_id", "local_authority_name",
            "road_name", "road_category", "road_type",
            "latitude", "longitude", "link_length_km", "link_length_miles",
            "pedal_cycles", "two_wheeled_motor_vehicles", "cars_and_taxis",
            "buses_and_coaches", "lgvs",
            "hgvs_2_rigid_axle", "hgvs_3_rigid_axle", "hgvs_4_or_more_rigid_axle",
            "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle", "hgvs_6_articulated_axle",
            "all_hgvs", "all_motor_vehicles"
        ]
        df = df[[c for c in keep if c in df.columns]]

    elif dataset_type == "raw_counts":
        keep = [
            "count_point_id", "direction_of_travel", "year", "count_date", "hour",
            "region_id", "region_name", "local_authority_id", "local_authority_name",
            "road_name", "road_category", "road_type",
            "latitude", "longitude", "link_length_km", "link_length_miles",
            "pedal_cycles", "two_wheeled_motor_vehicles", "cars_and_taxis",
            "buses_and_coaches", "lgvs",
            "hgvs_2_rigid_axle", "hgvs_3_rigid_axle", "hgvs_4_or_more_rigid_axle",
            "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle", "hgvs_6_articulated_axle",
            "all_hgvs", "all_motor_vehicles"
        ]
        df = df[[c for c in keep if c in df.columns]]

    else:
        raise ValueError(f"Unknown dataset_type: {dataset_type}")

    return df


def clean_and_upload(csv_path, dataset_type):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    df = clean_dataset(df, dataset_type)

    now = datetime.utcnow()
    filename = f"{dataset_type}_traffic_counts.parquet"
    local_path = os.path.join(PROCESSED_DIR, filename)

    df.to_parquet(local_path, index=False)

    dest = f"reference/traffic_counts/{dataset_type}/{filename}"
    upload_file_to_gcs(BUCKET, local_path, dest)
    print(f"Uploaded {dataset_type} dataset to gs://{BUCKET}/{dest}")


if __name__ == "__main__":
    # Example usage (can adapt for argparse if you want flexibility)
    aadf_csv = "/opt/airflow/data/historical/dft_traffic_counts_aadf/dft_traffic_counts_aadf.csv"
    raw_csv = "/opt/airflow/data/historical/dft_traffic_counts_raw_counts/dft_traffic_counts_raw_counts.csv"

    clean_and_upload(aadf_csv, "aadf")
    clean_and_upload(raw_csv, "raw_counts")
