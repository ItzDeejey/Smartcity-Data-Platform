# scripts/kafka_producers/stream_all_datasets_to_kafka.py
import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer

def make_message(row, dataset_type):
    """
    Map each dataset type to its own JSON structure.
    """
    if dataset_type == "accident":
        return {
            "accident_index": str(row.get("accident_index", "")),
            "year": int(row.get("accident_year", 0)),
            "severity": int(row.get("accident_severity", 0)),
            "num_vehicles": int(row.get("number_of_vehicles", 0)),
            "num_casualties": int(row.get("number_of_casualties", 0)),
            "date": str(row.get("date", "")),
            "time": str(row.get("time", "")),
            "longitude": float(row.get("longitude", 0.0)) if pd.notna(row.get("longitude")) else None,
            "latitude": float(row.get("latitude", 0.0)) if pd.notna(row.get("latitude")) else None,
            "road_type": str(row.get("road_type", "")),
            "weather_conditions": str(row.get("weather_conditions", "")),
        }

    elif dataset_type == "casualty":
        return {
            "accident_index": str(row.get("accident_index", "")),
            "year": int(row.get("accident_year", 0)),
            "vehicle_reference": int(row.get("vehicle_reference", 0)),
            "casualty_reference": int(row.get("casualty_reference", 0)),
            "casualty_class": int(row.get("casualty_class", 0)),
            "sex_of_casualty": int(row.get("sex_of_casualty", 0)),
            "age_of_casualty": int(row.get("age_of_casualty", 0)),
            "casualty_severity": int(row.get("casualty_severity", 0)),
            "casualty_type": int(row.get("casualty_type", 0)),
        }

    elif dataset_type == "vehicle":
        return {
            "accident_index": str(row.get("accident_index", "")),
            "year": int(row.get("accident_year", 0)),
            "vehicle_reference": int(row.get("vehicle_reference", 0)),
            "vehicle_type": int(row.get("vehicle_type", 0)),
            "vehicle_manoeuvre": int(row.get("vehicle_manoeuvre", 0)),
            "first_point_of_impact": int(row.get("first_point_of_impact", 0)),
            "sex_of_driver": int(row.get("sex_of_driver", 0)),
            "age_of_driver": int(row.get("age_of_driver", 0)),
            "engine_capacity_cc": int(row.get("engine_capacity_cc", 0)),
            "propulsion_code": int(row.get("propulsion_code", 0)),
        }

    else:
        raise ValueError(f"Unknown dataset_type: {dataset_type}")

def stream(csv_path, kafka_bootstrap, topic, dataset_type, delay=1.0):
    """
    Read CSV and send rows to Kafka topic as JSON messages.
    """
    df = pd.read_csv(csv_path)
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10
    )

    print(f"Streaming {len(df)} rows from {csv_path} → topic={topic}")
    for _, row in df.iterrows():
        msg = make_message(row, dataset_type)
        producer.send(topic, value=msg)
        print("Sent:", msg)
        time.sleep(delay)

    producer.flush()
    print("Finished streaming.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--dataset_type", required=True, choices=["accident", "casualty", "vehicle"])
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between messages in seconds")
    args = parser.parse_args()

    stream(args.csv, args.kafka, args.topic, args.dataset_type, args.delay)
