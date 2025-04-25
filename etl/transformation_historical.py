import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (optional but future-proof)
load_dotenv()

# Constants
BASE_FOLDER = "data/data_by_sensor"
LOCATION_FILE = "data/active_locations_filtered.jsonl"
SENSOR_UNITS_FILE = "data/active_sensor_info.jsonl"
OUTPUT_FILE = "data/US_data_structured_cleaned.json"

# ---- Helpers ----


def convert_types(obj):
    """Recursively convert types to JSON serializable format."""
    if isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_types(i) for i in obj]
    elif isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    else:
        return obj


def load_sensor_units(path):
    sensor_units = {}
    with open(path, "r") as f:
        for line in f:
            record = json.loads(line)
            key = (record["sensor_id"], record["parameter"])
            sensor_units[key] = record["units"]
    return sensor_units


def load_sensor_data(sensor_ids, base_folder):
    all_data = []
    for sensor_id in sensor_ids:
        folder_path = os.path.join(base_folder, f"sensor_{sensor_id}")
        if not os.path.isdir(folder_path):
            continue
        for filename in os.listdir(folder_path):
            if filename.endswith(".jsonl") and filename.startswith("sensor_"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, "r") as f:
                    for line in f:
                        record = json.loads(line)
                        record["sensor_id"] = sensor_id
                        all_data.append(record)
    return all_data


# ---- Main Transformation ----


def transform_historical_data():
    print("⏳ Starting transformation of historical sensor data...")
    sensor_units_map = load_sensor_units(SENSOR_UNITS_FILE)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fout:
        fout.write("[\n")

    first = True
    with open(LOCATION_FILE, "r", encoding="utf-8") as loc_file:
        for line in loc_file:
            loc = json.loads(line)
            location_entry = {
                "location_id": loc["id"],
                "location_name": loc["name"],
                "country": loc["country"],
                "locality": loc.get("locality"),
                "coordinates": loc.get("coordinates"),
                "sensors": [],
            }

            all_sensor_data = load_sensor_data(loc["active_sensor_ids"], BASE_FOLDER)
            if not all_sensor_data:
                continue

            df = pd.DataFrame(all_sensor_data)
            df.dropna(subset=["datetime", "parameter", "value"], inplace=True)
            df.drop_duplicates(
                subset=["sensor_id", "datetime", "parameter"], inplace=True
            )
            df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
            df["date"] = df["datetime"].dt.date
            df["hour"] = df["datetime"].dt.hour

            grouped = df.groupby(["sensor_id", "parameter"])
            for (sensor_id, parameter), group in grouped:
                units = sensor_units_map.get((sensor_id, parameter), "unknown")
                measurements = [
                    {
                        "date": row["date"].isoformat(),
                        "hour": int(row["hour"]),
                        "value": row["value"],
                    }
                    for _, row in group.iterrows()
                ]
                location_entry["sensors"].append(
                    {
                        "sensor_id": sensor_id,
                        "parameter": parameter,
                        "units": units,
                        "measurements": measurements,
                    }
                )

            # Write to file
            location_entry = convert_types(location_entry)
            with open(OUTPUT_FILE, "a", encoding="utf-8") as fout:
                if not first:
                    fout.write(",\n")
                else:
                    first = False
                json.dump(location_entry, fout, indent=2)

    with open(OUTPUT_FILE, "a", encoding="utf-8") as fout:
        fout.write("\n]")

    print("✔ Transformation complete. Data saved to:", OUTPUT_FILE)


# ---- CLI Entry Point ----

if __name__ == "__main__":
    transform_historical_data()
