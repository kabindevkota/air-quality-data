import json, os, pandas as pd, numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SENSOR_FILE = "data/active_locations_filtered.jsonl"
SENSOR_UNITS_FILE = "data/active_sensor_info.jsonl"
EXTRACTED_FILE = "data/realtime_raw.json"
TRANSFORMED_FILE = "data/realtime_transformed.json"

def transform_realtime_data():
    with open(EXTRACTED_FILE) as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)
    df.dropna(subset=["datetime", "parameter", "value"], inplace=True)
    df.drop_duplicates(subset=["sensor_id", "datetime", "parameter"], inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df["date"] = df["datetime"].dt.date
    df["hour"] = df["datetime"].dt.hour

    location_map = {}
    location_lookup = {}

    with open(SENSOR_FILE) as f:
        for line in f:
            loc = json.loads(line)
            location_entry = {
                "location_id": loc["id"],
                "location_name": loc["name"],
                "country": loc["country"],
                "locality": loc.get("locality"),
                "coordinates": loc.get("coordinates"),
                "sensors": []
            }
            location_lookup[loc["id"]] = location_entry
            for sensor_id in loc["active_sensor_ids"]:
                location_map[sensor_id] = loc["id"]

    sensor_units_map = {}
    with open(SENSOR_UNITS_FILE) as f:
        for line in f:
            entry = json.loads(line)
            sensor_units_map[(entry["sensor_id"], entry["parameter"])] = entry["units"]

    grouped = df.groupby(["sensor_id", "parameter"])
    sensor_data_grouped = {}

    for (sensor_id, parameter), group in grouped:
        location_id = location_map.get(sensor_id)
        if location_id is None:
            continue
        key = (location_id, sensor_id, parameter)
        if key not in sensor_data_grouped:
            sensor_data_grouped[key] = []
        for _, row in group.iterrows():
            sensor_data_grouped[key].append({
                "date": row["date"].isoformat(),
                "hour": int(row["hour"]),
                "value": row["value"]
            })

    for (location_id, sensor_id, parameter), measurements in sensor_data_grouped.items():
        units = sensor_units_map.get((sensor_id, parameter), "unknown")
        location_lookup[location_id]["sensors"].append({
            "sensor_id": sensor_id,
            "parameter": parameter,
            "units": units,
            "measurements": measurements
        })

    def convert(obj):
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        elif isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        return obj

    output = convert(list(location_lookup.values()))
    with open(TRANSFORMED_FILE, "w") as f:
        json.dump(output, f, indent=2)
