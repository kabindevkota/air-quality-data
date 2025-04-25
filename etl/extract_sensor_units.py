import os
import json

# File paths
LOCATION_FILE = "data/US_with_sensors.json"
ACTIVE_FILE = "data/active_locations_filtered.jsonl"
OUTPUT_FILE = "data/active_sensor_info.jsonl"


def build_sensor_lookup(location_file):
    """Builds a sensor_id → parameter/unit lookup from all locations."""
    with open(location_file, "r") as f:
        locations = json.load(f)

    sensor_lookup = {}
    for loc in locations:
        for sensor in loc.get("sensors", []):
            sensor_lookup[sensor["sensor_id"]] = {
                "parameter": sensor["parameter"],
                "units": sensor["units"],
            }

    return sensor_lookup


def extract_active_sensor_info(active_file, sensor_lookup, output_file):
    """Writes info for only the active sensors into a JSONL file."""
    active_sensor_set = set()

    with open(active_file, "r") as f:
        for line in f:
            loc = json.loads(line)
            active_sensor_set.update(loc.get("active_sensor_ids", []))

    active_sensor_entries = []
    for sensor_id in sorted(active_sensor_set):
        if sensor_id in sensor_lookup:
            entry = {
                "sensor_id": sensor_id,
                "parameter": sensor_lookup[sensor_id]["parameter"],
                "units": sensor_lookup[sensor_id]["units"],
            }
            active_sensor_entries.append(entry)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        for entry in active_sensor_entries:
            f.write(json.dumps(entry) + "\n")

    print(
        f"✔ Saved {len(active_sensor_entries)} active sensors with units to '{output_file}'"
    )


if __name__ == "__main__":
    print("Building sensor lookup...")
    sensor_lookup = build_sensor_lookup(LOCATION_FILE)

    print("Extracting active sensor info...")
    extract_active_sensor_info(ACTIVE_FILE, sensor_lookup, OUTPUT_FILE)
