import os
import time
import json
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY_1")
HEADERS = {"X-API-Key": API_KEY}
BASE_URL = "https://api.openaq.org/v3"

# -----------------------------
# Location & Sensor Extraction
# -----------------------------


def fetch_locations_with_sensors(limit=100, country="United States"):
    """Fetch all OpenAQ locations with sensors for the specified country."""
    all_locations = []
    page = 1

    while True:
        params = {"limit": limit, "page": page}
        response = requests.get(f"{BASE_URL}/locations", headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code} — {response.text}")
            break

        results = response.json().get("results", [])
        if not results:
            break

        for loc in results:
            sensors = loc.get("sensors", [])
            if sensors and loc["country"]["name"] == country:
                location_data = {
                    "id": loc["id"],
                    "name": loc["name"],
                    "country": loc["country"]["name"],
                    "locality": loc.get("locality"),
                    "coordinates": loc.get("coordinates", {}),
                    "sensors": [
                        {
                            "sensor_id": s["id"],
                            "parameter": s["parameter"]["name"],
                            "units": s["parameter"]["units"],
                        }
                        for s in sensors
                    ],
                }
                all_locations.append(location_data)

        print(f"Page {page} done. Total collected: {len(all_locations)}")
        page += 1
        time.sleep(1)

    return all_locations


def save_locations_to_file(locations, filename="data/US_with_sensors.json"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        json.dump(locations, f, indent=2)
    print(f"Saved {len(locations)} locations with sensors to '{filename}'")


# -----------------------------
# Active Sensor Filtering
# -----------------------------


def is_recent(utc_time: str, threshold_hours=48) -> bool:
    threshold = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
    dt = datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
    return dt >= threshold


def filter_active_sensors(location):
    location_id = location["id"]
    url = f"{BASE_URL}/locations/{location_id}/latest"

    for attempt in range(5):
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            data = response.json().get("results", [])
            active_ids = [
                sensor["sensorsId"]
                for sensor in data
                if is_recent(sensor["datetime"]["utc"])
            ]
            if active_ids:
                return {
                    "id": location["id"],
                    "name": location["name"],
                    "country": location["country"],
                    "locality": location.get("locality"),
                    "coordinates": location.get("coordinates"),
                    "active_sensor_ids": active_ids,
                }
            return None

        elif response.status_code == 429:
            wait_time = 10 * (2**attempt)
            print(f"⚠ Rate limited — retrying in {wait_time}s")
            time.sleep(wait_time)
        elif 500 <= response.status_code < 600:
            print(f"Server error {response.status_code}, skipping...")
            time.sleep(10)
            return None
        else:
            print(f"Failed to fetch latest for {location_id} — {response.status_code}")
            return None

    print(f"Max retries exceeded for location {location_id}")
    return None


def filter_and_save_active_locations(
    input_file="data/US_with_sensors.json",
    output_file="data/active_locations_filtered.jsonl",
    resume_file="data/last_processed_id.txt",
):
    os.makedirs("data", exist_ok=True)

    try:
        with open(input_file, "r") as f:
            locations = json.load(f)
    except Exception as e:
        print(f"Error reading locations file: {e}")
        return

    resume_id = 0
    if os.path.exists(resume_file):
        with open(resume_file, "r") as f:
            resume_id = int(f.read().strip())
            print(f"Resuming from location ID {resume_id}")

    for loc in locations:
        if loc["id"] <= resume_id:
            continue

        if not loc.get("sensors"):
            continue

        result = filter_active_sensors(loc)
        if result:
            with open(output_file, "a") as f:
                f.write(json.dumps(result) + "\n")
            with open(resume_file, "w") as f:
                f.write(str(loc["id"]))
            print(
                f"✔ Saved {result['name']} with {len(result['active_sensor_ids'])} sensors"
            )


# -----------------------------
# Entry Point
# -----------------------------

if __name__ == "__main__":
    print("Fetching all sensor-equipped US locations...")
    locations = fetch_locations_with_sensors()
    save_locations_to_file(locations)

    print("\nFiltering active locations and sensors...")
    filter_and_save_active_locations()
