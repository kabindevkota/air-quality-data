import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load API key and config
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY_1")
HEADERS = {"X-API-Key": API_KEY}
BASE_URL = "https://api.openaq.org/v3/sensors/{}/measurements"

# File paths
ACTIVE_FILE = "data/active_locations_filtered.jsonl"
RESUME_FILE = "data/sensor_resume_checkpoint.txt"
OUTPUT_DIR = "data/data_by_sensor"

# Extraction config
CHUNK_DAYS = 3
LIMIT = 100
MAX_PAGES = 1  # To prevent runaway pagination
END_DATE = datetime.now(timezone.utc)
START_DATE = END_DATE - timedelta(days=90)


# Load all sensors from active locations
def load_all_sensors():
    sensors = []
    with open(ACTIVE_FILE) as f:
        for line in f:
            loc = json.loads(line)
            for sensor_id in loc.get("active_sensor_ids", []):
                sensors.append(
                    {
                        "sensor_id": sensor_id,
                        "location_id": loc["id"],
                        "location_name": loc["name"],
                        "country": loc["country"],
                    }
                )
    return sensors


# Resume checkpoint
def load_resume():
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, "r") as f:
            line = f.read().strip()
            if line:
                date_str, sensor_id_str = line.split(",")
                try:
                    resume_date = datetime.strptime(
                        date_str, "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    resume_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                return resume_date, int(sensor_id_str)
    return None, None


# Fetch data for one sensor and one 3-day chunk
def fetch_measurements(sensor_id, dt_from, dt_to):
    page = 1
    results = []
    retry_delay = 10

    while page <= MAX_PAGES:
        params = {
            "datetime_from": dt_from.isoformat(),
            "datetime_to": dt_to.isoformat(),
            "limit": LIMIT,
            "page": page,
        }
        try:
            response = requests.get(
                BASE_URL.format(sensor_id), headers=HEADERS, params=params
            )
            if response.status_code == 200:
                data = response.json().get("results", [])
                if not data:
                    break
                results.extend(data)
                if len(data) < LIMIT:
                    break
                page += 1
                time.sleep(0.4)
            elif response.status_code == 429:
                print(
                    f"⚠ Rate limit hit for sensor {sensor_id}. Retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                print(f"⚠ HTTP {response.status_code} for sensor {sensor_id}")
                break
        except Exception as e:
            print(f"⚠ Error fetching sensor {sensor_id}: {e}")
            break
    return results


# Main extraction loop
def extract_all_measurements():
    all_sensors = load_all_sensors()
    resume_date, resume_sensor_id = load_resume()

    curr_date = resume_date if resume_date else END_DATE
    while curr_date > START_DATE:
        chunk_end = curr_date
        chunk_start = chunk_end - timedelta(days=CHUNK_DAYS)
        print(f"\nProcessing chunk: {chunk_start.date()} to {chunk_end.date()}")

        found_resume_sensor = (
            False if resume_date and chunk_end == resume_date else True
        )

        for sensor in all_sensors:
            sensor_id = sensor["sensor_id"]

            if not found_resume_sensor:
                if sensor_id == resume_sensor_id:
                    found_resume_sensor = True
                else:
                    print(
                        f"Skipping sensor {sensor_id} for {chunk_start.date()} to {chunk_end.date()}"
                    )
                    continue

            sensor_path = os.path.join(OUTPUT_DIR, f"sensor_{sensor_id}")
            os.makedirs(sensor_path, exist_ok=True)
            out_file = os.path.join(sensor_path, f"sensor_{sensor_id}.jsonl")

            print(f"Fetching sensor {sensor_id} ({sensor['location_name']})...")

            try:
                entries = fetch_measurements(sensor_id, chunk_start, chunk_end)
                valid_entries = []
                for e in entries:
                    if not all(
                        [
                            e.get("value") is not None,
                            "parameter" in e,
                            "period" in e,
                            "datetimeFrom" in e["period"],
                            "utc" in e["period"]["datetimeFrom"],
                        ]
                    ):
                        continue
                    valid_entries.append(
                        {
                            "datetime": e["period"]["datetimeFrom"]["utc"],
                            "parameter": e["parameter"]["name"],
                            "value": e["value"],
                        }
                    )

                if valid_entries:
                    with open(out_file, "a", encoding="utf-8") as f_out:
                        for v in valid_entries:
                            f_out.write(json.dumps(v) + "\n")
                    print(
                        f"✔ Saved {len(valid_entries)} entries for sensor {sensor_id}"
                    )
                else:
                    print(f"⚠ No valid data for sensor {sensor_id}")

                with open(RESUME_FILE, "w") as f:
                    f.write(f"{chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')},{sensor_id}")

                time.sleep(0.5)

            except Exception as e:
                print(f"⚠ Error processing sensor {sensor_id}: {e}")

        resume_date, resume_sensor_id = None, None
        curr_date -= timedelta(days=CHUNK_DAYS)

    print("\n✔ Historical extraction complete.")


if __name__ == "__main__":
    extract_all_measurements()
