import os, json, time, logging, requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY_1")
HEADERS = {"X-API-Key": f"{API_KEY}"}
BASE_URL = "https://api.openaq.org/v3/sensors/{}/measurements"

SENSOR_FILE = "data/active_locations_filtered.jsonl"
EXTRACTED_FILE = "data/realtime_raw.json"


def extract_realtime_data():
    today = datetime.now(timezone.utc).date()
    tomorrow = today + timedelta(days=1)
    all_data = []

    with open(SENSOR_FILE) as f:
        active_locations = [json.loads(line) for line in f]

    for loc in active_locations:
        for sid in loc["active_sensor_ids"]:
            try:
                url = BASE_URL.format(sid)
                params = {
                    "datetime_from": today.isoformat(),
                    "datetime_to": tomorrow.isoformat(),
                    "limit": 100,
                    "page": 1,
                }

                retry_delay = 10
                attempt = 0
                max_retries = 5

                while True:
                    try:
                        r = requests.get(
                            url, headers=HEADERS, params=params, timeout=10
                        )

                        if r.status_code == 429:
                            if attempt < max_retries:
                                logging.warning(
                                    f"429 Too Many Requests for sensor {sid} — retrying in {retry_delay}s"
                                )
                                time.sleep(retry_delay)
                                retry_delay *= 2
                                attempt += 1
                                continue
                            else:
                                break

                        if r.status_code != 200:
                            logging.warning(f"Sensor {sid} failed: {r.status_code}")
                            break

                        results = r.json().get("results", [])
                        if not results:
                            break

                        for entry in results:
                            all_data.append(
                                {
                                    "sensor_id": sid,
                                    "datetime": entry["period"]["datetimeFrom"]["utc"],
                                    "parameter": entry["parameter"]["name"],
                                    "value": entry["value"],
                                }
                            )

                        if len(results) < params["limit"]:
                            break
                        else:
                            params["page"] += 1

                        time.sleep(1)
                    except requests.exceptions.Timeout:
                        logging.error(f"Timeout for sensor {sid}")
                        break
            except Exception as e:
                logging.error(f"Unhandled error for sensor {sid}: {str(e)}")

    with open(EXTRACTED_FILE, "w") as f:
        json.dump(all_data, f)
    logging.info(f"Extracted {len(all_data)} records to {EXTRACTED_FILE}")
