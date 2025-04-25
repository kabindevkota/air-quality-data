import json, os, logging
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
TRANSFORMED_FILE = "data/realtime_transformed.json"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://host.docker.internal:27017/")
DB_NAME = "air_quality"
COLLECTION_NAME = "us_air_data"


def load_realtime_data():
    client = MongoClient(MONGO_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    with open(TRANSFORMED_FILE) as f:
        structured = json.load(f)

    for loc in structured:
        loc_id = loc["location_id"]
        for sensor in loc["sensors"]:
            sid = sensor["sensor_id"]
            for m in sensor["measurements"]:
                collection.update_one(
                    {"location_id": loc_id, "sensors.sensor_id": sid},
                    {"$addToSet": {"sensors.$.measurements": m}},
                )

    logging.info("MongoDB updated with transformed real-time data.")
