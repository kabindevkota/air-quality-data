from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "air_quality"
COLLECTION_NAME = "us_air_data"

client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]


def get_location_options():
    locations = collection.find({}, {"location_id": 1, "location_name": 1, "_id": 0})
    return [
        {"label": loc["location_name"], "value": loc["location_id"]}
        for loc in locations
    ]


def get_parameters_for_location(location_id):
    doc = collection.find_one({"location_id": location_id})
    params = set(sensor["parameter"] for sensor in doc.get("sensors", []))
    return [{"label": p.upper(), "value": p} for p in sorted(params)]


def get_location_markers():
    cursor = collection.find(
        {},
        {
            "_id": 0,
            "location_id": 1,
            "location_name": 1,
            "locality": 1,
            "coordinates": 1,
        },
    )
    data = []
    for doc in cursor:
        if doc.get("coordinates"):
            data.append(
                {
                    "location_id": doc["location_id"],
                    "location_name": doc["location_name"],
                    "locality": doc.get("locality"),
                    "lat": doc["coordinates"]["latitude"],
                    "lon": doc["coordinates"]["longitude"],
                }
            )
    return pd.DataFrame
