import os
import ijson
from decimal import Decimal
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "air_quality"
COLLECTION_NAME = "us_air_data"
INPUT_FILE = "data/US_data_structured_cleaned.json"
BATCH_SIZE = 500


def connect_to_mongo(uri=MONGO_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]


def convert_decimals(obj):
    """Recursively convert Decimal objects to float."""
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj


def load_json_to_mongo(file_path, collection, batch_size=BATCH_SIZE):
    batch = []

    with open(file_path, "r", encoding="utf-8") as f:
        for record in ijson.items(f, "item"):
            record = convert_decimals(record)
            batch.append(record)

            if len(batch) >= batch_size:
                collection.insert_many(batch)
                print(f"Inserted {len(batch)} records...")
                batch.clear()

    if batch:
        collection.insert_many(batch)
        print(f"Inserted final {len(batch)} records.")

    print("✔ All records inserted into MongoDB.")


def main():
    collection = connect_to_mongo()

    clear = input("Clear existing collection? (y/n): ").strip().lower()
    if clear == "y":
        result = collection.delete_many({})
        print(f"Cleared {result.deleted_count} existing documents from the collection.")

    print("Starting MongoDB batch load...")
    load_json_to_mongo(INPUT_FILE, collection, BATCH_SIZE)


if __name__ == "__main__":
    main()
