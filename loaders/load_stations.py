# loaders/load_stations.py

from __future__ import annotations
import json
from loguru import logger

from config.settings import load_env
from connectors.mongodb_client import MongoSettings, MongoDBClient
from ingest.s3_client import S3Client
from ingest.s3_reader import S3JSONLReader

from models.stations_model import StationModel


# -----------------------------------------------------------
# 🔍 Extract stations array from an InfoClimat JSONL file
# -----------------------------------------------------------
def extract_stations_from_file(s3_reader: S3JSONLReader, s3_key: str) -> list[dict]:
    """
    Reads only the first JSONL line from an InfoClimat file
    and extracts the 'stations' array.
    """

    logger.info(f"📄 Extracting stations from {s3_key}")

    for raw_line in s3_reader.s3.stream_jsonl_lines(s3_key):
        obj = json.loads(raw_line)
        data = obj.get("_airbyte_data", {})
        stations = data.get("stations", [])

        logger.success(f"✔ Found {len(stations)} station(s) in {s3_key}")
        return stations

    raise ValueError(f"File {s3_key} is empty or unreadable")


# -----------------------------------------------------------
# 📌 UPSERT ONE STATION
# -----------------------------------------------------------
def upsert_station(collection, station_raw: dict):
    """
    Insert station if new.
    Update ONLY if fields differ.
    """

    try:
        station = StationModel.model_validate(station_raw)
    except Exception as e:
        logger.error(f"❌ Invalid station record: {station_raw}")
        raise e

    doc = station.model_dump()

    existing = collection.find_one({"id": doc["id"]})

    if not existing:
        collection.insert_one(doc)
        logger.success(f"🆕 Inserted station {doc['id']} ({doc['name']})")
        return

    if existing != doc:
        collection.replace_one({"id": doc["id"]}, doc)
        logger.success(f"🔄 Updated station {doc['id']} ({doc['name']})")
    else:
        logger.info(f"🟩 Station unchanged: {doc['id']} ({doc['name']})")


# -----------------------------------------------------------
# 🚀 MAIN LOADER — ALL files
# -----------------------------------------------------------
def load_all_stations():
    """
    Loads stations from ALL InfoClimat JSONL files.
    Inserts new stations.
    Updates existing stations only if changed.
    """

    logger.info("🔧 Loading environment...")
    load_env()

    # Connect MongoDB
    mongo_settings = MongoSettings.from_env()
    mongo = MongoDBClient(mongo_settings)
    mongo.connect()

    # Clients
    s3_client = S3Client()
    s3_reader = S3JSONLReader()
    collection = mongo.get_collection(mongo_settings.stations_collection)

    # Find all InfoClimat files
    all_files = s3_client.list_jsonl_files()
    info_files = [f for f in all_files if "infoclimat" in f.lower()]

    if not info_files:
        raise RuntimeError("❌ No InfoClimat source files found in S3!")

    logger.info(f"📂 Found {len(info_files)} InfoClimat file(s)")

    # Process each file
    for s3_key in sorted(info_files):
        logger.info(f"📌 Processing InfoClimat file: {s3_key}")

        stations = extract_stations_from_file(s3_reader, s3_key)

        for st in stations:
            upsert_station(collection, st)

    mongo.close()
    logger.info("🏁 Stations ingestion complete.")


# -----------------------------------------------------------
if __name__ == "__main__":
    load_all_stations()
