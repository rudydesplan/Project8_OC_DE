# loaders/load_metadata.py

from __future__ import annotations
import json
from loguru import logger

from config.settings import load_env
from connectors.mongodb_client import MongoSettings, MongoDBClient
from ingest.s3_client import S3Client
from ingest.s3_reader import S3JSONLReader
from models.metadata_model import MetadataModel


# ----------------------------------------------------------------------
# EXTRACT METADATA FROM ONE INFOCLIMAT FILE
# ----------------------------------------------------------------------
def extract_metadata_from_file(s3_reader: S3JSONLReader, s3_key: str) -> dict:
    """
    Reads the *first line* of an InfoClimat JSONL file
    and extracts the metadata block.
    """
    logger.info(f"📄 Extracting metadata from {s3_key}")

    # ⬇⬇⬇ FIX: use the underlying S3 stream, not iter_raw()
    for raw_line in s3_reader.s3.stream_jsonl_lines(s3_key):
        obj = json.loads(raw_line)

        if "_airbyte_data" not in obj:
            continue

        data = obj["_airbyte_data"]
        metadata = data.get("metadata")

        if metadata:
            logger.success(f"✔ Metadata extracted from {s3_key}")
            return metadata

        # If metadata not inside _airbyte_data but top-level (rare)
        if "metadata" in obj:
            logger.success(f"✔ Metadata extracted from {s3_key}")
            return obj["metadata"]

    raise ValueError(f"No metadata block found inside file: {s3_key}")



# ----------------------------------------------------------------------
# UPSERT METADATA INTO MONGODB
# ----------------------------------------------------------------------
def upsert_metadata(mongo: MongoDBClient, metadata_dict: dict):
    """
    Insert or update metadata document.
    Only one metadata row should exist:
        id = "infoclimat"
    """

    collection = mongo.get_collection(mongo.settings.metadata_collection)

    # Convert to Pydantic model
    metadata_model = MetadataModel(**metadata_dict)

    # Does metadata already exist?
    existing = collection.find_one({"id": metadata_model.id})

    if not existing:
        # New → insert
        collection.insert_one(metadata_model.model_dump())
        logger.success("🆕 Inserted new metadata document (id='infoclimat')")
        return

    # Existing → check differences
    updates = {}
    for field, new_value in metadata_model.model_dump().items():
        old_value = existing.get(field)
        if new_value != old_value:
            updates[field] = new_value

    if not updates:
        logger.info("✔ Metadata unchanged → no update required")
        return

    collection.update_one(
        {"id": metadata_model.id},
        {"$set": updates},
    )

    logger.success(f"🔄 Metadata updated: {list(updates.keys())}")


# ----------------------------------------------------------------------
# MAIN PIPELINE: INGEST ALL INFOCLIMAT METADATA FILES
# ----------------------------------------------------------------------
def load_all_metadata():
    """
    Loads metadata from first InfoClimat file found.
    Metadata is global → only one document maintained in MongoDB.
    """

    logger.info("🔧 Loading environment...")
    load_env()

    # MongoDB client
    mongo_settings = MongoSettings.from_env()
    mongo = MongoDBClient(mongo_settings)
    mongo.connect()

    # S3 + reader
    s3_client = S3Client()
    s3_reader = S3JSONLReader()

    # Find all InfoClimat files in S3
    all_files = s3_client.list_jsonl_files()
    infoclimat_files = [f for f in all_files if "infoclimat" in f.lower()]

    if not infoclimat_files:
        logger.warning("⚠ No InfoClimat metadata files found in S3.")
        mongo.close()
        return

    # Metadata is identical in all InfoClimat files → read the first one
    s3_key = infoclimat_files[0]
    logger.info(f"📌 Using metadata from file: {s3_key}")

    try:
        metadata_dict = extract_metadata_from_file(s3_reader, s3_key)
        upsert_metadata(mongo, metadata_dict)

    except Exception as e:
        logger.error(f"❌ Metadata ingestion failed for {s3_key}: {e}")
        raise

    mongo.close()
    logger.info("🏁 Metadata ingestion complete.")


# ----------------------------------------------------------------------

if __name__ == "__main__":
    load_all_metadata()
