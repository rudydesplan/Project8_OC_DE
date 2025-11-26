from __future__ import annotations
from loguru import logger

from config.settings import load_env
from connectors.mongodb_client import MongoSettings, MongoDBClient
from ingest.s3_client import S3Client
from ingest.s3_reader import S3JSONLReader
from ingest.ingestion_tracker import IngestionTracker
from models.hourly_staging_model import HourlyStagingModel


# ----------------------------------------------------------------------
# 🔍 Resolve station ID from MongoDB based on S3 file path
# ----------------------------------------------------------------------
def resolve_station_id(mongo: MongoDBClient, s3_key: str) -> str:
    key = s3_key.lower()
    stations = mongo.get_collection(mongo.settings.stations_collection)

    if "ichtegem" in key:
        city = "Ichtegem"

    elif "madeleine" in key:
        city = "La Madeleine"

    else:
        return None

    doc = stations.find_one({"city": city}, {"id": 1})
    if not doc:
        raise ValueError(f"No station found in DB for city={city}")

    station_id = doc["id"]
    logger.info(f"📌 Resolved station for file {s3_key}: {station_id}")
    return station_id


# ----------------------------------------------------------------------
# INGEST ONE FILE INTO STAGING
# ----------------------------------------------------------------------
def ingest_file_to_staging(
    s3_key: str,
    s3_reader: S3JSONLReader,
    mongo: MongoDBClient,
    tracker: IngestionTracker,
):
    logger.info(f"🚀 Starting ingestion for {s3_key}")

    tracker.start_ingestion(s3_key)

    staging_collection = mongo.get_collection(mongo.settings.staging_collection)

    station_id_override = resolve_station_id(mongo, s3_key)

    validated_docs = []
    lines_read = 0

    try:
        for record in s3_reader.iter_records(s3_key):
            lines_read += 1

            # Inject station ID if inferred from path
            if station_id_override:
                record["id_station"] = station_id_override
            else:
                if "id_station" not in record:
                    raise ValueError(
                        f"id_station is missing in record and cannot be inferred for file {s3_key}"
                    )

            # Inject s3_key for DQ lineage
            record["s3_key"] = s3_key

            model = HourlyStagingModel.model_validate(record)
            validated_docs.append(model.model_dump())

        # Insert in bulk
        if validated_docs:
            result = staging_collection.insert_many(validated_docs, ordered=False)
            logger.success(f"Inserted {len(result.inserted_ids)} rows into staging.")

        # Compute hash
        file_hash = s3_reader.s3.compute_file_hash(s3_key)

        tracker.mark_success(
            s3_key=s3_key,
            lines_read=lines_read,
            file_hash=file_hash,
        )

        logger.success(f"✔ Ingestion complete for {s3_key}")

    except Exception as e:
        logger.error(f"❌ Error during ingestion of {s3_key}: {e}")
        tracker.mark_failure(s3_key=s3_key, error_message=str(e))
        raise


# ----------------------------------------------------------------------
# INGEST ALL NEW OR MODIFIED FILES
# ----------------------------------------------------------------------
def ingest_all_staging():

    logger.info("🔧 Loading environment...")
    load_env()

    mongo_settings = MongoSettings.from_env()
    mongo = MongoDBClient(mongo_settings)
    mongo.connect()

    s3_client = S3Client()
    s3_reader = S3JSONLReader()
    tracker = IngestionTracker(mongo)

    s3_files = s3_client.list_jsonl_files()
    logger.info(f"📂 {len(s3_files)} JSONL files found in S3")

    known_files = tracker.list_known_files()

    for s3_key in s3_files:

        is_new = s3_key not in known_files
        current_hash = s3_client.compute_file_hash(s3_key)
        previous_hash = tracker.get_file_hash(s3_key)
        is_modified = (previous_hash is not None and current_hash != previous_hash)
        is_success = tracker.was_successful(s3_key)

        if is_new:
            logger.info(f"🟦 NEW FILE → ingest: {s3_key}")

        elif is_modified:
            logger.info(f"🟨 MODIFIED FILE → re-ingest: {s3_key}")

        elif not is_success:
            logger.info(f"🟧 FAILED → retry: {s3_key}")

        else:
            logger.info(f"🟩 SKIP: already successfully processed → {s3_key}")
            continue

        ingest_file_to_staging(s3_key, s3_reader, mongo, tracker)

    mongo.close()
    logger.info("🏁 Staging ingestion complete.")


if __name__ == "__main__":
    ingest_all_staging()
