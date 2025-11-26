# database/check_db_state.py

from loguru import logger
from connectors.mongodb_client import MongoSettings, MongoDBClient
from config.settings import load_env

# --- Import all JSONSchemas ---
from database.schemas.stations_schema import STATIONS_VALIDATOR
from database.schemas.metadata_schema import METADATA_VALIDATOR
from database.schemas.hourly_staging_schema import HOURLY_STAGING_SCHEMA
from database.schemas.hourly_measurements_schema import HOURLY_MEASUREMENTS_SCHEMA
from database.schemas.ingestion_tracker_schema import INGESTION_TRACKER_SCHEMA


EXPECTED = {
    "stations": STATIONS_VALIDATOR,
    "metadata": METADATA_VALIDATOR,
    "hourly_staging": HOURLY_STAGING_SCHEMA,
    "hourly_measurements": HOURLY_MEASUREMENTS_SCHEMA,
    "ingestion_tracker": INGESTION_TRACKER_SCHEMA,
}


def check_db_state():
    """
    Check if MongoDB validators match expected JSONSchemas.
    Detects schema drift by comparing $jsonSchema blocks.
    """

    load_env()
    settings = MongoSettings.from_env()

    mongo = MongoDBClient(settings)
    mongo.connect()
    db = mongo.get_database()

    logger.info("Checking MongoDB validators for schema drift...")

    for coll_name, expected_schema in EXPECTED.items():

        resp = db.command("listCollections", filter={"name": coll_name})
        coll_info = resp["cursor"]["firstBatch"]

        if not coll_info:
            logger.error(f"Missing collection: {coll_name}")
            continue

        validator = coll_info[0]["options"].get("validator", {})

        # --- NEW: unwrap `$jsonSchema`
        actual_schema = validator.get("$jsonSchema")

        if actual_schema != expected_schema:
            logger.error(f"❌ Schema drift detected in collection '{coll_name}'")
            logger.debug(f"Expected: {expected_schema}")
            logger.debug(f"Actual  : {actual_schema}")
        else:
            logger.success(f"✔ Schema OK for '{coll_name}'")

    mongo.close()


if __name__ == "__main__":
    check_db_state()
