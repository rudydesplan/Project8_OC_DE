# database/init_db.py

from loguru import logger
from connectors.mongodb_client import MongoSettings, MongoDBClient
from config.settings import load_env

# --- JSONSchemas ---
from database.schemas.stations_schema import STATIONS_VALIDATOR
from database.schemas.metadata_schema import METADATA_VALIDATOR
from database.schemas.hourly_staging_schema import HOURLY_STAGING_SCHEMA
from database.schemas.hourly_measurements_schema import HOURLY_MEASUREMENTS_SCHEMA
from database.schemas.ingestion_tracker_schema import INGESTION_TRACKER_SCHEMA

# --- Seed data ---
from database.stations_seed import STATIONS_SEED_DATA


# --------------------------------------------------------------------
# COLLECTION → VALIDATOR mapping
# --------------------------------------------------------------------
COLLECTION_SCHEMAS = {
    "stations": STATIONS_VALIDATOR,
    "metadata": METADATA_VALIDATOR,
    "hourly_staging": HOURLY_STAGING_SCHEMA,
    "hourly_measurements": HOURLY_MEASUREMENTS_SCHEMA,
    "ingestion_tracker": INGESTION_TRACKER_SCHEMA,
}


def init_database():
    """
    Initialize MongoDB:
    - Create DB if missing
    - Create or update collections + validators
    - Insert static stations seed data
    """

    load_env()
    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    db = mongo.get_database()

    logger.info("Initializing MongoDB collections with validators...")

    # -------------------------------------------------------
    # Create / update collections
    # -------------------------------------------------------
    for coll_name, schema in COLLECTION_SCHEMAS.items():
        validator = {"$jsonSchema": schema}

        try:
            if coll_name in db.list_collection_names():
                logger.info(f"Updating validator for existing collection '{coll_name}'")

                db.command(
                    "collMod",
                    coll_name,
                    validator=validator,
                    validationLevel="strict",
                )

            else:
                logger.info(f"Creating collection '{coll_name}' with validator")

                db.create_collection(
                    coll_name,
                    validator=validator,
                    validationLevel="strict",
                )

        except Exception as e:
            logger.error(f"Error initializing collection '{coll_name}': {e}")
            raise

    logger.success("All collections initialized!")

    # -------------------------------------------------------
    # INSERT STATIONS SEED DATA
    # -------------------------------------------------------
    stations_coll = db["stations"]

    for station in STATIONS_SEED_DATA:
        station_id = station["id"]

        exists = stations_coll.find_one({"id": station_id})

        if exists:
            logger.info(f"Station '{station_id}' already exists → skip")
            continue

        try:
            stations_coll.insert_one(station)
            logger.success(f"Inserted station seed: {station_id}")
        except Exception as e:
            logger.error(f"❌ Failed inserting station '{station_id}': {e}")
            raise

    logger.success("🌱 Stations seed insertion complete!")
    mongo.close()


if __name__ == "__main__":
    init_database()
