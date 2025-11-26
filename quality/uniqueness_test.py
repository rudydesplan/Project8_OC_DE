from connectors.mongodb_client import MongoDBClient, MongoSettings
from loguru import logger
import pathlib

from dotenv import load_dotenv
load_dotenv()


def test_staging_uniqueness():

    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    final = mongo.get_collection("hourly_measurements")

    logger.info("\n🔍 Checking uniqueness in hourly_measurements (id_station, dh_utc)...")

    pipeline = [
        {
            "$group": {
                "_id": {
                    "id_station": "$id_station",
                    "dh_utc": "$dh_utc"
                },
                "count": {"$sum": 1},
                "docs": {"$push": "$_id"}
            }
        },
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = list(final.aggregate(pipeline))

    if not duplicates:
        logger.success("✔ Uniqueness OK: no duplicates in final.")
    else:
        logger.error(f"❌ Found {len(duplicates)} duplicate groups in final!")

        for dup in duplicates:
            station = dup["_id"]["id_station"]
            dh = dup["_id"]["dh_utc"]
            count = dup["count"]

            logger.error(f"  → Duplicate: station={station}, dh_utc={dh}, count={count}")

    mongo.close()


if __name__ == "__main__":
    test_staging_uniqueness()
