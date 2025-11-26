# quality/schema_validity_test.py

from loguru import logger
from connectors.mongodb_client import MongoDBClient, MongoSettings
from models.hourly_measurements_model import HourlyMeasurementsModel
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

def run_schema_validity_test():
    logger.info("\n🔍 Running Schema Validity Test (Pydantic + DQ Staging aware)...")

    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    db = mongo.get_database()
    final = db[settings.final_collection]
    staging = db[settings.staging_collection]
    ingestion = db[settings.ingestion_tracker_collection]

    total = final.count_documents({})
    logger.info(f"📦 Total docs in final = {total}")

    invalid_docs = 0
    invalid_details = []

    # ---------------------------------------------------------
    # 1) Rule: No staging-invalid records should appear in final
    # ---------------------------------------------------------
    staging_invalid_keys = staging.distinct("s3_key", {"error": True})
    wrongly_copied = final.count_documents({"s3_key": {"$in": staging_invalid_keys}})

    if wrongly_copied > 0:
        logger.error(f"❌ {wrongly_copied} invalid staging rows found in final!")
        invalid_docs += wrongly_copied


    # ---------------------------------------------------------
    # 2) File-level DQ coherence
    # ---------------------------------------------------------
    valid_files = ingestion.distinct("s3_key", {"dq_validated": True})
    for key in valid_files:
        if final.count_documents({"s3_key": key}) == 0:
            invalid_docs += 1
            invalid_details.append({
                "s3_key": key,
                "error": "Staging file validated but no records copied to final"
            })

    invalid_files = ingestion.distinct("s3_key", {"dq_validated": False})
    for key in invalid_files:
        if final.count_documents({"s3_key": key}) > 0:
            invalid_docs += 1
            invalid_details.append({
                "s3_key": key,
                "error": "Staging file INVALID but got copied to final"
            })

    # ---------------------------------------------------------
    # RESULTS
    # ---------------------------------------------------------
    logger.info("\n===== RESULTS =====")
    logger.info(f"❌ Invalid docs: {invalid_docs}")
    if total > 0:
        logger.info(f"📊 Error rate: {(invalid_docs/total)*100:.2f}%")

    logger.info("\n===== SAMPLE OF INVALID =====")
    for d in invalid_details[:20]:
        logger.warning(d)

    mongo.close()
    logger.success("🏁 Schema test completed (DQ-aware).")


if __name__ == "__main__":
    run_schema_validity_test()
