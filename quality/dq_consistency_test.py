# quality/dq_consistency_test.py

from loguru import logger
from connectors.mongodb_client import MongoDBClient, MongoSettings

def run_dq_consistency_test():
    logger.info("\n🔍 Running Test 3: DQ Consistency Staging → Final")

    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    db = mongo.get_database()
    staging = db[settings.staging_collection]
    final = db[settings.final_collection]
    ingestion = db[settings.ingestion_tracker_collection]

    # ---------------------------------------------------------
    # 1) STAGING VALID FILES MUST APPEAR IN FINAL
    # ---------------------------------------------------------
    valid_files = ingestion.distinct("s3_key", {"dq_validated": True})
    missing_in_final = []

    for key in valid_files:
        if final.count_documents({"s3_key": key}) == 0:
            missing_in_final.append(key)

    if missing_in_final:
        logger.error(f"❌ {len(missing_in_final)} validated staging files missing in final:")
        for k in missing_in_final[:20]:
            logger.warning(k)
    else:
        logger.success("✔ All validated staging files appear in final.")

    # ---------------------------------------------------------
    # 2) INVALID STAGING FILES MUST NOT APPEAR IN FINAL
    # ---------------------------------------------------------
    invalid_files = ingestion.distinct("s3_key", {"dq_validated": False})
    wrongly_in_final = []

    for key in invalid_files:
        if final.count_documents({"s3_key": key}) > 0:
            wrongly_in_final.append(key)

    if wrongly_in_final:
        logger.error("❌ Invalid files present in final:")
        for k in wrongly_in_final[:20]:
            logger.warning(k)
    else:
        logger.success("✔ No invalid staging file leaked into final.")
 
    # ---------------------------------------------------------
    # 5) SOURCE DISTRIBUTION CHECK
    # If source exists in staging validated → must appear in final
    # ---------------------------------------------------------
    source_map = {
        "infoclimat": lambda k: "InfoClimat" in k,
        "wunderground": lambda k: ("Ichtegem" in k or "Madeleine" in k),
    }

    for source_name, detector in source_map.items():
        staging_sources = [k for k in valid_files if detector(k)]
        final_sources = [k for k in final.distinct("s3_key") if detector(k)]

        if len(staging_sources) > 0 and len(final_sources) == 0:
            logger.error(f"❌ Source '{source_name}' appears in staging but NOT in final!")

    logger.success("🏁 Test 3 completed.")


if __name__ == "__main__":
    run_dq_consistency_test()
