# quality/volume_test_v2.py

from connectors.mongodb_client import MongoDBClient, MongoSettings
from ingest.s3_reader import S3JSONLReader
from loguru import logger
import json

from dotenv import load_dotenv
load_dotenv()

def count_infoclimat_from_jsonl(reader, key: str) -> int:
    """Count expanded InfoClimat hourly rows inside JSONL lines."""
    total = 0
    for line in reader.s3.stream_jsonl_lines(key):
        try:
            raw = json.loads(line)
        except:
            continue

        data = raw.get("_airbyte_data", {})
        hourly = data.get("hourly", {})

        for station_code, rows in hourly.items():
            if station_code == "_params":
                continue
            if isinstance(rows, list):
                total += len(rows)

    return total


def run_volume_test_v2():

    logger.info("\n🔍 Running Volume Test V2 (DQ-aware)")

    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    staging = mongo.get_collection("hourly_staging")
    final = mongo.get_collection("hourly_measurements")

    reader = S3JSONLReader()

    s3_keys = staging.distinct("s3_key")

    for s3_key in s3_keys:

        logger.info(f"\n📦 Volume check for {s3_key}")

        # -------------------------------------------------------
        # 1️⃣ Expected count directly from S3 raw files
        # -------------------------------------------------------
        if "InfoClimat" in s3_key:
            expected_total = count_infoclimat_from_jsonl(reader, s3_key)
        else:
            # 1 row = 1 line
            expected_total = sum(1 for _ in reader.s3.stream_jsonl_lines(s3_key))

        # -------------------------------------------------------
        # 2️⃣ Staging counts (with dq_checked split)
        # -------------------------------------------------------
        staging_total = staging.count_documents({"s3_key": s3_key})
        staging_valid = staging.count_documents({"s3_key": s3_key, "dq_checked": True})
        staging_invalid = staging.count_documents({"s3_key": s3_key, "dq_checked": False})

        # -------------------------------------------------------
        # 3️⃣ Final count (only valid rows must pass)
        # -------------------------------------------------------
        final_count = final.count_documents({"s3_key": s3_key})

        # -------------------------------------------------------
        # LOGGING DETAILS
        # -------------------------------------------------------
        logger.info(f"  • Expected rows in S3       = {expected_total}")
        logger.info(f"  • STAGING total rows        = {staging_total}")
        logger.info(f"     ↳ dq_checked = True      = {staging_valid}")
        logger.info(f"     ↳ dq_checked = False     = {staging_invalid}")
        logger.info(f"  • FINAL rows                = {final_count}")

        # -------------------------------------------------------
        # VALIDATION RULES
        # -------------------------------------------------------

        # RULE A — staging_total must match S3 total
        if staging_total != expected_total:
            logger.warning(f"⚠️ STAGING mismatch: expected={expected_total}, got={staging_total}")
        else:
            logger.success("✔ STAGING raw volume matches S3")

        # RULE B — final must match staging_valid
        if final_count != staging_valid:
            logger.error(
                f"❌ FINAL mismatch: final={final_count} but staging_valid={staging_valid}"
            )
        else:
            logger.success("✔ FINAL volume matches staging_valid")

        # RULE C — invalid rows must NEVER appear in final
        if staging_invalid > 0:
            invalid_any_in_final = final.count_documents({
                "s3_key": s3_key,
                "dq_checked": False  # should not exist in final
            })

            if invalid_any_in_final > 0:
                logger.error(f"❌ ERROR: invalid rows detected in FINAL for {s3_key}")
            else:
                logger.success("✔ Invalid staging rows correctly excluded from final")

    mongo.close()
    logger.success("\n🏁 Volume Test V2 complete.")


if __name__ == "__main__":
    run_volume_test_v2()
