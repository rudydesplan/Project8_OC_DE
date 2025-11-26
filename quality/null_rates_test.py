# quality/null_rates_test.py

from loguru import logger
from connectors.mongodb_client import MongoDBClient, MongoSettings
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

def run_null_rates_test():
    logger.info("\n🔍 Running Test 4: Null-Rates Analysis")

    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    db = mongo.get_database()
    final = db[settings.final_collection]

    total = final.count_documents({})
    logger.info(f"📦 Total documents in final = {total}")

    if total == 0:
        logger.error("❌ No documents in final — cannot compute null rates.")
        return

    # ---------------------------------------------------------
    # INITIALISATION
    # ---------------------------------------------------------
    fields = set()
    per_field_nulls = defaultdict(int)
    per_field_values = defaultdict(int)

    # For source-level analysis
    source_nulls = defaultdict(lambda: defaultdict(int))
    source_counts = defaultdict(int)

    # ---------------------------------------------------------
    # SCAN DOCUMENTS
    # ---------------------------------------------------------
    for doc in final.find({}):

        source = (
            "infoclimat" if "InfoClimat" in doc["s3_key"]
            else "wunderground" if ("Madeleine" in doc["s3_key"] or "Ichtegem" in doc["s3_key"])
            else "unknown"
        )

        source_counts[source] += 1

        for key, value in doc.items():

            if key in ["_id"]: 
                continue

            fields.add(key)

            per_field_values[key] += 1
            if value is None:
                per_field_nulls[key] += 1
                source_nulls[source][key] += 1

    # ---------------------------------------------------------
    # GLOBAL REPORT
    # ---------------------------------------------------------
    logger.info("\n===== GLOBAL NULL-RATES =====")
    results = []

    for f in sorted(fields):
        nulls = per_field_nulls[f]
        pct = (nulls / total) * 100
        results.append((f, nulls, pct))

    for f, n, p in results:
        logger.info(f"{f:20s} → nulls={n:4d}  ({p:5.2f}%)")

    # ---------------------------------------------------------
    # SOURCE-SPECIFIC REPORT
    # ---------------------------------------------------------
    logger.info("\n===== NULL-RATES BY SOURCE =====")

    for source in source_counts.keys():
        logger.info(f"\n--- {source.upper()} ({source_counts[source]} docs) ---")

        for f in sorted(fields):
            nulls = source_nulls[source][f]
            pct = (nulls / source_counts[source]) * 100 if source_counts[source] else 0
            logger.info(f"{f:20s} → nulls={nulls:4d}  ({pct:5.2f}%)")

    # ---------------------------------------------------------
    # FLAG COLUMNS WITH HIGH NULL-RATES
    # ---------------------------------------------------------
    logger.info("\n===== FIELDS WITH SUSPICIOUS NULL-RATES =====")

    suspicious = [r for r in results if r[2] > 70]

    if suspicious:
        logger.warning("⚠️ Fields with >70% nulls:")
        for f, n, p in suspicious:
            logger.warning(f" → {f}: {p:.2f}% null")
    else:
        logger.success("✔ No suspicious null-rates.")

    mongo.close()
    logger.success("\n🏁 Null-rates test completed.")


if __name__ == "__main__":
    run_null_rates_test()
