# quality/latency_test.py

from loguru import logger
from connectors.mongodb_client import MongoDBClient, MongoSettings
import time
from statistics import mean

from dotenv import load_dotenv
load_dotenv()

def measure(action_name, func, iterations=5):
    """
    Measure average latency of a DB call.
    """
    durations = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        durations.append((time.perf_counter() - start) * 1000)  # ms
    avg = mean(durations)
    logger.info(f"{action_name:<40} → {avg:8.3f} ms (avg over {iterations} runs)")
    return avg


def run_latency_test():
    logger.info("\n🚀 Running TEST 5: MongoDB Latency Benchmark")

    settings = MongoSettings.from_env()

    # ---------------------------------------------------------
    # 1) Measure connection latency
    # ---------------------------------------------------------
    start = time.perf_counter()
    mongo = MongoDBClient(settings)
    mongo.connect()
    conn_latency = (time.perf_counter() - start) * 1000
    logger.info(f"🔗 Connection established in {conn_latency:.3f} ms")

    db = mongo.get_database()
    final = db[settings.final_collection]

    logger.info(f"📦 Target collection: {settings.final_collection}")

    # ---------------------------------------------------------
    # 2) Latency for various read operations
    # ---------------------------------------------------------
    measure("find_one() any doc", lambda: final.find_one({}))

    measure("count_documents({})", lambda: final.count_documents({}))

    measure("find_one({id_station})", lambda: final.find_one({"id_station": {"$exists": True}}))

    measure("count_documents({source})", lambda: final.count_documents({"s3_key": {"$regex": "InfoClimat"}}))

    measure("simple sorted query", lambda: list(final.find({}).sort("dh_utc", -1).limit(1)))

    measure("aggregation (group by id_station)", lambda: list(
        final.aggregate([
            {"$group": {"_id": "$id_station", "count": {"$sum": 1}}},
            {"$limit": 50}
        ])
    ))

    # ---------------------------------------------------------
    # 3) Summary section
    # ---------------------------------------------------------
    logger.success("\n🏁 MongoDB latency benchmark completed.\n")
    mongo.close()


if __name__ == "__main__":
    run_latency_test()
