# quality/dq_validator.py
import pandas as pd
from datetime import datetime, timezone
from loguru import logger
import time

from pandera.errors import SchemaErrors

from connectors.mongodb_client import MongoDBClient, MongoSettings
from ingest.s3_reader import S3JSONLReader

from quality.infoclimat_schema import infoclimat_schema
from quality.wunderground_schema import wunderground_schema

from dotenv import load_dotenv
load_dotenv()

SCHEMAS = {
    "infoclimat": infoclimat_schema,
    "wunderground": wunderground_schema,
}

class DataQualityValidator:

    def __init__(self, mongo: MongoDBClient):
        self.mongo = mongo
        self.db = mongo.get_database()
        self.staging = self.db[mongo.settings.staging_collection]
        self.ingestion = self.db[mongo.settings.ingestion_tracker_collection]
        self.s3_reader = S3JSONLReader()

    # ---------------------------------------------------------
    def run(self):
        pending = list(
            self.ingestion.find(
                {
                    "success": True,
                    "$or": [
                        {"dq_validated": {"$exists": False}},
                        {"dq_validated": False},
                    ],
                },
                {"s3_key": 1},
            )
        )

        logger.info(f"📌 {len(pending)} file(s) pending DQ")

        for file_entry in pending:
            s3_key = file_entry["s3_key"]
            source = self.s3_reader.detect_source(s3_key)

            logger.info(f"🔍 Validating file: {s3_key} (source={source})")
            self.validate_file(s3_key, source)

    # ---------------------------------------------------------
    @staticmethod
    def stringify_keys(obj):
        """Recursively convert ALL dictionary keys to strings (Mongo-safe)."""
        if isinstance(obj, dict):
            return {str(k): DataQualityValidator.stringify_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [DataQualityValidator.stringify_keys(x) for x in obj]
        return obj

    # ---------------------------------------------------------
    def validate_file(self, s3_key: str, source: str):

        schema = SCHEMAS.get(source)
        if schema is None:
            logger.error(f"❌ Schema not found for source: {source}")
            return

        rows = list(self.staging.find({"s3_key": s3_key}))
        if not rows:
            logger.warning(f"⚠ No staging rows for {s3_key}")
            return

        valid_count = 0
        invalid_count = 0

        for row in rows:
            row_id = row["_id"]
            df = pd.DataFrame([row])

            try:
                schema.validate(df, lazy=True)

                # row is valid
                self.staging.update_one(
                    {"_id": row_id},
                    {
                        "$set": {
                            "dq_checked": True,
                            "error": None,
                        }
                    }
                )
                valid_count += 1

            except SchemaErrors as exc:
                invalid_count += 1

                # extract error messages
                failure_dict = exc.failure_cases.to_dict()

                # 🔥 Clean numeric keys → strings (Mongo-safe)
                clean_error = self.stringify_keys(failure_dict)

                self.staging.update_one(
                    {"_id": row_id},
                    {
                        "$set": {
                            "dq_checked": False,
                            "error": True,
                        }
                    }
                )

                logger.debug(f"❌ Row {row_id} failed: {clean_error}")

        # FILE LEVEL
        file_valid = (invalid_count == 0)

        self.ingestion.update_one(
            {"s3_key": s3_key},
            {
                "$set": {
                    "dq_validated": file_valid,
                    "dq_run_at": datetime.now(timezone.utc),
                }
            }
        )

        if file_valid:
            logger.success(f"🎉 File {s3_key} VALID")
        else:
            logger.error(f"❌ File {s3_key} INVALID ({invalid_count} bad rows)")


def run_all_dq_tests():
    settings = MongoSettings.from_env()
    mongo = MongoDBClient(settings)
    mongo.connect()

    dq = DataQualityValidator(mongo)
    dq.run()

    mongo.close()

# ---------------------------------------------------------
if __name__ == "__main__":
    run_all_dq_tests()
